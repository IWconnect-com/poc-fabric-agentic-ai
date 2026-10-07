"""Source registry: which tables are ingested and how. Lives in Git (config/sources/*.yaml).

Every identifier is validated here, so SQL generation downstream can trust it.
"""
from __future__ import annotations

import re
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .errors import SpecInvalid
from .models import LoadStrategy, TableDecision

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def check_ident(value: str) -> str:
    if not isinstance(value, str) or not _IDENT.fullmatch(value):
        raise ValueError(f"unsafe identifier {value!r}")
    return value


class SourceEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_id: str
    source_schema: str = "dbo"
    source_table: str
    target_table: str
    load_strategy: LoadStrategy
    business_key: list[str] = Field(min_length=1)
    columns: list[str] = Field(min_length=1)        # explicit list: never SELECT * from an ERP
    watermark_column: str | None = None
    lookback_hours: int = Field(default=24, ge=0, le=720)
    partition_column: str | None = None             # must be date/datetime; used for reconciliation
    measure_columns: list[str] = Field(default_factory=list)
    load_order: int = 100
    is_active: bool = True

    @field_validator("source_id", "source_schema", "source_table", "target_table", "watermark_column", "partition_column")
    @classmethod
    def _ident_scalar(cls, v: str | None) -> str | None:
        return None if v is None else check_ident(v)

    @field_validator("business_key", "columns", "measure_columns")
    @classmethod
    def _ident_list(cls, v: list[str]) -> list[str]:
        return [check_ident(x) for x in v]

    @model_validator(mode="after")
    def _consistent(self) -> "SourceEntry":
        wm = self.load_strategy == LoadStrategy.INCREMENTAL_WATERMARK
        if wm and not self.watermark_column:
            raise ValueError("incremental_watermark requires watermark_column")
        if not wm and self.watermark_column:
            raise ValueError("watermark_column is only valid with incremental_watermark")
        cols = set(self.columns)
        needed = set(self.business_key) | set(self.measure_columns)
        if self.watermark_column:
            needed.add(self.watermark_column)
        if self.partition_column:
            needed.add(self.partition_column)
        missing = needed - cols
        if missing:
            raise ValueError(f"columns must include {sorted(missing)}")
        return self


class Registry:
    def __init__(self, entries: list[SourceEntry]) -> None:
        ids = [e.source_id for e in entries]
        targets = [e.target_table for e in entries]
        if len(ids) != len(set(ids)):
            raise SpecInvalid("duplicate source_id in registry")
        if len(targets) != len(set(targets)):
            raise SpecInvalid("duplicate target_table in registry")
        self._entries = list(entries)

    @classmethod
    def from_yaml(cls, path: str | Path) -> "Registry":
        p = Path(path)
        files = sorted(p.glob("*.yaml")) if p.is_dir() else [p]
        entries: list[SourceEntry] = []
        for f in files:
            with open(f, encoding="utf-8") as fh:
                doc = yaml.safe_load(fh) or {}
            entries.extend(SourceEntry.model_validate(item) for item in doc.get("sources", []))
        return cls(entries)

    def active(self) -> list[SourceEntry]:
        return sorted((e for e in self._entries if e.is_active), key=lambda e: (e.load_order, e.source_id))

    def get(self, source_id: str) -> SourceEntry:
        for e in self._entries:
            if e.source_id == source_id:
                return e
        raise SpecInvalid(f"unknown source_id {source_id!r}")


def entry_from_decision(
    decision: TableDecision,
    *,
    columns: list[str],
    partition_column: str | None = None,
    measure_columns: list[str] | None = None,
    load_order: int = 100,
) -> SourceEntry:
    """Turn an approved TableDecision into a registry entry. Columns come from code (metadata read)."""
    if not decision.include:
        raise SpecInvalid("cannot build a registry entry from a skipped table", table=decision.table)
    schema, _, table = decision.table.rpartition(".")
    schema = schema or "dbo"
    return SourceEntry(
        source_id=f"{schema}_{table}".lower(),
        source_schema=schema,
        source_table=table,
        target_table=f"{schema}_{table}".lower(),
        load_strategy=decision.load_strategy,
        business_key=decision.business_key,
        columns=columns,
        watermark_column=decision.watermark_column,
        lookback_hours=decision.lookback_hours,
        partition_column=partition_column,
        measure_columns=measure_columns or [],
        load_order=load_order,
    )
