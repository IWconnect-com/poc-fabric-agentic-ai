"""Typed contracts. The LLM's output is one of these objects, never prose.

Parsing is strict: anything outside the enums or ranges is rejected before code
acts on it.
"""
from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, ConfigDict, Field, model_validator


class TableRole(str, Enum):
    FACT = "fact"
    DIMENSION = "dimension"
    REFERENCE = "reference"
    SKIP = "skip"


class LoadStrategy(str, Enum):
    INCREMENTAL_WATERMARK = "incremental_watermark"
    INCREMENTAL_HASH = "incremental_hash"
    FULL = "full"


class TableDecision(BaseModel):
    """One table, as proposed by the author step. Validated by code before use."""

    model_config = ConfigDict(extra="forbid")

    table: str = Field(pattern=r"^[A-Za-z0-9_]+\.[A-Za-z0-9_]+$|^[A-Za-z0-9_]+$")
    include: bool
    role: TableRole
    business_key: list[str] = Field(default_factory=list)
    load_strategy: LoadStrategy = LoadStrategy.FULL
    watermark_column: str | None = None
    lookback_hours: int = Field(default=24, ge=0, le=24 * 30)
    reason: str = Field(min_length=1)
    confidence: float = Field(ge=0.0, le=1.0)
    open_questions: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _consistent(self) -> "TableDecision":
        if self.include != (self.role != TableRole.SKIP):
            raise ValueError("include must be false exactly when role is 'skip'")
        if self.include and self.role in (TableRole.FACT, TableRole.DIMENSION) and not self.business_key:
            raise ValueError("fact and dimension tables need a business_key")
        if self.load_strategy == LoadStrategy.INCREMENTAL_WATERMARK and not self.watermark_column:
            raise ValueError("incremental_watermark requires watermark_column")
        if self.load_strategy != LoadStrategy.INCREMENTAL_WATERMARK and self.watermark_column:
            raise ValueError("watermark_column is only valid with incremental_watermark")
        return self


class AuthorSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    scope_ref: str
    tables: list[TableDecision]

    @model_validator(mode="after")
    def _unique_tables(self) -> "AuthorSpec":
        names = [t.table.lower() for t in self.tables]
        if len(names) != len(set(names)):
            raise ValueError("duplicate tables in spec")
        return self
