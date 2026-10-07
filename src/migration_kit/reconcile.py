"""Reconciliation: source vs silver, deterministic. This is the real 'judge' of an ingestion.

Counts and measure sums are compared per partition. Cross-engine row checksums
are deliberately not used: T-SQL and Spark hash differently, so they would
produce false mismatches.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Iterable, Mapping

from pydantic import BaseModel, Field

from .errors import ReconciliationMismatch, SpecInvalid
from .registry import SourceEntry


class PartitionStats(BaseModel):
    row_count: int
    sums: dict[str, Decimal] = Field(default_factory=dict)


@dataclass(frozen=True)
class Mismatch:
    partition: str
    field: str
    source_value: object
    target_value: object


def compare_partitions(
    source: Mapping[str, PartitionStats],
    target: Mapping[str, PartitionStats],
    tolerance: Decimal = Decimal("0"),
) -> list[Mismatch]:
    out: list[Mismatch] = []
    for part in sorted(set(source) | set(target)):
        s = source.get(part, PartitionStats(row_count=0))
        t = target.get(part, PartitionStats(row_count=0))
        if s.row_count != t.row_count:
            out.append(Mismatch(part, "row_count", s.row_count, t.row_count))
        for name in sorted(set(s.sums) | set(t.sums)):
            sv, tv = s.sums.get(name, Decimal(0)), t.sums.get(name, Decimal(0))
            if abs(sv - tv) > tolerance:
                out.append(Mismatch(part, name, sv, tv))
    return out


def assert_reconciled(
    source: Mapping[str, PartitionStats],
    target: Mapping[str, PartitionStats],
    tolerance: Decimal = Decimal("0"),
) -> None:
    mismatches = compare_partitions(source, target, tolerance)
    if mismatches:
        raise ReconciliationMismatch(
            f"{len(mismatches)} mismatch(es) across {len({m.partition for m in mismatches})} partition(s)",
            partitions=sorted({m.partition for m in mismatches}),
            fields=sorted({m.field for m in mismatches}),
        )


@dataclass(frozen=True)
class KeySetDiff:
    missing_in_target: list[tuple]
    deleted_in_source: list[tuple]


def diff_key_sets(source_keys: Iterable[tuple], target_active_keys: Iterable[tuple]) -> KeySetDiff:
    s, t = set(source_keys), set(target_active_keys)
    return KeySetDiff(missing_in_target=sorted(s - t), deleted_in_source=sorted(t - s))


def partition_label(value: object) -> str:
    """Matches the source query's CONVERT(VARCHAR(10), col, 23)."""
    if value is None:
        return "__null__"
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    raise SpecInvalid("partition_column must be a date or datetime", got=type(value).__name__)


def touched_partitions(rows: Iterable[Mapping], entry: SourceEntry) -> list[str]:
    if not entry.partition_column:
        return []
    return sorted({partition_label(r.get(entry.partition_column)) for r in rows})
