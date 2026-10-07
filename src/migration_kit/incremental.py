"""Pure incremental-load logic: windows, watermark advance, hash diffs. No I/O.

Late updates are handled by an overlapping window: each run re-reads
`lookback_hours` before the last watermark, and the silver merge is idempotent,
so re-reading a row is harmless.

Assumption (recorded in docs/DECISIONS.md): watermark column values are UTC.
Naive datetimes are treated as UTC. If the ERP stores local time, fix that at
the source query, not here.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, Mapping, Sequence, TypeVar

from .errors import SpecInvalid
from .models import LoadStrategy
from .registry import SourceEntry
from .state import LoadState

T = TypeVar("T")


def as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


@dataclass(frozen=True)
class Window:
    lower: datetime | None   # exclusive; None means "from the beginning"
    upper: datetime          # inclusive; rows newer than the run start are left for the next run
    initial: bool


def plan_window(entry: SourceEntry, state: LoadState, run_started_at: datetime) -> Window:
    if entry.load_strategy != LoadStrategy.INCREMENTAL_WATERMARK:
        raise SpecInvalid("plan_window only applies to incremental_watermark", source_id=entry.source_id)
    if run_started_at.tzinfo is None:
        raise SpecInvalid("run_started_at must be timezone-aware")
    upper = as_utc(run_started_at)
    if state.last_watermark is None:
        return Window(lower=None, upper=upper, initial=True)
    lower = as_utc(state.last_watermark) - timedelta(hours=entry.lookback_hours)
    return Window(lower=lower, upper=upper, initial=False)


def next_watermark(previous: datetime | None, rows: Iterable[Mapping], column: str) -> datetime | None:
    """Highest watermark seen. Never moves backwards, never jumps ahead of observed data."""
    seen = [as_utc(r[column]) for r in rows if r.get(column) is not None]
    if not seen:
        return previous
    highest = max(seen)
    return highest if previous is None else max(as_utc(previous), highest)


@dataclass(frozen=True)
class KeyDiff:
    new: list[tuple]
    changed: list[tuple]
    deleted: list[tuple]


def diff_hashes(source: Mapping[tuple, str], target: Mapping[tuple, str]) -> KeyDiff:
    """Compare source-computed row hashes with the hashes stored on active silver rows."""
    new = sorted(k for k in source if k not in target)
    changed = sorted(k for k in source if k in target and source[k] != target[k])
    deleted = sorted(k for k in target if k not in source)
    return KeyDiff(new=new, changed=changed, deleted=deleted)


def chunk(items: Sequence[T], size: int) -> list[list[T]]:
    if size < 1:
        raise ValueError("size must be >= 1")
    return [list(items[i : i + size]) for i in range(0, len(items), size)]
