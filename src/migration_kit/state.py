"""Load state (watermarks). Production store is a table in Fabric (Phase 3); this is the contract."""
from __future__ import annotations

from datetime import datetime
from typing import Literal, Protocol

from pydantic import BaseModel


class LoadState(BaseModel):
    source_id: str
    last_watermark: datetime | None = None
    last_success_run_id: str | None = None
    last_success_at: datetime | None = None
    last_status: Literal["never_run", "success", "failed"] = "never_run"
    last_failed_run_id: str | None = None


class StateStore(Protocol):
    def get(self, source_id: str) -> LoadState: ...

    def commit_success(self, source_id: str, run_id: str, watermark: datetime | None, at: datetime) -> None: ...

    def record_failure(self, source_id: str, run_id: str, at: datetime) -> None: ...


class InMemoryStateStore:
    def __init__(self) -> None:
        self._states: dict[str, LoadState] = {}

    def get(self, source_id: str) -> LoadState:
        return self._states.get(source_id, LoadState(source_id=source_id)).model_copy()

    def commit_success(self, source_id: str, run_id: str, watermark: datetime | None, at: datetime) -> None:
        prev = self.get(source_id)
        self._states[source_id] = prev.model_copy(
            update={
                "last_watermark": watermark if watermark is not None else prev.last_watermark,
                "last_success_run_id": run_id,
                "last_success_at": at,
                "last_status": "success",
            }
        )

    def record_failure(self, source_id: str, run_id: str, at: datetime) -> None:
        prev = self.get(source_id)
        # a failure never touches the watermark
        self._states[source_id] = prev.model_copy(update={"last_status": "failed", "last_failed_run_id": run_id})
