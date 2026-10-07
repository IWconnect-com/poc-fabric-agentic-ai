"""Structured audit log: who, which agent, what, when, allowed or denied.

Denials are logged as well as successes. Parameter values that look sensitive
are redacted before anything is written.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, Callable, Literal

from pydantic import BaseModel, Field

from .telemetry import current_run_id

_SENSITIVE = re.compile(
    r"(secret|password|passwd|token|api[_-]?key|credential|connection[_-]?string|authorization)",
    re.IGNORECASE,
)
REDACTED = "***REDACTED***"


def redact(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: (REDACTED if _SENSITIVE.search(str(k)) else redact(v)) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [redact(v) for v in value]
    return value


class AuditEvent(BaseModel):
    ts: str
    run_id: str | None = None
    agent: str
    identity: str = "unknown"
    action: str
    target: str | None = None
    result: Literal["allowed", "denied", "ok", "error"]
    reason: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)


Sink = Callable[[AuditEvent], None]
_logger = logging.getLogger("migration_kit.audit")


def _logging_sink(event: AuditEvent) -> None:
    _logger.info(json.dumps(event.model_dump(), default=str))


class AuditLog:
    def __init__(self, sink: Sink | None = None, identity: str = "unknown") -> None:
        self._sink = sink or _logging_sink
        self.identity = identity

    def record(
        self,
        *,
        agent: str,
        action: str,
        result: Literal["allowed", "denied", "ok", "error"],
        target: str | None = None,
        reason: str | None = None,
        params: dict[str, Any] | None = None,
    ) -> AuditEvent:
        event = AuditEvent(
            ts=datetime.now(timezone.utc).isoformat(),
            run_id=current_run_id(),
            agent=agent,
            identity=self.identity,
            action=action,
            target=target,
            result=result,
            reason=reason,
            params=redact(params or {}),
        )
        self._sink(event)
        return event
