"""Run context and tracing.

`run_context` gives every operation a run_id (used by audit logs and traces).
`span` uses OpenTelemetry when it is installed and configured, and is a no-op
otherwise, so the same code runs in unit tests, dev and prod.
"""
from __future__ import annotations

import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Iterator

_run_id: ContextVar[str | None] = ContextVar("kit_run_id", default=None)


@contextmanager
def run_context(run_id: str | None = None) -> Iterator[str]:
    rid = run_id or str(uuid.uuid4())
    token = _run_id.set(rid)
    try:
        yield rid
    finally:
        _run_id.reset(token)


def current_run_id() -> str | None:
    return _run_id.get()


class _NoopSpan:
    def set_attribute(self, key: str, value: Any) -> None:
        pass


def _tracer():
    try:
        from opentelemetry import trace  # optional dependency
    except ImportError:
        return None
    return trace.get_tracer("migration_kit")


def _clean(value: Any) -> Any:
    return value if isinstance(value, (str, bool, int, float)) else str(value)


@contextmanager
def span(name: str, **attributes: Any) -> Iterator[Any]:
    tracer = _tracer()
    if tracer is None:
        yield _NoopSpan()
        return
    with tracer.start_as_current_span(name) as s:
        rid = current_run_id()
        if rid:
            s.set_attribute("migration_kit.run_id", rid)
        for key, value in attributes.items():
            s.set_attribute(key, _clean(value))
        yield s


def record_token_usage(active_span: Any, input_tokens: int, output_tokens: int) -> None:
    active_span.set_attribute("gen_ai.usage.input_tokens", int(input_tokens))
    active_span.set_attribute("gen_ai.usage.output_tokens", int(output_tokens))
