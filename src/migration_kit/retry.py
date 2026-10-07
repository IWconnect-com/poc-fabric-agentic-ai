"""Retry with exponential backoff, for typed retryable errors only.

Only KitError with `retryable=True` is retried. Anything else propagates
immediately, so a policy violation or schema drift is never retried blindly.
Callers wrap raw SDK exceptions into typed errors (SourceUnavailable,
Throttled, ...) at the boundary.
"""
from __future__ import annotations

import random
import time
from typing import Callable, TypeVar

from .errors import KitError

T = TypeVar("T")


def full_jitter(delay: float) -> float:
    return random.uniform(0, delay)


def retry(
    fn: Callable[[], T],
    *,
    attempts: int = 3,
    base_delay: float = 0.5,
    max_delay: float = 30.0,
    sleep: Callable[[float], None] = time.sleep,
    jitter: Callable[[float], float] = full_jitter,
    on_retry: Callable[[int, KitError, float], None] | None = None,
) -> T:
    if attempts < 1:
        raise ValueError("attempts must be >= 1")
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except KitError as exc:
            if not exc.retryable or attempt == attempts:
                exc.context["attempts"] = attempt
                raise
            delay = min(max_delay, base_delay * 2 ** (attempt - 1))
            delay = max(jitter(delay), float(exc.context.get("retry_after", 0)))
            if on_retry:
                on_retry(attempt, exc, delay)
            sleep(delay)
    raise AssertionError("unreachable")
