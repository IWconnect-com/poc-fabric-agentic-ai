import pytest

from migration_kit.errors import (
    KitError,
    PolicyViolation,
    SchemaDrift,
    SourceUnavailable,
    Throttled,
    ToolDenied,
)
from migration_kit.retry import retry


def no_jitter(d):
    return d


def test_error_dict_is_machine_readable():
    err = ToolDenied("nope", tool="x")
    assert err.to_dict() == {"code": "tool_denied", "message": "nope", "retryable": False, "context": {"tool": "x"}}
    assert isinstance(err, PolicyViolation)


def test_only_transient_errors_are_retryable():
    assert SourceUnavailable("x").retryable and Throttled("x").retryable
    assert not SchemaDrift("x").retryable and not ToolDenied("x").retryable


def test_retries_transient_then_succeeds():
    calls, sleeps = [], []

    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise SourceUnavailable("db down")
        return "ok"

    assert retry(flaky, attempts=3, base_delay=1, sleep=sleeps.append, jitter=no_jitter) == "ok"
    assert sleeps == [1, 2]  # exponential backoff


def test_gives_up_after_max_attempts_and_reports_attempts():
    def always():
        raise SourceUnavailable("down")

    with pytest.raises(SourceUnavailable) as info:
        retry(always, attempts=2, sleep=lambda s: None, jitter=no_jitter)
    assert info.value.context["attempts"] == 2


def test_non_retryable_fails_immediately():
    calls = []

    def bad():
        calls.append(1)
        raise SchemaDrift("column gone")

    with pytest.raises(SchemaDrift):
        retry(bad, attempts=5, sleep=lambda s: None)
    assert len(calls) == 1


def test_retry_after_hint_is_respected():
    sleeps, n = [], []

    def throttled():
        n.append(1)
        if len(n) == 1:
            raise Throttled("429", retry_after=10)
        return "ok"

    retry(throttled, attempts=2, base_delay=1, sleep=sleeps.append, jitter=no_jitter)
    assert sleeps == [10]


def test_untyped_exceptions_are_not_swallowed():
    with pytest.raises(ValueError):
        retry(lambda: (_ for _ in ()).throw(ValueError("boom")), attempts=3, sleep=lambda s: None)


def test_attempts_must_be_positive():
    with pytest.raises(ValueError):
        retry(lambda: 1, attempts=0)


def test_base_error_defaults():
    assert KitError("x").retryable is False
