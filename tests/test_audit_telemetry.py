from migration_kit.audit import REDACTED, AuditLog, redact
from migration_kit.telemetry import current_run_id, record_token_usage, run_context, span


def test_redaction_is_recursive_and_key_based():
    data = {"table": "orders", "api_key": "abc", "nested": {"password": "p", "rows": 3}, "list": [{"token": "t"}]}
    out = redact(data)
    assert out["table"] == "orders"
    assert out["api_key"] == REDACTED
    assert out["nested"] == {"password": REDACTED, "rows": 3}
    assert out["list"] == [{"token": REDACTED}]


def test_business_key_is_not_over_redacted():
    assert redact({"business_key": ["ORDER_ID"]}) == {"business_key": ["ORDER_ID"]}


def test_audit_event_has_required_fields(audit, events):
    with run_context("run-1"):
        audit.record(agent="author_agent", action="read_stats", result="allowed", target="dbo.orders", params={"password": "x"})
    e = events[0]
    assert (e.run_id, e.agent, e.identity, e.action, e.result) == ("run-1", "author_agent", "test-identity", "read_stats", "allowed")
    assert e.params == {"password": REDACTED}
    assert e.ts.endswith("+00:00")


def test_run_context_sets_and_restores():
    assert current_run_id() is None
    with run_context() as rid:
        assert current_run_id() == rid
        with run_context("inner"):
            assert current_run_id() == "inner"
        assert current_run_id() == rid
    assert current_run_id() is None


def test_span_is_safe_without_opentelemetry():
    with span("step", table="orders", rows=3) as s:
        record_token_usage(s, 10, 5)  # must not raise
