import pytest

from migration_kit.errors import ApprovalRequired, ParamViolation, ToolDenied
from migration_kit.guard import ToolGuard

from conftest import CONFIG

POLICY = {
    "agents": {
        "author_agent": {
            "read_stats": {
                "params": {
                    "table": {"pattern": r"[A-Za-z0-9_]+", "required": True},
                    "limit": {"max": 100, "min": 1},
                    "mode": {"allowed_values": ["fast", "slow"]},
                    "note": {"forbidden_patterns": [r"drop\s+table"]},
                }
            },
            "propose_spec": {},
            "deploy": {"requires_approval": True},
        }
    }
}


@pytest.fixture
def guard(audit):
    return ToolGuard(POLICY, audit)


def test_allowed_call_passes_and_is_audited(guard, events):
    guard.check("author_agent", "read_stats", {"table": "orders", "limit": 10})
    assert [(e.result, e.action) for e in events] == [("allowed", "read_stats")]


def test_unknown_agent_and_tool_are_denied_and_audited(guard, events):
    with pytest.raises(ToolDenied):
        guard.check("rogue", "read_stats")
    with pytest.raises(ToolDenied):
        guard.check("author_agent", "delete_table")
    assert [e.result for e in events] == ["denied", "denied"]


def test_tool_without_params_rejects_any_param(guard):
    guard.check("author_agent", "propose_spec")
    with pytest.raises(ParamViolation):
        guard.check("author_agent", "propose_spec", {"anything": 1})


@pytest.mark.parametrize(
    "params",
    [
        {},  # required table missing
        {"table": "orders; DROP"},  # pattern
        {"table": "orders", "limit": 1000},  # max
        {"table": "orders", "limit": 0},  # min
        {"table": "orders", "limit": True},  # bool is not a number
        {"table": "orders", "mode": "turbo"},  # allowed_values
        {"table": "orders", "note": "please DROP   TABLE x"},  # forbidden pattern
        {"table": "orders", "extra": 1},  # unexpected param
    ],
)
def test_param_rules_are_enforced(guard, params):
    with pytest.raises(ParamViolation):
        guard.check("author_agent", "read_stats", params)


def test_approval_gate(guard):
    with pytest.raises(ApprovalRequired):
        guard.check("author_agent", "deploy")
    guard.check("author_agent", "deploy", approved=True)


def test_wrap_blocks_before_the_tool_runs(guard):
    ran = []
    safe = guard.wrap("author_agent", "read_stats", lambda **kw: ran.append(kw) or "rows")
    assert safe(table="orders") == "rows"
    with pytest.raises(ParamViolation):
        safe(table="x y")
    assert len(ran) == 1


def test_denied_params_are_redacted_in_audit(guard, events):
    with pytest.raises(ParamViolation):
        guard.check("author_agent", "read_stats", {"table": "orders", "api_key": "secret-value"})
    assert "secret-value" not in str(events[-1].params)


def test_real_config_loads_and_denies_by_default(audit):
    g = ToolGuard.from_yaml(CONFIG / "allowed_tools.yaml", audit)
    g.check("author_agent", "read_metadata", {"schema": "dbo"})
    g.check("author_agent", "read_stats", {"table": "dbo.orders", "stat": "row_count"})
    with pytest.raises(ToolDenied):
        g.check("author_agent", "write_bronze")
    with pytest.raises(ParamViolation):
        g.check("author_agent", "read_metadata", {"schema": "sys"})
    with pytest.raises(ToolDenied):
        g.check("operate_agent", "read_metadata", {"schema": "dbo"})
