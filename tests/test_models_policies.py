import pytest
from pydantic import ValidationError

from migration_kit.models import AuthorSpec, LoadStrategy, TableDecision, TableRole
from migration_kit.policies import ApprovalsSection, Policies, route_by_confidence

from conftest import CONFIG


def decision(**over):
    base = dict(
        table="dbo.orders", include=True, role="fact", business_key=["order_id"],
        load_strategy="incremental_watermark", watermark_column="modified_at",
        reason="orders header, has modified_at", confidence=0.9,
    )
    base.update(over)
    return base


def test_valid_decision_parses():
    d = TableDecision(**decision())
    assert d.role is TableRole.FACT and d.load_strategy is LoadStrategy.INCREMENTAL_WATERMARK
    assert d.lookback_hours == 24


@pytest.mark.parametrize(
    "over",
    [
        {"role": "banana"},                         # outside enum
        {"confidence": 1.5},                        # out of range
        {"watermark_column": None},                 # strategy needs it
        {"load_strategy": "full"},                  # watermark not allowed with full
        {"business_key": []},                       # fact needs a key
        {"include": False},                         # include must match role
        {"role": "skip"},                           # skip requires include false
        {"table": "dbo.orders; DROP TABLE x"},      # unsafe identifier
        {"reason": ""},                             # evidence required
        {"surprise": 1},                            # unknown field
    ],
)
def test_invalid_decisions_are_rejected(over):
    with pytest.raises(ValidationError):
        TableDecision(**decision(**over))


def test_skip_table_is_valid_without_key():
    d = TableDecision(**decision(role="skip", include=False, business_key=[],
                                 load_strategy="full", watermark_column=None))
    assert not d.include


def test_duplicate_tables_rejected():
    with pytest.raises(ValidationError):
        AuthorSpec(scope_ref="scope.md", tables=[decision(), decision(table="DBO.ORDERS")])


def test_policies_file_is_valid_and_strict():
    p = Policies.load(CONFIG / "policies.yaml")
    assert p.policies.agents_read_row_values is False
    assert "prod" not in p.policies.agents_write_environments
    assert "spec_merge" in p.approvals.always_human


def test_unknown_policy_key_is_rejected():
    with pytest.raises(ValidationError):
        Policies.model_validate({"version": 1, "typo": {}})


def test_route_by_confidence_thresholds():
    a = ApprovalsSection(auto_approve_min_confidence=0.85, human_review_min_confidence=0.65, always_human=[])
    assert route_by_confidence(0.85, a) == "auto_approve"
    assert route_by_confidence(0.84, a) == "human_review"
    assert route_by_confidence(0.65, a) == "human_review"
    assert route_by_confidence(0.64, a) == "reject"


def test_threshold_order_is_validated():
    with pytest.raises(ValidationError):
        ApprovalsSection(auto_approve_min_confidence=0.5, human_review_min_confidence=0.9, always_human=[])
