import pytest
from pydantic import ValidationError

from migration_kit.errors import SpecInvalid
from migration_kit.models import TableDecision
from migration_kit.registry import Registry, SourceEntry, check_ident, entry_from_decision

from conftest import CONFIG


def entry(**over):
    base = dict(
        source_id="dbo_orders", source_table="orders", target_table="dbo_orders",
        load_strategy="incremental_watermark", business_key=["order_id"],
        columns=["order_id", "amount", "modified_at"], watermark_column="modified_at",
    )
    base.update(over)
    return base


def test_valid_entry():
    e = SourceEntry(**entry())
    assert e.source_schema == "dbo" and e.lookback_hours == 24


@pytest.mark.parametrize(
    "over",
    [
        {"source_table": "orders; DROP TABLE x"},
        {"columns": ["order_id", "amount; --", "modified_at"]},
        {"watermark_column": None},                              # strategy needs it
        {"load_strategy": "full"},                               # watermark not allowed with full
        {"columns": ["order_id", "amount"]},                     # missing watermark column
        {"business_key": ["missing_col"]},
        {"business_key": []},
        {"columns": []},
        {"lookback_hours": -1},
        {"surprise": 1},
    ],
)
def test_invalid_entries_rejected(over):
    with pytest.raises(ValidationError):
        SourceEntry(**entry(**over))


def test_check_ident():
    assert check_ident("ok_1") == "ok_1"
    for bad in ["1abc", "a b", "a-b", "a;b", "", "a.b", "[a]"]:
        with pytest.raises(ValueError):
            check_ident(bad)


def test_duplicates_rejected():
    a = SourceEntry(**entry())
    with pytest.raises(SpecInvalid):
        Registry([a, SourceEntry(**entry(target_table="other"))])
    with pytest.raises(SpecInvalid):
        Registry([a, SourceEntry(**entry(source_id="other"))])


def test_active_sorted_by_load_order_and_skips_inactive():
    r = Registry([
        SourceEntry(**entry(source_id="b", target_table="b", load_order=20)),
        SourceEntry(**entry(source_id="a", target_table="a", load_order=10)),
        SourceEntry(**entry(source_id="c", target_table="c", load_order=1, is_active=False)),
    ])
    assert [e.source_id for e in r.active()] == ["a", "b"]
    with pytest.raises(SpecInvalid):
        r.get("nope")


def test_example_registry_file_loads():
    r = Registry.from_yaml(CONFIG / "sources")
    assert [e.source_id for e in r.active()] == ["dbo_countries", "dbo_customers", "dbo_orders"]


def test_entry_from_approved_decision():
    d = TableDecision(table="sales.orders", include=True, role="fact", business_key=["order_id"],
                      load_strategy="incremental_watermark", watermark_column="modified_at",
                      reason="orders", confidence=0.9)
    e = entry_from_decision(d, columns=["order_id", "modified_at"])
    assert (e.source_schema, e.source_table, e.source_id) == ("sales", "orders", "sales_orders")


def test_skipped_decision_cannot_become_entry():
    d = TableDecision(table="dbo.tmp", include=False, role="skip", reason="temp", confidence=0.9)
    with pytest.raises(SpecInvalid):
        entry_from_decision(d, columns=["x"])
