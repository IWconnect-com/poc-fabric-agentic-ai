from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from migration_kit.errors import ReconciliationMismatch, SpecInvalid
from migration_kit.incremental import as_utc, chunk, diff_hashes, next_watermark, plan_window
from migration_kit.reconcile import (PartitionStats, assert_reconciled, compare_partitions, diff_key_sets,
                             partition_label, touched_partitions)
from migration_kit.registry import SourceEntry
from migration_kit.state import InMemoryStateStore, LoadState

UTC = timezone.utc
NOW = datetime(2026, 9, 29, 12, 0, tzinfo=UTC)


def wm_entry(**over):
    base = dict(source_id="o", source_table="orders", target_table="o", load_strategy="incremental_watermark",
                business_key=["id"], columns=["id", "modified_at", "order_date", "amount"],
                watermark_column="modified_at", lookback_hours=48, partition_column="order_date", measure_columns=["amount"])
    base.update(over)
    return SourceEntry(**base)


# ------------------------------------------------------------------ windows
def test_initial_window_has_no_lower_bound():
    w = plan_window(wm_entry(), LoadState(source_id="o"), NOW)
    assert (w.lower, w.upper, w.initial) == (None, NOW, True)


def test_next_window_overlaps_by_lookback():
    state = LoadState(source_id="o", last_watermark=NOW - timedelta(hours=1))
    w = plan_window(wm_entry(), state, NOW)
    assert w.lower == NOW - timedelta(hours=49) and not w.initial


def test_naive_run_start_and_wrong_strategy_rejected():
    with pytest.raises(SpecInvalid):
        plan_window(wm_entry(), LoadState(source_id="o"), datetime(2026, 1, 1))
    with pytest.raises(SpecInvalid):
        plan_window(wm_entry(load_strategy="full", watermark_column=None), LoadState(source_id="o"), NOW)


def test_naive_datetimes_are_treated_as_utc():
    assert as_utc(datetime(2026, 1, 1, 10)).tzinfo == UTC


# ------------------------------------------------------------------ watermark
def test_watermark_moves_forward_only():
    rows = [{"modified_at": NOW - timedelta(hours=5)}, {"modified_at": NOW - timedelta(hours=2)}, {"modified_at": None}]
    assert next_watermark(None, rows, "modified_at") == NOW - timedelta(hours=2)
    assert next_watermark(NOW, rows, "modified_at") == NOW           # never backwards
    assert next_watermark(NOW, [], "modified_at") == NOW             # empty batch keeps it
    assert next_watermark(None, [], "modified_at") is None


# ------------------------------------------------------------------ hash diff
def test_diff_hashes_and_chunk():
    src = {(1,): "a", (2,): "b2", (4,): "d"}
    tgt = {(2,): "b", (3,): "c", (4,): "d"}
    d = diff_hashes(src, tgt)
    assert (d.new, d.changed, d.deleted) == ([(1,)], [(2,)], [(3,)])
    assert chunk([1, 2, 3, 4, 5], 2) == [[1, 2], [3, 4], [5]]
    with pytest.raises(ValueError):
        chunk([1], 0)


# ------------------------------------------------------------------ state
def test_failure_never_touches_watermark():
    s = InMemoryStateStore()
    s.commit_success("o", "r1", NOW, NOW)
    s.record_failure("o", "r2", NOW)
    st = s.get("o")
    assert st.last_watermark == NOW and st.last_status == "failed" and st.last_success_run_id == "r1"


def test_commit_without_watermark_keeps_previous():
    s = InMemoryStateStore()
    s.commit_success("o", "r1", NOW, NOW)
    s.commit_success("o", "r2", None, NOW)
    assert s.get("o").last_watermark == NOW


# ------------------------------------------------------------------ reconciliation
def stats(n, amount="0"):
    return PartitionStats(row_count=n, sums={"amount": Decimal(amount)})


def test_matching_partitions_reconcile():
    assert compare_partitions({"2026-09-01": stats(3, "10.5")}, {"2026-09-01": stats(3, "10.5")}) == []


def test_count_and_sum_mismatches_are_reported():
    m = compare_partitions({"p1": stats(3, "10"), "p2": stats(1, "5")}, {"p1": stats(2, "10"), "p2": stats(1, "6")})
    assert {(x.partition, x.field) for x in m} == {("p1", "row_count"), ("p2", "amount")}


def test_tolerance_and_missing_partitions():
    assert compare_partitions({"p": stats(1, "10.001")}, {"p": stats(1, "10")}, Decimal("0.01")) == []
    m = compare_partitions({"p": stats(2)}, {})
    assert m[0].field == "row_count" and m[0].target_value == 0
    assert compare_partitions({}, {"p": stats(2)})[0].source_value == 0


def test_assert_reconciled_raises_typed_error_with_partitions():
    with pytest.raises(ReconciliationMismatch) as info:
        assert_reconciled({"p1": stats(3)}, {"p1": stats(2)})
    assert info.value.context["partitions"] == ["p1"] and not info.value.retryable


def test_key_set_diff_finds_missing_and_deleted():
    d = diff_key_sets([(1,), (2,), (3,)], [(2,), (3,), (9,)])
    assert (d.missing_in_target, d.deleted_in_source) == ([(1,)], [(9,)])


def test_partition_labels_match_the_source_query_format():
    assert partition_label(datetime(2026, 9, 1, 13, 5)) == "2026-09-01"
    assert partition_label(date(2026, 9, 1)) == "2026-09-01"
    assert partition_label(None) == "__null__"
    with pytest.raises(SpecInvalid):
        partition_label("not a date")


def test_touched_partitions():
    rows = [{"order_date": date(2026, 9, 2)}, {"order_date": date(2026, 9, 1)}, {"order_date": date(2026, 9, 2)}]
    assert touched_partitions(rows, wm_entry()) == ["2026-09-01", "2026-09-02"]
    assert touched_partitions(rows, wm_entry(partition_column=None)) == []
