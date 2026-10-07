from datetime import datetime, timezone

import pytest

from migration_kit.errors import SpecInvalid
from migration_kit.incremental import Window
from migration_kit.registry import SourceEntry
from migration_kit.sqlgen import (MAX_PARAMS, extract_query, hash_query, keys_query, mark_deleted_sql,
                          max_keys_per_query, silver_merge_sql, sq, stats_query, tq)

UTC = timezone.utc
T0, T1 = datetime(2026, 9, 1, tzinfo=UTC), datetime(2026, 9, 2, tzinfo=UTC)


def make(strategy="incremental_watermark", **over):
    base = dict(source_id="o", source_table="orders", target_table="o", load_strategy=strategy,
                business_key=["order_id"], columns=["order_id", "amount", "modified_at", "order_date"],
                measure_columns=["amount"], partition_column="order_date")
    if strategy == "incremental_watermark":
        base["watermark_column"] = "modified_at"
    base.update(over)
    return SourceEntry(**base)


def test_quoting_rejects_unsafe_identifiers():
    assert tq("orders") == "[orders]" and sq("orders") == "`orders`"
    for bad in ["a]; DROP TABLE x;--", "a b", "a`b"]:
        with pytest.raises(ValueError):
            tq(bad)
        with pytest.raises(ValueError):
            sq(bad)


def test_initial_and_incremental_extract():
    e = make()
    q = extract_query(e, Window(None, T1, True))
    assert q.sql == "SELECT [order_id], [amount], [modified_at], [order_date] FROM [dbo].[orders] WHERE [modified_at] <= ? ORDER BY [modified_at]"
    assert q.params == (T1,)
    q = extract_query(e, Window(T0, T1, False))
    assert "[modified_at] > ? AND [modified_at] <= ?" in q.sql and q.params == (T0, T1)


def test_values_are_never_inlined():
    q = extract_query(make(), Window(T0, T1, False))
    assert "2026" not in q.sql


def test_full_extract_has_no_where_and_hash_strategy_is_refused():
    assert "WHERE" not in extract_query(make("full"), None).sql
    with pytest.raises(SpecInvalid):
        extract_query(make("incremental_hash"), None)
    with pytest.raises(SpecInvalid):
        extract_query(make(), None)


def test_hash_query_distinguishes_null_from_empty_and_covers_all_columns():
    sql = hash_query(make("incremental_hash")).sql
    assert "SHA2_256" in sql and sql.count("ISNULL(CAST(") == 4 and "N'<NULL>'" in sql and "N'|'" in sql


def test_keys_query_flattens_composite_keys_and_enforces_limits():
    e = make("full", business_key=["order_id", "amount"])
    q = keys_query(e, [(1, 2), (3, 4)])
    assert q.params == (1, 2, 3, 4)
    assert q.sql.count("[order_id] = ?") == 2 and " OR " in q.sql and "[_row_hash]" in q.sql
    with pytest.raises(SpecInvalid):
        keys_query(e, [])
    with pytest.raises(SpecInvalid):
        keys_query(e, [(1,)])
    assert max_keys_per_query(e) == MAX_PARAMS // 2
    with pytest.raises(SpecInvalid):
        keys_query(e, [(i, i) for i in range(max_keys_per_query(e) + 1)])


def test_stats_query_variants():
    e = make()
    q = stats_query(e, ["2026-09-01"], as_of=T1)
    assert "COUNT_BIG(*)" in q.sql and "SUM(CAST([amount] AS DECIMAL(38, 6)))" in q.sql
    assert "[modified_at] <= ?" in q.sql and "GROUP BY" in q.sql and q.params == (T1, "2026-09-01")
    plain = stats_query(make("full", partition_column=None, measure_columns=[]))
    assert "GROUP BY" not in plain.sql and "'all'" in plain.sql and plain.params == ()


def test_merge_watermark_strategy():
    sql = silver_merge_sql(make(), "bronze.dbo_orders", "silver.dbo_orders")
    assert "MERGE INTO `silver`.`dbo_orders`" in sql and "`bronze`.`dbo_orders`" in sql
    assert ":load_id" in sql and "PARTITION BY `order_id`" in sql
    assert "ORDER BY `modified_at` DESC, `_ingested_at` DESC" in sql
    assert "WHEN MATCHED AND s.`modified_at` >= t.`modified_at`" in sql      # never overwrite newer with older
    assert "t.`_is_deleted` = false" in sql                                    # reappearing rows are undeleted
    assert "*" not in sql                                                      # explicit columns only


def test_merge_hash_and_full_strategies():
    h = silver_merge_sql(make("incremental_hash"), "b.t", "s.t")
    assert "`_row_hash`" in h and "WHEN MATCHED THEN UPDATE" in h and "modified_at" in h  # modified_at is just a column
    f = silver_merge_sql(make("full"), "b.t", "s.t")
    assert "WHEN MATCHED THEN UPDATE" in f and "`_row_hash`" not in f and "ORDER BY `_ingested_at` DESC" in f


def test_table_names_are_validated():
    with pytest.raises(ValueError):
        silver_merge_sql(make(), "bronze.x; DROP", "silver.t")


def test_mark_deleted_is_soft_and_only_touches_active_rows():
    sql = mark_deleted_sql(make(), "silver.dbo_orders", "tmp.deleted_keys")
    import re

    assert "`_is_deleted` = true" in sql and "t.`_is_deleted` = false" in sql
    assert re.search(r"\bDELETE\b", sql, re.IGNORECASE) is None      # no DELETE statement, ever
