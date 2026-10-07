"""SQL generation. Identifiers are validated and quoted; values are always parameters.

  * Source queries are T-SQL with `?` placeholders (pyodbc style).
  * Lakehouse statements are Spark SQL with `:name` parameters.

The Spark statements are checked structurally by unit tests. They need an
integration run against a real Fabric dev lakehouse (Phase 3) before being trusted.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from .errors import SpecInvalid
from .incremental import Window
from .models import LoadStrategy
from .registry import SourceEntry, check_ident

MAX_PARAMS = 2000  # SQL Server allows 2100 parameters per statement


@dataclass(frozen=True)
class Query:
    sql: str
    params: tuple = ()


def tq(ident: str) -> str:
    return f"[{check_ident(ident)}]"


def sq(ident: str) -> str:
    return f"`{check_ident(ident)}`"


def _spark_table(name: str) -> str:
    return ".".join(sq(part) for part in name.split("."))


def _source_table(entry: SourceEntry) -> str:
    return f"{tq(entry.source_schema)}.{tq(entry.source_table)}"


def _cols(entry: SourceEntry) -> str:
    return ", ".join(tq(c) for c in entry.columns)


def row_hash_expr(entry: SourceEntry) -> str:
    """Hash over all columns; NULL and empty string hash differently."""
    parts = [f"ISNULL(CAST({tq(c)} AS NVARCHAR(4000)), N'<NULL>')" for c in entry.columns]
    joined = ", N'|', ".join(parts)
    return f"CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', CONCAT({joined})), 2)"


def extract_query(entry: SourceEntry, window: Window | None) -> Query:
    if entry.load_strategy == LoadStrategy.INCREMENTAL_HASH:
        raise SpecInvalid("use hash_query / keys_query for incremental_hash", source_id=entry.source_id)
    base = f"SELECT {_cols(entry)} FROM {_source_table(entry)}"
    if entry.load_strategy == LoadStrategy.FULL:
        return Query(base)
    if window is None:
        raise SpecInvalid("incremental_watermark needs a window", source_id=entry.source_id)
    wm = tq(entry.watermark_column)  # type: ignore[arg-type]
    if window.lower is None:
        return Query(f"{base} WHERE {wm} <= ? ORDER BY {wm}", (window.upper,))
    return Query(f"{base} WHERE {wm} > ? AND {wm} <= ? ORDER BY {wm}", (window.lower, window.upper))


def hash_query(entry: SourceEntry) -> Query:
    keys = ", ".join(tq(k) for k in entry.business_key)
    return Query(f"SELECT {keys}, {row_hash_expr(entry)} AS [_row_hash] FROM {_source_table(entry)}")


def max_keys_per_query(entry: SourceEntry) -> int:
    return max(1, MAX_PARAMS // len(entry.business_key))


def keys_query(entry: SourceEntry, keys: Sequence[tuple]) -> Query:
    """Rows for specific keys, with the source-computed hash, so silver stores the same hash."""
    if not keys:
        raise SpecInvalid("keys_query needs at least one key")
    if len(keys) > max_keys_per_query(entry):
        raise SpecInvalid("too many keys for one query; chunk first", limit=max_keys_per_query(entry))
    width = len(entry.business_key)
    clause = "(" + " AND ".join(f"{tq(k)} = ?" for k in entry.business_key) + ")"
    params: list = []
    for key in keys:
        if len(key) != width:
            raise SpecInvalid("key width does not match business_key")
        params.extend(key)
    where = " OR ".join([clause] * len(keys))
    sql = f"SELECT {_cols(entry)}, {row_hash_expr(entry)} AS [_row_hash] FROM {_source_table(entry)} WHERE {where}"
    return Query(sql, tuple(params))


def partition_expr(entry: SourceEntry) -> str:
    if entry.partition_column:
        return f"ISNULL(CONVERT(VARCHAR(10), {tq(entry.partition_column)}, 23), '__null__')"
    return "'all'"


def stats_query(entry: SourceEntry, partitions: Sequence[str] | None = None, as_of=None) -> Query:
    """Row counts and measure sums per partition, bounded by `as_of` to avoid racing live writes."""
    measures = "".join(f", SUM(CAST({tq(m)} AS DECIMAL(38, 6))) AS {tq(m)}" for m in entry.measure_columns)
    sql = f"SELECT {partition_expr(entry)} AS [_partition], COUNT_BIG(*) AS [_row_count]{measures} FROM {_source_table(entry)}"
    clauses: list[str] = []
    params: list = []
    if as_of is not None and entry.watermark_column:
        clauses.append(f"{tq(entry.watermark_column)} <= ?")
        params.append(as_of)
    if partitions and entry.partition_column:
        clauses.append(f"{partition_expr(entry)} IN ({', '.join('?' for _ in partitions)})")
        params.extend(partitions)
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    if entry.partition_column:
        sql += f" GROUP BY {partition_expr(entry)}"
    return Query(sql, tuple(params))


def silver_merge_sql(entry: SourceEntry, bronze_table: str, silver_table: str) -> str:
    """Idempotent upsert of one load into silver (Spark SQL, parameter :load_id).

    Dedupes within the batch, never overwrites newer data with older data
    (watermark strategy), and un-deletes rows that reappear.
    """
    keys = [sq(k) for k in entry.business_key]
    cols = [sq(c) for c in entry.columns]
    is_hash = entry.load_strategy == LoadStrategy.INCREMENTAL_HASH
    meta = ["`_load_id`", "`_ingested_at`"] + (["`_row_hash`"] if is_hash else [])
    if entry.watermark_column:
        order = f"{sq(entry.watermark_column)} DESC, `_ingested_at` DESC"
    else:
        order = "`_ingested_at` DESC"
    inner = ", ".join(cols + meta)
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    guard = f" AND s.{sq(entry.watermark_column)} >= t.{sq(entry.watermark_column)}" if entry.watermark_column else ""
    payload = cols + meta
    set_clause = ", ".join(f"t.{c} = s.{c}" for c in payload) + ", t.`_is_deleted` = false, t.`_deleted_at` = NULL"
    insert_cols = ", ".join(payload + ["`_is_deleted`", "`_deleted_at`"])
    insert_vals = ", ".join([f"s.{c}" for c in payload] + ["false", "NULL"])
    return (
        f"MERGE INTO {_spark_table(silver_table)} AS t\n"
        f"USING (\n"
        f"  SELECT {inner} FROM (\n"
        f"    SELECT {inner}, ROW_NUMBER() OVER (PARTITION BY {', '.join(keys)} ORDER BY {order}) AS `_rn`\n"
        f"    FROM {_spark_table(bronze_table)} WHERE `_load_id` = :load_id\n"
        f"  ) ranked WHERE `_rn` = 1\n"
        f") AS s\n"
        f"ON {on}\n"
        f"WHEN MATCHED{guard} THEN UPDATE SET {set_clause}\n"
        f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
    )


def mark_deleted_sql(entry: SourceEntry, silver_table: str, deleted_keys_table: str) -> str:
    """Soft-delete silver rows whose keys vanished from the source. Rows are never physically removed."""
    keys = [sq(k) for k in entry.business_key]
    on = " AND ".join(f"t.{k} = d.{k}" for k in keys)
    return (
        f"MERGE INTO {_spark_table(silver_table)} AS t\n"
        f"USING {_spark_table(deleted_keys_table)} AS d\n"
        f"ON {on}\n"
        f"WHEN MATCHED AND t.`_is_deleted` = false THEN UPDATE SET t.`_is_deleted` = true, t.`_deleted_at` = current_timestamp()"
    )
