# Fabric Notebook: nb_silver_transform
# Reads bronze tables, applies type-casting / cleaning / dedup, writes to silver.
# Paste each ## CELL block into a separate Fabric notebook cell.
# Runs inside lh_data (set as the notebook's default lakehouse).

# ============================================================
# ## CELL 1 — Config and helpers
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType, BooleanType
from datetime import datetime, timezone

LAKEHOUSE = "lh_poc_fabric_power_bi_dev"
BRONZE = "bronze"
SILVER = "silver"
NOTEBOOK = "silver"

run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
run_start = datetime.now(timezone.utc)
results = []  # (table, status, rows_in, rows_out, message)

# Tables loaded incrementally (bronze append-only, one row per version, Decision 8).
# None yet. check_and_write refuses them: they need latest-per-key by watermark + MERGE,
# not the full-load overwrite and arbitrary dedupe below.
INCREMENTAL_TABLES = set()


def log(table, status, rows_in, rows_out, msg=""):
    results.append((table, status, rows_in, rows_out, msg))
    print(f"[{status}] {SILVER}.{table}: {rows_in} -> {rows_out}  {msg}")


def short_error(e, limit=500):
    """Exception type + first line only. Full Spark messages carry stack traces
    and can quote row values; the run log must hold neither."""
    lines = str(e).strip().splitlines()
    text = f"{type(e).__name__}: {lines[0]}" if lines else type(e).__name__
    return text[:limit]


def safe_to_date(col_name):
    """Cast varchar to date, NULLs on parse failure."""
    return F.to_date(F.col(col_name)).alias(col_name)


def dedupe(df, key_col):
    """Keep one row per key; which row is kept is arbitrary. Full loads only:
    incremental tables need the latest row per key (see INCREMENTAL_TABLES)."""
    return df.dropDuplicates([key_col])


def write_silver(df, table_name):
    """Overwrite the silver table as a managed Delta table."""
    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{LAKEHOUSE}.{SILVER}.{table_name}")


def blank_or_null(col_name):
    return F.col(col_name).isNull() | (F.trim(F.col(col_name).cast("string")) == "")


def new_nulls(src, df, skip, renamed):
    """Per column: how many values were present in bronze but are NULL in silver
    (failed cast or unrecognised flag). Counts only, never values."""
    cols = [c for c in df.columns if c not in skip and renamed.get(c, c) in src.columns]
    if not cols:
        return {}
    before = src.agg(*[F.count(F.when(blank_or_null(renamed.get(c, c)), 1)).alias(c) for c in cols]).first()
    after = df.agg(*[F.count(F.when(F.col(c).isNull(), 1)).alias(c) for c in cols]).first()
    return {c: after[c] - before[c] for c in cols if after[c] > before[c]}


def check_and_write(df, table_name, rows_in, key_col, src, renamed=None):
    """Check the cleaned `df` (before dedupe) against bronze `src`, dedupe, overwrite silver.
    Null or uncastable key -> FAIL, table NOT written (last good version stays).
    Duplicate keys or values turned NULL by a cast -> WARN, table written.
    `renamed` maps silver column -> bronze column where the name changed."""
    if table_name in INCREMENTAL_TABLES:
        raise NotImplementedError(f"{table_name} is incremental: needs latest-per-key + MERGE, not overwrite")
    renamed = renamed or {}

    null_keys = df.filter(F.col(key_col).isNull()).count()
    if null_keys:
        log(table_name, "FAIL", rows_in, 0, f"{null_keys} row(s) with null or uncastable key {key_col}")
        return

    # Full load: bronze is overwritten each run, so a duplicate key came from the source.
    warns = []
    dupes = rows_in - df.select(key_col).distinct().count()
    if dupes:
        warns.append(f"{dupes} duplicate key(s) {key_col} dropped")
    for col_name, n in new_nulls(src, df, {key_col}, renamed).items():
        warns.append(f"{n} value(s) became NULL in {col_name}")

    out = dedupe(df, key_col)
    write_silver(out, table_name)
    log(table_name, "WARN" if warns else "OK", rows_in, out.count(), "; ".join(warns))


def write_run_log(results_list):
    """Append run results to gold._run_log Delta table.
    Future: write to sqldb_control.meta.run_log via JDBC + managed identity."""
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType as IT, TimestampType

    ts = datetime.now(timezone.utc)
    schema = StructType([
        StructField("run_id", StringType(), False),
        StructField("notebook", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("status", StringType(), False),
        StructField("rows_in", IT(), False),
        StructField("rows_out", IT(), False),
        StructField("error_message", StringType(), True),
        StructField("run_ts", TimestampType(), False),
    ])
    rows = [
        (run_id, NOTEBOOK, r[0], r[1], r[2], r[3], r[4] if r[4] else None, ts)
        for r in results_list
    ]
    if rows:
        log_df = spark.createDataFrame(rows, schema)
        log_df.write.format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(f"{LAKEHOUSE}.gold._run_log")
        print(f"Run log: {len(rows)} entries appended to gold._run_log")


# ============================================================
# ## CELL 2 — silver.party
# ============================================================

try:
    src = spark.table(f"{LAKEHOUSE}.{BRONZE}.party")
    rows_in = src.count()

    df = src.select(
        F.col("Party_Identifier").cast("bigint"),
        F.trim(F.col("Party_Name")).alias("Party_Name"),
        F.col("Begin_Date").cast(DateType()),
        safe_to_date("End_Date"),                      # varchar -> date
        F.trim(F.col("Party_Type_Code")).alias("Party_Type_Code"),
    )
    check_and_write(df, "party", rows_in, "Party_Identifier", src)
except Exception as e:
    log("party", "FAIL", 0, 0, short_error(e))


# ============================================================
# ## CELL 3 — silver.person
# ============================================================

try:
    src = spark.table(f"{LAKEHOUSE}.{BRONZE}.person")
    rows_in = src.count()

    df = src.select(
        F.col("Person_Identifier").cast("bigint"),
        F.trim(F.col("First_Name")).alias("First_Name"),
        F.trim(F.col("Middle_Name")).alias("Middle_Name"),
        F.trim(F.col("Last_Name")).alias("Last_Name"),
        F.trim(F.col("Full_Legal_Name")).alias("Full_Legal_Name"),
        F.trim(F.col("Nickname")).alias("Nickname"),
        F.trim(F.col("Suffix_Name")).alias("Suffix_Name"),
        F.col("Birth_Date").cast(DateType()),
        F.trim(F.col("Birth_Place_Name")).alias("Birth_Place_Name"),
        F.trim(F.col("Gender_Code")).alias("Gender_Code"),
        F.trim(F.col("Prefix_Name")).alias("Prefix_Name"),
    )
    check_and_write(df, "person", rows_in, "Person_Identifier", src)
except Exception as e:
    log("person", "FAIL", 0, 0, short_error(e))


# ============================================================
# ## CELL 4 — silver.vehicle
# ============================================================

try:
    src = spark.table(f"{LAKEHOUSE}.{BRONZE}.vehicle")
    rows_in = src.count()

    df = src.select(
        F.col("Insurable_Object_Identifier").cast("bigint"),
        F.col("Vehicle_Model_Year").cast(IntegerType()),
        F.trim(F.col("Vehicle_Model_Name")).alias("Vehicle_Model_Name"),
        F.col("Vehicle_Driving_Wheel_Quantity").cast(IntegerType()),
        F.trim(F.col("Vehicle_Make_Name")).alias("Vehicle_Make_Name"),
        F.upper(F.trim(F.col("Vehicle_Identification_Number"))).alias("Vehicle_Identification_Number"),
    )
    check_and_write(df, "vehicle", rows_in, "Insurable_Object_Identifier", src)
except Exception as e:
    log("vehicle", "FAIL", 0, 0, short_error(e))


# ============================================================
# ## CELL 5 — silver.policy
# ============================================================

try:
    src = spark.table(f"{LAKEHOUSE}.{BRONZE}.policy")
    rows_in = src.count()

    df = src.select(
        F.col("Policy_Identifier").cast("bigint"),
        F.col("Effective_Date").cast(DateType()),
        F.col("Expiration_Date").cast(DateType()),
        F.trim(F.col("Policy_Number")).alias("Policy_Number"),
        F.trim(F.col("Status_Code")).alias("Status_Code"),
        F.col("Geographic_Location_Identifier").cast("bigint"),
    )
    check_and_write(df, "policy", rows_in, "Policy_Identifier", src)
except Exception as e:
    log("policy", "FAIL", 0, 0, short_error(e))


# ============================================================
# ## CELL 6 — silver.coverage
# ============================================================

try:
    src = spark.table(f"{LAKEHOUSE}.{BRONZE}.coverage")
    rows_in = src.count()

    df = src.select(
        F.col("Coverage_Identifier").cast("bigint"),
        F.trim(F.col("Coverage_Description")).alias("Coverage_Description"),
        F.trim(F.col("Coverage_Name")).alias("Coverage_Name"),
        F.col("Coverage_Type_Identifier").cast("bigint"),
        F.col("Coverage_Group_Identifier").cast("bigint"),
        F.trim(F.col("Coverage_Part_Code")).alias("Coverage_Part_Code"),
    )
    check_and_write(df, "coverage", rows_in, "Coverage_Identifier", src)
except Exception as e:
    log("coverage", "FAIL", 0, 0, short_error(e))


# ============================================================
# ## CELL 7 — silver.occurrence
# ============================================================

try:
    src = spark.table(f"{LAKEHOUSE}.{BRONZE}.occurrence")
    rows_in = src.count()

    df = src.select(
        F.col("Occurrence_Identifier").cast("bigint"),
        F.when(F.upper(F.trim(F.col("Catastrophic_Event_Indicator"))).isin("Y", "YES", "TRUE", "1"), True)
         .when(F.upper(F.trim(F.col("Catastrophic_Event_Indicator"))).isin("N", "NO", "FALSE", "0"), False)
         .otherwise(None)
         .cast(BooleanType())
         .alias("Is_Catastrophic_Event"),
        F.col("Geographic_Location_Identifier").cast("bigint"),
        F.col("Occurrence_Begin_Date").cast(DateType()),
        F.col("Occurrence_End_Date").cast(DateType()),
        F.trim(F.col("Occurrence_Begin_Time")).alias("Occurrence_Begin_Time"),
        F.trim(F.col("Occurrence_End_Time")).alias("Occurrence_End_Time"),
        F.trim(F.col("Occurrence_Name")).alias("Occurrence_Name"),
    )
    check_and_write(df, "occurrence", rows_in, "Occurrence_Identifier", src,
                    renamed={"Is_Catastrophic_Event": "Catastrophic_Event_Indicator"})
except Exception as e:
    log("occurrence", "FAIL", 0, 0, short_error(e))


# ============================================================
# ## CELL 8 — silver.claim
# ============================================================

try:
    src = spark.table(f"{LAKEHOUSE}.{BRONZE}.claim")
    rows_in = src.count()

    df = src.select(
        F.col("Claim_Identifier").cast("bigint"),
        F.trim(F.col("Catastrophe_Identifier")).alias("Catastrophe_Identifier"),
        F.trim(F.col("Claim_Description")).alias("Claim_Description"),
        F.col("Claims_Made_Date").cast(DateType()),
        F.trim(F.col("Company_Claim_Number")).alias("Company_Claim_Number"),
        F.trim(F.col("Company_Subclaim_Number")).alias("Company_Subclaim_Number"),
        F.col("Insurable_Object_Identifier").cast("bigint"),
        F.col("Occurrence_Identifier").cast("bigint"),
        safe_to_date("Entry_Into_Claims_Made_Program_Date"),   # varchar -> date
        F.col("Claim_Open_Date").cast(DateType()),
        F.col("Claim_Close_Date").cast(DateType()),
        F.col("Claim_Reopen_Date").cast(DateType()),
        F.trim(F.col("Claim_Status_Code")).alias("Claim_Status_Code"),
        F.col("Claim_Reported_Date").cast(DateType()),
    )
    check_and_write(df, "claim", rows_in, "Claim_Identifier", src)
except Exception as e:
    log("claim", "FAIL", 0, 0, short_error(e))


# ============================================================
# ## CELL 9 — silver.peril_code_map
# ============================================================

try:
    src = spark.table(f"{LAKEHOUSE}.{BRONZE}.peril_code_map")
    rows_in = src.count()

    df = src.select(
        F.trim(F.col("peril_code")).alias("peril_code"),
        F.trim(F.col("peril_description")).alias("peril_description"),
        F.trim(F.col("peril_category")).alias("peril_category"),
        F.trim(F.col("line_applicability")).alias("line_applicability"),
        F.when(F.upper(F.trim(F.col("catastrophe_eligible"))).isin("Y", "YES", "TRUE", "1"), True)
         .when(F.upper(F.trim(F.col("catastrophe_eligible"))).isin("N", "NO", "FALSE", "0"), False)
         .otherwise(None)
         .cast(BooleanType())
         .alias("catastrophe_eligible"),
        F.when(F.upper(F.trim(F.col("standard_covered_flag"))).isin("Y", "YES", "TRUE", "1"), True)
         .when(F.upper(F.trim(F.col("standard_covered_flag"))).isin("N", "NO", "FALSE", "0"), False)
         .otherwise(None)
         .cast(BooleanType())
         .alias("standard_covered_flag"),
    )
    check_and_write(df, "peril_code_map", rows_in, "peril_code", src)
except Exception as e:
    log("peril_code_map", "FAIL", 0, 0, short_error(e))


# ============================================================
# ## CELL 10 — Summary + run log
# ============================================================

from pyspark.sql import Row

summary = spark.createDataFrame(
    [Row(run_id=run_id, table=r[0], status=r[1],
         rows_in=r[2], rows_out=r[3], message=r[4])
     for r in results]
)
summary.show(truncate=False)

# Persist run log to Delta (append)
try:
    write_run_log(results)
except Exception as e:
    print(f"WARNING: Could not write run log: {e}")

failed = [r for r in results if r[1] == "FAIL"]
if failed:
    tables = ", ".join(r[0] for r in failed)
    raise Exception(f"Silver transform failed for: {tables}")

elapsed = (datetime.now(timezone.utc) - run_start).total_seconds()
print(f"Silver transform complete. {len(results)} tables, {len(failed)} failures, {elapsed:.1f}s elapsed.")
