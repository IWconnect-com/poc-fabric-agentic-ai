# Fabric Notebook: nb_gold_transform
# Reads silver tables, builds star-schema gold layer (dim + fact).
# Paste each ## CELL block into a separate Fabric notebook cell.
# Runs inside lh_data (set as the notebook's default lakehouse).

# ============================================================
# ## CELL 1 — Config and helpers
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType, BooleanType
from datetime import datetime, timezone

LAKEHOUSE = "lh_poc_fabric_power_bi_dev"
SILVER = "silver"
GOLD = "gold"
NOTEBOOK = "gold"

run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
run_start = datetime.now(timezone.utc)
results = []


def log(table, status, rows_in, rows_out, msg=""):
    results.append((table, status, rows_in, rows_out, msg))
    print(f"[{status}] {GOLD}.{table}: {rows_in} -> {rows_out}  {msg}")


def write_gold(df, table_name):
    """Overwrite the gold table as a managed Delta table."""
    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{LAKEHOUSE}.{GOLD}.{table_name}")


def key_problems(df, key_cols):
    """Return a description of null or duplicate keys, or "" if the key is clean."""
    any_null = F.lit(False)
    for c in key_cols:
        any_null = any_null | F.col(c).isNull()
    nulls = df.filter(any_null).count()
    dupes = df.count() - df.select(*key_cols).distinct().count()
    problems = []
    if nulls:
        problems.append(f"{nulls} row(s) with null key {key_cols}")
    if dupes:
        problems.append(f"{dupes} duplicate key(s) {key_cols}")
    return "; ".join(problems)


def check_and_write(df, table_name, rows_in, key_cols):
    """Check row count and key, then overwrite the gold table.
    On a problem the table is logged FAIL and NOT written, so the last good version stays."""
    rows_out = df.count()
    problems = []
    if rows_out != rows_in:
        problems.append(f"row count changed {rows_in} -> {rows_out}")
    keys = key_problems(df, key_cols)
    if keys:
        problems.append(keys)
    if problems:
        log(table_name, "FAIL", rows_in, rows_out, "; ".join(problems))
        return
    write_gold(df, table_name)
    log(table_name, "OK", rows_in, rows_out)


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
# ## CELL 2 — dim_party
# ============================================================

try:
    src = spark.table(f"{LAKEHOUSE}.{SILVER}.party")
    rows_in = src.count()

    df = src.select(
        F.col("Party_Identifier").alias("party_id"),
        F.col("Party_Name").alias("party_name"),
        F.col("Begin_Date").alias("effective_from"),
        F.col("End_Date").alias("effective_to"),
        F.col("Party_Type_Code").alias("party_type"),
    )

    check_and_write(df, "dim_party", rows_in, ["party_id"])
except Exception as e:
    log("dim_party", "FAIL", 0, 0, str(e))


# ============================================================
# ## CELL 3 — dim_person
# ============================================================

try:
    src = spark.table(f"{LAKEHOUSE}.{SILVER}.person")
    rows_in = src.count()

    df = src.select(
        F.col("Person_Identifier").alias("person_id"),
        F.col("First_Name").alias("first_name"),
        F.col("Middle_Name").alias("middle_name"),
        F.col("Last_Name").alias("last_name"),
        F.col("Full_Legal_Name").alias("full_legal_name"),
        F.col("Nickname").alias("nickname"),
        F.col("Suffix_Name").alias("suffix"),
        F.col("Birth_Date").alias("birth_date"),
        F.col("Birth_Place_Name").alias("birth_place"),
        F.col("Gender_Code").alias("gender"),
        F.col("Prefix_Name").alias("prefix"),
    )

    check_and_write(df, "dim_person", rows_in, ["person_id"])
except Exception as e:
    log("dim_person", "FAIL", 0, 0, str(e))


# ============================================================
# ## CELL 4 — dim_vehicle
# ============================================================

try:
    src = spark.table(f"{LAKEHOUSE}.{SILVER}.vehicle")
    rows_in = src.count()

    df = src.select(
        F.col("Insurable_Object_Identifier").alias("vehicle_id"),
        F.col("Vehicle_Make_Name").alias("make"),
        F.col("Vehicle_Model_Name").alias("model"),
        F.col("Vehicle_Model_Year").alias("model_year"),
        F.col("Vehicle_Driving_Wheel_Quantity").alias("driving_wheels"),
        F.col("Vehicle_Identification_Number").alias("vin"),
    )

    check_and_write(df, "dim_vehicle", rows_in, ["vehicle_id"])
except Exception as e:
    log("dim_vehicle", "FAIL", 0, 0, str(e))


# ============================================================
# ## CELL 5 — dim_policy
# ============================================================

try:
    src = spark.table(f"{LAKEHOUSE}.{SILVER}.policy")
    rows_in = src.count()

    df = src.select(
        F.col("Policy_Identifier").alias("policy_id"),
        F.col("Policy_Number").alias("policy_number"),
        F.col("Effective_Date").alias("effective_date"),
        F.col("Expiration_Date").alias("expiration_date"),
        F.col("Status_Code").alias("status"),
        F.col("Geographic_Location_Identifier").alias("location_id"),
    )

    check_and_write(df, "dim_policy", rows_in, ["policy_id"])
except Exception as e:
    log("dim_policy", "FAIL", 0, 0, str(e))


# ============================================================
# ## CELL 6 — dim_coverage
# ============================================================

try:
    src = spark.table(f"{LAKEHOUSE}.{SILVER}.coverage")
    rows_in = src.count()

    df = src.select(
        F.col("Coverage_Identifier").alias("coverage_id"),
        F.col("Coverage_Name").alias("coverage_name"),
        F.col("Coverage_Description").alias("coverage_description"),
        F.col("Coverage_Type_Identifier").alias("coverage_type_id"),
        F.col("Coverage_Group_Identifier").alias("coverage_group_id"),
        F.col("Coverage_Part_Code").alias("coverage_part"),
    )

    check_and_write(df, "dim_coverage", rows_in, ["coverage_id"])
except Exception as e:
    log("dim_coverage", "FAIL", 0, 0, str(e))


# ============================================================
# ## CELL 7 — dim_occurrence
# ============================================================

try:
    src = spark.table(f"{LAKEHOUSE}.{SILVER}.occurrence")
    rows_in = src.count()

    df = src.select(
        F.col("Occurrence_Identifier").alias("occurrence_id"),
        F.col("Occurrence_Name").alias("occurrence_name"),
        F.col("Occurrence_Begin_Date").alias("begin_date"),
        F.col("Occurrence_End_Date").alias("end_date"),
        F.col("Occurrence_Begin_Time").alias("begin_time"),
        F.col("Occurrence_End_Time").alias("end_time"),
        F.col("Is_Catastrophic_Event").alias("is_catastrophic"),
        F.col("Geographic_Location_Identifier").alias("location_id"),
    )

    check_and_write(df, "dim_occurrence", rows_in, ["occurrence_id"])
except Exception as e:
    log("dim_occurrence", "FAIL", 0, 0, str(e))


# ============================================================
# ## CELL 8 — dim_peril_code
# ============================================================

try:
    src = spark.table(f"{LAKEHOUSE}.{SILVER}.peril_code_map")
    rows_in = src.count()

    df = src.select(
        F.col("peril_code"),
        F.col("peril_description"),
        F.col("peril_category"),
        F.col("line_applicability"),
        F.col("catastrophe_eligible").alias("is_catastrophe_eligible"),
        F.col("standard_covered_flag").alias("is_standard_covered"),
    )

    check_and_write(df, "dim_peril_code", rows_in, ["peril_code"])
except Exception as e:
    log("dim_peril_code", "FAIL", 0, 0, str(e))


# ============================================================
# ## CELL 9 — fact_claim (enriched with occurrence + vehicle)
# ============================================================

try:
    claim = spark.table(f"{LAKEHOUSE}.{SILVER}.claim")
    occurrence = spark.table(f"{LAKEHOUSE}.{SILVER}.occurrence")
    vehicle = spark.table(f"{LAKEHOUSE}.{SILVER}.vehicle")

    rows_in = claim.count()

    df = claim.alias("c") \
        .join(
            occurrence.alias("o"),
            F.col("c.Occurrence_Identifier") == F.col("o.Occurrence_Identifier"),
            "left"
        ) \
        .join(
            vehicle.alias("v"),
            F.col("c.Insurable_Object_Identifier") == F.col("v.Insurable_Object_Identifier"),
            "left"
        ) \
        .select(
            # Claim keys and measures
            F.col("c.Claim_Identifier").alias("claim_id"),
            F.col("c.Company_Claim_Number").alias("claim_number"),
            F.col("c.Company_Subclaim_Number").alias("subclaim_number"),
            F.col("c.Claim_Description").alias("claim_description"),
            F.col("c.Claim_Status_Code").alias("claim_status"),
            F.col("c.Catastrophe_Identifier").alias("catastrophe_id"),

            # Dates
            F.col("c.Claims_Made_Date").alias("claims_made_date"),
            F.col("c.Claim_Open_Date").alias("open_date"),
            F.col("c.Claim_Close_Date").alias("close_date"),
            F.col("c.Claim_Reopen_Date").alias("reopen_date"),
            F.col("c.Claim_Reported_Date").alias("reported_date"),
            F.col("c.Entry_Into_Claims_Made_Program_Date").alias("claims_program_entry_date"),

            # Derived: days open
            F.datediff(
                F.coalesce(F.col("c.Claim_Close_Date"), F.current_date()),
                F.col("c.Claim_Open_Date")
            ).alias("days_open"),

            # Derived: is_open flag
            F.when(F.col("c.Claim_Close_Date").isNull(), True)
             .otherwise(False)
             .alias("is_open"),

            # FK references to dims
            F.col("c.Occurrence_Identifier").alias("occurrence_id"),
            F.col("c.Insurable_Object_Identifier").alias("vehicle_id"),

            # Denormalized occurrence fields
            F.col("o.Occurrence_Name").alias("occurrence_name"),
            F.col("o.Is_Catastrophic_Event").alias("is_catastrophic"),
            F.col("o.Occurrence_Begin_Date").alias("occurrence_date"),

            # Denormalized vehicle fields
            F.col("v.Vehicle_Make_Name").alias("vehicle_make"),
            F.col("v.Vehicle_Model_Name").alias("vehicle_model"),
            F.col("v.Vehicle_Model_Year").alias("vehicle_year"),
        )

    check_and_write(df, "fact_claim", rows_in, ["claim_id"])
except Exception as e:
    log("fact_claim", "FAIL", 0, 0, str(e))


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
    raise Exception(f"Gold transform failed for: {tables}")

elapsed = (datetime.now(timezone.utc) - run_start).total_seconds()
print(f"Gold transform complete. {len(results)} tables, {len(failed)} failures, {elapsed:.1f}s elapsed.")
