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

LAKEHOUSE = "lh_data"
BRONZE = "bronze"
SILVER = "silver"

run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
results = []  # (table, status, rows_in, rows_out, message)


def log(table, status, rows_in, rows_out, msg=""):
    results.append((table, status, rows_in, rows_out, msg))
    print(f"[{status}] {SILVER}.{table}: {rows_in} -> {rows_out}  {msg}")


def safe_to_date(col_name):
    """Cast varchar to date, NULLs on parse failure."""
    return F.to_date(F.col(col_name)).alias(col_name)


def dedupe(df, key_col):
    """Keep one row per key. Full-load bronze has no duplicates normally,
    but this guards against double-runs or future incremental appends."""
    return df.dropDuplicates([key_col])


def write_silver(df, table_name):
    """Overwrite the silver table as a managed Delta table."""
    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{LAKEHOUSE}.{SILVER}.{table_name}")


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
    df = dedupe(df, "Party_Identifier")

    write_silver(df, "party")
    log("party", "OK", rows_in, df.count())
except Exception as e:
    log("party", "FAIL", 0, 0, str(e))


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
    df = dedupe(df, "Person_Identifier")

    write_silver(df, "person")
    log("person", "OK", rows_in, df.count())
except Exception as e:
    log("person", "FAIL", 0, 0, str(e))


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
    df = dedupe(df, "Insurable_Object_Identifier")

    write_silver(df, "vehicle")
    log("vehicle", "OK", rows_in, df.count())
except Exception as e:
    log("vehicle", "FAIL", 0, 0, str(e))


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
    df = dedupe(df, "Policy_Identifier")

    write_silver(df, "policy")
    log("policy", "OK", rows_in, df.count())
except Exception as e:
    log("policy", "FAIL", 0, 0, str(e))


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
    df = dedupe(df, "Coverage_Identifier")

    write_silver(df, "coverage")
    log("coverage", "OK", rows_in, df.count())
except Exception as e:
    log("coverage", "FAIL", 0, 0, str(e))


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
    df = dedupe(df, "Occurrence_Identifier")

    write_silver(df, "occurrence")
    log("occurrence", "OK", rows_in, df.count())
except Exception as e:
    log("occurrence", "FAIL", 0, 0, str(e))


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
    df = dedupe(df, "Claim_Identifier")

    write_silver(df, "claim")
    log("claim", "OK", rows_in, df.count())
except Exception as e:
    log("claim", "FAIL", 0, 0, str(e))


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
    df = dedupe(df, "peril_code")

    write_silver(df, "peril_code_map")
    log("peril_code_map", "OK", rows_in, df.count())
except Exception as e:
    log("peril_code_map", "FAIL", 0, 0, str(e))


# ============================================================
# ## CELL 10 — Summary
# ============================================================

from pyspark.sql import Row

summary = spark.createDataFrame(
    [Row(run_id=run_id, table=r[0], status=r[1],
         rows_in=r[2], rows_out=r[3], message=r[4])
     for r in results]
)
summary.show(truncate=False)

failed = [r for r in results if r[1] == "FAIL"]
if failed:
    tables = ", ".join(r[0] for r in failed)
    raise Exception(f"Silver transform failed for: {tables}")

print(f"Silver transform complete. {len(results)} tables, {len(failed)} failures.")
