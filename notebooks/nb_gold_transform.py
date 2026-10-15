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

LAKEHOUSE = "lh_data"
SILVER = "silver"
GOLD = "gold"

run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
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

    write_gold(df, "dim_party")
    log("dim_party", "OK", rows_in, df.count())
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

    write_gold(df, "dim_person")
    log("dim_person", "OK", rows_in, df.count())
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

    write_gold(df, "dim_vehicle")
    log("dim_vehicle", "OK", rows_in, df.count())
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

    write_gold(df, "dim_policy")
    log("dim_policy", "OK", rows_in, df.count())
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

    write_gold(df, "dim_coverage")
    log("dim_coverage", "OK", rows_in, df.count())
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

    write_gold(df, "dim_occurrence")
    log("dim_occurrence", "OK", rows_in, df.count())
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

    write_gold(df, "dim_peril_code")
    log("dim_peril_code", "OK", rows_in, df.count())
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

    write_gold(df, "fact_claim")
    log("fact_claim", "OK", rows_in, df.count())
except Exception as e:
    log("fact_claim", "FAIL", 0, 0, str(e))


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
    raise Exception(f"Gold transform failed for: {tables}")

print(f"Gold transform complete. {len(results)} tables, {len(failed)} failures.")
