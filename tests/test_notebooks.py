"""Static checks on the Fabric notebooks (they need Spark, so they are not executed here)."""
import re

from conftest import ROOT

GOLD = (ROOT / "notebooks" / "nb_gold_transform.py").read_text()

GOLD_TABLES = {
    "dim_party", "dim_person", "dim_vehicle", "dim_policy",
    "dim_coverage", "dim_occurrence", "dim_peril_code", "fact_claim",
}


def test_every_gold_table_is_written_through_the_key_check():
    calls = re.findall(r'check_and_write\(df, "(\w+)", rows_in, \[([^\]]*)\]\)', GOLD)
    assert {table for table, _ in calls} == GOLD_TABLES
    assert all(keys.strip() for _, keys in calls), "every table needs at least one key column"


def test_write_gold_is_only_called_by_check_and_write():
    # one definition + one call inside check_and_write
    assert len(re.findall(r"\bwrite_gold\(", GOLD)) == 2
    body = GOLD.split("def check_and_write", 1)[1].split("\ndef ", 1)[0]
    assert "write_gold(df, table_name)" in body


def test_key_problems_are_checked_before_the_write():
    body = GOLD.split("def check_and_write", 1)[1].split("\ndef ", 1)[0]
    assert body.index("key_problems(") < body.index("write_gold(")
    assert "rows_out != rows_in" in body
