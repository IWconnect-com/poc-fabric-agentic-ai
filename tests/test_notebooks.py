"""Static checks on the Fabric notebooks (they need Spark, so they are not executed here)."""
import re

import pytest

from conftest import ROOT

GOLD = (ROOT / "notebooks" / "nb_gold_transform.py").read_text(encoding="utf-8")
SILVER = (ROOT / "notebooks" / "nb_silver_transform.py").read_text(encoding="utf-8")

GOLD_TABLES = {
    "dim_party", "dim_person", "dim_vehicle", "dim_policy",
    "dim_coverage", "dim_occurrence", "dim_peril_code", "fact_claim",
}
SILVER_TABLES = {
    "party", "person", "vehicle", "policy",
    "coverage", "occurrence", "claim", "peril_code_map",
}


def function_body(text, name):
    return text.split(f"def {name}", 1)[1].split("\ndef ", 1)[0]


def load_function(text, name):
    """Exec a pure-Python helper from a notebook so it can be tested without Spark."""
    ns = {}
    exec(f"def {name}" + function_body(text, name).split("\n\n\n", 1)[0], ns)
    return ns[name]


# ---- gold ----

def test_every_gold_table_is_written_through_the_key_check():
    calls = re.findall(r'check_and_write\(df, "(\w+)", rows_in, \[([^\]]*)\]\)', GOLD)
    assert {table for table, _ in calls} == GOLD_TABLES
    assert all(keys.strip() for _, keys in calls), "every table needs at least one key column"


def test_write_gold_is_only_called_by_check_and_write():
    # one definition + one call inside check_and_write
    assert len(re.findall(r"\bwrite_gold\(", GOLD)) == 2
    assert "write_gold(df, table_name)" in function_body(GOLD, "check_and_write")


def test_key_problems_are_checked_before_the_write():
    body = function_body(GOLD, "check_and_write")
    assert body.index("key_problems(") < body.index("write_gold(")
    assert "rows_out != rows_in" in body


# ---- silver ----

def test_every_silver_table_is_written_through_the_checks():
    calls = re.findall(r'check_and_write\(df, "(\w+)", rows_in, "(\w+)", src', SILVER)
    assert {table for table, _ in calls} == SILVER_TABLES


def test_write_silver_and_dedupe_are_only_called_by_check_and_write():
    body = function_body(SILVER, "check_and_write")
    assert len(re.findall(r"\bwrite_silver\(", SILVER)) == 2 and "write_silver(" in body
    assert len(re.findall(r"\bdedupe\(", SILVER)) == 2 and "dedupe(" in body


def test_silver_null_key_fails_before_the_write():
    body = function_body(SILVER, "check_and_write")
    assert body.index('"FAIL"') < body.index("write_silver(")


def test_silver_refuses_incremental_tables():
    # overwrite + arbitrary dedupe is wrong for incremental; it must stop, not run silently
    body = function_body(SILVER, "check_and_write")
    guard = re.search(r"\n    if table_name in INCREMENTAL_TABLES:\s*\n\s+raise ", body)
    assert guard and guard.start() < body.index("write_silver(")


def silver_cells():
    return {
        m.group(1): m.group(2)
        for m in re.finditer(r"## CELL \d+ — silver\.(\w+)\n(.*?)(?=\n# =+\n# ## CELL|\Z)", SILVER, re.S)
    }


def test_every_silver_table_carries_bronze_lineage():
    cells = silver_cells()
    assert set(cells) == SILVER_TABLES
    missing = [t for t, body in cells.items() if "*lineage(src)" not in body.split("check_and_write", 1)[0]]
    assert missing == []


def test_lineage_column_names_match_the_pipeline_docs():
    # the Copy activity's Additional columns are set by hand in Fabric; names must match exactly
    names = re.search(r"BRONZE_LINEAGE = \[([^\]]*)\]", SILVER).group(1)
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    for name in re.findall(r'"(\w+)"', names):
        assert f"`{name}`" in readme


def test_silver_rows_are_stamped_with_the_run_before_the_write():
    body = function_body(SILVER, "check_and_write")
    stamp = min(body.index('"_silver_run_id"'), body.index('"_processed_at"'))
    assert stamp < body.index("write_silver(")


def test_lineage_columns_are_not_data_checked():
    assert 'not c.startswith("_")' in function_body(SILVER, "new_nulls")


# ---- error messages (both notebooks) ----

@pytest.mark.parametrize("text", [GOLD, SILVER], ids=["gold", "silver"])
def test_failures_log_short_error_not_str_e(text):
    assert "str(e)" not in text.split("def write_run_log", 1)[1].split("## CELL 10", 1)[0]
    fail_logs = re.findall(r'log\("\w+", "FAIL", [^\n]*', text)
    assert fail_logs and all("short_error(e)" in line for line in fail_logs)


@pytest.mark.parametrize("text", [GOLD, SILVER], ids=["gold", "silver"])
def test_short_error_keeps_type_and_first_line_only(text):
    short_error = load_function(text, "short_error")
    msg = short_error(ValueError("bad cast\n  at line 1\n  at line 2"))
    assert msg == "ValueError: bad cast"
    assert len(short_error(RuntimeError("x" * 5000))) == 500
    assert short_error(KeyError()) == "KeyError"
