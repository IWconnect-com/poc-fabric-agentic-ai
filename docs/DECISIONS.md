# Decisions and known limits

Change only by PR with a CODEOWNER review. Superseded decisions stay, marked as such.

| # | Decision | Why |
|---|---|---|
| 1 | No mirroring. Metadata-driven Fabric pipelines (Lookup, ForEach, Copy) | Visible, controllable runs |
| 2 | Silver and gold as Materialized Lake Views (SQL); notebooks only as fallback | Declarative, lineage, DQ constraints, in Git |
| 3 | Agent proposes, code disposes: LLM output is a typed object validated before use | Only ambiguous judgement is probabilistic |
| 4 | Agents see metadata and aggregate stats only, enforced by DB permissions (VIEW DEFINITION, no SELECT) | PII, client trust, prompt-injection surface |
| 5 | Registry is YAML in Git (source of truth); the runtime control table (`meta.pipeline_config` in Fabric SQL database `sqldb_control`, never in a source database) is seeded from it | Reviewable, versioned; pipeline Lookup needs a runtime table |
| 6 | Incremental by watermark with lookback; hash diff where no trustworthy modified column | Late updates re-read; merge idempotent |
| 7 | Watermark advances only after extract, land, merge, reconcile succeed | A failed run is simply re-run |
| 8 | Bronze append-only; silver latest-per-key; deletes are soft | Auditable, no data loss |
| 9 | Reconciliation compares counts and measure sums per partition | Cross-engine checksums give false alarms |
| 10 | Judge = deterministic reconciliation; LLM judge only for semantic review | An LLM grading its own report is not governance |
| 11 | Identity-based auth wherever possible; Key Vault for the rest; only env var is AZURE_KEYVAULT_URL | The best secret is the one that does not exist |
| 12 | **The agent does not choose which tables to migrate.** Client or we own the config; a dictionary translates field names | It is a business decision; metadata cannot reveal it |
| 13 | Agent is a front door with coarse tools (run_ingestion, get_run_status, ...). Fixed step order lives in the pipeline, not in the agent | A skipped step must not be an LLM mistake |
| 14 | Build the agent with Microsoft Agent Framework (GA at Build 2026); Foundry hosted agents and Osmos are preview: optional, not promised to clients | Do not sell preview features as production |
| 15 | Infra: Terraform + fabric-cicd; per-environment values in Variable Libraries; provisioning is code (plan/apply), never an agent | Repeatable, reviewable, least privilege |
| 16 | Fresh repo; prototype engine (incremental run engine, ports, state) kept only as an archive/reference | Fabric does the data movement; engine was unproven |
| 17 | One lakehouse (`lh_poc_fabric_power_bi_dev`) with `bronze`/`silver`/`gold` schemas; the layer is a fixed value in each pipeline, never a config column; `target_table` is the bare name (`bronze.party`). Split lakehouses per layer only if security or ownership requires | Same name across layers, fewer items, lineage readable |
| 18 | Copy to bronze uses Overwrite for `load_strategy='full'` (Append duplicated every row on rerun); incremental tables must never overwrite. Pipeline must fail on source-vs-bronze count mismatch | Found in the first run |
| 19 | Count check: Lookup counts the source table after Copy, an If condition compares it with Copy's `rowsCopied`, the Fail activity stops the run. Mapping stays empty. Stronger check (notebook counting the bronze table, measure sums) comes later | Cheap and reliable now; counting through the SQL endpoint right after a write can lag |
| 20 | Blob/CSV ingestion via Switch on `source_type` inside ForEach. `source_schema` = container name for blob rows. Count check skipped for blob (file is small, static); stronger check later | One pipeline handles all source types; config drives the routing |
| 21 | Count validation uses a single THROW-based Lookup instead of Lookup → If → Fail, because Fabric does not allow control-flow activities (If, Switch, ForEach) nested inside a Switch case or another If branch | Discovered during wiring; simpler and fewer activities |
| 22 | Silver layer via Spark notebook (not MLVs). One notebook transforms all 8 bronze tables: type-cast, trim, dedupe, write Delta to silver schema. Pipeline calls notebook after ingestion. Supersedes Decision 2 MLV-first preference | Notebooks handle error logging, try/catch per table, conditional logic; MLVs are read-only views with no materialization in Fabric lakehouses |
| 23 | Gold layer via Spark notebook. Star schema: 7 dimension tables (`dim_party`, `dim_person`, `dim_vehicle`, `dim_policy`, `dim_coverage`, `dim_occurrence`, `dim_peril_code`) + 1 fact table (`fact_claim`). fact_claim joins claim with occurrence and vehicle (left joins), adds derived `days_open` and `is_open`. All columns renamed to snake_case business names | Same pattern as silver; star schema gives clean semantic-model input; denormalized occurrence/vehicle on fact avoids extra joins in reports |
| 24 | Run log: both notebooks append per-table results (run_id, notebook, table, status, rows_in, rows_out, error_message, timestamp) to `gold._run_log` Delta table. DDL for `sqldb_control.meta.run_log` ready for when JDBC + managed identity is wired. Log write failure is a warning, not a fatal error | Operational visibility without JDBC dependency; migrate to SQL database later |
| 25 | Gold tables are written only through `check_and_write`: before the overwrite it checks that the row count equals the silver input and that the key column(s) have no nulls and no duplicates. On a problem the table is logged FAIL and **not written**, so the last good gold version stays; the notebook then fails the run. Enforced by `tests/test_notebooks.py` | `rows_in == rows_out` alone is always true for a select-only dim and catches nothing; a duplicate key silently breaks the semantic model's relationships |
| 26 | Silver tables are written only through `check_and_write`. Null or uncastable key: FAIL, table **not written**. Duplicate keys dropped by dedupe, or values that were present in bronze but became NULL after a cast: WARN with counts, table written. Duplicates are a WARN only because all loads are full (bronze overwritten, so duplicates came from the source); tables in `INCREMENTAL_TABLES` are refused until silver does latest-per-key by watermark + MERGE (Decision 8). Both notebooks log failures as `short_error(e)`: exception type + first line, max 500 chars | Silver used to drop rows and NULL bad values while logging OK, so gold's checks never saw the problem. Full Spark messages are stack traces and can quote row values (Decision 4) |

## Known limits
- Updates older than the lookback are missed by watermark; mitigate with scheduled full reconciliation (later).
- Hard deletes are invisible to a watermark.
- Rows with NULL watermark are never extracted; the config validator must reject such columns.
- Watermark values assumed UTC.
- Spark MERGE (named params, qualified names) and T-SQL queries are unverified on real systems.
- Terraform provider coverage and Foundry/Osmos preview status must be re-checked before client commitments.
- Version control for Fabric items is undecided: Fabric Git integration supports Azure DevOps and GitHub (GitLab: verify), the team uses GitLab for dev, and tenant settings need an admin. Until decided, Fabric items are not in Git.
- The DEV Azure SQL source holds every row 3 times (no primary keys; 2730 rows / 910 keys in `Party`, all 7 SQL tables 3x). Left as is by choice (2026-10-15): silver dedupes and logs the 7 SQL tables as WARN on every run; this is expected, not a new fault. Checked by hand only for one key that the copies are identical; if copies of a key ever differ, which one silver keeps is arbitrary.
- The count check trusts the number Copy reports. A source that changes during the run can cause a false mismatch.
