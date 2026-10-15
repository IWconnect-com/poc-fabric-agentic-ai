# fabric-migration-kit

Repeatable, governed migration of a client's data into Microsoft Fabric (bronze, silver, gold,
semantic model, report). An AI agent is the front door; Fabric and code do the data work.

## Rules
- The client or we own the config. The agent never decides which tables matter.
- Code does everything deterministic. The LLM only judges, returns typed objects, code validates.
- Agents see metadata and aggregate stats, never row values.
- No secrets, `.env` or URLs in code. The only env var is `AZURE_KEYVAULT_URL`.
- Success is decided by reconciliation, not by an agent's summary.

More: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) (code vs LLM) · [docs/DECISIONS.md](docs/DECISIONS.md) (what we decided and why) · [CLAUDE.md](CLAUDE.md) (rules for AI assistants)

## Get started
```
git clone <repo> && cd fabric-migration-kit
python -m venv .venv && source .venv/bin/activate     # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
pytest                                                # expect: all pass
az login                                              # DEV only; set AZURE_KEYVAULT_URL in your shell
```
Access to the DEV Fabric workspace, DEV Key Vault and Foundry project: ask your team lead. Never share secrets or the Key Vault URL in chat or code.

## What exists in Fabric today
These items were built by hand in the DEV workspace and are **not in Git yet** (next step: Git integration into `fabric/`).
- Fabric SQL database `sqldb_control`, table `meta.pipeline_config`: the list of tables to load. A person owns it. Seed script: `sql/meta_pipeline_config.sql`.
- Lakehouse `lh_poc_fabric_power_bi_dev` with schemas `bronze`, `silver`, `gold`. The layer is the schema (`bronze.party`), never part of the table name.
- Pipeline: Lookup (reads the config) -> ForEach -> Copy from Azure SQL into `bronze`. Full loads use **Overwrite** (Append duplicated every row on rerun).
- Check inside the pipeline: after each Copy, a Lookup counts the source table and an If condition compares it with the rows Copy reports. A mismatch fails the run with `COUNT_MISMATCH` and the table name. Tested with a forced failure.
- Verified by hand for all 7 tables: bronze counts equal the source.

Fabric pipeline gotchas (learned the hard way):
- Enter dynamic expressions through the **Add dynamic content** pane opened from that field. Typing `@...` into a plain text box sends the text to SQL literally.
- `item()` only exists at run time. **Preview data** and **Import schemas** on activities that use it show an "additional expressions" prompt. Do not import a schema into the Copy: keep **Mapping** empty so each table maps itself.
- Test a check by making it fail on purpose, then restore it.

## How we work
Never change `main` directly. For every change:
1. In VS Code, make a new branch from `main` (bottom-left branch name -> Create new branch).
2. Make your change and commit it (Source Control panel -> message -> Commit).
3. Click **Publish Branch**, then open a **Pull Request** on GitHub.
4. A teammate reviews it and merges. Switch back to `main` and press **Sync**.

Also:
- Unit tests never touch Azure (use fakes). Prod changes only via the deployment pipeline.
- Changed behaviour or a decision? Update this README or `docs/DECISIONS.md` in the same PR.
- Building something by hand in Fabric? Tell the team, and get it into Git so others can rebuild it.
- Found a wrong or missing step here? Fix it in a PR.

## Status (keep honest)
| Area | Status |
|---|---|
| Rules, policies, guard, audit, secrets, typed models | tests pass |
| Reconciliation, SQL generation | tests pass; **not yet run on real systems** |
| Control table + bronze pipeline (7 Azure SQL tables, full load) | **works in DEV, counts verified; built by hand, not in Git yet** |
| Count-mismatch check inside the pipeline (source count vs rows copied) | **works in DEV, failure path tested**; independent count of the bronze table not started |
| Blob CSV source | **works in DEV** — Switch routes sql/blob, peril_code_map in bronze |
| Silver layer (notebook) | **works in DEV** — 8 silver tables verified. Data checks (null key FAIL; duplicate keys, values NULLed by casts WARN) and short error messages **run in DEV 2026-10-15**: 7 SQL tables WARN (source has every row 3x, expected — see DECISIONS known limits), peril_code_map OK, no cast NULLs |
| Gold layer (notebook) | **works in DEV** — 7 dims + fact_claim, 0 failures with key checks (null/duplicate key, row count) on 2026-10-15; failure path of the key check not yet tested in Fabric |
| Run log (error handling) | notebooks append to `gold._run_log`; DDL for `meta.run_log` in `sqldb_control` ready |
| Foundry agent (front door) | not started |
| Terraform / fabric-cicd, Git integration of Fabric items | not started |
| Semantic model, report | not started |
