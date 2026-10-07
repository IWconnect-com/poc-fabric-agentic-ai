# Decisions and known limits

Change only by PR with a CODEOWNER review. Superseded decisions stay, marked as such.

| # | Decision | Why |
|---|---|---|
| 1 | No mirroring. Metadata-driven Fabric pipelines (Lookup, ForEach, Copy) | Visible, controllable runs |
| 2 | Silver and gold as Materialized Lake Views (SQL); notebooks only as fallback | Declarative, lineage, DQ constraints, in Git |
| 3 | Agent proposes, code disposes: LLM output is a typed object validated before use | Only ambiguous judgement is probabilistic |
| 4 | Agents see metadata and aggregate stats only, enforced by DB permissions (VIEW DEFINITION, no SELECT) | PII, client trust, prompt-injection surface |
| 5 | Registry is YAML in Git (source of truth); the runtime control table in Azure SQL is seeded from it | Reviewable, versioned; pipeline Lookup needs a runtime table |
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

## Known limits
- Updates older than the lookback are missed by watermark; mitigate with scheduled full reconciliation (later).
- Hard deletes are invisible to a watermark.
- Rows with NULL watermark are never extracted; the config validator must reject such columns.
- Watermark values assumed UTC.
- Spark MERGE (named params, qualified names) and T-SQL queries are unverified on real systems.
- Terraform provider coverage and Foundry/Osmos preview status must be re-checked before client commitments.
