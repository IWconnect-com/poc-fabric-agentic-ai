# Architecture: where code ends and the LLM starts

```
CLIENT / WE:  config (registry YAML) + dictionary + scope       human-owned, PR-approved
[CODE] Metadata extractor       read-only login, no SELECT, writes a metadata file
[CODE] Config validator         schema checks, key/watermark rules, dictionary lookups
[LLM ] Config reviewer          flags what rules cannot (optional, metadata only)
[CODE] Generator                templates + config -> pipeline, silver/gold SQL, DQ checks
[LLM ] Notebook drafter         dev only; reviewed, tested, reconciled
[FABRIC] Pipeline -> bronze -> MLV silver/gold -> semantic model -> report    no LLM
[CODE] Reconciliation           the judge after every run
[LLM ] Front-door agent         picks a tool, explains the result
   tools [CODE via ToolGuard]:  run_ingestion(source_id), get_run_status(run_id),
                                get_failure_details(run_id), ask_data_agent(question)
[LLM ] Failure triage           logs + DQ results (no row data) -> typed diagnosis
[LLM ] Improvement proposals    typed object -> PR for a human
[CODE] Audit, policies.yaml, Key Vault, approvals, telemetry                 around everything
```

Rules: every LLM box returns a typed object that code validates; every tool call goes through
ToolGuard (default deny, audited); secrets and connection values never reach an LLM; prod changes
need a human; scheduled runs make zero LLM calls.

Infrastructure: Terraform (capacity, workspaces, connections, roles) + fabric-cicd (items from Git),
per-environment values in Variable Libraries. Provisioning is code with plan/apply, never an agent.
