# fabric-migration-kit

Governed, repeatable migration of a client's data into Microsoft Fabric
(bronze, silver, gold, semantic model, report), with an AI agent as the
front door and Fabric + code doing the data work.

**Start here:** [ONBOARDING.md](ONBOARDING.md) -> [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) -> [docs/CONTRIBUTING.md](docs/CONTRIBUTING.md)

## Principles (details in docs/DECISIONS.md)
- The client or we own the config (registry + dictionary). The agent never decides which tables matter.
- Code does everything deterministic. The LLM only handles judgement, returns typed objects, and code validates them.
- Agents see metadata and aggregate stats, never row values.
- Secrets: none in code. The only env var is `AZURE_KEYVAULT_URL`. No URLs in code.
- Success is decided by reconciliation, not by an agent's summary.

## Status (keep this honest; update in the PR that changes it)
| Area | Status |
|---|---|
| Rules, policies, guard, audit, secrets, typed models | carried over from prototype; tests pass |
| Reconciliation, SQL generation | carried over; **not yet run on real systems** |
| Control table + Fabric pipeline (happy path) | not started |
| Foundry agent (front door) | not started |
| Terraform / fabric-cicd | not started |
| Medallion, semantic model, report | not started |
