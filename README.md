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

## How we work
- Short branches from `main`, PR with one reviewer, tests green.
- Unit tests never touch Azure (use fakes). Prod changes only via the deployment pipeline.
- Changed behaviour or a decision? Update this README or `docs/DECISIONS.md` in the same PR.
- Found a wrong or missing step here? Fix it in a PR.

## Status (keep honest)
| Area | Status |
|---|---|
| Rules, policies, guard, audit, secrets, typed models | tests pass |
| Reconciliation, SQL generation | tests pass; **not yet run on real systems** |
| Control table + Fabric pipeline (happy path) | not started |
| Foundry agent (front door) | not started |
| Terraform / fabric-cicd | not started |
| Medallion, semantic model, report | not started |
