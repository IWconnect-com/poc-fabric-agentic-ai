# Instructions for Claude Code (and any AI assistant)

Read README.md, docs/ARCHITECTURE.md, docs/DECISIONS.md first.

## Non-negotiable
- Never read, create or commit `.env` files, keys, passwords or connection strings. Never print secrets.
- The only env var is `AZURE_KEYVAULT_URL`. Everything else from Key Vault at runtime. No URLs in code.
- Agents see metadata and aggregate stats, never row values. The agent does not choose tables.
- LLM proposes typed objects; code validates and acts. No LLM in the scheduled data path.
- Tool calls go through ToolGuard (default deny, audited). Prod changes need a human PR.
- SQL identifiers go through `check_ident` / `tq` / `sq`; values are parameters.

## Working style
- Small phases; tests first; keep `pytest` green. Mutation-check behaviour changes.
- Update README status / DECISIONS in the same PR. Mark anything unverified as unverified.
