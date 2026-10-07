# Onboarding

Goal: be productive in a day without asking anyone. If a step here is wrong or missing,
fix it in a PR. That is the most useful contribution a new member can make.

## Day 1: understand
1. Read README.md, then docs/ARCHITECTURE.md (what is code, what is LLM).
2. Read docs/DECISIONS.md. Do not re-open a decision without a PR.
3. Read docs/CONTRIBUTING.md (branches, PRs, definition of done).

## Day 1: access (needs an approver; takes time, request it first)
| Need | Who approves | Check |
|---|---|---|
| DEV Fabric workspace (Contributor) | TODO | you can open the workspace |
| DEV Key Vault (Secrets User) | TODO | `az keyvault secret list --vault-name <dev vault>` works |
| Foundry project (DEV) | TODO | you can open it in the portal |

Never ask for, paste, or store secrets or the Key Vault URL in chat, tickets, or code.

## Day 1: local setup
```
git clone <repo> && cd fabric-migration-kit
python -m venv .venv && source .venv/bin/activate    # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
pytest                                               # expect: all pass
az login                                             # DEV only
```
Set `AZURE_KEYVAULT_URL` for DEV in your shell profile. It is the only environment variable
this project uses. There is no `.env` file, ever.

## Day 2-5: first contribution
Pick an issue labelled `good-first-issue`, branch from `main`, open a PR using the template.
