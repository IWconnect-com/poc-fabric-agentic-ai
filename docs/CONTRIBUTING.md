# Contributing

- Trunk-based: short-lived branches from `main`, PR required, at least one reviewer.
- CODEOWNERS must approve changes to policies, allowed tools, decisions, and infra.
- Anything that changes prod runs through the deployment pipeline under the CI identity, never from a laptop.
- Local dev uses `az login` against DEV only.
- Unit tests never touch Azure; use fakes. Integration tests are marked and run only against DEV.
- When you change behaviour, break it on purpose once (mutation check) to see a test fail.
- Keep code plain: small modules, typed models, no clever abstractions.

## Definition of done
Tests pass; no secrets, `.env` or hardcoded URLs; docs updated or "not needed (why)";
README status table still true; named reviewer for anything touching prod.
