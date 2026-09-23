# Contributing

AgentHub is a small public project. Focused pull requests are welcome.

## License

Contributions are licensed under the [MIT License](LICENSE).
Copyright (c) 2026 Riccardo Culler and contributors.

## Branches

- Open pull requests against `main`.
- Use a short, specific branch name (`docs/...`, `fix/...`, `feat/...`).

## What to include

One concern per pull request. Leave unrelated refactors for another change.

Do not commit secrets. That includes `.env`, tokens, private keys, and dumps of device credentials such as `robot_uuid`. Commit `.env.example` when a new setting needs to be documented, with the value left blank.

## Checks

Runtime changes: run `python -m pytest tests -q` locally.

Docs-only changes do not need a test run. This repository does not require a green GitHub check yet; if checks exist on the pull request, look at them and do not describe a failing check as passing.

## Commit messages

Use a readable imperative subject, with a conventional prefix when it fits:

- `docs:` documentation only
- `fix:` a defect
- `feat:` user-visible behavior

Say why in the body when the subject is not enough.

## Pull request text

Fill in the pull request template:

1. Problem / context
2. What changed
3. Out of scope
4. How to review

Screenshots are not needed for documentation-only changes. Link an issue when one exists. Keep the pull request as a draft only while the change is incomplete.
