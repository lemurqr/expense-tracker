Follow SPEC.md.

## Priorities
1. Working app first
2. Clean structure
3. Simple design over complexity

If a decision is unclear, choose the simplest safe solution.

## Current stack
- Python
- Flask
- Postgres
- Docker Compose

## Safety rules
- Never run tests against the live database.
- Use only the dedicated test database for tests.
- For risky work, list the files to change first before editing.
- Be extra careful with:
  - import logic
  - household / settlement logic
  - database writes
  - cleanup / reset logic
- Avoid migrations unless explicitly required by the task.
- Do not change unrelated files.

## Testing rules
- Do not run broad pytest by default.
- Prefer the smallest targeted test that proves the change.
- For DB-sensitive changes, state the exact test command before running it.

## Workflow
- Keep diffs narrow.
- Explain changes in plain English.
- Prefer preserving existing behavior unless explicitly asked to change it.
