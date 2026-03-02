# Postgres Verification

Use these commands when running via Docker Compose:

```bash
docker compose exec web python -c "from expense_tracker import create_app; create_app()"
docker compose exec db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "\dt"
docker compose exec db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT * FROM schema_version ORDER BY version;"
docker compose exec web curl -s http://localhost:5000/health/db
```

Expected:
- `\dt` includes app tables like `users`, `expenses`, `categories`, `schema_version`.
- `schema_version` has rows up to the latest migration.
- `/health/db` returns `{ "ok": true, ... }`.
