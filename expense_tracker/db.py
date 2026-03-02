import os
import re
import sqlite3
from pathlib import Path
from urllib.parse import urlparse

try:
    import psycopg
    from psycopg.rows import tuple_row
except ImportError:  # pragma: no cover - dependency optional for sqlite-only environments
    psycopg = None
    tuple_row = None


class CompatRow:
    def __init__(self, columns, values):
        self._columns = tuple(columns)
        self._values = tuple(values)
        self._lookup = {name: idx for idx, name in enumerate(self._columns)}

    def __getitem__(self, key):
        if isinstance(key, str):
            return self._values[self._lookup[key]]
        return self._values[key]

    def __iter__(self):
        return iter(self._values)


class CompatCursor:
    def __init__(self, cursor):
        self._cursor = cursor

    @property
    def rowcount(self):
        return getattr(self._cursor, "rowcount", -1)

    @property
    def description(self):
        return self._cursor.description

    def fetchone(self):
        row = self._cursor.fetchone()
        return self._adapt_row(row)

    def fetchall(self):
        return [self._adapt_row(row) for row in self._cursor.fetchall()]

    def _adapt_row(self, row):
        if row is None:
            return None
        if isinstance(row, sqlite3.Row):
            return row
        columns = [col.name if hasattr(col, "name") else col[0] for col in (self.description or [])]
        return CompatRow(columns, row)


class CompatConnection:
    def __init__(self, conn, backend):
        self._conn = conn
        self.backend = backend

    def execute(self, sql, params=None):
        rewritten_sql, rewritten_params = rewrite_sql(self.backend, sql, params)
        cur = self._conn.execute(rewritten_sql, rewritten_params or ())
        return CompatCursor(cur)

    def close(self):
        self._conn.close()

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def __enter__(self):
        self._conn.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        return self._conn.__exit__(exc_type, exc, tb)


def is_postgres_url(value):
    return bool(value) and (value.startswith("postgresql://") or value.startswith("postgres://"))


def _convert_qmark_placeholders(sql):
    pieces = sql.split("?")
    if len(pieces) == 1:
        return sql
    return "%s".join(pieces)


def rewrite_sql(backend, sql, params):
    rewritten_sql = sql
    rewritten_params = params

    if backend == "postgres":
        if "last_insert_rowid()" in rewritten_sql:
            rewritten_sql = rewritten_sql.replace("last_insert_rowid()", "lastval()")
        pragma_match = re.match(r"\s*PRAGMA\s+table_info\(([^)]+)\)", rewritten_sql, re.IGNORECASE)
        if pragma_match:
            table_name = pragma_match.group(1).strip().strip("'\"")
            rewritten_sql = (
                "SELECT column_name AS name "
                "FROM information_schema.columns "
                "WHERE table_schema = current_schema() AND table_name = %s "
                "ORDER BY ordinal_position"
            )
            rewritten_params = (table_name,)
        elif "?" in rewritten_sql:
            rewritten_sql = _convert_qmark_placeholders(rewritten_sql)

        if rewritten_params is None:
            rewritten_params = ()
        elif not isinstance(rewritten_params, (tuple, list, dict)):
            rewritten_params = (rewritten_params,)

    return rewritten_sql, rewritten_params


def parse_database_config(database_path=None):
    db_url = os.environ.get("DATABASE_URL", "").strip()
    if is_postgres_url(db_url):
        parsed = urlparse(db_url)
        db_name = parsed.path.lstrip("/") or "postgres"
        return {
            "backend": "postgres",
            "database_url": db_url,
            "database_name": db_name,
            "database_path": database_path,
        }

    return {
        "backend": "sqlite",
        "database_url": None,
        "database_name": Path(database_path).name if database_path else "sqlite",
        "database_path": database_path,
    }


def connect_db(config):
    backend = config["backend"]
    if backend == "postgres":
        if psycopg is None:
            raise RuntimeError("psycopg is required when DATABASE_URL points to Postgres")
        conn = psycopg.connect(config["database_url"], row_factory=tuple_row)
        return CompatConnection(conn, backend="postgres")

    db_path = config["database_path"]
    if db_path:
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    return CompatConnection(conn, backend="sqlite")
