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

    def insert_ignore(self, table, columns, values, conflict_cols):
        placeholders = ", ".join(["?"] * len(columns))
        column_sql = ", ".join(columns)
        if self.backend == "postgres":
            if conflict_cols:
                conflict_sql = ", ".join(conflict_cols)
                sql = (
                    f"INSERT INTO {table} ({column_sql}) VALUES ({placeholders}) "
                    f"ON CONFLICT ({conflict_sql}) DO NOTHING"
                )
            else:
                sql = f"INSERT INTO {table} ({column_sql}) VALUES ({placeholders})"
        else:
            sql = f"INSERT OR IGNORE INTO {table} ({column_sql}) VALUES ({placeholders})"
        return self.execute(sql, tuple(values))

    def upsert(self, table, columns, values, conflict_cols, update_cols):
        placeholders = ", ".join(["?"] * len(columns))
        column_sql = ", ".join(columns)
        conflict_sql = ", ".join(conflict_cols)
        if update_cols:
            set_sql = ", ".join([f"{col} = excluded.{col}" for col in update_cols])
            action_sql = f"DO UPDATE SET {set_sql}"
        else:
            action_sql = "DO NOTHING"
        sql = (
            f"INSERT INTO {table} ({column_sql}) VALUES ({placeholders}) "
            f"ON CONFLICT ({conflict_sql}) {action_sql}"
        )
        return self.execute(sql, tuple(values))

    def now_text(self):
        if self.backend == "postgres":
            return "CURRENT_TIMESTAMP::text"
        return "datetime('now')"

    def has_table(self, table_name):
        if self.backend == "postgres":
            row = self.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = ?",
                (table_name,),
            ).fetchone()
            return row is not None
        row = self.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?", (table_name,)).fetchone()
        return row is not None

    def has_column(self, table_name, column_name):
        if self.backend == "postgres":
            row = self.execute(
                "SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = ? AND column_name = ?",
                (table_name, column_name),
            ).fetchone()
            return row is not None
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table_name):
            raise ValueError("Unsafe table name")
        rows = self.execute(f"PRAGMA table_info({table_name})").fetchall()
        return any((row["name"] if hasattr(row, "keys") else row[1]) == column_name for row in rows)

    def last_insert_id(self):
        if self.backend == "postgres":
            row = self.execute("SELECT lastval() AS id").fetchone()
        else:
            row = self.execute("SELECT last_insert_rowid() AS id").fetchone()
        return row["id"]

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
    if "?" not in sql:
        return sql
    out = []
    in_single_quote = False
    in_double_quote = False
    i = 0
    while i < len(sql):
        ch = sql[i]
        if ch == "'" and not in_double_quote:
            if in_single_quote and i + 1 < len(sql) and sql[i + 1] == "'":
                out.append("''")
                i += 2
                continue
            in_single_quote = not in_single_quote
            out.append(ch)
        elif ch == '"' and not in_single_quote:
            in_double_quote = not in_double_quote
            out.append(ch)
        elif ch == "?" and not in_single_quote and not in_double_quote:
            out.append("%s")
        else:
            out.append(ch)
        i += 1
    return "".join(out)


def _escape_psycopg_percent_literals(sql):
    """Escape percent signs that are not valid psycopg placeholders."""
    if "%" not in sql:
        return sql

    out = []
    i = 0
    while i < len(sql):
        ch = sql[i]
        if ch != "%":
            out.append(ch)
            i += 1
            continue

        if i + 1 < len(sql):
            nxt = sql[i + 1]
            if nxt in {"s", "b", "t", "%"}:
                out.append("%" + nxt)
                i += 2
                continue

        out.append("%%")
        i += 1

    return "".join(out)


def rewrite_sql(backend, sql, params):
    rewritten_sql = sql
    rewritten_params = params

    if backend == "postgres":
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

        rewritten_sql = _escape_psycopg_percent_literals(rewritten_sql)

        if rewritten_params is None:
            rewritten_params = ()
        elif not isinstance(rewritten_params, (tuple, list, dict)):
            rewritten_params = (rewritten_params,)

    return rewritten_sql, rewritten_params


def parse_database_config(database_path=None, prefer_test_database_url=False):
    db_url = ""
    if prefer_test_database_url:
        db_url = os.environ.get("TEST_DATABASE_URL", "").strip()
    if not db_url:
        db_url = os.environ.get("DATABASE_URL", "").strip()
    if is_postgres_url(db_url):
        parsed = urlparse(db_url)
        db_name = parsed.path.lstrip("/") or "postgres"
        if prefer_test_database_url and db_name == "expense_tracker":
            raise RuntimeError(
                "Unsafe test database configuration: tests cannot run against the live 'expense_tracker' database. "
                "Set TEST_DATABASE_URL to the dedicated test database."
            )
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
