#!/usr/bin/env python3
"""One-time migration tool to copy Expense Tracker data from SQLite to Postgres.

This script is intentionally standalone so it can run in Docker containers
without importing application modules.
"""

import os
import sqlite3
from collections import Counter
from datetime import datetime
from pathlib import Path

import psycopg
from psycopg import sql

TABLE_ORDER = [
    "users",
    "households",
    "household_members",
    "household_invites",
    "categories",
    "category_rules",
    "expenses",
    "audit_logs",
    "settlement_payments",
    "import_staging",
]

OPTIONAL_TABLES = {"settlement_payments", "import_staging"}
VERIFICATION_SAMPLE_SIZE = 10


def resolve_sqlite_path():
    env_path = os.environ.get("SQLITE_PATH", "").strip()
    if env_path:
        return Path(env_path)

    docker_default = Path("/app/instance/expense_tracker.sqlite")
    if docker_default.exists():
        return docker_default

    return Path("instance/expense_tracker.sqlite")


def sqlite_table_exists(conn, table_name):
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def pg_table_exists(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = current_schema() AND table_name = %s
            """,
            (table_name,),
        )
        return cur.fetchone() is not None


def sqlite_columns(conn, table_name):
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return [row[1] for row in rows]


def pg_columns(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = current_schema() AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table_name,),
        )
        return [row[0] for row in cur.fetchall()]


def pg_column_metadata(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                column_name,
                is_nullable,
                data_type,
                udt_name,
                column_default
            FROM information_schema.columns
            WHERE table_schema = current_schema() AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table_name,),
        )
        return [
            {
                "column_name": row[0],
                "is_nullable": row[1],
                "data_type": row[2],
                "udt_name": row[3],
                "column_default": row[4],
            }
            for row in cur.fetchall()
        ]


def _is_empty(value):
    return value is None or (isinstance(value, str) and value.strip() == "")


def default_for(column_name, meta):
    normalized = column_name.lower()
    data_type = (meta.get("data_type") or "").lower()

    if any(token in normalized for token in ("created_at", "updated_at", "applied_at", "last_used")):
        return datetime.utcnow().isoformat()

    if "enabled" in normalized:
        return 1

    if "reviewed" in normalized:
        return 0

    if any(token in normalized for token in ("hits", "confidence", "priority")):
        return 0

    numeric_types = {
        "smallint",
        "integer",
        "bigint",
        "real",
        "double precision",
        "numeric",
        "decimal",
    }
    if data_type in numeric_types:
        return 0

    if data_type == "boolean":
        return 0

    if data_type in {"text", "character varying", "character"}:
        print(
            f"Warning: defaulting required text column '{column_name}' to empty string; "
            "review data quality if this is unexpected."
        )
        return ""

    return None


def count_rows_sqlite(conn, table_name):
    return int(conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])


def count_rows_pg(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(sql.SQL("SELECT COUNT(*) FROM {} ").format(sql.Identifier(table_name)))
        return int(cur.fetchone()[0])


def validate_target_schema(pg_conn):
    missing_required = [
        table_name
        for table_name in TABLE_ORDER
        if table_name not in OPTIONAL_TABLES and not pg_table_exists(pg_conn, table_name)
    ]
    if missing_required:
        missing = ", ".join(missing_required)
        raise SystemExit(
            "Postgres schema is missing required tables: "
            f"{missing}. Start the app once to run migrations, then rerun this script."
        )


def copy_table(sqlite_conn, pg_conn, table_name):
    if not sqlite_table_exists(sqlite_conn, table_name):
        if table_name in OPTIONAL_TABLES:
            return {
                "source_rows": 0,
                "copied_rows": 0,
                "status": "skipped (missing in SQLite; optional)",
            }
        raise RuntimeError(f"Required source table is missing in SQLite: {table_name}")

    if not pg_table_exists(pg_conn, table_name):
        if table_name in OPTIONAL_TABLES:
            return {
                "source_rows": 0,
                "copied_rows": 0,
                "status": "skipped (missing in Postgres; optional)",
            }
        raise RuntimeError(f"Required destination table is missing in Postgres: {table_name}")

    src_cols = sqlite_columns(sqlite_conn, table_name)
    dst_column_info = pg_column_metadata(pg_conn, table_name)
    dst_cols = [col["column_name"] for col in dst_column_info]
    common_cols = [col for col in src_cols if col in dst_cols]

    if not common_cols:
        if table_name in OPTIONAL_TABLES:
            return {
                "source_rows": count_rows_sqlite(sqlite_conn, table_name),
                "copied_rows": 0,
                "status": "skipped (no common columns; optional)",
            }
        raise RuntimeError(f"No common columns found for table '{table_name}'")

    select_sql = f"SELECT {', '.join(common_cols)} FROM {table_name}"
    source_rows = sqlite_conn.execute(select_sql).fetchall()

    pg_meta = {
        col["column_name"]: {
            "is_nullable": col["is_nullable"],
            "data_type": col["data_type"],
            "column_default": col["column_default"],
        }
        for col in dst_column_info
    }
    not_null_cols = [col for col, meta in pg_meta.items() if meta["is_nullable"] == "NO"]

    insert_cols = list(common_cols)
    for required_col in not_null_cols:
        if required_col not in insert_cols:
            insert_cols.append(required_col)

    rows = []
    filled_counts = Counter()
    source_null_counts = Counter()
    for source_row in source_rows:
        row_map = dict(source_row)
        row_values = []
        for col in insert_cols:
            val = row_map.get(col)
            if col in not_null_cols and _is_empty(val):
                source_null_counts[col] += 1
                val = default_for(col, pg_meta[col])
                if _is_empty(val):
                    raise RuntimeError(
                        "Cannot determine non-null default for NOT NULL column "
                        f"'{col}' in table '{table_name}'."
                    )
                filled_counts[col] += 1
            row_values.append(val)
        rows.append(tuple(row_values))

    for col in not_null_cols:
        if source_null_counts[col] > 0:
            print(f"{table_name}: filled {source_null_counts[col]} missing {col}")

    if not rows:
        return {"source_rows": 0, "copied_rows": 0, "status": "copied"}

    col_list = sql.SQL(", ").join(sql.Identifier(col) for col in insert_cols)
    value_placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in insert_cols)
    insert_sql = sql.SQL(
        "INSERT INTO {table} ({columns}) VALUES ({values}) ON CONFLICT DO NOTHING"
    ).format(
        table=sql.Identifier(table_name),
        columns=col_list,
        values=value_placeholders,
    )

    with pg_conn.cursor() as cur:
        cur.executemany(insert_sql, rows)

    if filled_counts:
        columns = ", ".join(sorted(filled_counts))
        total_filled = sum(filled_counts.values())
        print(f"Filled {total_filled} missing NOT NULL values in columns: {columns} (table: {table_name})")

    return {"source_rows": len(rows), "copied_rows": len(rows), "status": "copied"}


def reset_sequence(pg_conn, table_name):
    if not pg_table_exists(pg_conn, table_name):
        return

    if "id" not in pg_columns(pg_conn, table_name):
        return

    with pg_conn.cursor() as cur:
        cur.execute("SELECT pg_get_serial_sequence(%s, 'id')", (table_name,))
        seq_name = cur.fetchone()[0]
        if not seq_name:
            return

        cur.execute(sql.SQL("SELECT COALESCE(MAX(id), 0) FROM {} ").format(sql.Identifier(table_name)))
        max_id = int(cur.fetchone()[0])

        if max_id == 0:
            cur.execute("SELECT setval(%s, 1, false)", (seq_name,))
        else:
            cur.execute("SELECT setval(%s, %s, true)", (seq_name, max_id))


def verify_counts(sqlite_conn, pg_conn, table_names):
    mismatches = []
    for table_name in table_names:
        if not sqlite_table_exists(sqlite_conn, table_name) or not pg_table_exists(pg_conn, table_name):
            continue
        src_count = count_rows_sqlite(sqlite_conn, table_name)
        dst_count = count_rows_pg(pg_conn, table_name)
        if src_count != dst_count:
            mismatches.append((table_name, src_count, dst_count))
    return mismatches


def print_sample_emails(pg_conn):
    if not pg_table_exists(pg_conn, "users"):
        print("Sample user emails: users table not found")
        return

    email_column = "email" if "email" in pg_columns(pg_conn, "users") else "username"
    query = sql.SQL("SELECT {} FROM users ORDER BY id LIMIT %s").format(sql.Identifier(email_column))
    with pg_conn.cursor() as cur:
        cur.execute(query, (VERIFICATION_SAMPLE_SIZE,))
        rows = cur.fetchall()

    samples = [row[0] for row in rows if row[0]]
    print(f"Sample user emails ({email_column}): {samples}")


def main():
    sqlite_path = resolve_sqlite_path()
    if not sqlite_path.exists():
        raise SystemExit(
            "SQLite database not found at "
            f"{sqlite_path}. Set SQLITE_PATH to the correct source file."
        )

    database_url = os.environ.get("DATABASE_URL", "").strip()
    if not database_url:
        raise SystemExit("DATABASE_URL is required and must point to Postgres")
    if not database_url.startswith(("postgres://", "postgresql://")):
        raise SystemExit("DATABASE_URL must start with postgres:// or postgresql://")

    print(f"Using SQLite source: {sqlite_path}")
    print("Validating Postgres schema...")

    sqlite_conn = sqlite3.connect(str(sqlite_path))
    sqlite_conn.row_factory = sqlite3.Row

    with psycopg.connect(database_url) as pg_conn:
        validate_target_schema(pg_conn)

        copied_tables = []
        summary = {}

        for table_name in TABLE_ORDER:
            result = copy_table(sqlite_conn, pg_conn, table_name)
            summary[table_name] = result
            if result["status"] == "copied":
                copied_tables.append(table_name)

        for table_name in copied_tables:
            reset_sequence(pg_conn, table_name)

        mismatches = verify_counts(sqlite_conn, pg_conn, TABLE_ORDER)

        print("\nMigration summary:")
        for table_name in TABLE_ORDER:
            row = summary[table_name]
            print(
                f"- {table_name}: source={row['source_rows']} copied={row['copied_rows']} status={row['status']}"
            )

        if mismatches:
            print("\nCount verification found differences (often expected when Postgres already has data):")
            for table_name, src_count, dst_count in mismatches:
                print(f"- {table_name}: sqlite={src_count}, postgres={dst_count}")
        else:
            print("\nCount verification passed for migrated tables.")

        print_sample_emails(pg_conn)

    sqlite_conn.close()


if __name__ == "__main__":
    main()
