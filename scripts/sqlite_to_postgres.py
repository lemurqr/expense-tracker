#!/usr/bin/env python3
"""One-time migration tool to copy Expense Tracker data from SQLite to Postgres."""

import argparse
import os
import sqlite3
from pathlib import Path

from expense_tracker.db import connect_db, parse_database_config
from expense_tracker.db_migrations import apply_migrations

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


def sqlite_table_exists(conn, table_name):
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def pg_table_exists(conn, table_name):
    row = conn.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def sqlite_columns(conn, table_name):
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return [row[1] for row in rows]


def pg_columns(conn, table_name):
    rows = conn.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = current_schema() AND table_name = ?
        ORDER BY ordinal_position
        """,
        (table_name,),
    ).fetchall()
    return [row[0] for row in rows]


def count_rows(conn, table_name):
    return int(conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])


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
    dst_cols = pg_columns(pg_conn, table_name)
    common_cols = [col for col in src_cols if col in dst_cols]

    if not common_cols:
        if table_name in OPTIONAL_TABLES:
            return {
                "source_rows": count_rows(sqlite_conn, table_name),
                "copied_rows": 0,
                "status": "skipped (no common columns; optional)",
            }
        raise RuntimeError(f"No common columns found for table '{table_name}'")

    select_sql = f"SELECT {', '.join(common_cols)} FROM {table_name}"
    rows = sqlite_conn.execute(select_sql).fetchall()

    if not rows:
        return {"source_rows": 0, "copied_rows": 0, "status": "copied"}

    placeholders = ", ".join(["?"] * len(common_cols))
    insert_sql = (
        f"INSERT INTO {table_name} ({', '.join(common_cols)}) "
        f"VALUES ({placeholders})"
    )

    pg_conn.execute(insert_sql, rows[0])
    for row in rows[1:]:
        pg_conn.execute(insert_sql, row)

    return {"source_rows": len(rows), "copied_rows": len(rows), "status": "copied"}


def reset_sequence(pg_conn, table_name):
    if not pg_table_exists(pg_conn, table_name):
        return

    has_id = "id" in pg_columns(pg_conn, table_name)
    if not has_id:
        return

    pg_conn.execute(
        """
        SELECT setval(
            pg_get_serial_sequence(?, 'id'),
            COALESCE((SELECT MAX(id) FROM {}), 1),
            (SELECT COUNT(*) > 0 FROM {})
        )
        """.format(table_name, table_name),
        (table_name,),
    )


def verify_counts(sqlite_conn, pg_conn, copied_tables):
    mismatches = []
    for table_name in copied_tables:
        if not sqlite_table_exists(sqlite_conn, table_name) or not pg_table_exists(pg_conn, table_name):
            continue
        src_count = count_rows(sqlite_conn, table_name)
        dst_count = count_rows(pg_conn, table_name)
        if src_count != dst_count:
            mismatches.append((table_name, src_count, dst_count))
    return mismatches


def print_sample_emails(pg_conn):
    if not pg_table_exists(pg_conn, "users"):
        print("Sample user emails: users table not found")
        return

    email_column = "email" if "email" in pg_columns(pg_conn, "users") else "username"
    rows = pg_conn.execute(
        f"SELECT {email_column} FROM users ORDER BY id LIMIT ?",
        (VERIFICATION_SAMPLE_SIZE,),
    ).fetchall()
    samples = [row[0] for row in rows if row[0]]
    print(f"Sample user emails ({email_column}): {samples}")


def main():
    parser = argparse.ArgumentParser(description="One-time migration from SQLite to Postgres")
    parser.add_argument(
        "--sqlite-path",
        default="instance/expense_tracker.sqlite",
        help="Path to source SQLite DB (default: instance/expense_tracker.sqlite)",
    )
    args = parser.parse_args()

    sqlite_path = Path(args.sqlite_path)
    if not sqlite_path.exists():
        raise SystemExit(f"SQLite database not found at: {sqlite_path}")

    database_url = os.environ.get("DATABASE_URL", "").strip()
    if not database_url:
        raise SystemExit("DATABASE_URL is required and must point to Postgres")

    pg_config = parse_database_config(None)
    if pg_config["backend"] != "postgres":
        raise SystemExit("DATABASE_URL must be a postgres:// or postgresql:// URL")

    print("Applying Postgres migrations...")
    apply_migrations(pg_config)

    sqlite_conn = sqlite3.connect(str(sqlite_path))
    try:
        pg_conn = connect_db(pg_config)
        copied_tables = []
        summary = {}

        try:
            pg_conn.execute("BEGIN")
            for table_name in TABLE_ORDER:
                result = copy_table(sqlite_conn, pg_conn, table_name)
                summary[table_name] = result
                if result["status"] == "copied":
                    copied_tables.append(table_name)

            for table_name in copied_tables:
                reset_sequence(pg_conn, table_name)

            pg_conn.commit()
        except Exception:
            pg_conn.rollback()
            raise

        print("\nMigration summary:")
        for table_name in TABLE_ORDER:
            row = summary[table_name]
            print(
                f"- {table_name}: source={row['source_rows']} copied={row['copied_rows']} status={row['status']}"
            )

        mismatches = verify_counts(sqlite_conn, pg_conn, TABLE_ORDER)
        if mismatches:
            print("\nCount verification failed:")
            for table_name, src_count, dst_count in mismatches:
                print(f"- {table_name}: sqlite={src_count}, postgres={dst_count}")
        else:
            print("\nCount verification passed for migrated tables.")

        print_sample_emails(pg_conn)

    finally:
        sqlite_conn.close()
        if 'pg_conn' in locals():
            pg_conn.close()


if __name__ == "__main__":
    main()
