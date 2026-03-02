#!/usr/bin/env python3
import argparse
import sqlite3
from pathlib import Path

from expense_tracker.db import connect_db, parse_database_config
from expense_tracker.db_migrations import REQUIRED_TABLES, apply_migrations


def copy_table(sqlite_conn, pg_conn, table):
    rows = sqlite_conn.execute(f"SELECT * FROM {table}").fetchall()
    if not rows:
        return 0
    columns = [desc[0] for desc in sqlite_conn.execute(f"SELECT * FROM {table} LIMIT 1").description]
    placeholders = ", ".join(["?"] * len(columns))
    insert_sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
    for row in rows:
        pg_conn.execute(insert_sql, tuple(row))
    return len(rows)


def reset_sequence(pg_conn, table):
    pg_conn.execute(
        f"SELECT setval(pg_get_serial_sequence('{table}', 'id'), COALESCE(MAX(id), 1), MAX(id) IS NOT NULL) FROM {table}"
    )


def main():
    parser = argparse.ArgumentParser(description="Copy expense tracker data from SQLite to Postgres")
    parser.add_argument("--sqlite-path", default="instance/expense_tracker.sqlite")
    args = parser.parse_args()

    sqlite_path = Path(args.sqlite_path)
    if not sqlite_path.exists():
        raise SystemExit(f"SQLite DB not found: {sqlite_path}")

    db_config = parse_database_config(str(sqlite_path))
    if db_config["backend"] != "postgres":
        raise SystemExit("Set DATABASE_URL to a postgres:// or postgresql:// URL before running this script")

    apply_migrations(db_config)

    src = sqlite3.connect(str(sqlite_path))
    try:
        dst = connect_db(db_config)
        try:
            with dst:
                for table in REQUIRED_TABLES.keys():
                    count = copy_table(src, dst, table)
                    print(f"{table}: copied {count} rows")
                for table in REQUIRED_TABLES.keys():
                    reset_sequence(dst, table)
        finally:
            dst.close()
    finally:
        src.close()


if __name__ == "__main__":
    main()
