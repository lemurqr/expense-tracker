#!/usr/bin/env python3
import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from expense_tracker.db import parse_database_config
from expense_tracker.db_migrations import apply_migrations, get_db_health


def main():
    parser = argparse.ArgumentParser(description="Check and print DB schema health")
    parser.add_argument("db_path", nargs="?", default="instance/expense_tracker.sqlite", help="Path to SQLite DB (ignored when DATABASE_URL is postgres)")
    parser.add_argument("--migrate", action="store_true", help="Apply migrations before checking")
    args = parser.parse_args()

    config = parse_database_config(args.db_path)
    if args.migrate:
        apply_migrations(config)

    print(json.dumps(get_db_health(config), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
