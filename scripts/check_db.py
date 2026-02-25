#!/usr/bin/env python3
import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from expense_tracker.db_migrations import apply_migrations, get_db_health


def main():
    parser = argparse.ArgumentParser(description="Check and print DB schema health")
    parser.add_argument("db_path", help="Path to SQLite DB")
    parser.add_argument("--migrate", action="store_true", help="Apply migrations before checking")
    args = parser.parse_args()

    if args.migrate:
        apply_migrations(args.db_path)

    print(json.dumps(get_db_health(args.db_path), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
