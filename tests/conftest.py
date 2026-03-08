import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


POSTGRES_CLEANUP_TABLES = [
    "audit_logs",
    "import_staging",
    "settlement_payments",
    "expenses",
    "category_rules",
    "categories",
    "household_members",
    "household_invites",
    "households",
    "users",
]


def reset_postgres_tables(db):
    for table in POSTGRES_CLEANUP_TABLES:
        db.execute(f"DELETE FROM {table}")
    db.commit()
