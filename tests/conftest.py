import os
import sys
from urllib.parse import urlparse
from urllib.parse import urlunparse

import pytest

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

LIVE_DB_NAME = "expense_tracker"
TEST_DB_NAME = "expense_tracker_test"


def _postgres_db_name(database_url):
    parsed = urlparse((database_url or "").strip())
    return parsed.path.lstrip("/")


def _postgres_url_with_db_name(database_url, db_name):
    parsed = urlparse((database_url or "").strip())
    return urlunparse(parsed._replace(path=f"/{db_name}"))


def assert_not_live_database(database_url, env_var_name):
    db_name = _postgres_db_name(database_url)
    if db_name == LIVE_DB_NAME:
        raise RuntimeError(
            f"Unsafe test database configuration: {env_var_name} points to live database '{LIVE_DB_NAME}'. "
            f"Use '{TEST_DB_NAME}' instead."
        )


def get_test_postgres_url():
    url = os.environ.get("TEST_DATABASE_URL", "").strip()
    if not url:
        runtime_url = os.environ.get("DATABASE_URL", "").strip()
        if runtime_url.startswith(("postgres://", "postgresql://")):
            url = _postgres_url_with_db_name(runtime_url, TEST_DB_NAME)
            os.environ["TEST_DATABASE_URL"] = url
    if not url:
        pytest.skip("Postgres URL not configured (set TEST_DATABASE_URL)")
    if not url.startswith(("postgres://", "postgresql://")):
        pytest.skip("TEST_DATABASE_URL must be a postgres:// or postgresql:// URL")
    assert_not_live_database(url, "TEST_DATABASE_URL")
    return url


def pytest_sessionstart(session):
    test_database_url = os.environ.get("TEST_DATABASE_URL", "").strip()
    if test_database_url.startswith(("postgres://", "postgresql://")):
        assert_not_live_database(test_database_url, "TEST_DATABASE_URL")


def reset_postgres_tables(db):
    config_name = db.config.get("database_name") if hasattr(db, "config") else None
    if config_name != TEST_DB_NAME:
        raise RuntimeError(
            f"Refusing to truncate tables on database '{config_name}'. Tests may only cleanup '{TEST_DB_NAME}'."
        )
    for table in POSTGRES_CLEANUP_TABLES:
        db.execute(f"DELETE FROM {table}")
    db.commit()
