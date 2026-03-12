import os
import sys
from urllib.parse import urlparse

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from expense_tracker.db import connect_db, parse_database_config
from expense_tracker.db_migrations import apply_migrations


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
    return parsed._replace(path=f"/{db_name}").geturl()


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
        raise RuntimeError("Unsafe test database configuration: TEST_DATABASE_URL must be set for tests.")
    if not url.startswith(("postgres://", "postgresql://")):
        raise RuntimeError("Unsafe test database configuration: TEST_DATABASE_URL must use postgres:// or postgresql://.")
    assert_not_live_database(url, "TEST_DATABASE_URL")
    if _postgres_db_name(url) != TEST_DB_NAME:
        raise RuntimeError(
            f"Unsafe test database configuration: TEST_DATABASE_URL must point to '{TEST_DB_NAME}'."
        )
    return url


def pytest_sessionstart(session):
    get_test_postgres_url()


def reset_postgres_tables(db):
    config_name = db.config.get("database_name") if hasattr(db, "config") else None
    if config_name != TEST_DB_NAME:
        raise RuntimeError(
            f"Refusing to truncate tables on database '{config_name}'. Tests may only cleanup '{TEST_DB_NAME}'."
        )
    for table in POSTGRES_CLEANUP_TABLES:
        db.execute(f"DELETE FROM {table}")
    db.commit()


@pytest.fixture(scope="session")
def postgres_test_database_url():
    return get_test_postgres_url()


@pytest.fixture()
def postgres_test_database(postgres_test_database_url):
    config = parse_database_config(prefer_test_database_url=True)
    apply_migrations(config)
    with connect_db(config) as db:
        reset_postgres_tables(db)
    yield postgres_test_database_url
