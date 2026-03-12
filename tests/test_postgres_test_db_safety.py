import pytest

from expense_tracker.db import parse_database_config

from tests.conftest import (
    TEST_DB_NAME,
    _postgres_url_with_db_name,
    assert_not_live_database,
    get_test_postgres_url,
)


def test_get_test_postgres_url_requires_test_database_url(monkeypatch):
    monkeypatch.delenv("TEST_DATABASE_URL", raising=False)

    with pytest.raises(RuntimeError, match="TEST_DATABASE_URL must be set"):
        get_test_postgres_url()


def test_assert_not_live_database_raises_for_runtime_database_name():
    with pytest.raises(RuntimeError, match="Unsafe test database configuration"):
        assert_not_live_database("postgresql://user:pass@db:5432/expense_tracker", "TEST_DATABASE_URL")


def test_postgres_url_with_db_name_replaces_path_only():
    rewritten = _postgres_url_with_db_name(
        "postgresql://user:pass@db:5432/expense_tracker?sslmode=disable",
        TEST_DB_NAME,
    )

    assert rewritten == "postgresql://user:pass@db:5432/expense_tracker_test?sslmode=disable"


def test_get_test_postgres_url_rejects_non_test_database_name(monkeypatch):
    monkeypatch.setenv("TEST_DATABASE_URL", "postgresql://user:pass@db:5432/other_db")

    with pytest.raises(RuntimeError, match="must point to 'expense_tracker_test'"):
        get_test_postgres_url()


def test_parse_database_config_prefers_test_database_url(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@db:5432/expense_tracker")
    monkeypatch.setenv("TEST_DATABASE_URL", "postgresql://user:pass@db:5432/expense_tracker_test")

    config = parse_database_config(prefer_test_database_url=True)

    assert config["database_name"] == TEST_DB_NAME


def test_parse_database_config_raises_when_testing_uses_live_database(monkeypatch):
    monkeypatch.setenv("TEST_DATABASE_URL", "postgresql://user:pass@db:5432/expense_tracker")

    with pytest.raises(RuntimeError, match="Unsafe test database configuration"):
        parse_database_config(prefer_test_database_url=True)
