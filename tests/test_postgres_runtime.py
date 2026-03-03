import json
import os
from datetime import datetime

import pytest

from expense_tracker import create_app


@pytest.fixture(scope="module")
def postgres_url():
    url = os.environ.get("TEST_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not url or not (url.startswith("postgres://") or url.startswith("postgresql://")):
        pytest.skip("Postgres URL not configured (set TEST_DATABASE_URL)")
    return url


@pytest.fixture()
def app(postgres_url, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", postgres_url)
    app = create_app({"TESTING": True, "SECRET_KEY": "test", "DATABASE": "/tmp/ignored.sqlite"})
    with app.app_context():
        app.init_db()
        db = app.get_db()
        for table in [
            "audit_logs",
            "import_staging",
            "expenses",
            "category_rules",
            "categories",
            "household_members",
            "household_invites",
            "households",
            "users",
        ]:
            db.execute(f"DELETE FROM {table}")
        db.commit()
    yield app


@pytest.fixture()
def client(app):
    return app.test_client()


def register(client, username="pguser", password="password"):
    return client.post("/register", data={"username": username, "password": password}, follow_redirects=True)


def login(client, username="pguser", password="password"):
    return client.post("/login", data={"username": username, "password": password}, follow_redirects=True)


def test_postgres_runtime_paths(client):
    register_response = register(client)
    assert register_response.status_code == 200

    login_response = login(client)
    assert login_response.status_code == 200

    health = client.get("/health/db")
    assert health.status_code == 200
    payload = health.get_json()
    assert payload["backend"] == "postgres"
    assert payload["ok"] is True

    with client.application.app_context():
        db = client.application.get_db()
        user = db.execute("SELECT id FROM users WHERE username = ?", ("pguser",)).fetchone()
        category_count = db.execute("SELECT COUNT(*) AS count FROM categories WHERE user_id = ?", (user["id"],)).fetchone()["count"]
        assert category_count > 0

        import_id = "pg-preview"
        db.execute("DELETE FROM import_staging WHERE import_id = ?", (import_id,))
        row_payload = {
            "user_id": user["id"],
            "row_index": 0,
            "date": "2026-03-01",
            "amount": -12.34,
            "description": "PG Merchant",
            "normalized_description": "pg merchant",
            "vendor": "PG Merchant",
            "category": "Groceries",
            "confidence": 90,
            "confidence_label": "High",
            "suggested_source": "rule",
            "vendor_key": "pg merchant",
            "vendor_rule_key": "pg merchant",
            "description_rule_key": "pg merchant",
        }
        db.execute(
            """
            INSERT INTO import_staging (import_id, household_id, user_id, created_at, row_json, status)
            VALUES (?, ?, ?, ?, ?, 'preview')
            """,
            (import_id, 1, user["id"], datetime.utcnow().isoformat(), json.dumps(row_payload)),
        )
        db.commit()

    confirm = client.post("/import/csv", data={"action": "confirm", "import_id": "pg-preview"}, follow_redirects=True)
    assert confirm.status_code == 200
    assert b"Imported 1 transaction" in confirm.data

    settlement = client.get("/settlement")
    assert settlement.status_code == 200

    with client.application.app_context():
        db = client.application.get_db()
        expense = db.execute("SELECT id FROM expenses ORDER BY id DESC LIMIT 1").fetchone()

    bulk_delete = client.post(
        "/expenses/bulk-delete",
        data=json.dumps({"ids": [expense["id"]]}),
        content_type="application/json",
    )
    assert bulk_delete.status_code == 200
    assert bulk_delete.get_json()["ok"] is True


def test_dashboard_loads_with_postgres_rounding(client):
    register_response = register(client, username="pgdash", password="password")
    assert register_response.status_code == 200

    login_response = login(client, username="pgdash", password="password")
    assert login_response.status_code == 200

    with client.application.app_context():
        db = client.application.get_db()
        user = db.execute("SELECT id FROM users WHERE username = ?", ("pgdash",)).fetchone()
        category = db.execute(
            "SELECT id FROM categories WHERE user_id = ? ORDER BY id ASC LIMIT 1", (user["id"],)
        ).fetchone()
        household = db.execute(
            "SELECT household_id FROM household_members WHERE user_id = ?", (user["id"],)
        ).fetchone()["household_id"]

        db.execute(
            """
            INSERT INTO expenses (date, amount, category_id, description, user_id, household_id, paid_by)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("2026-03-01", -10.125, category["id"], "Coffee", user["id"], household, "DK"),
        )
        db.execute(
            """
            INSERT INTO expenses (date, amount, category_id, description, user_id, household_id, paid_by)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("2026-03-02", -20.235, category["id"], "Lunch", user["id"], household, "YZ"),
        )
        db.execute(
            """
            INSERT INTO expenses (date, amount, category_id, description, user_id, household_id, paid_by)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("2026-03-03", -30.345, category["id"], "Groceries", user["id"], household, "DK"),
        )
        db.commit()

    dashboard = client.get("/dashboard")
    assert dashboard.status_code == 200


def _latest_invite_code(db):
    return db.execute("SELECT code FROM household_invites ORDER BY id DESC LIMIT 1").fetchone()["code"]


def test_postgres_household_membership_and_roles(client):
    register(client, username="dk_owner", password="password")
    login(client, username="dk_owner", password="password")

    with client.application.app_context():
        db = client.application.get_db()
        owner = db.execute("SELECT id FROM users WHERE username = ?", ("dk_owner",)).fetchone()
        owner_membership = db.execute(
            "SELECT household_id, role FROM household_members WHERE user_id = ?",
            (owner["id"],),
        ).fetchone()
        assert owner_membership["role"] == "owner"

    self_invite = client.post("/household", data={"invite_email": "dk_owner"}, follow_redirects=True)
    assert b"cannot invite yourself" in self_invite.data.lower()

    with client.application.app_context():
        db = client.application.get_db()
        owner = db.execute("SELECT id FROM users WHERE username = ?", ("dk_owner",)).fetchone()
        owner_membership = db.execute(
            "SELECT role FROM household_members WHERE user_id = ?",
            (owner["id"],),
        ).fetchone()
        assert owner_membership["role"] == "owner"

    client.get("/logout", follow_redirects=True)
    register(client, username="yz_member", password="password")
    login(client, username="yz_member", password="password")
    client.get("/logout", follow_redirects=True)

    login(client, username="dk_owner", password="password")
    invite_response = client.post("/household", data={"invite_email": "yz_member"}, follow_redirects=True)
    assert b"Invite created" in invite_response.data

    with client.application.app_context():
        db = client.application.get_db()
        code = _latest_invite_code(db)

    client.get("/logout", follow_redirects=True)
    login(client, username="yz_member", password="password")
    join_response = client.post("/household/join", data={"code": code}, follow_redirects=True)
    assert b"Joined household successfully" in join_response.data

    with client.application.app_context():
        db = client.application.get_db()
        owner = db.execute("SELECT id FROM users WHERE username = ?", ("dk_owner",)).fetchone()
        member = db.execute("SELECT id FROM users WHERE username = ?", ("yz_member",)).fetchone()
        owner_membership = db.execute(
            "SELECT household_id, role FROM household_members WHERE user_id = ?",
            (owner["id"],),
        ).fetchone()
        member_membership = db.execute(
            "SELECT household_id, role FROM household_members WHERE user_id = ?",
            (member["id"],),
        ).fetchone()
        assert member_membership["household_id"] == owner_membership["household_id"]
        assert member_membership["role"] == "member"

    client.get("/logout", follow_redirects=True)
    login(client, username="dk_owner", password="password")
    duplicate_member_invite = client.post("/household", data={"invite_email": "yz_member"}, follow_redirects=True)
    assert b"already in your household" in duplicate_member_invite.data.lower()
    client.get("/logout", follow_redirects=True)
    login(client, username="yz_member", password="password")

    member_invite = client.post("/household", data={"invite_email": "someone"}, follow_redirects=True)
    assert b"Only household owner can invite members" in member_invite.data

    second_join = client.post("/household/join", data={"code": code}, follow_redirects=True)
    assert b"already a member of this household" in second_join.data.lower()

    with client.application.app_context():
        db = client.application.get_db()
        owner = db.execute("SELECT id FROM users WHERE username = ?", ("dk_owner",)).fetchone()
        owner_role = db.execute("SELECT role FROM household_members WHERE user_id = ?", (owner["id"],)).fetchone()["role"]
        assert owner_role == "owner"

    client.get("/logout", follow_redirects=True)
    login(client, username="dk_owner", password="password")
    owner_invite = client.post("/household", data={"invite_email": "fresh_user"}, follow_redirects=True)
    assert b"Invite created" in owner_invite.data
