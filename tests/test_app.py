from pathlib import Path

import pytest

from expense_tracker import create_app


@pytest.fixture()
def app(tmp_path: Path):
    db_path = tmp_path / "test.sqlite"
    app = create_app({"TESTING": True, "SECRET_KEY": "test", "DATABASE": str(db_path)})

    with app.app_context():
        app.init_db()

    yield app


@pytest.fixture()
def client(app):
    return app.test_client()


def register(client, username="user1", password="password"):
    return client.post("/register", data={"username": username, "password": password}, follow_redirects=True)


def login(client, username="user1", password="password"):
    return client.post("/login", data={"username": username, "password": password}, follow_redirects=True)


def test_register_login_logout(client):
    response = register(client)
    assert b"Registration successful" in response.data

    response = login(client)
    assert b"Dashboard" in response.data

    response = client.get("/logout", follow_redirects=True)
    assert b"Login" in response.data


def test_category_expense_crud_and_export(client):
    register(client)
    login(client)

    cat_response = client.post("/categories", data={"name": "Health"}, follow_redirects=True)
    assert b"Category added" in cat_response.data

    with client.application.app_context():
        db = client.application.get_db()
        category_id = db.execute("SELECT id FROM categories WHERE name = 'Health'").fetchone()["id"]

    add_response = client.post(
        "/expenses/new",
        data={
            "date": "2026-01-15",
            "amount": "12.50",
            "category_id": str(category_id),
            "description": "Medicine",
        },
        follow_redirects=True,
    )
    assert b"Expense added" in add_response.data
    assert b"Medicine" in add_response.data

    with client.application.app_context():
        db = client.application.get_db()
        expense_id = db.execute("SELECT id FROM expenses WHERE description = 'Medicine'").fetchone()["id"]

    edit_response = client.post(
        f"/expenses/{expense_id}/edit",
        data={"date": "2026-01-16", "amount": "15.00", "category_id": "", "description": "Updated"},
        follow_redirects=True,
    )
    assert b"Expense updated" in edit_response.data

    csv_response = client.get("/export/csv")
    assert csv_response.status_code == 200
    assert b"date,amount,category,description" in csv_response.data
    assert b"Updated" in csv_response.data

    delete_response = client.post(f"/expenses/{expense_id}/delete", follow_redirects=True)
    assert b"Expense deleted" in delete_response.data


def test_dashboard_month_filter(client):
    register(client)
    login(client)

    client.post(
        "/expenses/new",
        data={"date": "2026-02-01", "amount": "20", "category_id": "", "description": "A"},
        follow_redirects=True,
    )
    client.post(
        "/expenses/new",
        data={"date": "2026-03-01", "amount": "30", "category_id": "", "description": "B"},
        follow_redirects=True,
    )

    response = client.get("/dashboard?month=2026-02")
    assert b"$20.00" in response.data
    assert b"$30.00" not in response.data
