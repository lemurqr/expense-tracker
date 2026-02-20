from pathlib import Path
import json

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

    dashboard_response = client.get("/dashboard?month=2026-01")
    assert b"Medicine" in dashboard_response.data

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


def test_import_cibc_headerless_csv(client):
    register(client)
    login(client)

    fixture = Path(__file__).parent / "fixtures" / "cibc_headerless.csv"
    with fixture.open("rb") as f:
        preview_response = client.post(
            "/import/csv",
            data={"action": "preview", "csv_file": (f, "cibc_headerless.csv")},
            content_type="multipart/form-data",
        )

    assert b"Detected format: <strong>headerless</strong>" in preview_response.data
    assert b"Coffee Shop" in preview_response.data
    assert b"Payroll" in preview_response.data

    parsed_rows = [
        {
            "user_id": 1,
            "date": "2026-01-10",
            "amount": -5.5,
            "description": "Coffee Shop",
            "normalized_description": "coffee shop",
            "category": "",
        },
        {
            "user_id": 1,
            "date": "2026-01-11",
            "amount": 1200.0,
            "description": "Payroll",
            "normalized_description": "payroll",
            "category": "",
        },
    ]
    confirm_response = client.post(
        "/import/csv",
        data={"action": "confirm", "parsed_rows": json.dumps(parsed_rows)},
        follow_redirects=True,
    )
    assert b"Imported 2 transaction(s)." in confirm_response.data

    with client.application.app_context():
        db = client.application.get_db()
        rows = db.execute(
            "SELECT date, amount, description FROM expenses ORDER BY date ASC"
        ).fetchall()
    assert rows[0]["amount"] == -5.5
    assert rows[1]["amount"] == 1200.0


def test_import_cp1252_csv_fallback(client):
    register(client)
    login(client)

    fixture = Path(__file__).parent / "fixtures" / "cp1252_import.csv"
    with fixture.open("rb") as f:
        preview_response = client.post(
            "/import/csv",
            data={"action": "preview", "csv_file": (f, "cp1252_import.csv")},
            content_type="multipart/form-data",
        )

    assert preview_response.status_code == 200
    assert "Caf" in preview_response.get_data(as_text=True)


def test_import_header_based_csv_with_mapping(client):
    register(client)
    login(client)

    fixture = Path(__file__).parent / "fixtures" / "header_based.csv"
    with fixture.open("rb") as f:
        preview_response = client.post(
            "/import/csv",
            data={
                "action": "preview",
                "map_date": "0",
                "map_description": "1",
                "map_amount": "",
                "map_debit": "2",
                "map_credit": "3",
                "map_category": "4",
                "csv_file": (f, "header_based.csv"),
            },
            content_type="multipart/form-data",
        )

    assert b"Detected format: <strong>header</strong>" in preview_response.data
    assert b"Grocery Store" in preview_response.data

    parsed_rows = [
        {
            "user_id": 1,
            "date": "2026-02-01",
            "amount": -45.1,
            "description": "Grocery Store",
            "normalized_description": "grocery store",
            "category": "Food",
        },
        {
            "user_id": 1,
            "date": "2026-02-02",
            "amount": 12.34,
            "description": "Refund",
            "normalized_description": "refund",
            "category": "Other",
        },
    ]
    confirm_response = client.post(
        "/import/csv",
        data={"action": "confirm", "parsed_rows": json.dumps(parsed_rows)},
        follow_redirects=True,
    )
    assert b"Imported 2 transaction(s)." in confirm_response.data

    duplicate_response = client.post(
        "/import/csv",
        data={"action": "confirm", "parsed_rows": json.dumps(parsed_rows)},
        follow_redirects=True,
    )
    assert b"Imported 0 transaction(s)." in duplicate_response.data


def test_transfer_and_personal_excluded_from_shared_totals(client):
    register(client)
    login(client)

    with client.application.app_context():
        db = client.application.get_db()
        grocery_id = db.execute("SELECT id FROM categories WHERE name = 'Groceries'").fetchone()["id"]
        transfer_id = db.execute("SELECT id FROM categories WHERE name = 'Transfers'").fetchone()["id"]
        personal_id = db.execute("SELECT id FROM categories WHERE name = 'Personal'").fetchone()["id"]

    client.post(
        "/expenses/new",
        data={"date": "2026-04-01", "amount": "100", "category_id": str(grocery_id), "description": "IGA"},
        follow_redirects=True,
    )
    client.post(
        "/expenses/new",
        data={"date": "2026-04-02", "amount": "40", "category_id": str(personal_id), "description": "Spa day"},
        follow_redirects=True,
    )
    client.post(
        "/expenses/new",
        data={"date": "2026-04-03", "amount": "300", "category_id": str(transfer_id), "description": "Transfer to savings"},
        follow_redirects=True,
    )

    response = client.get("/dashboard?month=2026-04")
    text = response.get_data(as_text=True)
    assert "Total spending (includes Personal, excludes Transfers):</strong> $140.00" in text
    assert "Shared spending (excludes Personal + Transfers):</strong> $100.00" in text


def test_refund_keeps_original_category_not_transfer(client):
    register(client)
    login(client)

    parsed_rows = [
        {
            "user_id": 1,
            "date": "2026-05-01",
            "amount": 18.5,
            "description": "Refund from grocery store",
            "normalized_description": "refund from grocery store",
            "category": "Groceries",
        }
    ]
    client.post(
        "/import/csv",
        data={"action": "confirm", "parsed_rows": json.dumps(parsed_rows)},
        follow_redirects=True,
    )

    with client.application.app_context():
        db = client.application.get_db()
        row = db.execute(
            """
            SELECT c.name as category, e.is_transfer
            FROM expenses e
            LEFT JOIN categories c ON c.id = e.category_id
            WHERE e.description = 'Refund from grocery store'
            """
        ).fetchone()

    assert row["category"] == "Groceries"
    assert row["is_transfer"] == 0


def test_legacy_category_mapping_and_transfer_mapping_on_import(client):
    register(client)
    login(client)

    parsed_rows = [
        {
            "user_id": 1,
            "date": "2026-06-01",
            "amount": -35.0,
            "description": "Weekly market",
            "normalized_description": "weekly market",
            "category": "Food",
        },
        {
            "user_id": 1,
            "date": "2026-06-02",
            "amount": -125.0,
            "description": "Payment thank you",
            "normalized_description": "payment thank you",
            "category": "",
        },
    ]

    client.post(
        "/import/csv",
        data={"action": "confirm", "parsed_rows": json.dumps(parsed_rows)},
        follow_redirects=True,
    )

    with client.application.app_context():
        db = client.application.get_db()
        rows = db.execute(
            """
            SELECT e.description, c.name as category, e.is_transfer
            FROM expenses e
            LEFT JOIN categories c ON c.id = e.category_id
            ORDER BY e.date ASC
            """
        ).fetchall()

    assert rows[0]["category"] == "Groceries"
    assert rows[0]["is_transfer"] == 0
    assert rows[1]["category"] == "Transfers"
    assert rows[1]["is_transfer"] == 1
