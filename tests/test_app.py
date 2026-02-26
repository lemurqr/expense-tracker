from pathlib import Path
import io
import json
import sqlite3
from datetime import datetime, timedelta

import pytest

from expense_tracker import (
    create_app,
    infer_category,
    normalize_text,
    extract_pattern,
    parse_csv_transactions,
    derive_vendor,
    detect_header_and_mapping,
)


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


def stage_import_preview(client, rows, preview_id="preview-1", created_at=None):
    timestamp = created_at or datetime.utcnow().isoformat()
    with client.application.app_context():
        db = client.application.get_db()
        db.execute("DELETE FROM import_staging WHERE import_id = ?", (preview_id,))
        for row in rows:
            db.execute(
                """
                INSERT INTO import_staging (import_id, household_id, user_id, created_at, row_json, status)
                VALUES (?, ?, ?, ?, ?, 'preview')
                """,
                (preview_id, 1, 1, timestamp, json.dumps(row)),
            )
        db.commit()
    return preview_id


def confirm_import(client, rows, **form_data):
    import_id = stage_import_preview(client, rows)
    payload = {"action": "confirm", "import_id": import_id}
    payload.update(form_data)
    return client.post("/import/csv", data=payload, follow_redirects=True)


def test_register_login_logout(client):
    response = register(client)
    assert b"Registration successful" in response.data

    with client.application.app_context():
        db = client.application.get_db()
        user = db.execute("SELECT password_hash FROM users WHERE username = ?", ("user1",)).fetchone()
    assert user is not None
    assert user["password_hash"] != "password"
    assert user["password_hash"].startswith("scrypt:")

    response = login(client)
    assert b"Dashboard" in response.data

    response = client.get("/logout", follow_redirects=True)
    assert b"Login" in response.data


def test_login_rejects_incorrect_password(client):
    register(client)

    response = login(client, password="wrong-password")

    assert b"Incorrect username or password." in response.data


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


def test_dashboard_date_range_filter_and_totals(client):
    register(client)
    login(client)

    client.post(
        "/expenses/new",
        data={"date": "2026-01-31", "amount": "-10", "category_id": "", "description": "Outside"},
        follow_redirects=True,
    )
    client.post(
        "/expenses/new",
        data={"date": "2026-02-10", "amount": "-20", "category_id": "", "description": "Inside A"},
        follow_redirects=True,
    )
    client.post(
        "/expenses/new",
        data={"date": "2026-03-05", "amount": "-30", "category_id": "", "description": "Inside B"},
        follow_redirects=True,
    )

    response = client.get("/dashboard?start=2026-02-01&end=2026-03-31")
    assert b"Inside A" in response.data
    assert b"Inside B" in response.data
    assert b"Outside" not in response.data
    assert b"$-50.00" in response.data


def test_export_csv_respects_date_range(client):
    register(client)
    login(client)

    client.post(
        "/expenses/new",
        data={"date": "2026-02-01", "amount": "-10", "category_id": "", "description": "In CSV"},
        follow_redirects=True,
    )
    client.post(
        "/expenses/new",
        data={"date": "2026-04-01", "amount": "-11", "category_id": "", "description": "Out CSV"},
        follow_redirects=True,
    )

    csv_response = client.get("/export/csv?start=2026-02-01&end=2026-03-01")
    assert csv_response.status_code == 200
    assert b"In CSV" in csv_response.data
    assert b"Out CSV" not in csv_response.data


def test_settlement_respects_date_range(client):
    register(client)
    login(client)

    client.post(
        "/expenses/new",
        data={"date": "2026-02-10", "amount": "-40", "paid_by": "DK", "category_id": "", "description": "Shared DK"},
        follow_redirects=True,
    )
    client.post(
        "/expenses/new",
        data={"date": "2026-02-12", "amount": "-10", "paid_by": "YZ", "category_id": "", "description": "Shared YZ"},
        follow_redirects=True,
    )
    client.post(
        "/expenses/new",
        data={"date": "2026-04-12", "amount": "-100", "paid_by": "YZ", "category_id": "", "description": "Outside"},
        follow_redirects=True,
    )

    response = client.get("/dashboard?start=2026-02-01&end=2026-02-28")
    assert b"DK shared paid: $40.00" in response.data
    assert b"YZ shared paid: $10.00" in response.data


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
    confirm_response = confirm_import(client, parsed_rows)
    assert b"Imported 2 transaction(s)." in confirm_response.data

    with client.application.app_context():
        db = client.application.get_db()
        rows = db.execute(
            "SELECT date, amount, description FROM expenses ORDER BY date ASC"
        ).fetchall()
    assert rows[0]["amount"] == -5.5
    assert rows[1]["amount"] == 1200.0


def test_detect_cibc_headerless_with_extra_columns_and_trailing_empties():
    rows = [["2026-01-10", "Coffee Shop", "5.50", "", "CARD123", "", ""]]

    has_header, mapping, header_row_index = detect_header_and_mapping(rows)

    assert has_header is False
    assert mapping["date"] == "0"
    assert mapping["description"] == "1"
    assert mapping["debit"] == "2"
    assert mapping["credit"] == "3"
    assert header_row_index == 0


def test_detect_cibc_headerless_when_only_credit_column_is_numeric():
    rows = [["2026-01-11", "Payroll", "", "1200.00", "EXTRA"]]

    has_header, mapping, header_row_index = detect_header_and_mapping(rows)

    assert has_header is False
    assert mapping["date"] == "0"
    assert mapping["description"] == "1"
    assert mapping["debit"] == "2"
    assert mapping["credit"] == "3"
    assert header_row_index == 0


def test_import_cibc_headerless_uses_first_non_empty_row_for_detection(client):
    register(client)
    login(client)

    csv_content = "\n\n2026-01-10,Coffee Shop,5.50,,1234\n"
    preview_response = client.post(
        "/import/csv",
        data={"action": "preview", "csv_file": (io.BytesIO(csv_content.encode("utf-8")), "cibc.csv")},
        content_type="multipart/form-data",
    )

    assert preview_response.status_code == 200
    assert b"Coffee Shop" in preview_response.data


def test_import_csv_persists_mapping_in_session_after_preview(client):
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

    assert preview_response.status_code == 200

    with client.session_transaction() as session_data:
        saved_mapping = session_data.get("csv_mapping")

    assert saved_mapping["date_col"] == "0"
    assert saved_mapping["desc_col"] == "1"
    assert saved_mapping["debit_col"] == "2"
    assert saved_mapping["credit_col"] == "3"


def test_import_csv_cibc_auto_detection_overrides_saved_mapping(client):
    register(client)
    login(client)

    with client.session_transaction() as session_data:
        session_data["csv_mapping"] = {
            "date_col": "2",
            "desc_col": "3",
            "amount_col": "1",
            "debit_col": "",
            "credit_col": "",
            "vendor_col": "",
            "category_col": "",
            "has_header": True,
            "detected_format": "header",
        }

    fixture = Path(__file__).parent / "fixtures" / "cibc_headerless.csv"
    with fixture.open("rb") as f:
        preview_response = client.post(
            "/import/csv",
            data={"action": "preview", "csv_file": (f, "cibc_headerless.csv")},
            content_type="multipart/form-data",
        )

    assert b"Coffee Shop" in preview_response.data

    with client.session_transaction() as session_data:
        saved_mapping = session_data.get("csv_mapping")

    assert saved_mapping["date_col"] == "0"
    assert saved_mapping["desc_col"] == "1"
    assert saved_mapping["debit_col"] == "2"
    assert saved_mapping["credit_col"] == "3"
    assert saved_mapping["detected_format"] == "cibc_headerless"



def test_import_csv_auto_maps_headerless_with_extra_columns_and_shows_note(client):
    register(client)
    login(client)

    csv_content = "2026-01-10,Coffee Shop,5.50,,****1234\n"
    preview_response = client.post(
        "/import/csv",
        data={"action": "preview", "csv_file": (io.BytesIO(csv_content.encode("utf-8")), "cibc-extra.csv")},
        content_type="multipart/form-data",
    )

    assert preview_response.status_code == 200
    assert b"Coffee Shop" in preview_response.data
    assert b"Auto-mapped CIBC headerless format" in preview_response.data


def test_import_csv_get_prefills_saved_mapping_for_user(client):
    register(client)
    login(client)

    with client.session_transaction() as session_data:
        session_data["csv_mapping_by_user"] = {
            "1": {
                "date_col": "0",
                "desc_col": "1",
                "amount_col": "",
                "debit_col": "2",
                "credit_col": "3",
                "vendor_col": "",
                "category_col": "",
                "has_header": False,
                "detected_format": "cibc_headerless",
            }
        }

    response = client.get("/import/csv")

    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert '<option value="0" selected>Column 1</option>' in html
    assert '<option value="1" selected>Column 2</option>' in html


def test_import_confirm_apply_same_vendor_learns_single_vendor_rule(client):
    register(client)
    login(client)

    parsed_rows = [
        {
            "user_id": 1,
            "date": "2026-01-10",
            "amount": -5.5,
            "description": "Coffee order one",
            "normalized_description": "coffee order one",
            "vendor": "Coffee Shop Montreal",
            "category": "",
            "auto_category": "",
        },
        {
            "user_id": 1,
            "date": "2026-01-11",
            "amount": -8.25,
            "description": "Coffee order two",
            "normalized_description": "coffee order two",
            "vendor": "Coffee Shop Montreal",
            "category": "",
            "auto_category": "",
        },
    ]

    confirm_response = confirm_import(client, parsed_rows, override_category_0="Restaurants", override_category_1="Restaurants")

    assert b"Imported 2 transaction(s)." in confirm_response.data

    with client.application.app_context():
        db = client.application.get_db()
        rule_count = db.execute(
            """
            SELECT COUNT(*) as count
            FROM category_rules
            WHERE user_id = ? AND key_type = ? AND pattern = ?
            """,
            (1, "vendor", "coffee shop montreal"),
        ).fetchone()["count"]

    assert rule_count == 1


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

    assert b"Detected format: <strong>headered</strong>" in preview_response.data
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
    confirm_response = confirm_import(client, parsed_rows)
    assert b"Imported 2 transaction(s)." in confirm_response.data

    duplicate_response = confirm_import(client, parsed_rows)
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
    confirm_import(client, parsed_rows)

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

    confirm_import(client, parsed_rows)

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
    assert rows[1]["category"] == "Credit Card Payments"
    assert rows[1]["is_transfer"] == 1


def test_accent_insensitive_cafe_maps_to_bakery_and_coffee():
    category = infer_category("Café latte", "", ["Bakery & Coffee", "Restaurants"])
    assert category == "Bakery & Coffee"


def test_openai_maps_to_personal():
    category = infer_category("OpenAI monthly", "", ["Personal", "Subscriptions"])
    assert category == "Personal"


def test_apple_bill_maps_to_subscriptions_case_insensitive():
    category = infer_category("APPLE.COM/BILL", "", ["Subscriptions", "Electronics"])
    assert category == "Subscriptions"


def test_apple_store_prefers_electronics_then_general_shopping_fallback():
    preferred = infer_category("APPLE STORE TORONTO", "", ["Electronics", "General Shopping"])
    fallback = infer_category("APPLE ONLINE STORE", "", ["General Shopping"])
    assert preferred == "Electronics"
    assert fallback == "General Shopping"


def test_metro_maps_to_groceries():
    category = infer_category("METRO", "", ["Groceries", "General Shopping"])
    assert category == "Groceries"


def test_normalize_text_is_accent_and_punctuation_insensitive():
    assert normalize_text("  Café,   Dépôt!!  ") == "cafe depot"


def test_stoplist_prevents_learning_generic_patterns(client):
    register(client)
    login(client)

    with client.application.app_context():
        db = client.application.get_db()
        groceries_id = db.execute("SELECT id FROM categories WHERE name = 'Groceries'").fetchone()["id"]

    add = client.post(
        "/expenses/new",
        data={"date": "2026-07-01", "amount": "25", "category_id": "", "description": "shop"},
        follow_redirects=True,
    )
    assert b"Expense added" in add.data

    with client.application.app_context():
        db = client.application.get_db()
        expense_id = db.execute("SELECT id FROM expenses WHERE description = 'shop'").fetchone()["id"]

    client.post(
        f"/expenses/{expense_id}/edit",
        data={"date": "2026-07-01", "amount": "25", "category_id": str(groceries_id), "description": "shop"},
        follow_redirects=True,
    )

    with client.application.app_context():
        db = client.application.get_db()
        rule = db.execute("SELECT * FROM category_rules WHERE pattern = 'shop'").fetchone()
    assert rule is None


def test_learn_rule_create_and_update_via_manual_edits(client):
    register(client)
    login(client)

    with client.application.app_context():
        db = client.application.get_db()
        groceries_id = db.execute("SELECT id FROM categories WHERE name = 'Groceries'").fetchone()["id"]
        subscriptions_id = db.execute("SELECT id FROM categories WHERE name = 'Subscriptions'").fetchone()["id"]

    client.post(
        "/expenses/new",
        data={"date": "2026-07-02", "amount": "10", "category_id": "", "description": "Apple"},
        follow_redirects=True,
    )

    with client.application.app_context():
        db = client.application.get_db()
        expense_id = db.execute("SELECT id FROM expenses WHERE description = 'Apple'").fetchone()["id"]

    client.post(
        f"/expenses/{expense_id}/edit",
        data={"date": "2026-07-02", "amount": "10", "category_id": str(groceries_id), "description": "Apple"},
        follow_redirects=True,
    )

    with client.application.app_context():
        db = client.application.get_db()
        rule = db.execute("SELECT pattern, category_id, source FROM category_rules WHERE pattern = ?", (extract_pattern("Apple"),)).fetchone()
    assert rule["category_id"] == groceries_id
    assert rule["source"] == "manual_edit"

    client.post(
        f"/expenses/{expense_id}/edit",
        data={"date": "2026-07-02", "amount": "10", "category_id": str(subscriptions_id), "description": "Apple"},
        follow_redirects=True,
    )

    with client.application.app_context():
        db = client.application.get_db()
        updated = db.execute("SELECT category_id FROM category_rules WHERE pattern = ?", (extract_pattern("Apple"),)).fetchone()
    assert updated["category_id"] == subscriptions_id


def test_categorizer_prefers_learned_rule_before_heuristics(client):
    register(client)
    login(client)

    with client.application.app_context():
        db = client.application.get_db()
        subscriptions_id = db.execute("SELECT id FROM categories WHERE name = 'Subscriptions'").fetchone()["id"]
        electronics_id = db.execute("SELECT id FROM categories WHERE name = 'Electronics'").fetchone()["id"]

    client.post(
        "/expenses/new",
        data={"date": "2026-07-03", "amount": "99", "category_id": str(subscriptions_id), "description": "Apple Store Downtown"},
        follow_redirects=True,
    )

    with client.application.app_context():
        db = client.application.get_db()
        expense_id = db.execute("SELECT id FROM expenses WHERE description = 'Apple Store Downtown'").fetchone()["id"]

    client.post(
        f"/expenses/{expense_id}/edit",
        data={"date": "2026-07-03", "amount": "99", "category_id": str(electronics_id), "description": "Apple Store Downtown"},
        follow_redirects=True,
    )

    preview = confirm_import(
        client,
        [{"user_id": 1, "date": "2026-07-04", "amount": -15.0, "description": "Apple Store Downtown", "normalized_description": "apple store downtown", "category": ""}],
    )
    assert b"Imported 1 transaction(s)." in preview.data

    with client.application.app_context():
        db = client.application.get_db()
        row = db.execute(
            """
            SELECT c.name as category FROM expenses e
            LEFT JOIN categories c ON c.id = e.category_id
            WHERE e.description = 'Apple Store Downtown' AND e.date = '2026-07-04'
            """
        ).fetchone()
        assert row["category"] == "Electronics"


def test_import_learning_integration_apple_to_subscriptions(client):
    register(client)
    login(client)

    first_import = confirm_import(
        client,
        [{"user_id": 1, "date": "2026-08-01", "amount": -9.99, "description": "Apple", "normalized_description": "apple", "category": ""}],
        override_category_0="Subscriptions",
    )
    assert b"Imported 1 transaction(s)." in first_import.data

    second_import = confirm_import(
        client,
        [{"user_id": 1, "date": "2026-08-02", "amount": -9.99, "description": "Apple", "normalized_description": "apple", "category": ""}],
    )
    assert b"Imported 1 transaction(s)." in second_import.data

    with client.application.app_context():
        db = client.application.get_db()
        row = db.execute(
            """
            SELECT c.name as category
            FROM expenses e
            LEFT JOIN categories c ON c.id = e.category_id
            WHERE e.description = 'Apple' AND e.date = '2026-08-02'
            """
        ).fetchone()
        rule = db.execute("SELECT hits, source FROM category_rules WHERE pattern = ?", ("apple",)).fetchone()

    assert row["category"] == "Subscriptions"
    assert rule["source"] == "import_override"
    assert rule["hits"] >= 1


def test_signed_amount_from_debit_credit_mapping():
    rows = [
        ["2026-09-01", "Coffee Shop", "5.20", ""],
        ["2026-09-02", "Refund", "", "11.25"],
    ]
    mapping = {"date": "0", "description": "1", "amount": "", "debit": "2", "credit": "3", "vendor": "", "category": ""}
    parsed, skipped = parse_csv_transactions(rows, mapping, user_id=1)
    assert skipped == 0
    assert parsed[0]["amount"] == -5.2
    assert parsed[1]["amount"] == 11.25


def test_amex_amount_is_normalized_to_canonical_sign_for_charges():
    rows = [["2026-09-10", "Restaurant", "20.00"]]
    mapping = {"date": "0", "description": "1", "amount": "2", "debit": "", "credit": "", "vendor": "", "category": ""}
    parsed, skipped = parse_csv_transactions(rows, mapping, user_id=1, bank_type="amex")
    assert skipped == 0
    assert parsed[0]["amount"] == -20.0


def test_amex_payment_amount_embedded_in_description_is_extracted_and_cleaned():
    rows = [["2026-09-11", "Online payment -162.67", ""]]
    mapping = {"date": "0", "description": "1", "amount": "2", "debit": "", "credit": "", "vendor": "", "category": ""}
    parsed, skipped = parse_csv_transactions(rows, mapping, user_id=1, bank_type="amex")

    assert skipped == 0
    assert parsed[0]["amount"] == 162.67
    assert parsed[0]["description"] == "Online payment"
    assert parsed[0]["category"] == "Credit Card Payments"


def test_vendor_mapped_column_is_stored_on_import(client):
    register(client)
    login(client)

    parsed_rows = [
        {
            "user_id": 1,
            "date": "2026-09-03",
            "amount": -25.0,
            "description": "POS PURCHASE METRO 1123",
            "vendor": "Metro",
            "normalized_description": "pos purchase metro 1123",
            "category": "",
        }
    ]
    confirm_import(client, parsed_rows)

    with client.application.app_context():
        db = client.application.get_db()
        row = db.execute("SELECT vendor FROM expenses WHERE date = '2026-09-03'").fetchone()
    assert row["vendor"] == "Metro"


def test_vendor_derived_from_description_when_missing():
    assert derive_vendor("POS PURCHASE TIM HORTONS 88991") == "tim hortons"


def test_vendor_first_learning_and_reuse(client):
    register(client)
    login(client)

    first_import = confirm_import(
        client,
        [{"user_id": 1, "date": "2026-09-04", "amount": -7.0, "description": "POS PURCHASE TIM HORTONS 101", "vendor": "Tim Hortons", "normalized_description": "pos purchase tim hortons 101", "category": ""}],
        override_category_0="Bakery & Coffee",
    )
    assert b"Imported 1 transaction(s)." in first_import.data

    with client.application.app_context():
        db = client.application.get_db()
        rule = db.execute("SELECT key_type, pattern FROM category_rules WHERE source = 'import_override' ORDER BY id DESC LIMIT 1").fetchone()
    assert rule["key_type"] == "vendor"

    second_import = confirm_import(
        client,
        [{"user_id": 1, "date": "2026-09-05", "amount": -8.0, "description": "TIM HORTONS #55", "vendor": "Tim Hortons", "normalized_description": "tim hortons 55", "category": ""}],
    )
    assert b"Imported 1 transaction(s)." in second_import.data

    with client.application.app_context():
        db = client.application.get_db()
        row = db.execute(
            """
            SELECT c.name as category
            FROM expenses e
            LEFT JOIN categories c ON c.id = e.category_id
            WHERE e.date = '2026-09-05'
            """
        ).fetchone()
    assert row["category"] == "Bakery & Coffee"


def test_learned_vendor_sets_confidence_and_source(client):
    register(client)
    login(client)

    confirm_import(
        client,
        [{"user_id": 1, "date": "2026-10-01", "amount": -8.0, "description": "TIM HORTONS #1", "vendor": "Tim Hortons", "normalized_description": "tim hortons 1", "category": ""}],
        override_category_0="Bakery & Coffee",
    )

    confirm_import(
        client,
        [{"user_id": 1, "date": "2026-10-02", "amount": -9.0, "description": "TIM HORTONS #2", "vendor": "Tim Hortons", "normalized_description": "tim hortons 2", "category": ""}],
    )

    with client.application.app_context():
        db = client.application.get_db()
        row = db.execute("SELECT category_confidence, category_source FROM expenses WHERE date = '2026-10-02'").fetchone()

    assert row["category_confidence"] == 95
    assert row["category_source"] == "learned_vendor"


def test_keyword_vendor_and_description_confidence_scores(client):
    register(client)
    login(client)

    confirm_import(
        client,
        [
            {"user_id": 1, "date": "2026-10-03", "amount": -20.0, "description": "Unknown lunch", "vendor": "metro", "normalized_description": "unknown lunch", "category": ""},
            {"user_id": 1, "date": "2026-10-04", "amount": -30.0, "description": "metro", "vendor": "random vendor", "normalized_description": "metro", "category": ""},
        ],
    )

    with client.application.app_context():
        db = client.application.get_db()
        vendor_row = db.execute("SELECT category_confidence, category_source FROM expenses WHERE date = '2026-10-03'").fetchone()
        description_row = db.execute("SELECT category_confidence, category_source FROM expenses WHERE date = '2026-10-04'").fetchone()

    assert vendor_row["category_confidence"] == 75
    assert vendor_row["category_source"] == "keyword_vendor"
    assert description_row["category_confidence"] == 65
    assert description_row["category_source"] == "keyword_description"


def test_transfer_sets_source_transfer_and_confidence_100(client):
    register(client)
    login(client)

    confirm_import(
        client,
        [{"user_id": 1, "date": "2026-10-05", "amount": -125.0, "description": "Payment thank you", "normalized_description": "payment thank you", "category": ""}],
    )

    dashboard = client.get("/dashboard?month=2026-10")

    with client.application.app_context():
        db = client.application.get_db()
        row = db.execute("SELECT category_confidence, category_source FROM expenses WHERE date = '2026-10-05'").fetchone()

    assert row["category_confidence"] == 100
    assert row["category_source"] == "transfer"
    assert b'confidence-transfer">Transfer<' in dashboard.data


def test_preview_renders_confidence_badges(client):
    register(client)
    login(client)

    csv_content = "date,description,debit,credit\n2026-10-06,TIM HORTONS,8.00,\n"
    preview_response = client.post(
        "/import/csv",
        data={"action": "preview", "csv_file": (io.BytesIO(csv_content.encode("utf-8")), "preview.csv")},
        content_type="multipart/form-data",
    )

    html = preview_response.get_data(as_text=True)
    assert "Legend:" in html
    assert "confidence-badge" in html
    assert "Source" in html

def test_apply_same_vendor_endpoint_updates_preview_state(client):
    register(client)
    login(client)

    import_id = stage_import_preview(
        client,
        [
            {"row_index": 0, "description": "Coffee 1", "vendor": "Coffee Shop", "vendor_key": "coffee shop", "category": "", "confidence": 25, "suggested_source": "unknown"},
            {"row_index": 1, "description": "Coffee 2", "vendor": "Coffee Shop", "vendor_key": "coffee shop", "category": "", "confidence": 25, "suggested_source": "unknown"},
            {"row_index": 2, "description": "Other", "vendor": "Book Store", "vendor_key": "book store", "category": "", "confidence": 25, "suggested_source": "unknown"},
        ],
    )

    response = client.post('/import/csv/apply_override', json={"match_type": "vendor", "match_key": "coffee shop", "category_name": "Restaurants", "import_id": import_id})
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["updated_count"] == 2
    assert {item["row_index"] for item in payload["updated_rows"]} == {0, 1}

    with client.application.app_context():
        db = client.application.get_db()
        rows = [json.loads(item["row_json"]) for item in db.execute("SELECT row_json FROM import_staging WHERE import_id = ? ORDER BY id", (import_id,)).fetchall()]
    assert rows[0]["override_category"] == "Restaurants"
    assert rows[1]["override_category"] == "Restaurants"
    assert rows[2].get("override_category", "") == ""




def test_amex_headered_preview_auto_maps_expected_columns(client):
    register(client)
    login(client)

    fixture = Path(__file__).parent / "fixtures" / "amex_headered.csv"
    with fixture.open("rb") as f:
        preview_response = client.post(
            "/import/csv",
            data={"action": "preview", "csv_file": (f, "amex_headered.csv")},
            content_type="multipart/form-data",
        )

    text = preview_response.get_data(as_text=True)
    assert preview_response.status_code == 200
    assert "Detected format: <strong>headered</strong> · detected_format: <strong>headered</strong>" in text
    assert "auto_mapped_fields: date=<strong>Date</strong>, description=<strong>Description</strong>, amount=<strong>Amount</strong>, vendor=<strong>Merchant</strong>, debit=<strong>None</strong>, credit=<strong>None</strong>" in text

    with client.session_transaction() as sess:
        mapping = sess["csv_mapping"]
    assert mapping["date_col"] == "0"
    assert mapping["desc_col"] == "2"
    assert mapping["amount_col"] == "3"
    assert mapping["vendor_col"] == "4"
    assert mapping["debit_col"] == ""
    assert mapping["credit_col"] == ""


def test_amex_header_auto_mapping_with_summary_rows(client):
    register(client)
    login(client)

    fixture = Path(__file__).parent / "fixtures" / "amex_with_summary.csv"
    with fixture.open("rb") as f:
        preview_response = client.post(
            "/import/csv",
            data={"action": "preview", "csv_file": (f, "amex.csv")},
            content_type="multipart/form-data",
        )

    text = preview_response.get_data(as_text=True)
    assert preview_response.status_code == 200
    assert "header_row_index: <strong>2</strong>" in text
    assert "RESTAURANT XYZ" in text
    assert "ONLINE PAYMENT" in text

    with client.session_transaction() as sess:
        mapping = sess["csv_mapping"]
    assert mapping["date_col"] == "0"
    assert mapping["desc_col"] == "2"
    assert mapping["amount_col"] == "3"
    assert mapping["vendor_col"] == "4"
    assert mapping["debit_col"] == ""
    assert mapping["credit_col"] == ""


def _insert_expense(client, *, date, amount, category, paid_by=None, is_transfer=0):
    with client.application.app_context():
        db = client.application.get_db()
        user_id = db.execute("SELECT id FROM users WHERE username = 'user1'").fetchone()["id"]
        category_row = db.execute(
            "SELECT id FROM categories WHERE user_id = ? AND name = ?",
            (user_id, category),
        ).fetchone()
        db.execute(
            """
            INSERT INTO expenses (user_id, date, amount, category_id, description, paid_by, is_transfer, is_personal)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                date,
                amount,
                category_row["id"] if category_row else None,
                f"{category} test",
                paid_by if paid_by is not None else "",
                is_transfer,
                1 if category == "Personal" else 0,
            ),
        )
        db.commit()


def test_household_settlement_pet_rule(client):
    register(client)
    login(client)

    _insert_expense(client, date="2026-01-02", amount=-100, category="Pet Food & Care", paid_by="DK")
    _insert_expense(client, date="2026-01-03", amount=-60, category="Pet Food & Care", paid_by="YZ")

    response = client.get("/dashboard?month=2026-01")
    text = response.get_data(as_text=True)

    assert "Pet paid by DK (reimbursed by YZ): $100.00" in text
    assert "Pet paid by YZ (not shared): $60.00" in text
    assert "Result:</strong> YZ owes DK $100.00" in text


def test_household_settlement_combined_netting(client):
    register(client)
    login(client)

    _insert_expense(client, date="2026-02-02", amount=-70, category="Groceries", paid_by="DK")
    _insert_expense(client, date="2026-02-03", amount=-130, category="Groceries", paid_by="YZ")
    _insert_expense(client, date="2026-02-04", amount=-100, category="Pet Food & Care", paid_by="DK")

    response = client.get("/dashboard?month=2026-02")
    text = response.get_data(as_text=True)

    assert "DK shared paid: $70.00" in text
    assert "YZ shared paid: $130.00" in text
    assert "Result:</strong> YZ owes DK $70.00" in text


def test_household_settlement_excludes_personal_transfer_and_credit_card_payments(client):
    register(client)
    login(client)

    _insert_expense(client, date="2026-03-02", amount=-40, category="Personal", paid_by="DK")
    _insert_expense(client, date="2026-03-03", amount=-70, category="Credit Card Payments", paid_by="DK")
    _insert_expense(client, date="2026-03-04", amount=-60, category="Groceries", paid_by="DK", is_transfer=1)
    _insert_expense(client, date="2026-03-05", amount=-20, category="Groceries", paid_by="DK")

    response = client.get("/dashboard?month=2026-03")
    text = response.get_data(as_text=True)

    assert "DK shared paid: $20.00" in text
    assert "Shared total: $20.00" in text


def test_household_settlement_missing_paid_by_warning(client):
    register(client)
    login(client)

    _insert_expense(client, date="2026-04-02", amount=-10, category="Groceries", paid_by=None)
    _insert_expense(client, date="2026-04-03", amount=-15, category="Pet Food & Care", paid_by="")
    _insert_expense(client, date="2026-04-04", amount=-20, category="Personal", paid_by=None)

    response = client.get("/dashboard?month=2026-04")
    text = response.get_data(as_text=True)

    assert "⚠ 2 transactions missing Paid by — settlement may be incomplete." in text


def test_import_preview_applies_default_paid_by_when_column_missing(client):
    register(client)
    login(client)

    csv_content = "Date,Description,Debit,Credit\n2026-01-10,Coffee,12.00,\n"
    response = client.post(
        "/import/csv",
        data={
            "action": "preview",
            "import_default_paid_by": "YZ",
            "csv_file": (io.BytesIO(csv_content.encode("utf-8")), "default.csv"),
        },
        content_type="multipart/form-data",
    )

    assert response.status_code == 200
    assert 'name="override_paid_by_0"' in response.get_data(as_text=True)
    with client.application.app_context():
        db = client.application.get_db()
        row = db.execute(
            "SELECT row_json FROM import_staging ORDER BY id DESC LIMIT 1"
        ).fetchone()
    assert json.loads(row["row_json"])["paid_by"] == "YZ"


def test_import_confirm_uses_per_row_paid_by_override(client):
    register(client)
    login(client)

    parsed_rows = [
        {
            "user_id": 1,
            "row_index": 0,
            "date": "2026-02-10",
            "amount": -10.0,
            "description": "Coffee",
            "normalized_description": "coffee",
            "vendor": "Coffee",
            "category": "",
            "paid_by": "",
        }
    ]
    confirm_import(client, parsed_rows, override_paid_by_0="YZ")

    with client.application.app_context():
        db = client.application.get_db()
        row = db.execute("SELECT paid_by FROM expenses WHERE description = 'Coffee'").fetchone()
    assert row["paid_by"] == "YZ"


def test_manual_add_edit_paid_by_saved_and_shown_on_dashboard(client):
    register(client)
    login(client)

    add_response = client.post(
        "/expenses/new",
        data={"date": "2026-03-05", "amount": "21", "category_id": "", "description": "Manual", "paid_by": "DK"},
        follow_redirects=True,
    )
    assert b"Expense added" in add_response.data

    with client.application.app_context():
        db = client.application.get_db()
        expense_id = db.execute("SELECT id FROM expenses WHERE description = 'Manual'").fetchone()["id"]

    edit_response = client.post(
        f"/expenses/{expense_id}/edit",
        data={"date": "2026-03-05", "amount": "21", "category_id": "", "description": "Manual", "paid_by": "YZ"},
        follow_redirects=True,
    )
    assert b"Expense updated" in edit_response.data

    dashboard = client.get("/dashboard?month=2026-03")
    assert b"Manual" in dashboard.data
    assert b">YZ<" in dashboard.data


def test_import_confirm_blocks_missing_paid_by_for_spending_rows(client):
    register(client)
    login(client)

    parsed_rows = [
        {
            "user_id": 1,
            "row_index": 0,
            "date": "2026-04-10",
            "amount": -25.0,
            "description": "No payer",
            "normalized_description": "no payer",
            "vendor": "No payer",
            "category": "",
            "paid_by": "",
        }
    ]

    response = confirm_import(client, parsed_rows, import_default_paid_by="")
    assert b"Cannot import spending rows with missing Paid by" in response.data

    with client.application.app_context():
        db = client.application.get_db()
        count = db.execute("SELECT COUNT(*) as c FROM expenses WHERE description = 'No payer'").fetchone()["c"]
    assert count == 0


def test_edit_expense_accepts_negative_amount(client):
    register(client)
    login(client)

    client.post(
        "/expenses/new",
        data={"date": "2026-11-01", "amount": "15", "category_id": "", "description": "To edit"},
        follow_redirects=True,
    )

    with client.application.app_context():
        db = client.application.get_db()
        expense_id = db.execute("SELECT id FROM expenses WHERE description = 'To edit'").fetchone()["id"]

    response = client.post(
        f"/expenses/{expense_id}/edit",
        data={"date": "2026-11-02", "amount": "-15.25", "category_id": "", "description": "To edit"},
        follow_redirects=True,
    )
    assert b"Expense updated" in response.data

    with client.application.app_context():
        db = client.application.get_db()
        row = db.execute("SELECT amount FROM expenses WHERE id = ?", (expense_id,)).fetchone()
    assert row["amount"] == -15.25


def test_import_csv_handles_quotes_and_newlines_in_description(client):
    register(client)
    login(client)

    csv_content = 'Date,Description,Amount\n2026-11-01,"Coffee ""Large""\nSecond line",-12.34\n'
    preview = client.post(
        "/import/csv",
        data={"action": "preview", "csv_file": (io.BytesIO(csv_content.encode("utf-8")), "quotes.csv")},
        content_type="multipart/form-data",
    )
    assert preview.status_code == 200

    import_id = preview.get_data(as_text=True).split('name="import_id" value="')[1].split('"', 1)[0]

    response = client.post(
        "/import/csv",
        data={"action": "confirm", "import_id": import_id, "import_default_paid_by": "DK"},
        follow_redirects=True,
    )
    assert b"Imported 1 transaction(s)." in response.data


def test_import_confirm_uses_default_paid_by(client):
    register(client)
    login(client)

    rows = [
        {
            "row_index": 0,
            "user_id": 1,
            "date": "2026-11-03",
            "amount": -20.0,
            "description": "Default paid by",
            "normalized_description": "default paid by",
            "vendor": "Store",
            "category": "",
            "paid_by": "",
        }
    ]

    response = confirm_import(client, rows, import_default_paid_by="YZ")
    assert b"Imported 1 transaction(s)." in response.data

    with client.application.app_context():
        db = client.application.get_db()
        row = db.execute("SELECT paid_by FROM expenses WHERE description = 'Default paid by'").fetchone()
    assert row["paid_by"] == "YZ"


def test_import_preview_expiration_shows_friendly_message(client):
    register(client)
    login(client)

    rows = [{"row_index": 0, "user_id": 1, "date": "2026-11-04", "amount": -3.0, "description": "Expired", "normalized_description": "expired", "category": ""}]
    preview_id = stage_import_preview(client, rows, preview_id="expired-1")
    with client.application.app_context():
        db = client.application.get_db()
        db.execute("DELETE FROM import_staging WHERE import_id = ?", (preview_id,))
        db.commit()

    response = client.post(
        "/import/csv",
        data={"action": "confirm", "import_id": preview_id},
        follow_redirects=True,
    )

    assert b"Preview expired. Please re-upload the file." in response.data


def test_import_confirm_applies_vendor_and_category_overrides(client):
    register(client)
    login(client)

    rows = [
        {
            "row_index": 0,
            "user_id": 1,
            "date": "2026-11-05",
            "amount": -11.0,
            "description": "Override row",
            "normalized_description": "override row",
            "vendor": "Original Vendor",
            "category": "Groceries",
            "auto_category": "Groceries",
            "paid_by": "DK",
        }
    ]

    response = confirm_import(
        client,
        rows,
        override_vendor_0="Updated Vendor",
        override_category_0="Restaurants",
    )
    assert b"Imported 1 transaction(s)." in response.data

    with client.application.app_context():
        db = client.application.get_db()
        row = db.execute(
            """
            SELECT e.vendor, c.name as category
            FROM expenses e
            LEFT JOIN categories c ON c.id = e.category_id
            WHERE e.description = 'Override row'
            """
        ).fetchone()
    assert row["vendor"] == "Updated Vendor"
    assert row["category"] == "Restaurants"



def test_import_confirm_override_can_learn_description_rule(client):
    register(client)
    login(client)

    parsed_rows = [
        {
            "row_index": 0,
            "user_id": 1,
            "date": "2026-10-11",
            "amount": -12.50,
            "description": "Unique Alpha Vendorless Charge",
            "normalized_description": "unique alpha vendorless charge",
            "vendor": " ",
            "vendor_key": "",
            "vendor_rule_key": "",
            "description_rule_key": "unique alpha vendorless",
            "category": "",
            "auto_category": "",
            "paid_by": "DK",
        }
    ]

    response = confirm_import(client, parsed_rows, override_category_0="Restaurants")
    assert b"Imported 1 transaction(s)." in response.data

    with client.application.app_context():
        db = client.application.get_db()
        rule = db.execute(
            "SELECT key_type, pattern FROM category_rules WHERE source = 'import_override' ORDER BY id DESC LIMIT 1"
        ).fetchone()
    assert rule["key_type"] == "description"
    assert rule["pattern"] == "unique alpha vendorless"



def test_single_row_delete_removes_expense_via_row_action(client):
    register(client)
    login(client)

    client.post(
        "/expenses/new",
        data={"date": "2026-02-03", "amount": "42", "category_id": "", "description": "Single Delete Item"},
        follow_redirects=True,
    )

    with client.application.app_context():
        db = client.application.get_db()
        expense_id = db.execute("SELECT id FROM expenses WHERE description = 'Single Delete Item'").fetchone()["id"]

    response = client.post(f"/expenses/{expense_id}/delete", follow_redirects=True)

    assert response.status_code == 200
    assert b"Expense deleted" in response.data

    with client.application.app_context():
        db = client.application.get_db()
        remaining = db.execute("SELECT id FROM expenses WHERE id = ?", (expense_id,)).fetchone()

    assert remaining is None




def test_single_row_delete_via_bulk_endpoint_removes_expense_without_unknown_action(client):
    register(client)
    login(client)

    client.post(
        "/expenses/new",
        data={"date": "2026-02-08", "amount": "12", "category_id": "", "description": "Single Row Bulk Delete", "paid_by": "DK"},
        follow_redirects=True,
    )

    with client.application.app_context():
        db = client.application.get_db()
        expense_id = db.execute(
            "SELECT id FROM expenses WHERE description = 'Single Row Bulk Delete'"
        ).fetchone()["id"]

    response = client.post(
        "/expenses/bulk",
        data={"action": "delete_expense", "expense_id": str(expense_id)},
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"Deleted 1 transactions" in response.data
    assert b"Unknown bulk action" not in response.data

    with client.application.app_context():
        db = client.application.get_db()
        remaining = db.execute("SELECT id FROM expenses WHERE id = ?", (expense_id,)).fetchone()

    assert remaining is None


def test_bulk_delete_removes_multiple_rows_for_same_user(client):
    register(client)
    login(client)

    client.post(
        "/expenses/new",
        data={"date": "2026-02-01", "amount": "10", "category_id": "", "description": "Bulk A", "paid_by": "DK"},
        follow_redirects=True,
    )
    client.post(
        "/expenses/new",
        data={"date": "2026-02-02", "amount": "20", "category_id": "", "description": "Bulk B", "paid_by": "YZ"},
        follow_redirects=True,
    )

    with client.application.app_context():
        db = client.application.get_db()
        ids = [row["id"] for row in db.execute("SELECT id FROM expenses WHERE description IN ('Bulk A', 'Bulk B')").fetchall()]

    response = client.post(
        "/expenses/bulk",
        data={"action": "delete", "month": "2026-02", "selected_ids": [str(v) for v in ids]},
        follow_redirects=True,
    )

    assert b"Deleted 2 transactions" in response.data
    with client.application.app_context():
        db = client.application.get_db()
        count = db.execute("SELECT COUNT(*) as count FROM expenses WHERE description IN ('Bulk A', 'Bulk B')").fetchone()["count"]
    assert count == 0




def test_bulk_delete_with_audit_log_reference_does_not_fail(client):
    register(client)
    login(client)

    client.post(
        "/expenses/new",
        data={"date": "2026-02-07", "amount": "55", "category_id": "", "description": "Delete With Audit"},
        follow_redirects=True,
    )

    with client.application.app_context():
        db = client.application.get_db()
        expense_id = db.execute("SELECT id FROM expenses WHERE description = 'Delete With Audit'").fetchone()["id"]
        db.execute(
            """
            INSERT INTO audit_logs (household_id, user_id, action, entity, entity_id, meta_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (1, 1, "create", "expense", expense_id, '{"source":"test"}'),
        )
        db.commit()

    response = client.post(
        "/expenses/bulk",
        data={"action": "delete", "selected_ids": [str(expense_id)]},
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"Deleted 1 transactions" in response.data

    with client.application.app_context():
        db = client.application.get_db()
        remaining_expense = db.execute("SELECT id FROM expenses WHERE id = ?", (expense_id,)).fetchone()
        audit_row = db.execute(
            "SELECT entity, entity_id FROM audit_logs WHERE entity = 'expense' AND entity_id = ?",
            (expense_id,),
        ).fetchone()

    assert remaining_expense is None
    assert audit_row is not None
    assert audit_row["entity"] == "expense"
    assert audit_row["entity_id"] == expense_id

def test_bulk_update_category_sets_multiple_rows(client):
    register(client)
    login(client)

    client.post("/categories", data={"name": "Bulk Category"}, follow_redirects=True)
    client.post(
        "/expenses/new",
        data={"date": "2026-02-03", "amount": "30", "category_id": "", "description": "Cat A"},
        follow_redirects=True,
    )
    client.post(
        "/expenses/new",
        data={"date": "2026-02-04", "amount": "40", "category_id": "", "description": "Cat B"},
        follow_redirects=True,
    )

    with client.application.app_context():
        db = client.application.get_db()
        category_id = db.execute("SELECT id FROM categories WHERE user_id = 1 AND name = 'Bulk Category'").fetchone()["id"]
        ids = [row["id"] for row in db.execute("SELECT id FROM expenses WHERE description IN ('Cat A', 'Cat B')").fetchall()]

    response = client.post(
        "/expenses/bulk",
        data={"action": "set_category", "category_id": str(category_id), "selected_ids": [str(v) for v in ids]},
        follow_redirects=True,
    )

    assert b"Updated 2 transactions" in response.data
    with client.application.app_context():
        db = client.application.get_db()
        rows = db.execute(
            "SELECT category_id FROM expenses WHERE id IN (?, ?) ORDER BY id",
            (ids[0], ids[1]),
        ).fetchall()
    assert all(row["category_id"] == category_id for row in rows)




def test_bulk_update_paid_by_sets_multiple_rows(client):
    register(client)
    login(client)

    client.post(
        "/expenses/new",
        data={"date": "2026-02-03", "amount": "30", "category_id": "", "description": "Paid A", "paid_by": "DK"},
        follow_redirects=True,
    )
    client.post(
        "/expenses/new",
        data={"date": "2026-02-04", "amount": "40", "category_id": "", "description": "Paid B", "paid_by": "DK"},
        follow_redirects=True,
    )

    with client.application.app_context():
        db = client.application.get_db()
        ids = [row["id"] for row in db.execute("SELECT id FROM expenses WHERE description IN ('Paid A', 'Paid B')").fetchall()]

    response = client.post(
        "/expenses/bulk",
        data={"action": "set_paid_by", "paid_by": "YZ", "selected_ids": [str(v) for v in ids]},
        follow_redirects=True,
    )

    assert b"Updated 2 transactions" in response.data
    with client.application.app_context():
        db = client.application.get_db()
        rows = db.execute("SELECT paid_by FROM expenses WHERE id IN (?, ?) ORDER BY id", (ids[0], ids[1])).fetchall()
    assert all(row["paid_by"] == "YZ" for row in rows)

def test_bulk_actions_prevent_cross_user_modification(client):
    register(client, username="user1", password="password")
    register(client, username="user2", password="password")

    login(client, username="user1", password="password")
    client.post(
        "/expenses/new",
        data={"date": "2026-02-10", "amount": "50", "category_id": "", "description": "Owner Row"},
        follow_redirects=True,
    )
    client.get("/logout")

    login(client, username="user2", password="password")
    client.post(
        "/expenses/new",
        data={"date": "2026-02-11", "amount": "60", "category_id": "", "description": "Other Row"},
        follow_redirects=True,
    )

    with client.application.app_context():
        db = client.application.get_db()
        user1_expense_id = db.execute(
            "SELECT id FROM expenses WHERE description = 'Owner Row'"
        ).fetchone()["id"]
        user2_expense_id = db.execute(
            "SELECT id FROM expenses WHERE description = 'Other Row'"
        ).fetchone()["id"]

    response = client.post(
        "/expenses/bulk",
        data={"action": "delete", "selected_ids": [str(user1_expense_id), str(user2_expense_id)]},
        follow_redirects=True,
    )

    assert b"invalid" in response.data.lower()
    with client.application.app_context():
        db = client.application.get_db()
        owner_row_exists = db.execute("SELECT 1 FROM expenses WHERE id = ?", (user1_expense_id,)).fetchone()
        other_row_exists = db.execute("SELECT 1 FROM expenses WHERE id = ?", (user2_expense_id,)).fetchone()
    assert owner_row_exists is not None
    assert other_row_exists is not None


def test_two_users_in_same_household_see_same_expenses(client):
    register(client, "dk", "password")
    login(client, "dk", "password")

    client.post(
        "/expenses/new",
        data={"date": "2026-02-10", "amount": "-25", "paid_by": "DK", "category_id": "", "description": "Shared Pizza"},
        follow_redirects=True,
    )

    owner_household = client.get("/household")
    assert owner_household.status_code == 200
    create_invite = client.post("/household", data={"invite_email": "yz@example.com"}, follow_redirects=True)
    assert b"Invite created" in create_invite.data

    with client.application.app_context():
        db = client.application.get_db()
        code = db.execute("SELECT code FROM household_invites ORDER BY id DESC LIMIT 1").fetchone()["code"]

    client.get("/logout", follow_redirects=True)
    register(client, "yz", "password")
    login(client, "yz", "password")
    join_response = client.post("/household/join", data={"code": code}, follow_redirects=True)
    assert b"Joined household successfully" in join_response.data

    dashboard = client.get("/dashboard?start=2026-02-01&end=2026-02-28")
    assert b"Shared Pizza" in dashboard.data


def test_user_outside_household_cannot_access_expense_detail(client):
    register(client, "dk2", "password")
    login(client, "dk2", "password")
    client.post(
        "/expenses/new",
        data={"date": "2026-02-10", "amount": "-25", "paid_by": "DK", "category_id": "", "description": "Secret Expense"},
        follow_redirects=True,
    )
    with client.application.app_context():
        db = client.application.get_db()
        expense_id = db.execute("SELECT id FROM expenses WHERE description = 'Secret Expense'").fetchone()["id"]

    client.get("/logout", follow_redirects=True)
    register(client, "outsider", "password")
    login(client, "outsider", "password")

    detail = client.get(f"/expenses/{expense_id}", follow_redirects=True)
    assert b"Expense not found" in detail.data


def test_audit_log_tracks_create_edit_delete_and_import(client):
    register(client, "auditor", "password")
    login(client, "auditor", "password")

    client.post(
        "/expenses/new",
        data={"date": "2026-01-10", "amount": "-12.0", "paid_by": "DK", "category_id": "", "description": "Audit Item"},
        follow_redirects=True,
    )

    with client.application.app_context():
        db = client.application.get_db()
        expense_id = db.execute("SELECT id FROM expenses WHERE description = 'Audit Item'").fetchone()["id"]

    client.post(
        f"/expenses/{expense_id}/edit",
        data={"date": "2026-01-11", "amount": "-13.0", "paid_by": "YZ", "category_id": "", "description": "Audit Item Updated"},
        follow_redirects=True,
    )

    parsed_rows = [
        {
            "user_id": 1,
            "date": "2026-01-12",
            "amount": -5.5,
            "description": "Imported audit",
            "normalized_description": "imported audit",
            "category": "",
            "vendor": "",
            "paid_by": "DK",
        }
    ]
    confirm_import(client, parsed_rows, import_default_paid_by="DK")

    client.post(f"/expenses/{expense_id}/delete", follow_redirects=True)

    detail = client.get(f"/expenses/{expense_id}", follow_redirects=True)
    assert b"Expense not found" in detail.data

    with client.application.app_context():
        db = client.application.get_db()
        actions = [row["action"] for row in db.execute("SELECT action FROM audit_logs ORDER BY id ASC").fetchall()]
    assert "create" in actions
    assert "edit" in actions
    assert "delete" in actions
    assert "import" in actions



def test_rules_route_migrates_legacy_category_rules_columns(tmp_path: Path):
    db_path = tmp_path / "legacy_rules.sqlite"

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                name TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE category_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                pattern TEXT NOT NULL,
                category_id INTEGER NOT NULL,
                priority INTEGER NOT NULL DEFAULT 100
            )
            """
        )
        conn.execute("INSERT INTO users (id, username, password_hash) VALUES (1, 'legacy-user', 'hash')")
        conn.execute("INSERT INTO categories (id, user_id, name) VALUES (1, 1, 'Food')")
        conn.execute("INSERT INTO category_rules (user_id, pattern, category_id, priority) VALUES (1, 'coffee', 1, 100)")
        conn.commit()

    app = create_app({"TESTING": True, "SECRET_KEY": "test", "DATABASE": str(db_path)})
    client = app.test_client()

    with client.session_transaction() as session_data:
        session_data["user_id"] = 1

    response = client.get("/rules")

    assert response.status_code == 200

    with app.app_context():
        db = app.get_db()
        columns = {row["name"] for row in db.execute("PRAGMA table_info(category_rules)").fetchall()}

    assert {"last_used_at", "hits", "created_at", "key_type", "source", "enabled", "is_enabled"}.issubset(columns)
def test_user_password_column_migrates_to_password_hash(tmp_path: Path):
    db_path = tmp_path / "legacy.sqlite"

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password TEXT NOT NULL
            )
            """
        )
        conn.execute("INSERT INTO users (username, password) VALUES (?, ?)", ("legacy-user", "legacy-secret"))
        conn.commit()

    app = create_app({"TESTING": True, "SECRET_KEY": "test", "DATABASE": str(db_path)})
    with app.app_context():
        app.init_db()
        db = app.get_db()
        columns = {row["name"] for row in db.execute("PRAGMA table_info(users)").fetchall()}
        migrated = db.execute(
            "SELECT password, password_hash FROM users WHERE username = ?",
            ("legacy-user",),
        ).fetchone()

    assert "password_hash" in columns
    assert migrated["password_hash"] == "legacy-secret"


def test_app_auto_initializes_database_on_first_request(tmp_path: Path):
    db_path = tmp_path / "fresh" / "expense_tracker.sqlite"
    app = create_app({"TESTING": True, "SECRET_KEY": "test", "DATABASE": str(db_path)})
    client = app.test_client()

    response = client.get("/register")

    assert response.status_code == 200
    assert db_path.exists()

    with app.app_context():
        db = app.get_db()
        tables = {
            row["name"]
            for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }

    assert {"users", "categories", "expenses", "category_rules", "audit_logs"}.issubset(tables)


def test_dev_reset_db_route_requires_debug_flag(client):
    response = client.get("/dev/reset-db")
    assert response.status_code == 404
    assert b"DEV ONLY" in response.data


def test_dev_reset_db_route_recreates_database(tmp_path: Path):
    db_path = tmp_path / "dev" / "expense_tracker.sqlite"
    app = create_app(
        {
            "TESTING": True,
            "DEBUG": True,
            "SECRET_KEY": "test",
            "DATABASE": str(db_path),
        }
    )
    client = app.test_client()

    register(client, "reset-user", "password")

    with app.app_context():
        db = app.get_db()
        user_count_before = db.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"]
    assert user_count_before == 1

    response = client.get("/dev/reset-db", follow_redirects=True)

    assert response.status_code == 200
    assert b"DEV ONLY: database reset complete" in response.data
    assert b"Register" in response.data

    with app.app_context():
        db = app.get_db()
        user_count_after = db.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"]
    assert user_count_after == 0


def test_import_preview_creates_staging_rows_and_returns_import_id(client):
    register(client)
    login(client)

    csv_content = "Date,Description,Debit,Credit\n2026-01-10,Coffee,12.00,\n"
    response = client.post(
        "/import/csv",
        data={"action": "preview", "csv_file": (io.BytesIO(csv_content.encode("utf-8")), "preview.csv")},
        content_type="multipart/form-data",
    )

    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert 'name="import_id" value="' in html
    import_id = html.split('name="import_id" value="')[1].split('"', 1)[0]

    with client.application.app_context():
        db = client.application.get_db()
        count = db.execute("SELECT COUNT(*) AS c FROM import_staging WHERE import_id = ?", (import_id,)).fetchone()["c"]
    assert count == 1


def test_import_confirm_works_when_session_cleared(client):
    register(client)
    login(client)

    csv_content = "Date,Description,Debit,Credit\n2026-01-10,Coffee,12.00,\n"
    preview = client.post(
        "/import/csv",
        data={"action": "preview", "csv_file": (io.BytesIO(csv_content.encode("utf-8")), "session-clear.csv")},
        content_type="multipart/form-data",
    )
    import_id = preview.get_data(as_text=True).split('name="import_id" value="')[1].split('"', 1)[0]

    with client.session_transaction() as sess:
        sess.clear()
        sess["user_id"] = 1

    confirm = client.post(
        "/import/csv",
        data={"action": "confirm", "import_id": import_id, "import_default_paid_by": "DK"},
        follow_redirects=True,
    )
    assert b"Imported 1 transaction(s)." in confirm.data

    with client.application.app_context():
        db = client.application.get_db()
        count = db.execute("SELECT COUNT(*) AS c FROM expenses WHERE description = 'Coffee'").fetchone()["c"]
    assert count == 1


def test_import_confirm_deletes_staging_rows_after_import(client):
    register(client)
    login(client)

    rows = [{"row_index": 0, "user_id": 1, "date": "2026-11-20", "amount": -9.0, "description": "Cleanup", "normalized_description": "cleanup", "category": "", "paid_by": "DK"}]
    import_id = stage_import_preview(client, rows, preview_id="cleanup-import")

    response = client.post(
        "/import/csv",
        data={"action": "confirm", "import_id": import_id, "import_default_paid_by": "DK"},
        follow_redirects=True,
    )
    assert b"Imported 1 transaction(s)." in response.data

    with client.application.app_context():
        db = client.application.get_db()
        count = db.execute("SELECT COUNT(*) AS c FROM import_staging WHERE import_id = ?", (import_id,)).fetchone()["c"]
    assert count == 0


def test_import_confirm_preview_expired_when_import_id_missing_or_empty(client):
    register(client)
    login(client)

    missing_id_response = client.post(
        "/import/csv",
        data={"action": "confirm", "import_id": "does-not-exist"},
        follow_redirects=True,
    )
    assert b"Preview expired. Please re-upload the file." in missing_id_response.data

    empty_id_response = client.post(
        "/import/csv",
        data={"action": "confirm", "import_id": ""},
        follow_redirects=True,
    )
    assert b"Preview expired. Please re-upload the file." in empty_id_response.data
