from pathlib import Path
import io
import json
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
    with client.session_transaction() as session_data:
        previews = session_data.get("import_preview_by_user") or {}
        previews["1"] = {"preview_id": preview_id, "rows": rows, "created_at": timestamp}
        session_data["import_preview_by_user"] = previews
    return preview_id


def confirm_import(client, rows, **form_data):
    preview_id = stage_import_preview(client, rows)
    payload = {"action": "confirm", "preview_id": preview_id}
    payload.update(form_data)
    return client.post("/import/csv", data=payload, follow_redirects=True)


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

    with client.session_transaction() as sess:
        sess["import_preview_by_user"] = {
            "1": {
                "rows": [
                    {"row_index": 0, "description": "Coffee 1", "vendor": "Coffee Shop", "vendor_key": "coffee shop", "category": "", "confidence": 25, "suggested_source": "unknown"},
                    {"row_index": 1, "description": "Coffee 2", "vendor": "Coffee Shop", "vendor_key": "coffee shop", "category": "", "confidence": 25, "suggested_source": "unknown"},
                    {"row_index": 2, "description": "Other", "vendor": "Book Store", "vendor_key": "book store", "category": "", "confidence": 25, "suggested_source": "unknown"},
                ]
            }
        }

    response = client.post('/import/csv/apply_vendor', json={"vendor_key": "coffee shop", "category_name": "Restaurants"})
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["updated_count"] == 2
    assert {item["row_index"] for item in payload["updated_rows"]} == {0, 1}

    with client.session_transaction() as sess:
        rows = sess["import_preview_by_user"]["1"]["rows"]
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
    with client.session_transaction() as session_data:
        rows = session_data["import_preview_by_user"]["1"]["rows"]
    assert rows[0]["paid_by"] == "YZ"


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

    with client.session_transaction() as session_data:
        preview_id = session_data["import_preview_by_user"]["1"]["preview_id"]

    response = client.post(
        "/import/csv",
        data={"action": "confirm", "preview_id": preview_id, "import_default_paid_by": "DK"},
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
    expired_at = (datetime.utcnow() - timedelta(minutes=31)).isoformat()
    preview_id = stage_import_preview(client, rows, preview_id="expired-1", created_at=expired_at)

    response = client.post(
        "/import/csv",
        data={"action": "confirm", "preview_id": preview_id},
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
