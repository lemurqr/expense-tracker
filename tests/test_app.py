from pathlib import Path
import json

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


def test_detect_cibc_headerless_with_extra_columns_and_trailing_empties():
    rows = [["2026-01-10", "Coffee Shop", "5.50", "", "CARD123", "", ""]]

    has_header, mapping = detect_header_and_mapping(rows)

    assert has_header is False
    assert mapping["date"] == "0"
    assert mapping["description"] == "1"
    assert mapping["debit"] == "2"
    assert mapping["credit"] == "3"


def test_detect_cibc_headerless_when_only_credit_column_is_numeric():
    rows = [["2026-01-11", "Payroll", "", "1200.00", "EXTRA"]]

    has_header, mapping = detect_header_and_mapping(rows)

    assert has_header is False
    assert mapping["date"] == "0"
    assert mapping["description"] == "1"
    assert mapping["debit"] == "2"
    assert mapping["credit"] == "3"


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

    preview = client.post(
        "/import/csv",
        data={
            "action": "confirm",
            "parsed_rows": json.dumps([
                {"user_id": 1, "date": "2026-07-04", "amount": -15.0, "description": "Apple Store Downtown", "normalized_description": "apple store downtown", "category": ""}
            ]),
        },
        follow_redirects=True,
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

    first_import = client.post(
        "/import/csv",
        data={
            "action": "confirm",
            "parsed_rows": json.dumps([
                {"user_id": 1, "date": "2026-08-01", "amount": -9.99, "description": "Apple", "normalized_description": "apple", "category": ""}
            ]),
            "override_category_0": "Subscriptions",
        },
        follow_redirects=True,
    )
    assert b"Imported 1 transaction(s)." in first_import.data

    second_import = client.post(
        "/import/csv",
        data={
            "action": "confirm",
            "parsed_rows": json.dumps([
                {"user_id": 1, "date": "2026-08-02", "amount": -9.99, "description": "Apple", "normalized_description": "apple", "category": ""}
            ]),
        },
        follow_redirects=True,
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
    parsed = parse_csv_transactions(rows, mapping, user_id=1)
    assert parsed[0]["amount"] == -5.2
    assert parsed[1]["amount"] == 11.25


def test_amex_amount_is_normalized_to_canonical_sign_for_charges():
    rows = [["2026-09-10", "Restaurant", "20.00"]]
    mapping = {"date": "0", "description": "1", "amount": "2", "debit": "", "credit": "", "vendor": "", "category": ""}
    parsed = parse_csv_transactions(rows, mapping, user_id=1, bank_type="amex")
    assert parsed[0]["amount"] == -20.0


def test_amex_payment_amount_embedded_in_description_is_extracted_and_cleaned():
    rows = [["2026-09-11", "Online payment -162.67", ""]]
    mapping = {"date": "0", "description": "1", "amount": "2", "debit": "", "credit": "", "vendor": "", "category": ""}
    parsed = parse_csv_transactions(rows, mapping, user_id=1, bank_type="amex")

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
    client.post('/import/csv', data={"action": "confirm", "parsed_rows": json.dumps(parsed_rows)}, follow_redirects=True)

    with client.application.app_context():
        db = client.application.get_db()
        row = db.execute("SELECT vendor FROM expenses WHERE date = '2026-09-03'").fetchone()
    assert row["vendor"] == "Metro"


def test_vendor_derived_from_description_when_missing():
    assert derive_vendor("POS PURCHASE TIM HORTONS 88991") == "tim hortons"


def test_vendor_first_learning_and_reuse(client):
    register(client)
    login(client)

    first_import = client.post(
        "/import/csv",
        data={
            "action": "confirm",
            "parsed_rows": json.dumps([
                {"user_id": 1, "date": "2026-09-04", "amount": -7.0, "description": "POS PURCHASE TIM HORTONS 101", "vendor": "Tim Hortons", "normalized_description": "pos purchase tim hortons 101", "category": ""}
            ]),
            "override_category_0": "Bakery & Coffee",
        },
        follow_redirects=True,
    )
    assert b"Imported 1 transaction(s)." in first_import.data

    with client.application.app_context():
        db = client.application.get_db()
        rule = db.execute("SELECT key_type, pattern FROM category_rules WHERE source = 'import_override' ORDER BY id DESC LIMIT 1").fetchone()
    assert rule["key_type"] == "vendor"

    second_import = client.post(
        "/import/csv",
        data={
            "action": "confirm",
            "parsed_rows": json.dumps([
                {"user_id": 1, "date": "2026-09-05", "amount": -8.0, "description": "TIM HORTONS #55", "vendor": "Tim Hortons", "normalized_description": "tim hortons 55", "category": ""}
            ]),
        },
        follow_redirects=True,
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
