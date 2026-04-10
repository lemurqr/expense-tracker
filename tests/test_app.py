from pathlib import Path
import csv
import os
import io
import json
import re
import sqlite3
from decimal import Decimal
from datetime import datetime, timedelta

import pytest
import expense_tracker as expense_tracker_module

from tests.conftest import LIVE_DB_NAME, get_test_postgres_url

from expense_tracker import (
    create_app,
    infer_category,
    normalize_text,
    extract_pattern,
    parse_csv_transactions,
    normalize_amount,
    derive_vendor,
    detect_header_and_mapping,
    detect_cibc_headerless_mapping,
    DEFAULT_CATEGORIES,
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
                INSERT INTO import_staging (import_id, household_id, user_id, created_at, row_json, status, selected)
                VALUES (?, ?, ?, ?, ?, 'preview', ?)
                """,
                (preview_id, 1, 1, timestamp, json.dumps({**row, "selected": bool(row.get("selected", True))}), 1 if row.get("selected", True) else 0),
            )
        db.commit()
    return preview_id


def confirm_import(client, rows, **form_data):
    import_id = stage_import_preview(client, rows)
    payload = {"action": "confirm", "import_id": import_id}
    payload.update(form_data)
    return client.post("/import/csv", data=payload, follow_redirects=True)


def extract_import_id_from_html(html):
    hidden_match = re.search(r'name="import_id" value="([^"]+)"', html)
    if hidden_match:
        return hidden_match.group(1)
    card_match = re.search(r'data-import-id="([^"]+)"', html)
    if card_match:
        return card_match.group(1)
    raise AssertionError("Could not find import_id in preview HTML")


def extract_selected_row_ids_from_html(html):
    return re.findall(r'name="selected_row_ids" value="(\d+)"', html)


def test_db_health_reports_backend_and_schema_version(client):
    response = client.get("/health/db")
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["backend"] == "sqlite"
    assert payload["schema_version"] >= 10
    assert payload["ok"] is True




def test_import_preview_get_show_all_query_param_controls_row_limit(client):
    register(client)
    login(client)

    rows = []
    for idx in range(51):
        rows.append(
            {
                "user_id": 1,
                "row_index": idx,
                "date": "2026-03-01",
                "amount": -5.0 - idx,
                "description": f"Toggle Merchant {idx}",
                "normalized_description": f"toggle merchant {idx}",
                "vendor": f"Toggle Merchant {idx}",
                "category": "Groceries",
                "confidence": 90,
                "confidence_label": "High",
                "suggested_source": "rule",
                "vendor_key": f"toggle merchant {idx}",
                "vendor_rule_key": f"toggle merchant {idx}",
                "description_rule_key": f"toggle merchant {idx}",
            }
        )

    import_id = stage_import_preview(client, rows, preview_id="preview-get-toggle-51")

    limited_preview = client.get(f"/import/csv?import_id={import_id}")
    limited_html = limited_preview.get_data(as_text=True)
    assert limited_preview.status_code == 200
    assert "Showing 25 of 51 rows" in limited_html
    assert limited_html.count('class="preview-row"') == 25

    all_rows_preview = client.get(f"/import/csv?import_id={import_id}&show_all=1")
    all_rows_html = all_rows_preview.get_data(as_text=True)
    assert all_rows_preview.status_code == 200
    assert "Showing 51 of 51 rows" in all_rows_html
    assert all_rows_html.count('class="preview-row"') == 51


def test_import_preview_apply_options_show_all_rows_works_for_normal_size_preview(client):
    register(client)
    login(client)

    rows = []
    for idx in range(29):
        rows.append(
            {
                "user_id": 1,
                "row_index": idx,
                "date": "2026-03-02",
                "amount": -5.0 - idx,
                "description": f"Normal Merchant {idx}",
                "normalized_description": f"normal merchant {idx}",
                "vendor": f"Normal Merchant {idx}",
                "category": "Groceries",
                "confidence": 90,
                "confidence_label": "High",
                "suggested_source": "rule",
                "vendor_key": f"normal merchant {idx}",
                "vendor_rule_key": f"normal merchant {idx}",
                "description_rule_key": f"normal merchant {idx}",
            }
        )

    import_id = stage_import_preview(client, rows, preview_id="preview-normal-29-show-all")

    default_preview = client.get(f"/import/csv?import_id={import_id}")
    default_text = default_preview.get_data(as_text=True)
    assert default_preview.status_code == 200
    assert "Showing 25 of 29 rows" in default_text
    assert default_text.count('class="preview-row"') == 25

    apply_options_preview = client.get(f"/import/csv?import_id={import_id}&show_all_rows=0&show_all_rows=1")
    apply_options_text = apply_options_preview.get_data(as_text=True)
    assert apply_options_preview.status_code == 200
    assert "Showing 29 of 29 rows" in apply_options_text
    assert apply_options_text.count('class="preview-row"') == 29
    assert 'name="show_all_rows" value="1" checked' in apply_options_text


def test_import_preview_show_all_toggle_and_confirm_imports_all_rows(client):
    register(client)
    login(client)

    parsed_rows = []
    for idx in range(51):
        parsed_rows.append(
            {
                "user_id": 1,
                "row_index": idx,
                "date": "2026-02-01",
                "amount": -10.0 - idx,
                "description": f"Merchant {idx}",
                "normalized_description": f"merchant {idx}",
                "vendor": f"Merchant {idx}",
                "category": "Groceries",
                "confidence": 90,
                "confidence_label": "High",
                "suggested_source": "rule",
                "vendor_key": f"merchant {idx}",
                "vendor_rule_key": f"merchant {idx}",
                "description_rule_key": f"merchant {idx}",
            }
        )

    import_id = stage_import_preview(client, parsed_rows, preview_id="preview-show-all-51")

    default_preview = client.get(f"/import/csv?import_id={import_id}")
    default_text = default_preview.get_data(as_text=True)
    assert default_preview.status_code == 200
    assert "Showing 25 of 51 rows" in default_text
    assert default_text.count('class="preview-row"') == 25

    show_all_preview = client.get(f"/import/csv?import_id={import_id}&show_all=1")
    show_all_text = show_all_preview.get_data(as_text=True)
    assert show_all_preview.status_code == 200
    assert "Showing 51 of 51 rows" in show_all_text
    assert show_all_text.count('class="preview-row"') == 51

    confirm_response = client.post(
        "/import/csv",
        data={"action": "confirm", "import_id": import_id, "show_all_rows": "1"},
        follow_redirects=True,
    )
    assert b"Imported 51 transaction(s)." in confirm_response.data



def test_import_preview_large_show_all_defaults_to_limited_without_both_flags(client):
    register(client)
    login(client)

    rows = []
    for idx in range(501):
        rows.append(
            {
                "user_id": 1,
                "row_index": idx,
                "date": "2026-02-01",
                "amount": -10.0 - idx,
                "description": f"Large Merchant {idx}",
                "normalized_description": f"large merchant {idx}",
                "vendor": f"Large Merchant {idx}",
                "category": "Groceries",
                "confidence": 90,
                "confidence_label": "High",
                "suggested_source": "rule",
            }
        )

    import_id = stage_import_preview(client, rows, preview_id="preview-large-501")

    response = client.get(f"/import/csv?import_id={import_id}&show_all=1")
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "Showing 25 of 501 rows" in html
    assert html.count('class="preview-row"') == 25
    assert "This preview has more than 500 rows." in html


def test_import_preview_large_show_all_renders_all_rows_when_both_flags_set(client):
    register(client)
    login(client)

    rows = []
    for idx in range(501):
        rows.append(
            {
                "user_id": 1,
                "row_index": idx,
                "date": "2026-02-01",
                "amount": -10.0 - idx,
                "description": f"Large Merchant {idx}",
                "normalized_description": f"large merchant {idx}",
                "vendor": f"Large Merchant {idx}",
                "category": "Groceries",
                "confidence": 90,
                "confidence_label": "High",
                "suggested_source": "rule",
            }
        )

    import_id = stage_import_preview(client, rows, preview_id="preview-large-501-both")

    response = client.get(f"/import/csv?import_id={import_id}&show_all=1&confirm_show_all=1")
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "Showing 501 of 501 rows" in html
    assert html.count('class="preview-row"') == 501


def test_import_preview_large_show_all_checkbox_state_persists_after_submit(client):
    register(client)
    login(client)

    rows = []
    for idx in range(501):
        rows.append(
            {
                "user_id": 1,
                "row_index": idx,
                "date": "2026-02-01",
                "amount": -10.0 - idx,
                "description": f"Large Merchant {idx}",
                "normalized_description": f"large merchant {idx}",
                "vendor": f"Large Merchant {idx}",
                "category": "Groceries",
                "confidence": 90,
                "confidence_label": "High",
                "suggested_source": "rule",
            }
        )

    import_id = stage_import_preview(client, rows, preview_id="preview-large-501-state")

    only_show_all = client.get(f"/import/csv?import_id={import_id}&show_all=1")
    only_show_all_html = only_show_all.get_data(as_text=True)
    assert only_show_all.status_code == 200
    assert 'name="show_all" value="1" checked' in only_show_all_html
    assert 'name="confirm_show_all" value="1" checked' not in only_show_all_html

    both_checked = client.get(f"/import/csv?import_id={import_id}&show_all=1&confirm_show_all=1")
    both_checked_html = both_checked.get_data(as_text=True)
    assert both_checked.status_code == 200
    assert 'name="show_all" value="1" checked' in both_checked_html
    assert 'name="confirm_show_all" value="1" checked' in both_checked_html


def test_import_preview_large_show_all_rerun_with_duplicate_checkbox_params_renders_full_rows(client):
    register(client)
    login(client)

    rows = []
    for idx in range(655):
        rows.append(
            {
                "user_id": 1,
                "row_index": idx,
                "date": "2026-02-01",
                "amount": -10.0 - idx,
                "description": f"Large Merchant {idx}",
                "normalized_description": f"large merchant {idx}",
                "vendor": f"Large Merchant {idx}",
                "category": "Groceries",
                "confidence": 90,
                "confidence_label": "High",
                "suggested_source": "rule",
            }
        )

    import_id = stage_import_preview(client, rows, preview_id="preview-large-655-duplicate-flags")

    limited = client.get(f"/import/csv?import_id={import_id}&show_all=0&confirm_show_all=0")
    limited_html = limited.get_data(as_text=True)
    assert limited.status_code == 200
    assert "Showing 25 of 655 rows" in limited_html
    assert limited_html.count('class="preview-row"') == 25

    rerun = client.get(
        f"/import/csv?import_id={import_id}&show_all=0&show_all=1&confirm_show_all=0&confirm_show_all=1"
    )
    rerun_html = rerun.get_data(as_text=True)
    assert rerun.status_code == 200
    assert "Showing 655 of 655 rows" in rerun_html
    assert rerun_html.count('class="preview-row"') == 655
    assert 'name="show_all" value="1" checked' in rerun_html
    assert 'name="confirm_show_all" value="1" checked' in rerun_html

    rerun_again = client.get(
        f"/import/csv?import_id={import_id}&show_all=0&show_all=1&confirm_show_all=0&confirm_show_all=1"
    )
    rerun_again_html = rerun_again.get_data(as_text=True)
    assert rerun_again.status_code == 200
    assert "Showing 655 of 655 rows" in rerun_again_html
    assert rerun_again_html.count('class="preview-row"') == 655
    assert 'name="show_all" value="1" checked' in rerun_again_html
    assert 'name="confirm_show_all" value="1" checked' in rerun_again_html



def test_import_preview_selection_works_without_show_all_toggle(client):
    register(client)
    login(client)

    rows = []
    for idx in range(30):
        rows.append(
            {
                "user_id": 1,
                "row_index": idx,
                "date": "2026-12-28",
                "amount": -10.0 - idx,
                "description": f"No-toggle row {idx}",
                "normalized_description": f"no-toggle row {idx}",
                "vendor": f"Vendor {idx}",
                "category": "",
                "confidence": 40,
                "confidence_label": "Low",
                "suggested_source": "unknown",
                "paid_by": "DK",
            }
        )

    csv_preview = client.post(
        "/import/csv",
        data={
            "action": "preview",
            "map_date": "0",
            "map_description": "1",
            "map_debit": "2",
            "import_default_paid_by": "DK",
            "csv_file": (
                io.BytesIO(
                    ("Date,Description,Debit,Credit\n" + "\n".join([f"2026-12-28,No-toggle row {i},{10+i:.2f}," for i in range(30)])).encode("utf-8")
                ),
                "no-toggle.csv",
            ),
        },
        content_type="multipart/form-data",
    )

    assert csv_preview.status_code == 200
    html = csv_preview.get_data(as_text=True)
    import_id = extract_import_id_from_html(html)
    selected_ids = extract_selected_row_ids_from_html(html)
    assert len(selected_ids) == 25

    selected_two = [selected_ids[0], selected_ids[1]]
    confirm_response = client.post(
        "/import/csv",
        data={
            "action": "confirm",
            "import_id": import_id,
            "selected_row_ids_submitted": "1",
            "selected_row_ids": selected_two,
            "import_default_paid_by": "DK",
        },
        follow_redirects=True,
    )

    assert b"Imported 2 transaction(s)." in confirm_response.data

    with client.application.app_context():
        db = client.application.get_db()
        imported_count = db.execute(
            "SELECT COUNT(*) AS c FROM expenses WHERE description LIKE 'No-toggle row %'"
        ).fetchone()["c"]

    assert imported_count == 2


def test_import_preview_selection_stays_correct_after_show_all_toggle(client):
    register(client)
    login(client)

    rows = []
    for idx in range(30):
        rows.append(
            {
                "row_index": idx,
                "user_id": 1,
                "date": "2026-12-29",
                "amount": -20.0 - idx,
                "description": f"Toggle-select row {idx}",
                "normalized_description": f"toggle-select row {idx}",
                "vendor": f"Vendor {idx}",
                "category": "",
                "confidence": 40,
                "confidence_label": "Low",
                "suggested_source": "unknown",
                "paid_by": "DK",
            }
        )

    import_id = stage_import_preview(client, rows, preview_id="toggle-selection-import")

    show_all_preview = client.get(f"/import/csv?import_id={import_id}&show_all=1")
    assert show_all_preview.status_code == 200

    with client.application.app_context():
        db = client.application.get_db()
        staged_ids = [
            row["id"]
            for row in db.execute(
                "SELECT id FROM import_staging WHERE import_id = ? ORDER BY id ASC", (import_id,)
            ).fetchall()
        ]

    client.post("/import/preview/selection/bulk", json={"import_id": import_id, "selected": False, "scope": "all"})
    client.post("/import/preview/selection", json={"import_id": import_id, "row_id": staged_ids[0], "selected": True})
    client.post("/import/preview/selection", json={"import_id": import_id, "row_id": staged_ids[1], "selected": True})

    limited_preview = client.get(f"/import/csv?import_id={import_id}&show_all=0")
    assert limited_preview.status_code == 200

    confirm_response = client.post(
        "/import/csv",
        data={"action": "confirm", "import_id": import_id, "import_default_paid_by": "DK"},
        follow_redirects=True,
    )

    assert b"Imported 2 transaction(s)." in confirm_response.data

    with client.application.app_context():
        db = client.application.get_db()
        imported = db.execute(
            "SELECT description FROM expenses WHERE description LIKE 'Toggle-select row %' ORDER BY description"
        ).fetchall()

    assert [row["description"] for row in imported] == ["Toggle-select row 0", "Toggle-select row 1"]


def test_import_preview_toggle_is_reversible_and_preserves_staged_edits(client):
    register(client)
    login(client)

    parsed_rows = []
    for idx in range(51):
        parsed_rows.append(
            {
                "user_id": 1,
                "row_index": idx,
                "date": "2026-02-01",
                "amount": -10.0 - idx,
                "description": f"Merchant {idx}",
                "normalized_description": f"merchant {idx}",
                "vendor": f"Merchant {idx}",
                "category": "Groceries",
                "confidence": 90,
                "confidence_label": "High",
                "suggested_source": "rule",
                "vendor_key": f"merchant {idx}",
                "vendor_rule_key": f"merchant {idx}",
                "description_rule_key": f"merchant {idx}",
                "paid_by": "",
            }
        )

    import_id = stage_import_preview(client, parsed_rows, preview_id="preview-edit-persist-51")

    apply_response = client.post(
        "/import/csv/apply_preview_edits",
        data={
            "import_id": import_id,
            "show_all": "1",
            "confirm_show_all": "0",
            "override_paid_by_0": "YZ",
            "override_category_0": "Restaurants",
        },
        follow_redirects=True,
    )
    apply_text = apply_response.get_data(as_text=True)
    assert apply_response.status_code == 200
    assert "Showing 51 of 51 rows" in apply_text
    assert 'name="override_paid_by_0"' in apply_text
    assert '<option value="YZ" selected>YZ</option>' in apply_text
    assert '<option value="Restaurants" selected>Restaurants</option>' in apply_text

    with client.application.app_context():
        db = client.application.get_db()
        staged = db.execute(
            "SELECT row_json FROM import_staging WHERE import_id = ? ORDER BY id LIMIT 1",
            (import_id,),
        ).fetchone()
    staged_row = json.loads(staged["row_json"])
    assert staged_row["paid_by"] == "YZ"
    assert staged_row["override_category"] == "Restaurants"

    limited_preview = client.get(f"/import/csv?import_id={import_id}&show_all=0")
    limited_text = limited_preview.get_data(as_text=True)
    assert limited_preview.status_code == 200
    assert "Showing 25 of 51 rows" in limited_text
    assert limited_text.count('class="preview-row"') == 25
    assert '<option value="YZ" selected>YZ</option>' in limited_text

    confirm_response = client.post(
        "/import/csv",
        data={"action": "confirm", "import_id": import_id, "show_all_rows": "0"},
        follow_redirects=True,
    )
    assert b"Imported 51 transaction(s)." in confirm_response.data



def test_confirm_import_only_selected_rows_are_inserted(client):
    register(client)
    login(client)

    rows = [
        {"user_id": 1, "row_index": i, "date": "2026-09-01", "amount": -10.0 - i, "description": f"Row {i}", "vendor": f"Row {i}", "category": "Groceries", "selected": i < 2}
        for i in range(5)
    ]
    import_id = stage_import_preview(client, rows, preview_id="preview-selected-only")
    response = client.post('/import/csv', data={"action": "confirm", "import_id": import_id}, follow_redirects=True)
    assert response.status_code == 200
    assert b"Imported 2 transaction(s)." in response.data


def test_row_update_amount_override_used_on_confirm(client):
    register(client)
    login(client)
    import_id = stage_import_preview(
        client,
        [{"user_id": 1, "row_index": 0, "date": "2026-09-02", "amount": -10.0, "description": "Coffee", "vendor": "Coffee", "category": "Groceries", "paid_by": "DK"}],
        preview_id="preview-override-amount",
    )
    with client.application.app_context():
        db = client.application.get_db()
        row_id = db.execute("SELECT id FROM import_staging WHERE import_id = ?", (import_id,)).fetchone()["id"]

    update_response = client.post('/import/preview/row_update', json={"import_id": import_id, "row_id": row_id, "amount_override": "123.45"})
    assert update_response.status_code == 200

    client.post('/import/csv', data={"action": "confirm", "import_id": import_id}, follow_redirects=True)
    with client.application.app_context():
        db = client.application.get_db()
        amount = db.execute("SELECT amount FROM expenses WHERE description = 'Coffee'").fetchone()["amount"]
    assert amount == pytest.approx(123.45)


def test_confirm_import_handles_decimal_amount_override_in_preview_state(client, monkeypatch):
    register(client)
    login(client)
    rows = [
        {
            "user_id": 1,
            "row_index": 0,
            "date": "2026-09-02",
            "amount": -10.0,
            "description": "Decimal Override",
            "vendor": "Decimal Override",
            "category": "Groceries",
            "paid_by": "DK",
        }
    ]
    import_id = stage_import_preview(client, rows, preview_id="preview-decimal-override")

    original_get_records = expense_tracker_module.get_staged_preview_row_records

    def _records_with_decimal(*args, **kwargs):
        records = original_get_records(*args, **kwargs)
        for record in records:
            record["row"]["amount_override"] = Decimal("12.34")
        return records

    monkeypatch.setattr(expense_tracker_module, "get_staged_preview_row_records", _records_with_decimal)

    confirm_response = client.post(
        "/import/csv",
        data={"action": "confirm", "import_id": import_id},
        follow_redirects=True,
    )

    assert confirm_response.status_code == 200
    assert b"Imported 1 transaction(s)." in confirm_response.data


def test_confirm_normalizes_transfer_and_payment_as_outflows(client):
    register(client)
    login(client)
    rows = [
        {"user_id": 1, "row_index": 0, "date": "2026-09-03", "amount": 180.56, "description": "E-TRANSFER SENT TO A", "vendor": "Bank", "category": "", "paid_by": "DK"},
        {"user_id": 1, "row_index": 1, "date": "2026-09-03", "amount": 719.73, "description": "BILL PAYMENT TELUS", "vendor": "Bank", "category": "", "paid_by": "DK"},
        {"user_id": 1, "row_index": 2, "date": "2026-09-03", "amount": 50.00, "description": "REFUND FROM STORE", "vendor": "Store", "category": "", "paid_by": "DK"},
    ]
    confirm_import(client, rows)
    with client.application.app_context():
        db = client.application.get_db()
        inserted = db.execute("SELECT description, amount FROM expenses ORDER BY id ASC").fetchall()
    assert inserted[0]["amount"] == pytest.approx(-180.56)
    assert inserted[1]["amount"] == pytest.approx(-719.73)
    assert inserted[2]["amount"] == pytest.approx(50.0)


def test_dedupe_ignores_paid_by_and_category(client):
    register(client)
    login(client)
    rows = [{"user_id": 1, "row_index": 0, "date": "2026-09-04", "amount": -20.0, "description": "Same Tx", "vendor": "Shop", "category": "Groceries", "paid_by": "DK"}]
    first = confirm_import(client, rows)
    assert b"Imported 1 transaction(s)." in first.data
    rows_second = [{"user_id": 1, "row_index": 0, "date": "2026-09-04", "amount": -20.0, "description": "Same Tx", "vendor": "Shop", "category": "Restaurants", "paid_by": "YZ"}]
    second = confirm_import(client, rows_second)
    assert b"Imported 0 transaction(s)." in second.data
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
    assert b"date,amount,paid_by,category,subcategory,vendor,description,confidence,source" in csv_response.data
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
    assert b'id="spend-details-chart"' in response.data


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


def test_export_csv_includes_extended_columns_and_respects_dashboard_tx_filters(client):
    register(client)
    login(client)

    with client.application.app_context():
        db = client.application.get_db()
        user_id = db.execute("SELECT id FROM users WHERE username = ?", ("user1",)).fetchone()["id"]
        household_id = db.execute("SELECT household_id FROM household_members WHERE user_id = ? LIMIT 1", (user_id,)).fetchone()[
            "household_id"
        ]
        category_id = db.execute("SELECT id FROM categories WHERE name = 'Groceries' AND user_id = ?", (user_id,)).fetchone()["id"]
        db.execute(
            "INSERT INTO subcategories (user_id, category_id, name, created_at) VALUES (?, ?, ?, ?)",
            (user_id, category_id, "Produce", datetime.utcnow().isoformat()),
        )
        subcategory_id = db.last_insert_id()
        db.execute(
            """
            INSERT INTO expenses
                (user_id, household_id, date, amount, category_id, subcategory_id, description, vendor, paid_by, category_confidence, category_source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                household_id,
                "2026-02-15",
                -42.75,
                category_id,
                subcategory_id,
                "Honeycrisp apples",
                "Fresh Farm",
                "DK",
                88,
                "manual",
            ),
        )
        db.execute(
            """
            INSERT INTO expenses
                (user_id, household_id, date, amount, category_id, description, vendor, paid_by, category_confidence, category_source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                household_id,
                "2026-02-16",
                -9.50,
                category_id,
                "Should be filtered out",
                "Other Store",
                "YZ",
                55,
                "rule",
            ),
        )
        db.commit()

    csv_response = client.get("/export/csv?start=2026-02-01&end=2026-02-28&tx_vendor_q=fresh")
    assert csv_response.status_code == 200
    text = csv_response.get_data(as_text=True)
    rows = list(csv.reader(io.StringIO(text)))

    assert rows[0] == ["date", "amount", "paid_by", "category", "subcategory", "vendor", "description", "confidence", "source"]
    assert len(rows) == 2
    assert rows[1] == ["2026-02-15", "-42.75", "DK", "Groceries", "Produce", "Fresh Farm", "Honeycrisp apples", "88", "manual"]


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
    text = response.get_data(as_text=True)
    assert "Total shared expenses (DK+YZ)</td><td>$50.00" in text
    assert "Net settlement (this period)" in text


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


def test_detect_header_mapping_prefers_vendor_column_over_description_aliases():
    rows = [["Date", "Description", "Vendor", "Amount", "Memo"]]

    has_header, mapping, _ = detect_header_and_mapping(rows)

    assert has_header is True
    assert mapping["description"] == "1"
    assert mapping["vendor"] == "2"


def test_import_csv_preserves_manual_vendor_mapping_on_reupload(client):
    register(client)
    login(client)

    csv_content = "Date,Description,Vendor,Amount\n2026-01-10,Coffee purchase,Coffee Shop,5.50\n"

    first_preview = client.post(
        "/import/csv",
        data={"action": "preview", "csv_file": (io.BytesIO(csv_content.encode("utf-8")), "manual-vendor.csv")},
        content_type="multipart/form-data",
    )
    assert first_preview.status_code == 200

    second_preview = client.post(
        "/import/csv",
        data={
            "action": "preview",
            "map_date": "0",
            "map_description": "1",
            "map_vendor": "2",
            "map_amount": "3",
            "csv_file": (io.BytesIO(csv_content.encode("utf-8")), "manual-vendor.csv"),
        },
        content_type="multipart/form-data",
    )
    assert second_preview.status_code == 200

    third_preview = client.post(
        "/import/csv",
        data={"action": "preview", "csv_file": (io.BytesIO(csv_content.encode("utf-8")), "manual-vendor.csv")},
        content_type="multipart/form-data",
    )
    assert third_preview.status_code == 200

    with client.session_transaction() as sess:
        mapping = sess["csv_mapping"]

    assert mapping["desc_col"] == "1"
    assert mapping["vendor_col"] == "2"

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




def test_cibc_headerless_preview_includes_debit_and_credit_rows(client):
    register(client)
    login(client)

    csv_content = "\n".join([
        "2026-01-10,Groceries,52.10,,****1111",
        "2026-01-11,Coffee,6.35,,****1111",
        "2026-01-12,Payment Received,,200.00,****1111",
    ])
    response = client.post(
        "/import/csv",
        data={"action": "preview", "csv_file": (io.BytesIO(csv_content.encode("utf-8")), "cibc.csv")},
        content_type="multipart/form-data",
    )

    text = response.get_data(as_text=True)
    assert response.status_code == 200
    assert "Groceries" in text
    assert "Coffee" in text
    assert "Payment Received" in text
    assert "Parsed 3 rows (2 debit, 1 credit)" in text

    with client.session_transaction() as sess:
        mapping = sess["csv_mapping"]
    assert mapping["debit_col"] == "2"
    assert mapping["credit_col"] == "3"
    assert mapping["amount_col"] == ""


def test_cibc_headerless_skip_payments_keeps_purchases(client):
    register(client)
    login(client)

    csv_content = "\n".join([
        "2026-01-10,Groceries,52.10,,****1111",
        "2026-01-11,Coffee,6.35,,****1111",
        "2026-01-12,Payment Thank You,,200.00,****1111",
    ])
    response = client.post(
        "/import/csv",
        data={"action": "preview", "skip_payments": "1", "csv_file": (io.BytesIO(csv_content.encode("utf-8")), "cibc.csv")},
        content_type="multipart/form-data",
    )

    text = response.get_data(as_text=True)
    assert response.status_code == 200
    assert "Groceries" in text
    assert "Coffee" in text
    assert "Payment Thank You" not in text
    assert "payment-like rows: 1" in text


def test_parse_csv_transactions_debit_credit_signs_and_diagnostics():
    rows = [
        ["2026-01-10", "Groceries", "52.10", ""],
        ["2026-01-11", "Payment Thank You", "", "200.00"],
    ]
    mapping = {"date": "0", "description": "1", "amount": "", "debit": "2", "credit": "3", "vendor": "", "category": ""}

    parsed, diagnostics = parse_csv_transactions(rows, mapping, user_id=1)

    assert [row["amount"] for row in parsed] == [-52.1, 200.0]
    assert diagnostics["rows_with_debit"] == 1
    assert diagnostics["rows_with_credit"] == 1
    assert diagnostics["skipped_rows"] == 0


def test_normalize_amount_manual_tracker_forces_expenses_negative():
    amount, classification = normalize_amount(719.73, source_type="manual_tracker", is_refund_or_payment=False)
    assert amount == -719.73
    assert classification == "expense"

    amount, classification = normalize_amount(-50.59, source_type="manual_tracker", is_refund_or_payment=False)
    assert amount == -50.59
    assert classification == "expense"


def test_parse_csv_transactions_manual_tracker_positive_amount_becomes_negative():
    rows = [["2026-01-10", "School Fee", "719.73", "School & Education"]]
    mapping = {
        "date": "0",
        "description": "1",
        "amount": "2",
        "debit": "",
        "credit": "",
        "vendor": "",
        "category": "3",
        "paid_by": "",
    }

    parsed, _ = parse_csv_transactions(rows, mapping, user_id=1, source_type="manual_tracker")

    assert len(parsed) == 1
    assert parsed[0]["amount"] == -719.73
    assert parsed[0]["amount_classification"] == "expense"




def test_detect_header_and_mapping_identifies_manual_tracker_total_exp_amount_column():
    rows = [
        ["Date", "Description", "Yuliana Exp", "Denys Exp", "Payable to Denys", "Split %", "Total exp..."],
        ["2026-01-10", "School Fee", "200.00", "519.73", "519.73", "50%", "719.73"],
    ]

    has_header, mapping, header_row_index = detect_header_and_mapping(rows)

    assert has_header is True
    assert header_row_index == 0
    assert mapping["amount"] == "6"


def test_manual_tracker_total_exp_mapping_imports_rows_without_missing_amount(client):
    register(client)
    login(client)

    fixture = Path(__file__).parent / "fixtures" / "manual_tracker_total_exp.csv"
    with fixture.open("rb") as f:
        preview_response = client.post(
            "/import/csv",
            data={"action": "preview", "csv_file": (f, "manual_tracker_total_exp.csv")},
            content_type="multipart/form-data",
        )

    assert preview_response.status_code == 200
    text = preview_response.get_data(as_text=True)
    assert "School Fee" in text
    assert "Groceries" in text
    assert "missing amount: 0" in text

    import_id = text.split('name="import_id" value="')[1].split('"', 1)[0]
    confirm = client.post(
        "/import/csv",
        data={"action": "confirm", "import_id": import_id, "import_default_paid_by": "DK"},
        follow_redirects=True,
    )
    assert b"Imported 2 transaction(s)." in confirm.data


def test_detect_header_and_mapping_does_not_auto_pick_split_or_payable_or_person_columns_as_amount():
    rows = [["Date", "Description", "Yuliana Exp", "Denys Exp", "Payable to Denys", "Split %"]]

    has_header, mapping, _ = detect_header_and_mapping(rows)

    assert has_header is True
    assert mapping["amount"] == ""

def test_detect_cibc_headerless_mapping_uses_two_amount_columns_not_single_amount():
    rows = [
        ["2026-01-10", "Groceries", "52.10", "", "memo"],
        ["2026-01-11", "Payment", "", "200.00", "memo"],
        ["2026-01-12", "Fuel", "40.00", "", "memo"],
    ]

    mapping = detect_cibc_headerless_mapping(rows)

    assert mapping is not None
    assert mapping["debit"] == "2"
    assert mapping["credit"] == "3"
    assert mapping["amount"] == ""


def test_preview_shows_detected_debit_credit_labels(client):
    register(client)
    login(client)

    csv_content = "2026-01-10,Coffee,5.50,,****1234\n"
    response = client.post(
        "/import/csv",
        data={"action": "preview", "csv_file": (io.BytesIO(csv_content.encode("utf-8")), "cibc.csv")},
        content_type="multipart/form-data",
    )

    text = response.get_data(as_text=True)
    assert "Detected Debit column:" in text
    assert "Detected Credit column:" in text

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


def test_dashboard_shared_category_chart_and_repayment_markup(client):
    register(client)
    login(client)

    with client.application.app_context():
        db = client.application.get_db()
        grocery_id = db.execute("SELECT id FROM categories WHERE name = 'Groceries'").fetchone()["id"]
        transfer_id = db.execute("SELECT id FROM categories WHERE name = 'Transfers'").fetchone()["id"]
        personal_id = db.execute("SELECT id FROM categories WHERE name = 'Personal'").fetchone()["id"]

    client.post(
        "/expenses/new",
        data={"date": "2026-04-01", "amount": "-100", "category_id": str(grocery_id), "description": "IGA"},
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

    assert response.status_code == 200
    assert 'id="spend-details-chart"' in text
    assert "Spend details" in text
    assert "Shared Expenses and Settlements" in text
    assert "legend: {" in text
    assert 'data-spend-mode="period"' in text
    assert 'data-spend-mode="ytd"' in text
    assert 'id="category-analytics-view"' not in text
    assert "Pie Chart" not in text
    assert "Total spending (includes Personal, excludes Transfers):" not in text
    assert "Shared spending (excludes Personal + Transfers):" not in text
    assert "Monthly Summary" not in text
    assert 'id="record-repayment-panel"' in text
    assert "data-settlement-tab=\"record-repayment-panel\"" in text


def test_dashboard_shared_category_chart_shows_categories_in_pie_data(client):
    register(client)
    login(client)

    with client.application.app_context():
        db = client.application.get_db()
        for index in range(12):
            db.execute("INSERT INTO categories (user_id, name) VALUES (?, ?)", (1, f"Category {index + 1}"),)
        category_rows = db.execute(
            "SELECT id, name FROM categories WHERE name LIKE 'Category %' ORDER BY name ASC"
        ).fetchall()
        db.commit()

    for index, row in enumerate(category_rows):
        client.post(
            "/expenses/new",
            data={
                "date": "2026-04-10",
                "amount": str(-(index + 1)),
                "category_id": str(row["id"]),
                "description": f"Expense {index + 1}",
            },
            follow_redirects=True,
        )

    response = client.get("/dashboard?month=2026-04")
    text = response.get_data(as_text=True)

    assert response.status_code == 200
    assert 'id="spend-details-chart"' in text
    assert "Shared Expenses and Settlements" in text
    assert "legend: {" in text

    match = re.search(r"const sharedCategoryAnalytics = ({.*?});", text, re.DOTALL)
    assert match is not None

    analytics = json.loads(match.group(1))
    chart_data = analytics["pie_period"]
    assert len(chart_data) == 6
    assert analytics["pie_ytd"]
    assert all(item["value"] > 0 for item in chart_data)
    assert chart_data[-1]["label"] == "Other"
    assert chart_data[-1]["value"] == 28.0


def test_dashboard_shared_category_chart_hides_zero_current_month_categories(client):
    register(client)
    login(client)

    with client.application.app_context():
        db = client.application.get_db()
        groceries_id = db.execute("SELECT id FROM categories WHERE name = 'Groceries'").fetchone()["id"]
        gifts_id = db.execute("SELECT id FROM categories WHERE name = 'Gifts & Presents'").fetchone()["id"]

    client.post(
        "/expenses/new",
        data={"date": "2026-03-10", "amount": "-75", "category_id": str(gifts_id), "description": "Last month only"},
        follow_redirects=True,
    )
    client.post(
        "/expenses/new",
        data={"date": "2026-04-10", "amount": "-25", "category_id": str(groceries_id), "description": "Current month"},
        follow_redirects=True,
    )

    response = client.get("/dashboard?month=2026-04")
    text = response.get_data(as_text=True)
    analytics = json.loads(re.search(r"const sharedCategoryAnalytics = ({.*?});", text, re.DOTALL).group(1))
    chart_labels = [row["label"] for row in analytics["pie_period"]]

    assert "Groceries" in chart_labels
    assert "Gifts & Presents" not in chart_labels


def test_dashboard_shared_category_chart_uses_custom_date_range_period(client):
    register(client)
    login(client)

    with client.application.app_context():
        db = client.application.get_db()
        groceries_id = db.execute("SELECT id FROM categories WHERE name = 'Groceries'").fetchone()["id"]
        gifts_id = db.execute("SELECT id FROM categories WHERE name = 'Gifts & Presents'").fetchone()["id"]
        for expense_date, amount, category_id, description in [
            ("2026-01-05", -15, gifts_id, "YTD only"),
            ("2026-07-10", -40, groceries_id, "July groceries"),
            ("2026-08-12", -50, gifts_id, "August gifts"),
            ("2026-09-03", -60, groceries_id, "September groceries"),
            ("2026-10-01", -999, gifts_id, "Outside range"),
        ]:
            db.execute(
                """
                INSERT INTO expenses (household_id, user_id, date, amount, category_id, description, is_transfer, is_personal)
                VALUES (?, ?, ?, ?, ?, ?, 0, 0)
                """,
                (None, 1, expense_date, amount, category_id, description),
            )
        db.commit()

    response = client.get("/dashboard?start=2026-07-01&end=2026-09-30")
    text = response.get_data(as_text=True)
    analytics = json.loads(re.search(r"const sharedCategoryAnalytics = ({.*?});", text, re.DOTALL).group(1))

    assert "Shared categories for 2026-07-01 → 2026-09-30" in text
    assert analytics["period_label"] == "2026-07-01 → 2026-09-30"
    assert analytics["ytd_label"] == "year-to-date through 2026-09"
    assert analytics["pie_period"] == [
        {"label": "Groceries", "value": 100.0, "subcategories": []},
        {"label": "Gifts & Presents", "value": 50.0, "subcategories": []},
    ]
    assert analytics["pie_ytd"][0]["label"] == "Groceries"
    assert analytics["pie_ytd"][0]["value"] == 100.0
    assert analytics["pie_ytd"][1]["label"] == "Gifts & Presents"
    assert analytics["pie_ytd"][1]["value"] == 65.0


def test_dashboard_malformed_date_range_does_not_500(client):
    register(client)
    login(client)

    response = client.get("/dashboard?start=foo&end=bar")

    assert response.status_code == 200
    text = response.get_data(as_text=True)
    analytics = json.loads(re.search(r"const sharedCategoryAnalytics = ({.*?});", text, re.DOTALL).group(1))

    assert "Shared categories for" in text
    assert analytics["period_label"]


def test_dashboard_spend_details_yoy_category_aggregation_for_custom_date_range(client):
    register(client)
    login(client)

    with client.application.app_context():
        db = client.application.get_db()
        groceries_id = db.execute("SELECT id FROM categories WHERE name = 'Groceries'").fetchone()["id"]
        gifts_id = db.execute("SELECT id FROM categories WHERE name = 'Gifts & Presents'").fetchone()["id"]
        for expense_date, amount, category_id, description in [
            ("2025-07-05", -80, groceries_id, "Current groceries"),
            ("2025-08-06", -35, gifts_id, "Current gifts"),
            ("2024-07-05", -50, groceries_id, "Prior groceries"),
            ("2024-08-06", -70, gifts_id, "Prior gifts"),
            ("2025-10-01", -999, groceries_id, "Outside range"),
        ]:
            db.execute(
                """
                INSERT INTO expenses (household_id, user_id, date, amount, category_id, description, is_transfer, is_personal)
                VALUES (?, ?, ?, ?, ?, ?, 0, 0)
                """,
                (None, 1, expense_date, amount, category_id, description),
            )
        db.commit()

    response = client.get("/dashboard?start=2025-07-01&end=2025-09-30")
    analytics = json.loads(re.search(r"const sharedCategoryAnalytics = ({.*?});", response.get_data(as_text=True), re.DOTALL).group(1))

    assert analytics["yoy"]["period"]["current_label"] == "2025-07-01 to 2025-09-30"
    assert analytics["yoy"]["period"]["prior_label"] == "2024-07-01 to 2024-09-30"
    assert analytics["yoy"]["period"]["categories"] == [
        {"id": groceries_id, "label": "Groceries", "current_value": 80.0, "prior_value": 50.0},
        {"id": gifts_id, "label": "Gifts & Presents", "current_value": 35.0, "prior_value": 70.0},
    ]


def test_dashboard_spend_details_yoy_category_aggregation_for_ytd(client):
    register(client)
    login(client)

    with client.application.app_context():
        db = client.application.get_db()
        groceries_id = db.execute("SELECT id FROM categories WHERE name = 'Groceries'").fetchone()["id"]
        utilities_id = db.execute("SELECT id FROM categories WHERE name = 'Utilities'").fetchone()["id"]
        for expense_date, amount, category_id, description in [
            ("2025-01-10", -20, groceries_id, "YTD groceries 1"),
            ("2025-09-02", -30, groceries_id, "YTD groceries 2"),
            ("2025-03-01", -40, utilities_id, "YTD utilities"),
            ("2024-01-10", -10, groceries_id, "Prior YTD groceries"),
            ("2024-04-15", -60, utilities_id, "Prior YTD utilities"),
            ("2024-10-05", -999, groceries_id, "Outside prior YTD"),
        ]:
            db.execute(
                """
                INSERT INTO expenses (household_id, user_id, date, amount, category_id, description, is_transfer, is_personal)
                VALUES (?, ?, ?, ?, ?, ?, 0, 0)
                """,
                (None, 1, expense_date, amount, category_id, description),
            )
        db.commit()

    response = client.get("/dashboard?start=2025-07-01&end=2025-09-30")
    analytics = json.loads(re.search(r"const sharedCategoryAnalytics = ({.*?});", response.get_data(as_text=True), re.DOTALL).group(1))

    assert analytics["yoy"]["ytd"]["current_label"] == "2025 YTD through 2025-09-30"
    assert analytics["yoy"]["ytd"]["prior_label"] == "2024 YTD through 2024-09-30"
    assert analytics["yoy"]["ytd"]["categories"] == [
        {"id": groceries_id, "label": "Groceries", "current_value": 50.0, "prior_value": 10.0},
        {"id": utilities_id, "label": "Utilities", "current_value": 40.0, "prior_value": 60.0},
    ]


def test_dashboard_spend_details_yoy_subcategory_drilldown_data(client):
    register(client)
    login(client)

    with client.application.app_context():
        db = client.application.get_db()
        groceries_id = db.execute("SELECT id FROM categories WHERE name = 'Groceries'").fetchone()["id"]
        db.execute("INSERT INTO subcategories (user_id, category_id, name) VALUES (?, ?, ?)", (1, groceries_id, "Produce"))
        db.execute("INSERT INTO subcategories (user_id, category_id, name) VALUES (?, ?, ?)", (1, groceries_id, "Dairy"))
        produce_id = db.execute("SELECT id FROM subcategories WHERE name = 'Produce'").fetchone()["id"]
        dairy_id = db.execute("SELECT id FROM subcategories WHERE name = 'Dairy'").fetchone()["id"]
        for expense_date, amount, subcategory_id, description in [
            ("2025-07-05", -25, produce_id, "Current produce"),
            ("2025-08-05", -15, dairy_id, "Current dairy"),
            ("2024-07-05", -10, produce_id, "Prior produce"),
            ("2024-08-05", -22, dairy_id, "Prior dairy"),
        ]:
            db.execute(
                """
                INSERT INTO expenses (household_id, user_id, date, amount, category_id, subcategory_id, description, is_transfer, is_personal)
                VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0)
                """,
                (None, 1, expense_date, amount, groceries_id, subcategory_id, description),
            )
        db.commit()

    response = client.get("/dashboard?start=2025-07-01&end=2025-09-30")
    analytics = json.loads(re.search(r"const sharedCategoryAnalytics = ({.*?});", response.get_data(as_text=True), re.DOTALL).group(1))
    drilldown = analytics["yoy"]["period"]["subcategories"][str(groceries_id)]

    assert drilldown["category_label"] == "Groceries"
    assert drilldown["rows"] == [
        {"label": "Produce", "current_value": 25.0, "prior_value": 10.0},
        {"label": "Dairy", "current_value": 15.0, "prior_value": 22.0},
    ]


def test_dashboard_spend_details_yoy_ytd_subcategory_drilldown_data(client):
    register(client)
    login(client)

    with client.application.app_context():
        db = client.application.get_db()
        groceries_id = db.execute("SELECT id FROM categories WHERE name = 'Groceries'").fetchone()["id"]
        db.execute("INSERT INTO subcategories (user_id, category_id, name) VALUES (?, ?, ?)", (1, groceries_id, "Produce"))
        db.execute("INSERT INTO subcategories (user_id, category_id, name) VALUES (?, ?, ?)", (1, groceries_id, "Dairy"))
        produce_id = db.execute("SELECT id FROM subcategories WHERE name = 'Produce'").fetchone()["id"]
        dairy_id = db.execute("SELECT id FROM subcategories WHERE name = 'Dairy'").fetchone()["id"]
        for expense_date, amount, subcategory_id, description in [
            ("2025-01-07", -18, produce_id, "Current ytd produce"),
            ("2025-03-02", -12, dairy_id, "Current ytd dairy"),
            ("2024-01-05", -6, produce_id, "Prior ytd produce"),
            ("2024-04-11", -22, dairy_id, "Prior ytd dairy"),
        ]:
            db.execute(
                """
                INSERT INTO expenses (household_id, user_id, date, amount, category_id, subcategory_id, description, is_transfer, is_personal)
                VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0)
                """,
                (None, 1, expense_date, amount, groceries_id, subcategory_id, description),
            )
        db.commit()

    response = client.get("/dashboard?start=2025-07-01&end=2025-09-30")
    analytics = json.loads(re.search(r"const sharedCategoryAnalytics = ({.*?});", response.get_data(as_text=True), re.DOTALL).group(1))
    drilldown = analytics["yoy"]["ytd"]["subcategories"][str(groceries_id)]

    assert drilldown["category_label"] == "Groceries"
    assert drilldown["rows"] == [
        {"label": "Produce", "current_value": 18.0, "prior_value": 6.0},
        {"label": "Dairy", "current_value": 12.0, "prior_value": 22.0},
    ]


def test_dashboard_spend_details_mode_labels_and_compact_table_headers(client):
    register(client)
    login(client)

    response = client.get("/dashboard?start=2026-02-01&end=2026-02-28&spend_view=compare&spend_mode=period")
    text = response.get_data(as_text=True)

    assert 'data-spend-detail-mode="mix"' in text
    assert 'data-spend-detail-mode="trend"' in text
    assert 'data-spend-detail-mode="period-vs-ly"' in text
    assert 'data-spend-detail-mode="yoy"' in text
    assert '>Spend Mix<' in text
    assert '>Trend<' in text
    assert '>Period vs LY<' in text
    assert 'id="spend-yoy-label-heading">Category<' in text
    assert 'id="spend-yoy-current-heading">Current period<' in text
    assert 'id="spend-yoy-comparison-heading">Prior-year same period<' in text
    assert 'id="spend-yoy-delta-heading">Delta<' in text
    assert "yoyLabelHeading.textContent = state.level === 'subcategories' ? 'Subcategory' : 'Category';" in text
    assert "const selectedDetail = bucket.subcategories?.[String(selectedCategory.id)] || null;" in text
    assert "spendDetailsSubtitle.textContent = comparisonState.level === 'subcategories'" in text
    assert "? `${comparisonState.categoryLabel} subcategories — ${comparisonText}`" in text
    assert 'Current YTD' in text
    assert 'Prior-year YTD' in text
    assert "const initialSpendView = new URL(window.location.href).searchParams.get('spend_view') || ''" in text
    assert 'applySpendDetailModeState(spendDetailMode);' in text
    assert 'chartCanvas.title = ""' in text
    assert 'legend: { display: false }' in text
    assert "const spendDetailsBreakdown = document.getElementById('spend-details-breakdown');" in text
    assert "spendDetailsBreakdown.hidden = spendDetailMode !== 'mix';" in text
    assert "const spendCategorySelect = document.getElementById('spend-category-select');" in text
    assert "if (spendCategorySelect) spendCategorySelect.disabled = false;" in text


def test_dashboard_spend_details_compare_query_state_is_preserved_in_markup(client):
    register(client)
    login(client)

    response = client.get(
        "/dashboard?month=2026-03&settlement_tab=record-repayment-panel&spend_mode=ytd&spend_view=compare&spend_compare=yoy"
    )
    text = response.get_data(as_text=True)

    assert 'name="spend_mode" value="ytd"' in text
    assert 'name="spend_view" value="compare"' in text
    assert 'name="spend_compare" value="yoy"' in text
    assert 'spend_view=compare' in text
    assert 'spend_compare=yoy' in text
    assert 'name="spend_mode" value="ytd"' in text


def test_dashboard_spend_details_mode_switch_keeps_mix_markup(client):
    register(client)
    login(client)

    with client.application.app_context():
        db = client.application.get_db()
        groceries_id = db.execute("SELECT id FROM categories WHERE name = 'Groceries'").fetchone()["id"]
        db.execute(
            """
            INSERT INTO expenses (household_id, user_id, date, amount, category_id, description, is_transfer, is_personal)
            VALUES (?, ?, ?, ?, ?, ?, 0, 0)
            """,
            (None, 1, "2026-04-10", -42, groceries_id, "Groceries"),
        )
        db.commit()

    response = client.get("/dashboard?month=2026-04")
    text = response.get_data(as_text=True)
    analytics = json.loads(re.search(r"const sharedCategoryAnalytics = ({.*?});", text, re.DOTALL).group(1))

    assert 'data-spend-detail-mode="mix"' in text
    assert 'data-spend-detail-mode="trend"' in text
    assert 'data-spend-detail-mode="period-vs-ly"' in text
    assert 'data-spend-detail-mode="yoy"' in text
    assert 'id="spend-details-chart"' in text
    assert analytics["pie_period"] == [{"label": "Groceries", "value": 42.0, "subcategories": []}]


def test_dashboard_spend_mix_summary_and_category_dropdown_render(client):
    register(client)
    login(client)

    with client.application.app_context():
        db = client.application.get_db()
        groceries_id = db.execute("SELECT id FROM categories WHERE name = 'Groceries'").fetchone()["id"]
        gifts_id = db.execute("SELECT id FROM categories WHERE name = 'Gifts & Presents'").fetchone()["id"]
        db.execute(
            """
            INSERT INTO expenses (household_id, user_id, date, amount, category_id, description, is_transfer, is_personal)
            VALUES (?, ?, ?, ?, ?, ?, 0, 0)
            """,
            (None, 1, "2026-04-10", -80, groceries_id, "Groceries"),
        )
        db.execute(
            """
            INSERT INTO expenses (household_id, user_id, date, amount, category_id, description, is_transfer, is_personal)
            VALUES (?, ?, ?, ?, ?, ?, 0, 0)
            """,
            (None, 1, "2026-04-12", -20, gifts_id, "Gifts"),
        )
        db.commit()

    response = client.get("/dashboard?month=2026-04")
    text = response.get_data(as_text=True)
    analytics = json.loads(re.search(r"const sharedCategoryAnalytics = ({.*?});", text, re.DOTALL).group(1))

    assert 'id="spend-summary-total"' in text
    assert 'id="spend-category-select"' in text
    assert analytics["summary"]["period_total"] == 100.0
    assert analytics["summary"]["months_count"] == 1
    assert [row["label"] for row in analytics["category_options"]] == ["Groceries", "Gifts & Presents"]


def test_dashboard_spend_mix_category_selection_builds_subcategory_breakdown(client):
    register(client)
    login(client)

    with client.application.app_context():
        db = client.application.get_db()
        groceries_id = db.execute("SELECT id FROM categories WHERE name = 'Groceries'").fetchone()["id"]
        db.execute("INSERT INTO subcategories (user_id, category_id, name) VALUES (?, ?, ?)", (1, groceries_id, "Produce"))
        db.execute("INSERT INTO subcategories (user_id, category_id, name) VALUES (?, ?, ?)", (1, groceries_id, "Dairy"))
        produce_id = db.execute("SELECT id FROM subcategories WHERE name = 'Produce'").fetchone()["id"]
        dairy_id = db.execute("SELECT id FROM subcategories WHERE name = 'Dairy'").fetchone()["id"]
        db.execute(
            """
            INSERT INTO expenses (household_id, user_id, date, amount, category_id, subcategory_id, description, is_transfer, is_personal)
            VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0)
            """,
            (None, 1, "2026-04-05", -40, groceries_id, produce_id, "Produce"),
        )
        db.execute(
            """
            INSERT INTO expenses (household_id, user_id, date, amount, category_id, subcategory_id, description, is_transfer, is_personal)
            VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0)
            """,
            (None, 1, "2026-04-08", -25, groceries_id, dairy_id, "Dairy"),
        )
        db.commit()

    response = client.get("/dashboard?month=2026-04")
    analytics = json.loads(re.search(r"const sharedCategoryAnalytics = ({.*?});", response.get_data(as_text=True), re.DOTALL).group(1))
    breakdown = analytics["mix_by_category"][str(groceries_id)]

    assert breakdown["category_label"] == "Groceries"
    assert breakdown["total"] == 65.0
    assert breakdown["pie_rows"] == [
        {"label": "Produce", "value": 40.0},
        {"label": "Dairy", "value": 25.0},
    ]


def test_dashboard_spend_trend_all_categories_and_selected_category_subcategories(client):
    register(client)
    login(client)

    with client.application.app_context():
        db = client.application.get_db()
        groceries_id = db.execute("SELECT id FROM categories WHERE name = 'Groceries'").fetchone()["id"]
        gifts_id = db.execute("SELECT id FROM categories WHERE name = 'Gifts & Presents'").fetchone()["id"]
        db.execute("INSERT INTO subcategories (user_id, category_id, name) VALUES (?, ?, ?)", (1, groceries_id, "Produce"))
        db.execute("INSERT INTO subcategories (user_id, category_id, name) VALUES (?, ?, ?)", (1, groceries_id, "Dairy"))
        produce_id = db.execute("SELECT id FROM subcategories WHERE name = 'Produce'").fetchone()["id"]
        dairy_id = db.execute("SELECT id FROM subcategories WHERE name = 'Dairy'").fetchone()["id"]
        rows = [
            ("2026-01-05", -10, groceries_id, produce_id, "Jan produce"),
            ("2026-01-08", -8, gifts_id, None, "Jan gifts"),
            ("2026-02-02", -12, groceries_id, dairy_id, "Feb dairy"),
            ("2026-02-12", -6, gifts_id, None, "Feb gifts"),
            ("2026-03-01", -14, groceries_id, produce_id, "Mar produce"),
        ]
        for date_value, amount, category_id, subcategory_id, description in rows:
            db.execute(
                """
                INSERT INTO expenses (household_id, user_id, date, amount, category_id, subcategory_id, description, is_transfer, is_personal)
                VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0)
                """,
                (None, 1, date_value, amount, category_id, subcategory_id, description),
            )
        db.commit()

    response = client.get("/dashboard?start=2026-01-01&end=2026-03-31")
    analytics = json.loads(re.search(r"const sharedCategoryAnalytics = ({.*?});", response.get_data(as_text=True), re.DOTALL).group(1))

    assert analytics["trend"]["months"] == ["2026-01", "2026-02", "2026-03"]
    assert analytics["trend"]["all_categories"]["series"] == [
        {"id": str(groceries_id), "label": "Groceries", "values": [10.0, 12.0, 14.0], "total": 36.0},
        {"id": str(gifts_id), "label": "Gifts & Presents", "values": [8.0, 6.0, 0.0], "total": 14.0},
    ]
    groceries_trend = analytics["trend"]["by_category"][str(groceries_id)]
    assert groceries_trend["category_label"] == "Groceries"
    assert groceries_trend["series"] == [
        {"label": "Produce", "values": [10.0, 0.0, 14.0], "total": 24.0},
        {"label": "Dairy", "values": [0.0, 12.0, 0.0], "total": 12.0},
    ]


def test_dashboard_spend_trend_uses_top_five_plus_other_grouping(client):
    register(client)
    login(client)

    with client.application.app_context():
        db = client.application.get_db()
        category_ids = []
        for index in range(7):
            db.execute("INSERT INTO categories (user_id, name) VALUES (?, ?)", (1, f"Trend Category {index + 1}"))
            category_ids.append(
                db.execute("SELECT id FROM categories WHERE name = ?", (f"Trend Category {index + 1}",)).fetchone()["id"]
            )
        for index, category_id in enumerate(category_ids, start=1):
            db.execute(
                """
                INSERT INTO expenses (household_id, user_id, date, amount, category_id, description, is_transfer, is_personal)
                VALUES (?, ?, ?, ?, ?, ?, 0, 0)
                """,
                (None, 1, "2026-01-10", -float(100 - index), category_id, f"Category {index}"),
            )
        db.commit()

    response = client.get("/dashboard?start=2026-01-01&end=2026-01-31")
    analytics = json.loads(re.search(r"const sharedCategoryAnalytics = ({.*?});", response.get_data(as_text=True), re.DOTALL).group(1))
    rows = analytics["trend"]["all_categories"]["series"]

    assert len(rows) == 6
    assert rows[-1]["label"] == "Other"
    assert rows[-1]["total"] == 187.0


def test_dashboard_spend_details_groups_other_rows_for_compact_comparison_tables(client):
    register(client)
    login(client)

    with client.application.app_context():
        db = client.application.get_db()
        category_names = [
            "Groceries",
            "Utilities",
            "Dining",
            "Pets",
            "Home",
            "Travel",
            "Health",
            "Entertainment",
            "Shopping",
        ]
        category_ids = {}
        for index, name in enumerate(category_names, start=1):
            existing = db.execute("SELECT id FROM categories WHERE name = ?", (name,)).fetchone()
            if existing:
                category_ids[name] = existing["id"]
                continue
            db.execute("INSERT INTO categories (user_id, name) VALUES (?, ?)", (1, name))
            category_ids[name] = db.execute("SELECT id FROM categories WHERE name = ?", (name,)).fetchone()["id"]

        for index, name in enumerate(category_names, start=1):
            db.execute(
                """
                INSERT INTO expenses (household_id, user_id, date, amount, category_id, description, is_transfer, is_personal)
                VALUES (?, ?, ?, ?, ?, ?, 0, 0)
                """,
                (None, 1, f"2026-02-{index:02d}", -(100 - index), category_ids[name], f"Current {name}"),
            )
            db.execute(
                """
                INSERT INTO expenses (household_id, user_id, date, amount, category_id, description, is_transfer, is_personal)
                VALUES (?, ?, ?, ?, ?, ?, 0, 0)
                """,
                (None, 1, f"2025-02-{index:02d}", -(50 - index), category_ids[name], f"Prior {name}"),
            )
        db.commit()

    response = client.get("/dashboard?start=2026-02-01&end=2026-02-28")
    analytics = json.loads(re.search(r"const sharedCategoryAnalytics = ({.*?});", response.get_data(as_text=True), re.DOTALL).group(1))

    rows = analytics["yoy"]["period"]["categories"]
    assert len(rows) == 9
    assert rows[-1]["label"] == "Other"
    assert rows[-1]["current_value"] == 91.0
    assert rows[-1]["prior_value"] == 41.0
    assert [child["label"] for child in rows[-1]["children"]] == ["Shopping"]




def test_shared_category_chart_nets_reimbursements_and_excludes_nonpositive_categories(client):
    register(client)
    login(client)

    with client.application.app_context():
        db = client.application.get_db()
        groceries_id = db.execute("SELECT id FROM categories WHERE name = 'Groceries'").fetchone()["id"]
        gifts_id = db.execute("SELECT id FROM categories WHERE name = 'Gifts & Presents'").fetchone()["id"]

    client.post(
        "/expenses/new",
        data={"date": "2026-04-01", "amount": "-500", "category_id": str(groceries_id), "description": "Groceries expense"},
        follow_redirects=True,
    )
    client.post(
        "/expenses/new",
        data={"date": "2026-04-02", "amount": "100", "category_id": str(groceries_id), "description": "Groceries reimbursement"},
        follow_redirects=True,
    )
    client.post(
        "/expenses/new",
        data={"date": "2026-04-03", "amount": "20", "category_id": str(gifts_id), "description": "Gift reimbursement only"},
        follow_redirects=True,
    )

    response = client.get("/dashboard?month=2026-04")
    text = response.get_data(as_text=True)

    match = re.search(r"const sharedCategoryAnalytics = ({.*?});", text, re.DOTALL)
    assert match is not None
    analytics = json.loads(match.group(1))
    chart_data = analytics["pie_period"]

    assert any(item["label"] == "Groceries" and item["value"] == 400.0 for item in chart_data)
    assert not any(item["label"] == "Gifts" for item in chart_data)


def test_dashboard_spend_details_period_columns_current_last_ytd(client):
    register(client)
    login(client)

    with client.application.app_context():
        db = client.application.get_db()
        groceries_id = db.execute("SELECT id FROM categories WHERE name = 'Groceries'").fetchone()["id"]

    client.post("/expenses/new", data={"date": "2026-03-15", "amount": "-20", "category_id": str(groceries_id), "description": "Mar"}, follow_redirects=True)
    client.post("/expenses/new", data={"date": "2026-04-10", "amount": "-100", "category_id": str(groceries_id), "description": "Apr"}, follow_redirects=True)
    client.post("/expenses/new", data={"date": "2026-04-11", "amount": "40", "category_id": str(groceries_id), "description": "Apr refund"}, follow_redirects=True)
    client.post("/expenses/new", data={"date": "2026-05-01", "amount": "-30", "category_id": str(groceries_id), "description": "May"}, follow_redirects=True)

    response = client.get("/dashboard?month=2026-04")
    text = response.get_data(as_text=True)
    match = re.search(r"const sharedCategoryAnalytics = ({.*?});", text, re.DOTALL)
    analytics = json.loads(match.group(1))
    groceries_row = next(row for row in analytics["table"] if row["label"] == "Groceries")

    assert groceries_row["current_month"] == 60.0
    assert groceries_row["last_month"] == 20.0
    assert groceries_row["year_to_date"] == 80.0


def test_dashboard_spend_details_excludes_transfers(client):
    register(client)
    login(client)
    with client.application.app_context():
        db = client.application.get_db()
        groceries_id = db.execute("SELECT id FROM categories WHERE name = 'Groceries'").fetchone()["id"]
        transfers_id = db.execute("SELECT id FROM categories WHERE name = 'Transfers'").fetchone()["id"]

    client.post("/expenses/new", data={"date": "2026-04-10", "amount": "-100", "category_id": str(groceries_id), "description": "Food"}, follow_redirects=True)
    client.post("/expenses/new", data={"date": "2026-04-11", "amount": "-1000", "category_id": str(transfers_id), "description": "Move"}, follow_redirects=True)

    response = client.get("/dashboard?month=2026-04")
    text = response.get_data(as_text=True)
    analytics = json.loads(re.search(r"const sharedCategoryAnalytics = ({.*?});", text, re.DOTALL).group(1))
    assert any(row["label"] == "Groceries" for row in analytics["table"])
    assert not any(row["label"] == "Transfers" for row in analytics["table"])


def test_subcategory_rollup_and_category_page_subcategory_crud(client):
    register(client)
    login(client)

    with client.application.app_context():
        db = client.application.get_db()
        groceries_id = db.execute("SELECT id FROM categories WHERE name = 'Groceries'").fetchone()["id"]

    client.post(
        "/expenses/new",
        data={"date": "2026-04-05", "amount": "-50", "category_id": str(groceries_id), "description": "Produce expense"},
        follow_redirects=True,
    )
    client.post(
        "/expenses/new",
        data={"date": "2026-04-06", "amount": "-25", "category_id": str(groceries_id), "description": "No subcategory"},
        follow_redirects=True,
    )

    with client.application.app_context():
        db = client.application.get_db()
        user_id = db.execute("SELECT id FROM users WHERE username = ?", ("user1",)).fetchone()["id"]
        db.execute(
            "INSERT INTO subcategories (user_id, category_id, name, created_at) VALUES (?, ?, ?, ?)",
            (user_id, groceries_id, "Produce", datetime.utcnow().isoformat()),
        )
        subcat_id = db.execute("SELECT id FROM subcategories WHERE name = 'Produce'").fetchone()["id"]
        db.execute(
            "UPDATE expenses SET subcategory_id = ? WHERE user_id = ? AND description = ?",
            (subcat_id, user_id, "Produce expense"),
        )
        db.commit()

    dashboard = client.get("/dashboard?month=2026-04")
    analytics = json.loads(re.search(r"const sharedCategoryAnalytics = ({.*?});", dashboard.get_data(as_text=True), re.DOTALL).group(1))
    groceries_row = next(row for row in analytics["table"] if row["label"] == "Groceries")
    assert groceries_row["current_month"] == 75.0
    assert [item["label"] for item in groceries_row["subcategories"]] == ["Produce"]
    assert groceries_row["subcategories"][0]["current_month"] == 50.0
    assert groceries_row["subcategories"][0]["last_month"] == 0.0
    assert groceries_row["subcategories"][0]["year_to_date"] == 50.0

    categories_page = client.get("/categories")
    assert categories_page.status_code == 200
    assert b"Produce" in categories_page.data
    assert b"Expand all" in categories_page.data
    assert b"Collapse all" in categories_page.data
    assert b"Export categories CSV" in categories_page.data

    rename = client.post(f"/subcategories/{subcat_id}/edit", data={"name": "Fruit"}, follow_redirects=True)
    assert b"Subcategory updated." in rename.data

    blocked_delete = client.post(f"/subcategories/{subcat_id}/delete", follow_redirects=True)
    assert b"Cannot delete subcategory while expenses still reference it." in blocked_delete.data


def test_category_delete_blocked_when_expenses_reference_category(client):
    register(client)
    login(client)

    with client.application.app_context():
        db = client.application.get_db()
        groceries_id = db.execute("SELECT id FROM categories WHERE name = 'Groceries'").fetchone()["id"]

    response = client.post(
        f"/categories/{groceries_id}/delete",
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"Cannot delete category while expenses still reference it." in response.data

    with client.application.app_context():
        db = client.application.get_db()
        still_exists = db.execute("SELECT id FROM categories WHERE id = ?", (groceries_id,)).fetchone()
        assert still_exists is not None


def test_category_delete_removes_subcategories_and_budget_rows_without_500(client):
    register(client)
    login(client)

    with client.application.app_context():
        db = client.application.get_db()
        user_id = db.execute("SELECT id FROM users WHERE username = ?", ("user1",)).fetchone()["id"]
        groceries_id = db.execute("SELECT id FROM categories WHERE name = 'Groceries'").fetchone()["id"]
        household_id = db.execute("SELECT household_id FROM household_members WHERE user_id = ? LIMIT 1", (user_id,)).fetchone()[
            "household_id"
        ]

        db.execute(
            "DELETE FROM expenses WHERE user_id = ? AND category_id = ?",
            (user_id, groceries_id),
        )
        db.execute(
            "INSERT INTO subcategories (user_id, category_id, name, created_at) VALUES (?, ?, ?, ?)",
            (user_id, groceries_id, "Delete Me", datetime.utcnow().isoformat()),
        )
        subcategory_id = db.last_insert_id()
        db.execute(
            """
            INSERT INTO monthly_budgets
            (household_id, month, view_mode, scope_mode, category_id, subcategory_id, budget_type, budget_amount, rollover_amount)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (household_id, "2026-04", "household", "shared", groceries_id, 0, "Flexible", 100, 0),
        )
        db.execute(
            """
            INSERT INTO monthly_budgets
            (household_id, month, view_mode, scope_mode, category_id, subcategory_id, budget_type, budget_amount, rollover_amount)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (household_id, "2026-04", "household", "shared", groceries_id, subcategory_id, "Flexible", 50, 0),
        )
        db.commit()

    response = client.post(
        f"/categories/{groceries_id}/delete",
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"Category deleted." in response.data

    with client.application.app_context():
        db = client.application.get_db()
        category_row = db.execute("SELECT id FROM categories WHERE id = ?", (groceries_id,)).fetchone()
        subcategory_row = db.execute("SELECT id FROM subcategories WHERE id = ?", (subcategory_id,)).fetchone()
        budget_rows = db.execute(
            "SELECT COUNT(*) AS c FROM monthly_budgets WHERE category_id = ? OR subcategory_id = ?",
            (groceries_id, subcategory_id),
        ).fetchone()["c"]

        assert category_row is None
        assert subcategory_row is None
        assert budget_rows == 0


def test_categories_csv_export_includes_categories_with_and_without_subcategories(client):
    register(client)
    login(client)

    with client.application.app_context():
        db = client.application.get_db()
        user_id = db.execute("SELECT id FROM users WHERE username = ?", ("user1",)).fetchone()["id"]
        groceries_id = db.execute("SELECT id FROM categories WHERE user_id = ? AND name = ?", (user_id, "Groceries")).fetchone()["id"]
        db.execute(
            "INSERT INTO categories (user_id, name) VALUES (?, ?)",
            (user_id, "Household Test Category"),
        )
        db.execute(
            "INSERT INTO subcategories (user_id, category_id, name, created_at) VALUES (?, ?, ?, ?)",
            (user_id, groceries_id, "Produce", datetime.utcnow().isoformat()),
        )
        db.commit()

    response = client.get("/categories/export.csv")

    assert response.status_code == 200
    assert response.mimetype == "text/csv"
    content_disposition = response.headers.get("Content-Disposition", "")
    assert "attachment" in content_disposition
    assert "categories-" in content_disposition

    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert rows[0] == ["category", "subcategory"]
    assert ["Groceries", "Produce"] in rows
    assert ["Household Test Category", ""] in rows


def test_expense_forms_render_when_subcategories_exist(client):
    register(client)
    login(client)

    with client.application.app_context():
        db = client.application.get_db()
        groceries_id = db.execute("SELECT id FROM categories WHERE name = 'Groceries'").fetchone()["id"]
        user_id = db.execute("SELECT id FROM users WHERE username = ?", ("user1",)).fetchone()["id"]
        db.execute(
            "INSERT INTO subcategories (user_id, category_id, name, created_at) VALUES (?, ?, ?, ?)",
            (user_id, groceries_id, "Produce", datetime.utcnow().isoformat()),
        )
        subcategory_id = db.execute(
            "SELECT id FROM subcategories WHERE user_id = ? AND category_id = ? AND name = ?",
            (user_id, groceries_id, "Produce"),
        ).fetchone()["id"]
        household_id = db.execute("SELECT household_id FROM household_members WHERE user_id = ? LIMIT 1", (user_id,)).fetchone()["household_id"]
        db.execute(
            "INSERT INTO expenses (user_id, household_id, date, amount, category_id, subcategory_id, description, vendor, paid_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (user_id, household_id, "2026-06-01", -10, groceries_id, subcategory_id, "Seed expense", "Vendor", ""),
        )
        expense_id = db.last_insert_id()
        db.commit()

    add_form = client.get("/expenses/new")
    assert add_form.status_code == 200
    assert b"Subcategory" in add_form.data

    edit_form = client.get(f"/expenses/{expense_id}/edit")
    assert edit_form.status_code == 200
    assert b"Subcategory" in edit_form.data


def test_expense_form_subcategory_selection_and_dashboard_display(client):
    register(client)
    login(client)

    with client.application.app_context():
        db = client.application.get_db()
        groceries_id = db.execute("SELECT id FROM categories WHERE name = 'Groceries'").fetchone()["id"]
        user_id = db.execute("SELECT id FROM users WHERE username = ?", ("user1",)).fetchone()["id"]
        db.execute(
            "INSERT INTO subcategories (user_id, category_id, name, created_at) VALUES (?, ?, ?, ?)",
            (user_id, groceries_id, "Produce", datetime.utcnow().isoformat()),
        )
        subcategory_id = db.execute(
            "SELECT id FROM subcategories WHERE user_id = ? AND category_id = ? AND name = ?",
            (user_id, groceries_id, "Produce"),
        ).fetchone()["id"]
        db.commit()

    new_form = client.get("/expenses/new")
    assert b"Subcategory" in new_form.data

    created = client.post(
        "/expenses/new",
        data={
            "date": "2026-06-02",
            "amount": "-33.50",
            "category_id": str(groceries_id),
            "subcategory_id": str(subcategory_id),
            "description": "Farmer market",
        },
        follow_redirects=True,
    )
    assert created.status_code == 200
    assert b">Groceries<" in created.data
    assert b">Produce<" in created.data

    with client.application.app_context():
        db = client.application.get_db()
        saved = db.execute(
            "SELECT id, category_id, subcategory_id, updated_at FROM expenses WHERE description = ?",
            ("Farmer market",),
        ).fetchone()

    assert saved["category_id"] == groceries_id
    assert saved["subcategory_id"] == subcategory_id

    edit_form = client.get(f"/expenses/{saved['id']}/edit")
    assert b"Subcategory" in edit_form.data
    assert b"Produce" in edit_form.data

    client.post(
        f"/expenses/{saved['id']}/edit",
        data={
            "date": "2026-06-02",
            "amount": "-33.50",
            "category_id": str(groceries_id),
            "subcategory_id": "",
            "description": "Farmer market",
            "updated_at": saved["updated_at"],
        },
        follow_redirects=True,
    )

    with client.application.app_context():
        db = client.application.get_db()
        edited = db.execute(
            "SELECT subcategory_id FROM expenses WHERE id = ?",
            (saved["id"],),
        ).fetchone()
    assert edited["subcategory_id"] is None


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
    parsed, diagnostics = parse_csv_transactions(rows, mapping, user_id=1)
    assert diagnostics["skipped_rows"] == 0
    assert parsed[0]["amount"] == -5.2
    assert parsed[1]["amount"] == 11.25


def test_amex_amount_is_normalized_to_canonical_sign_for_charges():
    rows = [["2026-09-10", "Restaurant", "20.00"]]
    mapping = {"date": "0", "description": "1", "amount": "2", "debit": "", "credit": "", "vendor": "", "category": ""}
    parsed, diagnostics = parse_csv_transactions(rows, mapping, user_id=1, bank_type="amex")
    assert diagnostics["skipped_rows"] == 0
    assert parsed[0]["amount"] == -20.0


def test_amex_payment_amount_embedded_in_description_is_extracted_and_cleaned():
    rows = [["2026-09-11", "Online payment -162.67", ""]]
    mapping = {"date": "0", "description": "1", "amount": "2", "debit": "", "credit": "", "vendor": "", "category": ""}
    parsed, diagnostics = parse_csv_transactions(rows, mapping, user_id=1, bank_type="amex")

    assert diagnostics["skipped_rows"] == 0
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
    assert "Suggested Subcategory" in html
    assert "<th>Source</th>" not in html
    assert "title=\"Source:" in html

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




def test_import_preview_subcategory_override_persists_and_imports(client):
    register(client)
    login(client)

    with client.application.app_context():
        db = client.application.get_db()
        groceries_id = db.execute("SELECT id FROM categories WHERE user_id = 1 AND name = 'Groceries'").fetchone()["id"]
        dairy_id = db.execute(
            "INSERT INTO subcategories (user_id, category_id, name) VALUES (?, ?, ?)",
            (1, groceries_id, "Dairy"),
        ).lastrowid
        db.execute(
            "INSERT INTO expenses (user_id, household_id, date, amount, category_id, subcategory_id, description, vendor, paid_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (1, 1, "2026-10-01", -9.99, groceries_id, dairy_id, "Milk", "Corner Store", "DK"),
        )
        db.commit()

    csv_content = "date,description,vendor,debit,credit\n2026-10-02,Milk,Corner Store,12.00,\n"
    preview_response = client.post(
        "/import/csv",
        data={"action": "preview", "csv_file": (io.BytesIO(csv_content.encode("utf-8")), "preview-sub.csv")},
        content_type="multipart/form-data",
    )
    html = preview_response.get_data(as_text=True)
    assert "Dairy" in html

    import_id = extract_import_id_from_html(html)
    with client.application.app_context():
        db = client.application.get_db()
        row_id = db.execute("SELECT id FROM import_staging WHERE import_id = ? ORDER BY id LIMIT 1", (import_id,)).fetchone()["id"]

    update_response = client.post(
        "/import/preview/row_update",
        json={"import_id": import_id, "row_id": row_id, "override_category": "Groceries", "override_subcategory": "Dairy"},
    )
    assert update_response.status_code == 200

    client.get(f"/import/csv?import_id={import_id}&show_all=1")

    confirm_response = client.post(
        "/import/csv",
        data={"action": "confirm", "import_id": import_id, "import_default_paid_by": "DK", "override_category_0": "Groceries", "override_subcategory_0": "Dairy"},
        follow_redirects=True,
    )
    assert confirm_response.status_code == 200

    with client.application.app_context():
        db = client.application.get_db()
        inserted = db.execute(
            """
            SELECT sc.name AS subcategory
            FROM expenses e
            LEFT JOIN subcategories sc ON sc.id = e.subcategory_id
            WHERE e.user_id = 1 AND e.date = '2026-10-02' AND e.description = 'Milk'
            ORDER BY e.id DESC LIMIT 1
            """
        ).fetchone()
        staged = json.loads(db.execute("SELECT row_json FROM import_staging WHERE id = ?", (row_id,)).fetchone()["row_json"])

    assert staged["override_subcategory"] == "Dairy"
    assert inserted["subcategory"] == "Dairy"



def test_import_preview_uses_mapped_csv_subcategory_when_valid_for_selected_category(client):
    register(client)
    login(client)

    with client.application.app_context():
        db = client.application.get_db()
        utilities_id = db.execute("SELECT id FROM categories WHERE user_id = 1 AND name = 'Utilities'").fetchone()["id"]
        db.execute(
            "INSERT INTO subcategories (user_id, category_id, name) VALUES (?, ?, ?)",
            (1, utilities_id, "Internet"),
        )
        db.commit()

    csv_content = "date,description,vendor,category,subcategory,debit,credit\n2026-10-02,ISP Bill,My ISP,Utilities,Internet,90.00,\n"
    preview_response = client.post(
        "/import/csv",
        data={
            "action": "preview",
            "map_date": "0",
            "map_description": "1",
            "map_vendor": "2",
            "map_category": "3",
            "map_subcategory": "4",
            "map_debit": "5",
            "map_credit": "6",
            "csv_file": (io.BytesIO(csv_content.encode("utf-8")), "preview-sub-valid.csv"),
        },
        content_type="multipart/form-data",
    )
    assert preview_response.status_code == 200
    html = preview_response.get_data(as_text=True)
    assert 'data-current-subcategory="Internet"' in html

    with client.session_transaction() as session_data:
        saved_mapping = session_data.get("csv_mapping")
    assert saved_mapping["subcategory_col"] == "4"


def test_import_preview_clears_mapped_csv_subcategory_when_invalid_for_selected_category(client):
    register(client)
    login(client)

    csv_content = "date,description,vendor,category,subcategory,debit,credit\n2026-10-02,ISP Bill,My ISP,Utilities,NotARealSubcategory,90.00,\n"
    preview_response = client.post(
        "/import/csv",
        data={
            "action": "preview",
            "map_date": "0",
            "map_description": "1",
            "map_vendor": "2",
            "map_category": "3",
            "map_subcategory": "4",
            "map_debit": "5",
            "map_credit": "6",
            "csv_file": (io.BytesIO(csv_content.encode("utf-8")), "preview-sub-invalid.csv"),
        },
        content_type="multipart/form-data",
    )
    assert preview_response.status_code == 200
    html = preview_response.get_data(as_text=True)
    assert 'data-current-subcategory=""' in html

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
        scope = "shared"
        if category == "Personal":
            if paid_by == "DK":
                scope = "dk_personal"
            elif paid_by == "YZ":
                scope = "yz_personal"
        db.execute(
            """
            INSERT INTO expenses (user_id, date, amount, category_id, description, paid_by, scope, is_transfer, is_personal)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                date,
                amount,
                category_row["id"] if category_row else None,
                f"{category} test",
                paid_by if paid_by is not None else "",
                scope,
                is_transfer,
                1 if category == "Personal" else 0,
            ),
        )
        db.commit()


def test_household_settlement_shared_math_sign_and_direction(client):
    register(client)
    login(client)

    _insert_expense(client, date="2026-02-02", amount=-100, category="Groceries", paid_by="DK")
    _insert_expense(client, date="2026-02-03", amount=-20, category="Groceries", paid_by="YZ")

    response = client.get("/dashboard?month=2026-02")
    text = response.get_data(as_text=True)

    assert "Total shared expenses (DK+YZ)</td><td>$120.00" in text
    assert "Shared settlement" in text
    assert "YZ→DK $40.00" in text
    assert "Net settlement (this period)" in text


def test_household_settlement_includes_positive_shared_reimbursement(client):
    register(client)
    login(client)

    _insert_expense(client, date="2026-02-02", amount=-100, category="Groceries", paid_by="DK")
    _insert_expense(client, date="2026-02-03", amount=40, category="Gifts", paid_by="YZ")

    response = client.get("/dashboard?month=2026-02")
    text = response.get_data(as_text=True)

    assert "Total shared expenses (DK+YZ)</td><td>$60.00" in text
    assert "DK paid (shared)</td><td>$100.00" in text
    assert "YZ paid (shared)</td><td>$-40.00" in text
    assert "Each share (50/50)</td><td>$30.00" in text
    assert "Shared settlement" in text
    assert "YZ→DK $70.00" in text


def test_household_settlement_excludes_positive_transfer_reimbursement(client):
    register(client)
    login(client)

    _insert_expense(client, date="2026-02-02", amount=-100, category="Groceries", paid_by="DK")
    _insert_expense(client, date="2026-02-03", amount=40, category="Gifts", paid_by="YZ", is_transfer=1)

    response = client.get("/dashboard?month=2026-02")
    text = response.get_data(as_text=True)

    assert "Total shared expenses (DK+YZ)</td><td>$100.00" in text
    assert "Each share (50/50)</td><td>$50.00" in text
    assert "YZ→DK $50.00" in text


def test_household_settlement_pet_rule_increases_period_delta(client):
    register(client)
    login(client)

    _insert_expense(client, date="2026-01-02", amount=-100, category="Pet Food & Care", paid_by="DK")
    _insert_expense(client, date="2026-01-03", amount=-60, category="Pet Food & Care", paid_by="YZ")

    response = client.get("/dashboard?month=2026-01")
    text = response.get_data(as_text=True)

    assert "Pet reimbursement (YZ→DK)</td><td>YZ→DK $100.00" in text
    assert "Net settlement (this period)" in text
    assert "YZ→DK $100.00" in text




def test_edit_repayment_updates_values(client):
    register(client)
    login(client)

    response = client.post(
        "/settlement-payments",
        data={
            "month": "2026-03",
            "date": "2026-03-10",
            "from_person": "DK",
            "to_person": "YZ",
            "amount": "30.00",
            "note": "initial",
        },
        follow_redirects=True,
    )
    assert response.status_code == 200

    with client.application.app_context():
        db = client.application.get_db()
        payment = db.execute("SELECT id FROM settlement_payments ORDER BY id DESC LIMIT 1").fetchone()

    response = client.post(
        f"/settlement-payments/{payment['id']}/edit",
        data={
            "month": "2026-03",
            "date": "2026-03-12",
            "from_person": "YZ",
            "to_person": "DK",
            "amount": "45.25",
            "note": "updated",
        },
        follow_redirects=True,
    )
    assert response.status_code == 200

    with client.application.app_context():
        db = client.application.get_db()
        updated = db.execute(
            "SELECT date, from_person, to_person, amount, note FROM settlement_payments WHERE id = ?",
            (payment["id"],),
        ).fetchone()

    assert updated["date"] == "2026-03-12"
    assert updated["from_person"] == "YZ"
    assert updated["to_person"] == "DK"
    assert float(updated["amount"]) == 45.25
    assert updated["note"] == "updated"

def test_repayments_affect_closing_balance_with_signs(client):
    register(client)
    login(client)

    _insert_expense(client, date="2026-03-02", amount=-200, category="Groceries", paid_by="DK")

    response = client.post(
        "/settlement-payments",
        data={"month": "2026-03", "date": "2026-03-10", "from_person": "DK", "to_person": "YZ", "amount": "30", "note": "payback"},
        follow_redirects=True,
    )
    assert response.status_code == 200
    response = client.post(
        "/settlement-payments",
        data={"month": "2026-03", "date": "2026-03-11", "from_person": "YZ", "to_person": "DK", "amount": "10", "note": "partial"},
        follow_redirects=True,
    )
    text = response.get_data(as_text=True)

    assert "Repayments DK→YZ (this period)</td><td>$30.00" in text
    assert "Repayments YZ→DK (this period)</td><td>$10.00" in text
    assert "Closing balance (life-to-date)</td><td>+120.00" in text


def test_monthly_breakdown_totals_row_and_owes_columns(client):
    register(client)
    login(client)

    _insert_expense(client, date="2026-01-03", amount=-100, category="Groceries", paid_by="DK")
    _insert_expense(client, date="2026-01-08", amount=-40, category="Pet Food & Care", paid_by="DK")
    _insert_expense(client, date="2026-02-02", amount=-90, category="Groceries", paid_by="YZ")

    response = client.get("/dashboard?start=2026-01-01&end=2026-02-28")
    text = response.get_data(as_text=True)

    assert "2026-01" in text and "$140.00" in text
    assert "2026-02" in text and "$90.00" in text
    assert "<td><strong>Total</strong></td>" in text
    assert "$230.00" in text


def test_monthly_breakdown_nets_positive_reimbursement(client):
    register(client)
    login(client)

    _insert_expense(client, date="2026-03-01", amount=-100, category="Groceries", paid_by="DK")
    _insert_expense(client, date="2026-03-05", amount=40, category="Gifts", paid_by="YZ")

    response = client.get("/dashboard?start=2026-03-01&end=2026-03-31")
    text = response.get_data(as_text=True)

    assert "2026-03" in text
    assert "$60.00" in text
    assert "$0.00" in text
    assert "$70.00" in text


def test_settlement_template_has_tab_labels(client):
    register(client)
    login(client)

    response = client.get("/dashboard?month=2026-02")
    text = response.get_data(as_text=True)

    assert 'Shared Expenses' in text
    assert 'Balance &amp; Repayments' in text
    assert 'Record Repayment' in text
    assert 'Monthly Breakdown' in text
    assert 'data-settlement-tab="shared-expenses-panel"' in text
    assert 'data-settlement-tab="monthly-breakdown-panel"' in text
    assert 'id="shared-expenses-section"' not in text


def test_dashboard_transactions_template_has_collapsible_bulk_and_filters(client):
    register(client)
    login(client)

    response = client.get("/dashboard?month=2026-02")
    text = response.get_data(as_text=True)

    assert '<details id="transactions-bulk-actions"' in text
    assert '<details id="transactions-filters"' in text


def test_dashboard_transactions_template_has_split_vendor_and_description_filters(client):
    register(client)
    login(client)

    response = client.get("/dashboard?month=2026-02")
    text = response.get_data(as_text=True)

    assert 'name="tx_vendor_q"' in text
    assert 'name="tx_description_q"' in text
    assert 'Vendor contains' in text
    assert 'Description contains' in text
    assert 'Vendor/Description contains' not in text


def test_dashboard_vendor_and_description_filters_can_be_combined(client):
    register(client)
    login(client)
    with client.application.app_context():
        db = client.application.get_db()
        groceries_id = db.execute("SELECT id FROM categories WHERE name = 'Groceries'").fetchone()["id"]

    client.post(
        "/expenses/new",
        data={"date": "2026-02-01", "amount": "-10", "category_id": str(groceries_id), "vendor": "Amazon", "description": "Gift card"},
        follow_redirects=True,
    )
    client.post(
        "/expenses/new",
        data={"date": "2026-02-02", "amount": "-20", "category_id": str(groceries_id), "vendor": "Amazon", "description": "Groceries"},
        follow_redirects=True,
    )
    client.post(
        "/expenses/new",
        data={"date": "2026-02-03", "amount": "-30", "category_id": str(groceries_id), "vendor": "Target", "description": "Gift card"},
        follow_redirects=True,
    )

    vendor_only = client.get("/dashboard?month=2026-02&tx_vendor_q=amazon")
    vendor_text = vendor_only.get_data(as_text=True)
    assert "Gift card" in vendor_text
    assert "Groceries" in vendor_text
    assert "Target" not in vendor_text

    description_only = client.get("/dashboard?month=2026-02&tx_description_q=gift")
    description_text = description_only.get_data(as_text=True)
    assert "Gift card" in description_text
    assert "Target" in description_text
    assert "Groceries" not in description_text

    combined = client.get("/dashboard?month=2026-02&tx_vendor_q=amazon&tx_description_q=gift")
    combined_text = combined.get_data(as_text=True)
    assert "Amazon" in combined_text
    assert "Gift card" in combined_text
    assert "Groceries" not in combined_text
    assert "Target" not in combined_text


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


def test_manual_add_and_edit_scope_saved(client):
    register(client)
    login(client)

    add_response = client.post(
        "/expenses/new",
        data={"date": "2026-03-05", "amount": "21", "category_id": "", "description": "Scoped", "paid_by": "DK", "scope": "dk_personal"},
        follow_redirects=True,
    )
    assert b"Expense added" in add_response.data

    with client.application.app_context():
        db = client.application.get_db()
        expense = db.execute("SELECT id, scope FROM expenses WHERE description = 'Scoped'").fetchone()
        assert expense["scope"] == "dk_personal"
        expense_id = expense["id"]

    edit_response = client.post(
        f"/expenses/{expense_id}/edit",
        data={"date": "2026-03-05", "amount": "21", "category_id": "", "description": "Scoped", "paid_by": "DK", "scope": "shared"},
        follow_redirects=True,
    )
    assert b"Expense updated" in edit_response.data

    with client.application.app_context():
        db = client.application.get_db()
        updated_scope = db.execute("SELECT scope FROM expenses WHERE id = ?", (expense_id,)).fetchone()["scope"]
    assert updated_scope == "shared"


def test_dashboard_scope_filter(client):
    register(client)
    login(client)

    client.post("/expenses/new", data={"date": "2026-03-01", "amount": "20", "category_id": "", "description": "Shared Row", "scope": "shared"}, follow_redirects=True)
    client.post("/expenses/new", data={"date": "2026-03-02", "amount": "10", "category_id": "", "description": "DK Personal Row", "scope": "dk_personal", "paid_by": "DK"}, follow_redirects=True)

    response = client.get("/dashboard?month=2026-03&tx_scope=dk_personal")
    html = response.get_data(as_text=True)
    assert "DK Personal Row" in html
    assert "Shared Row" not in html


def test_settlement_uses_scope_and_excludes_personal_scopes(client):
    register(client)
    login(client)

    with client.application.app_context():
        db = client.application.get_db()
        user_id = db.execute("SELECT id FROM users WHERE username = 'user1'").fetchone()["id"]
        groceries_id = db.execute("SELECT id FROM categories WHERE user_id = ? AND name = 'Groceries'", (user_id,)).fetchone()["id"]
        db.execute(
            """
            INSERT INTO expenses (user_id, household_id, date, amount, category_id, description, paid_by, scope, is_transfer, is_personal)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0)
            """,
            (user_id, 1, "2026-04-01", -100.0, groceries_id, "Shared expense", "DK", "shared"),
        )
        db.execute(
            """
            INSERT INTO expenses (user_id, household_id, date, amount, category_id, description, paid_by, scope, is_transfer, is_personal)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0)
            """,
            (user_id, 1, "2026-04-02", -500.0, groceries_id, "Personal expense", "DK", "dk_personal"),
        )
        db.commit()

    response = client.get("/dashboard?month=2026-04")
    text = response.get_data(as_text=True)
    assert "Total shared expenses (DK+YZ)</td><td>$100.00" in text




def test_dashboard_row_actions_include_current_filter_state(client):
    register(client)
    login(client)

    client.post(
        "/expenses/new",
        data={"date": "2026-03-05", "amount": "21", "category_id": "", "description": "Manual", "vendor": "Shop", "paid_by": "DK"},
        follow_redirects=True,
    )

    response = client.get(
        "/dashboard?month=2026-03&tx_vendor_q=shop&tx_description_q=manual&tx_transfer_mode=exclude&settlement_tab=record-repayment-panel&spend_mode=ytd"
    )
    html = response.get_data(as_text=True)

    assert "/expenses/1/edit?month=2026-03" in html
    assert "tx_vendor_q=shop" in html
    assert "tx_description_q=manual" in html
    assert "tx_transfer_mode=exclude" in html
    assert "settlement_tab=record-repayment-panel" in html
    assert "spend_mode=ytd" in html
    assert 'name="tx_vendor_q" value="shop"' in html
    assert 'name="settlement_tab" value="record-repayment-panel"' in html
    assert 'name="spend_mode" value="ytd"' in html


def test_edit_expense_redirects_back_with_dashboard_state(client):
    register(client)
    login(client)

    client.post(
        "/expenses/new",
        data={"date": "2026-03-05", "amount": "21", "category_id": "", "description": "Manual", "vendor": "Shop", "paid_by": "DK"},
        follow_redirects=True,
    )

    response = client.post(
        "/expenses/1/edit",
        data={
            "date": "2026-03-05",
            "amount": "21",
            "category_id": "",
            "description": "Manual",
            "vendor": "Shop",
            "paid_by": "YZ",
            "month": "2026-03",
            "tx_vendor_q": "shop",
            "tx_transfer_mode": "exclude",
            "settlement_tab": "monthly-breakdown-panel",
            "spend_mode": "ytd",
        },
        follow_redirects=False,
    )

    location = response.headers["Location"]
    assert response.status_code == 302
    assert "/dashboard?" in location
    assert "month=2026-03" in location
    assert "tx_vendor_q=shop" in location
    assert "tx_transfer_mode=exclude" in location
    assert "settlement_tab=monthly-breakdown-panel" in location
    assert "spend_mode=ytd" in location


def test_delete_expense_redirects_back_with_dashboard_state(client):
    register(client)
    login(client)

    client.post(
        "/expenses/new",
        data={"date": "2026-03-05", "amount": "21", "category_id": "", "description": "Manual", "vendor": "Shop", "paid_by": "DK"},
        follow_redirects=True,
    )

    response = client.post(
        "/expenses/1/delete",
        data={
            "month": "2026-03",
            "tx_vendor_q": "shop",
            "tx_transfer_mode": "exclude",
            "settlement_tab": "record-repayment-panel",
            "spend_mode": "ytd",
        },
        follow_redirects=False,
    )

    location = response.headers["Location"]
    assert response.status_code == 302
    assert "/dashboard?" in location
    assert "month=2026-03" in location
    assert "tx_vendor_q=shop" in location
    assert "tx_transfer_mode=exclude" in location
    assert "settlement_tab=record-repayment-panel" in location
    assert "spend_mode=ytd" in location


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


def test_import_confirm_persists_mapped_scope_column(client):
    register(client)
    login(client)

    csv_content = (
        "date,description,amount,paid_by,category,scope\n"
        "2026-11-05,Scoped import,-20.00,DK,Groceries,DK Personal\n"
    )
    preview = client.post(
        "/import/csv",
        data={
            "action": "preview",
            "map_date": "0",
            "map_description": "1",
            "map_amount": "2",
            "map_paid_by": "3",
            "map_category": "4",
            "map_scope": "5",
            "csv_file": (io.BytesIO(csv_content.encode("utf-8")), "scope.csv"),
        },
        content_type="multipart/form-data",
    )
    assert preview.status_code == 200
    import_id = extract_import_id_from_html(preview.get_data(as_text=True))

    response = client.post(
        "/import/csv",
        data={"action": "confirm", "import_id": import_id, "import_default_paid_by": "DK"},
        follow_redirects=True,
    )
    assert b"Imported 1 transaction(s)." in response.data

    with client.application.app_context():
        db = client.application.get_db()
        row = db.execute("SELECT scope FROM expenses WHERE description = 'Scoped import'").fetchone()
    assert row["scope"] == "dk_personal"


def test_import_preview_displays_mapped_scope(client):
    register(client)
    login(client)

    csv_content = (
        "date,description,amount,paid_by,category,scope\n"
        "2026-11-06,Scope preview row,-21.00,YZ,Groceries,YZ Personal\n"
    )
    preview = client.post(
        "/import/csv",
        data={
            "action": "preview",
            "map_date": "0",
            "map_description": "1",
            "map_amount": "2",
            "map_paid_by": "3",
            "map_category": "4",
            "map_scope": "5",
            "csv_file": (io.BytesIO(csv_content.encode("utf-8")), "scope-preview.csv"),
        },
        content_type="multipart/form-data",
    )

    html = preview.get_data(as_text=True)
    assert preview.status_code == 200
    assert "Scope" in html
    assert "YZ Personal" in html


def test_import_confirm_scope_fallback_without_mapped_scope_column(client):
    register(client)
    login(client)

    with client.application.app_context():
        db = client.application.get_db()
        personal_id = db.execute("SELECT id FROM categories WHERE user_id = ? AND name = 'Personal'", (1,)).fetchone()["id"]
        groceries_id = db.execute("SELECT id FROM categories WHERE user_id = ? AND name = 'Groceries'", (1,)).fetchone()["id"]

    rows = [
        {
            "row_index": 0,
            "user_id": 1,
            "date": "2026-11-07",
            "amount": -12.0,
            "description": "Fallback DK personal",
            "normalized_description": "fallback dk personal",
            "vendor": "Store",
            "category": "Personal",
            "category_id": personal_id,
            "paid_by": "DK",
        },
        {
            "row_index": 1,
            "user_id": 1,
            "date": "2026-11-08",
            "amount": -13.0,
            "description": "Fallback shared",
            "normalized_description": "fallback shared",
            "vendor": "Store",
            "category": "Groceries",
            "category_id": groceries_id,
            "paid_by": "YZ",
        },
    ]

    response = confirm_import(client, rows, import_default_paid_by="")
    assert b"Imported 2 transaction(s)." in response.data

    with client.application.app_context():
        db = client.application.get_db()
        imported = db.execute(
            "SELECT description, scope FROM expenses WHERE description IN ('Fallback DK personal', 'Fallback shared') ORDER BY description"
        ).fetchall()
    scopes = {row["description"]: row["scope"] for row in imported}
    assert scopes["Fallback DK personal"] == "dk_personal"
    assert scopes["Fallback shared"] == "shared"


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


def test_import_preview_bulk_apply_paid_by_overwrites_selected_rows(client):
    register(client)
    login(client)

    rows = []
    for idx, paid_by in enumerate(["", "YZ", "DK", "", "YZ"]):
        rows.append(
            {
                "row_index": idx,
                "user_id": 1,
                "date": "2026-12-01",
                "amount": -10.0 - idx,
                "description": f"Bulk paid row {idx}",
                "normalized_description": f"bulk paid row {idx}",
                "vendor": f"Vendor {idx}",
                "category": "Groceries",
                "confidence": 90,
                "confidence_label": "High",
                "suggested_source": "rule",
                "paid_by": paid_by,
            }
        )

    import_id = stage_import_preview(client, rows, preview_id="bulk-paid-by")

    with client.application.app_context():
        db = client.application.get_db()
        staging_ids = [row["id"] for row in db.execute("SELECT id FROM import_staging WHERE import_id = ? ORDER BY id", (import_id,)).fetchall()]

    response = client.post(
        "/import/preview/action",
        data={
            "import_id": import_id,
            "action": "apply_paid_by_selected",
            "paid_by_value": "DK",
            "selected_row_ids": [str(staging_ids[0]), str(staging_ids[2]), str(staging_ids[4])],
        },
        follow_redirects=True,
    )
    assert b"Updated Paid by for 3 rows." in response.data

    with client.application.app_context():
        db = client.application.get_db()
        staged_rows = [json.loads(row["row_json"]) for row in db.execute("SELECT row_json FROM import_staging WHERE import_id = ? ORDER BY id", (import_id,)).fetchall()]

    assert staged_rows[0]["paid_by"] == "DK"
    assert staged_rows[1]["paid_by"] == "YZ"
    assert staged_rows[2]["paid_by"] == "DK"
    assert staged_rows[3]["paid_by"] == ""
    assert staged_rows[4]["paid_by"] == "DK"


def test_import_preview_bulk_apply_category_overwrites_selected_rows(client):
    register(client)
    login(client)

    client.post("/categories", data={"name": "Bulk Category Override"}, follow_redirects=True)

    with client.application.app_context():
        db = client.application.get_db()
        category_id = db.execute(
            "SELECT id FROM categories WHERE user_id = 1 AND name = 'Bulk Category Override'"
        ).fetchone()["id"]

    rows = []
    for idx in range(4):
        rows.append(
            {
                "row_index": idx,
                "user_id": 1,
                "date": "2026-12-02",
                "amount": -20.0 - idx,
                "description": f"Bulk category row {idx}",
                "normalized_description": f"bulk category row {idx}",
                "vendor": f"Vendor {idx}",
                "category": "Groceries",
                "confidence": 90,
                "confidence_label": "High",
                "suggested_source": "rule",
                "paid_by": "DK",
            }
        )

    import_id = stage_import_preview(client, rows, preview_id="bulk-category")

    with client.application.app_context():
        db = client.application.get_db()
        staging_ids = [row["id"] for row in db.execute("SELECT id FROM import_staging WHERE import_id = ? ORDER BY id", (import_id,)).fetchall()]

    response = client.post(
        "/import/preview/action",
        data={
            "import_id": import_id,
            "action": "apply_category_selected",
            "category_id": str(category_id),
            "selected_row_ids": [str(staging_ids[1]), str(staging_ids[3])],
        },
        follow_redirects=True,
    )
    assert b"Updated Category for 2 rows." in response.data

    with client.application.app_context():
        db = client.application.get_db()
        staged_rows = [json.loads(row["row_json"]) for row in db.execute("SELECT row_json FROM import_staging WHERE import_id = ? ORDER BY id", (import_id,)).fetchall()]

    assert staged_rows[0]["category"] == "Groceries"
    assert staged_rows[1]["category"] == "Bulk Category Override"
    assert staged_rows[1]["category_id"] == category_id
    assert staged_rows[1]["category_name"] == "Bulk Category Override"
    assert staged_rows[2]["category"] == "Groceries"
    assert staged_rows[3]["category"] == "Bulk Category Override"




def test_import_preview_category_selected_persists_for_all_selected_and_confirm(client):
    register(client)
    login(client)

    client.post("/categories", data={"name": "Selected Bulk Category"}, follow_redirects=True)
    with client.application.app_context():
        db = client.application.get_db()
        category_id = db.execute(
            "SELECT id FROM categories WHERE user_id = 1 AND name = 'Selected Bulk Category'"
        ).fetchone()["id"]

    rows = [
        {"row_index": 0, "user_id": 1, "date": "2026-12-08", "amount": -11.0, "description": "Selected A", "normalized_description": "selected a", "vendor": "Bulk", "category": "", "confidence": 20, "confidence_label": "Low", "suggested_source": "unknown", "paid_by": "DK"},
        {"row_index": 1, "user_id": 1, "date": "2026-12-08", "amount": -12.0, "description": "Selected B", "normalized_description": "selected b", "vendor": "Bulk", "category": "", "confidence": 20, "confidence_label": "Low", "suggested_source": "unknown", "paid_by": "DK"},
        {"row_index": 2, "user_id": 1, "date": "2026-12-08", "amount": -13.0, "description": "Selected C", "normalized_description": "selected c", "vendor": "Bulk", "category": "", "confidence": 20, "confidence_label": "Low", "suggested_source": "unknown", "paid_by": "DK"},
        {"row_index": 3, "user_id": 1, "date": "2026-12-08", "amount": -14.0, "description": "Unselected D", "normalized_description": "unselected d", "vendor": "Bulk", "category": "", "confidence": 20, "confidence_label": "Low", "suggested_source": "unknown", "paid_by": "DK"},
    ]
    import_id = stage_import_preview(client, rows, preview_id="selected-category-end-to-end")

    with client.application.app_context():
        db = client.application.get_db()
        staged = db.execute("SELECT id FROM import_staging WHERE import_id = ? ORDER BY id", (import_id,)).fetchall()
        staged_ids = [row["id"] for row in staged]

    client.post("/import/preview/selection", json={"import_id": import_id, "row_id": staged_ids[3], "selected": False})

    apply_response = client.post(
        "/import/preview/category_selected",
        json={"import_id": import_id, "category_id": str(category_id)},
    )
    assert apply_response.status_code == 200
    assert apply_response.get_json()["updated"] == 3

    confirm_response = client.post(
        "/import/csv",
        data={"action": "confirm", "import_id": import_id},
        follow_redirects=True,
    )
    assert b"Imported 3 transaction(s)." in confirm_response.data

    with client.application.app_context():
        db = client.application.get_db()
        imported = db.execute(
            """
            SELECT e.description, c.name AS category
            FROM expenses e
            LEFT JOIN categories c ON c.id = e.category_id
            WHERE e.description IN ('Selected A', 'Selected B', 'Selected C', 'Unselected D')
            ORDER BY e.description
            """
        ).fetchall()

    assert [row["description"] for row in imported] == ["Selected A", "Selected B", "Selected C"]
    assert all(row["category"] == "Selected Bulk Category" for row in imported)


def test_import_confirm_uses_first_submit_selected_ids_and_category_overrides(client):
    register(client)
    login(client)

    client.post("/categories", data={"name": "Immediate Confirm Category"}, follow_redirects=True)

    rows = [
        {"row_index": 0, "user_id": 1, "date": "2026-12-20", "amount": -11.0, "description": "Immediate row A", "normalized_description": "immediate row a", "vendor": "Now", "category": "", "confidence": 20, "confidence_label": "Low", "suggested_source": "unknown", "paid_by": "DK"},
        {"row_index": 1, "user_id": 1, "date": "2026-12-20", "amount": -12.0, "description": "Immediate row B", "normalized_description": "immediate row b", "vendor": "Now", "category": "", "confidence": 20, "confidence_label": "Low", "suggested_source": "unknown", "paid_by": "DK"},
        {"row_index": 2, "user_id": 1, "date": "2026-12-20", "amount": -13.0, "description": "Immediate row C", "normalized_description": "immediate row c", "vendor": "Now", "category": "", "confidence": 20, "confidence_label": "Low", "suggested_source": "unknown", "paid_by": "DK"},
    ]
    import_id = stage_import_preview(client, rows, preview_id="confirm-first-submit")

    with client.application.app_context():
        db = client.application.get_db()
        staged_ids = [row["id"] for row in db.execute("SELECT id FROM import_staging WHERE import_id = ? ORDER BY id", (import_id,)).fetchall()]

    client.post("/import/preview/selection/bulk", json={"import_id": import_id, "selected": False, "scope": "all"})

    response = client.post(
        "/import/csv",
        data={
            "action": "confirm",
            "import_id": import_id,
            "selected_row_ids": [str(staged_ids[0]), str(staged_ids[2])],
            "override_category_0": "Immediate Confirm Category",
            "override_category_1": "",
            "override_category_2": "Immediate Confirm Category",
        },
        follow_redirects=True,
    )
    assert b"Imported 2 transaction(s)." in response.data

    with client.application.app_context():
        db = client.application.get_db()
        imported = db.execute(
            """
            SELECT e.description, c.name AS category
            FROM expenses e
            LEFT JOIN categories c ON c.id = e.category_id
            WHERE e.description IN ('Immediate row A', 'Immediate row B', 'Immediate row C')
            ORDER BY e.description
            """
        ).fetchall()

    assert [row["description"] for row in imported] == ["Immediate row A", "Immediate row C"]
    assert all(row["category"] == "Immediate Confirm Category" for row in imported)


def test_import_confirm_first_attempt_succeeds_without_retry_after_selection_change(client):
    register(client)
    login(client)

    rows = [
        {"row_index": 0, "user_id": 1, "date": "2026-12-21", "amount": -21.0, "description": "One-click row A", "normalized_description": "one-click row a", "vendor": "One", "category": "Groceries", "confidence": 90, "confidence_label": "High", "suggested_source": "rule", "paid_by": "DK", "selected": False},
        {"row_index": 1, "user_id": 1, "date": "2026-12-21", "amount": -22.0, "description": "One-click row B", "normalized_description": "one-click row b", "vendor": "One", "category": "Groceries", "confidence": 90, "confidence_label": "High", "suggested_source": "rule", "paid_by": "DK", "selected": False},
    ]
    import_id = stage_import_preview(client, rows, preview_id="confirm-no-retry")

    with client.application.app_context():
        db = client.application.get_db()
        staged_ids = [row["id"] for row in db.execute("SELECT id FROM import_staging WHERE import_id = ? ORDER BY id", (import_id,)).fetchall()]

    response = client.post(
        "/import/csv",
        data={
            "action": "confirm",
            "import_id": import_id,
            "selected_row_ids": [str(staged_ids[0]), str(staged_ids[1])],
        },
        follow_redirects=True,
    )

    assert b"Imported 2 transaction(s)." in response.data

def test_import_confirm_uses_staging_bulk_edits(client):
    register(client)
    login(client)

    client.post("/categories", data={"name": "Staged Bulk Category"}, follow_redirects=True)
    with client.application.app_context():
        db = client.application.get_db()
        category_id = db.execute("SELECT id FROM categories WHERE user_id = 1 AND name = 'Staged Bulk Category'").fetchone()["id"]

    rows = [
        {
            "row_index": 0,
            "user_id": 1,
            "date": "2026-12-03",
            "amount": -31.0,
            "description": "Bulk edited import row",
            "normalized_description": "bulk edited import row",
            "vendor": "Bulk Vendor",
            "category": "Groceries",
            "confidence": 90,
            "confidence_label": "High",
            "suggested_source": "rule",
            "paid_by": "",
        },
        {
            "row_index": 1,
            "user_id": 1,
            "date": "2026-12-03",
            "amount": -32.0,
            "description": "Untouched import row",
            "normalized_description": "untouched import row",
            "vendor": "Other Vendor",
            "category": "Groceries",
            "confidence": 90,
            "confidence_label": "High",
            "suggested_source": "rule",
            "paid_by": "YZ",
        },
    ]
    import_id = stage_import_preview(client, rows, preview_id="confirm-staged-edits")

    with client.application.app_context():
        db = client.application.get_db()
        first_id = db.execute("SELECT id FROM import_staging WHERE import_id = ? ORDER BY id LIMIT 1", (import_id,)).fetchone()["id"]

    client.post(
        "/import/preview/action",
        data={
            "import_id": import_id,
            "action": "apply_paid_by_selected",
            "paid_by_value": "DK",
            "selected_row_ids": [str(first_id)],
        },
        follow_redirects=True,
    )
    client.post(
        "/import/preview/action",
        data={
            "import_id": import_id,
            "action": "apply_category_selected",
            "category_id": str(category_id),
            "selected_row_ids": [str(first_id)],
        },
        follow_redirects=True,
    )

    response = client.post(
        "/import/csv",
        data={"action": "confirm", "import_id": import_id},
        follow_redirects=True,
    )
    assert b"Imported 2 transaction(s)." in response.data

    with client.application.app_context():
        db = client.application.get_db()
        edited = db.execute(
            """
            SELECT e.paid_by, c.name as category
            FROM expenses e
            LEFT JOIN categories c ON c.id = e.category_id
            WHERE e.description = 'Bulk edited import row'
            """
        ).fetchone()
        untouched = db.execute(
            "SELECT paid_by FROM expenses WHERE description = 'Untouched import row'"
        ).fetchone()
        remaining_staging = db.execute("SELECT COUNT(*) as c FROM import_staging WHERE import_id = ?", (import_id,)).fetchone()["c"]

    assert edited["paid_by"] == "DK"
    assert edited["category"] == "Staged Bulk Category"
    assert untouched["paid_by"] == "YZ"
    assert remaining_staging == 2


def test_import_confirm_uses_row_level_category_override_without_apply_all_matching(client):
    register(client)
    login(client)

    rows = [
        {
            "row_index": 0,
            "user_id": 1,
            "date": "2026-12-05",
            "amount": -14.0,
            "description": "Single row category override",
            "normalized_description": "single row category override",
            "vendor": "Local Store",
            "category": "",
            "confidence": 25,
            "confidence_label": "Low",
            "suggested_source": "unknown",
            "paid_by": "DK",
        }
    ]
    import_id = stage_import_preview(client, rows, preview_id="row-override-no-apply-all")

    with client.application.app_context():
        db = client.application.get_db()
        row_id = db.execute("SELECT id FROM import_staging WHERE import_id = ?", (import_id,)).fetchone()["id"]

    update_response = client.post(
        "/import/preview/row_update",
        json={"import_id": import_id, "row_id": row_id, "override_category": "Groceries"},
    )
    assert update_response.status_code == 200

    confirm_response = client.post(
        "/import/csv",
        data={"action": "confirm", "import_id": import_id},
        follow_redirects=True,
    )
    assert b"Imported 1 transaction(s)." in confirm_response.data

    with client.application.app_context():
        db = client.application.get_db()
        expense = db.execute(
            """
            SELECT c.name as category
            FROM expenses e
            LEFT JOIN categories c ON c.id = e.category_id
            WHERE e.description = 'Single row category override'
            """
        ).fetchone()

    assert expense["category"] == "Groceries"


def test_row_level_category_override_survives_preview_filter_toggle_and_confirm(client):
    register(client)
    login(client)

    rows = [
        {
            "row_index": 0,
            "user_id": 1,
            "date": "2026-12-06",
            "amount": -9.0,
            "description": "Filter toggle override row",
            "normalized_description": "filter toggle override row",
            "vendor": "Market",
            "category": "",
            "confidence": 25,
            "confidence_label": "Low",
            "suggested_source": "unknown",
            "paid_by": "DK",
        },
        {
            "row_index": 1,
            "user_id": 1,
            "date": "2026-12-06",
            "amount": -12.0,
            "description": "High confidence row",
            "normalized_description": "high confidence row",
            "vendor": "Cafe",
            "category": "Restaurants",
            "confidence": 90,
            "confidence_label": "High",
            "suggested_source": "rule",
            "paid_by": "YZ",
        },
    ]
    import_id = stage_import_preview(client, rows, preview_id="row-override-filter-toggle")

    with client.application.app_context():
        db = client.application.get_db()
        row_id = db.execute("SELECT id FROM import_staging WHERE import_id = ? ORDER BY id LIMIT 1", (import_id,)).fetchone()["id"]

    update_response = client.post(
        "/import/preview/row_update",
        json={"import_id": import_id, "row_id": row_id, "override_category": "Groceries"},
    )
    assert update_response.status_code == 200

    preview_response = client.get(f"/import/csv?import_id={import_id}&low_confidence=1", follow_redirects=True)
    assert preview_response.status_code == 200

    confirm_response = client.post(
        "/import/csv",
        data={"action": "confirm", "import_id": import_id},
        follow_redirects=True,
    )
    assert b"Imported 2 transaction(s)." in confirm_response.data

    with client.application.app_context():
        db = client.application.get_db()
        expense = db.execute(
            """
            SELECT c.name as category
            FROM expenses e
            LEFT JOIN categories c ON c.id = e.category_id
            WHERE e.description = 'Filter toggle override row'
            """
        ).fetchone()

    assert expense["category"] == "Groceries"


def test_import_preview_bulk_action_requires_selection(client):
    register(client)
    login(client)

    rows = [
        {
            "row_index": 0,
            "user_id": 1,
            "date": "2026-12-04",
            "amount": -8.0,
            "description": "No selection row",
            "normalized_description": "no selection row",
            "vendor": "Vendor",
            "category": "Groceries",
            "confidence": 90,
            "confidence_label": "High",
            "suggested_source": "rule",
            "paid_by": "YZ",
        }
    ]
    import_id = stage_import_preview(client, rows, preview_id="no-selection")

    response = client.post(
        "/import/preview/action",
        data={
            "import_id": import_id,
            "action": "apply_paid_by_selected",
            "paid_by_value": "DK",
        },
        follow_redirects=True,
    )
    assert b"Please select at least one row first." in response.data

    with client.application.app_context():
        db = client.application.get_db()
        row = db.execute("SELECT row_json FROM import_staging WHERE import_id = ?", (import_id,)).fetchone()
    assert json.loads(row["row_json"])["paid_by"] == "YZ"


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


def test_import_confirm_persists_staging_rows_with_results_after_import(client):
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
        outcome = db.execute("SELECT import_status FROM import_staging WHERE import_id = ?", (import_id,)).fetchone()["import_status"]
    assert count == 1
    assert outcome == "inserted"


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

def test_import_preview_shows_unknown_csv_category_unmapped(client):
    register(client)
    login(client)

    rows = [
        {
            "user_id": 1,
            "row_index": 0,
            "date": "2026-02-01",
            "amount": -15.0,
            "description": "Camp fee",
            "normalized_description": "camp fee",
            "vendor": "Camp",
            "category": "",
            "csv_category_name": "  David   Camp ",
            "csv_category_match_status": "unknown",
            "category_id": None,
            "mapped_category_id": None,
            "confidence": 25,
            "confidence_label": "Low",
            "suggested_source": "unknown_csv_category",
            "vendor_key": "camp",
            "vendor_rule_key": "camp",
            "description_rule_key": "camp fee",
            "paid_by": "DK",
        }
    ]

    import_id = stage_import_preview(client, rows, preview_id="preview-unknown-csv-category")
    response = client.get(f"/import/csv?import_id={import_id}")
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "Unknown Categories" in html
    assert "David   Camp" in html
    assert "Unknown category:   David   Camp" in html


def test_import_apply_all_unknown_category_mappings_updates_all_rows(client):
    register(client)
    login(client)

    with client.application.app_context():
        db = client.application.get_db()
        restaurants = db.execute(
            "SELECT id FROM categories WHERE user_id = 1 AND name = 'Restaurants'"
        ).fetchone()["id"]

    rows = [
        {
            "user_id": 1,
            "row_index": 0,
            "date": "2026-02-01",
            "amount": -25.0,
            "description": "Practice one",
            "normalized_description": "practice one",
            "vendor": "Sports",
            "category": "",
            "csv_category_name": "Hockey training",
            "csv_category_match_status": "unknown",
            "category_id": None,
            "mapped_category_id": None,
            "confidence": 25,
            "confidence_label": "Low",
            "suggested_source": "unknown_csv_category",
            "vendor_key": "sports",
            "vendor_rule_key": "sports",
            "description_rule_key": "practice one",
            "paid_by": "DK",
        },
        {
            "user_id": 1,
            "row_index": 1,
            "date": "2026-02-02",
            "amount": -35.0,
            "description": "Practice two",
            "normalized_description": "practice two",
            "vendor": "Sports",
            "category": "",
            "csv_category_name": "Hockey training",
            "csv_category_match_status": "unknown",
            "category_id": None,
            "mapped_category_id": None,
            "confidence": 25,
            "confidence_label": "Low",
            "suggested_source": "unknown_csv_category",
            "vendor_key": "sports",
            "vendor_rule_key": "sports",
            "description_rule_key": "practice two",
            "paid_by": "DK",
        },
    ]

    import_id = stage_import_preview(client, rows, preview_id="preview-map-unknown")
    response = client.post(
        "/import/csv",
        data={
            "action": "apply_all_mappings",
            "import_id": import_id,
            "map_unknown::Hockey training": str(restaurants),
            "apply_unknown_all::Hockey training": "1",
        },
        follow_redirects=True,
    )
    assert b"Applied mappings to 2 row(s)." in response.data

    with client.application.app_context():
        db = client.application.get_db()
        staged_rows = [
            json.loads(row["row_json"])
            for row in db.execute(
                "SELECT row_json FROM import_staging WHERE import_id = ? ORDER BY id", (import_id,)
            ).fetchall()
        ]

    assert all(row["mapped_category_id"] == restaurants for row in staged_rows)
    assert all(row["category_id"] == restaurants for row in staged_rows)
    assert all(row["csv_category_match_status"] == "mapped" for row in staged_rows)


def test_import_confirm_uses_mapped_category_and_never_creates_categories(client):
    register(client)
    login(client)

    with client.application.app_context():
        db = client.application.get_db()
        restaurants = db.execute(
            "SELECT id FROM categories WHERE user_id = 1 AND name = 'Restaurants'"
        ).fetchone()["id"]
        category_count_before = db.execute(
            "SELECT COUNT(*) AS c FROM categories WHERE user_id = 1"
        ).fetchone()["c"]

    rows = [
        {
            "user_id": 1,
            "row_index": 0,
            "date": "2026-04-01",
            "amount": -45.0,
            "description": "Mapped row",
            "normalized_description": "mapped row",
            "vendor": "Vendor A",
            "category": "Restaurants",
            "csv_category_name": "Hockey training",
            "csv_category_match_status": "mapped",
            "category_id": restaurants,
            "mapped_category_id": restaurants,
            "confidence": 100,
            "confidence_label": "High",
            "suggested_source": "csv_mapped",
            "vendor_key": "vendor a",
            "vendor_rule_key": "vendor a",
            "description_rule_key": "mapped row",
            "paid_by": "DK",
        },
        {
            "user_id": 1,
            "row_index": 1,
            "date": "2026-04-02",
            "amount": -20.0,
            "description": "Unmapped row",
            "normalized_description": "unmapped row",
            "vendor": "Vendor B",
            "category": "",
            "csv_category_name": "David Camp",
            "csv_category_match_status": "unknown",
            "category_id": None,
            "mapped_category_id": None,
            "confidence": 25,
            "confidence_label": "Low",
            "suggested_source": "unknown_csv_category",
            "vendor_key": "vendor b",
            "vendor_rule_key": "vendor b",
            "description_rule_key": "unmapped row",
            "paid_by": "DK",
        },
    ]

    response = confirm_import(client, rows)
    assert b"Imported 2 transaction(s)." in response.data
    assert b"1 rows imported as Uncategorized because CSV categories were not mapped." in response.data

    with client.application.app_context():
        db = client.application.get_db()
        expenses = db.execute(
            "SELECT description, category_id, category_source FROM expenses ORDER BY date"
        ).fetchall()
        category_count_after = db.execute(
            "SELECT COUNT(*) AS c FROM categories WHERE user_id = 1"
        ).fetchone()["c"]

    assert expenses[0]["description"] == "Mapped row"
    assert expenses[0]["category_id"] == restaurants
    assert expenses[1]["description"] == "Unmapped row"
    assert expenses[1]["category_id"] is None
    assert expenses[1]["category_source"] == "unknown_csv_category"
    assert category_count_after == category_count_before



def test_postgres_login_default_categories_idempotent(monkeypatch):
    database_url = get_test_postgres_url()

    monkeypatch.setenv("TEST_DATABASE_URL", database_url)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    app = create_app({"TESTING": True, "SECRET_KEY": "test"})

    with app.app_context():
        app.init_db()
        db = app.get_db()
        assert db.config["database_name"] != LIVE_DB_NAME, "Tests must never use live database expense_tracker"

    client = app.test_client()
    username = f"pg_user_{datetime.utcnow().timestamp()}"
    register_response = register(client, username=username, password="password")
    assert register_response.status_code == 200

    first_login = login(client, username=username, password="password")
    assert first_login.status_code == 200

    second_login = login(client, username=username, password="password")
    assert second_login.status_code == 200

    with app.app_context():
        db = app.get_db()
        user = db.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone()
        total_count = db.execute("SELECT COUNT(*) FROM categories WHERE user_id = ?", (user["id"],)).fetchone()[0]
        distinct_count = db.execute(
            "SELECT COUNT(DISTINCT name) FROM categories WHERE user_id = ?",
            (user["id"],),
        ).fetchone()[0]

    assert total_count == len(DEFAULT_CATEGORIES)
    assert distinct_count == len(DEFAULT_CATEGORIES)


def test_import_confirm_manual_tracker_positive_amount_is_inserted_negative(client):
    register(client)
    login(client)

    rows = [
        {
            "row_index": 0,
            "user_id": 1,
            "date": "2026-01-10",
            "amount": 719.73,
            "description": "School supplies",
            "normalized_description": "school supplies",
            "vendor": "school store",
            "category": "School & Education",
            "source_type": "manual_tracker",
            "is_refund_or_payment": False,
            "paid_by": "DK",
        }
    ]

    response = confirm_import(client, rows, import_default_paid_by="DK")
    assert response.status_code == 200

    with client.application.app_context():
        db = client.application.get_db()
        expense = db.execute("SELECT amount FROM expenses WHERE description = ?", ("School supplies",)).fetchone()
    assert expense is not None
    assert expense["amount"] == -719.73


def test_import_confirm_manual_tracker_negative_amount_stays_negative(client):
    register(client)
    login(client)

    rows = [
        {
            "row_index": 0,
            "user_id": 1,
            "date": "2026-01-10",
            "amount": -50.59,
            "description": "Groceries",
            "normalized_description": "groceries",
            "vendor": "market",
            "category": "Groceries",
            "source_type": "manual_tracker",
            "is_refund_or_payment": False,
            "paid_by": "DK",
        }
    ]

    response = confirm_import(client, rows, import_default_paid_by="DK")
    assert response.status_code == 200

    with client.application.app_context():
        db = client.application.get_db()
        expense = db.execute("SELECT amount FROM expenses WHERE description = ?", ("Groceries",)).fetchone()
    assert expense is not None
    assert expense["amount"] == -50.59


def test_import_confirm_manual_tracker_refund_is_inserted_positive(client):
    register(client)
    login(client)

    rows = [
        {
            "row_index": 0,
            "user_id": 1,
            "date": "2026-01-10",
            "amount": 25.0,
            "description": "Refund - school credit",
            "normalized_description": "refund - school credit",
            "vendor": "school",
            "category": "School & Education",
            "source_type": "manual_tracker",
            "is_refund_or_payment": True,
            "paid_by": "",
        }
    ]

    response = confirm_import(client, rows)
    assert response.status_code == 200

    with client.application.app_context():
        db = client.application.get_db()
        expense = db.execute("SELECT amount FROM expenses WHERE description = ?", ("Refund - school credit",)).fetchone()
    assert expense is not None
    assert expense["amount"] == 25.0

def test_import_confirm_manual_tracker_reimbursement_is_inserted_positive(client):
    register(client)
    login(client)

    rows = [
        {
            "row_index": 0,
            "user_id": 1,
            "date": "2026-01-10",
            "amount": 126.46,
            "description": "Reimbursement from employer",
            "normalized_description": "reimbursement from employer",
            "vendor": "employer",
            "category": "School & Education",
            "source_type": "manual_tracker",
            "is_refund_or_payment": False,
            "paid_by": "",
        }
    ]

    response = confirm_import(client, rows)
    assert response.status_code == 200

    with client.application.app_context():
        db = client.application.get_db()
        expense = db.execute("SELECT amount FROM expenses WHERE description = ?", ("Reimbursement from employer",)).fetchone()
    assert expense is not None
    assert expense["amount"] == 126.46


def test_import_confirm_inserts_only_selected_rows(client):
    register(client)
    login(client)
    rows = [
        {"row_index": i, "date": f"2026-11-0{i+1}", "amount": -10.0 - i, "description": f"Row {i+1}", "normalized_description": f"row {i+1}", "vendor": f"Vendor {i+1}", "category": "Groceries", "confidence": 90, "confidence_label": "High", "suggested_source": "rule"}
        for i in range(5)
    ]
    import_id = stage_import_preview(client, rows, preview_id="selected-only")

    with client.application.app_context():
        db = client.application.get_db()
        staged = db.execute("SELECT id FROM import_staging WHERE import_id = ? ORDER BY id", (import_id,)).fetchall()
        selected_ids = [staged[0]["id"], staged[3]["id"]]

    response = client.post(
        "/import/csv",
        data={"action": "confirm", "import_id": import_id, "import_default_paid_by": "DK", "selected_row_ids": [str(selected_ids[0]), str(selected_ids[1])]},
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert b"Preview rows: 5" in response.data
    assert b"Selected: 2" in response.data
    assert b"Skipped unselected: 3" in response.data

    with client.application.app_context():
        db = client.application.get_db()
        count = db.execute("SELECT COUNT(*) AS c FROM expenses WHERE description LIKE 'Row %'").fetchone()["c"]
        assert count == 2


def test_import_confirm_persists_single_row_category_override_without_apply_all(client):
    register(client)
    login(client)

    rows = [
        {"row_index": 0, "date": "2026-11-01", "amount": -12.5, "description": "One-off merchant", "normalized_description": "one-off merchant", "vendor": "One-off", "category": "", "confidence": 25, "confidence_label": "Low", "suggested_source": "unknown"},
    ]
    import_id = stage_import_preview(client, rows, preview_id="single-row-override")

    response = client.post(
        "/import/csv",
        data={"action": "confirm", "import_id": import_id, "import_default_paid_by": "DK", "override_category_0": "Groceries"},
        follow_redirects=True,
    )
    assert response.status_code == 200

    with client.application.app_context():
        db = client.application.get_db()
        groceries = db.execute("SELECT id FROM categories WHERE user_id = 1 AND name = 'Groceries'").fetchone()["id"]
        staged_row = json.loads(db.execute("SELECT row_json FROM import_staging WHERE import_id = ?", (import_id,)).fetchone()["row_json"])
        expense = db.execute("SELECT category_id FROM expenses WHERE description = 'One-off merchant'").fetchone()

    assert staged_row["override_category"] == "Groceries"
    assert expense is not None
    assert expense["category_id"] == groceries


def test_import_confirm_persists_single_row_category_override_with_only_one_selected_row(client):
    register(client)
    login(client)

    rows = [
        {"row_index": 0, "date": "2026-11-01", "amount": -14.25, "description": "Selected row", "normalized_description": "selected row", "vendor": "Vendor A", "category": "", "confidence": 25, "confidence_label": "Low", "suggested_source": "unknown"},
        {"row_index": 1, "date": "2026-11-02", "amount": -8.0, "description": "Unselected row", "normalized_description": "unselected row", "vendor": "Vendor B", "category": "", "confidence": 25, "confidence_label": "Low", "suggested_source": "unknown"},
    ]
    import_id = stage_import_preview(client, rows, preview_id="single-selected-override")

    with client.application.app_context():
        db = client.application.get_db()
        selected_id = db.execute("SELECT id FROM import_staging WHERE import_id = ? ORDER BY id LIMIT 1", (import_id,)).fetchone()["id"]
        groceries = db.execute("SELECT id FROM categories WHERE user_id = 1 AND name = 'Groceries'").fetchone()["id"]

    response = client.post(
        "/import/csv",
        data={
            "action": "confirm",
            "import_id": import_id,
            "import_default_paid_by": "DK",
            "selected_row_ids": [str(selected_id)],
            "override_category_0": "Groceries",
        },
        follow_redirects=True,
    )
    assert response.status_code == 200

    with client.application.app_context():
        db = client.application.get_db()
        selected_expense = db.execute("SELECT category_id FROM expenses WHERE description = 'Selected row'").fetchone()
        unselected_expense = db.execute("SELECT id FROM expenses WHERE description = 'Unselected row'").fetchone()

    assert selected_expense is not None
    assert selected_expense["category_id"] == groceries
    assert unselected_expense is None


def test_import_confirm_uses_persisted_single_row_override_after_preview_toggles(client):
    register(client)
    login(client)

    rows = [
        {"row_index": 0, "date": "2026-12-01", "amount": -15.0, "description": "Toggle target", "normalized_description": "toggle target", "vendor": "Toggle Vendor", "category": "", "confidence": 40, "confidence_label": "Low", "suggested_source": "unknown"},
        {"row_index": 1, "date": "2026-12-02", "amount": -5.0, "description": "High confidence row", "normalized_description": "high confidence row", "vendor": "Toggle Vendor", "category": "", "confidence": 90, "confidence_label": "High", "suggested_source": "rule"},
    ]
    import_id = stage_import_preview(client, rows, preview_id="override-after-toggle")

    with client.application.app_context():
        db = client.application.get_db()
        row_id = db.execute("SELECT id FROM import_staging WHERE import_id = ? ORDER BY id LIMIT 1", (import_id,)).fetchone()["id"]
        groceries = db.execute("SELECT id FROM categories WHERE user_id = 1 AND name = 'Groceries'").fetchone()["id"]

    update_resp = client.post(
        "/import/preview/row_update",
        json={"import_id": import_id, "row_id": row_id, "override_category": "Groceries"},
    )
    assert update_resp.status_code == 200

    assert client.get(f"/import/csv?import_id={import_id}&show_all=1").status_code == 200
    assert client.get(f"/import/csv?import_id={import_id}&show_all=1&low_confidence=1").status_code == 200

    response = client.post(
        "/import/csv",
        data={"action": "confirm", "import_id": import_id, "import_default_paid_by": "DK"},
        follow_redirects=True,
    )
    assert response.status_code == 200

    with client.application.app_context():
        db = client.application.get_db()
        expense = db.execute("SELECT category_id FROM expenses WHERE description = 'Toggle target'").fetchone()
    assert expense is not None
    assert expense["category_id"] == groceries


def test_import_apply_all_matching_still_updates_and_imports_all_matching_rows(client):
    register(client)
    login(client)

    rows = [
        {"row_index": 0, "date": "2026-12-10", "amount": -11.0, "description": "Coffee 1", "normalized_description": "coffee 1", "vendor": "Coffee Shop", "vendor_key": "coffee shop", "category": "", "confidence": 25, "confidence_label": "Low", "suggested_source": "unknown"},
        {"row_index": 1, "date": "2026-12-11", "amount": -12.0, "description": "Coffee 2", "normalized_description": "coffee 2", "vendor": "Coffee Shop", "vendor_key": "coffee shop", "category": "", "confidence": 25, "confidence_label": "Low", "suggested_source": "unknown"},
    ]
    import_id = stage_import_preview(client, rows, preview_id="apply-all-still-works")

    apply_resp = client.post(
        "/import/csv/apply_override",
        json={"match_type": "vendor", "match_key": "coffee shop", "category_name": "Restaurants", "import_id": import_id},
    )
    assert apply_resp.status_code == 200
    assert apply_resp.get_json()["updated_count"] == 2

    response = client.post(
        "/import/csv",
        data={"action": "confirm", "import_id": import_id, "import_default_paid_by": "DK"},
        follow_redirects=True,
    )
    assert response.status_code == 200

    with client.application.app_context():
        db = client.application.get_db()
        restaurants = db.execute("SELECT id FROM categories WHERE user_id = 1 AND name = 'Restaurants'").fetchone()["id"]
        imported = db.execute(
            "SELECT description, category_id FROM expenses WHERE description IN ('Coffee 1', 'Coffee 2') ORDER BY description"
        ).fetchall()

    assert [row["description"] for row in imported] == ["Coffee 1", "Coffee 2"]
    assert all(row["category_id"] == restaurants for row in imported)


def test_import_preview_toggle_queries_keep_selection_flags(client):
    register(client)
    login(client)
    rows = [
        {"row_index": i, "date": f"2026-12-0{i+1}", "amount": -5.0 - i, "description": f"Toggle {i+1}", "normalized_description": f"toggle {i+1}", "vendor": "Toggle", "category": "Groceries", "confidence": 40 if i % 2 == 0 else 90, "confidence_label": "Low", "suggested_source": "rule"}
        for i in range(5)
    ]
    import_id = stage_import_preview(client, rows, preview_id="selection-toggle")

    with client.application.app_context():
        db = client.application.get_db()
        staged = db.execute("SELECT id FROM import_staging WHERE import_id = ? ORDER BY id", (import_id,)).fetchall()
        keep_ids = [staged[1]["id"], staged[4]["id"]]

    client.post("/import/preview/selection/bulk", json={"import_id": import_id, "selected": False, "scope": "all"})
    client.post("/import/preview/selection", json={"import_id": import_id, "row_id": keep_ids[0], "selected": True})
    client.post("/import/preview/selection", json={"import_id": import_id, "row_id": keep_ids[1], "selected": True})

    resp = client.get(f"/import/csv?import_id={import_id}&show_all=1")
    assert resp.status_code == 200

    resp_low = client.get(f"/import/csv?import_id={import_id}&show_all=1&low_confidence=1")
    assert resp_low.status_code == 200

    with client.application.app_context():
        db = client.application.get_db()
        selected_flags = [row["selected"] == 1 for row in db.execute("SELECT selected FROM import_staging WHERE import_id = ? ORDER BY id", (import_id,)).fetchall()]
        assert selected_flags == [False, True, False, False, True]


def test_import_preview_selection_endpoint_persists_single_toggle(client):
    register(client)
    login(client)
    rows = [
        {"row_index": i, "date": "2026-12-01", "amount": -5.0 - i, "description": f"Select {i+1}", "normalized_description": f"select {i+1}", "vendor": "Select", "category": "Groceries", "confidence": 70, "confidence_label": "Medium", "suggested_source": "rule"}
        for i in range(3)
    ]
    import_id = stage_import_preview(client, rows, preview_id="selection-endpoint")

    with client.application.app_context():
        db = client.application.get_db()
        row_id = db.execute("SELECT id FROM import_staging WHERE import_id = ? ORDER BY id LIMIT 1", (import_id,)).fetchone()["id"]

    response = client.post(
        "/import/preview/selection",
        json={"import_id": import_id, "row_id": row_id, "selected": False},
    )
    assert response.status_code == 200

    with client.application.app_context():
        db = client.application.get_db()
        selected = db.execute("SELECT selected FROM import_staging WHERE id = ?", (row_id,)).fetchone()["selected"]
        assert selected == 0


def test_import_preview_selection_bulk_endpoint_updates_all_rows(client):
    register(client)
    login(client)
    rows = [
        {"row_index": i, "date": "2026-12-01", "amount": -9.0 - i, "description": f"Bulk {i+1}", "normalized_description": f"bulk {i+1}", "vendor": "Bulk", "category": "Groceries", "confidence": 70, "confidence_label": "Medium", "suggested_source": "rule"}
        for i in range(4)
    ]
    import_id = stage_import_preview(client, rows, preview_id="selection-bulk")

    response = client.post(
        "/import/preview/selection/bulk",
        json={"import_id": import_id, "selected": False, "scope": "all"},
    )
    assert response.status_code == 200

    with client.application.app_context():
        db = client.application.get_db()
        rows = db.execute("SELECT selected FROM import_staging WHERE import_id = ?", (import_id,)).fetchall()
        assert all(row["selected"] == 0 for row in rows)


def test_import_confirm_skips_duplicate_when_paid_by_and_category_differ(client):
    register(client)
    login(client)

    rows = [
        {
            "row_index": 0,
            "date": "2026-11-20",
            "amount": -42.5,
            "description": "Freshco #101",
            "normalized_description": "freshco #101",
            "vendor": "Freshco",
            "category": "Groceries",
            "confidence": 90,
            "confidence_label": "High",
            "suggested_source": "rule",
            "source_type": "bank",
            "paid_by": "DK",
        }
    ]

    first = confirm_import(client, rows, import_default_paid_by="DK")
    assert first.status_code == 200
    assert b"Imported 1 transaction(s)." in first.data

    second_rows = [dict(rows[0], category="Uncategorized", paid_by="YZ")]
    second = confirm_import(client, second_rows, import_default_paid_by="YZ")
    assert second.status_code == 200
    assert b"Imported 0 transaction(s)." in second.data
    assert b"Skipped duplicates: 1" in second.data

    with client.application.app_context():
        db = client.application.get_db()
        count = db.execute("SELECT COUNT(*) AS c FROM expenses WHERE description = ?", ("Freshco #101",)).fetchone()["c"]
        assert count == 1


def test_import_skipped_duplicate_can_be_overridden(client):
    register(client)
    login(client)

    rows = [
        {
            "row_index": 0,
            "date": "2026-11-20",
            "amount": -42.5,
            "description": "Freshco override dup",
            "normalized_description": "freshco override dup",
            "vendor": "Freshco",
            "category": "Groceries",
            "confidence": 90,
            "confidence_label": "High",
            "suggested_source": "rule",
            "source_type": "bank",
            "paid_by": "DK",
        }
    ]

    import_id = stage_import_preview(client, rows, preview_id="override-dup")
    first = client.post("/import/csv", data={"action": "confirm", "import_id": import_id, "import_default_paid_by": "DK"}, follow_redirects=True)
    assert b"Imported 1 transaction(s)." in first.data

    second_id = stage_import_preview(client, rows, preview_id="override-dup-2")
    second = client.post("/import/csv", data={"action": "confirm", "import_id": second_id, "import_default_paid_by": "DK"}, follow_redirects=True)
    assert b"Skipped duplicates: 1" in second.data

    with client.application.app_context():
        db = client.application.get_db()
        skipped_row_id = db.execute(
            "SELECT id FROM import_staging WHERE import_id = ? AND import_status = 'skipped' AND skipped_reason = 'duplicate'",
            (second_id,),
        ).fetchone()["id"]

    overridden = client.post(
        "/import/csv",
        data={"action": "import_skipped_selected", "import_id": second_id, "selected_skipped_row_ids": [str(skipped_row_id)]},
        follow_redirects=True,
    )
    assert b"Imported 1 previously skipped row(s)." in overridden.data

    with client.application.app_context():
        db = client.application.get_db()
        count = db.execute("SELECT COUNT(*) AS c FROM expenses WHERE description = ?", ("Freshco override dup",)).fetchone()["c"]
        assert count == 2


def test_import_skipped_selected_with_empty_selection_is_safe(client):
    register(client)
    login(client)

    rows = [
        {
            "row_index": 0,
            "date": "2026-11-20",
            "amount": -42.5,
            "description": "Freshco empty selection",
            "normalized_description": "freshco empty selection",
            "vendor": "Freshco",
            "category": "Groceries",
            "confidence": 90,
            "confidence_label": "High",
            "suggested_source": "rule",
            "source_type": "bank",
            "paid_by": "DK",
        }
    ]

    import_id = stage_import_preview(client, rows, preview_id="override-empty")
    first = client.post("/import/csv", data={"action": "confirm", "import_id": import_id, "import_default_paid_by": "DK"}, follow_redirects=True)
    assert b"Imported 1 transaction(s)." in first.data

    second_id = stage_import_preview(client, rows, preview_id="override-empty-2")
    second = client.post("/import/csv", data={"action": "confirm", "import_id": second_id, "import_default_paid_by": "DK"}, follow_redirects=True)
    assert b"Skipped duplicates: 1" in second.data

    no_selection = client.post(
        "/import/csv",
        data={"action": "import_skipped_selected", "import_id": second_id},
        follow_redirects=True,
    )
    assert no_selection.status_code == 200
    assert b"No skipped rows were selected." in no_selection.data

    with client.application.app_context():
        db = client.application.get_db()
        count = db.execute("SELECT COUNT(*) AS c FROM expenses WHERE description = ?", ("Freshco empty selection",)).fetchone()["c"]
        assert count == 1


def test_budget_page_renders_with_defaults(client):
    register(client)
    login(client)

    response = client.get("/budget")
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "Budget" in html
    assert "Save budget changes" in html
    assert "Copy from last month" in html
    assert "<th>Subcategory</th>" not in html


def test_budget_save_and_summary_numbers(client):
    register(client)
    login(client)

    with client.application.app_context():
        db = client.application.get_db()
        groceries = db.execute(
            "SELECT id FROM categories WHERE user_id = ? AND name = 'Groceries'",
            (1,),
        ).fetchone()["id"]
        db.execute(
            """
            INSERT INTO expenses (user_id, household_id, date, amount, category_id, paid_by, is_transfer, is_personal)
            VALUES (?, ?, ?, ?, ?, ?, 0, 0)
            """,
            (1, 1, "2026-03-05", -120.0, groceries, "DK"),
        )
        db.commit()

    save_response = client.post(
        "/budget/save",
        data={
            "month": "2026-03",
            "view": "household",
            "scope": "shared",
            "row_key": [f"{groceries}:0"],
            f"type_{groceries}:0": "Fixed",
            f"budget_{groceries}:0": "300",
            f"rollover_{groceries}:0": "10",
        },
        follow_redirects=True,
    )
    html = save_response.get_data(as_text=True)

    assert save_response.status_code == 200
    assert "Budget changes saved." in html
    assert "$300.00" in html
    assert "$120.00" in html
    assert "$180.00" in html


def test_budget_copy_from_last_month(client):
    register(client)
    login(client)

    with client.application.app_context():
        db = client.application.get_db()
        groceries = db.execute(
            "SELECT id FROM categories WHERE user_id = ? AND name = 'Groceries'",
            (1,),
        ).fetchone()["id"]
        db.execute(
            """
            INSERT INTO monthly_budgets (
                household_id, month, view_mode, scope_mode, category_id, subcategory_id, budget_type, budget_amount, rollover_amount
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (1, "2026-02", "household", "shared", groceries, 0, "Flexible", 450.0, 25.0),
        )
        db.commit()

    response = client.post(
        "/budget/copy-last-month",
        data={"month": "2026-03", "view": "household", "scope": "shared"},
        follow_redirects=True,
    )
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "Copied budget settings from last month." in html
    assert "$450.00" in html
    assert "25.00" in html


def test_budget_view_and_scope_filters(client):
    register(client)
    login(client)

    with client.application.app_context():
        db = client.application.get_db()
        groceries = db.execute("SELECT id FROM categories WHERE user_id = ? AND name = 'Groceries'", (1,)).fetchone()["id"]
        personal = db.execute("SELECT id FROM categories WHERE user_id = ? AND name = 'Personal'", (1,)).fetchone()["id"]
        db.execute(
            "INSERT INTO expenses (user_id, household_id, date, amount, category_id, paid_by, is_transfer, is_personal) VALUES (?, ?, ?, ?, ?, ?, 0, 0)",
            (1, 1, "2026-03-01", -60.0, groceries, "DK"),
        )
        db.execute(
            "INSERT INTO expenses (user_id, household_id, date, amount, category_id, paid_by, is_transfer, is_personal) VALUES (?, ?, ?, ?, ?, ?, 0, 1)",
            (1, 1, "2026-03-02", -20.0, personal, "DK"),
        )
        db.commit()

    shared_html = client.get("/budget?month=2026-03&view=household&scope=shared").get_data(as_text=True)
    personal_html = client.get("/budget?month=2026-03&view=household&scope=personal").get_data(as_text=True)
    dk_html = client.get("/budget?month=2026-03&view=dk&scope=all").get_data(as_text=True)

    assert "$60.00" in shared_html
    assert "$20.00" in personal_html
    assert "$80.00" in dk_html


def test_budget_page_renders_nested_subcategory_rows(client):
    register(client)
    login(client)

    with client.application.app_context():
        db = client.application.get_db()
        groceries = db.execute("SELECT id FROM categories WHERE user_id = ? AND name = 'Groceries'", (1,)).fetchone()["id"]
        cursor = db.execute(
            "INSERT INTO subcategories (user_id, category_id, name) VALUES (?, ?, ?)",
            (1, groceries, "Produce"),
        )
        subcategory_id = cursor.lastrowid
        db.execute(
            """
            INSERT INTO monthly_budgets (
                household_id, month, view_mode, scope_mode, category_id, subcategory_id, budget_type, budget_amount, rollover_amount
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (1, "2026-03", "household", "shared", groceries, subcategory_id, "Flexible", 80.0, 0.0),
        )
        db.commit()

    response = client.get("/budget?month=2026-03&view=household&scope=shared")
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "budget-collapse-toggle" in html
    assert "budget-sub-row" in html
    assert "Produce" in html
