import csv
import hashlib
import io
import os
import sqlite3
import json
import re
import unicodedata
import uuid
from datetime import date, datetime, timedelta
from functools import wraps

from flask import (
    Flask,
    flash,
    g,
    redirect,
    render_template,
    request,
    session,
    url_for,
    Response,
    jsonify,
)
from werkzeug.security import check_password_hash, generate_password_hash

from .db_migrations import apply_migrations, get_db_health


class DatabaseInitError(RuntimeError):
    """Raised when the SQLite database cannot be initialized."""


DEFAULT_CATEGORIES = [
    "Groceries",
    "Restaurants",
    "Bakery & Coffee",
    "Mortgage",
    "Condo Fees",
    "Property Tax",
    "Utilities",
    "Home Maintenance & Repairs",
    "Furniture & Appliances",
    "Gas & Fuel",
    "Car Maintenance & Registration",
    "Insurance",
    "Parking",
    "Public Transit",
    "School & Education",
    "Sports & Activities",
    "Camps & Lessons",
    "Equipment",
    "Pet Food & Care",
    "Entertainment",
    "Subscriptions",
    "Activities & Recreation",
    "Tickets & Events",
    "General Shopping",
    "Electronics",
    "Cosmetics & Personal Care",
    "Clothing",
    "Pharmacy & Medical",
    "Dentist & Dental",
    "Alcohol & Wine",
    "Gifts & Presents",
    "Travel & Vacation",
    "Personal",
    "Credit Card Payments",
    "Transfers",
]
LEGACY_CATEGORY_MAPPING = {
    "food": "Groceries",
    "boulangerie": "Bakery & Coffee",
    "sushi": "Restaurants",
    "eating out": "Restaurants",
    "dine out": "Restaurants",
    "house": "Home Maintenance & Repairs",
    "home": "Home Maintenance & Repairs",
    "furniture": "Furniture & Appliances",
    "appliance": "Furniture & Appliances",
    "deck": "Home Maintenance & Repairs",
    "air conditioner": "Home Maintenance & Repairs",
    "hydro-quebec": "Utilities",
    "internet": "Utilities",
    "virgin": "Utilities",
    "gas": "Gas & Fuel",
    "stm": "Public Transit",
    "parking": "Parking",
    "car registration": "Car Maintenance & Registration",
    "car dl": "Car Maintenance & Registration",
    "david hockey": "Sports & Activities",
    "equipment david": "Equipment",
    "david summer camp": "Camps & Lessons",
    "david piano": "Activities & Recreation",
    "ecole ste-anne": "School & Education",
    "cookie food": "Pet Food & Care",
    "amazon": "General Shopping",
    "electronics": "Electronics",
    "cosmetics": "Cosmetics & Personal Care",
    "cinema": "Entertainment",
    "tickets": "Tickets & Events",
    "aquaparc": "Activities & Recreation",
    "ski": "Activities & Recreation",
    "tennis": "Activities & Recreation",
    "mortgage": "Mortgage",
    "condo fees": "Condo Fees",
    "property tax": "Property Tax",
    "payment thank you": "Credit Card Payments",
    "transfer": "Transfers",
    "return": "Transfers",
    "points": "Transfers",
}
TRANSFER_KEYWORDS = [
    "payment received",
    "credit card payment",
    "transfer",
    "e-transfer",
    "direct deposit",
    "refund",
    "return",
    "points",
]

IMPORT_PREVIEW_DEFAULT_LIMIT = 25
IMPORT_PREVIEW_SHOW_ALL_WARNING_THRESHOLD = 500
LEARNING_STOPLIST = {
    "shop",
    "store",
    "payment",
    "merci",
    "service",
    "purchase",
    "debit",
    "credit",
    "transaction",
    "interest",
}
LEARNING_SPECIAL_PATTERNS = ["apple.com/bill"]
PAYMENT_KEYWORDS = ["payment received", "thank you", "online payment", "autopay", "payment thank you", "payment"]
PERSONAL_KEYWORDS = ["salon", "spa", "barber", "gym", "hobby", "massage", "openai", "open ai", "chatgpt"]
TAG_KEYWORDS = {"david": "David", "denys": "Denys", "cookie": "Cookie"}
MERCHANT_RULES = [
    ("Groceries", ["metro", "iga", "provigo", "loblaws", "super c"]),
    ("Bakery & Coffee", ["boulangerie", "bakery", "patisserie", "cafe", "coffee", "starbucks", "tim hortons"]),
    ("Gas & Fuel", ["gas", "esso", "shell", "petro"]),
    ("Public Transit", ["stm"]),
    ("General Shopping", ["amazon", "shop", "walmart", "canadian tire"]),
    ("Utilities", ["hydro", "bell", "videotron", "virgin"]),
    ("Sports & Activities", ["hockey", "tennis", "ski", "camp", "piano"]),
    ("Subscriptions", ["apple.com/bill", "apple bill", "itunes", "icloud", "apple music", "apple tv", "netflix", "disney", "spotify"]),
]
HEADER_ALIASES = {
    "date": ["date", "transaction date", "posting date"],
    "amount": ["amount"],
    "debit": ["debit"],
    "credit": ["credit"],
    "description": ["description", "details", "memo", "merchant", "payee"],
    "vendor": ["vendor", "merchant", "payee", "name", "merchant name"],
    "category": ["category"],
    "paid_by": ["paid by", "paid_by", "payer", "owner"],
}
VENDOR_NOISE_TOKENS = {
    "pos",
    "purchase",
    "debit",
    "credit",
    "auth",
    "interac",
    "transaction",
    "card",
    "payment",
}
PET_CATEGORIES = [
    "Pet Food & Care",
    "Pet",
    "Vet",
    "Pet Insurance",
]


def normalize_header_name(value):
    return " ".join((value or "").strip().lower().split())


def parse_money(value):
    text = (value or "").strip()
    if not text:
        return None
    cleaned = text.replace(",", "").replace("$", "")
    if cleaned.startswith("(") and cleaned.endswith(")"):
        cleaned = f"-{cleaned[1:-1]}"
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_transaction_date(value):
    cleaned = (value or "").strip()
    if not cleaned:
        return None

    cleaned = cleaned.replace(".", "")
    for fmt in [
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%m/%d/%y",
        "%d/%m/%Y",
        "%d/%m/%y",
        "%d %b %Y",
        "%d %B %Y",
    ]:
        try:
            return datetime.strptime(cleaned, fmt)
        except ValueError:
            continue
    return None


def normalize_paid_by(value):
    cleaned = normalize_header_name(value)
    if cleaned in {"dk", "denys", "d"}:
        return "DK"
    if cleaned in {"yz", "yuliya", "wife", "y"}:
        return "YZ"
    return ""


def extract_embedded_amount(description):
    text = (description or "").strip()
    match = re.search(r"(?<!\d)([-+]?\d[\d,]*\.\d{1,2})(?!\d)", text)
    if not match:
        return None, text

    amount = parse_money(match.group(1))
    if amount is None:
        return None, text

    cleaned_description = f"{text[:match.start()]} {text[match.end():]}"
    cleaned_description = re.sub(r"\s+", " ", cleaned_description).strip(" -\t")
    return amount, cleaned_description


def detect_bank_type(header_row):
    normalized_headers = {normalize_header_name(col) for col in (header_row or [])}
    if {"date", "description", "amount"}.issubset(normalized_headers):
        return "amex"
    return "default"


def normalize_description(value):
    normalized = unicodedata.normalize("NFKD", (value or "").strip().lower())
    no_accents = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", no_accents)


def normalize_csv_category_name(value):
    return re.sub(r"\s+", " ", (value or "").strip())


def resolve_csv_category_mapping(raw_name, category_lookup):
    csv_category_name = normalize_csv_category_name(raw_name)
    if not csv_category_name:
        return "", None, ""
    category = category_lookup.get(normalize_description(csv_category_name))
    if not category:
        return csv_category_name, None, "unknown"
    return csv_category_name, category["id"], "matched"


def build_unknown_category_rows(rows):
    unknown = {}
    for row in rows:
        if row.get("csv_category_match_status") != "unknown":
            continue
        name = row.get("csv_category_name")
        if not name:
            continue
        unknown[name] = row.get("mapped_category_id")
    return [{"name": name, "mapped_category_id": mapped_id} for name, mapped_id in sorted(unknown.items())]


def normalize_text(value):
    normalized = normalize_description(value)
    normalized = re.sub(r"[^\w\s/]", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def derive_vendor(description):
    normalized = normalize_text(description)
    if not normalized:
        return ""
    tokens = [token for token in normalized.split() if token not in VENDOR_NOISE_TOKENS]
    while tokens and re.fullmatch(r"[a-z]*\d+[a-z\d-]*", tokens[-1]):
        tokens.pop()
    if not tokens:
        return ""
    return " ".join(tokens[:4])


def extract_pattern(value, max_words=3):
    normalized = normalize_text(value)
    if not normalized:
        return ""

    for token in LEARNING_SPECIAL_PATTERNS:
        if token in normalized:
            return token

    words = normalized.split()
    if not words:
        return ""

    word_count = min(max_words, len(words))
    while word_count > 0:
        candidate = " ".join(words[:word_count])
        if candidate not in LEARNING_STOPLIST:
            return candidate
        word_count -= 1
    return ""


def pick_existing_category(preferred, available_categories, fallback=None):
    if not preferred and not fallback:
        return ""

    if not available_categories:
        return preferred or fallback or ""

    available_lookup = {
        normalize_description(category_name): category_name for category_name in available_categories
    }

    for choice in [preferred, fallback]:
        if not choice:
            continue
        found = available_lookup.get(normalize_description(choice))
        if found:
            return found

    return ""


def derive_tags(description):
    normalized = normalize_description(description)
    return [tag for keyword, tag in TAG_KEYWORDS.items() if keyword in normalized]


def map_category_name(raw_category):
    cleaned = (raw_category or "").strip()
    if not cleaned:
        return ""

    if cleaned in DEFAULT_CATEGORIES:
        return cleaned

    normalized = normalize_description(cleaned)
    return LEGACY_CATEGORY_MAPPING.get(normalized, cleaned)


def infer_category(description, raw_category, available_categories=None):
    mapped = map_category_name(raw_category)
    normalized_desc = normalize_description(description)

    if mapped:
        return pick_existing_category(mapped, available_categories) or mapped

    if any(keyword in normalized_desc for keyword in PAYMENT_KEYWORDS):
        return pick_existing_category("Credit Card Payments", available_categories, "Transfers")

    for keyword in PERSONAL_KEYWORDS:
        if keyword in normalized_desc:
            return pick_existing_category("Personal", available_categories)

    if "apple online store" in normalized_desc or "apple store" in normalized_desc:
        return pick_existing_category("Electronics", available_categories, "General Shopping")

    if "ikea" in normalized_desc:
        return pick_existing_category("Furniture & Appliances", available_categories, "General Shopping")

    if "costco" in normalized_desc and pick_existing_category("Groceries", available_categories):
        return pick_existing_category("Groceries", available_categories)

    for category, keywords in MERCHANT_RULES:
        if any(keyword in normalized_desc for keyword in keywords):
            return pick_existing_category(category, available_categories)

    if any(keyword in normalized_desc for keyword in TRANSFER_KEYWORDS):
        return pick_existing_category("Transfers", available_categories)

    return ""


def is_transfer_transaction(description, category_name):
    normalized_category = normalize_description(category_name)
    if normalized_category in {"transfers", "credit card payments"}:
        return True
    if normalized_category and normalized_category not in {"transfers", "credit card payments"}:
        return False
    normalized_desc = normalize_description(description)
    transfer_terms = TRANSFER_KEYWORDS + PAYMENT_KEYWORDS
    return any(keyword in normalized_desc for keyword in transfer_terms)


def confidence_label(confidence):
    if confidence >= 80:
        return "High"
    if confidence >= 50:
        return "Medium"
    return "Low"


def detect_header_and_mapping(rows):
    mapping = {"date": "", "description": "", "vendor": "", "amount": "", "debit": "", "credit": "", "category": "", "paid_by": ""}
    if not rows:
        return False, mapping, 0

    header_row_index = 0
    scan_limit = min(len(rows), 50)
    for idx in range(scan_limit):
        candidate = [normalize_header_name(cell) for cell in rows[idx]]
        cells = {cell for cell in candidate if cell}
        has_date = "date" in cells or "transaction date" in cells or "date processed" in cells
        has_amount = "amount" in cells
        has_desc_or_merchant = "description" in cells or "merchant" in cells
        if has_date and has_amount and has_desc_or_merchant:
            header_row_index = idx
            break

    first_row = rows[header_row_index] if rows else []
    while first_row and not first_row[-1].strip():
        first_row = first_row[:-1]

    normalized = [normalize_header_name(col) for col in first_row]
    normalized_lookup = {value: str(i) for i, value in enumerate(normalized) if value}

    mapping["date"] = normalized_lookup.get("date", "") or normalized_lookup.get("transaction date", "") or normalized_lookup.get("date processed", "")
    mapping["description"] = normalized_lookup.get("description", "") or normalized_lookup.get("merchant", "")
    mapping["amount"] = normalized_lookup.get("amount", "")
    mapping["debit"] = normalized_lookup.get("debit", "")
    mapping["credit"] = normalized_lookup.get("credit", "")
    mapping["vendor"] = normalized_lookup.get("merchant", "") or normalized_lookup.get("vendor", "") or normalized_lookup.get("description", "")
    mapping["category"] = normalized_lookup.get("category", "")
    mapping["paid_by"] = normalized_lookup.get("paid by", "") or normalized_lookup.get("paid_by", "") or normalized_lookup.get("payer", "")

    has_header = any(mapping[field] != "" for field in ["date", "amount", "debit", "credit", "description"])
    if has_header:
        if mapping["amount"] != "":
            mapping["debit"] = ""
            mapping["credit"] = ""
        return True, mapping, header_row_index

    first_row = rows[0] if rows else []
    first_date = first_row[0] if len(first_row) > 0 else ""
    description = first_row[1].strip() if len(first_row) > 1 else ""
    debit = first_row[2] if len(first_row) > 2 else ""
    credit = first_row[3] if len(first_row) > 3 else ""

    if (
        parse_transaction_date(first_date) is not None
        and bool(description)
        and (parse_money(debit) is not None or parse_money(credit) is not None or (not debit.strip() and not credit.strip()))
    ):
        mapping.update({"date": "0", "description": "1", "debit": "2", "credit": "3", "amount": ""})
    return False, mapping, 0


def detect_cibc_headerless_mapping(rows):
    def is_numeric_or_blank(value):
        cleaned = (value or "").strip()
        if not cleaned:
            return True
        return parse_money(cleaned) is not None

    first_non_empty_row = None
    for raw_row in rows:
        trimmed_row = [cell.strip() for cell in raw_row]
        if any(trimmed_row):
            first_non_empty_row = trimmed_row
            break

    if first_non_empty_row is None or len(first_non_empty_row) < 4:
        return None

    if (
        parse_transaction_date(first_non_empty_row[0]) is not None
        and is_numeric_or_blank(first_non_empty_row[2])
        and is_numeric_or_blank(first_non_empty_row[3])
    ):
        return {"date": "0", "description": "1", "debit": "2", "credit": "3", "amount": "", "vendor": "", "category": "", "paid_by": ""}

    return None


def detect_amex_headered_mapping(rows, header_row_index):
    if not rows:
        return None

    header_row = rows[header_row_index] if 0 <= header_row_index < len(rows) else []
    normalized = [normalize_header_name(col) for col in header_row]
    lookup = {value: str(i) for i, value in enumerate(normalized) if value}

    if "amount" not in lookup:
        return None

    mapping = {
        "date": lookup.get("date", "") or lookup.get("transaction date", "") or lookup.get("date processed", ""),
        "description": lookup.get("description", "") or lookup.get("merchant", ""),
        "vendor": lookup.get("merchant", "") or lookup.get("description", ""),
        "amount": lookup.get("amount", ""),
        "debit": "",
        "credit": "",
        "category": lookup.get("category", ""),
        "paid_by": lookup.get("paid by", "") or lookup.get("paid_by", "") or lookup.get("payer", ""),
    }

    if mapping["date"] == "" or mapping["description"] == "":
        return None

    return mapping


def build_csv_mapping_payload(mapping, has_header, detected_format, file_signature=""):
    return {
        "date_col": mapping.get("date", ""),
        "desc_col": mapping.get("description", ""),
        "amount_col": mapping.get("amount", ""),
        "debit_col": mapping.get("debit", ""),
        "credit_col": mapping.get("credit", ""),
        "vendor_col": mapping.get("vendor", ""),
        "category_col": mapping.get("category", ""),
        "paid_by_col": mapping.get("paid_by", ""),
        "has_header": bool(has_header),
        "detected_format": detected_format,
        "file_signature": file_signature or "",
    }


def build_file_signature(filename, header_row):
    cleaned_filename = (filename or "").strip().lower()
    cleaned_header = [cell.strip() for cell in (header_row or [])]
    signature_base = f"{cleaned_filename}|{'|'.join(cleaned_header)}"
    return hashlib.sha256(signature_base.encode("utf-8")).hexdigest()


def mapping_from_payload(payload):
    if not payload:
        return {"date": "", "description": "", "vendor": "", "amount": "", "debit": "", "credit": "", "category": "", "paid_by": ""}
    return {
        "date": payload.get("date_col", ""),
        "description": payload.get("desc_col", ""),
        "vendor": payload.get("vendor_col", ""),
        "amount": payload.get("amount_col", ""),
        "debit": payload.get("debit_col", ""),
        "credit": payload.get("credit_col", ""),
        "category": payload.get("category_col", ""),
        "paid_by": payload.get("paid_by_col", ""),
    }


def get_saved_csv_mapping_for_user(user_id, file_signature=""):
    mapping_by_user = session.get("csv_mapping_by_user") or {}
    payload = mapping_by_user.get(str(user_id))
    if payload:
        payload_signature = payload.get("file_signature", "")
        if not file_signature or payload_signature == file_signature:
            return payload
        return {}

    legacy_payload = session.get("csv_mapping") or {}
    legacy_signature = legacy_payload.get("file_signature", "")
    if not file_signature or legacy_signature == file_signature:
        return legacy_payload
    return {}


def save_csv_mapping_for_user(user_id, mapping, has_header, detected_format, file_signature=""):
    payload = build_csv_mapping_payload(mapping, has_header, detected_format, file_signature=file_signature)
    mapping_by_user = session.get("csv_mapping_by_user") or {}
    mapping_by_user[str(user_id)] = payload
    session["csv_mapping_by_user"] = mapping_by_user
    # Backward-compatible key used by existing sessions/tests.
    session["csv_mapping"] = payload
    session.modified = True




def cleanup_expired_import_staging(db, max_age_hours=24):
    cutoff = (datetime.utcnow() - timedelta(hours=max_age_hours)).isoformat()
    db.execute("DELETE FROM import_staging WHERE created_at < ?", (cutoff,))


def save_import_preview_state(user_id, rows, preview_id=None):
    # Kept for compatibility with tests/session consumers that only need preview ID.
    generated_preview_id = str(uuid.uuid4()) if preview_id is None else preview_id
    previews = session.get("import_preview_by_user") or {}
    previews[str(user_id)] = {
        "preview_id": generated_preview_id,
        "rows": [],
        "created_at": datetime.utcnow().isoformat(),
    }
    session["import_preview_by_user"] = previews
    session.modified = True
    return generated_preview_id


def get_import_preview_show_all(user_id, import_id):
    by_user = session.get("import_preview_show_all_by_user") or {}
    user_values = by_user.get(str(user_id)) or {}
    return bool(user_values.get(import_id, False))


def save_import_preview_show_all(user_id, import_id, enabled):
    if not import_id:
        return
    by_user = session.get("import_preview_show_all_by_user") or {}
    user_values = by_user.get(str(user_id)) or {}
    user_values[import_id] = bool(enabled)
    by_user[str(user_id)] = user_values
    session["import_preview_show_all_by_user"] = by_user
    session.modified = True


def clear_import_preview_show_all(user_id, import_id):
    by_user = session.get("import_preview_show_all_by_user") or {}
    user_values = by_user.get(str(user_id)) or {}
    if import_id in user_values:
        user_values.pop(import_id, None)
        by_user[str(user_id)] = user_values
        session["import_preview_show_all_by_user"] = by_user
        session.modified = True




def preview_rows_for_display(rows, show_all=False, limit=IMPORT_PREVIEW_DEFAULT_LIMIT):
    total_rows = len(rows)
    if show_all:
        return rows, total_rows, total_rows
    displayed_rows = rows[:limit]
    return displayed_rows, len(displayed_rows), total_rows

def stage_import_preview_rows(db, import_id, rows, household_id=None, user_id=None):
    created_at = datetime.utcnow().isoformat()
    for row in rows:
        db.execute(
            """
            INSERT INTO import_staging (import_id, household_id, user_id, created_at, row_json, status)
            VALUES (?, ?, ?, ?, ?, 'preview')
            """,
            (import_id, household_id, user_id, created_at, json.dumps(row)),
        )


def get_staged_preview_rows(db, import_id, household_id=None, user_id=None):
    records = get_staged_preview_row_records(db, import_id, household_id=household_id, user_id=user_id)
    parsed_rows = []
    for record in records:
        row = dict(record["row"])
        row["staging_id"] = record["id"]
        parsed_rows.append(row)
    return parsed_rows


def get_staged_preview_row_records(db, import_id, household_id=None, user_id=None):
    filters = ["import_id = ?"]
    params = [import_id]
    if household_id is not None:
        filters.append("household_id = ?")
        params.append(household_id)
    if user_id is not None:
        filters.append("user_id = ?")
        params.append(user_id)

    staged_rows = db.execute(
        f"""
        SELECT id, row_json FROM import_staging
        WHERE {' AND '.join(filters)}
        ORDER BY id ASC
        """,
        tuple(params),
    ).fetchall()
    if not staged_rows:
        return []

    parsed_rows = []
    for row in staged_rows:
        try:
            parsed_rows.append({"id": row["id"], "row": json.loads(row["row_json"])})
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
    return parsed_rows


def update_staged_preview_row(db, staging_id, row):
    db.execute("UPDATE import_staging SET row_json = ? WHERE id = ?", (json.dumps(row), staging_id))

def placeholder_columns_from_mapping(mapping):
    indices = []
    for value in (mapping or {}).values():
        try:
            indices.append(int(value))
        except (TypeError, ValueError):
            continue
    if not indices:
        return []
    return [f"Column {i + 1}" for i in range(max(indices) + 1)]


def should_auto_map_cibc_headerless(rows, mapping, detected_format):
    if detected_format != "headerless":
        return None

    if any((mapping.get(field) or "").strip() for field in ["date", "description", "amount", "debit", "credit", "vendor", "category", "paid_by"]):
        return None

    def is_numeric_or_blank(value):
        cleaned = (value or "").strip()
        if not cleaned:
            return True
        return parse_money(cleaned) is not None

    first_non_empty_row = None
    for raw_row in rows:
        trimmed_row = [cell.strip() for cell in raw_row]
        if any(trimmed_row):
            first_non_empty_row = trimmed_row
            break

    if first_non_empty_row is None or len(first_non_empty_row) < 4:
        return None

    if (
        parse_transaction_date(first_non_empty_row[0]) is not None
        and is_numeric_or_blank(first_non_empty_row[2])
        and is_numeric_or_blank(first_non_empty_row[3])
    ):
        return {"date": "0", "description": "1", "debit": "2", "credit": "3", "amount": "", "vendor": "", "category": "", "paid_by": ""}

    return None


def parse_csv_transactions(rows, mapping, user_id, bank_type="default"):
    parsed_rows = []
    skipped_rows = 0
    for row_index, raw_row in enumerate(rows):
        row = [cell.strip() for cell in raw_row]

        def get_value(field):
            column = mapping.get(field, "")
            if column == "":
                return ""
            try:
                idx = int(column)
            except ValueError:
                return ""
            return row[idx] if idx < len(row) else ""

        parsed_date = parse_transaction_date(get_value("date"))
        row_description = get_value("description")
        row_category = get_value("category")
        csv_category_name = normalize_csv_category_name(row_category)
        normalized_description = normalize_description(row_description)
        row_vendor = get_value("vendor") or derive_vendor(row_description)
        row_paid_by = normalize_paid_by(get_value("paid_by"))

        amount = None
        amount_col = mapping.get("amount", "")
        if amount_col != "":
            amount = parse_money(get_value("amount"))
        else:
            debit_value = parse_money(get_value("debit"))
            credit_value = parse_money(get_value("credit"))
            if debit_value is not None:
                amount = -abs(debit_value)
            elif credit_value is not None:
                amount = abs(credit_value)

        if amount is None and bank_type == "amex" and any(keyword in normalized_description for keyword in PAYMENT_KEYWORDS):
            extracted_amount, cleaned_description = extract_embedded_amount(row_description)
            if extracted_amount is not None:
                amount = extracted_amount
                row_description = cleaned_description
                normalized_description = normalize_description(row_description)
                row_vendor = get_value("vendor") or derive_vendor(row_description)

        if parsed_date is None or amount is None:
            skipped_rows += 1
            continue

        if bank_type == "amex":
            if any(keyword in normalized_description for keyword in PAYMENT_KEYWORDS):
                amount = abs(amount)
            else:
                amount = -abs(amount)

        parsed_rows.append(
            {
                "row_index": row_index,
                "user_id": user_id,
                "date": parsed_date.date().isoformat(),
                "amount": round(amount, 2),
                "description": row_description,
                "vendor": row_vendor,
                "vendor_key": normalize_text(row_vendor),
                "vendor_rule_key": extract_pattern(row_vendor, max_words=4),
                "normalized_description": normalized_description,
                "description_rule_key": extract_pattern(row_description),
                "category": infer_category(row_description, row_category),
                "csv_category_name": csv_category_name,
                "tags": derive_tags(row_description),
                "paid_by": row_paid_by,
            }
        )
    return parsed_rows, skipped_rows


def decode_csv_bytes(file_bytes):
    for encoding in ["utf-8-sig", "utf-8", "cp1252", "latin-1"]:
        try:
            return file_bytes.decode(encoding)
        except UnicodeDecodeError:
            continue
    return None


def ai_categorize_stub(_description, _vendor):
    return ""


def create_app(test_config=None):
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY="dev",
        DATABASE=os.path.join(app.instance_path, "expense_tracker.sqlite"),
        ENABLE_LEARNING_RULES=True,
        ENABLE_AI_CATEGORIZATION=False,
    )

    if test_config is not None:
        app.config.update(test_config)

    os.makedirs(app.instance_path, exist_ok=True)
    app.config.setdefault("DB_INIT_ERROR", None)

    @app.teardown_appcontext
    def close_db(_=None):
        db = g.pop("db", None)
        if db is not None:
            db.close()

    def get_db():
        if "db" not in g:
            try:
                os.makedirs(os.path.dirname(app.config["DATABASE"]), exist_ok=True)
                g.db = sqlite3.connect(app.config["DATABASE"])
                g.db.row_factory = sqlite3.Row
                g.db.execute("PRAGMA foreign_keys = ON")
                g.db.execute("PRAGMA busy_timeout = 5000")
            except sqlite3.Error as exc:
                message = f"Unable to open SQLite database at {app.config['DATABASE']}: {exc}"
                print(f"[DB ERROR] {message}")
                app.config["DB_INIT_ERROR"] = message
                raise DatabaseInitError(message) from exc
        return g.db

    def init_db():
        try:
            apply_migrations(app.config["DATABASE"])
            app.config["DB_INIT_ERROR"] = None
        except (sqlite3.Error, OSError, RuntimeError) as exc:
            message = f"Failed to initialize SQLite database at {app.config['DATABASE']}: {exc}"
            print(f"[DB INIT ERROR] {message}")
            app.config["DB_INIT_ERROR"] = message
            raise DatabaseInitError(message) from exc

    @app.cli.command("init-db")
    def init_db_command():
        init_db()
        print("Initialized the database.")

    @app.route("/init-db")
    def init_db_route():
        init_db()
        return "Database initialized."

    @app.get("/health/db")
    def db_health():
        try:
            return jsonify(get_db_health(app.config["DATABASE"]))
        except sqlite3.Error as exc:
            return jsonify({
                "ok": False,
                "schema_version": 0,
                "missing_tables": [],
                "missing_columns": {},
                "missing_indexes": [],
                "error": str(exc),
            }), 500

    def login_required(view):
        @wraps(view)
        def wrapped_view(**kwargs):
            if g.user is None:
                return redirect(url_for("login"))
            return view(**kwargs)

        return wrapped_view

    def resolve_dashboard_filters(args, default_to_current_month=True):
        selected_month = (args.get("month") or "").strip()
        start_date = (args.get("start") or "").strip()
        end_date = (args.get("end") or "").strip()
        quick = (args.get("preset") or "").strip()

        def parse_date(value):
            if not value:
                return None
            try:
                return datetime.strptime(value, "%Y-%m-%d").date()
            except ValueError:
                return None

        today = date.today()
        if quick == "this_month":
            first = today.replace(day=1)
            start_date, end_date = first.isoformat(), today.isoformat()
            selected_month = ""
        elif quick == "last_month":
            first_of_this_month = today.replace(day=1)
            end = first_of_this_month - timedelta(days=1)
            start = end.replace(day=1)
            start_date, end_date = start.isoformat(), end.isoformat()
            selected_month = ""
        elif quick == "last_3_months":
            first_of_this_month = today.replace(day=1)
            start = (first_of_this_month - timedelta(days=1)).replace(day=1)
            start = (start - timedelta(days=1)).replace(day=1)
            start_date, end_date = start.isoformat(), today.isoformat()
            selected_month = ""
        elif quick == "ytd":
            start = today.replace(month=1, day=1)
            start_date, end_date = start.isoformat(), today.isoformat()
            selected_month = ""

        parsed_start = parse_date(start_date)
        parsed_end = parse_date(end_date)

        if start_date and end_date and parsed_start and parsed_end and parsed_start > parsed_end:
            parsed_start, parsed_end = parsed_end, parsed_start
            start_date, end_date = parsed_start.isoformat(), parsed_end.isoformat()

        if selected_month and (parsed_start or parsed_end):
            selected_month = ""

        filter_sql = "e.household_id = ?"
        params = [g.household_id]
        period_label = "All time"

        if parsed_start and parsed_end:
            filter_sql += " AND e.date BETWEEN ? AND ?"
            params.extend([start_date, end_date])
            period_label = f"{start_date} → {end_date}"
        elif selected_month:
            filter_sql += " AND e.date LIKE ?"
            params.append(f"{selected_month}%")
            period_label = selected_month
        else:
            if default_to_current_month:
                selected_month = today.strftime("%Y-%m")
                filter_sql += " AND e.date LIKE ?"
                params.append(f"{selected_month}%")
                period_label = selected_month

        return {
            "filter_sql": filter_sql,
            "params": params,
            "selected_month": selected_month,
            "start_date": start_date,
            "end_date": end_date,
            "period_label": period_label,
        }

    def current_filter_redirect_params(values_source):
        month = (values_source.get("month") or "").strip()
        start_date = (values_source.get("start") or "").strip()
        end_date = (values_source.get("end") or "").strip()

        params = {}
        if start_date and end_date:
            params["start"] = start_date
            params["end"] = end_date
        elif month:
            params["month"] = month
        return params


    def get_period_start_for_opening(filters):
        if filters["start_date"]:
            return filters["start_date"]
        if filters["selected_month"]:
            return f"{filters['selected_month']}-01"
        return None

    def _fetch_settlement_expense_totals(db, household_id, start_date=None, end_date=None, before_date=None):
        pet_placeholders = ", ".join(["?"] * len(PET_CATEGORIES))
        where_parts = ["e.household_id = ?", "e.is_transfer = 0", "e.is_personal = 0", "e.amount < 0"]
        params = [household_id]
        if before_date:
            where_parts.append("e.date < ?")
            params.append(before_date)
        else:
            if start_date:
                where_parts.append("e.date >= ?")
                params.append(start_date)
            if end_date:
                where_parts.append("e.date <= ?")
                params.append(end_date)

        where_sql = " AND ".join(where_parts)
        row = db.execute(
            f"""
            SELECT
                COALESCE(SUM(CASE
                    WHEN COALESCE(c.name, '') NOT IN ({pet_placeholders}) AND e.paid_by = 'DK'
                    THEN ABS(e.amount) ELSE 0 END), 0) AS dk_paid_shared,
                COALESCE(SUM(CASE
                    WHEN COALESCE(c.name, '') NOT IN ({pet_placeholders}) AND e.paid_by = 'YZ'
                    THEN ABS(e.amount) ELSE 0 END), 0) AS yz_paid_shared,
                COALESCE(SUM(CASE
                    WHEN COALESCE(c.name, '') IN ({pet_placeholders}) AND e.paid_by = 'DK'
                    THEN ABS(e.amount) ELSE 0 END), 0) AS pet_paid_by_dk,
                COALESCE(SUM(CASE
                    WHEN COALESCE(c.name, '') IN ({pet_placeholders}) AND e.paid_by = 'YZ'
                    THEN ABS(e.amount) ELSE 0 END), 0) AS pet_paid_by_yz,
                COALESCE(SUM(CASE
                    WHEN COALESCE(c.name, '') NOT IN ({pet_placeholders})
                    THEN ABS(e.amount) ELSE 0 END), 0) AS total_shared,
                COALESCE(SUM(ABS(e.amount)), 0) AS total_settlement_expenses
            FROM expenses e
            LEFT JOIN categories c ON e.category_id = c.id
            WHERE {where_sql}
            """,
            tuple(PET_CATEGORIES * 5 + params),
        ).fetchone()

        dk_paid_shared = float(row["dk_paid_shared"] or 0)
        yz_paid_shared = float(row["yz_paid_shared"] or 0)
        total_shared = float(row["total_shared"] or 0)
        each_share = round(total_shared / 2, 2)
        shared_delta = round(dk_paid_shared - each_share, 2)
        pet_paid_by_dk = float(row["pet_paid_by_dk"] or 0)
        pet_paid_by_yz = float(row["pet_paid_by_yz"] or 0)
        pet_delta = round(pet_paid_by_dk, 2)

        return {
            "dk_paid_shared": round(dk_paid_shared, 2),
            "yz_paid_shared": round(yz_paid_shared, 2),
            "total_shared": round(total_shared, 2),
            "each_share": each_share,
            "shared_delta": shared_delta,
            "pet_paid_by_dk": round(pet_paid_by_dk, 2),
            "pet_paid_by_yz": round(pet_paid_by_yz, 2),
            "pet_delta": pet_delta,
            "period_net_delta": round(shared_delta + pet_delta, 2),
            "total_settlement_expenses": round(float(row["total_settlement_expenses"] or 0), 2),
        }

    def _fetch_repayment_totals(db, household_id, start_date=None, end_date=None, before_date=None):
        where_parts = ["household_id = ?"]
        params = [household_id]
        if before_date:
            where_parts.append("date < ?")
            params.append(before_date)
        else:
            if start_date:
                where_parts.append("date >= ?")
                params.append(start_date)
            if end_date:
                where_parts.append("date <= ?")
                params.append(end_date)
        where_sql = " AND ".join(where_parts)
        row = db.execute(
            f"""
            SELECT
                COALESCE(SUM(CASE WHEN from_person = 'DK' AND to_person = 'YZ' THEN amount ELSE 0 END), 0) AS repayments_dk_to_yz,
                COALESCE(SUM(CASE WHEN from_person = 'YZ' AND to_person = 'DK' THEN amount ELSE 0 END), 0) AS repayments_yz_to_dk
            FROM settlement_payments
            WHERE {where_sql}
            """,
            tuple(params),
        ).fetchone()
        dk_to_yz = round(float(row["repayments_dk_to_yz"] or 0), 2)
        yz_to_dk = round(float(row["repayments_yz_to_dk"] or 0), 2)
        return {
            "repayments_dk_to_yz": dk_to_yz,
            "repayments_yz_to_dk": yz_to_dk,
            "repayment_effect": round(dk_to_yz - yz_to_dk, 2),
        }

    def calculate_settlement_ledger(db, household_id, filters):
        period = _fetch_settlement_expense_totals(
            db, household_id, start_date=filters["start_date"], end_date=filters["end_date"]
        ) if (filters["start_date"] or filters["end_date"]) else _fetch_settlement_expense_totals(
            db, household_id, start_date=None, end_date=None
        )

        if filters["selected_month"] and not filters["start_date"] and not filters["end_date"]:
            month_prefix = f"{filters['selected_month']}%"
            period = _fetch_settlement_expense_totals_for_month(db, household_id, month_prefix)
            repayments_period = _fetch_repayments_for_month(db, household_id, month_prefix)
        else:
            repayments_period = _fetch_repayment_totals(db, household_id, start_date=filters["start_date"], end_date=filters["end_date"])

        opening_cutoff = get_period_start_for_opening(filters)
        if opening_cutoff:
            opening_expenses = _fetch_settlement_expense_totals(db, household_id, before_date=opening_cutoff)
            opening_repayments = _fetch_repayment_totals(db, household_id, before_date=opening_cutoff)
            opening_balance = round(opening_expenses["period_net_delta"] + opening_repayments["repayment_effect"], 2)
        else:
            opening_balance = 0.0

        closing_balance = round(opening_balance + period["period_net_delta"] + repayments_period["repayment_effect"], 2)
        period.update(repayments_period)
        period.update({
            "opening_balance": opening_balance,
            "closing_balance": closing_balance,
            "dk_owes_now": round(max(0, -closing_balance), 2),
            "yz_owes_now": round(max(0, closing_balance), 2),
        })
        return period

    def _fetch_settlement_expense_totals_for_month(db, household_id, month_prefix):
        return _fetch_settlement_expense_totals_like_month(db, household_id, month_prefix)

    def _fetch_settlement_expense_totals_like_month(db, household_id, month_prefix):
        pet_placeholders = ", ".join(["?"] * len(PET_CATEGORIES))
        row = db.execute(
            f"""
            SELECT
                COALESCE(SUM(CASE WHEN COALESCE(c.name, '') NOT IN ({pet_placeholders}) AND e.paid_by='DK' THEN ABS(e.amount) ELSE 0 END), 0) AS dk_paid_shared,
                COALESCE(SUM(CASE WHEN COALESCE(c.name, '') NOT IN ({pet_placeholders}) AND e.paid_by='YZ' THEN ABS(e.amount) ELSE 0 END), 0) AS yz_paid_shared,
                COALESCE(SUM(CASE WHEN COALESCE(c.name, '') IN ({pet_placeholders}) AND e.paid_by='DK' THEN ABS(e.amount) ELSE 0 END), 0) AS pet_paid_by_dk,
                COALESCE(SUM(CASE WHEN COALESCE(c.name, '') IN ({pet_placeholders}) AND e.paid_by='YZ' THEN ABS(e.amount) ELSE 0 END), 0) AS pet_paid_by_yz,
                COALESCE(SUM(CASE WHEN COALESCE(c.name, '') NOT IN ({pet_placeholders}) THEN ABS(e.amount) ELSE 0 END), 0) AS total_shared,
                COALESCE(SUM(ABS(e.amount)), 0) AS total_settlement_expenses
            FROM expenses e
            LEFT JOIN categories c ON e.category_id = c.id
            WHERE e.household_id = ? AND e.is_transfer = 0 AND e.is_personal = 0 AND e.amount < 0 AND e.date LIKE ?
            """,
            tuple(PET_CATEGORIES * 5 + [household_id, month_prefix]),
        ).fetchone()
        dk_paid_shared = float(row["dk_paid_shared"] or 0)
        yz_paid_shared = float(row["yz_paid_shared"] or 0)
        total_shared = float(row["total_shared"] or 0)
        each_share = round(total_shared / 2, 2)
        shared_delta = round(dk_paid_shared - each_share, 2)
        pet_delta = round(float(row["pet_paid_by_dk"] or 0), 2)
        return {
            "dk_paid_shared": round(dk_paid_shared, 2),
            "yz_paid_shared": round(yz_paid_shared, 2),
            "total_shared": round(total_shared, 2),
            "each_share": each_share,
            "shared_delta": shared_delta,
            "pet_paid_by_dk": round(float(row["pet_paid_by_dk"] or 0), 2),
            "pet_paid_by_yz": round(float(row["pet_paid_by_yz"] or 0), 2),
            "pet_delta": pet_delta,
            "period_net_delta": round(shared_delta + pet_delta, 2),
            "total_settlement_expenses": round(float(row["total_settlement_expenses"] or 0), 2),
        }

    def _fetch_repayments_for_month(db, household_id, month_prefix):
        row = db.execute(
            """
            SELECT
                COALESCE(SUM(CASE WHEN from_person='DK' AND to_person='YZ' THEN amount ELSE 0 END), 0) AS repayments_dk_to_yz,
                COALESCE(SUM(CASE WHEN from_person='YZ' AND to_person='DK' THEN amount ELSE 0 END), 0) AS repayments_yz_to_dk
            FROM settlement_payments
            WHERE household_id = ? AND date LIKE ?
            """,
            (household_id, month_prefix),
        ).fetchone()
        dk_to_yz = round(float(row["repayments_dk_to_yz"] or 0), 2)
        yz_to_dk = round(float(row["repayments_yz_to_dk"] or 0), 2)
        return {"repayments_dk_to_yz": dk_to_yz, "repayments_yz_to_dk": yz_to_dk, "repayment_effect": round(dk_to_yz - yz_to_dk, 2)}

    def build_monthly_breakdown(db, household_id, filters, opening_balance):
        month_rows = db.execute(
            """
            SELECT DISTINCT SUBSTR(date, 1, 7) AS month
            FROM expenses
            WHERE household_id = ?
              AND is_transfer = 0
              AND is_personal = 0
              AND amount < 0
              AND date >= ? AND date <= ?
            ORDER BY month ASC
            """,
            (household_id, filters["start_date"], filters["end_date"]),
        ).fetchall() if (filters["start_date"] and filters["end_date"]) else db.execute(
            """
            SELECT DISTINCT SUBSTR(date, 1, 7) AS month
            FROM expenses
            WHERE household_id = ?
              AND is_transfer = 0
              AND is_personal = 0
              AND amount < 0
              AND date LIKE ?
            ORDER BY month ASC
            """,
            (household_id, f"{filters['selected_month']}%"),
        ).fetchall() if filters["selected_month"] else []

        running = opening_balance
        rows = []
        totals = {"total_expenses": 0.0, "dk_owes": 0.0, "yz_owes": 0.0, "repayments_dk_to_yz": 0.0, "repayments_yz_to_dk": 0.0, "net_delta": 0.0}
        for m in month_rows:
            month = m["month"]
            expense = _fetch_settlement_expense_totals_like_month(db, household_id, f"{month}%")
            repayments = _fetch_repayments_for_month(db, household_id, f"{month}%")
            month_net_delta = expense["period_net_delta"]
            running = round(running + month_net_delta + repayments["repayment_effect"], 2)
            dk_owes = round(abs(month_net_delta) if month_net_delta < 0 else 0, 2)
            yz_owes = round(month_net_delta if month_net_delta > 0 else 0, 2)
            row = {
                "month": month,
                "total_expenses": expense["total_settlement_expenses"],
                "dk_owes": dk_owes,
                "yz_owes": yz_owes,
                "dk_paid_shared": expense["dk_paid_shared"],
                "yz_paid_shared": expense["yz_paid_shared"],
                "each_share": expense["each_share"],
                "repayments_dk_to_yz": repayments["repayments_dk_to_yz"],
                "repayments_yz_to_dk": repayments["repayments_yz_to_dk"],
                "net_delta": month_net_delta,
                "running_balance": running,
            }
            rows.append(row)
            totals["total_expenses"] += row["total_expenses"]
            totals["dk_owes"] += row["dk_owes"]
            totals["yz_owes"] += row["yz_owes"]
            totals["repayments_dk_to_yz"] += row["repayments_dk_to_yz"]
            totals["repayments_yz_to_dk"] += row["repayments_yz_to_dk"]
            totals["net_delta"] += row["net_delta"]

        return rows, {k: round(v, 2) for k, v in totals.items()}

    def ensure_user_household(user_id, db=None):
        db = db or get_db()
        membership = db.execute(
            """
            SELECT hm.household_id, hm.role
            FROM household_members hm
            WHERE hm.user_id = ?
            ORDER BY hm.id ASC
            LIMIT 1
            """,
            (user_id,),
        ).fetchone()
        if membership is not None:
            db.execute("UPDATE expenses SET household_id = ? WHERE user_id = ? AND household_id IS NULL", (membership["household_id"], user_id))
            return membership["household_id"], membership["role"]

        household_name = f"{user_id}-household"
        db.execute("INSERT INTO households (name) VALUES (?)", (household_name,))
        household_id = db.execute("SELECT last_insert_rowid() as id").fetchone()["id"]
        db.execute(
            "INSERT INTO household_members (household_id, user_id, role) VALUES (?, ?, 'owner')",
            (household_id, user_id),
        )
        db.execute("UPDATE expenses SET household_id = ? WHERE user_id = ?", (household_id, user_id))
        return household_id, "owner"

    def get_user_household(user_id, db=None):
        db = db or get_db()
        household_id, role = ensure_user_household(user_id, db)
        return {"household_id": household_id, "role": role}

    def log_audit(action, expense_id=None, details=None, user_id=None, db=None):
        actor_id = user_id or (g.user["id"] if getattr(g, "user", None) else None)
        if actor_id is None:
            return
        db = db or get_db()
        payload = json.dumps(details or {})
        columns = {row["name"] for row in db.execute("PRAGMA table_info(audit_logs)").fetchall()}
        if "expense_id" in columns:
            db.execute(
                "INSERT INTO audit_logs (user_id, action, expense_id, details) VALUES (?, ?, ?, ?)",
                (actor_id, action, expense_id, payload),
            )
            return
        db.execute(
            """
            INSERT INTO audit_logs (household_id, user_id, action, entity, entity_id, meta_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (g.household_id, actor_id, action, "expense" if expense_id is not None else None, expense_id, payload),
        )

    def get_household_expense(expense_id):
        return get_db().execute(
            "SELECT * FROM expenses WHERE id = ? AND household_id = ?",
            (expense_id, g.household_id),
        ).fetchone()

    def render_db_init_error_response():
        message = app.config.get("DB_INIT_ERROR") or "Database initialization failed."
        return f"<h1>Database initialization failed</h1><p>{message}</p>", 500

    @app.before_request
    def load_logged_in_user():
        if app.config.get("DB_INIT_ERROR"):
            return render_db_init_error_response()

        user_id = session.get("user_id")
        g.household_id = None
        g.household_role = None
        if user_id is None:
            g.user = None
        else:
            g.user = get_db().execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
            if g.user is not None:
                household = get_user_household(g.user["id"])
                g.household_id = household["household_id"]
                g.household_role = household["role"]

    def resolve_learned_category(user_id, key_type, pattern, available_categories, db):
        if not app.config.get("ENABLE_LEARNING_RULES", True):
            return ""
        if not pattern:
            return ""

        rule = db.execute(
            """
            SELECT cr.id, COALESCE(cr.category, c.name) as category_name
            FROM category_rules cr
            LEFT JOIN categories c ON c.id = cr.category_id
            WHERE cr.user_id = ? AND cr.key_type = ? AND cr.pattern = ? AND COALESCE(cr.enabled, cr.is_enabled, 1) = 1
            ORDER BY cr.priority ASC, cr.hits DESC, cr.id DESC
            LIMIT 1
            """,
            (user_id, key_type, pattern),
        ).fetchone()

        if rule is None:
            rule = db.execute(
                """
                SELECT cr.id, COALESCE(cr.category, c.name) as category_name
                FROM category_rules cr
                LEFT JOIN categories c ON c.id = cr.category_id
                WHERE cr.user_id = ? AND cr.key_type = ? AND ? LIKE cr.pattern || '%' AND COALESCE(cr.enabled, cr.is_enabled, 1) = 1
                ORDER BY LENGTH(cr.pattern) DESC, cr.priority ASC, cr.hits DESC
                LIMIT 1
                """,
                (user_id, key_type, pattern),
            ).fetchone()

        if rule is None:
            return ""

        category = pick_existing_category(rule["category_name"], available_categories)
        if not category:
            return ""

        db.execute(
            "UPDATE category_rules SET hits = hits + 1, last_used_at = CURRENT_TIMESTAMP WHERE id = ?",
            (rule["id"],),
        )
        db.commit()
        return category

    def categorize_transaction(user_id, description, vendor, raw_category, available_categories, db):
        if is_transfer_transaction(description, raw_category):
            transfer_category = pick_existing_category("Credit Card Payments", available_categories, "Transfers")
            return {"category": transfer_category, "confidence": 100, "source": "transfer"}

        vendor_pattern = extract_pattern(vendor or derive_vendor(description), max_words=4)
        learned_vendor = resolve_learned_category(user_id, "vendor", vendor_pattern, available_categories, db)
        if learned_vendor:
            return {"category": learned_vendor, "confidence": 95, "source": "learned_vendor"}

        desc_pattern = extract_pattern(description)
        learned_description = resolve_learned_category(user_id, "description", desc_pattern, available_categories, db)
        if learned_description:
            return {"category": learned_description, "confidence": 90, "source": "learned_description"}

        keyword_vendor = infer_category(vendor or "", raw_category, available_categories)
        if keyword_vendor:
            return {"category": keyword_vendor, "confidence": 75, "source": "keyword_vendor"}

        keyword_description = infer_category(description, raw_category, available_categories)
        if keyword_description:
            return {"category": keyword_description, "confidence": 65, "source": "keyword_description"}

        if app.config.get("ENABLE_AI_CATEGORIZATION", False):
            ai_category = ai_categorize_stub(description, vendor or "")
            if ai_category:
                return {"category": ai_category, "confidence": 25, "source": "unknown"}

        return {"category": "", "confidence": 25, "source": "unknown"}

    def learn_rule(user_id, description, vendor, category_id, source):
        if not app.config.get("ENABLE_LEARNING_RULES", True):
            return
        db = get_db()
        category = db.execute(
            "SELECT name FROM categories WHERE id = ? AND user_id = ?",
            (category_id, user_id),
        ).fetchone()
        if category is None:
            return
        if is_transfer_transaction(description, category["name"]):
            return

        vendor_pattern = extract_pattern(vendor or "", max_words=4)
        description_pattern = extract_pattern(description)

        key_type = "vendor" if vendor_pattern else "description"
        pattern = vendor_pattern if vendor_pattern else description_pattern
        if not pattern or pattern in LEARNING_STOPLIST or len(pattern) < 3:
            return

        rule_columns = {row["name"] for row in db.execute("PRAGMA table_info(category_rules)").fetchall()}
        has_category_id = "category_id" in rule_columns
        has_is_enabled = "is_enabled" in rule_columns

        existing = db.execute(
            "SELECT id FROM category_rules WHERE user_id = ? AND key_type = ? AND pattern = ?",
            (user_id, key_type, pattern),
        ).fetchone()
        if existing:
            set_parts = ["category = ?", "source = ?", "enabled = 1"]
            params = [category["name"], source]
            if has_category_id:
                set_parts.append("category_id = ?")
                params.append(category_id)
            if has_is_enabled:
                set_parts.append("is_enabled = 1")
            params.append(existing["id"])
            db.execute(f"UPDATE category_rules SET {', '.join(set_parts)} WHERE id = ?", params)
        else:
            columns = ["user_id", "key_type", "pattern", "category", "source", "enabled"]
            values = [user_id, key_type, pattern, category["name"], source, 1]
            if has_category_id:
                columns.append("category_id")
                values.append(category_id)
            if has_is_enabled:
                columns.append("is_enabled")
                values.append(1)
            placeholders = ", ".join(["?"] * len(values))
            db.execute(
                f"INSERT INTO category_rules ({', '.join(columns)}) VALUES ({placeholders})",
                values,
            )
        db.commit()

    def ensure_default_categories(user_id):
        db = get_db()

        existing = db.execute(
            "SELECT id, name FROM categories WHERE user_id = ?",
            (user_id,),
        ).fetchall()
        existing_lookup = {normalize_description(row["name"]): row["id"] for row in existing}

        for category in DEFAULT_CATEGORIES:
            db.execute(
                "INSERT OR IGNORE INTO categories (user_id, name) VALUES (?, ?)",
                (user_id, category),
            )

        categories = db.execute(
            "SELECT id, name FROM categories WHERE user_id = ?",
            (user_id,),
        ).fetchall()
        lookup = {row["name"]: row["id"] for row in categories}

        for old_name, new_name in LEGACY_CATEGORY_MAPPING.items():
            old_id = existing_lookup.get(normalize_description(old_name))
            new_id = lookup.get(new_name)
            if old_id and new_id and old_id != new_id:
                db.execute(
                    "UPDATE expenses SET category_id = ? WHERE user_id = ? AND category_id = ?",
                    (new_id, user_id, old_id),
                )
                db.execute(
                    "DELETE FROM categories WHERE user_id = ? AND id = ?",
                    (user_id, old_id),
                )

        db.execute(
            """
            UPDATE expenses
            SET is_personal = CASE WHEN category_id = (SELECT id FROM categories WHERE user_id = ? AND name = 'Personal') THEN 1 ELSE 0 END,
                is_transfer = CASE WHEN category_id IN (
                    SELECT id FROM categories WHERE user_id = ? AND name IN ('Transfers', 'Credit Card Payments')
                ) THEN 1 ELSE 0 END
            WHERE user_id = ?
            """,
            (user_id, user_id, user_id),
        )
        db.commit()

    @app.route("/")
    def index():
        if g.user:
            return redirect(url_for("dashboard"))
        return redirect(url_for("login"))

    @app.route("/register", methods=("GET", "POST"))
    def register():
        if request.method == "POST":
            username = request.form["username"].strip()
            password = request.form["password"]
            error = None
            if not username:
                error = "Username is required."
            elif not password:
                error = "Password is required."

            if error is None:
                try:
                    db = get_db()
                    db.execute(
                        "INSERT INTO users (username, password_hash) VALUES (?, ?)",
                        (username, generate_password_hash(password)),
                    )
                    db.commit()
                    user_id = db.execute(
                        "SELECT id FROM users WHERE username = ?", (username,)
                    ).fetchone()["id"]
                    ensure_default_categories(user_id)
                    flash("Registration successful. Please login.")
                    return redirect(url_for("login"))
                except sqlite3.IntegrityError:
                    error = "User already exists."

            flash(error)
        return render_template("register.html")

    @app.route("/login", methods=("GET", "POST"))
    def login():
        if request.method == "POST":
            username = request.form["username"].strip()
            password = request.form["password"]
            db = get_db()
            user = db.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()
            error = None

            if user is None or not check_password_hash(user["password_hash"], password):
                error = "Incorrect username or password."

            if error is None:
                session.clear()
                session["user_id"] = user["id"]
                ensure_default_categories(user["id"])
                return redirect(url_for("dashboard"))

            flash(error)

        return render_template("login.html")

    @app.route("/logout")
    def logout():
        session.clear()
        return redirect(url_for("login"))

    @app.route("/household", methods=("GET", "POST"))
    @login_required
    def household_settings():
        db = get_db()
        if request.method == "POST":
            if g.household_role != "owner":
                flash("Only household owner can invite members.")
                return redirect(url_for("household_settings"))
            invite_email = (request.form.get("invite_email") or "").strip()
            invite_code = uuid.uuid4().hex[:8].upper()
            db.execute(
                "INSERT INTO household_invites (household_id, created_by_user_id, email, code) VALUES (?, ?, ?, ?)",
                (g.household_id, g.user["id"], invite_email or None, invite_code),
            )
            db.commit()
            flash(f"Invite created. Share this code: {invite_code}")
            return redirect(url_for("household_settings"))

        members = db.execute(
            """
            SELECT u.username, hm.role
            FROM household_members hm
            JOIN users u ON u.id = hm.user_id
            WHERE hm.household_id = ?
            ORDER BY hm.id ASC
            """,
            (g.household_id,),
        ).fetchall()
        invites = db.execute(
            "SELECT code, email, created_at FROM household_invites WHERE household_id = ? ORDER BY id DESC LIMIT 10",
            (g.household_id,),
        ).fetchall()
        return render_template("household.html", members=members, invites=invites, role=g.household_role)

    @app.route("/household/join", methods=("GET", "POST"))
    @login_required
    def join_household():
        if request.method == "POST":
            code = (request.form.get("code") or "").strip().upper()
            if not code:
                flash("Invite code is required.")
                return redirect(url_for("join_household"))
            db = get_db()
            invite = db.execute("SELECT * FROM household_invites WHERE code = ?", (code,)).fetchone()
            if invite is None:
                flash("Invalid invite code.")
                return redirect(url_for("join_household"))

            existing = db.execute("SELECT id FROM household_members WHERE user_id = ?", (g.user["id"],)).fetchone()
            if existing is not None:
                db.execute("DELETE FROM household_members WHERE user_id = ?", (g.user["id"],))
            db.execute(
                "INSERT OR IGNORE INTO household_members (household_id, user_id, role) VALUES (?, ?, 'member')",
                (invite["household_id"], g.user["id"]),
            )
            db.execute(
                "UPDATE expenses SET household_id = ? WHERE user_id = ?",
                (invite["household_id"], g.user["id"]),
            )
            db.commit()
            flash("Joined household successfully.")
            return redirect(url_for("dashboard"))

        return render_template("join_household.html")

    @app.route("/dashboard")
    @login_required
    def dashboard():
        filters = resolve_dashboard_filters(request.args)
        db = get_db()

        expenses = db.execute(
            """
            SELECT e.id, e.date, e.amount, e.description, c.name as category, e.updated_at,
                   e.category_confidence, e.category_source, e.paid_by
            FROM expenses e
            LEFT JOIN categories c ON e.category_id = c.id
            WHERE {filter_sql}
            ORDER BY e.date DESC, e.id DESC
            """.format(filter_sql=filters["filter_sql"]),
            tuple(filters["params"]),
        ).fetchall()

        summary = db.execute(
            """
            SELECT COALESCE(c.name, 'Uncategorized') as category, ROUND(SUM(e.amount), 2) as total
            FROM expenses e
            LEFT JOIN categories c ON e.category_id = c.id
            WHERE {filter_sql} AND e.is_transfer = 0 AND e.is_personal = 0
            GROUP BY COALESCE(c.name, 'Uncategorized')
            ORDER BY total DESC
            """.format(filter_sql=filters["filter_sql"]),
            tuple(filters["params"]),
        ).fetchall()

        total = db.execute(
            """
            SELECT ROUND(COALESCE(SUM(amount), 0), 2) as total
            FROM expenses e
            WHERE {filter_sql} AND e.is_transfer = 0
            """.format(filter_sql=filters["filter_sql"]),
            tuple(filters["params"]),
        ).fetchone()["total"]

        shared_total = db.execute(
            """
            SELECT ROUND(COALESCE(SUM(amount), 0), 2) as total
            FROM expenses e
            WHERE {filter_sql} AND e.is_transfer = 0 AND e.is_personal = 0
            """.format(filter_sql=filters["filter_sql"]),
            tuple(filters["params"]),
        ).fetchone()["total"]

        settlement = calculate_settlement_ledger(db, g.household_id, filters)
        monthly_breakdown, monthly_totals = build_monthly_breakdown(
            db, g.household_id, filters, settlement["opening_balance"]
        )
        settlement["monthly_breakdown"] = monthly_breakdown
        settlement["monthly_totals"] = monthly_totals

        repayment_filters = [filters["filter_sql"].replace("e.", "")]
        repayment_params = list(filters["params"])
        repayments = db.execute(
            """
            SELECT id, date, from_person, to_person, amount, note
            FROM settlement_payments
            WHERE {where_sql}
            ORDER BY date DESC, id DESC
            """.format(where_sql=" AND ".join(repayment_filters)),
            tuple(repayment_params),
        ).fetchall()

        all_categories = db.execute(
            "SELECT id, name FROM categories WHERE user_id = ? ORDER BY name",
            (g.user["id"],),
        ).fetchall()

        return render_template(
            "dashboard.html",
            expenses=expenses,
            summary=summary,
            total=total,
            shared_total=shared_total,
            settlement=settlement,
            repayments=repayments,
            selected_month=filters["selected_month"],
            start_date=filters["start_date"],
            end_date=filters["end_date"],
            period_label=filters["period_label"],
            all_categories=all_categories,
            household_role=g.household_role,
        )

    @app.post("/settlement-payments")
    @login_required
    def create_settlement_payment():
        payment_date = (request.form.get("date") or "").strip()
        from_person = normalize_paid_by(request.form.get("from_person", ""))
        to_person = normalize_paid_by(request.form.get("to_person", ""))
        amount = parse_money(request.form.get("amount", ""))
        note = (request.form.get("note") or "").strip()

        if not payment_date:
            flash("Repayment date is required.")
            return redirect(url_for("dashboard", **current_filter_redirect_params(request.form)))
        if from_person not in {"DK", "YZ"} or to_person not in {"DK", "YZ"}:
            flash("Repayment parties must be DK or YZ.")
            return redirect(url_for("dashboard", **current_filter_redirect_params(request.form)))
        if from_person == to_person:
            flash("From and To must be different people.")
            return redirect(url_for("dashboard", **current_filter_redirect_params(request.form)))
        if amount is None or amount <= 0:
            flash("Repayment amount must be greater than 0.")
            return redirect(url_for("dashboard", **current_filter_redirect_params(request.form)))

        db = get_db()
        db.execute(
            """
            INSERT INTO settlement_payments (household_id, date, from_person, to_person, amount, note, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (g.household_id, payment_date, from_person, to_person, amount, note, datetime.utcnow().isoformat()),
        )
        db.commit()
        flash("Repayment recorded")
        return redirect(url_for("dashboard", **current_filter_redirect_params(request.form)))

    @app.post("/settlement-payments/<int:payment_id>/delete")
    @login_required
    def delete_settlement_payment(payment_id):
        db = get_db()
        db.execute("DELETE FROM settlement_payments WHERE id = ? AND household_id = ?", (payment_id, g.household_id))
        db.commit()
        flash("Repayment deleted")
        return redirect(url_for("dashboard", **current_filter_redirect_params(request.form)))

    @app.route("/expenses/new", methods=("GET", "POST"))
    @login_required
    def create_expense():
        db = get_db()
        categories = db.execute(
            "SELECT * FROM categories WHERE user_id = ? ORDER BY name", (g.user["id"],)
        ).fetchall()

        if request.method == "POST":
            expense_date = request.form["date"]
            amount = request.form["amount"]
            paid_by = normalize_paid_by(request.form.get("paid_by", ""))
            category_id = request.form.get("category_id") or None
            description = request.form.get("description", "").strip()
            category_name = db.execute(
                "SELECT name FROM categories WHERE id = ? AND user_id = ?",
                (category_id, g.user["id"]),
            ).fetchone()
            resolved_category = category_name["name"] if category_name else ""
            if not resolved_category:
                available_category_names = [row["name"] for row in categories]
                categorized = categorize_transaction(g.user["id"], description, "", "", available_category_names, db)
                resolved_category = categorized["category"]
                if resolved_category:
                    found = db.execute(
                        "SELECT id FROM categories WHERE user_id = ? AND name = ?",
                        (g.user["id"], resolved_category),
                    ).fetchone()
                    if found:
                        category_id = found["id"]

            categorization = categorize_transaction(
                g.user["id"], description, derive_vendor(description), resolved_category, [row["name"] for row in categories], db
            )
            if resolved_category and categorization["source"] == "unknown":
                categorization = {"category": resolved_category, "confidence": 25, "source": "unknown"}

            try:
                amount_value = float(amount)
            except ValueError:
                flash("Amount must be a valid number.")
                return render_template("expense_form.html", categories=categories, expense=None)

            if paid_by not in {"", "DK", "YZ"}:
                flash("Paid by must be DK or YZ.")
                return render_template("expense_form.html", categories=categories, expense=None)

            db.execute(
                """
                INSERT INTO expenses (
                    user_id, household_id, date, amount, category_id, description, vendor, paid_by,
                    is_transfer, is_personal, category_confidence, category_source, tags
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    g.user["id"],
                    g.household_id,
                    expense_date,
                    amount_value,
                    category_id,
                    description,
                    derive_vendor(description),
                    paid_by,
                    1 if is_transfer_transaction(description, resolved_category) else 0,
                    1 if resolved_category == "Personal" else 0,
                    categorization["confidence"],
                    categorization["source"],
                    json.dumps(derive_tags(description)),
                ),
            )
            expense_id = db.execute("SELECT last_insert_rowid() as id").fetchone()["id"]
            log_audit("create", expense_id=expense_id, details={"description": description, "amount": amount_value}, db=db)
            db.commit()
            flash("Expense added.")
            return redirect(url_for("dashboard"))

        return render_template("expense_form.html", categories=categories, expense=None)

    def get_user_expense(expense_id):
        expense = get_household_expense(expense_id)
        return expense

    @app.get("/expenses/<int:expense_id>")
    @login_required
    def expense_detail(expense_id):
        db = get_db()
        expense = db.execute(
            """
            SELECT e.*, COALESCE(c.name, 'Uncategorized') AS category
            FROM expenses e
            LEFT JOIN categories c ON c.id = e.category_id
            WHERE e.id = ? AND e.household_id = ?
            """,
            (expense_id, g.household_id),
        ).fetchone()
        if expense is None:
            flash("Expense not found.")
            return redirect(url_for("dashboard"))

        audit_columns = {row["name"] for row in db.execute("PRAGMA table_info(audit_logs)").fetchall()}
        if "expense_id" in audit_columns:
            logs = db.execute(
                """
                SELECT al.*, u.username
                FROM audit_logs al
                LEFT JOIN users u ON u.id = al.user_id
                WHERE al.expense_id = ?
                ORDER BY al.created_at DESC, al.id DESC
                """,
                (expense_id,),
            ).fetchall()
        else:
            logs = db.execute(
                """
                SELECT al.*, u.username
                FROM audit_logs al
                LEFT JOIN users u ON u.id = al.user_id
                WHERE al.entity = 'expense' AND al.entity_id = ?
                ORDER BY al.created_at DESC, al.id DESC
                """,
                (expense_id,),
            ).fetchall()
        parsed_logs = []
        for row in logs:
            raw_payload = row["meta_json"] if "meta_json" in row.keys() else row["details"]
            details = raw_payload or ""
            try:
                details = json.loads(details) if details else {}
            except (TypeError, ValueError, json.JSONDecodeError):
                details = {"raw": raw_payload}
            parsed_logs.append({"action": row["action"], "username": row["username"], "created_at": row["created_at"], "details": details})

        return render_template("expense_detail.html", expense=expense, audit_logs=parsed_logs)

    @app.route("/expenses/<int:expense_id>/edit", methods=("GET", "POST"))
    @login_required
    def edit_expense(expense_id):
        db = get_db()
        expense = get_user_expense(expense_id)
        if expense is None:
            flash("Expense not found.")
            return redirect(url_for("dashboard"))

        categories = db.execute(
            "SELECT * FROM categories WHERE user_id = ? ORDER BY name", (g.user["id"],)
        ).fetchall()

        if request.method == "POST":
            expense_date = request.form["date"]
            amount = request.form["amount"]
            paid_by = normalize_paid_by(request.form.get("paid_by", ""))
            category_id = request.form.get("category_id") or None
            description = request.form.get("description", "").strip()
            submitted_updated_at = (request.form.get("updated_at") or "").strip()
            effective_updated_at = submitted_updated_at or (expense["updated_at"] or "")
            redirect_params = current_filter_redirect_params(request.form)
            category_name = db.execute(
                "SELECT name FROM categories WHERE id = ? AND user_id = ?",
                (category_id, g.user["id"]),
            ).fetchone()
            available_category_names = [row["name"] for row in categories]
            previous_category_id = expense["category_id"]
            resolved_category = category_name["name"] if category_name else ""
            if category_name is None:
                categorized = categorize_transaction(g.user["id"], description, expense["vendor"] or "", "", available_category_names, db)
                resolved_category = categorized["category"]
            if category_name is None and resolved_category:
                found = db.execute(
                    "SELECT id FROM categories WHERE user_id = ? AND name = ?",
                    (g.user["id"], resolved_category),
                ).fetchone()
                if found:
                    category_id = found["id"]

            categorization = categorize_transaction(
                g.user["id"], description, derive_vendor(description), resolved_category, available_category_names, db
            )
            if resolved_category and categorization["source"] == "unknown":
                categorization = {"category": resolved_category, "confidence": 25, "source": "unknown"}

            try:
                amount_value = float(amount)
            except ValueError:
                flash("Amount must be a valid number.")
                return render_template("expense_form.html", categories=categories, expense=expense, filter_params=redirect_params)

            if paid_by not in {"", "DK", "YZ"}:
                flash("Paid by must be DK or YZ.")
                return render_template("expense_form.html", categories=categories, expense=expense, filter_params=redirect_params)

            result = db.execute(
                """
                UPDATE expenses
                SET date = ?, amount = ?, category_id = ?, description = ?, vendor = ?, is_transfer = ?, is_personal = ?,
                    category_confidence = ?, category_source = ?, tags = ?, paid_by = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ? AND household_id = ? AND COALESCE(updated_at, '') = ?
                """,
                (
                    expense_date,
                    amount_value,
                    category_id,
                    description,
                    derive_vendor(description),
                    1 if is_transfer_transaction(description, resolved_category) else 0,
                    1 if resolved_category == "Personal" else 0,
                    categorization["confidence"],
                    categorization["source"],
                    json.dumps(derive_tags(description)),
                    paid_by,
                    expense_id,
                    g.household_id,
                    effective_updated_at,
                ),
            )
            if result.rowcount == 0:
                flash("This transaction was edited in another session. Please reload.")
                db.rollback()
                return redirect(url_for("edit_expense", expense_id=expense_id, **redirect_params))
            log_audit("edit", expense_id=expense_id, details={"description": description, "amount": amount_value}, db=db)
            db.commit()
            if category_id and str(previous_category_id or "") != str(category_id):
                learn_rule(g.user["id"], description, expense["vendor"] or derive_vendor(description), category_id, "manual_edit")
            flash("Expense updated.")
            return redirect(url_for("dashboard", **redirect_params))

        return render_template("expense_form.html", categories=categories, expense=expense, filter_params=current_filter_redirect_params(request.args))

    def delete_household_expenses(db, expense_ids):
        ids = []
        for raw_id in expense_ids:
            try:
                ids.append(int(raw_id))
            except (TypeError, ValueError):
                continue

        ids = list(dict.fromkeys(ids))
        if not ids:
            return {"ok": False, "error": "Please select at least one transaction.", "deleted": 0}

        placeholders = ", ".join(["?"] * len(ids))
        expenses = db.execute(
            f"SELECT id, description, amount FROM expenses WHERE household_id = ? AND id IN ({placeholders})",
            [g.household_id, *ids],
        ).fetchall()
        if len(expenses) != len(ids):
            return {"ok": False, "error": "One or more selected transactions are invalid.", "deleted": 0}

        try:
            with db:
                audit_columns = {row["name"] for row in db.execute("PRAGMA table_info(audit_logs)").fetchall()}
                if "expense_id" in audit_columns:
                    db.execute(
                        f"DELETE FROM audit_logs WHERE expense_id IN ({placeholders})",
                        ids,
                    )
                result = db.execute(
                    f"DELETE FROM expenses WHERE household_id = ? AND id IN ({placeholders})",
                    [g.household_id, *ids],
                )
                for expense in expenses:
                    log_audit(
                        "delete",
                        expense_id=expense["id"],
                        details={"description": expense["description"], "amount": expense["amount"]},
                        db=db,
                    )
        except sqlite3.IntegrityError:
            return {
                "ok": False,
                "error": "Unable to delete one or more transactions due to related records. Please try again after refreshing.",
                "deleted": 0,
            }

        return {"ok": True, "deleted": result.rowcount}

    @app.post("/expenses/<int:expense_id>/delete")
    @login_required
    def delete_expense(expense_id):
        db = get_db()
        redirect_params = current_filter_redirect_params(request.form)
        result = delete_household_expenses(db, [expense_id])
        if not result["ok"]:
            app.logger.warning("Single delete failed for expense_id=%s household_id=%s: %s", expense_id, g.household_id, result["error"])
            flash(result["error"])
            return redirect(url_for("dashboard", **redirect_params))

        app.logger.info("Single delete succeeded for expense_id=%s household_id=%s", expense_id, g.household_id)
        flash("Expense deleted.")
        return redirect(url_for("dashboard", **redirect_params))

    @app.post("/expenses/bulk")
    @login_required
    def bulk_expense_action():
        db = get_db()
        action = request.form["action"].strip().lower() if "action" in request.form else ""
        if action in {"delete", "delete_selected", "delete_expense"}:
            action = "delete"
        redirect_params = current_filter_redirect_params(request.form)

        def redirect_dashboard():
            return redirect(url_for("dashboard", **redirect_params))

        raw_ids = []
        for key in ("selected_ids", "expense_ids", "ids"):
            raw_ids.extend(request.form.getlist(key))
        for key in ("single_selected_id", "selected_id", "expense_id", "id"):
            value = request.form.get(key)
            if value:
                raw_ids.append(value)

        ids = []
        for raw_id in raw_ids:
            if raw_id is None:
                continue
            if isinstance(raw_id, str):
                id_chunks = [chunk.strip() for chunk in raw_id.split(",")]
            else:
                id_chunks = [raw_id]

            for chunk in id_chunks:
                if not chunk:
                    continue
                try:
                    ids.append(int(chunk))
                except (TypeError, ValueError):
                    continue

        ids = list(dict.fromkeys(ids))
        if not ids:
            flash("Please select at least one transaction.")
            return redirect_dashboard()

        placeholders = ", ".join(["?"] * len(ids))

        if action == "delete":
            result = delete_household_expenses(db, ids)
            if not result["ok"]:
                app.logger.warning("Bulk delete failed for household_id=%s ids=%s: %s", g.household_id, ids, result["error"])
                flash(result["error"])
                return redirect_dashboard()

            app.logger.info("Bulk delete succeeded for household_id=%s ids=%s deleted=%s", g.household_id, ids, result["deleted"])
            flash(f"Deleted {result['deleted']} transactions")
            return redirect_dashboard()

        owner_count = db.execute(
            f"SELECT COUNT(*) as count FROM expenses WHERE household_id = ? AND id IN ({placeholders})",
            [g.household_id, *ids],
        ).fetchone()["count"]
        if owner_count != len(ids):
            flash("One or more selected transactions are invalid.")
            return redirect_dashboard()

        if action == "set_category":
            category_id = (request.form.get("category_id") or "").strip()
            if not category_id:
                flash("Please choose a category.")
                return redirect_dashboard()
            category = db.execute(
                "SELECT id FROM categories WHERE id = ? AND user_id = ?",
                (category_id, g.user["id"]),
            ).fetchone()
            if category is None:
                flash("Invalid category.")
                return redirect_dashboard()
            result = db.execute(
                f"UPDATE expenses SET category_id = ? WHERE household_id = ? AND id IN ({placeholders})",
                [category_id, g.household_id, *ids],
            )
            db.commit()
            flash(f"Updated {result.rowcount} transactions")
            return redirect_dashboard()

        if action == "set_paid_by":
            paid_by = normalize_paid_by(request.form.get("paid_by", ""))
            if paid_by not in {"", "DK", "YZ"}:
                flash("Invalid Paid by value.")
                return redirect_dashboard()
            result = db.execute(
                f"UPDATE expenses SET paid_by = ? WHERE household_id = ? AND id IN ({placeholders})",
                [paid_by, g.household_id, *ids],
            )
            db.commit()
            flash(f"Updated {result.rowcount} transactions")
            return redirect_dashboard()

        if action == "set_transfer":
            is_transfer = 1 if request.form.get("is_transfer") in {"1", "true", "on", "yes"} else 0
            result = db.execute(
                f"UPDATE expenses SET is_transfer = ? WHERE household_id = ? AND id IN ({placeholders})",
                [is_transfer, g.household_id, *ids],
            )
            db.commit()
            flash(f"Updated {result.rowcount} transactions")
            return redirect_dashboard()

        flash("Unknown bulk action.")
        return redirect_dashboard()

    @app.route("/categories", methods=("GET", "POST"))
    @login_required
    def categories():
        db = get_db()
        if request.method == "POST":
            name = request.form["name"].strip()
            if name:
                try:
                    db.execute(
                        "INSERT INTO categories (user_id, name) VALUES (?, ?)", (g.user["id"], name)
                    )
                    db.commit()
                    flash("Category added.")
                except sqlite3.IntegrityError:
                    flash("Category already exists.")
            else:
                flash("Category name is required.")

        items = db.execute(
            "SELECT * FROM categories WHERE user_id = ? ORDER BY name", (g.user["id"],)
        ).fetchall()
        return render_template("categories.html", categories=items)

    @app.route("/categories/<int:category_id>/edit", methods=("GET", "POST"))
    @login_required
    def edit_category(category_id):
        db = get_db()
        category = db.execute(
            "SELECT * FROM categories WHERE id = ? AND user_id = ?", (category_id, g.user["id"])
        ).fetchone()
        if category is None:
            flash("Category not found.")
            return redirect(url_for("categories"))

        if request.method == "POST":
            name = request.form["name"].strip()
            if not name:
                flash("Category name is required.")
                return render_template("category_form.html", category=category)
            try:
                db.execute(
                    "UPDATE categories SET name = ? WHERE id = ? AND user_id = ?",
                    (name, category_id, g.user["id"]),
                )
                db.commit()
                flash("Category updated.")
                return redirect(url_for("categories"))
            except sqlite3.IntegrityError:
                flash("Category already exists.")

        return render_template("category_form.html", category=category)

    @app.post("/categories/<int:category_id>/delete")
    @login_required
    def delete_category(category_id):
        db = get_db()
        db.execute(
            "UPDATE expenses SET category_id = NULL WHERE category_id = ? AND user_id = ?",
            (category_id, g.user["id"]),
        )
        db.execute("DELETE FROM categories WHERE id = ? AND user_id = ?", (category_id, g.user["id"]))
        db.commit()
        flash("Category deleted.")
        return redirect(url_for("categories"))

    @app.route("/export/csv")
    @login_required
    def export_csv():
        filters = resolve_dashboard_filters(request.args, default_to_current_month=False)
        db = get_db()

        query = """
            SELECT e.date, e.amount, COALESCE(c.name, 'Uncategorized') as category, e.description
            FROM expenses e
            LEFT JOIN categories c ON e.category_id = c.id
            WHERE {filter_sql}
        """
        query = query.format(filter_sql=filters["filter_sql"])
        params = list(filters["params"])

        query += " ORDER BY e.date ASC"
        rows = db.execute(query, tuple(params)).fetchall()

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["date", "amount", "category", "description"])
        for row in rows:
            writer.writerow([row["date"], row["amount"], row["category"], row["description"]])

        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={
                "Content-Disposition": (
                    f"attachment; filename=expenses-{filters['selected_month']}.csv"
                    if filters["selected_month"]
                    else f"attachment; filename=expenses-{filters['start_date'] or 'all'}-{filters['end_date'] or 'all'}.csv"
                )
            },
        )

    @app.post("/import/csv/apply_override")
    @login_required
    def apply_preview_category_override():
        payload = request.get_json(silent=True) or {}
        match_type = (payload.get("match_type") or "vendor").strip()
        match_key = normalize_text(payload.get("match_key", ""))
        category_name = (payload.get("category_name") or "").strip()
        if match_type not in {"vendor", "description"} or not match_key or not category_name:
            return jsonify({"updated_count": 0, "updated_rows": []}), 400

        db = get_db()
        category_rows = db.execute("SELECT id, name FROM categories WHERE user_id = ?", (g.user["id"],)).fetchall()
        available_category_names = [row["name"] for row in category_rows]

        import_id = (payload.get("import_id") or "").strip()
        if not import_id:
            return jsonify({"updated_count": 0, "updated_rows": []}), 400

        records = get_staged_preview_row_records(db, import_id, household_id=g.household_id, user_id=g.user["id"])
        updated_rows = []

        for record in records:
            row = record["row"]
            row_match_key = ""
            if match_type == "vendor":
                row_match_key = row.get("vendor_rule_key") or normalize_text(row.get("vendor_key") or row.get("vendor", ""))
            else:
                row_match_key = row.get("description_rule_key") or extract_pattern(row.get("description", ""))
            if row_match_key != match_key:
                continue

            categorized = categorize_transaction(
                g.user["id"],
                row.get("description", ""),
                row.get("vendor", ""),
                category_name,
                available_category_names,
                db,
            )
            row["category"] = category_name
            row["override_category"] = category_name
            row["confidence"] = categorized["confidence"]
            row["confidence_label"] = confidence_label(categorized["confidence"])
            row["suggested_source"] = categorized["source"]
            update_staged_preview_row(db, record["id"], row)
            updated_rows.append(
                {
                    "row_index": row.get("row_index"),
                    "category": category_name,
                    "confidence": categorized["confidence"],
                    "confidence_label": confidence_label(categorized["confidence"]),
                    "source": categorized["source"],
                }
            )

        if updated_rows:
            db.commit()
        return jsonify({"updated_count": len(updated_rows), "updated_rows": updated_rows})

    @app.post("/import/csv/apply_preview_edits")
    @login_required
    def apply_preview_edits():
        import_id = (request.form.get("import_id") or "").strip()
        if not import_id:
            flash("Preview expired. Please re-upload the file.")
            return redirect(url_for("import_csv"))

        show_all = request.form.get("show_all") == "1"
        confirm_show_all = request.form.get("confirm_show_all") == "1"
        low_confidence = request.form.get("low_confidence") == "1"

        db = get_db()
        records = get_staged_preview_row_records(db, import_id, household_id=g.household_id, user_id=g.user["id"])
        if not records:
            flash("Preview expired. Please re-upload the file.")
            return redirect(url_for("import_csv"))

        category_rows = db.execute(
            "SELECT id, name FROM categories WHERE user_id = ? ORDER BY name", (g.user["id"],)
        ).fetchall()
        category_lookup = {normalize_description(row["name"]): row for row in category_rows}

        for record in records:
            row = record["row"]
            row_index = row.get("row_index")
            if row_index is None:
                continue

            paid_by_override = normalize_paid_by(request.form.get(f"override_paid_by_{row_index}", row.get("paid_by", "")))
            if paid_by_override:
                row["paid_by"] = paid_by_override
            else:
                row.pop("paid_by", None)

            category_override = (request.form.get(f"override_category_{row_index}", "") or "").strip()
            if category_override:
                matched = category_lookup.get(normalize_description(category_override))
                if matched:
                    row["category"] = matched["name"]
                    row["category_name"] = matched["name"]
                    row["override_category"] = matched["name"]
                    row["category_id"] = matched["id"]
                    row["mapped_category_id"] = matched["id"]
                    if row.get("csv_category_name"):
                        row["csv_category_match_status"] = "mapped"
            else:
                row.pop("override_category", None)

            vendor_override = (request.form.get(f"override_vendor_{row_index}", "") or "").strip()
            if vendor_override:
                row["vendor"] = vendor_override

            update_staged_preview_row(db, record["id"], row)

        db.commit()
        save_import_preview_show_all(g.user["id"], import_id, show_all)

        redirect_args = {"import_id": import_id}
        if show_all:
            redirect_args["show_all"] = "1"
        if confirm_show_all:
            redirect_args["confirm_show_all"] = "1"
        if low_confidence:
            redirect_args["low_confidence"] = "1"
        return redirect(url_for("import_csv", **redirect_args))

    @app.post("/import/preview/action")
    @login_required
    def import_preview_action():
        import_id = (request.form.get("import_id") or "").strip()
        action = (request.form.get("action") or "").strip()
        show_all = request.form.get("show_all") == "1"
        confirm_show_all = request.form.get("confirm_show_all") == "1"
        low_confidence = request.form.get("low_confidence") == "1"

        redirect_args = {"import_id": import_id}
        if show_all:
            redirect_args["show_all"] = "1"
        if confirm_show_all:
            redirect_args["confirm_show_all"] = "1"
        if low_confidence:
            redirect_args["low_confidence"] = "1"

        if not import_id:
            flash("Preview expired. Please re-upload the file.")
            return redirect(url_for("import_csv"))

        selected_row_ids = []
        for raw_id in request.form.getlist("selected_row_ids"):
            try:
                parsed_id = int(raw_id)
            except (TypeError, ValueError):
                continue
            if parsed_id > 0:
                selected_row_ids.append(parsed_id)
        selected_row_ids = list(dict.fromkeys(selected_row_ids))
        if not selected_row_ids:
            flash("Please select at least one row first.")
            return redirect(url_for("import_csv", **redirect_args))

        db = get_db()
        placeholders = ",".join(["?"] * len(selected_row_ids))
        staged_rows = db.execute(
            f"""
            SELECT id, row_json FROM import_staging
            WHERE import_id = ? AND household_id = ? AND user_id = ? AND id IN ({placeholders})
            ORDER BY id
            """,
            (import_id, g.household_id, g.user["id"], *selected_row_ids),
        ).fetchall()
        if not staged_rows:
            flash("No selected rows were found in this preview.")
            return redirect(url_for("import_csv", **redirect_args))

        try:
            if action == "apply_paid_by_selected":
                paid_by_value = normalize_paid_by(request.form.get("paid_by_value") or request.form.get("import_default_paid_by") or "")
                if not paid_by_value:
                    flash("Choose a Paid by value before applying.")
                    return redirect(url_for("import_csv", **redirect_args))

                for staged_row in staged_rows:
                    row = json.loads(staged_row["row_json"])
                    row["paid_by"] = paid_by_value
                    update_staged_preview_row(db, staged_row["id"], row)

                db.commit()
                flash(f"Updated Paid by for {len(staged_rows)} rows.")

            elif action == "apply_category_selected":
                category_id_raw = (request.form.get("category_id") or "").strip()
                selected_category_name = ""
                selected_category_id = None

                if category_id_raw:
                    try:
                        selected_category_id = int(category_id_raw)
                    except ValueError:
                        flash("Invalid category selection.")
                        return redirect(url_for("import_csv", **redirect_args))

                    category = db.execute(
                        "SELECT id, name FROM categories WHERE id = ? AND user_id = ?",
                        (selected_category_id, g.user["id"]),
                    ).fetchone()
                    if not category:
                        flash("Selected category was not found.")
                        return redirect(url_for("import_csv", **redirect_args))
                    selected_category_name = category["name"]

                for staged_row in staged_rows:
                    row = json.loads(staged_row["row_json"])
                    row["category"] = selected_category_name
                    if selected_category_name:
                        row["override_category"] = selected_category_name
                        row["category_name"] = selected_category_name
                        row["category_id"] = selected_category_id
                        row["mapped_category_id"] = selected_category_id
                        if row.get("csv_category_name"):
                            row["csv_category_match_status"] = "mapped"
                    else:
                        row.pop("override_category", None)
                        row.pop("category_name", None)
                        row.pop("category_id", None)
                        row.pop("mapped_category_id", None)
                    update_staged_preview_row(db, staged_row["id"], row)

                db.commit()
                flash(f"Updated Category for {len(staged_rows)} rows.")
            else:
                flash("Unknown bulk action.")
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            db.rollback()
            flash(f"Could not update selected rows: {exc}")

        return redirect(url_for("import_csv", **redirect_args))

    @app.route("/import/csv", methods=("GET", "POST"))
    @login_required
    def import_csv():
        default_mapping = {"date": "", "description": "", "vendor": "", "amount": "", "debit": "", "credit": "", "category": "", "paid_by": ""}
        saved_payload = get_saved_csv_mapping_for_user(g.user["id"])
        saved_mapping = mapping_from_payload(saved_payload)

        if request.method == "POST":
            action = request.form.get("action", "preview")
            import_default_paid_by = normalize_paid_by(request.form.get("import_default_paid_by", ""))

            if action == "apply_all_mappings":
                import_id = (request.form.get("import_id") or "").strip()
                if not import_id:
                    flash("Preview expired. Please re-upload the file.")
                    return redirect(url_for("import_csv"))

                db = get_db()
                records = get_staged_preview_row_records(db, import_id, household_id=g.household_id, user_id=g.user["id"])
                if not records:
                    flash("Preview expired. Please re-upload the file.")
                    return redirect(url_for("import_csv"))

                category_rows = db.execute(
                    "SELECT id, name FROM categories WHERE user_id = ? ORDER BY name", (g.user["id"],)
                ).fetchall()
                categories_by_id = {row["id"]: row["name"] for row in category_rows}
                mapping_by_category_name = {}
                for key, value in request.form.items():
                    if not key.startswith("map_unknown::"):
                        continue
                    csv_name = normalize_csv_category_name(key.split("::", 1)[1])
                    apply_all = request.form.get(f"apply_unknown_all::{csv_name}") == "1"
                    if not apply_all:
                        continue
                    try:
                        category_id = int((value or "").strip())
                    except (TypeError, ValueError):
                        continue
                    if category_id not in categories_by_id:
                        continue
                    mapping_by_category_name[csv_name] = {"id": category_id, "name": categories_by_id[category_id]}

                if not mapping_by_category_name:
                    flash("No category mappings were selected.")
                    return redirect(url_for("import_csv", import_id=import_id))

                updated_rows = 0
                for record in records:
                    row = record["row"]
                    csv_name = normalize_csv_category_name(row.get("csv_category_name", ""))
                    mapped = mapping_by_category_name.get(csv_name)
                    if not mapped:
                        continue
                    row["category"] = mapped["name"]
                    row["category_name"] = mapped["name"]
                    row["category_id"] = mapped["id"]
                    row["mapped_category_id"] = mapped["id"]
                    row["override_category"] = mapped["name"]
                    row["csv_category_name"] = csv_name
                    row["csv_category_match_status"] = "mapped"
                    update_staged_preview_row(db, record["id"], row)
                    updated_rows += 1

                db.commit()
                flash(f"Applied mappings to {updated_rows} row(s).")
                return redirect(url_for("import_csv", import_id=import_id))

            if action == "confirm":
                import_id = request.form.get("import_id", "") or request.form.get("preview_id", "")
                db = get_db()
                parsed_rows = get_staged_preview_rows(db, import_id, household_id=g.household_id, user_id=g.user["id"])
                if not parsed_rows:
                    flash("Preview expired. Please re-upload the file.")
                    return redirect(url_for("import_csv"))
                imported_count = 0
                preview_count = len(parsed_rows)

                category_rows = db.execute(
                    "SELECT id, name FROM categories WHERE user_id = ?", (g.user["id"],)
                ).fetchall()
                categories_by_id = {row["id"]: row["name"] for row in category_rows}
                category_lookup = {normalize_description(row["name"]): row["id"] for row in category_rows}
                available_category_names = [row["name"] for row in category_rows]
                learned_rule_keys = set()

                raw_default_paid_by = request.form.get("import_default_paid_by")
                default_paid_by = normalize_paid_by(raw_default_paid_by or "")
                has_paid_by_overrides = any(key.startswith("override_paid_by_") for key in request.form.keys())
                if raw_default_paid_by is None and not has_paid_by_overrides:
                    default_paid_by = "DK"
                skipped_count = 0
                uncategorized_unmapped_count = 0
                for index, row in enumerate(parsed_rows):
                    row_index = row.get("row_index", index)
                    override = request.form.get(f"override_category_{row_index}", "") or row.get("override_category", "")
                    paid_by_override = normalize_paid_by(
                        request.form.get(f"override_paid_by_{row_index}", "") or row.get("paid_by", "") or default_paid_by
                    )
                    vendor_override = request.form.get(f"override_vendor_{row_index}", "").strip()
                    if vendor_override:
                        row["vendor"] = vendor_override
                    elif not row.get("vendor"):
                        row["vendor"] = derive_vendor(row.get("description", ""))
                    if override:
                        row["category"] = override
                    row["paid_by"] = paid_by_override

                    if row.get("amount", 0) < 0 and not paid_by_override:
                        flash("Cannot import spending rows with missing Paid by. Fill missing values and confirm again.")
                        return redirect(url_for("import_csv"))

                    candidates = db.execute(
                        """
                        SELECT description FROM expenses
                        WHERE household_id = ? AND date = ? AND amount = ?
                        """,
                        (g.household_id, row["date"], row["amount"]),
                    ).fetchall()
                    if any(
                        normalize_description(item["description"]) == row["normalized_description"]
                        for item in candidates
                    ):
                        skipped_count += 1
                        continue

                    category_id = None
                    assigned_category = ""
                    categorized = {"confidence": 25, "source": "unknown"}

                    if override:
                        assigned_category = pick_existing_category(override, available_category_names)
                        if assigned_category:
                            category_id = category_lookup.get(normalize_description(assigned_category))
                            categorized = {"confidence": 100, "source": "import_override"}
                    if not category_id:
                        staged_category_id = row.get("mapped_category_id") if row.get("mapped_category_id") is not None else row.get("category_id")
                        try:
                            staged_category_id = int(staged_category_id) if staged_category_id not in (None, "") else None
                        except (TypeError, ValueError):
                            staged_category_id = None
                        if staged_category_id and staged_category_id in categories_by_id:
                            category_id = staged_category_id
                            assigned_category = categories_by_id[staged_category_id]
                            categorized = {"confidence": 100, "source": "csv_mapped"}

                    if not category_id and row.get("csv_category_name"):
                        categorized = {"confidence": 25, "source": "unknown_csv_category"}
                        uncategorized_unmapped_count += 1
                    elif not category_id:
                        categorized_full = categorize_transaction(
                            g.user["id"],
                            row.get("description", ""),
                            row.get("vendor", ""),
                            row.get("category", ""),
                            available_category_names,
                            db,
                        )
                        assigned_category = categorized_full["category"]
                        categorized = categorized_full
                        if assigned_category:
                            category_id = category_lookup.get(normalize_description(assigned_category))

                    is_personal = assigned_category == "Personal"
                    is_transfer = is_transfer_transaction(row.get("description", ""), assigned_category)

                    db.execute(
                        """
                        INSERT INTO expenses (
                            user_id, household_id, date, amount, category_id, description, vendor, paid_by,
                            is_transfer, is_personal, category_confidence, category_source, tags
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            g.user["id"],
                            g.household_id,
                            row["date"],
                            row["amount"],
                            category_id,
                            row["description"],
                            row.get("vendor", "") or derive_vendor(row.get("description", "")),
                            row.get("paid_by", ""),
                            1 if is_transfer else 0,
                            1 if is_personal else 0,
                            categorized["confidence"],
                            categorized["source"],
                            json.dumps(row.get("tags") or derive_tags(row.get("description", ""))),
                        ),
                    )
                    if override and category_id and normalize_description(override) != normalize_description(
                        row.get("auto_category", "")
                    ):
                        vendor_pattern = extract_pattern(row.get("vendor", ""), max_words=4)
                        description_pattern = extract_pattern(row.get("description", ""))
                        rule_identity = (vendor_pattern, description_pattern, category_id)
                        if rule_identity not in learned_rule_keys:
                            learn_rule(g.user["id"], row.get("description", ""), row.get("vendor", ""), category_id, "import_override")
                            learned_rule_keys.add(rule_identity)
                    imported_count += 1

                mapping = {
                    "date": request.form.get("map_date", ""),
                    "description": request.form.get("map_description", ""),
                    "vendor": request.form.get("map_vendor", ""),
                    "amount": request.form.get("map_amount", ""),
                    "debit": request.form.get("map_debit", ""),
                    "credit": request.form.get("map_credit", ""),
                    "category": request.form.get("map_category", ""),
                    "paid_by": request.form.get("map_paid_by", ""),
                }
                detected_format = request.form.get("detected_format", "manual")
                has_header = request.form.get("has_header", "0") == "1"
                save_csv_mapping_for_user(g.user["id"], mapping, has_header, detected_format, file_signature=request.form.get("file_signature", ""))

                log_audit("import", details={"imported_count": imported_count, "preview_count": preview_count, "skipped_count": skipped_count}, db=db)
                db.execute("DELETE FROM import_staging WHERE import_id = ?", (import_id,))
                db.commit()
                save_import_preview_state(g.user["id"], [], preview_id="")
                clear_import_preview_show_all(g.user["id"], import_id)
                flash(f"Imported {imported_count} transaction(s).")
                flash(f"Preview rows: {preview_count} · inserted: {imported_count} · skipped: {skipped_count}.")
                if uncategorized_unmapped_count:
                    flash(f"{uncategorized_unmapped_count} rows imported as Uncategorized because CSV categories were not mapped.")
                return redirect(url_for("dashboard"))

            file = request.files.get("csv_file")
            if file is None or not file.filename:
                flash("Please choose a CSV file.")
                return render_template("import_csv.html", preview_rows=[], mapping=saved_mapping, columns=[], categories=[], preview_id="", detected_mode=None, unknown_category_rows=[])

            file_bytes = file.read()
            content = decode_csv_bytes(file_bytes)
            if content is None:
                flash("Could not read file encoding. Please re-save as CSV UTF-8.")
                return render_template("import_csv.html", preview_rows=[], mapping=saved_mapping, columns=[], categories=[], preview_id="", detected_mode=None, unknown_category_rows=[])

            rows = list(csv.reader(io.StringIO(content)))
            rows = [row for row in rows if any(cell.strip() for cell in row)]
            if not rows:
                flash("CSV is empty.")
                return render_template("import_csv.html", preview_rows=[], mapping=saved_mapping, columns=[], categories=[], preview_id="", detected_mode=None, unknown_category_rows=[])

            has_header, inferred_mapping, header_row_index = detect_header_and_mapping(rows)
            detected_format = "header" if has_header else "headerless"
            columns = rows[header_row_index] if has_header else [f"Column {i + 1}" for i in range(len(rows[0]))]
            file_signature = build_file_signature(file.filename, rows[header_row_index] if has_header else [])
            saved_payload = get_saved_csv_mapping_for_user(g.user["id"], file_signature=file_signature)
            saved_mapping = mapping_from_payload(saved_payload)

            amex_mapping = detect_amex_headered_mapping(rows, header_row_index) if has_header else None
            if amex_mapping:
                mapping = amex_mapping
                detected_format = "headered"
            else:
                explicit_mapping = {
                    "date": request.form.get("map_date", ""),
                    "description": request.form.get("map_description", ""),
                    "vendor": request.form.get("map_vendor", ""),
                    "amount": request.form.get("map_amount", ""),
                    "debit": request.form.get("map_debit", ""),
                    "credit": request.form.get("map_credit", ""),
                    "category": request.form.get("map_category", ""),
                    "paid_by": request.form.get("map_paid_by", ""),
                }
                auto_mapping = should_auto_map_cibc_headerless(rows, explicit_mapping, detected_format) if header_row_index == 0 else None
                if auto_mapping:
                    mapping = auto_mapping
                    detected_format = "cibc_headerless"
                else:
                    auto_detected_mapping = detect_cibc_headerless_mapping(rows) if not has_header and header_row_index == 0 else None
                    if auto_detected_mapping:
                        inferred_mapping = auto_detected_mapping
                        detected_format = "cibc_headerless"

                    mapping = {
                        "date": request.form.get("map_date") if request.form.get("map_date") is not None else inferred_mapping["date"],
                        "description": request.form.get("map_description") if request.form.get("map_description") is not None else inferred_mapping["description"],
                        "vendor": request.form.get("map_vendor") if request.form.get("map_vendor") is not None else inferred_mapping["vendor"],
                        "amount": request.form.get("map_amount") if request.form.get("map_amount") is not None else inferred_mapping["amount"],
                        "debit": request.form.get("map_debit") if request.form.get("map_debit") is not None else inferred_mapping["debit"],
                        "credit": request.form.get("map_credit") if request.form.get("map_credit") is not None else inferred_mapping["credit"],
                        "category": request.form.get("map_category") if request.form.get("map_category") is not None else inferred_mapping["category"],
                        "paid_by": request.form.get("map_paid_by") if request.form.get("map_paid_by") is not None else inferred_mapping["paid_by"],
                    }

                    for field in mapping:
                        if mapping[field] == "" and saved_mapping.get(field, "") != "":
                            mapping[field] = saved_mapping[field]

            data_rows = rows[(header_row_index + 1):] if has_header else rows
            bank_type = detect_bank_type(rows[header_row_index] if has_header else [])
            parsed_rows, skipped_rows = parse_csv_transactions(data_rows, mapping, g.user["id"], bank_type=bank_type)
            for row in parsed_rows:
                if not row.get("paid_by"):
                    row["paid_by"] = import_default_paid_by
            db = get_db()
            cleanup_expired_import_staging(db)
            category_rows = db.execute(
                "SELECT id, name FROM categories WHERE user_id = ? ORDER BY name", (g.user["id"],)
            ).fetchall()
            category_lookup = {normalize_description(row["name"]): row for row in category_rows}
            available_category_names = [row["name"] for row in category_rows]
            for row in parsed_rows:
                categorized = categorize_transaction(
                    g.user["id"], row.get("description", ""), row.get("vendor", ""), row.get("category", ""), available_category_names, db
                )
                row["auto_category"] = categorized["category"]
                row["suggested_source"] = categorized["source"]
                row["confidence"] = categorized["confidence"]
                row["confidence_label"] = confidence_label(categorized["confidence"])

                csv_category_name, csv_matched_category_id, csv_match_status = resolve_csv_category_mapping(
                    row.get("csv_category_name", ""), category_lookup
                )
                row["csv_category_name"] = csv_category_name
                row["csv_category_match_status"] = csv_match_status

                if csv_match_status == "matched":
                    matched_name = category_lookup[normalize_description(csv_category_name)]["name"]
                    row["category"] = matched_name
                    row["category_name"] = matched_name
                    row["category_id"] = csv_matched_category_id
                    row["mapped_category_id"] = csv_matched_category_id
                elif csv_match_status == "unknown":
                    row["category"] = ""
                    row["category_name"] = ""
                    row["category_id"] = None
                    row["mapped_category_id"] = None
                    row["suggested_source"] = "unknown_csv_category"
                    row["confidence"] = 25
                    row["confidence_label"] = confidence_label(25)
                else:
                    resolved_category = categorized["category"]
                    row["category"] = resolved_category
                    row["category_name"] = resolved_category
                    row["category_id"] = None
                    row["mapped_category_id"] = None

            import_id = str(uuid.uuid4())
            show_all = request.form.get("show_all_rows") == "1"
            requires_show_all_confirmation = len(parsed_rows) > IMPORT_PREVIEW_SHOW_ALL_WARNING_THRESHOLD
            show_all_confirmed = request.form.get("confirm_show_all") == "1"
            if requires_show_all_confirmation and show_all and not show_all_confirmed:
                show_all = False
                flash("This preview has more than 500 rows. Check 'Confirm show all rows' to render all rows.")
            save_import_preview_show_all(g.user["id"], import_id, show_all)
            stage_import_preview_rows(db, import_id, parsed_rows, household_id=g.household_id, user_id=g.user["id"])
            db.commit()
            save_import_preview_state(g.user["id"], [], preview_id=import_id)
            preview_rows, displayed_rows_count, total_rows_count = preview_rows_for_display(parsed_rows, show_all=show_all)
            unknown_category_rows = build_unknown_category_rows(parsed_rows)
            def mapped_column_name(field):
                value = mapping.get(field, "")
                if value == "":
                    return None
                try:
                    idx = int(value)
                except (TypeError, ValueError):
                    return value
                return columns[idx] if 0 <= idx < len(columns) else None

            auto_mapped_fields = {
                "date": mapped_column_name("date"),
                "description": mapped_column_name("description"),
                "amount": mapped_column_name("amount"),
                "vendor": mapped_column_name("vendor"),
                "debit": mapped_column_name("debit"),
                "credit": mapped_column_name("credit"),
                "paid_by": mapped_column_name("paid_by"),
            }

            save_csv_mapping_for_user(g.user["id"], mapping, has_header, detected_format, file_signature=file_signature)

            return render_template(
                "import_csv.html",
                preview_rows=preview_rows,
                mapping=mapping,
                columns=columns,
                categories=category_rows,
                preview_id=import_id,
                detected_mode="headered" if has_header else "headerless",
                has_header=has_header,
                detected_format=detected_format,
                auto_mapping_applied=(detected_format == "cibc_headerless"),
                header_row_index=header_row_index if has_header else None,
                skipped_rows=skipped_rows,
                auto_mapped_fields=auto_mapped_fields,
                file_signature=file_signature,
                import_default_paid_by=import_default_paid_by,
                show_all_rows=show_all,
                displayed_rows_count=displayed_rows_count,
                total_rows_count=total_rows_count,
                show_all_warning_threshold=IMPORT_PREVIEW_SHOW_ALL_WARNING_THRESHOLD,
                requires_show_all_confirmation=requires_show_all_confirmation,
                low_confidence_filter=False,
                unknown_category_rows=unknown_category_rows,
            )

        import_id = (request.args.get("import_id") or "").strip()
        if import_id:
            db = get_db()
            parsed_rows = get_staged_preview_rows(db, import_id, household_id=g.household_id, user_id=g.user["id"])
            if not parsed_rows:
                flash("Preview expired. Please re-upload the file.")
                return redirect(url_for("import_csv"))

            show_all_values = request.args.getlist("show_all")
            show_all_param = show_all_values[-1] if show_all_values else None
            show_all = get_import_preview_show_all(g.user["id"], import_id)
            low_confidence_filter = request.args.get("low_confidence") == "1"
            if show_all_param is not None:
                show_all = show_all_param == "1"

            requires_show_all_confirmation = len(parsed_rows) > IMPORT_PREVIEW_SHOW_ALL_WARNING_THRESHOLD
            confirm_show_all = request.args.get("confirm_show_all") == "1"
            if requires_show_all_confirmation and show_all and not confirm_show_all:
                show_all = False
                flash("This preview has more than 500 rows. Check 'Confirm show all rows' to render all rows.")

            save_import_preview_show_all(g.user["id"], import_id, show_all)
            preview_rows, displayed_rows_count, total_rows_count = preview_rows_for_display(parsed_rows, show_all=show_all)
            unknown_category_rows = build_unknown_category_rows(parsed_rows)
            category_rows = db.execute(
                "SELECT id, name FROM categories WHERE user_id = ? ORDER BY name", (g.user["id"],)
            ).fetchall()

            return render_template(
                "import_csv.html",
                preview_rows=preview_rows,
                mapping=saved_mapping or default_mapping,
                columns=placeholder_columns_from_mapping(saved_mapping),
                categories=category_rows,
                preview_id=import_id,
                detected_mode="staged",
                has_header=saved_payload.get("has_header", False),
                detected_format=saved_payload.get("detected_format", ""),
                auto_mapping_applied=False,
                header_row_index=None,
                skipped_rows=0,
                auto_mapped_fields={},
                file_signature=saved_payload.get("file_signature", ""),
                import_default_paid_by="",
                show_all_rows=show_all,
                displayed_rows_count=displayed_rows_count,
                total_rows_count=total_rows_count,
                show_all_warning_threshold=IMPORT_PREVIEW_SHOW_ALL_WARNING_THRESHOLD,
                requires_show_all_confirmation=requires_show_all_confirmation,
                low_confidence_filter=low_confidence_filter,
                unknown_category_rows=unknown_category_rows,
            )

        return render_template(
            "import_csv.html",
            preview_rows=[],
            mapping=saved_mapping or default_mapping,
            columns=placeholder_columns_from_mapping(saved_mapping),
            categories=[],
            preview_id="",
            detected_mode=None,
            has_header=saved_payload.get("has_header", False),
            detected_format=saved_payload.get("detected_format", ""),
            auto_mapping_applied=False,
            header_row_index=None,
            skipped_rows=0,
            auto_mapped_fields={},
            file_signature=saved_payload.get("file_signature", ""),
            import_default_paid_by="",
            show_all_rows=False,
            displayed_rows_count=0,
            total_rows_count=0,
            show_all_warning_threshold=IMPORT_PREVIEW_SHOW_ALL_WARNING_THRESHOLD,
            requires_show_all_confirmation=False,
            low_confidence_filter=False,
            unknown_category_rows=[],
        )

    @app.route("/rules")
    @login_required
    def rules():
        db = get_db()
        rules = db.execute(
            """
            SELECT cr.id, cr.key_type, cr.pattern, cr.hits, cr.last_used_at, cr.source,
                   COALESCE(cr.enabled, cr.is_enabled, 1) as is_enabled,
                   COALESCE(cr.category, c.name) as category
            FROM category_rules cr
            LEFT JOIN categories c ON c.id = cr.category_id
            WHERE cr.user_id = ?
            ORDER BY cr.priority ASC, cr.hits DESC, cr.id DESC
            """,
            (g.user["id"],),
        ).fetchall()
        categories = db.execute(
            "SELECT id, name FROM categories WHERE user_id = ? ORDER BY name", (g.user["id"],)
        ).fetchall()
        return render_template("rules.html", rules=rules, categories=categories)

    @app.post("/rules/<int:rule_id>/delete")
    @login_required
    def delete_rule(rule_id):
        db = get_db()
        db.execute("DELETE FROM category_rules WHERE id = ? AND user_id = ?", (rule_id, g.user["id"]))
        db.commit()
        flash("Rule deleted.")
        return redirect(url_for("rules"))

    @app.post("/rules/<int:rule_id>/update")
    @login_required
    def update_rule(rule_id):
        db = get_db()
        category_id = request.form.get("category_id")
        is_enabled = 1 if request.form.get("is_enabled") == "1" else 0
        category = db.execute(
            "SELECT id, name FROM categories WHERE id = ? AND user_id = ?",
            (category_id, g.user["id"]),
        ).fetchone()
        if category is None:
            flash("Invalid category.")
            return redirect(url_for("rules"))

        rule_columns = {row["name"] for row in db.execute("PRAGMA table_info(category_rules)").fetchall()}
        set_parts = ["category = ?", "enabled = ?"]
        params = [category["name"], is_enabled]
        if "category_id" in rule_columns:
            set_parts.append("category_id = ?")
            params.append(category["id"])
        if "is_enabled" in rule_columns:
            set_parts.append("is_enabled = ?")
            params.append(is_enabled)

        params.extend([rule_id, g.user["id"]])
        db.execute(
            f"UPDATE category_rules SET {', '.join(set_parts)} WHERE id = ? AND user_id = ?",
            params,
        )
        db.commit()
        flash("Rule updated.")
        return redirect(url_for("rules"))


    @app.route("/dev/reset-db")
    def dev_reset_db():
        dev_enabled = app.debug or os.environ.get("ENABLE_DEV_DB_RESET") == "1"
        if not dev_enabled:
            return "DEV ONLY: database reset is disabled.", 404

        db = g.pop("db", None)
        if db is not None:
            db.close()

        db_path = app.config["DATABASE"]
        if os.path.exists(db_path):
            os.remove(db_path)

        init_db()
        flash("DEV ONLY: database reset complete.")
        return redirect(url_for("register"))

    with app.app_context():
        try:
            init_db()
        except DatabaseInitError:
            pass

    app.get_db = get_db
    app.init_db = init_db
    return app
