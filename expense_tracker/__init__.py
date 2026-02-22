import csv
import hashlib
import io
import os
import sqlite3
import json
import re
import unicodedata
from datetime import date, datetime
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
    mapping = {"date": "", "description": "", "vendor": "", "amount": "", "debit": "", "credit": "", "category": ""}
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
        return {"date": "0", "description": "1", "debit": "2", "credit": "3", "amount": "", "vendor": "", "category": ""}

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
        return {"date": "", "description": "", "vendor": "", "amount": "", "debit": "", "credit": "", "category": ""}
    return {
        "date": payload.get("date_col", ""),
        "description": payload.get("desc_col", ""),
        "vendor": payload.get("vendor_col", ""),
        "amount": payload.get("amount_col", ""),
        "debit": payload.get("debit_col", ""),
        "credit": payload.get("credit_col", ""),
        "category": payload.get("category_col", ""),
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




def get_import_preview_state(user_id):
    previews = session.get("import_preview_by_user") or {}
    return previews.get(str(user_id)) or {"rows": []}


def save_import_preview_state(user_id, rows):
    previews = session.get("import_preview_by_user") or {}
    previews[str(user_id)] = {"rows": rows}
    session["import_preview_by_user"] = previews
    session.modified = True

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

    if any((mapping.get(field) or "").strip() for field in ["date", "description", "amount", "debit", "credit", "vendor", "category"]):
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
        return {"date": "0", "description": "1", "debit": "2", "credit": "3", "amount": "", "vendor": "", "category": ""}

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
        normalized_description = normalize_description(row_description)
        row_vendor = get_value("vendor") or derive_vendor(row_description)

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
                "normalized_description": normalized_description,
                "category": infer_category(row_description, row_category),
                "tags": derive_tags(row_description),
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

    @app.teardown_appcontext
    def close_db(_=None):
        db = g.pop("db", None)
        if db is not None:
            db.close()

    def get_db():
        if "db" not in g:
            g.db = sqlite3.connect(app.config["DATABASE"])
            g.db.row_factory = sqlite3.Row
        return g.db

    def init_db():
        db = get_db()
        with app.open_resource("schema.sql") as f:
            db.executescript(f.read().decode("utf8"))
        db.commit()

    @app.cli.command("init-db")
    def init_db_command():
        init_db()
        print("Initialized the database.")

    @app.route("/init-db")
    def init_db_route():
        init_db()
        return "Database initialized."

    def login_required(view):
        @wraps(view)
        def wrapped_view(**kwargs):
            if g.user is None:
                return redirect(url_for("login"))
            return view(**kwargs)

        return wrapped_view

    @app.before_request
    def load_logged_in_user():
        user_id = session.get("user_id")
        if user_id is None:
            g.user = None
        else:
            g.user = get_db().execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()

    def ensure_schema_updates():
        db = get_db()
        columns = {row["name"] for row in db.execute("PRAGMA table_info(expenses)").fetchall()}
        if "is_transfer" not in columns:
            db.execute("ALTER TABLE expenses ADD COLUMN is_transfer INTEGER NOT NULL DEFAULT 0")
        if "is_personal" not in columns:
            db.execute("ALTER TABLE expenses ADD COLUMN is_personal INTEGER NOT NULL DEFAULT 0")
        if "tags" not in columns:
            db.execute("ALTER TABLE expenses ADD COLUMN tags TEXT")
        if "vendor" not in columns:
            db.execute("ALTER TABLE expenses ADD COLUMN vendor TEXT")
        if "paid_by" not in columns:
            db.execute("ALTER TABLE expenses ADD COLUMN paid_by TEXT NOT NULL DEFAULT 'DK'")
        if "category_confidence" not in columns:
            db.execute("ALTER TABLE expenses ADD COLUMN category_confidence INTEGER")
        if "category_source" not in columns:
            db.execute("ALTER TABLE expenses ADD COLUMN category_source TEXT")

        db.execute(
            """
            CREATE TABLE IF NOT EXISTS category_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                key_type TEXT NOT NULL DEFAULT 'description',
                pattern TEXT NOT NULL,
                category_id INTEGER NOT NULL,
                priority INTEGER NOT NULL DEFAULT 100,
                hits INTEGER NOT NULL DEFAULT 0,
                source TEXT NOT NULL,
                is_enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                last_used_at TEXT,
                UNIQUE(user_id, key_type, pattern),
                FOREIGN KEY (user_id) REFERENCES users (id),
                FOREIGN KEY (category_id) REFERENCES categories (id)
            )
            """
        )
        rule_columns = {row["name"] for row in db.execute("PRAGMA table_info(category_rules)").fetchall()}
        if "key_type" not in rule_columns:
            db.execute("ALTER TABLE category_rules RENAME TO category_rules_old")
            db.execute(
                """
                CREATE TABLE category_rules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    key_type TEXT NOT NULL DEFAULT 'description',
                    pattern TEXT NOT NULL,
                    category_id INTEGER NOT NULL,
                    priority INTEGER NOT NULL DEFAULT 100,
                    hits INTEGER NOT NULL DEFAULT 0,
                    source TEXT NOT NULL,
                    is_enabled INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    last_used_at TEXT,
                    UNIQUE(user_id, key_type, pattern),
                    FOREIGN KEY (user_id) REFERENCES users (id),
                    FOREIGN KEY (category_id) REFERENCES categories (id)
                )
                """
            )
            db.execute(
                """
                INSERT INTO category_rules (
                    id, user_id, key_type, pattern, category_id, priority, hits, source, is_enabled, created_at, updated_at, last_used_at
                )
                SELECT id, user_id, 'description', pattern, category_id, priority, hits, source, COALESCE(is_enabled, 1), created_at, updated_at, last_used_at
                FROM category_rules_old
                """
            )
            db.execute("DROP TABLE category_rules_old")
        elif "is_enabled" not in rule_columns:
            db.execute("ALTER TABLE category_rules ADD COLUMN is_enabled INTEGER NOT NULL DEFAULT 1")
        db.commit()

    def resolve_learned_category(user_id, key_type, pattern, available_categories, db):
        if not app.config.get("ENABLE_LEARNING_RULES", True):
            return ""
        if not pattern:
            return ""

        rule = db.execute(
            """
            SELECT cr.id, c.name as category_name
            FROM category_rules cr
            JOIN categories c ON c.id = cr.category_id
            WHERE cr.user_id = ? AND cr.key_type = ? AND cr.pattern = ? AND cr.is_enabled = 1
            ORDER BY cr.priority ASC, cr.hits DESC, cr.updated_at DESC
            LIMIT 1
            """,
            (user_id, key_type, pattern),
        ).fetchone()

        if rule is None:
            rule = db.execute(
                """
                SELECT cr.id, c.name as category_name
                FROM category_rules cr
                JOIN categories c ON c.id = cr.category_id
                WHERE cr.user_id = ? AND cr.key_type = ? AND ? LIKE cr.pattern || '%' AND cr.is_enabled = 1
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

        existing = db.execute(
            "SELECT id FROM category_rules WHERE user_id = ? AND key_type = ? AND pattern = ?",
            (user_id, key_type, pattern),
        ).fetchone()
        if existing:
            db.execute(
                """
                UPDATE category_rules
                SET category_id = ?, source = ?, updated_at = CURRENT_TIMESTAMP, is_enabled = 1
                WHERE id = ?
                """,
                (category_id, source, existing["id"]),
            )
        else:
            db.execute(
                """
                INSERT INTO category_rules (user_id, key_type, pattern, category_id, source)
                VALUES (?, ?, ?, ?, ?)
                """,
                (user_id, key_type, pattern, category_id, source),
            )
        db.commit()

    def ensure_default_categories(user_id):
        db = get_db()
        ensure_schema_updates()

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
                        "INSERT INTO users (username, password) VALUES (?, ?)",
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

            if user is None or not check_password_hash(user["password"], password):
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

    @app.route("/dashboard")
    @login_required
    def dashboard():
        selected_month = request.args.get("month") or date.today().strftime("%Y-%m")
        month_like = f"{selected_month}%"
        db = get_db()

        expenses = db.execute(
            """
            SELECT e.id, e.date, e.amount, e.description, c.name as category,
                   e.category_confidence, e.category_source
            FROM expenses e
            LEFT JOIN categories c ON e.category_id = c.id
            WHERE e.user_id = ? AND e.date LIKE ?
            ORDER BY e.date DESC, e.id DESC
            """,
            (g.user["id"], month_like),
        ).fetchall()

        summary = db.execute(
            """
            SELECT COALESCE(c.name, 'Uncategorized') as category, ROUND(SUM(e.amount), 2) as total
            FROM expenses e
            LEFT JOIN categories c ON e.category_id = c.id
            WHERE e.user_id = ? AND e.date LIKE ? AND e.is_transfer = 0 AND e.is_personal = 0
            GROUP BY COALESCE(c.name, 'Uncategorized')
            ORDER BY total DESC
            """,
            (g.user["id"], month_like),
        ).fetchall()

        total = db.execute(
            """
            SELECT ROUND(COALESCE(SUM(amount), 0), 2) as total
            FROM expenses
            WHERE user_id = ? AND date LIKE ? AND is_transfer = 0
            """,
            (g.user["id"], month_like),
        ).fetchone()["total"]

        shared_total = db.execute(
            """
            SELECT ROUND(COALESCE(SUM(amount), 0), 2) as total
            FROM expenses
            WHERE user_id = ? AND date LIKE ? AND is_transfer = 0 AND is_personal = 0
            """,
            (g.user["id"], month_like),
        ).fetchone()["total"]

        pet_placeholders = ", ".join(["?"] * len(PET_CATEGORIES))
        settlement_row = db.execute(
            f"""
            SELECT
                ROUND(COALESCE(SUM(CASE
                    WHEN e.amount < 0
                         AND e.paid_by = 'DK'
                         AND COALESCE(c.name, '') IN ({pet_placeholders})
                    THEN ABS(e.amount) ELSE 0 END), 0), 2) as pet_paid_by_dk,
                ROUND(COALESCE(SUM(CASE
                    WHEN e.amount < 0
                         AND e.paid_by = 'YZ'
                         AND COALESCE(c.name, '') IN ({pet_placeholders})
                    THEN ABS(e.amount) ELSE 0 END), 0), 2) as pet_paid_by_yz,
                ROUND(COALESCE(SUM(CASE
                    WHEN e.amount < 0
                         AND e.paid_by = 'DK'
                         AND e.is_transfer = 0
                         AND COALESCE(c.name, '') <> 'Personal'
                         AND COALESCE(c.name, '') <> 'Credit Card Payments'
                         AND COALESCE(c.name, '') NOT IN ({pet_placeholders})
                    THEN ABS(e.amount) ELSE 0 END), 0), 2) as dk_shared,
                ROUND(COALESCE(SUM(CASE
                    WHEN e.amount < 0
                         AND e.paid_by = 'YZ'
                         AND e.is_transfer = 0
                         AND COALESCE(c.name, '') <> 'Personal'
                         AND COALESCE(c.name, '') <> 'Credit Card Payments'
                         AND COALESCE(c.name, '') NOT IN ({pet_placeholders})
                    THEN ABS(e.amount) ELSE 0 END), 0), 2) as yz_shared,
                SUM(CASE
                    WHEN e.amount < 0
                         AND (
                            (e.is_transfer = 0
                             AND COALESCE(c.name, '') <> 'Personal'
                             AND COALESCE(c.name, '') <> 'Credit Card Payments')
                            OR COALESCE(c.name, '') IN ({pet_placeholders})
                         )
                         AND (e.paid_by IS NULL OR TRIM(e.paid_by) = '')
                    THEN 1 ELSE 0 END) as missing_paid_by_count
            FROM expenses e
            LEFT JOIN categories c ON e.category_id = c.id
            WHERE e.user_id = ? AND e.date LIKE ?
            """,
            PET_CATEGORIES * 5 + [g.user["id"], month_like],
        ).fetchone()

        dk_shared = settlement_row["dk_shared"] or 0
        yz_shared = settlement_row["yz_shared"] or 0
        shared_total_abs = round(dk_shared + yz_shared, 2)
        each_share = round(shared_total_abs / 2, 2)
        pet_paid_by_dk = settlement_row["pet_paid_by_dk"] or 0
        pet_paid_by_yz = settlement_row["pet_paid_by_yz"] or 0
        missing_paid_by_count = settlement_row["missing_paid_by_count"] or 0

        shared_receiver = "DK" if dk_shared > each_share else "YZ"
        shared_payer = "YZ" if shared_receiver == "DK" else "DK"
        shared_owes = round(abs(dk_shared - each_share), 2)

        obligations = []
        if pet_paid_by_dk > 0:
            obligations.append({"from": "YZ", "to": "DK", "amount": pet_paid_by_dk})
        if shared_owes > 0:
            obligations.append({"from": shared_payer, "to": shared_receiver, "amount": shared_owes})

        final_direction = "Settled"
        final_amount = 0.0
        if len(obligations) == 1:
            final_direction = f"{obligations[0]['from']} owes {obligations[0]['to']}"
            final_amount = obligations[0]["amount"]
        elif len(obligations) == 2:
            first, second = obligations
            if first["from"] == second["from"] and first["to"] == second["to"]:
                final_direction = f"{first['from']} owes {first['to']}"
                final_amount = first["amount"] + second["amount"]
            else:
                diff = first["amount"] - second["amount"]
                if diff > 0:
                    final_direction = f"{first['from']} owes {first['to']}"
                    final_amount = diff
                elif diff < 0:
                    final_direction = f"{second['from']} owes {second['to']}"
                    final_amount = abs(diff)

        if final_amount <= 0.005:
            final_direction = "Settled"
            final_amount = 0.0

        settlement = {
            "dk_shared": dk_shared,
            "yz_shared": yz_shared,
            "shared_total": shared_total_abs,
            "each_share": each_share,
            "pet_paid_by_dk": pet_paid_by_dk,
            "pet_paid_by_yz": pet_paid_by_yz,
            "missing_paid_by_count": missing_paid_by_count,
            "result_direction": final_direction,
            "result_amount": round(final_amount, 2),
        }

        return render_template(
            "dashboard.html",
            expenses=expenses,
            summary=summary,
            total=total,
            shared_total=shared_total,
            settlement=settlement,
            selected_month=selected_month,
        )

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
                if amount_value <= 0:
                    raise ValueError
            except ValueError:
                flash("Amount must be a positive number.")
                return render_template("expense_form.html", categories=categories, expense=None)

            db.execute(
                """
                INSERT INTO expenses (
                    user_id, date, amount, category_id, description, vendor, paid_by,
                    is_transfer, is_personal, category_confidence, category_source, tags
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    g.user["id"],
                    expense_date,
                    amount_value,
                    category_id,
                    description,
                    derive_vendor(description),
                    "DK",
                    1 if is_transfer_transaction(description, resolved_category) else 0,
                    1 if resolved_category == "Personal" else 0,
                    categorization["confidence"],
                    categorization["source"],
                    json.dumps(derive_tags(description)),
                ),
            )
            db.commit()
            flash("Expense added.")
            return redirect(url_for("dashboard"))

        return render_template("expense_form.html", categories=categories, expense=None)

    def get_user_expense(expense_id):
        expense = get_db().execute(
            "SELECT * FROM expenses WHERE id = ? AND user_id = ?", (expense_id, g.user["id"])
        ).fetchone()
        return expense

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
            category_id = request.form.get("category_id") or None
            description = request.form.get("description", "").strip()
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
                if amount_value <= 0:
                    raise ValueError
            except ValueError:
                flash("Amount must be a positive number.")
                return render_template("expense_form.html", categories=categories, expense=expense)

            db.execute(
                """
                UPDATE expenses
                SET date = ?, amount = ?, category_id = ?, description = ?, vendor = ?, is_transfer = ?, is_personal = ?,
                    category_confidence = ?, category_source = ?, tags = ?
                WHERE id = ? AND user_id = ?
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
                    expense_id,
                    g.user["id"],
                ),
            )
            db.commit()
            if category_id and str(previous_category_id or "") != str(category_id):
                learn_rule(g.user["id"], description, expense["vendor"] or derive_vendor(description), category_id, "manual_edit")
            flash("Expense updated.")
            return redirect(url_for("dashboard"))

        return render_template("expense_form.html", categories=categories, expense=expense)

    @app.post("/expenses/<int:expense_id>/delete")
    @login_required
    def delete_expense(expense_id):
        db = get_db()
        db.execute("DELETE FROM expenses WHERE id = ? AND user_id = ?", (expense_id, g.user["id"]))
        db.commit()
        flash("Expense deleted.")
        return redirect(url_for("dashboard"))

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
        selected_month = request.args.get("month")
        db = get_db()

        query = """
            SELECT e.date, e.amount, COALESCE(c.name, 'Uncategorized') as category, e.description
            FROM expenses e
            LEFT JOIN categories c ON e.category_id = c.id
            WHERE e.user_id = ?
        """
        params = [g.user["id"]]

        if selected_month:
            query += " AND e.date LIKE ?"
            params.append(f"{selected_month}%")

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
                "Content-Disposition": f"attachment; filename=expenses-{selected_month or 'all'}.csv"
            },
        )

    @app.post("/import/csv/apply_vendor")
    @login_required
    def apply_vendor_category_override():
        payload = request.get_json(silent=True) or {}
        vendor_key = normalize_text(payload.get("vendor_key", ""))
        category_name = (payload.get("category_name") or "").strip()
        if not vendor_key or not category_name:
            return jsonify({"updated_count": 0, "updated_rows": []}), 400

        db = get_db()
        category_rows = db.execute("SELECT id, name FROM categories WHERE user_id = ?", (g.user["id"],)).fetchall()
        available_category_names = [row["name"] for row in category_rows]

        preview_state = get_import_preview_state(g.user["id"])
        rows = preview_state.get("rows") or []
        updated_rows = []

        for row in rows:
            if normalize_text(row.get("vendor_key") or row.get("vendor", "")) != vendor_key:
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
            updated_rows.append(
                {
                    "row_index": row.get("row_index"),
                    "category": category_name,
                    "confidence": categorized["confidence"],
                    "confidence_label": confidence_label(categorized["confidence"]),
                    "source": categorized["source"],
                }
            )

        save_import_preview_state(g.user["id"], rows)
        return jsonify({"updated_count": len(updated_rows), "updated_rows": updated_rows})

    @app.route("/import/csv", methods=("GET", "POST"))
    @login_required
    def import_csv():
        default_mapping = {"date": "", "description": "", "vendor": "", "amount": "", "debit": "", "credit": "", "category": ""}
        saved_payload = get_saved_csv_mapping_for_user(g.user["id"])
        saved_mapping = mapping_from_payload(saved_payload)

        if request.method == "POST":
            action = request.form.get("action", "preview")

            if action == "confirm":
                preview_state = get_import_preview_state(g.user["id"])
                state_rows = preview_state.get("rows") or []
                parsed_rows = json.loads(request.form.get("parsed_rows", "[]"))
                if state_rows:
                    parsed_rows = state_rows
                db = get_db()
                imported_count = 0

                category_rows = db.execute(
                    "SELECT id, name FROM categories WHERE user_id = ?", (g.user["id"],)
                ).fetchall()
                category_lookup = {normalize_description(row["name"]): row["id"] for row in category_rows}
                available_category_names = [row["name"] for row in category_rows]
                learned_vendor_rules = set()

                for index, row in enumerate(parsed_rows):
                    row_index = row.get("row_index", index)
                    override = request.form.get(f"override_category_{row_index}", "") or row.get("override_category", "")
                    vendor_override = request.form.get(f"override_vendor_{row_index}", "").strip()
                    if vendor_override:
                        row["vendor"] = vendor_override
                    elif not row.get("vendor"):
                        row["vendor"] = derive_vendor(row.get("description", ""))
                    if override:
                        row["category"] = override

                    candidates = db.execute(
                        """
                        SELECT description FROM expenses
                        WHERE user_id = ? AND date = ? AND amount = ?
                        """,
                        (g.user["id"], row["date"], row["amount"]),
                    ).fetchall()
                    if any(
                        normalize_description(item["description"]) == row["normalized_description"]
                        for item in candidates
                    ):
                        continue

                    category_id = None
                    categorized = categorize_transaction(
                        g.user["id"],
                        row.get("description", ""),
                        row.get("vendor", ""),
                        row.get("category", ""),
                        available_category_names,
                        db,
                    )
                    assigned_category = categorized["category"]
                    if assigned_category:
                        category_id = category_lookup.get(normalize_description(assigned_category))
                    is_personal = assigned_category == "Personal"
                    is_transfer = is_transfer_transaction(row.get("description", ""), assigned_category)

                    db.execute(
                        """
                        INSERT INTO expenses (
                            user_id, date, amount, category_id, description, vendor, paid_by,
                            is_transfer, is_personal, category_confidence, category_source, tags
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            g.user["id"],
                            row["date"],
                            row["amount"],
                            category_id,
                            row["description"],
                            row.get("vendor", "") or derive_vendor(row.get("description", "")),
                            row.get("paid_by", "DK") or "DK",
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
                        vendor_rule_key = (vendor_pattern, category_id)
                        if vendor_pattern and vendor_rule_key not in learned_vendor_rules:
                            learn_rule(g.user["id"], row.get("description", ""), row.get("vendor", ""), category_id, "import_override")
                            learned_vendor_rules.add(vendor_rule_key)
                    imported_count += 1

                mapping = {
                    "date": request.form.get("map_date", ""),
                    "description": request.form.get("map_description", ""),
                    "vendor": request.form.get("map_vendor", ""),
                    "amount": request.form.get("map_amount", ""),
                    "debit": request.form.get("map_debit", ""),
                    "credit": request.form.get("map_credit", ""),
                    "category": request.form.get("map_category", ""),
                }
                detected_format = request.form.get("detected_format", "manual")
                has_header = request.form.get("has_header", "0") == "1"
                save_csv_mapping_for_user(g.user["id"], mapping, has_header, detected_format, file_signature=request.form.get("file_signature", ""))

                db.commit()
                save_import_preview_state(g.user["id"], [])
                flash(f"Imported {imported_count} transaction(s).")
                return redirect(url_for("dashboard"))

            file = request.files.get("csv_file")
            if file is None or not file.filename:
                flash("Please choose a CSV file.")
                return render_template("import_csv.html", preview_rows=[], mapping=saved_mapping, columns=[], categories=[], parsed_rows_json="[]", detected_mode=None)

            file_bytes = file.read()
            content = decode_csv_bytes(file_bytes)
            if content is None:
                flash("Could not read file encoding. Please re-save as CSV UTF-8.")
                return render_template("import_csv.html", preview_rows=[], mapping=saved_mapping, columns=[], categories=[], parsed_rows_json="[]", detected_mode=None)

            rows = list(csv.reader(io.StringIO(content)))
            rows = [row for row in rows if any(cell.strip() for cell in row)]
            if not rows:
                flash("CSV is empty.")
                return render_template("import_csv.html", preview_rows=[], mapping=saved_mapping, columns=[], categories=[], parsed_rows_json="[]", detected_mode=None)

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
                    }

                    for field in mapping:
                        if mapping[field] == "" and saved_mapping.get(field, "") != "":
                            mapping[field] = saved_mapping[field]

            data_rows = rows[(header_row_index + 1):] if has_header else rows
            bank_type = detect_bank_type(rows[header_row_index] if has_header else [])
            parsed_rows, skipped_rows = parse_csv_transactions(data_rows, mapping, g.user["id"], bank_type=bank_type)
            db = get_db()
            category_rows = db.execute(
                "SELECT id, name FROM categories WHERE user_id = ? ORDER BY name", (g.user["id"],)
            ).fetchall()
            available_category_names = [row["name"] for row in category_rows]
            for row in parsed_rows:
                categorized = categorize_transaction(
                    g.user["id"], row.get("description", ""), row.get("vendor", ""), row.get("category", ""), available_category_names, db
                )
                row["auto_category"] = categorized["category"]
                row["category"] = categorized["category"]
                row["suggested_source"] = categorized["source"]
                row["confidence"] = categorized["confidence"]
                row["confidence_label"] = confidence_label(categorized["confidence"])

            save_import_preview_state(g.user["id"], parsed_rows)
            preview_rows = parsed_rows[:20]
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
            }

            save_csv_mapping_for_user(g.user["id"], mapping, has_header, detected_format, file_signature=file_signature)

            return render_template(
                "import_csv.html",
                preview_rows=preview_rows,
                mapping=mapping,
                columns=columns,
                categories=category_rows,
                parsed_rows_json=json.dumps(parsed_rows),
                detected_mode="headered" if has_header else "headerless",
                has_header=has_header,
                detected_format=detected_format,
                auto_mapping_applied=(detected_format == "cibc_headerless"),
                header_row_index=header_row_index if has_header else None,
                skipped_rows=skipped_rows,
                auto_mapped_fields=auto_mapped_fields,
                file_signature=file_signature,
            )

        return render_template(
            "import_csv.html",
            preview_rows=[],
            mapping=saved_mapping or default_mapping,
            columns=placeholder_columns_from_mapping(saved_mapping),
            categories=[],
            parsed_rows_json="[]",
            detected_mode=None,
            has_header=saved_payload.get("has_header", False),
            detected_format=saved_payload.get("detected_format", ""),
            auto_mapping_applied=False,
            header_row_index=None,
            skipped_rows=0,
            auto_mapped_fields={},
            file_signature=saved_payload.get("file_signature", ""),
        )

    @app.route("/rules")
    @login_required
    def rules():
        db = get_db()
        rules = db.execute(
            """
            SELECT cr.id, cr.key_type, cr.pattern, cr.hits, cr.last_used_at, cr.source, cr.is_enabled, c.name as category
            FROM category_rules cr
            JOIN categories c ON c.id = cr.category_id
            WHERE cr.user_id = ?
            ORDER BY cr.hits DESC, cr.updated_at DESC
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
        db.execute(
            """
            UPDATE category_rules
            SET category_id = ?, is_enabled = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ? AND user_id = ?
            """,
            (category_id, is_enabled, rule_id, g.user["id"]),
        )
        db.commit()
        flash("Rule updated.")
        return redirect(url_for("rules"))

    app.get_db = get_db
    app.init_db = init_db
    return app
