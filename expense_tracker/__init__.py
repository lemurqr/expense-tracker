import csv
import io
import os
import sqlite3
import json
import re
import unicodedata
from datetime import date
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
PAYMENT_KEYWORDS = ["payment thank you", "payment"]
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
    "description": ["description", "merchant", "payee"],
    "category": ["category"],
}


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


def normalize_description(value):
    normalized = unicodedata.normalize("NFKD", (value or "").strip().lower())
    no_accents = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", no_accents)


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
    return any(keyword in normalized_desc for keyword in TRANSFER_KEYWORDS)


def detect_header_and_mapping(rows):
    first_row = rows[0] if rows else []
    normalized = [normalize_header_name(col) for col in first_row]
    mapping = {"date": "", "description": "", "amount": "", "debit": "", "credit": "", "category": ""}

    for field, aliases in HEADER_ALIASES.items():
        for idx, value in enumerate(normalized):
            if value in aliases:
                mapping[field] = str(idx)
                break

    has_header = any(mapping[field] != "" for field in ["date", "amount", "debit", "credit", "description"])
    if not has_header:
        first_date = first_row[0].strip() if len(first_row) > 0 else ""
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", first_date):
            mapping.update({"date": "0", "description": "1", "debit": "2", "credit": "3", "amount": ""})
    return has_header, mapping


def parse_csv_transactions(rows, mapping, user_id):
    parsed_rows = []
    for raw_row in rows:
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

        row_date = get_value("date")
        row_description = get_value("description")
        row_category = get_value("category")

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

        if not row_date or amount is None:
            continue

        final_category = infer_category(row_description, row_category)

        parsed_rows.append(
            {
                "user_id": user_id,
                "date": row_date,
                "amount": round(amount, 2),
                "description": row_description,
                "normalized_description": normalize_description(row_description),
                "category": final_category,
                "tags": derive_tags(row_description),
            }
        )
    return parsed_rows


def decode_csv_bytes(file_bytes):
    for encoding in ["utf-8-sig", "utf-8", "cp1252", "latin-1"]:
        try:
            return file_bytes.decode(encoding)
        except UnicodeDecodeError:
            continue
    return None


def create_app(test_config=None):
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY="dev",
        DATABASE=os.path.join(app.instance_path, "expense_tracker.sqlite"),
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
            SELECT e.id, e.date, e.amount, e.description, c.name as category
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

        return render_template(
            "dashboard.html",
            expenses=expenses,
            summary=summary,
            total=total,
            shared_total=shared_total,
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
                resolved_category = infer_category(description, "", available_category_names)
                if resolved_category:
                    found = db.execute(
                        "SELECT id FROM categories WHERE user_id = ? AND name = ?",
                        (g.user["id"], resolved_category),
                    ).fetchone()
                    if found:
                        category_id = found["id"]

            try:
                amount_value = float(amount)
                if amount_value <= 0:
                    raise ValueError
            except ValueError:
                flash("Amount must be a positive number.")
                return render_template("expense_form.html", categories=categories, expense=None)

            db.execute(
                """
                INSERT INTO expenses (user_id, date, amount, category_id, description)
                VALUES (?, ?, ?, ?, ?)
                """,
                (g.user["id"], expense_date, amount_value, category_id, description),
            )
            expense_id = db.execute("SELECT last_insert_rowid() as id").fetchone()["id"]
            db.execute(
                "UPDATE expenses SET is_transfer = ?, is_personal = ?, tags = ? WHERE id = ? AND user_id = ?",
                (
                    1 if is_transfer_transaction(description, resolved_category) else 0,
                    1 if resolved_category == "Personal" else 0,
                    json.dumps(derive_tags(description)),
                    expense_id,
                    g.user["id"],
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
            resolved_category = category_name["name"] if category_name else infer_category(description, "", available_category_names)
            if category_name is None and resolved_category:
                found = db.execute(
                    "SELECT id FROM categories WHERE user_id = ? AND name = ?",
                    (g.user["id"], resolved_category),
                ).fetchone()
                if found:
                    category_id = found["id"]

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
                SET date = ?, amount = ?, category_id = ?, description = ?, is_transfer = ?, is_personal = ?, tags = ?
                WHERE id = ? AND user_id = ?
                """,
                (
                    expense_date,
                    amount_value,
                    category_id,
                    description,
                    1 if is_transfer_transaction(description, resolved_category) else 0,
                    1 if resolved_category == "Personal" else 0,
                    json.dumps(derive_tags(description)),
                    expense_id,
                    g.user["id"],
                ),
            )
            db.commit()
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

    @app.route("/import/csv", methods=("GET", "POST"))
    @login_required
    def import_csv():
        if request.method == "POST":
            action = request.form.get("action", "preview")

            if action == "confirm":
                parsed_rows = json.loads(request.form.get("parsed_rows", "[]"))
                db = get_db()
                imported_count = 0

                category_rows = db.execute(
                    "SELECT id, name FROM categories WHERE user_id = ?", (g.user["id"],)
                ).fetchall()
                category_lookup = {normalize_description(row["name"]): row["id"] for row in category_rows}
                available_category_names = [row["name"] for row in category_rows]

                for row in parsed_rows:
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
                    assigned_category = infer_category(
                        row.get("description", ""),
                        row.get("category", ""),
                        available_category_names,
                    )
                    if assigned_category:
                        category_id = category_lookup.get(normalize_description(assigned_category))
                    is_personal = assigned_category == "Personal"
                    is_transfer = is_transfer_transaction(row.get("description", ""), assigned_category)

                    db.execute(
                        """
                        INSERT INTO expenses (user_id, date, amount, category_id, description, is_transfer, is_personal, tags)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            g.user["id"],
                            row["date"],
                            row["amount"],
                            category_id,
                            row["description"],
                            1 if is_transfer else 0,
                            1 if is_personal else 0,
                            json.dumps(row.get("tags") or derive_tags(row.get("description", ""))),
                        ),
                    )
                    imported_count += 1

                db.commit()
                flash(f"Imported {imported_count} transaction(s).")
                return redirect(url_for("dashboard"))

            file = request.files.get("csv_file")
            if file is None or not file.filename:
                flash("Please choose a CSV file.")
                return render_template("import_csv.html", preview_rows=[], mapping={}, columns=[])

            file_bytes = file.read()
            content = decode_csv_bytes(file_bytes)
            if content is None:
                flash("Could not read file encoding. Please re-save as CSV UTF-8.")
                return render_template("import_csv.html", preview_rows=[], mapping={}, columns=[])

            rows = list(csv.reader(io.StringIO(content)))
            rows = [row for row in rows if any(cell.strip() for cell in row)]
            if not rows:
                flash("CSV is empty.")
                return render_template("import_csv.html", preview_rows=[], mapping={}, columns=[])

            has_header, inferred_mapping = detect_header_and_mapping(rows)
            mapping = {
                "date": request.form.get("map_date", inferred_mapping["date"]),
                "description": request.form.get("map_description", inferred_mapping["description"]),
                "amount": request.form.get("map_amount", inferred_mapping["amount"]),
                "debit": request.form.get("map_debit", inferred_mapping["debit"]),
                "credit": request.form.get("map_credit", inferred_mapping["credit"]),
                "category": request.form.get("map_category", inferred_mapping["category"]),
            }

            data_rows = rows[1:] if has_header else rows
            parsed_rows = parse_csv_transactions(data_rows, mapping, g.user["id"])
            preview_rows = parsed_rows[:20]
            columns = rows[0] if has_header else [f"Column {i + 1}" for i in range(len(rows[0]))]

            return render_template(
                "import_csv.html",
                preview_rows=preview_rows,
                mapping=mapping,
                columns=columns,
                parsed_rows_json=json.dumps(parsed_rows),
                detected_mode="header" if has_header else "headerless",
            )

        return render_template(
            "import_csv.html",
            preview_rows=[],
            mapping={"date": "", "description": "", "amount": "", "debit": "", "credit": "", "category": ""},
            columns=[],
            parsed_rows_json="[]",
            detected_mode=None,
        )

    app.get_db = get_db
    app.init_db = init_db
    return app
