import csv
import io
import os
import sqlite3
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


DEFAULT_CATEGORIES = ["Food", "Transport", "Housing", "Utilities", "Entertainment", "Other"]


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

    def ensure_default_categories(user_id):
        db = get_db()
        for category in DEFAULT_CATEGORIES:
            db.execute(
                "INSERT OR IGNORE INTO categories (user_id, name) VALUES (?, ?)",
                (user_id, category),
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
            WHERE e.user_id = ? AND e.date LIKE ?
            GROUP BY COALESCE(c.name, 'Uncategorized')
            ORDER BY total DESC
            """,
            (g.user["id"], month_like),
        ).fetchall()

        total = db.execute(
            "SELECT ROUND(COALESCE(SUM(amount), 0), 2) as total FROM expenses WHERE user_id = ? AND date LIKE ?",
            (g.user["id"], month_like),
        ).fetchone()["total"]

        return render_template(
            "dashboard.html",
            expenses=expenses,
            summary=summary,
            total=total,
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
                SET date = ?, amount = ?, category_id = ?, description = ?
                WHERE id = ? AND user_id = ?
                """,
                (expense_date, amount_value, category_id, description, expense_id, g.user["id"]),
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

    app.get_db = get_db
    app.init_db = init_db
    return app
