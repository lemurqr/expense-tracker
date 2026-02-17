import random
from datetime import date, timedelta

from werkzeug.security import generate_password_hash

from expense_tracker import create_app


def main():
    app = create_app()
    with app.app_context():
        app.init_db()
        db = app.get_db()

        db.execute(
            "INSERT INTO users (username, password) VALUES (?, ?)",
            ("demo", generate_password_hash("demo123")),
        )
        user_id = db.execute("SELECT id FROM users WHERE username = 'demo'").fetchone()["id"]

        categories = ["Food", "Transport", "Housing", "Utilities", "Entertainment", "Other"]
        for c in categories:
            db.execute("INSERT INTO categories (user_id, name) VALUES (?, ?)", (user_id, c))

        category_rows = db.execute("SELECT id FROM categories WHERE user_id = ?", (user_id,)).fetchall()
        category_ids = [row["id"] for row in category_rows]

        start = date.today() - timedelta(days=90)
        for i in range(40):
            expense_date = (start + timedelta(days=i * 2)).isoformat()
            amount = round(random.uniform(5, 200), 2)
            category_id = random.choice(category_ids)
            db.execute(
                "INSERT INTO expenses (user_id, date, amount, category_id, description) VALUES (?, ?, ?, ?, ?)",
                (user_id, expense_date, amount, category_id, f"Sample expense {i + 1}"),
            )

        db.commit()
    print("Sample data generated. Login with demo / demo123")


if __name__ == "__main__":
    main()
