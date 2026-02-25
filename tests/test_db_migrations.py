import sqlite3

from expense_tracker.db_migrations import apply_migrations, get_db_health


def test_apply_migrations_on_empty_db(tmp_path):
    db_path = tmp_path / "empty.sqlite"

    apply_migrations(str(db_path))
    health = get_db_health(str(db_path))

    assert health["ok"] is True
    assert health["schema_version"] >= 3
    assert health["missing_tables"] == []
    assert health["missing_indexes"] == []


def test_apply_migrations_on_legacy_db(tmp_path):
    db_path = tmp_path / "legacy.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL
        );
        INSERT INTO users(username, password) VALUES ('alice', 'legacy-hash');

        CREATE TABLE categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            name TEXT NOT NULL
        );

        CREATE TABLE expenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            amount REAL NOT NULL,
            description TEXT,
            vendor TEXT
        );
        INSERT INTO expenses(user_id, date, amount, description, vendor)
        VALUES (1, '2025-01-15', 42.5, 'Coffee', '  Café Dépôt!!!  ');

        CREATE TABLE category_rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            key_type TEXT,
            pattern TEXT,
            category TEXT
        );
        INSERT INTO category_rules(user_id, key_type, pattern, category)
        VALUES (1, 'vendor', 'CAFE', 'Restaurants');
        """
    )
    conn.commit()
    conn.close()

    apply_migrations(str(db_path))
    health = get_db_health(str(db_path))
    assert health["ok"] is True

    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT password_hash FROM users WHERE username = 'alice'").fetchone()
    assert row[0] == "legacy-hash"

    row = conn.execute("SELECT vendor_normalized FROM expenses WHERE id = 1").fetchone()
    assert row[0] == "CAFE DEPOT"

    rule = conn.execute(
        "SELECT vendor_pattern, enabled, confidence FROM category_rules WHERE id = 1"
    ).fetchone()
    assert rule[0] == "CAFE"
    assert rule[1] == 1
    assert rule[2] == 0
    conn.close()
