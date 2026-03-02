import sqlite3

from expense_tracker.db_migrations import apply_migrations, get_db_health, migration_004


class _FakeCursor:
    def __init__(self, one=None, all_rows=None):
        self._one = one
        self._all = all_rows or []

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._all


class _FakePostgresMigration004Connection:
    def __init__(self):
        self.backend = "postgres"
        self.inserted_rows = False

    def execute(self, sql, params=None):
        normalized_sql = " ".join(sql.split())
        if "FROM information_schema.tables" in normalized_sql:
            return _FakeCursor(one=(1,))
        if "FROM information_schema.columns" in normalized_sql:
            return _FakeCursor(all_rows=[("id",), ("expense_id",), ("created_at",)])

        if normalized_sql.startswith("CREATE TABLE IF NOT EXISTS audit_logs_new"):
            return _FakeCursor()

        if "INSERT INTO audit_logs_new" in normalized_sql:
            if "COALESCE(created_at, CURRENT_TIMESTAMP)" in normalized_sql:
                raise RuntimeError("COALESCE types text and timestamp with time zone cannot be matched")
            if "COALESCE(created_at, (CURRENT_TIMESTAMP::text))" in normalized_sql:
                self.inserted_rows = True
                return _FakeCursor()

        if normalized_sql.startswith("DROP TABLE audit_logs"):
            return _FakeCursor()
        if normalized_sql.startswith("ALTER TABLE audit_logs_new RENAME TO audit_logs"):
            return _FakeCursor()

        raise AssertionError(f"Unexpected SQL in migration_004: {sql}")




def test_apply_migrations_on_empty_db(tmp_path):
    db_path = tmp_path / "empty.sqlite"

    apply_migrations(str(db_path))
    health = get_db_health(str(db_path))

    assert health["ok"] is True
    assert health["schema_version"] >= 7
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


def test_migration_004_uses_text_timestamp_fallback_for_postgres():
    conn = _FakePostgresMigration004Connection()

    migration_004(conn)

    assert conn.inserted_rows is True


def test_migration_004_converts_audit_logs_to_entity_rows(tmp_path):
    db_path = tmp_path / "legacy_audit.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL
        );
        INSERT INTO users(username, password_hash) VALUES ('alice', 'hash');

        CREATE TABLE expenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            amount REAL NOT NULL,
            description TEXT,
            FOREIGN KEY (user_id) REFERENCES users (id)
        );
        INSERT INTO expenses(user_id, date, amount, description) VALUES (1, '2026-01-01', 10.0, 'Coffee');

        CREATE TABLE audit_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            household_id INTEGER,
            user_id INTEGER NOT NULL,
            action TEXT NOT NULL,
            entity TEXT,
            entity_id INTEGER,
            expense_id INTEGER,
            details TEXT,
            meta_json TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id),
            FOREIGN KEY (expense_id) REFERENCES expenses (id)
        );
        INSERT INTO audit_logs(user_id, action, expense_id, details)
        VALUES (1, 'create', 1, '{"note":"legacy"}');

            """
    )
    conn.commit()

    migration_004(conn)
    conn.commit()
    columns = [row[1] for row in conn.execute("PRAGMA table_info(audit_logs)").fetchall()]
    assert "expense_id" not in columns
    row = conn.execute(
        "SELECT action, entity, entity_id, meta_json FROM audit_logs WHERE id = 1"
    ).fetchone()
    assert row[0] == "create"
    assert row[1] == "expense"
    assert row[2] == 1
    assert row[3] == '{"note":"legacy"}'
    conn.close()


def test_migration_005_creates_import_staging(tmp_path):
    db_path = tmp_path / "staging.sqlite"
    apply_migrations(str(db_path))

    conn = sqlite3.connect(db_path)
    tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    assert "import_staging" in tables

    indexes = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()}
    assert "idx_import_staging_import_id" in indexes
    assert "idx_import_staging_created_at" in indexes
    conn.close()


def test_migration_006_creates_settlement_payments(tmp_path):
    db_path = tmp_path / "settlement.sqlite"
    apply_migrations(str(db_path))

    conn = sqlite3.connect(db_path)
    tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    assert "settlement_payments" in tables

    indexes = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()}
    assert "idx_settlement_payments_household_date" in indexes
    assert "idx_settlement_payments_household_from" in indexes
    assert "idx_settlement_payments_household_to" in indexes
    conn.close()


def test_apply_migrations_does_not_close_passed_connection(tmp_path):
    db_path = tmp_path / "connection.sqlite"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    apply_migrations(conn)

    row = conn.execute("SELECT 1").fetchone()
    assert row[0] == 1
    conn.close()



def test_migration_007_creates_categories_unique_index(tmp_path):
    db_path = tmp_path / "categories_uq.sqlite"
    apply_migrations(str(db_path))

    conn = sqlite3.connect(db_path)
    indexes = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()}
    assert "uq_categories_user_name" in indexes
    conn.close()
