import argparse
import re
import unicodedata
from datetime import datetime
from pathlib import Path

from .db import connect_db, parse_database_config


REQUIRED_TABLES = {
    "users": {
        "columns": {"id", "username", "password_hash"},
        "indexes": set(),
    },
    "categories": {
        "columns": {"id", "user_id", "name"},
        "indexes": set(),
    },
    "expenses": {
        "columns": {
            "id",
            "user_id",
            "date",
            "amount",
            "category_id",
            "description",
            "vendor",
            "vendor_normalized",
            "paid_by",
            "household_id",
            "reviewed",
            "category_confidence",
            "category_source",
        },
        "indexes": {
            "idx_expenses_date",
            "idx_expenses_household_id",
            "idx_expenses_vendor_normalized",
        },
    },
    "category_rules": {
        "columns": {
            "id",
            "user_id",
            "key_type",
            "pattern",
            "vendor_pattern",
            "description_pattern",
            "priority",
            "hits",
            "last_used",
            "enabled",
            "confidence",
        },
        "indexes": {
            "idx_category_rules_vendor_pattern",
            "idx_category_rules_description_pattern",
            "idx_category_rules_enabled",
        },
    },
    "households": {
        "columns": {"id", "name", "created_at"},
        "indexes": set(),
    },
    "household_members": {
        "columns": {"id", "household_id", "user_id", "role", "created_at"},
        "indexes": set(),
    },
    "household_invites": {
        "columns": {"id", "household_id", "email", "token", "status", "created_at", "expires_at"},
        "indexes": set(),
    },
    "audit_logs": {
        "columns": {"id", "household_id", "user_id", "action", "entity", "entity_id", "meta_json", "created_at"},
        "indexes": set(),
    },
    "import_staging": {
        "columns": {"id", "import_id", "household_id", "user_id", "created_at", "row_json", "status"},
        "indexes": {"idx_import_staging_import_id", "idx_import_staging_created_at"},
    },
    "settlement_payments": {
        "columns": {"id", "household_id", "date", "from_person", "to_person", "amount", "note", "created_at"},
        "indexes": {
            "idx_settlement_payments_household_date",
            "idx_settlement_payments_household_from",
            "idx_settlement_payments_household_to",
        },
    },
}


def backend_name(conn):
    return getattr(conn, "backend", "sqlite")


def table_exists(conn, name):
    if backend_name(conn) == "postgres":
        row = conn.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = ?",
            (name,),
        ).fetchone()
        return row is not None

    row = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?", (name,)).fetchone()
    return row is not None


def column_exists(conn, table, column):
    if not table_exists(conn, table):
        return False
    if backend_name(conn) == "postgres":
        row = conn.execute(
            "SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = ? AND column_name = ?",
            (table, column),
        ).fetchone()
        return row is not None

    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(row[1] == column for row in rows)


def index_exists(conn, index_name):
    if backend_name(conn) == "postgres":
        row = conn.execute(
            "SELECT 1 FROM pg_indexes WHERE schemaname = current_schema() AND indexname = ?",
            (index_name,),
        ).fetchone()
        return row is not None

    row = conn.execute("SELECT 1 FROM sqlite_master WHERE type='index' AND name = ?", (index_name,)).fetchone()
    return row is not None


def add_column_if_missing(conn, table, col_def_sql):
    column = col_def_sql.split()[0]
    if backend_name(conn) == "postgres":
        conn.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col_def_sql}")
    elif not column_exists(conn, table, column):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col_def_sql}")


def create_index_if_missing(conn, index_name, create_sql):
    if not index_exists(conn, index_name):
        conn.execute(create_sql)


def ensure_table(conn, create_sql):
    if backend_name(conn) == "postgres":
        create_sql = create_sql.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "BIGSERIAL PRIMARY KEY")
    conn.execute(create_sql)


def get_table_columns(conn, table):
    if backend_name(conn) == "postgres":
        rows = conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = ? ORDER BY ordinal_position",
            (table,),
        ).fetchall()
        return {row[0] for row in rows}

    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {row[1] for row in rows}


def db_now_text(conn):
    if backend_name(conn) == "postgres":
        return "(CURRENT_TIMESTAMP::text)"
    return "CURRENT_TIMESTAMP"


def normalize_vendor(value):
    text = (value or "").strip().upper()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^A-Z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def migration_001(conn):
    ensure_table(
        conn,
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT,
            password TEXT,
            email TEXT
        )
        """,
    )
    ensure_table(
        conn,
        """
        CREATE TABLE IF NOT EXISTS categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            UNIQUE(user_id, name),
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
        """,
    )
    ensure_table(
        conn,
        """
        CREATE TABLE IF NOT EXISTS households (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """,
    )
    ensure_table(
        conn,
        """
        CREATE TABLE IF NOT EXISTS household_members (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            household_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            role TEXT NOT NULL DEFAULT 'member',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(household_id, user_id),
            FOREIGN KEY (household_id) REFERENCES households (id),
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
        """,
    )
    ensure_table(
        conn,
        """
        CREATE TABLE IF NOT EXISTS household_invites (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            household_id INTEGER NOT NULL,
            email TEXT,
            token TEXT UNIQUE,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            expires_at TEXT,
            created_by_user_id INTEGER,
            code TEXT,
            FOREIGN KEY (household_id) REFERENCES households (id),
            FOREIGN KEY (created_by_user_id) REFERENCES users (id)
        )
        """,
    )
    ensure_table(
        conn,
        """
        CREATE TABLE IF NOT EXISTS expenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            household_id INTEGER,
            date TEXT NOT NULL,
            amount REAL NOT NULL,
            category_id INTEGER,
            description TEXT,
            vendor TEXT,
            vendor_normalized TEXT,
            paid_by TEXT,
            reviewed INTEGER DEFAULT 0,
            is_transfer INTEGER NOT NULL DEFAULT 0,
            is_personal INTEGER NOT NULL DEFAULT 0,
            category_confidence INTEGER DEFAULT 0,
            category_source TEXT,
            tags TEXT,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id),
            FOREIGN KEY (household_id) REFERENCES households (id),
            FOREIGN KEY (category_id) REFERENCES categories (id)
        )
        """,
    )
    ensure_table(
        conn,
        """
        CREATE TABLE IF NOT EXISTS audit_logs (
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
            FOREIGN KEY (household_id) REFERENCES households (id),
            FOREIGN KEY (user_id) REFERENCES users (id),
            FOREIGN KEY (expense_id) REFERENCES expenses (id)
        )
        """,
    )
    ensure_table(
        conn,
        """
        CREATE TABLE IF NOT EXISTS category_rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            key_type TEXT NOT NULL DEFAULT 'vendor',
            pattern TEXT NOT NULL,
            vendor_pattern TEXT,
            description_pattern TEXT,
            category TEXT,
            category_id INTEGER,
            priority INTEGER NOT NULL DEFAULT 0,
            confidence INTEGER DEFAULT 0,
            hits INTEGER NOT NULL DEFAULT 0,
            last_used TEXT,
            last_used_at TEXT,
            source TEXT DEFAULT 'manual',
            enabled INTEGER NOT NULL DEFAULT 1,
            is_enabled INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id),
            FOREIGN KEY (category_id) REFERENCES categories (id)
        )
        """,
    )


def migration_002(conn):
    # Legacy transactions table support: create expenses and copy if mapping is clear.
    if table_exists(conn, "transactions") and not table_exists(conn, "expenses"):
        migration_001(conn)
        tx_columns = get_table_columns(conn, "transactions")
        if {"user_id", "date", "amount"}.issubset(tx_columns):
            optional_fields = [
                "category_id",
                "description",
                "vendor",
                "paid_by",
                "household_id",
            ]
            selected = ["user_id", "date", "amount"] + [c for c in optional_fields if c in tx_columns]
            insert_cols = ", ".join(selected)
            conn.execute(
                f"INSERT INTO expenses ({insert_cols}) SELECT {insert_cols} FROM transactions"
            )

    add_column_if_missing(conn, "users", "password_hash TEXT")
    add_column_if_missing(conn, "users", "email TEXT")

    if column_exists(conn, "users", "password") and column_exists(conn, "users", "password_hash"):
        conn.execute(
            """
            UPDATE users
            SET password_hash = COALESCE(NULLIF(password_hash, ''), password)
            WHERE password IS NOT NULL AND TRIM(password) != ''
            """
        )

    for col_def in [
        "household_id INTEGER DEFAULT NULL",
        "category_id INTEGER",
        "paid_by TEXT DEFAULT NULL",
        "vendor_normalized TEXT DEFAULT NULL",
        "reviewed INTEGER DEFAULT 0",
        "category_confidence INTEGER DEFAULT 0",
        "confidence INTEGER DEFAULT 0",
        "priority INTEGER DEFAULT 0",
        "key_type TEXT DEFAULT 'vendor'",
        "hits INTEGER DEFAULT 0",
        "last_used TEXT DEFAULT NULL",
        "enabled INTEGER DEFAULT 1",
        "updated_at TEXT DEFAULT NULL",
        "category_source TEXT",
    ]:
        add_column_if_missing(conn, "expenses", col_def)

    for col_def in [
        "vendor_pattern TEXT",
        "description_pattern TEXT",
        "confidence INTEGER DEFAULT 0",
        "priority INTEGER DEFAULT 0",
        "key_type TEXT DEFAULT 'vendor'",
        "hits INTEGER DEFAULT 0",
        "last_used TEXT DEFAULT NULL",
        "enabled INTEGER DEFAULT 1",
        "is_enabled INTEGER DEFAULT 1",
        "category_id INTEGER",
        "category TEXT",
        "last_used_at TEXT",
        "created_at TEXT",
        "source TEXT DEFAULT 'manual'",
    ]:
        add_column_if_missing(conn, "category_rules", col_def)

    for col_def in [
        "created_at TEXT",
    ]:
        add_column_if_missing(conn, "household_members", col_def)

    if column_exists(conn, "household_members", "created_at"):
        conn.execute("UPDATE household_members SET created_at = CURRENT_TIMESTAMP WHERE created_at IS NULL OR TRIM(created_at) = ''")

    for col_def in [
        "token TEXT",
        "status TEXT DEFAULT 'pending'",
        "expires_at TEXT",
        "created_by_user_id INTEGER",
        "code TEXT",
    ]:
        add_column_if_missing(conn, "household_invites", col_def)

    for col_def in [
        "household_id INTEGER DEFAULT NULL",
        "entity TEXT",
        "entity_id INTEGER",
        "meta_json TEXT",
        "expense_id INTEGER",
        "details TEXT",
    ]:
        add_column_if_missing(conn, "audit_logs", col_def)

    if column_exists(conn, "expenses", "reviewed"):
        conn.execute("UPDATE expenses SET reviewed = 0 WHERE reviewed IS NULL")
    if column_exists(conn, "category_rules", "enabled"):
        conn.execute("UPDATE category_rules SET enabled = 1 WHERE enabled IS NULL")
    if column_exists(conn, "category_rules", "confidence"):
        conn.execute("UPDATE category_rules SET confidence = 0 WHERE confidence IS NULL")


def migration_003(conn):
    if column_exists(conn, "expenses", "vendor") and column_exists(conn, "expenses", "vendor_normalized"):
        rows = conn.execute(
            "SELECT id, vendor FROM expenses WHERE vendor IS NOT NULL AND TRIM(vendor) != '' AND (vendor_normalized IS NULL OR TRIM(vendor_normalized) = '')"
        ).fetchall()
        for expense_id, vendor in rows:
            conn.execute(
                "UPDATE expenses SET vendor_normalized = ? WHERE id = ?",
                (normalize_vendor(vendor), expense_id),
            )

    if column_exists(conn, "category_rules", "vendor_pattern") and column_exists(conn, "category_rules", "pattern"):
        conn.execute(
            "UPDATE category_rules SET vendor_pattern = pattern WHERE key_type = 'vendor' AND (vendor_pattern IS NULL OR TRIM(vendor_pattern) = '')"
        )
    if column_exists(conn, "category_rules", "description_pattern") and column_exists(conn, "category_rules", "pattern"):
        conn.execute(
            "UPDATE category_rules SET description_pattern = pattern WHERE key_type = 'description' AND (description_pattern IS NULL OR TRIM(description_pattern) = '')"
        )
    if column_exists(conn, "category_rules", "last_used") and column_exists(conn, "category_rules", "last_used_at"):
        conn.execute(
            "UPDATE category_rules SET last_used = COALESCE(last_used, last_used_at) WHERE last_used IS NULL"
        )
    if column_exists(conn, "household_invites", "token") and column_exists(conn, "household_invites", "code"):
        conn.execute(
            "UPDATE household_invites SET token = COALESCE(NULLIF(token, ''), code) WHERE token IS NULL OR TRIM(token) = ''"
        )

    if column_exists(conn, "expenses", "date"):
        create_index_if_missing(
            conn,
            "idx_expenses_date",
            "CREATE INDEX idx_expenses_date ON expenses(date)",
        )
    if column_exists(conn, "expenses", "household_id"):
        create_index_if_missing(
            conn,
            "idx_expenses_household_id",
            "CREATE INDEX idx_expenses_household_id ON expenses(household_id)",
        )
    if column_exists(conn, "expenses", "vendor_normalized"):
        create_index_if_missing(
            conn,
            "idx_expenses_vendor_normalized",
            "CREATE INDEX idx_expenses_vendor_normalized ON expenses(vendor_normalized)",
        )
    if column_exists(conn, "category_rules", "vendor_pattern"):
        create_index_if_missing(
            conn,
            "idx_category_rules_vendor_pattern",
            "CREATE INDEX idx_category_rules_vendor_pattern ON category_rules(vendor_pattern)",
        )
    if column_exists(conn, "category_rules", "description_pattern"):
        create_index_if_missing(
            conn,
            "idx_category_rules_description_pattern",
            "CREATE INDEX idx_category_rules_description_pattern ON category_rules(description_pattern)",
        )
    if column_exists(conn, "category_rules", "enabled"):
        create_index_if_missing(
            conn,
            "idx_category_rules_enabled",
            "CREATE INDEX idx_category_rules_enabled ON category_rules(enabled)",
        )
    if column_exists(conn, "users", "email"):
        create_index_if_missing(
            conn,
            "idx_users_email_unique",
            "CREATE UNIQUE INDEX idx_users_email_unique ON users(email) WHERE email IS NOT NULL",
        )


def migration_004(conn):
    if not table_exists(conn, "audit_logs"):
        return

    columns = get_table_columns(conn, "audit_logs")
    if "expense_id" not in columns:
        return

    ensure_table(
        conn,
        """
        CREATE TABLE IF NOT EXISTS audit_logs_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            household_id INTEGER,
            user_id INTEGER NOT NULL,
            action TEXT NOT NULL,
            entity TEXT,
            entity_id INTEGER,
            meta_json TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
        """,
    )

    conn.execute(
        f"""
        INSERT INTO audit_logs_new (id, household_id, user_id, action, entity, entity_id, meta_json, created_at)
        SELECT
            id,
            household_id,
            user_id,
            action,
            COALESCE(NULLIF(entity, ''), CASE WHEN expense_id IS NOT NULL THEN 'expense' END),
            COALESCE(entity_id, expense_id),
            COALESCE(meta_json, details),
            COALESCE(created_at, {db_now_text(conn)})
        FROM audit_logs
        """
    )

    conn.execute("DROP TABLE audit_logs")
    conn.execute("ALTER TABLE audit_logs_new RENAME TO audit_logs")


def migration_005(conn):
    ensure_table(
        conn,
        """
        CREATE TABLE IF NOT EXISTS import_staging (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            import_id TEXT NOT NULL,
            household_id INTEGER,
            user_id INTEGER,
            created_at TEXT NOT NULL,
            row_json TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'preview'
        )
        """,
    )
    create_index_if_missing(
        conn,
        "idx_import_staging_import_id",
        "CREATE INDEX idx_import_staging_import_id ON import_staging(import_id)",
    )
    create_index_if_missing(
        conn,
        "idx_import_staging_created_at",
        "CREATE INDEX idx_import_staging_created_at ON import_staging(created_at)",
    )


def migration_006(conn):
    ensure_table(
        conn,
        """
        CREATE TABLE IF NOT EXISTS settlement_payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            household_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            from_person TEXT NOT NULL CHECK(from_person IN ('DK','YZ')),
            to_person TEXT NOT NULL CHECK(to_person IN ('DK','YZ')),
            amount REAL NOT NULL CHECK(amount > 0),
            note TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY (household_id) REFERENCES households (id)
        )
        """,
    )
    create_index_if_missing(
        conn,
        "idx_settlement_payments_household_date",
        "CREATE INDEX idx_settlement_payments_household_date ON settlement_payments(household_id, date)",
    )
    create_index_if_missing(
        conn,
        "idx_settlement_payments_household_from",
        "CREATE INDEX idx_settlement_payments_household_from ON settlement_payments(household_id, from_person)",
    )
    create_index_if_missing(
        conn,
        "idx_settlement_payments_household_to",
        "CREATE INDEX idx_settlement_payments_household_to ON settlement_payments(household_id, to_person)",
    )


def migration_007(conn):
    create_index_if_missing(
        conn,
        "uq_categories_user_name",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_categories_user_name ON categories(user_id, name)",
    )



MIGRATIONS = [
    (1, migration_001),
    (2, migration_002),
    (3, migration_003),
    (4, migration_004),
    (5, migration_005),
    (6, migration_006),
    (7, migration_007),
]


def _ensure_schema_version_table(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER PRIMARY KEY,
            applied_at TEXT NOT NULL
        )
        """
    )


def current_schema_version(conn):
    _ensure_schema_version_table(conn)
    row = conn.execute("SELECT MAX(version) AS version FROM schema_version").fetchone()
    return int(row[0] or 0)


def validate_required_schema(conn):
    health = inspect_db_health(conn)
    if health["missing_tables"] or any(health["missing_columns"].values()):
        raise RuntimeError(
            "Schema invariants failed after migrations. "
            f"Missing tables={health['missing_tables']}, missing columns={health['missing_columns']}"
        )


def _run_migrations(conn):
    _ensure_schema_version_table(conn)

    applied_versions = {
        row[0] for row in conn.execute("SELECT version FROM schema_version").fetchall()
    }

    for version, migration_fn in MIGRATIONS:
        if version in applied_versions:
            continue
        try:
            migration_fn(conn)
            conn.execute(
                "INSERT INTO schema_version(version, applied_at) VALUES (?, ?)",
                (version, datetime.utcnow().isoformat(timespec="seconds") + "Z"),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    validate_required_schema(conn)


def apply_migrations(db_or_config_or_path):
    if hasattr(db_or_config_or_path, "execute"):
        _run_migrations(db_or_config_or_path)
        return

    config = db_or_config_or_path if isinstance(db_or_config_or_path, dict) else parse_database_config(db_or_config_or_path)
    conn = connect_db(config)
    try:
        _run_migrations(conn)
    finally:
        conn.close()


def apply_migrations_from_url(url):
    config = parse_database_config(None)
    config["backend"] = "postgres"
    config["database_url"] = url
    apply_migrations(config)


def inspect_db_health(conn):
    _ensure_schema_version_table(conn)
    missing_tables = []
    missing_columns = {}
    missing_indexes = []

    for table_name, table_spec in REQUIRED_TABLES.items():
        if not table_exists(conn, table_name):
            missing_tables.append(table_name)
            missing_columns[table_name] = sorted(table_spec["columns"])
            missing_indexes.extend(sorted(table_spec["indexes"]))
            continue

        table_cols = get_table_columns(conn, table_name)
        absent_cols = sorted(col for col in table_spec["columns"] if col not in table_cols)
        missing_columns[table_name] = absent_cols

        for idx in sorted(table_spec["indexes"]):
            if not index_exists(conn, idx):
                missing_indexes.append(idx)

    return {
        "ok": not missing_tables and not any(missing_columns.values()) and not missing_indexes,
        "schema_version": current_schema_version(conn),
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
        "missing_indexes": sorted(set(missing_indexes)),
    }


def get_db_health(db_config_or_path):
    config = db_config_or_path if isinstance(db_config_or_path, dict) else parse_database_config(db_config_or_path)
    conn = connect_db(config)
    try:
        return inspect_db_health(conn)
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Check expense tracker DB schema health")
    parser.add_argument("db_path", help="Path to SQLite DB file")
    args = parser.parse_args()
    print(get_db_health(args.db_path))


if __name__ == "__main__":
    main()
