from expense_tracker.db import rewrite_sql


def test_rewrite_sql_postgres_converts_qmark_to_percent_s():
    sql, params = rewrite_sql("postgres", "SELECT * FROM expenses WHERE id = ? AND user_id = ?", (1, 2))
    assert sql == "SELECT * FROM expenses WHERE id = %s AND user_id = %s"
    assert params == (1, 2)


def test_rewrite_sql_postgres_escapes_percent_literals():
    sql, params = rewrite_sql("postgres", "SELECT * FROM expenses WHERE description LIKE '%income%'", ())
    assert sql == "SELECT * FROM expenses WHERE description LIKE '%%income%%'"
    assert params == ()
