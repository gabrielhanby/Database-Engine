# tools/schema_introspect.py

from tools.flagger import Flagger

SKIP_PK_CHECK = {"sqlite_sequence", "field_log"}

def get_primary_key_columns(conn, table: str, db_type: str) -> list[str]:
    if db_type == "sqlite":
        return _get_sqlite_pk(conn, table)
    elif db_type == "postgres":
        return _get_postgres_pk(conn, table)
    elif db_type == "mysql":
        return _get_mysql_pk(conn, table)
    else:
        return []

def _get_sqlite_pk(conn, table: str) -> list[str]:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return [row[1] for row in cur.fetchall() if row[5] == 1]  # col[5] = pk

def _get_postgres_pk(conn, table: str) -> list[str]:
    query = """
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_name = %s;
    """
    with conn.cursor() as cur:
        cur.execute(query, (table,))
        return [row[0] for row in cur.fetchall()]

def _get_mysql_pk(conn, table: str) -> list[str]:
    query = """
        SELECT column_name
        FROM information_schema.key_column_usage
        WHERE table_name = %s
          AND constraint_name = 'PRIMARY'
    """
    cur = conn.cursor()
    cur.execute(query, (table,))
    return [row[0] for row in cur.fetchall()]

def get_tables(conn, db_type: str) -> list[str]:
    if db_type == "sqlite":
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return [r[0] for r in cur.fetchall() if not r[0].startswith("sqlite_")]
    elif db_type == "postgres":
        cur = conn.cursor()
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_type = 'BASE TABLE';
        """)
        return [r[0] for r in cur.fetchall()]
    elif db_type == "mysql":
        cur = conn.cursor()
        cur.execute("SHOW TABLES")
        return [r[0] for r in cur.fetchall()]
    return []

def validate_primary_identifier(table: str, conn, db_type: str, expected_field: str, flagger: Flagger):
    if table in SKIP_PK_CHECK:
        return
    pk_columns = get_primary_key_columns(conn, table, db_type)
    if expected_field not in pk_columns:
        flagger.error("MISSING_PRIMARY_IDENTIFIER", {
            "table": table,
            "expected": expected_field,
            "primary_keys_found": pk_columns
        })

def get_columns(conn, table: str, db_type: str) -> list[str]:
    if db_type == "sqlite":
        cur = conn.execute(f"PRAGMA table_info({table})")
        return [row[1] for row in cur.fetchall()]
    else:
        query = f"SELECT * FROM {table} LIMIT 0"
        cur = conn.cursor()
        cur.execute(query)
        return [desc[0] for desc in cur.description]
