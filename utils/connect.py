import sqlite3
from utils.config import get_db_path, get_uuid_field

def get_connection() -> sqlite3.Connection:
    return sqlite3.connect(get_db_path())

def validate_uuid_column(conn: sqlite3.Connection, table: str):
    uuid_col = get_uuid_field()
    cur = conn.execute(f"PRAGMA table_info({table});")
    cols = [row[1] for row in cur.fetchall()]
    if uuid_col not in cols:
        raise ValueError(f"Table '{table}' missing UUID column '{uuid_col}'")

def get_all_tables(conn: sqlite3.Connection) -> list[str]:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"
    )
    tables = [r[0] for r in cur.fetchall()]
    # filter out tables without your UUID column
    valid = []
    for t in tables:
        try:
            validate_uuid_column(conn, t)
            valid.append(t)
        except ValueError:
            continue
    return valid

def get_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    cur = conn.execute(f"PRAGMA table_info({table});")
    return [row[1] for row in cur.fetchall()]
