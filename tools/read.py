# tools/read.py

from typing import List, Dict, Any
import sqlite3
from utils.connect import get_connection
from utils.config import get_uuid_field


def read_records(pkg: Any) -> List[Dict[str, Any]]:
    """
    Given a package with attributes:
      - table: str
      - uuids: List[str]
    Returns a list of dicts, one per matching row.
    """
    conn = get_connection()
    cur = conn.cursor()
    uuid_col = get_uuid_field()

    # No UUIDs, nothing to read
    if not getattr(pkg, 'uuids', None):
        conn.close()
        return []

    placeholders = ",".join("?" for _ in pkg.uuids)
    sql = f"SELECT * FROM {pkg.table} WHERE {uuid_col} IN ({placeholders})"
    cur.execute(sql, pkg.uuids)

    cols = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    conn.close()

    return [dict(zip(cols, row)) for row in rows]