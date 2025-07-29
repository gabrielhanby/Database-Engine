from utils.connect import get_connection
from utils.config import get_uuid_field
from utils.types import CreatePackage
from typing import List

def create_records(pkg: CreatePackage) -> List[str]:
    """
    Insert pkg.records into pkg.table.
    Returns list of provided or generated UUIDs.
    """
    conn = get_connection()
    cur = conn.cursor()
    uuids = []
    uuid_col = get_uuid_field()

    for rec in pkg.records:
        cols = ", ".join(rec.keys())
        placeholders = ", ".join("?" for _ in rec)
        sql = f"INSERT INTO {pkg.table} ({cols}) VALUES ({placeholders})"
        cur.execute(sql, list(rec.values()))
        # if user provided uuid use it, else grab lastrowid
        u = rec.get(uuid_col) or cur.lastrowid
        uuids.append(u)

    conn.commit()
    conn.close()
    return uuids
