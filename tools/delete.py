from utils.connect import get_connection
from utils.config import get_uuid_field
from utils.types import DeletePackage

def delete_records(pkg: DeletePackage) -> int:
    """
    DELETE FROM pkg.table WHERE uuid IN pkg.uuids.
    Returns number of rows removed.
    """
    conn = get_connection()
    cur = conn.cursor()
    uuid_col = get_uuid_field()

    placeholders = ", ".join("?" for _ in pkg.uuids)
    sql = f"DELETE FROM {pkg.table} WHERE {uuid_col} IN ({placeholders})"
    cur.execute(sql, pkg.uuids)
    removed = cur.rowcount

    conn.commit()
    conn.close()
    return removed
