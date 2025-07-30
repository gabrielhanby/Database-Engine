import logging
from utils.config import get_primary_identifier

def read_records(pkg, conn, db_type: str) -> dict:
    logging.debug(f"[DEBUG] read_records called for UUIDs: {pkg.uuids}")
    identifier = get_primary_identifier()
    merged = {}

    for table in pkg.filters:
        for uuid in pkg.uuids:
            query = f"SELECT * FROM {table} WHERE {identifier} = ?"
            row = conn.execute(query, (uuid,)).fetchone()
            logging.debug(f"[DEBUG] Fetched row for {table}, UUID={uuid}: {row}")
            merged.setdefault(uuid, {})[table] = dict(row) if row else {}

    logging.debug(f"[DEBUG] Merged records: {merged}")
    return merged
