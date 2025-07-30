# tools/create.py

from typing import Optional
from tools.flagger import Flagger
from tools.schema_introspect import get_columns
from utils.config import get_primary_identifier

def create_records(pkg, conn, db_type: str, flagger: Flagger, batch_id: Optional[str] = None):
    table = pkg.table
    identifier = get_primary_identifier()
    columns = get_columns(conn, table, db_type)

    if identifier not in columns:
        flagger.error("MISSING_IDENTIFIER_COLUMN", {
            "table": table,
            "expected": identifier,
            "available": columns
        })

    created_uuids = []
    for record in pkg.records:
        # Generate UUID if not present
        uuid_val = record.get(identifier)
        if not uuid_val:
            import uuid
            uuid_val = str(uuid.uuid4())
            record[identifier] = uuid_val

        # Validate all fields exist
        for field in record:
            if field not in columns:
                flagger.error("UNKNOWN_COLUMN", {
                    "table": table,
                    "field": field
                })

        # Insert
        field_names = list(record.keys())
        placeholders = ", ".join(["?"] * len(field_names)) if db_type == "sqlite" else ", ".join(["%s"] * len(field_names))
        query = f"INSERT INTO {table} ({', '.join(field_names)}) VALUES ({placeholders})"
        values = [record[f] for f in field_names]

        cur = conn.cursor()
        cur.execute(query, values)

        # Audit each field
        if batch_id:
            for field, value in record.items():
                log_query = f"""
                    INSERT INTO field_log (
                        batch_id, record_uuid, table_name, field_name,
                        old_value, new_value
                    ) VALUES ({placeholders})
                """
                log_values = [batch_id, uuid_val, table, field, None, str(value)]
                cur.execute(log_query, log_values)

        created_uuids.append(uuid_val)

    return created_uuids
