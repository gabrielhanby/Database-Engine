# tools/delete.py

from typing import Optional
from tools.flagger import Flagger
from tools.schema_introspect import get_columns
from utils.config import get_primary_identifier

def delete_records(pkg, conn, db_type: str, flagger: Flagger, batch_id: Optional[str] = None) -> bool:
    table = pkg.table
    identifier = get_primary_identifier()
    columns = get_columns(conn, table, db_type)

    if identifier not in columns:
        flagger.error("MISSING_IDENTIFIER_COLUMN", {
            "table": table,
            "expected": identifier,
            "available": columns
        })

    cur = conn.cursor()

    for record in pkg.records:
        if identifier not in record:
            flagger.error("MISSING_IDENTIFIER_IN_RECORD", {
                "table": table,
                "record": record
            })

        uuid_val = record[identifier]

        # Fetch full row before deletion (for audit)
        select_query = f"SELECT * FROM {table} WHERE {identifier} = %s" if db_type != "sqlite" else f"SELECT * FROM {table} WHERE {identifier} = ?"
        cur.execute(select_query, (uuid_val,))
        old_row = cur.fetchone()

        if old_row is None:
            flagger.warning("DELETE_RECORD_NOT_FOUND", {
                "table": table,
                "identifier": identifier,
                "value": uuid_val
            })
            continue  # Skip if not found

        old_data = dict(zip([desc[0] for desc in cur.description], old_row))

        # Delete row
        delete_query = f"DELETE FROM {table} WHERE {identifier} = %s" if db_type != "sqlite" else f"DELETE FROM {table} WHERE {identifier} = ?"
        cur.execute(delete_query, (uuid_val,))

        # Audit full row as "deleted"
        if batch_id:
            for field, value in old_data.items():
                audit_query = f"""
                    INSERT INTO field_log (
                        batch_id, record_uuid, table_name, field_name,
                        old_value, new_value
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                """ if db_type != "sqlite" else f"""
                    INSERT INTO field_log (
                        batch_id, record_uuid, table_name, field_name,
                        old_value, new_value
                    ) VALUES (?, ?, ?, ?, ?, ?)
                """
                audit_values = [batch_id, uuid_val, table, field, str(value), None]
                cur.execute(audit_query, audit_values)

    return True
