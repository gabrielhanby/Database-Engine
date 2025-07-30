# tools/update.py

from typing import Optional
from tools.flagger import Flagger
from tools.schema_introspect import get_columns
from utils.config import get_primary_identifier

def update_records(pkg, conn, db_type: str, flagger: Flagger, batch_id: Optional[str] = None) -> bool:
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

    for update in pkg.records:
        if identifier not in update:
            flagger.error("MISSING_IDENTIFIER_IN_RECORD", {
                "table": table,
                "record": update
            })

        uuid_val = update[identifier]

        # Pull original data
        select_query = f"SELECT * FROM {table} WHERE {identifier} = %s" if db_type != "sqlite" else f"SELECT * FROM {table} WHERE {identifier} = ?"
        cur.execute(select_query, (uuid_val,))
        old_row = cur.fetchone()

        if old_row is None:
            flagger.error("RECORD_NOT_FOUND", {
                "table": table,
                "identifier": identifier,
                "value": uuid_val
            })

        old_data = dict(zip([desc[0] for desc in cur.description], old_row))

        # Validate target fields
        for field in update:
            if field != identifier and field not in columns:
                flagger.error("UNKNOWN_COLUMN", {
                    "table": table,
                    "field": field
                })

        # Build update clause
        fields = [f for f in update if f != identifier]
        placeholders = ", ".join([f"{f} = %s" for f in fields]) if db_type != "sqlite" else ", ".join([f"{f} = ?" for f in fields])
        query = f"UPDATE {table} SET {placeholders} WHERE {identifier} = %s" if db_type != "sqlite" else f"UPDATE {table} SET {placeholders} WHERE {identifier} = ?"
        values = [update[f] for f in fields]
        values.append(uuid_val)

        cur.execute(query, values)

        # Audit changed fields only
        if batch_id:
            for field in fields:
                old = old_data.get(field)
                new = update[field]
                if str(old) != str(new):
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
                    audit_values = [batch_id, uuid_val, table, field, str(old), str(new)]
                    cur.execute(audit_query, audit_values)

    return True
