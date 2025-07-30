# tools/batch.py

import uuid
from typing import cast
from tools.flagger import Flagger
from tools.create import create_records
from tools.update import update_records
from tools.delete import delete_records
from utils.config import get_primary_identifier

def process_batch(pkg, conn, db_type: str, flagger: Flagger) -> dict:
    batch_id = str(uuid.uuid4())
    created = {}
    updated = []
    deleted = []
    identifier = get_primary_identifier()

    try:
        cur = conn.cursor()

        for group_name, ops in pkg.groups.items():
            if isinstance(ops, dict) and "type" in ops:
                op_type = ops["type"]
                records = ops["records"]
                table = ops["table"]

                if op_type == "delete":
                    result = delete_records(
                        pkg=_wrap(table, records),
                        conn=conn,
                        db_type=db_type,
                        flagger=flagger,
                        batch_id=batch_id
                    )
                    if result:
                        deleted.extend([r[identifier] for r in records])

                elif op_type == "update":
                    result = update_records(
                        pkg=_wrap(table, records),
                        conn=conn,
                        db_type=db_type,
                        flagger=flagger,
                        batch_id=batch_id
                    )
                    if result:
                        updated.extend([r[identifier] for r in records])

                elif op_type == "create":
                    new_ids = create_records(
                        pkg=_wrap(table, records),
                        conn=conn,
                        db_type=db_type,
                        flagger=flagger,
                        batch_id=batch_id
                    )
                    created[group_name] = new_ids[0] if len(new_ids) == 1 else new_ids

                else:
                    flagger.error("UNKNOWN_OPERATION_TYPE", {
                        "group": group_name,
                        "op_type": op_type
                    })

            else:
                # UUID = update block
                if _is_uuid(group_name):
                    if not isinstance(ops, list):
                        flagger.error("INVALID_UPDATE_GROUP", {
                            "group": group_name,
                            "expected": "list of change ops",
                            "actual": type(ops).__name__
                        })

                    typed_ops = cast(list[dict], ops)
                    result = update_records(
                        pkg=_wrap_multiple(group_name, typed_ops),
                        conn=conn,
                        db_type=db_type,
                        flagger=flagger,
                        batch_id=batch_id
                    )
                    if result:
                        updated.append(group_name)

                else:
                    # Named group → create
                    if not isinstance(ops, list):
                        flagger.error("INVALID_CREATE_GROUP", {
                            "group": group_name,
                            "expected": "list of change ops",
                            "actual": type(ops).__name__
                        })

                    new_uuid = str(uuid.uuid4())
                    for change in ops:
                        change["fields"][identifier] = new_uuid

                    typed_ops = cast(list[dict], ops)
                    grouped = _group_by_table(typed_ops)
                    for table, records in grouped.items():
                        create_records(
                            pkg=_wrap(table, records),
                            conn=conn,
                            db_type=db_type,
                            flagger=flagger,
                            batch_id=batch_id
                        )
                    created[group_name] = new_uuid

        conn.commit()
        return {
            "batch_id": batch_id,
            "created": created,
            "updated": updated,
            "deleted": deleted
        }

    except Exception as e:
        conn.rollback()
        raise e


# --- Helpers ---

def _is_uuid(val: str) -> bool:
    try:
        uuid.UUID(str(val))
        return True
    except ValueError:
        return False

def _wrap(table: str, records: list[dict]):
    class Pkg:
        def __init__(self):
            self.table = table
            self.records = records
    return Pkg()

def _wrap_multiple(uuid_val: str, ops: list[dict]):
    class Pkg:
        def __init__(self):
            self.table = None
            self.records = []
    pkg = Pkg()
    for op in ops:
        row = dict(op["fields"])
        row[op["identifier"]] = uuid_val
        pkg.table = op["table"]
        pkg.records.append(row)
    return pkg

def _group_by_table(change_ops: list[dict]) -> dict:
    out = {}
    for op in change_ops:
        t = op["table"]
        out.setdefault(t, []).append(op["fields"])
    return out
