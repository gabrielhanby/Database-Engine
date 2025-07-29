# tools/read_format.py

import json
from typing import Any, Dict, List
from utils.config       import get_uuid_field
from utils.types        import FlatFilter, GroupLogic, ReadFormatPackage, ReadPackage
from tools.read         import read_records

def _make_heading(label: str) -> str:
    """
    Produce a 31-char heading line with the label centered.
    E.g. label="1" → "-------------- 1 --------------"
    """
    total_length = 31
    label_str    = f" {label} "
    dash_total   = total_length - len(label_str)
    left         = dash_total // 2
    right        = dash_total - left
    return "-" * left + label_str + "-" * right

def _split_and_reassemble(text: str, delim: str, position: str) -> List[str]:
    """
    Split `text` on `delim`, then keep or drop the delimiter
    according to `position`: "before", "after", or "none".
    """
    parts = text.split(delim)
    if position == "before":
        return [parts[0]] + [delim + p for p in parts[1:]]
    if position == "after":
        return [p + delim for p in parts[:-1]] + [parts[-1]]
    # position == "none"
    return parts

def _matches(segment: str, operator: str, value: Any) -> bool:
    """
    Return True if `segment` satisfies the operator against `value`.
    """
    if operator == "contains":
        return value in segment
    if operator == "equals":
        return segment == value
    if operator == "begins":
        return segment.startswith(value)
    if operator == "ends":
        return segment.endswith(value)
    return False

def format_search_results(pkg: ReadFormatPackage) -> str:
    lines: List[str] = []

    # 1) Emit the raw filter package as NDJSON
    lines.append("---- FILTER PACKAGE (NDJSON) ----")
    for f in pkg.filters:
        obj: Dict[str, Any] = {
            "table":    f.table,
            "field":    f.field,
            "operator": f.operator,
            "value":    f.value,
            "logic":    f.logic,
            "index_by": f.index_by,
            "position": f.position,
            "group":    f.group
        }
        lines.append(json.dumps(obj))
    for g in pkg.group_logic:
        lines.append(json.dumps({
            "groups": g.groups,
            "logic":  g.logic,
            "type":   "group_logic"
        }))
    lines.append("")  # blank line before result blocks

    # 2) Prefetch fullName for each UUID from Contacts
    id_field = get_uuid_field()
    uuids = []
    for rec in pkg.records:
        uid = rec.get(id_field)
        if uid is not None and uid not in uuids:
            uuids.append(uid)
    contact_pkg = ReadPackage(table="Contact", uuids=uuids)
    contact_rows = read_records(contact_pkg)
    contact_map = {
        row.get(id_field): row.get("fullName", "")
        for row in contact_rows
    }

    # 3) Group records by UUID and collect only matching segments
    grouped_hits: Dict[str, Dict[str, Any]] = {}
    for rec in pkg.records:
        uid = rec.get(id_field)
        if uid is None:
            continue
        if uid not in grouped_hits:
            grouped_hits[uid] = {
                "fullName": contact_map.get(uid, ""),
                "hits": []  # list of "+ table.field > \"segment\""
            }
        for f in pkg.filters:
            # determine which fields to inspect
            if f.field == "*":
                fields_and_values = list(rec.items())
            else:
                if f.field not in rec:
                    continue
                fields_and_values = [(f.field, rec.get(f.field))]

            for field_name, raw in fields_and_values:
                text = "" if raw is None else str(raw)
                segments = (
                    _split_and_reassemble(text, f.index_by, f.position)
                    if f.index_by else
                    [text]
                )
                for seg in segments:
                    if _matches(seg, f.operator, f.value):
                        grouped_hits[uid]["hits"].append(
                            f"+ {f.table}.{field_name} > \"{seg}\""
                        )

    # 4) Filter out UUIDs with no hits
    final_uids = [uid for uid, info in grouped_hits.items() if info["hits"]]

    # 5) Emit one block per UUID with hits
    total_hits = 0
    for idx, uid in enumerate(final_uids, start=1):
        info = grouped_hits[uid]
        hits = info["hits"]
        hit_count = len(hits)
        total_hits += hit_count

        lines.append(_make_heading(str(idx)))
        lines.append(
            f"Full Name: {info['fullName']}, "
            f"Hits: {hit_count}, UUID: {uid}"
        )
        lines.extend(hits)
        lines.append("")  # blank line between blocks

    # 6) Footer summary
    lines.append(_make_heading("HITS"))
    lines.append(f"Total Hits: {total_hits}")
    lines.append(f"UUID Hits:  {len(final_uids)}")

    return "\n".join(lines)
