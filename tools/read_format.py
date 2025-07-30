import logging
from typing import Dict

def format_search_results(matches: Dict[str, list],
                          records: Dict[str, dict],
                          display_field: str = 'fullName') -> str:
    logging.debug("[DEBUG] format_search_results called")
    logging.debug(f"[DEBUG] Matches: {matches}")
    logging.debug(f"[DEBUG] Records: {records}")

    lines = []
    for uuid, hits in matches.items():
        rec  = records.get(uuid, {})
        name = rec.get('Contact', {}).get(display_field, '<No Data>')
        logging.debug(f"[DEBUG] Formatting UUID {uuid} with display name: {name}")
        lines.append(f"{name}, UUID: {uuid}")

        for hit in hits:
            for clause in hit['clauses']:
                tbl = hit['table']
                fld = clause['field']
                val = clause['value']
                lines.append(f"  - {tbl}.{fld} matched '{val}'")
                logging.debug(f"[DEBUG] Hit detail: {tbl}.{fld} matched '{val}'")

        lines.append("")

    # Totals
    total_uuids = len(matches)
    total_hits  = sum(len(h) for h in matches.values())
    lines.append(f"UUID Hits: {total_uuids}")
    lines.append(f"Total Hits: {total_hits}")

    result = "\n".join(lines)
    logging.debug(f"[DEBUG] Final formatted output:\n{result}")
    return result
