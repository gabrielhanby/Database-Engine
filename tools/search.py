import logging
from typing import Optional, Set
from tools.flagger import Flagger
from tools.schema_introspect import get_columns, get_all_identifiers
from utils.config import get_primary_identifier

def search_records(pkg, conn, db_type: str, flagger: Flagger,
                   delimiter: Optional[str] = None,
                   join_style: str = "clean") -> dict:
    logging.debug(f"[DEBUG] search_records called with filters: {pkg.filters}")
    identifier = get_primary_identifier()
    matches: dict[str, list[dict]] = {}
    combined_set: Optional[Set[str]] = None

    for table, clauses in pkg.filters.items():
        logging.debug(f"[DEBUG] Processing table '{table}' with clauses: {clauses}")
        columns = get_columns(conn, table, db_type)

        if identifier not in columns:
            flagger.error("MISSING_IDENTIFIER_COLUMN", {
                "table": table,
                "expected": identifier,
                "available": columns
            })

        universe = get_all_identifiers(conn, table, identifier)
        logging.debug(f"[DEBUG] Universe for table '{table}': {universe}")

        clause_sets: list[tuple[Set[str], str]] = []
        for clause in clauses:
            field = clause['field']
            op    = clause['operator']
            value = clause['value']
            logic = clause.get('logic', 'and').lower()

            matched = _evaluate_clause(conn, table, field, op, value)
            logging.debug(f"[DEBUG] Clause match ({table}.{field} {op} '{value}'): {matched}")

            if logic in ('nand', 'nor'):
                inverted = universe - matched
                logging.debug(f"[DEBUG] Inverted set for logic '{logic}': {inverted}")
                matched = inverted

            clause_sets.append((matched, logic))

        # Combine per-table clauses
        table_result = clause_sets[0][0]
        for idx, (s, logic) in enumerate(clause_sets[1:], start=1):
            prev = table_result
            if logic == 'or':
                table_result = prev.union(s)
            else:
                table_result = prev.intersection(s)
            logging.debug(f"[DEBUG] After applying '{logic}' at clause {idx}: {table_result}")

        # Merge into the global combined_set
        if combined_set is None:
            combined_set = table_result
        else:
            combined_set = combined_set.intersection(table_result)
        logging.debug(f"[DEBUG] Combined set after table '{table}': {combined_set}")

        # Record hits for formatting
        for uuid in table_result:
            matches.setdefault(uuid, []).append({
                'table':   table,
                'clauses': clauses
            })

    final = combined_set or set()
    logging.debug(f"[DEBUG] Final UUID set: {final}")
    logging.debug(f"[DEBUG] Matches dict: {matches}")
    return matches

def _evaluate_clause(conn, table, field, op, value):
    # TODO: translate into SQL and return set of UUIDs
    ...
