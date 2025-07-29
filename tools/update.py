import sqlite3
from typing import Set, List, Any, Dict
from utils.config import get_uuid_field
from utils.connect import get_connection, get_all_tables, get_columns, validate_uuid_column
from utils.types import SearchPackageFlat, SearchResultPackage, FlatFilter, GroupLogic

def search(pkg: SearchPackageFlat) -> SearchResultPackage:
    """
    Executes a grouped flat-filter search on any SQLite database.
    Returns a SearchResultPackage containing filters, group_logic, and matching UUIDs.
    """
    conn = get_connection()
    uuid_col = get_uuid_field()

    # Discover valid tables dynamically
    all_tables = get_all_tables(conn)

    # Organize filters by their group identifier
    groups: Dict[int, List[FlatFilter]] = {}
    for filt in pkg.filters:
        group_id = getattr(filt, "group", 1) or 1
        groups.setdefault(group_id, []).append(filt)

    # Compute match sets for each group
    group_results: Dict[int, Set[str]] = {}
    for group_id, flist in groups.items():
        result_set: Set[str] = set()
        for idx, filt in enumerate(flist):
            # Determine target tables
            target_tables = all_tables if filt.table == "*" else [filt.table]
            # Validate each table has the UUID column
            for tbl in target_tables:
                validate_uuid_column(conn, tbl)

            # Collect UUIDs matching this filter
            filter_set: Set[str] = set()
            for tbl in target_tables:
                columns = get_columns(conn, tbl)
                target_cols = columns if filt.field == "*" else [filt.field]

                clauses: List[str] = []
                params: List[Any] = []
                for col in target_cols:
                    op = filt.operator
                    if op == "contains":
                        clauses.append(f"{col} LIKE ?")
                        params.append(f"%{filt.value}%")
                    elif op == "begins":
                        clauses.append(f"{col} LIKE ?")
                        params.append(f"{filt.value}%")
                    elif op == "ends":
                        clauses.append(f"{col} LIKE ?")
                        params.append(f"%{filt.value}")
                    elif op == "equals":
                        clauses.append(f"{col} = ?")
                        params.append(filt.value)
                    else:
                        raise ValueError(f"Unsupported operator: {op}")

                where_clause = " OR ".join(clauses)
                sql = f"SELECT {uuid_col} FROM {tbl}"
                if where_clause:
                    sql += f" WHERE {where_clause}"

                rows = conn.execute(sql, params).fetchall()
                for row in rows:
                    filter_set.add(row[0])

            # Merge into the group's result set
            if idx == 0:
                result_set = filter_set
            else:
                logic = getattr(filt, "logic", "and")
                if logic == "and":
                    result_set &= filter_set
                elif logic == "or":
                    result_set |= filter_set
                elif logic == "nand" or logic == "nor":
                    result_set -= filter_set
                else:
                    raise ValueError(f"Unsupported logic: {logic}")

        group_results[group_id] = result_set

    # Combine group results according to group_logic
    final_set: Set[str] = set()
    glist: List[GroupLogic] = getattr(pkg, "group_logic", []) or []
    if not glist:
        # Default: AND all group result sets in numeric order
        for idx, gid in enumerate(sorted(group_results)):
            res = group_results[gid]
            if idx == 0:
                final_set = res.copy()
            else:
                final_set &= res
    else:
        for idx, gl in enumerate(glist):
            gids = gl.groups
            logic = gl.logic
            combined: Set[str] = set()
            for gid in gids:
                combined |= group_results.get(gid, set())

            if idx == 0:
                final_set = combined
            else:
                if logic == "and":
                    final_set &= combined
                elif logic == "or":
                    final_set |= combined
                elif logic == "nand" or logic == "nor":
                    final_set -= combined
                else:
                    raise ValueError(f"Unsupported group logic: {logic}")

    conn.close()
    return SearchResultPackage(
        filters=pkg.filters,
        group_logic=glist,
        uuids=list(final_set)
    )
