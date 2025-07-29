# tools/search.py

import sqlite3
from typing import Set, Dict, List
from utils.config  import get_uuid_field
from utils.connect import get_connection, get_all_tables, get_columns
from utils.types   import (
    SearchPackageFlat,
    SearchResultPackage,
    FlatFilter,
    GroupLogic,
)

def search(pkg: SearchPackageFlat) -> SearchResultPackage:
    """
    1) Expand wildcard tables/fields
    2) Partition filters by group, evaluate each group’s UUID set
    3) Combine group sets via group_logic (or AND across groups by default)
    """
    conn       = get_connection()
    uuid_col   = get_uuid_field()
    all_tables = get_all_tables(conn)

    # 1) Bucket filters by group
    group_filters: Dict[int, List[FlatFilter]] = {}
    for f in pkg.filters:
        group_filters.setdefault(f.group, []).append(f)

    # 2) Evaluate each group
    group_results: Dict[int, Set[str]] = {}
    for gid, filters in group_filters.items():
        group_set: Set[str] = set()
        first = True

        for f in filters:
            # Determine tables and columns to search
            target_tables = all_tables if f.table == "*" else [f.table]
            matching: Set[str] = set()

            for tbl in target_tables:
                cols = get_columns(conn, tbl)
                target_cols = cols if f.field == "*" else [f.field]

                # Build SQL clauses
                clauses: List[str] = []
                params:  List[str] = []
                for col in target_cols:
                    if f.operator == "contains":
                        clauses.append(f"{col} LIKE ?")
                        params.append(f"%{f.value}%")
                    elif f.operator == "begins":
                        clauses.append(f"{col} LIKE ?")
                        params.append(f"{f.value}%")
                    elif f.operator == "ends":
                        clauses.append(f"{col} LIKE ?")
                        params.append(f"%{f.value}")
                    elif f.operator == "equals":
                        clauses.append(f"{col} = ?")
                        params.append(f.value)
                    else:
                        raise ValueError(f"Unsupported operator: {f.operator}")

                where = " OR ".join(clauses)
                sql   = f"SELECT {uuid_col} FROM {tbl}"
                if where:
                    sql += f" WHERE {where}"

                rows = conn.execute(sql, params).fetchall()
                matching.update(r[0] for r in rows)

            # Combine into this group's set, handling first filter inversion
            if first:
                if f.logic in ("nand", "nor"):
                    # build universe of all UUIDs in target_tables
                    universe: Set[str] = set()
                    for tbl in target_tables:
                        uni_rows = conn.execute(f"SELECT {uuid_col} FROM {tbl}").fetchall()
                        universe.update(r[0] for r in uni_rows)
                    group_set = universe - matching
                else:
                    group_set = matching
                first = False
            else:
                if f.logic == "and":
                    group_set &= matching
                elif f.logic == "or":
                    group_set |= matching
                elif f.logic in ("nand", "nor"):
                    group_set -= matching
                else:
                    raise ValueError(f"Unsupported logic: {f.logic}")

        group_results[gid] = group_set

    # 3) Combine all groups via group_logic
    final_set: Set[str] = set()
    if pkg.group_logic:
        first_gl = True
        for gl in pkg.group_logic:
            # build subset for this group_logic entry
            subset: Set[str] = set()
            for i, g in enumerate(gl.groups):
                s = group_results.get(g, set())
                if i == 0:
                    subset = s.copy()
                else:
                    if gl.logic == "and":
                        subset &= s
                    elif gl.logic == "or":
                        subset |= s
                    elif gl.logic in ("nand", "nor"):
                        subset -= s
                    else:
                        raise ValueError(f"Unsupported group_logic: {gl.logic}")

            # merge into final_set
            if first_gl:
                final_set = subset
                first_gl = False
            else:
                if gl.logic == "and":
                    final_set &= subset
                elif gl.logic == "or":
                    final_set |= subset
                elif gl.logic in ("nand", "nor"):
                    final_set -= subset
                else:
                    raise ValueError(f"Unsupported group_logic: {gl.logic}")
    else:
        # default: AND across all groups
        first_grp = True
        for s in group_results.values():
            if first_grp:
                final_set = s.copy()
                first_grp = False
            else:
                final_set &= s

    conn.close()
    return SearchResultPackage(
        filters     = pkg.filters,
        group_logic = pkg.group_logic,
        uuids       = list(final_set)
    )
