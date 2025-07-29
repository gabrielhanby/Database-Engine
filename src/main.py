# src/main.py

import json
from datetime import datetime
from pathlib import Path

from utils.types        import SearchPackageFlat, ReadPackage, ReadFormatPackage
from tools.search      import search
from tools.read        import read_records
from tools.read_format import format_search_results

def main():
    # 1) build the filter package
    search_config = {
        "filters": [
            {
                "table":    "Notes",
                "field":    "body",
                "operator": "contains",
                "value":    "-GH-",
                "logic":    "and",
                "index_by": "\n",
                "position": "none"
            }
        ]
    }

    # 2) load & validate
    pkg = SearchPackageFlat.from_dict(search_config)

    # 3) run the search
    result_pkg = search(pkg)

    # 4) read full records for each table in the filters
    tables = {
        f.table for f in result_pkg.filters
        if f.table != "*"
    }
    all_records = []
    for table in tables:
        read_pkg = ReadPackage(table=table, uuids=result_pkg.uuids)
        all_records.extend(read_records(read_pkg))

    # 5) format the results
    fmt_pkg = ReadFormatPackage(
        filters     = result_pkg.filters,
        group_logic = result_pkg.group_logic,
        records     = all_records
    )
    report = format_search_results(fmt_pkg)

    # 6) write to file
    timestamp   = datetime.now().strftime("%Y%m%dT%H%M")
    out_dir     = Path(__file__).parent.parent / "output"
    out_dir.mkdir(exist_ok=True)
    report_path = out_dir / f"report_{timestamp}.txt"
    report_path.write_text(report, encoding="utf-8")

    print(f"Report written to {report_path}")

if __name__ == "__main__":
    main()
