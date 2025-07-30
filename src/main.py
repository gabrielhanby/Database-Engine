import logging
from pathlib import Path
from tools.search       import search_records
from tools.read         import read_records
from tools.read_format  import format_search_results
from tools.flagger      import Flagger
from utils.config       import load_settings

def main():
    logging.basicConfig(level=logging.DEBUG, format='%(message)s')
    logging.debug("[DEBUG] Starting main pipeline")

    settings = load_settings(
        Path(__file__).parent.parent / 'settings' / 'database_settings.json'
    )
    pkg     = settings.build_search_package()
    conn    = settings.get_connection()
    db_type = settings.get_db_type()
    flagger = Flagger()

    logging.debug(f"[DEBUG] Search package: {pkg}")
    matches = search_records(pkg, conn, db_type, flagger)
    records = read_records(pkg, conn, db_type)
    output  = format_search_results(matches, records)

    print(output)

if __name__ == '__main__':
    main()
