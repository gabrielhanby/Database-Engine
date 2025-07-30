# utils/config.py

import json
from pathlib import Path

_settings = None

def load_settings():
    global _settings
    if _settings is None:
        path = Path(__file__).parent.parent / "settings" / "database_settings.json"
        with open(path, "r") as f:
            _settings = json.load(f)
    return _settings

def get_settings():
    return load_settings()

def get_primary_identifier():
    ident = load_settings().get("primary_identifier", "costumerUUID")
    if ident.lower() in ("null", "none", "default"):
        return "costumerUUID"
    return ident

def get_db_path():
    return load_settings().get("connection", {}).get("path", "data/database.db")
