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

def get_db_path():
    return load_settings().get("db_path")

def get_uuid_field():
    return load_settings().get("uuid_field")
