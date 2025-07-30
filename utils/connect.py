# utils/connect.py

import sqlite3
import psycopg2
import mysql.connector

from utils.config import get_settings, get_primary_identifier
from tools.schema_introspect import get_tables, validate_primary_identifier, get_columns
from tools.flagger import Flagger

def get_connection():
    settings = get_settings()
    db_type = settings.get("database_type", "sqlite").lower()
    conn_info = settings.get("connection", {})

    if db_type == "sqlite":
        return sqlite3.connect(conn_info.get("path", "data/database.db"))
    elif db_type == "postgres":
        return psycopg2.connect(
            host=conn_info["host"],
            port=conn_info.get("port", 5432),
            user=conn_info["user"],
            password=conn_info["password"],
            dbname=conn_info["database"]
        )
    elif db_type == "mysql":
        return mysql.connector.connect(
            host=conn_info["host"],
            port=conn_info.get("port", 3306),
            user=conn_info["user"],
            password=conn_info["password"],
            database=conn_info["database"]
        )
    else:
        raise ValueError(f"Unsupported database type: {db_type}")

def validate_all_tables(conn, db_type: str, flagger: Flagger):
    identifier = get_primary_identifier()
    for table in get_tables(conn, db_type):
        validate_primary_identifier(table, conn, db_type, identifier, flagger)

def get_columns(conn, table: str, db_type: str) -> list[str]:
    # This is now forwarded to schema_introspect, kept for backward compatibility
    from tools.schema_introspect import get_columns as get_cols
    return get_cols(conn, table, db_type)
