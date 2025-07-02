import datetime
import os
import sqlite3
import csv
from typing import List, Tuple
from file_listing import FileType, list_files_in_folder
from abc_normalize import normalize_brand_id, normalize_text


def get_db_connection(db_name):
    """Establishes and returns a connection to the SQLite database."""
    conn = sqlite3.connect(db_name)
    conn.row_factory = sqlite3.Row
    return conn


def create_imported_logs(db_name: str, table_name: str):
    """Creates the '{table_name}' table if it doesn't already exist."""
    conn = get_db_connection(db_name)
    try:
        with conn:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (file_path TEXT PRIMARY KEY)
            """
            )
        print(f"Table '{table_name}' is ready. ✅")
        return True
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    finally:
        conn.close()


def insert_imported_logs_if_not_exists(db_name: str, table_name: str, file_path: str):
    """Inserts a file path into the imported logs table if it doesn't already exist.

    Args:
        table_name: Name of the imported logs table
        file_path: Path of the file to log

    Returns:
        bool: True if inserted successfully, False if already exists or error occurs
    """
    conn = get_db_connection(db_name)
    try:
        with conn:
            cursor = conn.cursor()
            cursor.execute(
                f"""
                INSERT OR IGNORE INTO {table_name} (file_path)
                VALUES (?)
            """,
                (file_path,),
            )
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return False
    finally:
        conn.close()


def summarize_by_imported_at(db_name: str, table_name: str) -> None:
    """Returns a summary of brand counts grouped by imported_at date.

    Args:
        db_name: Name of the SQLite database file
        table_name: Name of the table to summarize
    """
    conn = get_db_connection(db_name)
    try:
        with conn:
            cursor = conn.cursor()
            cursor.execute(
                f"""
                SELECT COUNT(1) as count, imported_at 
                FROM {table_name} 
                GROUP BY imported_at
                ORDER BY imported_at DESC
            """
            )
            results = cursor.fetchall()
            print("\nSummary by Import Date:")
            print("----------------------")
            print("ImportDate | Count")
            print("----------------------")
            for count, date in results:
                print(f"{date} | {count:5d}")
            print("----------------------")
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    finally:
        conn.close()
