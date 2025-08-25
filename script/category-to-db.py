import datetime
import os
import sqlite3
import csv
from typing import List, Tuple
from file_listing import FileType, list_files_in_folder
from abc_normalize import normalize_category_id, normalize_text
from abc_utils import (
    get_db_connection,
    create_imported_logs,
    insert_imported_logs_if_not_exists,
    summarize_by_imported_at,
    sync_s3,
)


# --- Database Configuration ---
SOURCE_FOLDER = "/Users/pakawin_m/workspace/kpc-gwl-category-brand-conf/data/kpg-sap-s3-outbound-prod/"
FILE_TYPE = FileType.CATEGORY
DB_NAME = "data/categories.db"
TABLE_NAME = "categories"
IMOPORTED_LOG_TABLE_NAME = "categories_imported_log"
OUTPUT_FILE_NAME = "S4P_CATEGORY_FULL_{today}_999999_1_1.CSV"


def create_category_config_table(table_name: str):
    """Creates the '{table_name}' table if it doesn't already exist."""
    conn = get_db_connection(DB_NAME)
    try:
        with conn:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    category_id TEXT PRIMARY KEY,
                    category_name TEXT NOT NULL,
                    imported_at DATE NOT NULL
                )
            """
            )
        print(f"Table '{table_name}' is ready. ✅")
        return True
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    finally:
        conn.close()


def upsert_categories(
    categories: List[Tuple[str, str, datetime.date]], table_name: str
):
    """
    Inserts or replaces a chunk of category records in a single transaction.

    Args:
        categories: A list of tuples, where each tuple contains
                  (category_id, category_name, imported_at).
    """
    # Check if there are any categories to process
    if not categories:
        print("No categories provided to upsert.")
        return

    sql = f"""
        INSERT INTO {table_name} (category_id, category_name, imported_at)
        VALUES (?, ?, ?)
        ON CONFLICT(category_id) DO UPDATE SET
            category_id=excluded.category_id,
            category_name=excluded.category_name
        WHERE {table_name}.category_name IS NOT excluded.category_name;
    """

    conn = get_db_connection(DB_NAME)
    try:
        # The 'with conn:' block automatically begins and commits/rollbacks a transaction.
        with conn:
            # Use executemany to efficiently process the entire list.
            conn.executemany(sql, categories)
    except sqlite3.Error as e:
        print(f"Failed to upsert a chunk of {len(categories)} categories: {e}")
    else:
        print(f"Successfully upserted/updated {len(categories)} categories. ✅")
    finally:
        conn.close()


# --- Main Logic ---


def category_to_db(file_path: str, date: datetime.date, table_name: str):
    """
    Main function to read category data from CSV files and load into the database.

    Args:
        files: A list of CSV file paths to process.
    """
    # 1. Ensure the database table exists
    assert create_category_config_table(table_name)

    chunk = []

    # 2. Iterate through each file and process its rows
    print(f"\nProcessing file: {file_path}...")
    try:
        with open(file_path, mode="r", encoding="utf-8") as csvfile:
            # Use DictReader to read CSV rows as dictionaries
            reader = csv.DictReader(csvfile, delimiter="|")
            for row in reader:
                # 3. Normalize data from each row
                category_id = normalize_category_id(row.get("CLASS", ""))
                category_text = normalize_text(row.get("KSCHG", ""))

                # Skip if essential data is missing
                if not category_id or not category_text:
                    print(f"Skipping row due to missing: {row}")
                    continue

                # 4. Upsert the processed data into the database
                chunk.append([category_id, category_text, date])

                if len(chunk) == 1000:
                    upsert_categories(chunk, table_name)
                    chunk = []

        print(f"Successfully processed {file_path}. 🎉")
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
    except Exception as e:
        print(f"An error occurred while processing {file_path}: {e}")

    if len(chunk):
        upsert_categories(chunk, table_name)
        chunk = []


def write_raw_record_of_delta_category(
    source_files: list[str], table_name: str, last_run: datetime.date, to_file: str
):
    """
    Finds the original, unprocessed lines for SKUs identified as missing.

    It reads a list of missing SKUs, searches the source files for those SKUs,
    and writes the exact original line to a new output file.

    Args:
        source_files: A list of raw data CSV file paths to search through.
    """

    # --- Step 1: Read the list of missing category_ids into a set for fast lookups ---
    try:
        conn = get_db_connection(DB_NAME)
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT category_id
            FROM {table_name}
            WHERE imported_at > '{last_run.isoformat()}'
        """
        )
        missing_categories = {row["category_id"] for row in cursor.fetchall()}
        conn.close()
    except sqlite3.Error as e:
        print(f"❌ Database error while fetching missing category_ids: {e}")
        return

    if not missing_categories:
        print("✅ No missing category_ids to process.")
        return

    print(
        f"Searching for the original lines of {len(missing_categories)} missing category_ids..."
    )

    # --- Step 2: Open the output file and search through the source files line by line ---
    found_count = 0
    header_written = False
    try:
        with open(to_file, "w", encoding="utf-8") as outfile:
            for file_path in source_files:
                try:
                    with open(file_path, "r", encoding="utf-8") as infile:
                        # Read the header to find the column index for "MATNR"
                        header_line = next(infile, None)
                        if not header_line:
                            continue  # Skip empty files

                        if not header_written:
                            outfile.write(header_line)
                            header_written = True

                        # Process the rest of the file
                        for line in infile:
                            try:
                                raw_category_id = line.strip().split("|")[0]
                                normalized_category_id = normalize_category_id(
                                    raw_category_id
                                )

                                if normalized_category_id in missing_categories:
                                    missing_categories.remove(normalized_category_id)
                                    outfile.write(
                                        line
                                    )  # Write the original, unmodified line
                                    found_count += 1
                            except IndexError:
                                # This will skip malformed lines that don't have enough columns
                                continue
                except FileNotFoundError:
                    print(f"Warning: Source file not found, skipping: {file_path}")
    except IOError as e:
        print(f"❌ Error writing to output file '{to_file}': {e}")

    print(
        f"\n✅ Process complete. Found and wrote {found_count} original lines to '{to_file}'."
    )


# --- Example Usage ---
if __name__ == "__main__":
    sync_s3()
    create_imported_logs(DB_NAME, IMOPORTED_LOG_TABLE_NAME)
    create_category_config_table(TABLE_NAME)

    all_files = list_files_in_folder(SOURCE_FOLDER, no_filter=True)
    category_files = [f for f in all_files if f.file_type is FILE_TYPE]
    category_files.sort(key=lambda k: k.datetime)

    last_run = None
    for category_file in category_files:
        date = datetime.datetime.strptime(category_file.datetime[:8], "%Y%m%d").date()
        if category_file.datetime[8:] == "999999":
            last_run = date
        if insert_imported_logs_if_not_exists(
            DB_NAME, IMOPORTED_LOG_TABLE_NAME, str(category_file.path)
        ):
            category_to_db(category_file.path, date, TABLE_NAME)

    summarize_by_imported_at(DB_NAME, TABLE_NAME)

    last_run = last_run or date
    print("last run", last_run)
    assert last_run
    today = datetime.date.today().isoformat().replace("-", "")
    category_files.sort(key=lambda k: k.datetime, reverse=True)
    write_raw_record_of_delta_category(
        [f.path for f in category_files],
        TABLE_NAME,
        datetime.date(2025, 8, 1),
        os.path.join(SOURCE_FOLDER, OUTPUT_FILE_NAME.format(today=today)),
    )
