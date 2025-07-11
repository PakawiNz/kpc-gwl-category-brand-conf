import datetime
import os
import sqlite3
import csv
from typing import List, Tuple
from file_listing import FileType, list_files_in_folder
from abc_normalize import (
    normalize_article_id,
    normalize_brand_id,
    normalize_category_id,
    normalize_text,
)
from abc_utils import (
    get_db_connection,
    create_imported_logs,
    insert_imported_logs_if_not_exists,
    summarize_by_imported_at,
    sync_s3,
)


# --- Configuration ---
SOURCE_FOLDER = "/Users/pakawin_m/workspace/kpc-gwl-category-brand-conf/data/kpg-sap-s3-outbound-prod/"
FILE_TYPE = FileType.ARTICLE
DB_NAME = "data/articles.db"
TABLE_NAME = "articles"
IMOPORTED_LOG_TABLE_NAME = "articles_imported_log"
OUTPUT_FILE_NAME = "S4P_ARTICLE_FULL_{today}_999999_1_1"


def create_article_config_table(table_name: str):
    """Creates the '{table_name}' table if it doesn't already exist."""
    conn = get_db_connection(DB_NAME)
    try:
        with conn:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    article_id TEXT PRIMARY KEY,
                    article_name TEXT NOT NULL,
                    category_id TEXT NOT NULL,
                    brand_id TEXT NOT NULL,
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


def upsert_articles(
    articles: List[Tuple[str, str, str, str, datetime.date]], table_name: str
):
    """
    Inserts or replaces a chunk of article records in a single transaction.

    Args:
        articles: A list of tuples, where each tuple contains
                  (article_id, article_name, category_id, brand_id, imported_at).
    """
    # Check if there are any articles to process
    if not articles:
        print("No articles provided to upsert.")
        return

    sql = f"""
        INSERT INTO {table_name} (article_id, article_name, category_id, brand_id, imported_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(article_id) DO UPDATE SET
            article_id=excluded.article_id,
            article_name=excluded.article_name,
            category_id=excluded.category_id,
            brand_id=excluded.brand_id
        WHERE {table_name}.article_id IS NOT excluded.article_id
        OR {table_name}.article_name IS NOT excluded.article_name
        OR {table_name}.category_id IS NOT excluded.category_id
        OR {table_name}.brand_id IS NOT excluded.brand_id;
    """

    conn = get_db_connection(DB_NAME)
    try:
        # The 'with conn:' block automatically begins and commits/rollbacks a transaction.
        with conn:
            # Use executemany to efficiently process the entire list.
            conn.executemany(sql, articles)
    except sqlite3.Error as e:
        print(f"Failed to upsert a chunk of {len(articles)} articles: {e}")
    else:
        print(f"Successfully upserted/updated {len(articles)} articles. ✅")
    finally:
        conn.close()


# --- Main Logic ---


def article_to_db(file_path: str, date: datetime.date, table_name: str):
    """
    Main function to read article data from CSV files and load into the database.

    Args:
        files: A list of CSV file paths to process.
    """
    # 1. Ensure the database table exists
    assert create_article_config_table(table_name)

    chunk = []

    # 2. Iterate through each file and process its rows
    print(f"\nProcessing file: {file_path}...")
    try:
        with open(file_path, mode="r", encoding="utf-8") as csvfile:
            # Use DictReader to read CSV rows as dictionaries
            reader = csv.DictReader(csvfile, delimiter="|")
            for row in reader:
                # 3. Normalize data from each row
                article_id = normalize_article_id(row.get("MATNR", ""))
                article_text = normalize_text(row.get("MAKTX", ""))
                category_id = normalize_category_id(row.get("MATKL", ""))[:3]
                brand_id = normalize_brand_id(row.get("BRAND_ID", "")) or "000"

                # Skip if essential data is missing
                if not article_id or not category_id or not brand_id:
                    print(f"Skipping row due to missing: {row}")
                    continue

                # 4. Upsert the processed data into the database
                chunk.append((article_id, article_text, category_id, brand_id, date))

                if len(chunk) == 1000:
                    upsert_articles(chunk, table_name)
                    chunk = []

        print(f"Successfully processed {file_path}. 🎉")
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
    except Exception as e:
        print(f"An error occurred while processing {file_path}: {e}")

    if len(chunk):
        upsert_articles(chunk, table_name)
        chunk = []


def write_raw_record_of_delta_article(
    source_files: list[str], table_name: str, last_run: datetime.date, to_file: str
):
    """
    Finds the original, unprocessed lines for SKUs identified as missing.

    It reads a list of missing SKUs, searches the source files for those SKUs,
    and writes the exact original line to a new output file.

    Args:
        source_files: A list of raw data CSV file paths to search through.
    """

    # --- Step 1: Read the list of missing article_ids into a set for fast lookups ---
    try:
        conn = get_db_connection(DB_NAME)
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT article_id
            FROM {table_name}
            WHERE imported_at > '{last_run.isoformat()}'
        """
        )
        missing_articles = {row["article_id"] for row in cursor.fetchall()}
        conn.close()
    except sqlite3.Error as e:
        print(f"❌ Database error while fetching missing article_ids: {e}")
        return

    if not missing_articles:
        print("✅ No missing article_ids to process.")
        return

    print(
        f"Searching for the original lines of {len(missing_articles)} missing article_ids..."
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
                                raw_article_id = line.strip().split("|")[0]
                                normalized_article_id = normalize_article_id(
                                    raw_article_id
                                )

                                if normalized_article_id in missing_articles:
                                    missing_articles.remove(normalized_article_id)
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


def write_raw_record_of_match_article(
    source_files: list[str],
    table_name: str,
    cat_brn: List[Tuple[str, str]],
    to_file: str,
):
    """
    Finds the original, unprocessed lines for SKUs identified as missing.

    It reads a list of missing SKUs, searches the source files for those SKUs,
    and writes the exact original line to a new output file.

    Args:
        source_files: A list of raw data CSV file paths to search through.
    """

    # --- Step 1: Read the list of missing article_ids into a set for fast lookups ---
    try:
        conn = get_db_connection(DB_NAME)
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT article_id
            FROM {table_name}
            WHERE (category_id, brand_id) IN (VALUES {','.join(['(?,?)' for _ in cat_brn])})
            """,
            [
                param
                for pair in cat_brn
                for param in [
                    normalize_category_id(pair[0]),
                    normalize_brand_id(pair[1]),
                ]
            ],
        )
        matched_articles = {row["article_id"] for row in cursor.fetchall()}
        conn.close()
    except sqlite3.Error as e:
        print(f"❌ Database error while fetching matched article_ids: {e}")
        return

    if not matched_articles:
        print("✅ No matched article_ids to process.")
        return

    print(
        f"Searching for the original lines of {len(matched_articles)} matched article_ids..."
    )

    # --- Step 2: Open the output file and search through the source files line by line ---
    found_count = 0
    header_written = False
    try:
        with open(to_file, "w", encoding="utf-8") as outfile:
            for file_path in source_files:
                if not matched_articles:
                    break
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
                                raw_article_id = line.strip().split("|")[0]
                                normalized_article_id = normalize_article_id(
                                    raw_article_id
                                )

                                if normalized_article_id in matched_articles:
                                    matched_articles.remove(normalized_article_id)
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
    create_article_config_table(TABLE_NAME)

    all_files = list_files_in_folder(SOURCE_FOLDER, no_filter=True)
    article_files = [f for f in all_files if f.file_type is FILE_TYPE]
    article_files.sort(key=lambda k: k.datetime)

    last_run = None
    for article_file in article_files:
        date = datetime.datetime.strptime(article_file.datetime[:8], "%Y%m%d").date()
        if article_file.datetime[8:] == "999999":
            last_run = date
        if insert_imported_logs_if_not_exists(
            DB_NAME, IMOPORTED_LOG_TABLE_NAME, str(article_file.path)
        ):
            article_to_db(article_file.path, date, TABLE_NAME)

    summarize_by_imported_at(DB_NAME, TABLE_NAME)

    last_run = last_run or date
    print("last run", last_run)
    today = datetime.date.today().isoformat().replace("-", "")
    article_files.sort(key=lambda k: k.datetime, reverse=True)
    write_raw_record_of_delta_article(
        [f.path for f in article_files],
        TABLE_NAME,
        last_run,
        os.path.join(SOURCE_FOLDER, OUTPUT_FILE_NAME.format(today=today)),
    )
    # write_raw_record_of_match_article(
    #     [f.path for f in article_files],
    #     TABLE_NAME,
    #     [("201", "CHN"), ("202", "CHN")],
    #     "data/ARTICLE_MATCH_{now}.csv".format(now=datetime.datetime.now().strftime('%Y%m%d_%H%M%s')),
    # )
