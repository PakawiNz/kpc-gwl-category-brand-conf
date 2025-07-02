import re
import sqlite3
import csv
from typing import List, Tuple

# --- Database Configuration ---
DB_NAME = "data/articles.db"
MISSING_RESULT_FILE = "data/articles.missing.csv"
RAW_MISSING_OUTPUT_FILE = "data/articles.missing.raw.csv"


def get_db_connection():
    """Establishes and returns a connection to the SQLite database."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn


def create_article_config_table(table_name: str):
    """Creates the '{table_name}' table if it doesn't already exist."""
    conn = get_db_connection()
    try:
        with conn:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    sku TEXT PRIMARY KEY,
                    category_id TEXT NOT NULL,
                    brand_id TEXT NOT NULL,
                    earnable BOOLEAN,
                    burnable BOOLEAN,
                    earnRate NUMBER
                )
            """
            )
        print(f"Table '{table_name}' is ready. ✅")
        return True
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    finally:
        conn.close()


def upsert_article(sku: str, category_id: str, brand_id: str):
    """
    Inserts a new article record or replaces it if the SKU already exists.
    This is an 'upsert' operation.
    """
    sql = """
        INSERT INTO article_config (sku, category_id, brand_id)
        VALUES (?, ?, ?)
        ON CONFLICT(sku) DO UPDATE SET
            category_id=excluded.category_id,
            brand_id=excluded.brand_id
    """
    conn = get_db_connection()
    try:
        with conn:
            conn.execute(sql, (sku, category_id, brand_id))
    except sqlite3.Error as e:
        print(f"Failed to upsert SKU {sku}: {e}")
    else:
        print(f"Successfully upsert SKU {sku}")
    finally:
        conn.close()


def upsert_articles(articles: List[Tuple[str, str, str]], table_name: str):
    """
    Inserts or replaces a chunk of article records in a single transaction.

    Args:
        articles: A list of tuples, where each tuple contains
                  (sku, category_id, brand_id).
    """
    # Check if there are any articles to process
    if not articles:
        print("No articles provided to upsert.")
        return

    sql = f"""
        INSERT INTO {table_name} (sku, category_id, brand_id)
        VALUES (?, ?, ?)
        ON CONFLICT(sku) DO UPDATE SET
            category_id=excluded.category_id,
            brand_id=excluded.brand_id;
    """

    conn = get_db_connection()
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


# --- Normalization Functions (Placeholders) ---


def normalize_article_id(id_str: str | None) -> str:
    """Normalizes the article ID by trimming and removing hyphens/underscores."""
    if id_str is None:
        return ""
    # Use re.sub() to replace all occurrences of '-' or '_'
    return re.sub(r"[-_]", "", str(id_str).strip())


def normalize_brand_id(id_str: str | None) -> str:
    """Normalizes the brand ID by trimming and converting to uppercase."""
    if id_str is None:
        return ""
    return str(id_str).strip().upper()


def normalize_category_id(id_str: str | None) -> str:
    """Normalizes the category ID by trimming, lowercasing, and removing hyphens/underscores."""
    if id_str is None:
        return ""
    # Use re.sub() to replace all occurrences of '-' or '_'
    return re.sub(r"[-_]", "", str(id_str).strip().lower())


# --- Main Logic ---


def article_to_db(files: list[str], table_name: str):
    """
    Main function to read article data from CSV files and load into the database.

    Args:
        files: A list of CSV file paths to process.
    """
    # 1. Ensure the database table exists
    assert create_article_config_table(table_name)

    chunk = []

    # 2. Iterate through each file and process its rows
    for file_path in files:
        print(f"\nProcessing file: {file_path}...")
        try:
            with open(file_path, mode="r", encoding="utf-8") as csvfile:
                # Use DictReader to read CSV rows as dictionaries
                reader = csv.DictReader(csvfile, delimiter="|")
                for row in reader:
                    # 3. Normalize data from each row
                    sku = normalize_article_id(row.get("MATNR", ""))
                    category_id = normalize_category_id(row.get("MATKL", ""))[:3]
                    brand_id = normalize_brand_id(row.get("BRAND_ID", "")) or "000"

                    # Skip if essential data is missing
                    if not sku or not category_id or not brand_id:
                        print(f"Skipping row due to missing: {row}")
                        continue

                    # 4. Upsert the processed data into the database
                    chunk.append([sku, category_id, brand_id])

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


def count_table_rows(table_name: str) -> int:
    """
    Counts the number of rows in the specified table.

    Args:
        table_name: Name of the table to count rows from

    Returns:
        int: Number of rows in the table
    """
    sql = f"SELECT COUNT(*) as count FROM {table_name}"

    conn = get_db_connection()
    try:
        with conn:
            result = conn.execute(sql).fetchone()
            count = result["count"]
            print(f"Table '{table_name}' has {count:,} rows")
            return count
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return 0
    finally:
        conn.close()


def find_missing_articles(actual_table: str, expected_table: str):
    """
    Finds articles that exist in the expected table but are missing from the actual table.

    Args:
        actual_table: Name of the table containing current articles
        expected_table: Name of the table containing expected articles

    Returns:
        None. Prints missing articles to console.
    """

    count_table_rows(actual_table)
    count_table_rows(expected_table)

    sql = f"""
        SELECT e.sku, e.category_id, e.brand_id 
        FROM {expected_table} e
        LEFT JOIN {actual_table} a ON e.sku = a.sku
        WHERE a.sku IS NULL
    """

    conn = get_db_connection()
    try:
        with conn:
            missing = conn.execute(sql).fetchall()

        if not missing:
            print("✅ No missing articles found!")
            return

        print(f"❌ Found {len(missing)} missing articles:")

        with open(MISSING_RESULT_FILE, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["sku", "category_id", "brand_id"])
            for row in missing:
                writer.writerow([row["sku"], row["category_id"], row["brand_id"]])

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    finally:
        conn.close()


def write_raw_record_of_missing_article(source_files: list[str]):
    """
    Finds the original, unprocessed lines for SKUs identified as missing.

    It reads a list of missing SKUs, searches the source files for those SKUs,
    and writes the exact original line to a new output file.

    Args:
        source_files: A list of raw data CSV file paths to search through.
    """

    # --- Step 1: Read the list of missing SKUs into a set for fast lookups ---
    try:
        with open(MISSING_RESULT_FILE, "r", newline="") as f:
            reader = csv.DictReader(f)
            missing_skus = {row["sku"] for row in reader}
    except FileNotFoundError:
        print(
            f"❌ Missing SKUs file not found: '{MISSING_RESULT_FILE}'. Please run 'find_missing_articles' first."
        )
        return

    if not missing_skus:
        print("✅ No missing SKUs to process.")
        return

    print(f"Searching for the original lines of {len(missing_skus)} missing SKUs...")

    # --- Step 2: Open the output file and search through the source files line by line ---
    found_count = 0
    header_written = False
    try:
        with open(RAW_MISSING_OUTPUT_FILE, "w", encoding="utf-8") as outfile:
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

                        try:
                            # Determine the index of the SKU column
                            columns = header_line.strip().split("|")
                            matnr_index = columns.index("MATNR")
                        except ValueError:
                            print(
                                f"Warning: 'MATNR' column not found in '{file_path}', skipping."
                            )
                            continue

                        # Process the rest of the file
                        for line in infile:
                            try:
                                raw_sku = line.strip().split("|")[matnr_index]
                                normalized_sku = normalize_article_id(raw_sku)

                                if normalized_sku in missing_skus:
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
        print(f"❌ Error writing to output file '{RAW_MISSING_OUTPUT_FILE}': {e}")

    print(
        f"\n✅ Process complete. Found and wrote {found_count} original lines to '{RAW_MISSING_OUTPUT_FILE}'."
    )


# --- Example Usage ---
if __name__ == "__main__":
    FILES = [
        "/Users/pakawin_m/workspace/kpc-gwl-category-brand-conf/data/kpg-sap-s3-outbound-prod/S4P_ARTICLE_FULL_20250616_144943_1_4.CSV",
        "/Users/pakawin_m/workspace/kpc-gwl-category-brand-conf/data/kpg-sap-s3-outbound-prod/S4P_ARTICLE_FULL_20250616_144943_2_4.CSV",
        "/Users/pakawin_m/workspace/kpc-gwl-category-brand-conf/data/kpg-sap-s3-outbound-prod/S4P_ARTICLE_FULL_20250616_144943_3_4.CSV",
        "/Users/pakawin_m/workspace/kpc-gwl-category-brand-conf/data/kpg-sap-s3-outbound-prod/S4P_ARTICLE_FULL_20250616_144943_4_4.CSV",
        "/Users/pakawin_m/workspace/kpc-gwl-category-brand-conf/data/kpg-sap-s3-outbound-prod/S4P_ARTICLE_DELTA_20250617_060009_1_1.CSV",
        "/Users/pakawin_m/workspace/kpc-gwl-category-brand-conf/data/kpg-sap-s3-outbound-prod/S4P_ARTICLE_DELTA_20250627_060003_1_1.CSV",
        # "/Users/pakawin_m/workspace/kpc-gwl-category-brand-conf/data/kpg-sap-s3-outbound-prod/S4P_ARTICLE_DELTA_20250627_161325_1_1.CSV"
        "/Users/pakawin_m/workspace/kpc-gwl-category-brand-conf/data/kpg-sap-s3-outbound-prod/S4P_ARTICLE_DELTA_20250628_060004_1_1.CSV",
        "/Users/pakawin_m/workspace/kpc-gwl-category-brand-conf/data/kpg-sap-s3-outbound-prod/S4P_ARTICLE_DELTA_20250629_060007_1_1.CSV",
        "/Users/pakawin_m/workspace/kpc-gwl-category-brand-conf/data/kpg-sap-s3-outbound-prod/S4P_ARTICLE_DELTA_20250630_060002_1_1.CSV",
        # "/Users/pakawin_m/workspace/kpc-gwl-category-brand-conf/data/kpg-sap-s3-outbound-prod/S4P_ARTICLE_DELTA_20250701_060007_1_1.CSV",
        # "/Users/pakawin_m/workspace/kpc-gwl-category-brand-conf/data/kpg-sap-s3-outbound-prod/S4P_ARTICLE_DELTA_20250702_060008_1_1.CSV",
    ]

    article_to_db(FILES, 'article_config_prod_as_of_20250702_1627')

    REFS_FILES = [
        "/Users/pakawin_m/workspace/kpc-gwl-category-brand-conf/data/kpg-sap-s3-outbound-prod/S4P_ARTICLE_FULL_20250702_150409_1_4.CSV",
        "/Users/pakawin_m/workspace/kpc-gwl-category-brand-conf/data/kpg-sap-s3-outbound-prod/S4P_ARTICLE_FULL_20250702_150409_2_4.CSV",
        "/Users/pakawin_m/workspace/kpc-gwl-category-brand-conf/data/kpg-sap-s3-outbound-prod/S4P_ARTICLE_FULL_20250702_150409_3_4.CSV",
        "/Users/pakawin_m/workspace/kpc-gwl-category-brand-conf/data/kpg-sap-s3-outbound-prod/S4P_ARTICLE_FULL_20250702_150409_4_4.CSV",
    ]

    article_to_db(REFS_FILES, "article_config_20250702")

    find_missing_articles("article_config_prod_as_of_20250702_1627", "article_config_20250702")

    write_raw_record_of_missing_article(REFS_FILES)
