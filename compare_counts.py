from collections import defaultdict
import csv
import json
import os

def read_counts_to_dict(filepath: str) -> dict:
    """
    Reads a pipe-delimited CSV file into a dictionary.
    The key is a tuple of (channel, pair), and the value is the count.
    Keys are normalized to uppercase to ensure case-insensitive comparison.
    """
    counts = {}
    if not os.path.exists(filepath):
        print(f"Error: File not found at {filepath}")
        return counts

    with open(filepath, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, delimiter='|')
        for i, row in enumerate(reader):
            if len(row) == 3:
                channel, pair, count_str = row
                # Normalize the key to be case-insensitive and stripped of whitespace
                key = (channel.strip().upper(), pair.strip().upper())
                try:
                    counts[key] = int(count_str.strip())
                except ValueError:
                    print(f"Warning: Could not parse count '{count_str}' on line {i+1} in {filepath}. Skipping.")
            else:
                print(f"Warning: Malformed row on line {i+1} in {filepath}: {row}. Skipping.")
    return counts

def compare_counts(db_file: str, sap_file: str):
    """Compares counts from two files and prints a detailed report of differences."""
    print(f"Comparing '{db_file}' and '{sap_file}'...\n")

    db_counts = read_counts_to_dict(db_file)
    sap_counts = read_counts_to_dict(sap_file)

    db_keys = set(db_counts.keys())
    sap_keys = set(sap_counts.keys())

    common_keys = db_keys.intersection(sap_keys)
    only_in_db_keys = db_keys.difference(sap_keys)
    only_in_sap_keys = sap_keys.difference(db_keys)

    print("--- Mismatched Counts (found in both files) ---")
    mismatches_found = False
    diff_count = defaultdict(int)
    db_lower_diff = []
    for key in sorted(list(common_keys)):
        if db_counts[key] != sap_counts[key] and key[1].isdigit():
            print(f"  - Key {key}: DB count = {db_counts[key]}, SAP count = {sap_counts[key]}")
            diff_count[key[0]] += abs(db_counts[key] - sap_counts[key])
            if (db_counts[key] < sap_counts[key]):
                db_lower_diff.append(f"  - Key {key}: DB count = {db_counts[key]}, SAP count = {sap_counts[key]}")
            mismatches_found = True
    if not mismatches_found:
        print("  (None)")

    print("\n--- Records Only in DB File ---")
    if not only_in_db_keys:
        print("  (None)")
    for key in sorted(list(only_in_db_keys)):
        if key[1].isdigit():
            print(f"  - Key {key}: Count = {db_counts[key]}")

    print("\n--- Records Only in SAP File ---")
    if not only_in_sap_keys:
        print("  (None)")
    for key in sorted(list(only_in_sap_keys)):
        if key[1].isdigit():
            print(f"  - Key {key}: Count = {sap_counts[key]}")

    print(f'\nSum diff {json.dumps(dict(diff_count), indent=2, ensure_ascii=False)}')
    
    print("\n--- Records which DB is lower ---")
    for diff in db_lower_diff:
        print(diff)

if __name__ == "__main__":
    compare_counts('./count-from-db.csv', './count-from-sap.csv')