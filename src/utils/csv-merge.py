import os
import uuid  # For generating unique file names, similar to ulid


def get_file_paths(folder_path: str) -> list[str]:
    """
    Get all file paths within a given folder, excluding directories.
    """
    file_paths = []
    for entry in os.listdir(folder_path):
        full_path = os.path.join(folder_path, entry)
        if os.path.isfile(full_path):
            file_paths.append(full_path)
    return file_paths


def merge_csv_files(input_files: list[str], output_file: str, header_rows: int):
    """
    Merges multiple CSV files into a single output file, skipping header rows
    from the second file onwards.
    """
    is_first_file = True
    try:
        with open(output_file, "wb") as outfile:  # Open in binary write mode
            for file_path in input_files:
                print(f"merging {file_path}")
                with open(file_path, "rb") as infile:  # Open in binary read mode
                    if is_first_file:
                        outfile.write(infile.read())
                        is_first_file = False
                    else:
                        # Skip header rows
                        for _ in range(header_rows):
                            infile.readline()  # Read and discard the header lines
                        outfile.write(infile.read())
    except Exception as e:
        raise IOError(f"Error merging CSV files: {e}")


# Example Usage (equivalent to the TypeScript example)
if __name__ == "__main__":
    folder_path = '/Users/pakawin_m/Library/CloudStorage/OneDrive-KingPowerGroup/[KPGDX-GWL] - Group-wide Loyalty - GWL - 07_Cutover Plan/cat-brand-master/20250624_143620/KPC_OFFLINE/ARTICLE'

    # Generate a unique ID similar to ulid. For simplicity, using uuid4.
    # If you strictly need ulid, you'd need to install the 'ulid-py' package.
    unique_id = str(uuid.uuid4()).replace("-", "")[
        :16
    ]  # ulid is 26 chars, uuid4 is longer. This truncates for similar length.

    output_csv_path = f"{folder_path}.{unique_id}.csv"

    input_files = sorted(get_file_paths(folder_path))
    merge_csv_files(input_files, output_csv_path, 2)
    print(f"Merged CSV files created at: {output_csv_path}")
