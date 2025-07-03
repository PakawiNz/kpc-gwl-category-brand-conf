import os
from enum import Enum, auto
from dataclasses import dataclass
from collections import defaultdict
from pathlib import Path
import pprint


# Represents the file types, similar to the TypeScript enum
class FileType(Enum):
    CATEGORY = auto()
    BRAND = auto()
    ARTICLE = auto()
    COMPANY = auto()
    BUSINESS_AREA = auto()
    COST_CENTER = auto()


# A dataclass to hold structured file information, replacing the JS object
@dataclass
class PathWithFileType:
    path: Path
    file_type: FileType
    nature: str
    datetime: str


# A mapping for efficient module name to FileType conversion
MODULE_TO_FILE_TYPE_MAP = {
    "category": FileType.CATEGORY,
    "brand": FileType.BRAND,
    "article": FileType.ARTICLE,
    "company": FileType.COMPANY,
    "bussinessarea": FileType.BUSINESS_AREA,
    "costcenter": FileType.COST_CENTER,
}


def get_file_type_from_name(module: str) -> FileType | None:
    """Gets the FileType enum from a module name string."""
    return MODULE_TO_FILE_TYPE_MAP.get(module.lower())


def list_files_in_folder(folder: str, start_date: str = "", no_filter=False) -> list[PathWithFileType]:
    """
    Lists, filters, and sorts files in a folder based on their name, nature, and date.

    This function finds the latest "FULL" file for each file type and then includes
    any subsequent files (e.g., deltas) that are newer than that "FULL" file.
    """
    all_files: list[PathWithFileType] = []

    # 1. Read all files and parse their metadata from the filename
    try:
        filenames = os.listdir(folder)
    except FileNotFoundError:
        print(f"Error: Folder not found at '{folder}'")
        return []

    for file_name in filenames:
        try:
            # Assumes format: env_module_nature_date_time...
            _, module, nature, date, time, *_ = file_name.split("_")

            file_type = get_file_type_from_name(module)

            # Add to list if the file type is valid and date is within range
            if file_type and start_date <= date:
                all_files.append(
                    PathWithFileType(
                        path=Path(folder) / file_name,
                        file_type=file_type,
                        nature=nature,
                        datetime=f"{date}{time}",
                    )
                )
        except ValueError:
            # Ignores files that don't match the expected naming convention
            print(f"Skipping file with incorrect format: {file_name}")
            continue

    if no_filter:
        return all_files

    # 2. Find the latest datetime for each "FULL" file type
    max_datetime_each_type = defaultdict(str)
    for f in all_files:
        if f.nature == "FULL":
            max_datetime_each_type[f.file_type] = max(
                max_datetime_each_type[f.file_type], f.datetime
            )

    # 3. Filter for "effective" files
    effective_files: list[PathWithFileType] = []
    for f in all_files:
        latest_full_dt = max_datetime_each_type.get(f.file_type)

        # Keep the file if it's the single latest "FULL" file
        if f.nature == "FULL":
            if f.datetime == latest_full_dt:
                effective_files.append(f)
        # Or keep it if it's a delta/other file newer than the latest "FULL" one
        else:
            if not latest_full_dt or latest_full_dt < f.datetime:
                effective_files.append(f)

    # 4. Sort the final list of files by datetime in ascending order
    return sorted(effective_files, key=lambda f: f.datetime)


if __name__ == "__main__":
    pprint.pprint(
        list_files_in_folder(
            "/Users/pakawin_m/workspace/kpc-gwl-category-brand-conf/data/kpg-sap-s3-outbound-prod"
        )
    )
