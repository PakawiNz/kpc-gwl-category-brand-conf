import XLSX from "xlsx";
import fs from "fs";
import path from "path";
import lodash from "lodash";
import { Any } from "../type.js";

/**
 * Reads an XLSX file and returns the first sheet's data as an array of objects.
 * @param filePath The path to the XLSX file.
 * @returns An array of objects, where each object represents a row.
 * @throws Error if the file cannot be read or if the workbook has no sheets.
 */
export function read_xlsx(filePath: string): {} {
  try {
    const resolvedPath = path.resolve(filePath);
    if (!fs.existsSync(resolvedPath)) {
      throw new Error(`File not found at path: ${resolvedPath}`);
    }
    const workbook = XLSX.readFile(resolvedPath);
    return Object.fromEntries(
      workbook.SheetNames.map((sheetName) => {
        const worksheet = workbook.Sheets[sheetName];
        return [
          sheetName,
          XLSX.utils.sheet_to_json(worksheet).map(item =>
            lodash.mapKeys(item as Any, (value: any, key: string) => {
              return key.toUpperCase().replace(/\s+/g, '_').replace(/\W/g, '');
            })
          ),
        ];
      })
    );
  } catch (error) {
    console.error("Error reading XLSX file:", error);
    throw error;
  }
}