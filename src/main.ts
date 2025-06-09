import XLSX from 'xlsx';
import fs from 'fs';
import path from 'path';
import { findMajorityBrandConfiguration, findMajorityCategoryConfiguration, normalizeCategoryAndBrandRule } from './transform.js';

/**
 * Reads an XLSX file and returns the first sheet's data as an array of objects.
 * @param filePath The path to the XLSX file.
 * @returns An array of objects, where each object represents a row.
 * @throws Error if the file cannot be read or if the workbook has no sheets.
 */
function read_xlsx(filePath: string): {} {
  try {
    const resolvedPath = path.resolve(filePath);
    if (!fs.existsSync(resolvedPath)) {
      throw new Error(`File not found at path: ${resolvedPath}`);
    }
    const workbook = XLSX.readFile(resolvedPath);
    return Object.fromEntries(workbook.SheetNames.map((sheetName) => {
        const worksheet = workbook.Sheets[sheetName];
        return [
            sheetName,
            XLSX.utils.sheet_to_json(worksheet),
        ]
    }))
  } catch (error) {
    console.error('Error reading XLSX file:', error);
    throw error;
  }
}


/**
 * Writes a JSON string to a specified file path.
 * @param filePath The path where the JSON file will be saved.
 * @param jsonString The JSON string to write.
 * @throws Error if the file cannot be written.
 */
function write_json(filePath: string, jsonObject: {}): void {
  const resolvedPath = path.resolve(filePath);
  fs.writeFileSync(resolvedPath, JSON.stringify(jsonObject, null, 2), 'utf8');
  console.log(`JSON data successfully written to ${resolvedPath}`);
}

const xlsxPath = './data/20250605 Master MD for SAP mapping.xlsx'
const baseName = xlsxPath.split('.').slice(0, -1).join('.')

const jsonObject = read_xlsx(xlsxPath)
write_json(baseName + '.json' , jsonObject)

write_json(baseName + '.norm.json', normalizeCategoryAndBrandRule(jsonObject))

write_json(baseName + '.category.json', findMajorityCategoryConfiguration(jsonObject))

write_json(baseName + '.brand.json', findMajorityBrandConfiguration(jsonObject))