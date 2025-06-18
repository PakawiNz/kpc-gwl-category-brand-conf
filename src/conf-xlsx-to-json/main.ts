import fs from "fs";
import path from "path";
import {
  findMajorityBrandConfiguration,
  findMajorityCategoryConfiguration,
  normalizeCategoryAndBrandRule,
} from "./transform.js";
import { read_xlsx } from "../utils/read-xlsx.js";

/**
 * Writes a JSON string to a specified file path.
 * @param filePath The path where the JSON file will be saved.
 * @param jsonString The JSON string to write.
 * @throws Error if the file cannot be written.
 */
function write_json(filePath: string, jsonObject: {}): void {
  const resolvedPath = path.resolve(filePath);
  fs.writeFileSync(resolvedPath, JSON.stringify(jsonObject, null, 2), "utf8");
  console.log(`JSON data successfully written to ${resolvedPath}`);
}

export function convertXlsxConfigurationToJson(xlsxPath: string, jsonPath?: string) {
  const baseName = xlsxPath.split(".").slice(0, -1).join(".");

  const jsonObject = read_xlsx(xlsxPath);
  write_json(baseName + ".json", jsonObject);

  const normalizedJsonObject = normalizeCategoryAndBrandRule(jsonObject);
  write_json(baseName + ".norm.json", normalizedJsonObject);

  write_json(
    baseName + ".category.json",
    findMajorityCategoryConfiguration(jsonObject)
  );

  write_json(
    baseName + ".brand.json",
    findMajorityBrandConfiguration(jsonObject)
  );
  if (jsonPath) {
    write_json(jsonPath, normalizedJsonObject);
  }
}
