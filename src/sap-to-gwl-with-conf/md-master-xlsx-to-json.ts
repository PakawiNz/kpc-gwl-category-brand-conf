import fs from "fs";
import lodash from "lodash";
import path from "path";
import { read_xlsx } from "../utils/read-xlsx.js";
import { timestampString } from "../utils/timestamp-string.js";
import { buildCompanyMap, COMPANY } from "./channels-master.js";

const COLUMN_CATEGORY_NUMBER = "CATEGORY_NUMBER"; // for master category config
const COLUMN_CATEGORY = "CATE";
const COLUMN_CATEGORY_TEXT = "CATEGORY_TEXT";
const COLUMN_BRAND_NUMBER = "BRAND_NUMBER";
const COLUMN_BRAND = "BRAND";
const COLUMN_BRAND_NAME = "BRAND_NAME";
const COLUMN_SKUS = "SKUS";
const COLUMN_COMPANY = "COMPANY";
const COLUMN_EARNABLE = "EARN";
const COLUMN_BURNABLE = "PAY_WITH_CARAT";
const COLUMN_EARN_RATE = "EARN_RATE";

type Config = [boolean, boolean, number];
interface SubResult {
  [key: string]: Config;
}
interface ConfigurationMapper {
  SKU: SubResult;
  CAT: SubResult;
  BRN: SubResult;
  ["CAT|BRN"]: SubResult;
  ["CAT|BRN|SKU"]: SubResult;
}
interface CompanyConfigurationMapper {
  [key: string]: ConfigurationMapper;
}

function normalizeArticleId(id: any): string {
  return `${id ?? ""}`.trim().replace(/[-_]/g, "");
}
function normalizeBrandId(id: any): string {
  return `${id ?? ""}`.trim().toUpperCase();
}
function normalizeCategoryId(id: any): string {
  return `${id ?? ""}`.trim().toLowerCase().replace(/[-_]/g, ""); //Handle case _ , - (PAR_001 => PAR-001 = Dup);
}
function normalizeCompany(id: any): string {
  return `${id ?? ""}`.trim().toUpperCase();
}

function getConfig(
  earnableRaw: any,
  burnableRaw: any,
  earnRateRaw: any
): Config {
  const earnable = `${earnableRaw ?? ""}`;
  const burnable = `${burnableRaw ?? ""}`;
  const earnRate = Number(earnRateRaw);

  const errors = {
    // "unclean category": !!category.match(/\W/),
    // "unclean brand": !!brand.match(/\W/),
    "invalid earnable": earnable != "Y" && earnable != "N",
    "invalid burnable": burnable != "Y" && burnable != "N",
    "invalid earn rate": isNaN(earnRate),
  };
  if (Object.values(errors).some(Boolean)) {
    const errorMessages = Object.entries(errors)
      .filter(([_, value]) => value)
      .map(([key]) => key)
      .join(", ");

    throw errorMessages;
  }

  return [earnable == "Y", burnable == "Y", earnRate];
}

/**
 * Transforms data (typically from an XLSX sheet) into a JSON string.
 * @param data The data to transform (e.g., an array of objects).
 * @returns A JSON string representation of the data.
 */
function normalizeCategoryAndBrandRule(
  sheets: any
): [CompanyConfigurationMapper, string[]] {
  const companyResult = buildCompanyMap<ConfigurationMapper>(() => ({
    SKU: {},
    CAT: {},
    BRN: {},
    ["CAT|BRN"]: {},
    ["CAT|BRN|SKU"]: {},
  }));

  const errors: string[] = [];
  for (const sheetName in sheets) {
    for (const item of sheets[sheetName]) {
      const categoryMaster = normalizeCategoryId(item[COLUMN_CATEGORY_NUMBER]);
      const brandMaster = normalizeBrandId(item[COLUMN_BRAND_NUMBER]);

      const skus = normalizeArticleId(item[COLUMN_SKUS]);
      const category =
        categoryMaster || //
        normalizeCategoryId(item[COLUMN_CATEGORY]);
      const brand =
        brandMaster || //
        normalizeBrandId(item[COLUMN_BRAND]);

      const companies: COMPANY[] = Object.values(COMPANY);
      // const companies: COMPANY[] =
      //   categoryMaster || brandMaster
      //     ? Object.values(COMPANY)
      //     : item[COLUMN_COMPANY].split(";").map(normalizeCompany);

      try {
        const keys = (() => {
          if (categoryMaster) return [["CAT", categoryMaster]];
          if (brandMaster) return [["BRN", brandMaster]];
          if (skus && category && brand) {
            return skus
              .split(/\s*;\s*/)
              .map((s: string) => ["CAT|BRN|SKU", `${category}|${brand}|${s.replace(/^0+/, "")}`]);
          } else if (category && brand) {
            return [["CAT|BRN", `${category}|${brand}`]];
          } else {
            throw "invalid key";
          }
        })() as [keyof ConfigurationMapper, string][];

        const config = getConfig(
          item[COLUMN_EARNABLE],
          item[COLUMN_BURNABLE],
          item[COLUMN_EARN_RATE]
        );

        for (const company of companies) {
          for (const [group, key] of keys) {
            try {
              if (companyResult[company][group][key])
                throw "duplicated configuration";
              companyResult[company][group][key] = config;
            } catch (e) {
              errors.push(`${e} [Sheet: "${sheetName}" Key: "${key}"]`);
            }
          }
        }
      } catch (e) {
        errors.push(
          `${e} [Sheet: "${sheetName}" Category: "${category}" Brand: "${brand}", SKUs: "${skus}"]`
        );
      }
    }
  }

  return [companyResult, errors];
}

// ================================================================================================================

function findMajority(data: string[]) {
  return lodash
    .chain(data)
    .countBy()
    .toPairs()
    .maxBy(([_, count]) => count)
    .get(0)
    .value();
}

function findMajorityCategoryConfiguration(sheets: any): {} {
  const categories: { [key: string]: string[] } = {};

  for (const sheetName in sheets) {
    for (const item of sheets[sheetName]) {
      const category = item[COLUMN_CATEGORY];
      if (category) {
        categories[category] = categories[category] ?? [];
        categories[category].push(
          [
            category,
            item[COLUMN_CATEGORY_TEXT],
            item[COLUMN_EARNABLE],
            item[COLUMN_BURNABLE],
            item[COLUMN_EARN_RATE],
          ]
            .map((a) => `${a ?? ""}`)
            .join("|")
        );
      }
    }
  }

  return lodash.sortBy(
    Object.keys(categories).map((category) =>
      findMajority(categories[category])
    )
  );
}

function findMajorityBrandConfiguration(sheets: any): {} {
  const brands: { [key: string]: string[] } = {};

  for (const sheetName in sheets) {
    for (const item of sheets[sheetName]) {
      const brand = item[COLUMN_BRAND];
      if (brand) {
        brands[brand] = brands[brand] ?? [];
        brands[brand].push(
          [
            brand,
            item[COLUMN_BRAND_NAME],
            item[COLUMN_EARNABLE],
            item[COLUMN_BURNABLE],
            item[COLUMN_EARN_RATE],
          ]
            .map((a) => `${a ?? ""}`)
            .join("|")
        );
      }
    }
  }

  return lodash.sortBy(
    Object.keys(brands).map((brand) => findMajority(brands[brand]))
  );
}

// ================================================================================================================

function writeToFile(filePath: string[], content: any): void {
  const resolvedPath = path.resolve(path.join(...filePath));
  fs.mkdirSync(path.dirname(resolvedPath), { recursive: true });
  if (!lodash.isString(content)) content = JSON.stringify(content, null, 2);
  fs.writeFileSync(resolvedPath, content, "utf8");
  console.log(`Data successfully written to ${resolvedPath}`);
}

export function convertXlsxConfigurationToJson(
  xlsxPath: string,
  jsonPath?: string
) {
  const folderPath = path.dirname(xlsxPath);
  const baseName = path.basename(xlsxPath).split(".").slice(0, -1).join(".");

  const jsonObject = read_xlsx(xlsxPath);
  writeToFile(
    [folderPath, "output", baseName + ".json"],
    jsonObject //
  );

  const [normalizedJsonObject, errors] =
    normalizeCategoryAndBrandRule(jsonObject);

  writeToFile(
    [folderPath, "output", baseName + ".norm.json"],
    normalizedJsonObject
  );
  const errorPath = [folderPath, "output", baseName + `.error.${timestampString()}.txt`]
  writeToFile(
    errorPath,
    errors.join("\n")
  );
  writeToFile(
    [folderPath, "output", baseName + ".category.json"],
    findMajorityCategoryConfiguration(jsonObject)
  );
  writeToFile(
    [folderPath, "output", baseName + ".brand.json"],
    findMajorityBrandConfiguration(jsonObject)
  );

  if (jsonPath) {
    writeToFile([jsonPath], normalizedJsonObject);
  }

  if (errors.length) throw `There is some errors in master file.\n${path.join(...errorPath)}`
}
