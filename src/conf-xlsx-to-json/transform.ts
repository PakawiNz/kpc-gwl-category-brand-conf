import lodash from "lodash";

const BRAND_FOR_CATEGORY_MASTER = "$$$";

const COLUMN_CATEGORY_NUMBER = "CATEGORY_NUMBER"; // for master category config
const COLUMN_CATEGORY = "CATE";
const COLUMN_CATEGORY_TEXT = "CATEGORY_TEXT";
const COLUMN_CATEGORY_SKUS = "SKUS";
const COLUMN_BRAND = "BRAND";
const COLUMN_BRAND_NAME = "BRAND_NAME";
const COLUMN_EARNABLE = "EARN";
const COLUMN_BURNABLE = "PAY_WITH_CARAT";
const COLUMN_EARN_RATE = "EARN_RATE";

type Config = [boolean, boolean, number];
interface SubResult {
  [key: string]: Config;
}
export interface Result {
  SKU: SubResult;
  CAT: SubResult;
  ["CAT|BRN"]: SubResult;
}

export function normalizeArticleId(id: any): string {
  return `${id ?? ""}`.trim().replace(/[-_]/g, "");
}
export function normalizeBrandId(id: any): string {
  return `${id ?? ""}`.trim().toUpperCase();
}
export function normalizeCategoryId(id: any): string {
  return `${id ?? ""}`.trim().toLowerCase().replace(/[-_]/g, ""); //Handle case _ , - (PAR_001 => PAR-001 = Dup);
}

function getKeys(
  category: any,
  brand: any,
  skus: any
): [keyof Result, string][] {
  skus = normalizeArticleId(skus);
  brand = normalizeBrandId(brand);
  category = normalizeCategoryId(category);
  if (skus) {
    return skus.split(/\s*;\s*/).map((s: string) => ["SKU", s]);
  } else if (brand == BRAND_FOR_CATEGORY_MASTER) {
    return [["CAT", category]];
  } else if (brand) {
    return [["CAT|BRN", `${category}|${brand}`]];
  } else {
    throw "invalid key";
  }
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
export function normalizeCategoryAndBrandRule(sheets: any): Result {
  const result: Result = {
    SKU: {},
    CAT: {},
    ["CAT|BRN"]: {},
  };

  for (const sheetName in sheets) {
    for (const item of sheets[sheetName]) {
      let category, brand;
      const categoryMaster = item[COLUMN_CATEGORY_NUMBER];
      const skus = item[COLUMN_CATEGORY_SKUS];

      if (categoryMaster) {
        category = categoryMaster;
        brand = BRAND_FOR_CATEGORY_MASTER;
      } else {
        category = item[COLUMN_CATEGORY];
        brand = item[COLUMN_BRAND];
      }

      if (!category) continue;

      try {
        const keys = getKeys(category, brand, skus);
        const config = getConfig(
          item[COLUMN_EARNABLE],
          item[COLUMN_BURNABLE],
          item[COLUMN_EARN_RATE]
        );

        for (const [group, key] of keys) {
          try {
            if (result[group][key]) throw "duplicated configuration";
            result[group][key] = config;
          } catch (e) {
            console.warn(e, `[Sheet: "${sheetName}" Key: "${key}"]`);
          }
        }
      } catch (e) {
        console.warn(
          e,
          `[Sheet: "${sheetName}" Category: "${category}" Brand: "${brand}", SKUs: "${skus}"]`
        );
      }
    }
  }

  return result;
}

function findMajority(data: string[]) {
  return lodash
    .chain(data)
    .countBy()
    .toPairs()
    .maxBy(([_, count]) => count)
    .get(0)
    .value();
}

export function findMajorityCategoryConfiguration(sheets: any): {} {
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

export function findMajorityBrandConfiguration(sheets: any): {} {
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
