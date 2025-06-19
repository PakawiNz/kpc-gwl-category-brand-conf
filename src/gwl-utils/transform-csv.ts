import { WriteStream } from "fs";
import {
  ConfigurationMapper,
  normalizeArticleId,
  normalizeBrandId,
  normalizeCategoryId,
} from "../conf-xlsx-to-json/transform.js";
import fs from "fs";
import { read_csv } from "../utils/read-csv.js";
import { Any } from "../type.js";

function formatCsvValue(value: any): string {
  return typeof value === "string" &&
    (value.includes(",") || value.includes('"'))
    ? `"${value.replace(/"/g, '""')}"`
    : value;
}
function prepOutputStream(headers: string[], csvPath: string): WriteStream {
  const outputPath = csvPath + ".transformed.csv";

  const outputStream = fs.createWriteStream(outputPath);

  // Write initial append rows
  outputStream.write("for-terra\n");
  outputStream.write(headers.join(",") + "\n");
  return outputStream;
}

export function transformArticleMaster(
  normalizedJsonObject: ConfigurationMapper,
  csvPath: string
) {
  const csvStream = read_csv(csvPath);
  const headers = [
    "SKU no.",
    "Product Name",
    "Category ID",
    "Brand Code",
    "Status",
    "Earn",
    "Pay With Carat",
    "Earn Rate",
  ];

  const outputStream = prepOutputStream(headers, csvPath);
  const invalidCategory = new Set();
  csvStream.on("data", (record: Any) => {
    if (!record.SKU_NO) return;
    const sku = normalizeArticleId(record.SKU_NO);
    const cat = normalizeCategoryId(record.CATEGORY_ID);
    const brand = normalizeBrandId(record.BRAND_CODE);
    if (!normalizedJsonObject.CAT[cat]) {
      invalidCategory.add(cat);
    }
    const config =
      normalizedJsonObject.SKU[sku] ||
      normalizedJsonObject["CAT|BRN"][`${cat}|${brand}`];

    const transformedRow: Any = {
      "SKU no.": record.SKU_NO,
      "Product Name": record.PRODUCT_NAME,
      "Category ID": record.CATEGORY_ID,
      "Brand Code": record.BRAND_CODE,
      Status: record.STATUS,
      Earn: config ? (config[0] ? "Y" : "N") : "",
      "Pay With Carat": config ? (config[1] ? "Y" : "N") : "",
      "Earn Rate": config ? config[2] : "",
    };

    const rowStr = headers
      .map((header) => formatCsvValue(transformedRow[header]))
      .join(",");
    outputStream.write(rowStr + "\n");
  });

  csvStream.on("end", () => {
    outputStream.end();
    console.log(`CSV data successfully written`);
    if (invalidCategory.size) {
      console.log(`invalid category ${JSON.stringify([...invalidCategory])}`);
    }
  });
}

export function transformCategoryMaster(
  normalizedJsonObject: ConfigurationMapper,
  csvPath: string
) {
  const csvStream = read_csv(csvPath);
  const headers = [
    "Product Category ID",
    "Product Category Name",
    "Earn",
    "Pay With Carat",
    "Earn Rate",
    //
  ];

  const outputStream = prepOutputStream(headers, csvPath);
  const invalidCategory = new Set();
  csvStream.on("data", (record: Any) => {
    if (!record.CODE) return;
    const cat = normalizeCategoryId(record.CODE);
    const config = normalizedJsonObject.CAT[cat];
    if (!config) {
      invalidCategory.add(cat);
    }
    const transformedRow: Any = {
      "Product Category ID": record.CODE,
      "Product Category Name": record.NAME,
      Earn: config ? (config[0] ? "Y" : "N") : "",
      "Pay With Carat": config ? (config[1] ? "Y" : "N") : "",
      "Earn Rate": config ? config[2] : "",
    };
    const rowStr = headers
      .map((header) => formatCsvValue(transformedRow[header]))
      .join(",");
    outputStream.write(rowStr + "\n");
  });

  csvStream.on("end", () => {
    outputStream.end();
    console.log(`CSV data successfully written`);
    if (invalidCategory.size) {
      console.log(`invalid category ${JSON.stringify([...invalidCategory])}`);
    }
  });
}

export function transformBrandMaster(
  normalizedJsonObject: ConfigurationMapper,
  csvPath: string
) {
  const csvStream = read_csv(csvPath);
  const headers = [
    "Brand Code",
    "Brand Name",
    "Earn",
    "Pay With Carat",
    //
  ];

  const outputStream = prepOutputStream(headers, csvPath);
  csvStream.on("data", (record: Any) => {
    if (!record.CODE) return;
    const transformedRow: Any = {
      "Brand Code": normalizeBrandId(record.CODE),
      "Brand Name": record.NAME,
      Earn: "N",
      "Pay With Carat": "N",
    };
    const rowStr = headers
      .map((header) => formatCsvValue(transformedRow[header]))
      .join(",");
    outputStream.write(rowStr + "\n");
  });

  csvStream.on("end", () => {
    outputStream.end();
    console.log(`CSV data successfully written`);
  });
}
