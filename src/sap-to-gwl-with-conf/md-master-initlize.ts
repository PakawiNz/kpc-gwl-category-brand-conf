import csvParser from "csv-parser";
import { getCSV } from "../utils/read-csv.js";
import {
  normalizeArticleId,
  normalizeBrandId,
  normalizeCategoryId,
  normalizeText,
} from "./convert-sap-to-gwl-product-files.js";
import {
  ConfigurationGetter,
  EarnBurnConfigurationJSON,
  FileType,
  PathWithFileType,
} from "./type.js";
import fs from "fs";
import { convertConfig } from "./md-master-function.js";
import { rowsToFileCSV, toCSVRecord } from "../utils/write-csv.js";
import path from "path";
import moment from "moment";
import { timestampString } from "../utils/timestamp-string.js";

const COST_CENTER_TYPE = [FileType.CATEGORY, FileType.BRAND];

export async function getAllCategoryAndBrand(
  files: PathWithFileType[],
  writeFolder: string,
  configurationGetters: ConfigurationGetter[]
) {
  const mapped = Object.fromEntries(
    files
      .filter(({ fileType }) => COST_CENTER_TYPE.includes(fileType))
      .map((file) => [file.fileType, file.path])
  );
  if (!mapped[FileType.CATEGORY]) throw "missing CATEGORY";
  if (!mapped[FileType.BRAND]) throw "missing BRAND";

  const categoryMapper = Object.fromEntries(
    getCSV(mapped[FileType.CATEGORY], "|").map((row) => [
      normalizeCategoryId(row["CLASS"]),
      normalizeText(row["KSCHG"]),
    ])
  );
  const brandMapper = Object.fromEntries(
    getCSV(mapped[FileType.BRAND], "|").map((row) => [
      normalizeBrandId(row["BRAND_ID"]),
      normalizeText(row["BRAND_DESCR"]),
    ])
  );

  const articles = files
    .filter(({ fileType }) => fileType == FileType.ARTICLE)
    .map((file) => file.path);
  const categories = new Set();
  const brands = new Set();
  const combi = new Set();

  for (const filePath of articles) {
    const streamFile = fs.createReadStream(filePath, { encoding: "utf-8" });
    await new Promise((resolve, reject) => {
      streamFile
        .pipe(getCSVParser())
        .on("data", (row) => {
          //   const sku = normalizeArticleId(row["MATNR"]);
          const categoryId = normalizeCategoryId(row["MATKL"]).substring(0, 3);
          const brandCode = normalizeBrandId(row["BRAND_ID"]);
          if (categoryId) categories.add(categoryId);
          if (brandCode) brands.add(brandCode);
          if (categoryId && brandCode) combi.add(`${categoryId}|${brandCode}`);
        })
        .on("end", () => resolve(undefined))
        .on("error", (e) => reject(e));
    });
  }
  const configCategoryAndBrand: { [key: string]: EarnBurnConfigurationJSON } =
    {};
  for (const cg of configurationGetters) {
    Object.assign(configCategoryAndBrand, (cg as any).jsonContent["CAT|BRN"]);
  }

  rowsToFileCSV(
    path.join(
      writeFolder,
      `CAT_MASTER_${timestampString()}.csv`
    ),
    [
      ["CATEGORY_ID", "CATEGORY_TEXT"],
      ...[...categories].sort().map((categoryId) => {
        const categoryText = categoryMapper[categoryId as string];
        if (!categoryText) return;
        return [categoryId, categoryText];
      }),
    ]
  );
  rowsToFileCSV(
    path.join(
      writeFolder,
      `BRN_MASTER_${timestampString()}.csv`
    ),
    [
      ["BRAND_ID", "BRAND_TEXT"],
      ...[...brands].sort().map((brandId) => {
        const brandText = brandMapper[brandId as string];
        if (!brandText) return;
        return [brandId, brandText];
      }),
    ]
  );
  rowsToFileCSV(
    path.join(
      writeFolder,
      `CAT_BRN_MASTER_${timestampString()}.csv`
    ),
    [
      [
        "CATEGORY_ID",
        "CATEGORY_TEXT",
        "BRAND_ID",
        "BRAND_TEXT",
        "EARN?",
        "BURN?",
        "EARN_RATE",
      ],
      ...[...combi].sort().map((l) => {
        const line = l as string;
        const [category, brand] = line.split("|");
        const conf = convertConfig(configCategoryAndBrand[line]);
        const categoryText = categoryMapper[category];
        const brandText = brandMapper[brand];
        if (!categoryText || !brandText) return;
        return [category, categoryText, brand, brandText, ...conf];
      }),
    ]
  );
}

function getCSVParser() {
  return csvParser({
    mapHeaders: ({ header }) =>
      header
        ?.replace(/^\uFEFF/, "") // Remove BOM
        .replace(/\u200B/g, "") // Remove zero-width spaces
        .trim(),
    skipLines: 0,
    separator: "|",
  });
}
