import fs from "fs";
import path from "path";
import { getCSV, getCSVReadStream } from "../utils/read-csv.js";
import { timestampString } from "../utils/timestamp-string.js";
import { rowsToFileCSV } from "../utils/write-csv.js";
import {
  normalizeBrandId,
  normalizeCategoryId,
  normalizeText,
} from "./convert-sap-to-gwl-product-files.js";
import { convertConfig } from "./md-master-function.js";
import {
  ConfigurationGetter,
  EarnBurnConfigurationJSON,
  FileType,
  PathWithFileType,
} from "./type.js";

export async function getAllCategoryAndBrand(
  files: PathWithFileType[],
  writeFolder: string,
  configurationGetters: ConfigurationGetter[]
) {
  writeFolder = path.join(writeFolder, "output", timestampString());
  fs.mkdirSync(writeFolder, { recursive: true });
  const mapped = Object.fromEntries(
    files
      .filter(({ fileType }) =>
        [FileType.CATEGORY, FileType.BRAND].includes(fileType)
      )
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

  let length = 0;
  for (const filePath of articles) {
    console.log("streaming:", filePath);
    const streamFile = getCSVReadStream(filePath, "|");
    await new Promise((resolve, reject) => {
      streamFile
        .on("data", (row) => {
          //   const sku = normalizeArticleId(row["MATNR"]);
          length += 1;
          const categoryId = normalizeCategoryId(row["MATKL"]).substring(0, 3);
          const brandCode = normalizeBrandId(row["BRAND_ID"]);
          if (categoryId) categories.add(categoryId);
          if (brandCode) brands.add(brandCode);
          if (categoryId && brandCode) combi.add(`${categoryId}|${brandCode}`);
        })
        .on("end", () => resolve(undefined))
        .on("error", (e) => reject(e));
    });
    console.log("completed ", length, "lines");
    length = 0;
  }
  
  const configCategoryAndBrand: { [key: string]: EarnBurnConfigurationJSON } =
    {};
  for (const cg of configurationGetters) {
    Object.assign(configCategoryAndBrand, (cg as any).jsonContent["CAT|BRN"]);
  }

  rowsToFileCSV(path.join(writeFolder, `CAT_MASTER_${timestampString()}.csv`), [
    ["CATEGORY_ID", "CATEGORY_TEXT"],
    ...[...categories].sort().map((categoryId) => {
      const categoryText = categoryMapper[categoryId as string];
      if (!categoryText) return;
      return [categoryId, categoryText];
    }),
  ]);
  rowsToFileCSV(path.join(writeFolder, `BRN_MASTER_${timestampString()}.csv`), [
    ["BRAND_ID", "BRAND_TEXT"],
    ...[...brands].sort().map((brandId) => {
      const brandText = brandMapper[brandId as string];
      if (!brandText) return;
      return [brandId, brandText];
    }),
  ]);
  rowsToFileCSV(
    path.join(writeFolder, `CAT_BRN_MASTER_${timestampString()}.csv`),
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
