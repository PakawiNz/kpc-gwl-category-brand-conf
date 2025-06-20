import csvParser from "csv-parser";
import { getCSV } from "../utils/read-csv.js";
import {
  normalizeArticleId,
  normalizeBrandId,
  normalizeCategoryId,
  normalizeText,
} from "./conversion.js";
import { ConfigurationGetter, FileType, PathWithFileType } from "./type.js";
import fs from "fs";
import { convertConfig } from "./config.js";

const COST_CENTER_TYPE = [FileType.CATEGORY, FileType.BRAND];

export async function getAllCategoryAndBrand(
  files: PathWithFileType[],
  writePath: string,
  configurationGetter: ConfigurationGetter
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
          combi.add(`${categoryId}|${brandCode}`);
        })
        .on("end", () => resolve(undefined))
        .on("error", (e) => reject(e));
    });
  }
  const jsonContent = (configurationGetter as any).jsonContent;
  const combii = [...combi]
  combii.sort()

  const writeStream = fs.createWriteStream(writePath);
  writeStream.write("CATEGORY_ID,BRAND_ID\n");
  combii.forEach((l) => {
    const line = l as string;
    const [category, brand] = line.split("|");
    const conf = convertConfig(jsonContent["CAT|BRN"][line]).join(",");
    const categoryText = categoryMapper[category];
    const brandText = brandMapper[brand];
    if (!categoryText || !brandText) return;
    writeStream.write(
      `${category},${categoryText},${brand},${brandText},${conf}\n`
    );
  });
  writeStream.end();
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
