import path from "path";
import { ChunkWriteStream } from "../utils/chunk-file-stream.js";
import {
  CHANNEL_CONFIGURATIONS,
  ChannelConfiguration,
  COMPANY,
} from "./channels-master.js";
import { DEFAULT_CONFIG } from "./md-master-function.js";
import { ConfigurationGetter, CSVRecord, FileType } from "./type.js";
import fs from "fs";
import { timestampString } from "../utils/timestamp-string.js";
import { ulid } from "ulid";
import { Writable } from "stream";

function isExcluded(category: string, brand: string, article: string): boolean {
  // if (category == "221" && brand == "GUC") return false;
  // return true;
  return false;
}

export function normalizeArticleId(id: string): string {
  return `${id ?? ""}`.trim().replace(/[-_]/g, "");
}
export function normalizeBrandId(id: string): string {
  return `${id ?? ""}`.trim().toUpperCase();
}
export function normalizeCategoryId(id: string): string {
  return `${id ?? ""}`.trim().toLowerCase().replace(/[-_]/g, ""); //Handle case _ , - (PAR_001 => PAR-001 = Dup);
}
export function normalizeText(text: string): string {
  text = `${text ?? ""}`;
  text = text.trim().replace(/"/g, '""');
  return `"${text}"`;
}

export class SapFileConversionService {
  confugurationGetter: ConfigurationGetter;
  constructor(confugurationGetter: ConfigurationGetter) {
    this.confugurationGetter = confugurationGetter;
  }
  getHeaderAndConverter(fileType: FileType, channel: ChannelConfiguration) {
    switch (fileType) {
      case FileType.CATEGORY:
        return {
          headers: this.categoryHeaders,
          convert: (row: CSVRecord) =>
            this.categoryConvert(
              row,
              channel.categoryConfig
                ? () => channel.categoryConfig
                : this.confugurationGetter
            ),
        };
      case FileType.BRAND:
        return {
          headers: this.brandHeaders,
          convert: (row: CSVRecord) =>
            this.brandConvert(
              row,
              channel.brandConfig
                ? () => channel.brandConfig
                : this.confugurationGetter
            ),
        };
      case FileType.ARTICLE:
        return {
          headers: this.articleHeaders,
          convert: (row: CSVRecord) =>
            this.articleConvert(
              row,
              channel.trimPaddedZeroSku,
              channel.articleConfig
                ? () => channel.articleConfig
                : this.confugurationGetter
            ),
        };
      default:
        throw "invalid file type";
    }
  }
  brandHeaders = [
    "Brand Code",
    "Brand Name",
    "Earn",
    "Pay With Carat",
    //
  ];
  brandConvert(row: CSVRecord, configurationGetter: ConfigurationGetter) {
    const brandId = normalizeBrandId(row["BRAND_ID"]);
    const brandDesc = normalizeText(row["BRAND_DESCR"]);
    if (!brandId || !brandDesc) return;
    const [earnable, burnable] =
      configurationGetter(undefined, brandId, undefined) ?? DEFAULT_CONFIG;
    return [brandId, brandDesc, earnable, burnable];
  }
  categoryHeaders = [
    "Product Category ID",
    "Product Category Name",
    "Earn",
    "Pay With Carat",
    "Earn Rate",
    //
  ];
  categoryConvert(row: CSVRecord, configurationGetter: ConfigurationGetter) {
    const categoryId = normalizeCategoryId(row["CLASS"]);
    if (categoryId.length != 3) return;
    const categoryDesc = normalizeText(row["KSCHG"]);
    const [earnable, burnable, earnRate] =
      configurationGetter(categoryId, undefined, undefined) ?? DEFAULT_CONFIG;
    if (earnable === "" || burnable === "" || earnRate === "") return;
    return [categoryId, categoryDesc, earnable, burnable, earnRate.toString()];
  }
  articleHeaders = [
    "SKU no.",
    "Product Name",
    "Category ID",
    "Brand Code",
    "Status",
    "Earn",
    "Pay With Carat",
    "Earn Rate",
    //
  ];
  articleConvert(
    row: CSVRecord,
    trimZero: boolean,
    configurationGetter: ConfigurationGetter
  ) {
    const sku = normalizeArticleId(row["MATNR"]);
    const trimmedSku = sku.replace(/^0+/, "");
    const categoryId = normalizeCategoryId(row["MATKL"]).substring(0, 3);
    const productName = normalizeText(row["MAKTX"]);
    const brandCode = normalizeBrandId(row["BRAND_ID"]);
    if (!sku || !categoryId || !brandCode) return;
    if (isExcluded(categoryId, brandCode, sku)) return;
    const config = configurationGetter(categoryId, brandCode, trimmedSku);
    if (!config) return;
    const [earnable, burnable, earnRate] = config;
    return [
      trimZero ? trimmedSku : sku,
      productName,
      categoryId,
      brandCode,
      "ACTIVE",
      earnable,
      burnable,
      earnRate.toString(),
    ];
  }
}

export function getConversionWritables(
  dstFileFolder: string,
  fileType: FileType,
  services: Record<COMPANY, SapFileConversionService>
): Writable[] {
  return CHANNEL_CONFIGURATIONS.map((channel) => {
    const service = services[channel.company];
    const { headers, convert } = service.getHeaderAndConverter(
      fileType,
      channel
    );
    let firstLine = true;
    let sapHeaders: string[];
    return new ChunkWriteStream({
      path: path.join(dstFileFolder, channel.code, fileType),
      filename: `${timestampString()}_${ulid()}`,
      extension: "csv",
      getFileHeader: () => `Unused Lines\n` + headers.join(", "),
      transformLine(line: string) {
        if (firstLine) {
          firstLine = false;
          sapHeaders = line.split("|");
        } else {
          const splittedLine = line.split("|");
          const record = Object.fromEntries(
            sapHeaders.map((header, i) => [header, splittedLine[i]])
          );
          return convert(record)?.join(",");
        }
      },
    });
  });
}

export async function listConvertedSkuConfigs(
  folderPath: string,
  includedFileTypes: FileType[]
) {
  const SKU_CONFIG_TYPE = [FileType.ARTICLE, FileType.BRAND, FileType.CATEGORY];
  const channelCodes = CHANNEL_CONFIGURATIONS.map((channel) => channel.code);
  const fileTypes = includedFileTypes.filter((ft) =>
    SKU_CONFIG_TYPE.includes(ft)
  ) as string[];
  const allFiles: [string, string, string][] = [];
  fs.readdirSync(folderPath).forEach((channelCode) => {
    if (channelCodes.includes(channelCode)) {
      fs.readdirSync(path.join(folderPath, channelCode)).forEach((fileType) => {
        if (fileTypes.includes(fileType)) {
          fs.readdirSync(path.join(folderPath, channelCode, fileType)).forEach(
            (filePath) => {
              if (filePath.toLowerCase().endsWith(".csv")) {
                const fullPath = path.join(
                  folderPath,
                  channelCode,
                  fileType,
                  filePath
                );
                allFiles.push([channelCode, fileType, fullPath]);
              }
            }
          );
        }
      });
    }
  });
  return allFiles;
}
