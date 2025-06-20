import { ChannelConfiguration } from "./channel.js";
import { DEFAULT_CONFIG } from "./config.js";
import { ConfigurationGetter, CSVRecord, FileType } from "./type.js";

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
  return `${text ?? ""}`.trim().replace(",", " ");
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
    const categoryId = normalizeCategoryId(row["MATKL"]).substring(0, 3);
    const productName = normalizeText(row["MAKTX"]);
    const brandCode = normalizeBrandId(row["BRAND_ID"]);
    if (!sku || !categoryId || !brandCode) return;
    const [earnable, burnable, earnRate] =
      configurationGetter(categoryId, brandCode, sku) ?? DEFAULT_CONFIG;
    return [
      trimZero ? sku.replace(/^0+/, "") : sku,
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
