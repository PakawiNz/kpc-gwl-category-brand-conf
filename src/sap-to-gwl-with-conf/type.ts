export type BooleanString = "Y" | "N" | "";
export type EarnBurnConfiguration = [BooleanString, BooleanString, number | ""];
export type EarnBurnConfigurationJSON = [boolean, boolean, number];
export type ConfigurationGetter = (
  category?: string,
  brand?: string,
  article?: string
) => EarnBurnConfiguration | undefined;

export interface CSVRecord {
  [key: string]: string;
}

export enum FileType {
  CATEGORY = "CATEGORY",
  BRAND = "BRAND",
  ARTICLE = "ARTICLE",
  COMPANY = "COMPANY",
  BUSINESS_AREA = "BUSINESS_AREA",
  COST_CENTER = "COST_CENTER",
}

export interface PathWithFileType {
  path: string;
  fileType: FileType;
}
