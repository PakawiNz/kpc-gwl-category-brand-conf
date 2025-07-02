import { EarnBurnConfiguration } from "./type.js";
import fs from "fs";

interface SubConfigurationMapper {
  [key: string]: EarnBurnConfiguration;
}
export interface ConfigurationMapper {
  SKU: SubConfigurationMapper;
  CAT: SubConfigurationMapper;
  ["CAT|BRN"]: SubConfigurationMapper;
  ["CAT|BRN|SKU"]: SubConfigurationMapper;
}
export interface CompanyConfigurationMapper {
  [key: string]: ConfigurationMapper;
}

export const DEFAULT_CONFIG: EarnBurnConfiguration = ["N", "N", 0];

export function convertConfig(config: any): EarnBurnConfiguration {
  return [
    config ? (config[0] ? "Y" : "N") : "",
    config ? (config[1] ? "Y" : "N") : "",
    config ? config[2] : "",
  ];
}

export function createConfigurationGetter(
  configJsonPath: string,
  company: string
) {
  const jsonContent = JSON.parse(
    fs.readFileSync(configJsonPath, "utf8")
  ) as CompanyConfigurationMapper;
  const fn = (
    category?: string,
    brand?: string,
    article?: string
  ): EarnBurnConfiguration | undefined => {
    if (
      category !== undefined &&
      brand !== undefined &&
      article !== undefined
    ) {
      const config =
        jsonContent[company]["CAT|BRN|SKU"][`${category}|${brand}|${article}`] ||
        jsonContent[company]["CAT|BRN"][`${category}|${brand}`];
      if (!jsonContent[company]["CAT"][category]) return;
      else return convertConfig(config);
    } else if (category !== undefined) {
      const config = jsonContent[company]["CAT"][category] ?? DEFAULT_CONFIG;
      return convertConfig(config);
    } else if (brand !== undefined) {
      return DEFAULT_CONFIG;
    }
  };
  fn.jsonContent = jsonContent[company];
  return fn;
}
