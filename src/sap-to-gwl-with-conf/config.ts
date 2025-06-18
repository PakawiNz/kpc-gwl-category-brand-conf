import { EarnBurnConfiguration } from "./type.js";
import fs from "fs";

interface SubConfigurationMapper {
  [key: string]: EarnBurnConfiguration;
}
export interface ConfigurationMapper {
  SKU: SubConfigurationMapper;
  CAT: SubConfigurationMapper;
  ["CAT|BRN"]: SubConfigurationMapper;
}

export const DEFAULT_CONFIG: EarnBurnConfiguration = ["N", "N", 0];

function convertConfig(config: any): EarnBurnConfiguration {
  return [
    config ? (config[0] ? "Y" : "N") : "",
    config ? (config[1] ? "Y" : "N") : "",
    config ? config[2] : "",
  ];
}

export function createConfigurationGetter(configJsonPath: string) {
  const jsonContent = JSON.parse(
    fs.readFileSync(configJsonPath, "utf8")
  ) as ConfigurationMapper;
  return (
    category?: string,
    brand?: string,
    article?: string
  ): EarnBurnConfiguration | undefined => {
    if (category !== undefined && brand !== undefined && article !== undefined) {
      const config =
        jsonContent["SKU"][article] ||
        jsonContent["CAT|BRN"][`${category}|${brand}`];
      return convertConfig(config);
    } else if (category !== undefined) {
      const config = jsonContent["CAT"][category] ?? DEFAULT_CONFIG;
      return convertConfig(config);
    } else if (brand !== undefined) {
      return DEFAULT_CONFIG;
    } else {
      throw "invalid configuration";
    }
  };
}
