import { DEFAULT_CONFIG } from "./config.js";
import { EarnBurnConfiguration } from "./type.js";

export class ChannelConfiguration {
  code: string;
  categoryConfig: EarnBurnConfiguration | undefined;
  brandConfig: EarnBurnConfiguration | undefined;
  articleConfig: EarnBurnConfiguration | undefined;
  trimPaddedZeroSku: boolean;
  constructor(
    code: string,
    categoryConfig: EarnBurnConfiguration | undefined,
    brandConfig: EarnBurnConfiguration | undefined,
    articleConfig: EarnBurnConfiguration | undefined,
    trimPaddedZeroSku: boolean
  ) {
    this.code = code;
    this.categoryConfig = categoryConfig;
    this.brandConfig = brandConfig;
    this.articleConfig = articleConfig;
    this.trimPaddedZeroSku = trimPaddedZeroSku;
  }
}

export const CHANNEL_CONFIGURATIONS = [
  new ChannelConfiguration(
    "KPD_OFFLINE",
    undefined,
    undefined,
    undefined,
    false
  ),
  new ChannelConfiguration(
    "KPT_OFFLINE",
    undefined,
    undefined,
    undefined,
    false
  ),
  new ChannelConfiguration(
    "KPC_OFFLINE",
    undefined,
    undefined,
    undefined,
    false
  ),
  new ChannelConfiguration(
    "KPC_ONLINE_TH",
    undefined,
    undefined,
    undefined,
    true
  ),
  new ChannelConfiguration(
    "KPC_ONLINE_CN",
    undefined,
    undefined,
    undefined,
    true
  ),
  new ChannelConfiguration(
    "KPC_ONLINE_THT",
    undefined,
    undefined,
    undefined,
    true
  ),
  new ChannelConfiguration(
    "KPC_ONLINE_FIRSTER",
    ["Y", "Y", 1],
    undefined,
    ["", "", ""],
    true
  ),
];
