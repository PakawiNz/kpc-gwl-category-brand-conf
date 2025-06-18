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

export const CHANNEL_DEFAULT_CONFIG: EarnBurnConfiguration = ["N", "N", 0];

export const CHANNEL_CONFIGURATIONS = [
  new ChannelConfiguration(
    "KPD_OFFLINE",
    undefined,
    CHANNEL_DEFAULT_CONFIG,
    undefined,
    false
  ),
  new ChannelConfiguration(
    "KPT_OFFLINE",
    undefined,
    CHANNEL_DEFAULT_CONFIG,
    undefined,
    false
  ),
  new ChannelConfiguration(
    "KPC_OFFLINE",
    undefined,
    CHANNEL_DEFAULT_CONFIG,
    undefined,
    false
  ),
  new ChannelConfiguration(
    "KPC_ONLINE_TH",
    undefined,
    CHANNEL_DEFAULT_CONFIG,
    undefined,
    true
  ),
  new ChannelConfiguration(
    "KPC_ONLINE_CN",
    undefined,
    CHANNEL_DEFAULT_CONFIG,
    undefined,
    true
  ),
  new ChannelConfiguration(
    "KPC_ONLINE_THT",
    undefined,
    CHANNEL_DEFAULT_CONFIG,
    undefined,
    true
  ),
  new ChannelConfiguration(
    "KPC_ONLINE_FIRSTER",
    ["Y", "Y", 1],
    CHANNEL_DEFAULT_CONFIG,
    ["", "", ""],
    true
  ),
];