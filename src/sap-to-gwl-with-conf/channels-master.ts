import { EarnBurnConfiguration } from "./type.js";

interface ChannelConfigurationParams
  extends Omit<ChannelConfiguration, "code" | "company" | "trimPaddedZeroSku"> {
  trimPaddedZeroSku?: boolean;
}
export class ChannelConfiguration {
  company: COMPANY;
  code: string;
  categoryConfig?: EarnBurnConfiguration;
  brandConfig?: EarnBurnConfiguration;
  articleConfig?: EarnBurnConfiguration;
  trimPaddedZeroSku: boolean;
  constructor(
    company: COMPANY,
    code: string,
    params?: ChannelConfigurationParams
  ) {
    this.company = company;
    this.code = code;
    this.categoryConfig = params?.categoryConfig;
    this.brandConfig = params?.brandConfig;
    this.articleConfig = params?.articleConfig;
    this.trimPaddedZeroSku = params?.trimPaddedZeroSku ?? false;
  }
}

export enum COMPANY {
  KPD = "KPD",
  KPT = "KPT",
  KPC = "KPC",
}

export const CHANNEL_CONFIGURATIONS = [
  new ChannelConfiguration(COMPANY.KPD, "KPD_OFFLINE"),
  new ChannelConfiguration(COMPANY.KPT, "KPT_OFFLINE"),
  new ChannelConfiguration(COMPANY.KPC, "KPC_OFFLINE"),
  new ChannelConfiguration(COMPANY.KPC, "KPC_ONLINE_TH", {
    trimPaddedZeroSku: true,
  }),
  new ChannelConfiguration(COMPANY.KPC, "KPC_ONLINE_CN", {
    trimPaddedZeroSku: true,
  }),
  new ChannelConfiguration(COMPANY.KPC, "KPC_ONLINE_THT", {
    trimPaddedZeroSku: true,
  }),
  new ChannelConfiguration(COMPANY.KPC, "KPC_ONLINE_FIRSTER", {
    categoryConfig: ["Y", "Y", 1],
    articleConfig: ["", "", ""],
    trimPaddedZeroSku: true,
  }),
];

export function buildCompanyMap<T>(
  builder: (company: COMPANY) => T
): Record<COMPANY, T> {
  const result: Record<COMPANY, T> = {} as any;
  for (const company in COMPANY) {
    result[company as COMPANY] = builder(company as COMPANY);
  }
  return result;
}
