import lodash from "lodash";
import dotenv from "dotenv";

dotenv.config();

interface Any {
  [key: string]: any;
}

class ChannelConfiguration {
  client_id: string;
  client_secret: string;
  partnerCode: string;
  brandCode: string;
  branchCode: string;
  cachedAccessToken?: string;

  constructor(
    client_id: string,
    client_secret: string,
    partnerCode: string,
    brandCode: string,
    branchCode: string
  ) {
    this.client_id = client_id;
    this.client_secret = client_secret;
    this.partnerCode = partnerCode;
    this.brandCode = brandCode;
    this.branchCode = branchCode;
  }

  async getAccessToken(usingCache = true) {
    if (!usingCache || !this.cachedAccessToken) {
      const url = `https://open-api-powerpass.kingpower.com/api/v1/partners/access-token`;
      const data = {
        grant_type: "client_credentials",
        client_id: this.client_id,
        client_secret: this.client_secret,
      };
      try {
        const res = await fetch(url, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify(data),
        });
        const data_1 = await res.json();
        this.cachedAccessToken = data_1.access_token;
      } catch (e) {
        console.error(e);
        return "";
      }
    }
    return this.cachedAccessToken;
  }
  async getHeaderConfig(usingCache = true) {
    const token = await this.getAccessToken(usingCache);
    return {
      Authorization: `Bearer ${token}`,
      partnerCode: this.partnerCode,
      branchCode: this.branchCode,
      brandCode: this.brandCode,
    };
  }
}

function getConfigFromEnv(key: string) {
  return process.env[key]!.split("|") as [
    string,
    string,
    string,
    string,
    string
  ];
}

const CHANNEL_CONFIGURATIONS: ChannelConfiguration[] = [
  new ChannelConfiguration(...getConfigFromEnv("KPC_POS_CHANNEL_CONFIG")),
  new ChannelConfiguration(...getConfigFromEnv("KPD_POS_CHANNEL_CONFIG")),
  new ChannelConfiguration(...getConfigFromEnv("KPT_POS_CHANNEL_CONFIG")),
  new ChannelConfiguration(...getConfigFromEnv("KPC_COMMERCE_CHANNEL_CONFIG")),
  new ChannelConfiguration(...getConfigFromEnv("KPC_FIRSTER_CHANNEL_CONFIG")),
  new ChannelConfiguration(...getConfigFromEnv("KPC_CHINA_CHANNEL_CONFIG")),
  new ChannelConfiguration(...getConfigFromEnv("KPC_THT_CHANNEL_CONFIG")),
];
const CHANNELS = lodash.keyBy(CHANNEL_CONFIGURATIONS, "client_id");

class Privilege {
  code: string;
  discount: number;
  burnCarat: boolean;
  tierName: string;
  maxEarnRate: number;
  constructor(
    code: string,
    discount: number,
    burnCarat: boolean,
    tierName: string,
    maxEarnRate: number
  ) {
    this.code = code;
    this.discount = discount;
    this.burnCarat = burnCarat;
    this.tierName = tierName;
    this.maxEarnRate = maxEarnRate;
  }
}

const PRIVILEGES = [
  new Privilege("PWPGNV5", 5, true, "NAVY", 1),
  new Privilege("PWPGSL10", 10, true, "SCARLET", 1),
  new Privilege("PWPGCR20", 20, true, "CROWN", 2),
  new Privilege("PWPGVG20", 20, true, "VEGA", 3),
  new Privilege("PWPGCT20", 20, true, "CRYSTAL", 1),
  new Privilege("PWPGVVIP30", 30, true, "VVIP", 3),
  new Privilege("PWPGBD25", 25, false, "", 0),
  new Privilege("PWPGCTBD30", 30, false, "", 0),
];
const PRIVILEGES_MAP = lodash.keyBy(PRIVILEGES, "code");
// const TIER_MAP = lodash.keyBy(lodash.filter(PRIVILEGES, 'tierCode'), 'tierCode');

type BooleanString = "Y" | "N";
class Article {
  channel: ChannelConfiguration;
  sku: string;
  earn: BooleanString;
  burn: BooleanString;
  earnRate: number;
  constructor(
    channel: ChannelConfiguration,
    sku: string,
    earn: BooleanString,
    burn: BooleanString,
    earnRate: number
  ) {
    this.channel = channel;
    this.sku = sku;
    this.earn = earn;
    this.burn = burn;
    this.earnRate = earnRate;
  }
}

const ARTICLES = [
  new Article(CHANNELS.KPC_COMMERCE, "8328994", "Y", "Y", 1),
  new Article(CHANNELS.KPC_COMMERCE, "8328995", "Y", "Y", 1),
  new Article(CHANNELS.KPC_COMMERCE, "8328996", "Y", "Y", 1),
  new Article(CHANNELS.KPC_FIRSTER, "4860195", "Y", "Y", 1),
  new Article(CHANNELS.KPC_FIRSTER, "4894738", "Y", "Y", 1),
  new Article(CHANNELS.KPC_FIRSTER, "F1TRANSFERTOGWL", "N", "N", 0),
  new Article(CHANNELS.KPC_CHINA, "4148531", "Y", "Y", 1),
  new Article(CHANNELS.KPC_THT, "4148531", "Y", "Y", 1),
  new Article(CHANNELS.KPD_POS, "000000000000475974", "Y", "Y", 3),
  new Article(CHANNELS.KPD_POS, "000000000008098252", "Y", "Y", 2),
  new Article(CHANNELS.KPT_POS, "000000000000475974", "Y", "Y", 3),
  new Article(CHANNELS.KPT_POS, "000000000008098252", "Y", "Y", 2),
  new Article(CHANNELS.KPC_POS, "000000000000475974", "Y", "Y", 3),
  new Article(CHANNELS.KPC_POS, "000000000008098252", "Y", "Y", 2),
];

class Member {
  id: string;
  tier: string;
  constructor(id: string, tier: string) {
    this.id = id;
    this.tier = tier;
  }
}
const MEMBERS = [
  new Member("2423992", "NAVY"),
  new Member("2421914", "SCARLET"),
  new Member("2424015", "CROWN"),
  new Member("2421916", "VEGA"),
  new Member("2424018", "VVIP"),
  new Member("2421913", "CRYSTAL"),
  // =============================================
  // new Member("2421913", "CRYSTAL"),
  // new Member("2421914", "SCARLET"),
  // new Member("2421916", "VEGA"),
  // new Member("2423992", "NAVY"),
  // new Member("2424000", "SCARLET"),
  // new Member("2424015", "CROWN"),
  // new Member("2424017", "VEGA"),
  // new Member("2424018", "VVIP"),
  // new Member("2424020", "CRYSTAL"),
  // new Member("2424021", "NAVY"),
  // new Member("2424023", "SCARLET"),
  // new Member("2424026", "CROWN"),
  // new Member("2424027", "VEGA"),
  // new Member("2424029", "CRYSTAL"),
  // new Member("2424031", "CRYSTAL"),
  // new Member("2424035", "CRYSTAL"),
  // new Member("2424037", "CRYSTAL"),
  // new Member("2424042", "CRYSTAL"),
  // new Member("2424044", "SCARLET"),
  // new Member("2424045", "SCARLET"),
  // new Member("2424046", "SCARLET"),
  // new Member("2424048", "SCARLET"),
  // new Member("2424049", "SCARLET"),
  // new Member("2424050", "SCARLET"),
  // new Member("2424051", "NAVY"),
  // new Member("2424053", "SCARLET"),
  // new Member("2424055", "CROWN"),
  // new Member("2424057", "VEGA"),
  // new Member("2424059", "VVIP"),
  // new Member("2424062", "CRYSTAL"),
  // new Member("2424064", "SCARLET"),
  // new Member("2424068", "CROWN"),
  // new Member("2424069", "VEGA"),
  // new Member("2427756", "CRYSTAL"),
];

async function verifyEligibility(
  channel: ChannelConfiguration,
  memberId: string,
  privilege: Privilege,
  articles: Article[]
) {
  const url = `https://open-api-powerpass.kingpower.com/api/v1/channels/members/${memberId}/promo-codes/${privilege.code}/validation`;
  const res = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(await channel.getHeaderConfig()),
    },
    body: JSON.stringify({ skus: articles.map((sku) => sku.sku) }),
  });
  const eligibility = await res.json();
  if (res.status != 200) {
    console.log(`❌ Fail to validate`, url, eligibility, privilege);
    return;
  }
  let hasError = false;
  if (!privilege.burnCarat) {
    if (eligibility.validation.eligiblePaymentMethods.includes("CARAT")) {
      console.log(
        `❌ CARAT should "not eligible" be in ${privilege.code} ${eligibility.validation.eligiblePaymentMethods}`
      );
      hasError = true;
    }
  } else {
    if (eligibility.validation.restrictPaymentMethods.includes("CARAT")) {
      console.log(
        `❌ CARAT should "eligible" be in ${privilege.code} ${eligibility.validation.restrictPaymentMethods}`
      );
      hasError = true;
    }
  }
  const skus = articles.map((article) => article.sku);
  if (skus.some((sku) => !eligibility.validation.eligibleSkus.includes(sku))) {
    console.log(
      `❌ All SKUS should "eligible" in ${privilege.code} EXP:${skus} ACT:${eligibility.validation.eligibleSkus}`
    );
    hasError = true;
  }
  if (hasError) {
    // console.log(eligibility);
  } else {
    console.log(`✅ valid eligibilty`, skus);
  }
}

async function getAndVerifyMemberPrivilege(
  channel: ChannelConfiguration,
  member: Member
): Promise<Privilege[]> {
  const res = await fetch(
    `https://open-api-powerpass.kingpower.com/api/v1/members/${member.id}/privileges`,
    {
      method: "GET",
      headers: {
        "Content-Type": "application/json",
        ...(await channel.getHeaderConfig()),
      },
    }
  );
  const result = await res.json();
  const privileges = result.data
    .map((p: Any) => PRIVILEGES_MAP[p.promoCode])
    .filter((pv: any) => !!pv) as Privilege[];
  if (privileges.length === 0) {
    console.log(`❌ No privilege found for ${member.id}`);
    return [];
  } else if (
    !privileges.some((privilege) => privilege.tierName == member.tier)
  ) {
    console.log(`❌ Tier invalid for ${member.id} EXP: ${member.tier}`, privileges);
    return [];
  } else {
    console.log(`✅ Tier valid for ${member.id} EXP: ${member.tier}`);
    return privileges;
  }
}

async function verifyEarn(
  channel: ChannelConfiguration,
  memberId: string,
  privilege: Privilege,
  articles: Article[]
) {
  const res = await fetch(
    "https://open-api-powerpass.kingpower.com/api/v1/channels/transactions/calculations/earns",
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...(await channel.getHeaderConfig()),
      },
      body: JSON.stringify({
        gwlNo: memberId,
        items: articles.map((article) => ({
          sku: article.sku,
          quantity: 1,
          originalPrice: 1000,
          netAmount: 1000,
        })),
        gwlPromoCodes: [],
        paymentMethods: [
          {
            paymentCode: "01",
            amount: 1000 * articles.length,
          },
        ],
      }),
    }
  );
  const skus = articles.map((article) => [article.sku, article.earnRate]);
  const expectedPoint = lodash.sum(
    articles.map((article) => Math.min(article.earnRate, privilege.maxEarnRate) * 40)
  );
  const result = await res.json();
  if (res.status != 200) {
    console.log(`❌ Fail to validate`, result, res.status);
  } else if (result.point != expectedPoint) {
    console.log(
      `❌ invalid earn: EXP:${expectedPoint} ACT:${result.point}`,
      skus
    );
  } else {
    console.log(`✅ valid earn`, skus);
  }
}

async function verifyBurn(
  channel: ChannelConfiguration,
  memberId: string,
  privilege: Privilege,
  articles: Article[]
) {
  const res = await fetch(
    "https://open-api-powerpass.kingpower.com/api/v1/channels/transactions/calculations/burns",
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...(await channel.getHeaderConfig()),
      },
      body: JSON.stringify({
        gwlNo: memberId,
        items: articles.map((article) => ({
          sku: article.sku,
          quantity: 1,
          originalPrice: 1000,
          netAmount: 1000,
        })),
      }),
    }
  );
  const result = await res.json();
  if (res.status != 200) {
    console.log(`❌ Fail to validate`, result, res.status);
    // console.log(`❌ Invalid burn EXP:${expectedPoint} ACT:${result.point} [${skus}]`);
    // console.log(result)
  } else {
    const caratWallet = result.wallets.find((w: Any) => w.currency === "CARAT");
    const eligibleSkus = caratWallet.validation.eligibleSkus;
    const restrictSkus = caratWallet.validation.restrictSkus;
    let hasError = false;
    for (const article of articles) {
      if (article.burn == "Y") {
        if (
          !eligibleSkus.includes(article.sku) ||
          restrictSkus.includes(article.sku)
        ) {
          console.log(`❌ invalid burn: ${article.sku} should "eligible"`);
          hasError = true;
          continue;
        }
      } else if (article.burn == "N") {
        if (
          eligibleSkus.includes(article.sku) ||
          !restrictSkus.includes(article.sku)
        ) {
          console.log(`❌ invalid burn: ${article.sku} should not "eligible"`);
          hasError = true;
          continue;
        }
      }
    }
    if (hasError) {
      // console.log(result);
    } else {
      console.log(`✅ valid burn`, eligibleSkus, restrictSkus);
    }
  }
}

async function main() {
  const memberPrivileges: { [key: Member["id"]]: Privilege[] } = {};
  const channel = CHANNEL_CONFIGURATIONS[0];
  for (const member of MEMBERS) {
    memberPrivileges[member.id] = await getAndVerifyMemberPrivilege(
      channel,
      member
    );
  }

  for (const channel of CHANNEL_CONFIGURATIONS) {
    console.log(
      "===================================================================================="
    );
    console.log(channel.partnerCode, channel.brandCode, channel.branchCode);
    if (!(await channel.getAccessToken())) {
      console.log("❌ Failed to get access token");
      return;
    }
    console.log("✅ Got access token");
    for (const member of MEMBERS) {
      console.log("==========================================");
      console.log(member.id, member.tier);
      const privileges = memberPrivileges[member.id];
      for (const privilege of privileges) {
        if (privilege.tierName) {
          const articles = ARTICLES.filter((a) => a.channel === channel);
          await verifyEligibility(channel, member.id, privilege, articles);
          await verifyEarn(channel, member.id, privilege, articles);
          await verifyBurn(channel, member.id, privilege, articles);
        }
      }
    }
    // const memberId = '2421916';
    // const privileges = await getMemberPrivilege(channel, memberId);

    // await verifyBurn(channel);
    // await verifyEarn(channel, [privileges[1]]);
  }
}

main();
