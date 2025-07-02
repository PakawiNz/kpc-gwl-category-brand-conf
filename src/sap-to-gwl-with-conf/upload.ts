import { CHANNEL_CONFIGURATIONS } from "./channels-master.js";
import lodash from "lodash";
import { FileType } from "./type.js";
import fs from "fs";
import path from "path";
import timer from "../utils/timer.js";

interface GetChannelResponse {
  data: {
    code: string;
    id: string;
  }[];
}

export class Uploader {
  private HOST: string;
  private CLIENT_ID: string;
  private CLIENT_SECRET: string;
  private ENDPOINT_GET_TOKEN: string;
  private channelsMapper?: { [key: string]: string };
  constructor() {
    this.HOST = process.env.POWERPASS_OPEN_API_HOST ?? "";
    this.CLIENT_ID = process.env.POWERPASS_OPEN_API_CLIENT_ID ?? "";
    this.CLIENT_SECRET = process.env.POWERPASS_OPEN_API_CLIENT_SECRET ?? "";
    this.ENDPOINT_GET_TOKEN =
      process.env.POWERPASS_OPEN_API_ENDPOINT_GET_TOKEN ?? "";
  }

  private async prepareUpload() {
    if (this.channelsMapper) return;
    const result = await this.getToken();
    const channels = await this.getChannel(result["access_token"], "");
    this.channelsMapper = lodash.mapValues(
      lodash.keyBy(CHANNEL_CONFIGURATIONS, "code"),
      (channel) =>
        channels.data.find((c) => c.code === channel.code)?.id as string
    );
  }

  private async getToken() {
    console.log("🔫🔫🔫 get token");
    const response = await fetch(this.ENDPOINT_GET_TOKEN, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body: new URLSearchParams({
        grant_type: "client_credentials",
        client_id: this.CLIENT_ID,
        client_secret: this.CLIENT_SECRET,
      }),
    });
    const data = await response.json();
    if (response.status != 200) throw data;
    return data;
  }

  private async getChannel(
    accessToken: string,
    codeKeyword: string
  ): Promise<GetChannelResponse> {
    console.log("🔫🔫🔫 get channel");
    const response = await fetch(
      `https://${this.HOST}/admin/channels?searchBy=CODE&searchKeyword=${codeKeyword}&perPage=100`,
      {
        method: "GET",
        headers: {
          Authorization: `Bearer ${accessToken}`,
        },
      }
    );
    if (!response.ok) {
      const errorData = await response.text();
      console.error("failed:", response.status, errorData);
      throw new Error(`failed with status ${response.status}: ${errorData}`);
    }
    const data = await response.json();
    return data;
  }

  private async getProduct(accessToken: string, channelId: string) {
    console.log("🔫🔫🔫 get product");
    const response = await fetch(
      `https://${this.HOST}/admin/channels/${channelId}/products`,
      {
        method: "GET",
        headers: {
          Authorization: `Bearer ${accessToken}`,
        },
      }
    );
    if (!response.ok) {
      const errorData = await response.text();
      console.error("failed:", response.status, errorData);
      throw new Error(`failed with status ${response.status}: ${errorData}`);
    }
    const data = await response.json();
    console.log(data);
    return data;
  }

  async uploadSkuConfig(filePaths: [string, string, string][]) {
    await this.prepareUpload();
    for (const [channelCode, fileType, filePath] of filePaths) {
      await timer(1000)
      const accessToken = (await this.getToken())["access_token"];
      console.log(channelCode, fileType);
      switch (fileType) {
        case FileType.CATEGORY:
          await this.importProductCategory(
            accessToken,
            this.channelsMapper![channelCode],
            filePath
          );
          continue;
        case FileType.BRAND:
          await this.importProductBrand(
            accessToken,
            this.channelsMapper![channelCode],
            filePath
          );
          continue;
        case FileType.ARTICLE:
          await this.importProduct(
            accessToken,
            this.channelsMapper![channelCode],
            filePath
          );
          continue;
      }
    }
  }

  private async importProductCategory(
    accessToken: string,
    channelId: string,
    filePath: string
  ) {
    const fileBuffer = fs.readFileSync(filePath);
    const formData = new FormData();
    const fileName = path.basename(filePath);
    formData.append(
      "file",
      new Blob([fileBuffer], { type: "text/csv" }),
      fileName
    );

    console.log("🔫🔫🔫 import product category", fileName);
    const response = await fetch(
      `https://${this.HOST}/admin/channels/${channelId}/categories/import`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${accessToken}`,
        },
        body: formData,
      }
    );
    if (!response.ok) {
      const errorData = await response.text();
      console.error("Upload failed:", response.status, errorData);
      throw new Error(
        `Upload failed with status ${response.status}: ${errorData}`
      );
    }
  }

  private async importProductBrand(
    accessToken: string,
    channelId: string,
    filePath: string
  ) {
    const fileBuffer = fs.readFileSync(filePath);
    const formData = new FormData();
    const fileName = path.basename(filePath);
    formData.append(
      "file",
      new Blob([fileBuffer], { type: "text/csv" }),
      fileName
    );

    console.log("🔫🔫🔫 import product brand", fileName);
    const response = await fetch(
      `https://${this.HOST}/admin/channels/${channelId}/brands/import`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${accessToken}`,
        },
        body: formData,
      }
    );
    if (!response.ok) {
      const errorData = await response.text();
      console.error("Upload failed:", response.status, errorData);
      throw new Error(
        `Upload failed with status ${response.status}: ${errorData}`
      );
    }
  }

  private async importProduct(
    accessToken: string,
    channelId: string,
    filePath: string
  ) {
    const fileBuffer = fs.readFileSync(filePath);
    const formData = new FormData();
    const fileName = path.basename(filePath);
    formData.append(
      "file",
      new Blob([fileBuffer], { type: "text/csv" }),
      fileName
    );

    console.log("🔫🔫🔫 import product", fileName);
    const response = await fetch(
      `https://${this.HOST}/admin/channels/${channelId}/products/import`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${accessToken}`,
        },
        body: formData,
      }
    );
    if (!response.ok) {
      const errorData = await response.text();
      console.error("Upload failed:", response.status, errorData);
      throw new Error(
        `Upload failed with status ${response.status}: ${errorData}`
      );
    }
  }

  async uploadCostCenterConfig(filePath: string) {
    await timer(1000)
    const token = (await this.getToken())["access_token"];
    await this.importCostCenter(token, filePath);
  }

  private async importCostCenter(accessToken: string, filePath: string) {
    const fileBuffer = fs.readFileSync(filePath);
    const formData = new FormData();
    const fileName = path.basename(filePath);
    formData.append(
      "file",
      new Blob([fileBuffer], { type: "text/csv" }),
      fileName
    );

    console.log("🔫🔫🔫 import product", fileName);
    const response = await fetch(
      `https://${this.HOST}/admin/cost-centers/import`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${accessToken}`,
        },
        body: formData,
      }
    );
    if (!response.ok) {
      const errorData = await response.text();
      console.error("Upload failed:", response.status, errorData);
      throw new Error(
        `Upload failed with status ${response.status}: ${errorData}`
      );
    }
  }
}
