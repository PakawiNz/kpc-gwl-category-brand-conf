import { CHANNEL_CONFIGURATIONS } from "./channel.js";
import lodash from "lodash";
import { FileType } from "./type.js";
import fs from 'fs';

interface GetChannelResponse {
  data: {
    code: string;
    id: string;
  }[];
}

export class Uploader {
  HOST: string;
  CLIENT_ID: string;
  CLIENT_SECRET: string;
  ENDPOINT_GET_TOKEN: string;
  channelsMapper?: { [key: string]: string };
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

  async getToken() {
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

  async getChannel(
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
      throw new Error(
        `failed with status ${response.status}: ${errorData}`
      );
    }
    const data = await response.json();
    return data;
  }

  async getProduct(accessToken: string, channelId: string) {
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
      throw new Error(
        `failed with status ${response.status}: ${errorData}`
      );
    }
    const data = await response.json();
    console.log(data);
    return data;
  }

  async upload(filePath: string, fileType: FileType) {
    await this.prepareUpload();
    // console.log(channelsMapper);
    //
    await this.importProductCategory()
  }

  async importProductCategory(
    accessToken: string,
    channelId: string,
    filePath: string
  ) {
    const file = await fs.readFile(filePath);
    const formData = new FormData();
    formData.append("file", new Blob([file]));

    console.log("🔫🔫🔫 import product category");
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
}
