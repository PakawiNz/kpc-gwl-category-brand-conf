import dotenv from "dotenv";
dotenv.config();

// ================================================================================================================
const SKU_CONFIG_XLSX_PATH =
  "/Users/pakawin_m/Library/CloudStorage/OneDrive-KingPowerGroup/[KPGDX-GWL] - Group-wide Loyalty - GWL - 07_Cutover Plan/cat-brand-master/GWL-SKU-CAT-BRAND-MASTER.xlsx";
const SKU_CONFIG_JSON_PATH =
  "/Users/pakawin_m/workspace/kpc-gwl-category-brand-conf/data/sku-configuration-prod/GWL-SKU-CAT-BRAND-MASTER.json";
const SOURCE_FOLDER =
  "/Users/pakawin_m/workspace/kpc-gwl-category-brand-conf/data/kpg-sap-s3-outbound-prod";
const DESTINATION_FOLDER =
  "/Users/pakawin_m/workspace/kpc-gwl-category-brand-conf/data/gwl-category-brand-master";
// const DESTINATION_FOLDER =
//   "/Users/pakawin_m/Library/CloudStorage/OneDrive-KingPowerGroup/[KPGDX-GWL] - Group-wide Loyalty - GWL - 07_Cutover Plan/cat-brand-master/";

import moment from "moment";
import { convertXlsxConfigurationToJson } from "./sap-to-gwl-with-conf/md-master-xlsx-to-json.js";
import { SapToGwlWithConfService } from "./sap-to-gwl-with-conf/service.js";
import { FileType } from "./sap-to-gwl-with-conf/type.js";

async function main() {
  // ================================================ sync data from S3
  const today = moment().format("YYYYMMDD");
  console.log(
    `aws s3 sync --exclude="*" --include "*_${today}_*" s3://${process.env.PRD_AWS_BUCKET_NAME} ./data`
  );

  // ================================================ transform MD master
  await convertXlsxConfigurationToJson(SKU_CONFIG_XLSX_PATH, {
    jsonPath: SKU_CONFIG_JSON_PATH,
    s3UploadParams: [
      {
        key: "cat-brand-sku-config.json",
        bucket: process.env.PRD_AWS_BUCKET_NAME!,
        accessKeyId: process.env.PRD_AWS_ACCESS_KEY_ID!,
        secretAccessKey: process.env.PRD_AWS_SECRET_ACCESS_KEY!,
      },
      {
        key: "cat-brand-sku-config.json",
        bucket: process.env.DEV_AWS_BUCKET_NAME!,
        accessKeyId: process.env.DEV_AWS_ACCESS_KEY_ID!,
        secretAccessKey: process.env.DEV_AWS_SECRET_ACCESS_KEY!,
      },
    ],
  });

  // ================================================
  const service = new SapToGwlWithConfService(
    SKU_CONFIG_JSON_PATH,
    SOURCE_FOLDER,
    DESTINATION_FOLDER,
    today
  );

  // ================================================ prepare master for MD
  // await service.executeSkuMaster()

  // ================================================ build and upload sku configs
  const skuFolder = await service.executeSkuConfig([
    // FileType.ARTICLE,
    FileType.CATEGORY,
    FileType.BRAND,
  ]);
  await service.executeUploadSkuConfig(skuFolder, [
    // FileType.ARTICLE,
    FileType.CATEGORY,
    FileType.BRAND,
  ]);

  // ================================================ build and upload cost center
  const costCenterCsv = await service.executeCostCenterConfig();
  await service.executeUploadCostCenterConfig(costCenterCsv);

  // ================================================
  console.log("complete");
}

// ================================================================================================================
main();
