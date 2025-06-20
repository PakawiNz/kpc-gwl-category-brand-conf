import dotenv from "dotenv";
dotenv.config();

// ================================================================================================================
const SKU_CONFIG_XLSX_PATH =
  "/Users/pakawin_m/Library/CloudStorage/OneDrive-KingPowerGroup/[KPGDX-GWL] - Group-wide Loyalty - GWL - 07_Cutover Plan/cat-brand-master/GWL-SKU-CAT-BRAND-MASTER.xlsx";
const SKU_CONFIG_JSON_PATH =
  "/Users/pakawin_m/workspace/kpc-gwl-category-brand-conf/data/sku-configuration-prod/GWL-SKU-CAT-BRAND-MASTER.xlsx";
const SOURCE_FOLDER =
  "/Users/pakawin_m/workspace/kpc-gwl-category-brand-conf/data/kpg-sap-s3-outbound-prod";
const DESTINATION_FOLDER =
  "/Users/pakawin_m/Library/CloudStorage/OneDrive-KingPowerGroup/[KPGDX-GWL] - Group-wide Loyalty - GWL - 07_Cutover Plan/cat-brand-master/";

const OVERRIDE_FOLDER_NAME = '20250620_192034'

import { convertXlsxConfigurationToJson } from "./conf-xlsx-to-json/main.js";
import { SapToGwlWithConfService } from "./sap-to-gwl-with-conf/service.js";
import path from "path";
async function main() {
  // convertXlsxConfigurationToJson(SKU_CONFIG_XLSX_PATH, SKU_CONFIG_JSON_PATH);
  const service = new SapToGwlWithConfService(
    SKU_CONFIG_JSON_PATH,
    SOURCE_FOLDER,
    DESTINATION_FOLDER
  );
  // console.log(JSON.stringify(service.listFilesInFolder().map(a => a.path), null, 2));
  // await service.executeSkuConfig();
  // await service.executeCostCenterConfig();
  // await service.executeSkuMaster()
  await service.executeUploadMaster(
    path.join(DESTINATION_FOLDER, OVERRIDE_FOLDER_NAME || service.startTime)
  );
  console.log("complete");
}

// ================================================================================================================
main();
