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
  "/Users/pakawin_m/Library/CloudStorage/OneDrive-KingPowerGroup/[KPGDX-GWL] - Group-wide Loyalty - GWL - 07_Cutover Plan/cat-brand-master/";

import { convertXlsxConfigurationToJson } from "./sap-to-gwl-with-conf/md-master-xlsx-to-json.js";
import { SapToGwlWithConfService } from "./sap-to-gwl-with-conf/service.js";
import { FileType } from "./sap-to-gwl-with-conf/type.js";

async function main() {
  // ================================================ transform MD master
  convertXlsxConfigurationToJson(SKU_CONFIG_XLSX_PATH, SKU_CONFIG_JSON_PATH);

  // ================================================
  const service = new SapToGwlWithConfService(
    SKU_CONFIG_JSON_PATH,
    SOURCE_FOLDER,
    DESTINATION_FOLDER,
    '20250617'
  );

  // ================================================ prepare master for MD
  // await service.executeSkuMaster()

  // ================================================ build and upload sku configs
  const skuFolder = await service.executeSkuConfig();
  await service.executeUploadSkuConfig(skuFolder, [
    FileType.ARTICLE,
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
