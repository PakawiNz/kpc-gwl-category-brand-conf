import path from "path";
import fs from "fs";
import { timestampString } from "../utils/timestamp-string.js";
import { buildCompanyMap, COMPANY } from "./channels-master.js";
import { convertCostCenterFiles as readAndConvertCostCenterFile } from "./convert-sap-to-gwl-costcenter-file.js"; // Corrected import path
import {
  getConversionWritables,
  listConvertedSkuConfigs,
  SapFileConversionService,
} from "./convert-sap-to-gwl-product-files.js";
import { createConfigurationGetter } from "./md-master-function.js";
import { getAllCategoryAndBrand } from "./md-master-initlize.js";
import { listFilesInFolder } from "./sap-master-listing.js";
import { FileType } from "./type.js";
import { Uploader } from "./upload.js";
import { Writable } from "stream";
import {
  closeAllWritables,
  pipeToMultipleWritables,
} from "../utils/pipeline-to-multiple-writables.js";

export class SapToGwlWithConfService {
  sourceFileFolder: string;
  destinationFileFolder: string;
  startDate: string = ""; /* YYYYMMDD */
  startTime: string;
  sapFileConversionServices: Record<COMPANY, SapFileConversionService>;
  constructor(
    configJsonPath: string,
    sourceFileFolder: string,
    destinationFileFolder: string,
    startDate: string = ""
  ) {
    this.sourceFileFolder = sourceFileFolder;
    this.destinationFileFolder = destinationFileFolder;
    this.startDate = startDate;
    this.startTime = timestampString();
    this.sapFileConversionServices = buildCompanyMap(
      (company) =>
        new SapFileConversionService(
          createConfigurationGetter(configJsonPath, company)
        )
    );
  }
  async executeSkuMaster(): Promise<void> {
    await getAllCategoryAndBrand(
      listFilesInFolder(this.sourceFileFolder, ""),
      this.destinationFileFolder,
      Object.values(this.sapFileConversionServices).map(
        (s) => s.confugurationGetter
      )
    );
  }
  async executeSkuConfig(includedFileTypes?: FileType[]): Promise<string> {
    const SKU_CONFIG_TYPE = [
      FileType.ARTICLE,
      FileType.BRAND,
      FileType.CATEGORY,
    ];
    includedFileTypes =
      includedFileTypes?.filter((ft) => SKU_CONFIG_TYPE.includes(ft)) ||
      SKU_CONFIG_TYPE;

    const files = listFilesInFolder(this.sourceFileFolder, this.startDate);
    const fileTypesWritable: Partial<Record<FileType, Writable[]>> = {};
    for (const fileType of includedFileTypes) {
      fileTypesWritable[fileType] = getConversionWritables(
        `${this.destinationFileFolder}/${this.startTime}`,
        fileType,
        this.sapFileConversionServices
      );
    }

    // files must be correctly ordered
    for (const file of files) {
      const { path, fileType } = file;
      if (includedFileTypes.includes(fileType)) {
        const readStream = fs.createReadStream(path, { encoding: "utf-8" });
        await pipeToMultipleWritables(
          readStream,
          fileTypesWritable[fileType]!,
          { closeWritablesOnEnd: false }
        );
      }
    }

    for (const writables of Object.values(fileTypesWritable)) {
      closeAllWritables(writables);
    }

    return path.join(this.destinationFileFolder, this.startTime);
  }
  async executeUploadSkuConfig(
    folderPath: string,
    includedFileTypes: FileType[]
  ) {
    const uploader = new Uploader();
    await uploader.uploadSkuConfig(
      await listConvertedSkuConfigs(folderPath, includedFileTypes)
    );
  }
  async executeCostCenterConfig(): Promise<string> {
    const csvfilePath = path.join(
      this.destinationFileFolder,
      this.startTime,
      `COST_CENTER_${this.startTime}.csv`
    );
    fs.mkdirSync(path.dirname(csvfilePath), { recursive: true });
    readAndConvertCostCenterFile(
      listFilesInFolder(this.sourceFileFolder, this.startDate),
      csvfilePath
    );
    return csvfilePath;
  }
  async executeUploadCostCenterConfig(filePath: string) {
    const uploader = new Uploader();
    await uploader.uploadCostCenterConfig(filePath);
  }
}
