import fs from "fs";
import moment from "moment";
import { ulid } from "ulid";
import { ChunkWriteStream } from "../utils/chunk-file-stream.js";
import { pipeToMultipleWritables } from "../utils/pipeline-to-multiple-writables.js";
import { CHANNEL_CONFIGURATIONS } from "./channel.js";
import { createConfigurationGetter } from "./config.js";
import { SapFileConversionService } from "./conversion.js";
import { convertCostCenterFiles } from "./costcenter.js";
import { listFilesInFolder } from "./lister.js";
import { FileType } from "./type.js";
import { Uploader } from "./upload.js";
import { getAllCategoryAndBrand } from "./combination.js";
import path from "path";

export class SapToGwlWithConfService {
  sourceFileFolder: string;
  destinationFileFolder: string;
  startDate: string = ""; /* YYYYMMDD */
  startTime: string;
  sapFileConversionService: SapFileConversionService;
  constructor(
    configJsonPath: string,
    sourceFileFolder: string,
    destinationFileFolder: string,
    startDate: string = ""
  ) {
    this.sourceFileFolder = sourceFileFolder;
    this.destinationFileFolder = destinationFileFolder;
    this.startDate = startDate;
    this.startTime = moment().format("YYYYMMDD_HHmmss");
    const configurationGetter = createConfigurationGetter(configJsonPath);
    this.sapFileConversionService = new SapFileConversionService(
      configurationGetter
    );
  }
  listFilesInFolder() {
    return listFilesInFolder(this.sourceFileFolder, this.startDate);
  }
  async executeCostCenterConfig(): Promise<void> {
    convertCostCenterFiles(
      listFilesInFolder(this.sourceFileFolder, this.startDate),
      `${this.destinationFileFolder}/COST_CENTER_${this.startTime}.csv`
    );
  }
  async executeSkuConfig(): Promise<void> {
    const SKU_CONFIG_TYPE = [
      // FileType.ARTICLE,
      FileType.BRAND,
      FileType.CATEGORY,
    ];
    const files = listFilesInFolder(this.sourceFileFolder, this.startDate);
    // files must be correctly ordered
    for (const file of files) {
      const { path, fileType } = file;
      if (SKU_CONFIG_TYPE.includes(fileType)) {
        await this.readAndConvertAndUpload(path, fileType);
      }
    }
  }
  async executeSkuMaster(): Promise<void> {
    await getAllCategoryAndBrand(
      listFilesInFolder(this.sourceFileFolder, this.startDate),
      `${this.destinationFileFolder}/CAT_BRN_MASTER_${this.startTime}.csv`,
      this.sapFileConversionService.confugurationGetter
    );
  }
  async readAndConvertAndUpload(
    filePath: string,
    fileType: FileType
  ): Promise<void> {
    const readStream = fs.createReadStream(filePath, { encoding: "utf-8" });
    const writeStreams = await Promise.all(
      CHANNEL_CONFIGURATIONS.map(async (channel) => {
        const { headers, convert } =
          this.sapFileConversionService.getHeaderAndConverter(
            fileType,
            channel
          );
        let firstLine = true;
        let sapHeaders: string[];
        return new ChunkWriteStream({
          path: `${this.destinationFileFolder}/${this.startTime}/${channel.code}/${fileType}`,
          filename: `${moment().format("YYYYMMDDHHmmss")}_${ulid()}`,
          extension: "csv",
          getFileHeader: () => `From: ${filePath}\n` + headers.join(", "),
          transformLine(line: string) {
            if (firstLine) {
              firstLine = false;
              sapHeaders = line.split("|");
            } else {
              const splittedLine = line.split("|");
              const record = Object.fromEntries(
                sapHeaders.map((header, i) => [header, splittedLine[i]])
              );
              return convert(record)?.join(",");
            }
          },
        });
      })
    );
    await pipeToMultipleWritables(readStream, writeStreams);
  }
  async executeUploadMaster(folderPath: string) {
    const uploader = new Uploader();
    const channelCodes = CHANNEL_CONFIGURATIONS.map((channel) => channel.code);
    const fileTypes = Object.values(FileType) as string[];
    const allFiles: [string, string, string][] = [];
    fs.readdirSync(folderPath).forEach((channelCode) => {
      if (channelCodes.includes(channelCode)) {
        fs.readdirSync(path.join(folderPath, channelCode)).forEach(
          (fileType) => {
            if (fileTypes.includes(fileType)) {
              fs.readdirSync(path.join(folderPath, channelCode, fileType)).forEach(
                (filePath) => {
                  if (filePath.toLowerCase().endsWith(".csv")) {
                    const fullPath = path.join(folderPath, channelCode, fileType, filePath)
                    allFiles.push([channelCode, fileType, fullPath]);
                  }
                }
              );
            }
          }
        );
      }
    });
    uploader.uploadAllFiles(allFiles)
  }
}
