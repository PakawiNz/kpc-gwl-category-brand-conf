import fs, { read } from "fs";
import { CHANNEL_CONFIGURATIONS } from "./channel.js";
import { FileType } from "./type.js";
import { ChunkWriteStream } from "../utils/chunk-file-stream.js";
import moment from "moment";
import { ulid } from "ulid";
import { SapFileConversionService } from "./conversion.js";
import { pipeToMultipleWritables } from "../utils/pipeline-to-multiple-writables.js";
import { createConfigurationGetter } from "./config.js";
import { listFilesInFolder } from "./lister.js";

export class SapToGwlWithConfService {
  startTime: string;
  sapFileConversionService: SapFileConversionService;
  constructor(configJsonPath: string) {
    this.startTime = moment().format("YYYYMMDD_HHmmss");
    const configurationGetter = createConfigurationGetter(configJsonPath);
    this.sapFileConversionService = new SapFileConversionService(
      configurationGetter
    );
  }
  async execute(
    sourceFileFolder: string,
    startDate: string = "" /* YYYYMMDD */
  ) {
    for (const file of listFilesInFolder(sourceFileFolder, startDate)) {
      const { path, fileType } = file;
      await this.readAndConvertAndUpload(path, fileType);
    }
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
          path: `./data/output/${this.startTime}/${channel.code}/${fileType}`,
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
}
