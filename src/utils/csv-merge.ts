import { createReadStream, createWriteStream, readdirSync, statSync } from "fs";
import { pipeline } from "stream/promises";
import path from "path";
import { Transform } from "stream";

export function getFilePaths(folderPath: string): string[] {
  // Get all files in directory
  const files = readdirSync(folderPath);

  // Map to full paths
  const filePaths = files.map((file) => path.join(folderPath, file));

  // Filter out directories
  return filePaths.filter((filePath) => statSync(filePath).isFile());
}

export async function mergeCsvFiles(
  inputFiles: string[],
  outputFile: string,
  headerRows: number
): Promise<void> {
  const outputStream = createWriteStream(outputFile);
  let isFirstFile = true;

  try {
    for (const file of inputFiles) {
      console.log("merging", file);
      const inputStream = createReadStream(file);

      // For first file, keep the headers
      if (isFirstFile) {
        await pipeline(inputStream, outputStream);
        isFirstFile = false;
      } else {
        // For subsequent files, skip 2 header lines
        let headerCount = 0;
        const transformStream = new Transform({
          transform(chunk, encoding, callback) {
            const data = chunk.toString();
            const lines = data.split("\n");
            console.log(lines.length)

            lines.forEach((line: string) => {
              if (headerCount >= headerRows) {
                this.push(line + "\n");
              }
              headerCount++;
            });

            callback();
          },
        });

        await pipeline(inputStream, transformStream, outputStream);
      }
    }
  } catch (error) {
    throw new Error(`Error merging CSV files: ${error.message}`);
  } finally {
    outputStream.end();
  }
}


  const folderPath =
    "/Users/pakawin_m/Library/CloudStorage/OneDrive-KingPowerGroup/[KPGDX-GWL] - Group-wide Loyalty - GWL - 07_Cutover Plan/cat-brand-master/20250624_094233/KPC_OFFLINE/ARTICLE";
  await mergeCsvFiles(getFilePaths(folderPath), folderPath + `.${ulid()}.csv`, 2);