import fs from "fs";
import path from "path";
import { createReadStream } from "fs";
import { createInterface } from "readline";
import { Transform } from "stream";
import { Any } from "../type.js";

/**
 * Reads a CSV file and streams its data as objects.
 * @param filePath The path to the CSV file.
 * @returns A Transform stream that emits objects representing each row.
 * @throws Error if the file cannot be read.
 */
export function getCSVReadStream(filePath: string, delimit = ","): Transform {
  const transform = new Transform({
    objectMode: true,
    transform(chunk, encoding, callback) {
      callback(null, chunk);
    },
  });

  (async () => {
    try {
      const resolvedPath = path.resolve(filePath);
      if (!fs.existsSync(resolvedPath)) {
        transform.emit(
          "error",
          new Error(`File not found at path: ${resolvedPath}`)
        );
        return;
      }

      let headers: string[] = [];

      const fileStream = createReadStream(resolvedPath, { encoding: "utf8" });
      const rl = createInterface({
        input: fileStream,
        crlfDelay: Infinity,
      });

      let isFirstLine = true;

      for await (const line of rl) {
        if (isFirstLine) {
          headers = line
            .split(delimit)
            .map((header) =>
              header
                .trim()
                .toUpperCase()
                .replace(/\s+/g, "_")
                .replace(/\W/g, "")
            );
          isFirstLine = false;
          continue;
        }

        // Split by delimiter but respect quotes
        // Escape special characters in delimiter for use in regex
        const escapedDelimit = delimit.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
        const regex = new RegExp(
          `(?:^|${escapedDelimit})("(?:[^"]*(?:""[^"]*)*)"|[^${escapedDelimit}]*)`,
          "g"
        );
        const values =
          line
            .match(regex)
            ?.map((val) => val.replace(new RegExp(`^${escapedDelimit}`), "")) // Remove leading delimiter
            ?.map((val) => {
              if (val.startsWith('"') && val.endsWith('"')) {
                return val.slice(1, -1).replace(/""/g, '"'); // Remove quotes and handle escaped quotes
              }
              return val;
            }) || [];

        const row: Any = {};
        headers.forEach((header, index) => {
          row[header] = values[index] || "";
        });
        transform.push(row);
      }

      transform.end();
    } catch (error) {
      console.error("Error reading CSV file:", error);
      transform.emit("error", error);
    }
  })();

  return transform;
}

export async function readCSVStream(
  streamFile: Transform,
  readFunction: (row: Record<string, string>) => void
): Promise<void> {
  await new Promise((resolve, reject) => {
    streamFile
      .on("data", readFunction)
      .on("end", () => resolve(undefined))
      .on("error", (e) => reject(e));
  });
}

export function getCSV(csvPath: string, delimiter: string) {
  const lines = fs.readFileSync(csvPath, "utf8").split("\n");
  let headers;
  let isFirstLine = true;
  const data = [];
  for (const line of lines) {
    if (isFirstLine) {
      headers = line.split(delimiter);
      isFirstLine = false;
    } else {
      const row = line.split(delimiter);
      data.push(Object.fromEntries(headers!.map((head, i) => [head, row[i]])));
    }
  }
  return data;
}
