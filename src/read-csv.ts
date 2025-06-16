import fs from "fs";
import path from "path";
import { Any } from "./type.js";
import { createReadStream } from 'fs';
import { createInterface } from 'readline';
import { Transform } from 'stream';

/**
 * Reads a CSV file and streams its data as objects.
 * @param filePath The path to the CSV file.
 * @returns A Transform stream that emits objects representing each row.
 * @throws Error if the file cannot be read.
 */
export function read_csv(filePath: string): Transform {
  const transform = new Transform({
    objectMode: true,
    transform(chunk, encoding, callback) {
      callback(null, chunk);
    }
  });

  (async () => {
    try {
      const resolvedPath = path.resolve(filePath);
      if (!fs.existsSync(resolvedPath)) {
        transform.emit('error', new Error(`File not found at path: ${resolvedPath}`));
        return;
      }

      let headers: string[] = [];
      
      const fileStream = createReadStream(resolvedPath, { encoding: 'utf8' });
      const rl = createInterface({
        input: fileStream,
        crlfDelay: Infinity
      });

      let isFirstLine = true;

      for await (const line of rl) {
        if (isFirstLine) {
          headers = line.split(',').map(header =>
            header.trim().toUpperCase().replace(/\s+/g, '_').replace(/\W/g, '')
          );
          isFirstLine = false;
          continue;
        }

        // Split by comma but respect quotes
        const values = line.match(/(?:^|,)("(?:[^"]*(?:""[^"]*)*)"|[^,]*)/g)
          ?.map(val => val.replace(/^,/, '')) // Remove leading comma
          ?.map(val => {
            if (val.startsWith('"') && val.endsWith('"')) {
              return val.slice(1, -1).replace(/""/g, '"'); // Remove quotes and handle escaped quotes
            }
            return val;
          }) || [];

        const row: Any = {};
        headers.forEach((header, index) => {
          let value = values[index] || '';
          // Try to parse JSON if the value looks like a JSON string
          if (value.startsWith('[') || value.startsWith('{')) {
            try {
              value = JSON.parse(value);
            } catch (e) {
              // Keep as string if parsing fails
            }
          }
          row[header] = value;
        });
        transform.push(row);
      }

      transform.end();

    } catch (error) {
      console.error("Error reading CSV file:", error);
      transform.emit('error', error);
    }
  })();

  return transform;
}


