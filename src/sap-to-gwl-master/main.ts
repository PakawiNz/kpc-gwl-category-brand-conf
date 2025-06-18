import { createReadStream } from "fs";

const sourceFilePath =
  "./data/kpg-sap-s3-outbound-prod/S4P_ARTICLE_FULL_20250616_144943_1_4.CSV"; // Your large source file
const outputDirectory = "./output_chunks"; // Where to save the parts

// --- Setup ---
// In a real scenario, you'd have a large file.
// For this demo, let's ensure the source and output directories exist.
import { S3Client } from "@aws-sdk/client-s3";
import fs from "fs";
import { Writable } from "stream"; // Added Readable
import { MultiFileStreamer } from "../utils/multi-file-stream.js";
import S3WriteStream from "../utils/s3-write-stream.js";

if (!fs.existsSync(sourceFilePath)) {
  console.log("Creating a dummy large source file...");
  // Create a 25MB dummy file for demonstration
  fs.writeFileSync(sourceFilePath, Buffer.alloc(25 * 1024 * 1024, "a"));
}
if (!fs.existsSync(outputDirectory)) {
  fs.mkdirSync(outputDirectory);
}
// --- End Setup ---

import { pipeToMultipleWritables } from "../utils/multi-file-stream.js"; // Import the new function
// 1. Create a readable stream from your large source file
const sourceStream = createReadStream(sourceFilePath);
const s3Client = new S3Client({});

class S3MultiFileStreamer extends MultiFileStreamer {
  _createWriteStream(filePath: string): Writable {
    return new S3WriteStream(s3Client, {
      Bucket: "kpg-sap-s3-outbound-dev" as string,
      Key: filePath,
    });
  }
}

// 2. Create instances of our custom multi-file writable streams
const chunker1 = new S3MultiFileStreamer({
  path: outputDirectory,
  filename: "data-chunk-alpha",
  extension: "csv",
  maxFileSize: 5 * 1024 * 1024, // Split into 5 MB files
  transformLine(line: string) {
    return line.split("|")[0];
  },
});

const chunker2 = new S3MultiFileStreamer({
  path: outputDirectory,
  filename: "data-chunk-beta", // Different filename for the second chunker
  extension: "txt", // Different extension
  maxFileSize: 3 * 1024 * 1024, // Different max file size
  transformLine(line: string) {
    // Example: uppercase and take first 2 parts
    return line.split('|')[1] ?? '??';
  },
});

const chunkers = [chunker1, chunker2];

// 3. Manually handle data flow to multiple chunkers
console.log("Starting the streaming and chunking process...");
pipeToMultipleWritables(sourceStream, chunkers)
  .then(() => {
    console.log("All chunkers successfully finished streaming.");
  })
  .catch((err) => {
    console.error("An error occurred during the multi-stream piping process:", err);
  });
