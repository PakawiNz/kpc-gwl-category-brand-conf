import { Writable, WritableOptions, Readable } from "stream";
import { createWriteStream } from "fs";
import path from "path";

export interface MultiFileStreamerOptions extends WritableOptions {
  path?: string;
  filename?: string;
  extension?: string;
  maxFileSize?: number;
  transformLine?: (line: string) => string;
  getFilename?: (part: number) => string;
}

export class MultiFileStreamer extends Writable {
  private basePath: string;
  private filename: string;
  private extension: string;
  private maxFileSize: number;
  private part: number;
  private currentFileSize: number;
  private currentWriteStream: Writable | null;
  private generatedFiles: string[];
  private lineBuffer: string; // Buffer for incomplete lines
  private transformLine?: (line: string) => string;
  private getFilename?: (part: number) => string;

  constructor(options: MultiFileStreamerOptions) {
    // decodeStrings: false ensures that if strings are passed, they are not re-encoded.
    // We'll handle buffer to string conversion manually for line processing.
    super({ ...options, decodeStrings: false });

    // Configuration
    this.basePath = options.path || ".";
    this.filename = options.filename || "output";
    this.extension = options.extension || "txt";
    this.maxFileSize = options.maxFileSize || 10 * 1024 * 1024; // Default: 10 MB
    // State
    this.part = 0; // Start at 0, will be incremented before creating the first file
    this.currentFileSize = 0;
    this.currentWriteStream = null;
    this.generatedFiles = [];
    this.lineBuffer = "";
    this.transformLine = options.transformLine;
    this.getFilename =
      options.getFilename ?? ((part) => `${this.filename}-part-${part}`);

    // Start with the first file
    this._openNewFileStream();
  }

  private _generateFilename(): string {
    // part number is 1-based for filenames
    return path.join(
      this.basePath,
      this.getFilename!(this.part) + `.${this.extension}`
    );
  }

  protected _createWriteStream(filePath: string): Writable {
    return createWriteStream(filePath);
  }

  private _openNewFileStream(): Promise<void> {
    return new Promise((resolve, reject) => {
      const createNewStream = () => {
        this.part++; // Increment for the new file part number
        const filePath = this._generateFilename();
        console.log(`Creating new file: ${filePath}`);
        this.generatedFiles.push(filePath);
        this.currentWriteStream = this._createWriteStream(filePath);
        this.currentWriteStream.on("error", (err) => {
          this.emit("error", err);
          reject(err);
        });
        this.currentFileSize = 0;
        resolve();
      };

      if (this.currentWriteStream) {
        // Use the callback of .end() which is called after the stream has finished.
        this.currentWriteStream.end(() => {
          this.currentWriteStream = null;
          createNewStream();
        });
      } else {
        createNewStream();
      }
    });
  }
  _write(
    chunk: Buffer | string,
    encoding: BufferEncoding,
    callback: (error?: Error | null) => void
  ): void {
    this._asyncWrite(chunk).then(callback).catch(callback);
  }
  private async _asyncWrite(chunk: Buffer | string): Promise<undefined> {
    const chunkStr = Buffer.isBuffer(chunk) ? chunk.toString("utf8") : chunk;
    this.lineBuffer += chunkStr;

    let lastNewlineIndex = this.lineBuffer.lastIndexOf("\n");

    while (lastNewlineIndex !== -1) {
      let lineToWrite = this.lineBuffer.substring(0, lastNewlineIndex + 1);
      if (this.transformLine) {
        lineToWrite = lineToWrite
          .split("\n")
          .map((line) => this.transformLine!(line))
          .join("\n");
      }
      // Convert back to buffer for accurate size and writing
      const lineToWriteBuffer = Buffer.from(lineToWrite, "utf8");

      // If current file is not empty AND this line would exceed maxFileSize, switch files.
      // A single line larger than maxFileSize will still be written to a file by itself.
      if (
        this.currentFileSize > 0 &&
        this.currentFileSize + lineToWriteBuffer.length > this.maxFileSize
      ) {
        await this._openNewFileStream();
      }

      this.currentWriteStream!.write(lineToWriteBuffer);
      this.currentFileSize += lineToWriteBuffer.length;

      this.lineBuffer = this.lineBuffer.substring(lastNewlineIndex + 1);
      lastNewlineIndex = this.lineBuffer.lastIndexOf("\n");
    }
  }

  // This method is called when the source stream has ended
  _final(callback: (error?: Error | null) => void): void {
    // Write any remaining data in lineBuffer to the current file
    if (this.lineBuffer.length > 0) {
      let finalLine = this.lineBuffer;
      if (this.transformLine) {
        finalLine = finalLine
          .split("\n")
          .map((line) => this.transformLine!(line))
          .join("\n");
      }
      const remainingBuffer = Buffer.from(finalLine, "utf8");
      // If current file has content and remaining buffer would overflow, create new file
      if (
        this.currentFileSize > 0 &&
        this.currentFileSize + remainingBuffer.length > this.maxFileSize
      ) {
        this._openNewFileStream();
      }
      this.currentWriteStream!.write(remainingBuffer);
      // this.currentFileSize += remainingBuffer.length; // Not strictly necessary for the final write
      this.lineBuffer = ""; // Clear buffer
    }

    if (this.currentWriteStream) {
      this.currentWriteStream.end(() => {
        this.currentWriteStream = null; // Clear stream reference
        callback();
      });
    } else {
      callback(); // No stream was active or needed
    }
  }

  getGeneratedFiles(): string[] {
    return [...this.generatedFiles];
  }
}

/**
 * Pipes a readable stream to multiple writable streams, managing backpressure and lifecycle events.
 * @param readable The source Readable stream.
 * @param writables An array of Writable streams to pipe to.
 * @returns A Promise that resolves when all writable streams have finished, or rejects on error.
 */
export function pipeToMultipleWritables(
  readable: Readable,
  writables: Writable[]
): Promise<void> {
  return new Promise((resolve, reject) => {
    if (!writables || writables.length === 0) {
      // If no writables, consume the readable stream to prevent memory leaks
      readable.on("data", () => {}); // Consume data
      readable.on("end", () => resolve());
      readable.on("error", reject);
      return;
    }

    const numWritables = writables.length;
    let finishedWritables = 0;

    function onError(err: Error) {
      // Ensure cleanup happens only once
      readable.destroy(err);
      writables.forEach((writable) => writable.destroy(err));
      reject(err);
    }

    readable.on("data", (chunk) => {
      let allCanWrite = true;
      for (const writable of writables) {
        if (!writable.destroyed && !writable.write(chunk)) {
          allCanWrite = false;
        }
      }
      if (!allCanWrite) {
        readable.pause();
        // Wait for all streams that returned false to drain
        const drainingPromises = writables
          .filter((w) => !w.destroyed && w.writableNeedDrain)
          .map((w) => new Promise<void>((r) => w.once("drain", r)));

        Promise.all(drainingPromises)
          .then(() => {
            if (!readable.destroyed) readable.resume();
          })
          .catch(onError); // Should not happen with 'drain' but good practice
      }
    });

    readable.on("end", () => {
      writables.forEach((writable) => {
        if (!writable.destroyed) writable.end();
      });
    });

    readable.on("error", onError);

    writables.forEach((writable) => {
      writable.on("finish", () => {
        finishedWritables++;
        if (finishedWritables === numWritables) resolve();
      });
      writable.on("error", onError);
    });
  });
}
