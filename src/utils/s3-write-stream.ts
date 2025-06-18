// A custom Node.js Writable stream to upload data directly to Amazon S3, written in TypeScript.
//
// This implementation uses the AWS SDK for JavaScript v3 and the S3 multipart
// upload API, which is the most efficient way to handle streams of unknown size.
//
// To run this:
// 1. Make sure you have Node.js and TypeScript installed.
// 2. Install dependencies:
//    npm install @aws-sdk/client-s3
//    npm install -D @types/node typescript
// 3. Configure your AWS credentials (e.g., via environment variables,
//    an EC2 instance role, or an AWS credentials file).
// 4. Compile and run:
//    tsc your-file-name.ts
//    node your-file-name.js

import { Writable } from "stream";
import {
  S3Client,
  CreateMultipartUploadCommand,
  UploadPartCommand,
  CompleteMultipartUploadCommand,
  AbortMultipartUploadCommand,
  CreateMultipartUploadCommandInput,
  CompletedPart,
} from "@aws-sdk/client-s3";

// Define the required S3 parameters for the stream
interface S3WriteStreamParams {
  Bucket: string;
  Key: string;
}

/**
 * @class S3WriteStream
 * @extends Writable
 *
 * A Writable stream for uploading data to an S3 object.
 * This class handles the S3 multipart upload API automatically.
 */
class S3WriteStream extends Writable {
  private s3Client: S3Client;
  private s3Params: S3WriteStreamParams;
  private partSize: number;
  // Use Omit to exclude properties that are already in s3Params
  private s3Options: Omit<CreateMultipartUploadCommandInput, "Bucket" | "Key">;

  private uploadId: string | null = null;
  private uploadedParts: CompletedPart[] = [];
  private partNumber = 1;
  private buffer: Buffer = Buffer.alloc(0);
  private isInitializing = false;

  /**
   * @param {S3Client} s3Client - An instance of the AWS S3Client.
   * @param {S3WriteStreamParams} s3Params - Parameters for the S3 object (Bucket, Key).
   * @param {number} [partSize=5 * 1024 * 1024] - The size of each part in bytes. S3 minimum is 5MB.
   * @param {object} [s3Options={}] - Additional options for CreateMultipartUploadCommand (e.g., ContentType).
   */
  constructor(
    s3Client: S3Client,
    s3Params: S3WriteStreamParams,
    partSize: number = 5 * 1024 * 1024,
    s3Options: Omit<CreateMultipartUploadCommandInput, "Bucket" | "Key"> = {}
  ) {
    super();

    if (!s3Client || !s3Params.Bucket || !s3Params.Key) {
      throw new Error("S3Client, Bucket, and Key are required.");
    }

    if (partSize < 5 * 1024 * 1024) {
      console.warn(
        `Part size is less than the S3 recommended minimum of 5MB. This may cause issues.`
      );
    }

    this.s3Client = s3Client;
    this.s3Params = s3Params;
    this.partSize = partSize;
    this.s3Options = s3Options;
  }

  /**
   * Initializes the multipart upload with S3.
   * @private
   */
  private async _initialize(): Promise<void> {
    this.isInitializing = true;
    try {
      const command = new CreateMultipartUploadCommand({
        ...this.s3Params,
        ...this.s3Options,
      });
      const response = await this.s3Client.send(command);
      this.uploadId = response.UploadId ?? null;
    } catch (err) {
      this.emit("error", err);
    } finally {
      this.isInitializing = false;
    }
  }

  /**
   * Uploads a chunk of data as a single part to S3.
   * @private
   */
  private async _uploadPart(chunk: Buffer, partNumber: number): Promise<void> {
    if (!this.uploadId) return; // Should not happen if logic is correct
    try {
      const command = new UploadPartCommand({
        ...this.s3Params,
        UploadId: this.uploadId,
        PartNumber: partNumber,
        Body: chunk,
      });
      const response = await this.s3Client.send(command);
      this.uploadedParts.push({ PartNumber: partNumber, ETag: response.ETag });
    } catch (err) {
      this.emit("error", err);
    }
  }

  /**
   * Node.js Writable stream _write method implementation.
   * @override
   */
  async _write(
    chunk: any,
    encoding: BufferEncoding,
    callback: (error?: Error | null) => void
  ): Promise<void> {
    if (!this.uploadId && !this.isInitializing) {
      await this._initialize();
    }

    while (this.isInitializing) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    if (!this.uploadId) {
      return callback(new Error("S3 multipart upload failed to initialize."));
    }

    this.buffer = Buffer.concat([this.buffer, chunk]);

    while (this.buffer.length >= this.partSize) {
      const partData = this.buffer.subarray(0, this.partSize);
      this.buffer = this.buffer.subarray(this.partSize);

      await this._uploadPart(partData, this.partNumber);
      this.partNumber++;
    }

    callback();
  }

  /**
   * Node.js Writable stream _final method implementation.
   * @override
   */
  async _final(callback: (error?: Error | null) => void): Promise<void> {
    // If _initialize was never called (i.e., no data was written), create an empty object.
    if (!this.uploadId) {
      await this._initialize();
    }

    if (!this.uploadId) {
      return callback(
        new Error(
          "S3 multipart upload failed to initialize and could not complete."
        )
      );
    }

    if (this.buffer.length > 0) {
      await this._uploadPart(this.buffer, this.partNumber);
    }

    try {
      const command = new CompleteMultipartUploadCommand({
        ...this.s3Params,
        UploadId: this.uploadId,
        MultipartUpload: {
          Parts: this.uploadedParts.sort(
            (a, b) => (a.PartNumber ?? 0) - (b.PartNumber ?? 0)
          ),
        },
      });
      await this.s3Client.send(command);
      callback();
    } catch (err: any) {
      callback(err);
    }
  }

  /**
   * Node.js Writable stream _destroy method implementation.
   * @override
   */
  async _destroy(
    error: Error | null,
    callback: (error: Error | null) => void
  ): Promise<void> {
    if (this.uploadId) {
      try {
        const command = new AbortMultipartUploadCommand({
          ...this.s3Params,
          UploadId: this.uploadId,
        });
        await this.s3Client.send(command);
        console.log("S3 multipart upload aborted successfully.");
      } catch (abortErr) {
        console.error("Failed to abort S3 multipart upload:", abortErr);
      }
    }
    callback(error);
  }
}

export default S3WriteStream;
