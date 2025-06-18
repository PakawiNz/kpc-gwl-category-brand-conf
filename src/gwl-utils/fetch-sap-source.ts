import {
  S3Client,
  PutObjectCommand,
  CreateBucketCommand,
  DeleteObjectCommand,
  DeleteBucketCommand,
  paginateListObjectsV2,
  GetObjectCommand,
} from "@aws-sdk/client-s3";

import dotenv from "dotenv";
import fs from "node:fs";

dotenv.config();


// Function to list objects from S3 bucket
export async function listBucketData(prefix?: string) {
  try {
    const s3Client = new S3Client({});
    const params = {
      Bucket: process.env.AWS_BUCKET_NAME,
      Prefix: prefix || "",
    };

    const objects = [];
    const paginator = paginateListObjectsV2({ client: s3Client }, params);
    for await (const page of paginator) {
      if (page.Contents) {
        objects.push(...page.Contents);
      }
    }
    return objects;
  } catch (error) {
    console.error("Error listing bucket data:", error);
    throw error;
  }
}

async function main() {
  const MY_STORAGE =
    "/Users/pakawin_m/workspace/kpc-gwl-category-brand-conf/data/kpg-sap-s3-outbound-prod";

  const fileInMyStorages = fs.readdirSync(MY_STORAGE)
  const objects = await listBucketData("kpg-sap-s3-outbound-prod");
  const newObjects = objects.filter(object => {
    return !fileInMyStorages.includes(object.Key!.split('/').pop()!)
  })
  console.log(newObjects)
}

main();
