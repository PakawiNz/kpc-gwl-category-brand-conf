import { FileType } from "./type.js";
import fs from "fs";

interface PathWithFileType {
  path: string;
  fileType: FileType;
}

export function listFilesInFolder(folder: string, startDate:string): PathWithFileType[] {
  return fs
    .readdirSync(folder)
    .map((fileName) => {
      const [, module,,date] = fileName.split("_"); // env, module, completion, date, time
      const fileType = getFileTypeFromName(module);
      if (fileType && (startDate < date)) {
        return {
          path: `${folder}/${fileName}`,
          fileType,
        };
      }
    })
    .filter((a) => !!a);
}

function getFileTypeFromName(module: string) {
  module = (module ?? "").toLowerCase();
  switch (module) {
    case "category":
      return FileType.CATEGORY;
    case "brand":
      return FileType.BRAND;
    case "article":
      return FileType.ARTICLE;
  }
}
