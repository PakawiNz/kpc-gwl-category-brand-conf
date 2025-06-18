import { FileType, PathWithFileType } from "./type.js";
import fs from "fs";
import lodash from "lodash";

export function listFilesInFolder(
  folder: string,
  startDate: string
): PathWithFileType[] {
  const allFiles = fs
    .readdirSync(folder)
    .map((fileName) => {
      const [, module, nature, date, time] = fileName.split("_"); // env, module, nature, date, time, part, partCount
      const fileType = getFileTypeFromName(module);
      if (fileType && startDate <= date) {
        return {
          path: `${folder}/${fileName}`,
          fileType,
          nature,
          datetime: `${date}${time}`,
        };
      }
    })
    .filter((a) => !!a);

  const maxDatetimeEachType: { [key: string]: string } = {};
  allFiles.forEach((f) => {
    if (f.nature == "FULL") {
      maxDatetimeEachType[f.fileType] =
        lodash.max([maxDatetimeEachType[f.fileType] ?? "", f.datetime]) ?? "";
    }
  });
  return allFiles.filter(f => {
    if (f.nature == "FULL") {
      return maxDatetimeEachType[f.fileType] == f.datetime;
    } else {
      return true;
    }
  });
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
    case "company":
      return FileType.COMPANY;
    case "bussinessarea":
      return FileType.BUSINESS_AREA;
    case "costcenter":
      return FileType.COST_CENTER;
  }
}
