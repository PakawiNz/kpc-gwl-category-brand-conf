import { FileType, PathWithFileType } from "./type.js";
import fs from "fs";

const COST_CENTER_TYPE = [
  FileType.BUSINESS_AREA,
  FileType.COMPANY,
  FileType.COST_CENTER,
];

function getCSV(csvPath: string, delimiter: string) {
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

export async function convertCostCenterFiles(
  files: PathWithFileType[],
  writePath: string
) {
  const mapped = Object.fromEntries(
    files
      .filter(({ fileType }) => COST_CENTER_TYPE.includes(fileType))
      .map((file) => [file.fileType, file.path])
  );
  if (!mapped[FileType.BUSINESS_AREA]) throw "missing BUSINESS_AREA";
  if (!mapped[FileType.COMPANY]) throw "missing COMPANY";
  if (!mapped[FileType.COST_CENTER]) throw "missing COST_CENTER";

  const companyMapper = Object.fromEntries(
    getCSV(mapped[FileType.COMPANY], "|").map((row) => [
      row["BUKRS"],
      [row["SORT1"] /* abbrv */, row["BUTXT"] /* fullname */],
    ])
  );
  const businessAreaMapper = Object.fromEntries(
    getCSV(mapped[FileType.BUSINESS_AREA], "|").map((row) => [
      row["GSBER"],
      row["GTEXT"],
    ])
  );
  const headers = [
    "Company Code",
    "Company Name",
    "Business Area Code",
    "Business Name",
    "Cost Center",
    "Cost Center Name",
  ];
  const costCenters = [];
  for (const record of getCSV(mapped[FileType.COST_CENTER], "|")) {
    if (!record["KOSTL"]) continue
    costCenters.push([
      companyMapper[record["KOKRS"]][0],
      companyMapper[record["KOKRS"]][1],
      record["GSBER"],
      businessAreaMapper[record["GSBER"]],
      record["KOSTL"],
      record["LTXT"],
      //
    ]);
  }
  fs.writeFileSync(
    writePath,
    [headers, ...costCenters].map((row) => row.join(",")).join("\n")
  );
}
