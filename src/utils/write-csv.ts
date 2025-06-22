import fs from "fs";

export function toCSVRecord(row: any[], delimit = ",") {
  return (
    row
      .map((cell) => {
        cell = `${cell ?? ""}`;
        cell = cell.replace(/"/g, '""');
        if (cell.includes(delimit)) {
          return `"${cell}"`;
        } else {
          return cell;
        }
      })
      .join(delimit) + "\n"
  );
}

export function rowsToFileCSV(filePath: string, rows: any[]) {
  const writeStream = fs.createWriteStream(filePath);
  rows.forEach((row) => {
    if (row) {
      writeStream.write(toCSVRecord(row));
    }
  });
  writeStream.end();
}
