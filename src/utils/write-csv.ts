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

export function rowsToFileCSV(filePath: string, rows: any[], escaped = false) {
  const writeStream = fs.createWriteStream(filePath);
  rows.forEach((row) => {
    if (row) {
      if (escaped) {
        writeStream.write(toCSVRecord(row));
      } else {
        writeStream.write(row.join(",") + "\n");
      }
    }
  });
  writeStream.end();
}
