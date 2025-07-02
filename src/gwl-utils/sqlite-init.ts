import sqlite3 from "sqlite3";

const db = new sqlite3.Database("./mydatabase.db", (err) => {
  if (err) {
    return console.error("Error opening database:", err.message);
  }
  console.log("Connected to the mydatabase.db SQLite database.");
});

enum Table {
  ArticleConfig = "ArticleConfig",
}

export function createArticleConfigTable(): Promise<void> {
  return new Promise((resolve, reject) => {
    db.serialize(() => {
      const createTableSql = `
      CREATE TABLE IF NOT EXISTS ${Table.ArticleConfig} (
        sku TEXT PRIMARY KEY,
        categoryId TEXT NOT NULL,
        brandId TEXT NOT NULL,
        earnable BOOLEAN,
        burnable BOOLEAN,
        earnRate NUMBER
      )`;

      db.run(createTableSql, (err) => {
        if (err) {
          console.error("Error creating table:", err.message);
          return reject(err);
        }
        console.log(
          `Table "${Table.ArticleConfig}" created or already exists.`
        );
      });

      const categoryIndexSql = `CREATE INDEX IF NOT EXISTS idx_article_config_category_id ON ${Table.ArticleConfig} (categoryId)`;
      db.run(categoryIndexSql, (err) => {
        if (err) {
          console.error("Error creating index on categoryId:", err.message);
          return reject(err);
        }
        console.log(`Index on "categoryId" created or already exists.`);
      });

      const brandIndexSql = `CREATE INDEX IF NOT EXISTS idx_article_config_brand_id ON ${Table.ArticleConfig} (brandId)`;
      db.run(brandIndexSql, (err) => {
        if (err) {
          console.error("Error creating index on brandId:", err.message);
          return reject(err);
        }
        console.log(`Index on "brandId" created or already exists.`);
        resolve();
      });
    });
  });
}

export function upsertArticle(
  sku: string,
  categoryId: string,
  brandId: string,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const sql = `
      INSERT INTO ${Table.ArticleConfig} (sku, categoryId, brandId)
      VALUES (?, ?, ?)
      ON CONFLICT(sku) DO UPDATE SET
        categoryId = excluded.categoryId,
        brandId = excluded.brandId
    `;

    const params = [sku, categoryId, brandId];

    db.run(sql, params, function (err) {
      if (err) {
        console.error(
          `Error upserting article config for SKU ${sku}:`,
          err.message
        );
        return reject(err);
      }
      if (this.changes > 0) {
        console.log(`Successfully upserted SKU: ${sku}`);
      }
      resolve();
    });
  });
}


export function upsertArticleConfig(
  sku: string,
  categoryId: string,
  brandId: string,
  earnable: boolean | null,
  burnable: boolean | null,
  earnRate: number | null
): Promise<void> {
  return new Promise((resolve, reject) => {
    const sql = `
      INSERT INTO ${Table.ArticleConfig} (sku, categoryId, brandId, earnable, burnable, earnRate)
      VALUES (?, ?, ?, ?, ?, ?)
      ON CONFLICT(sku) DO UPDATE SET
        categoryId = excluded.categoryId,
        brandId = excluded.brandId,
        earnable = excluded.earnable,
        burnable = excluded.burnable,
        earnRate = excluded.earnRate
    `;

    const params = [sku, categoryId, brandId, earnable, burnable, earnRate];

    db.run(sql, params, function (err) {
      if (err) {
        console.error(
          `Error upserting article config for SKU ${sku}:`,
          err.message
        );
        return reject(err);
      }
      if (this.changes > 0) {
        console.log(`Successfully upserted SKU: ${sku}`);
      }
      resolve();
    });
  });
}
