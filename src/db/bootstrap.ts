import { promises as fs } from "fs";
import type * as pg from "pg";

export async function applySqlFile(pool: pg.Pool, filePath: string): Promise<void> {
  const sql = await fs.readFile(filePath, "utf8");
  if (!sql.trim()) return;
  await pool.query(sql);
}
