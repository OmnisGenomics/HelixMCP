import * as pg from "pg";
import { Kysely, PostgresDialect } from "kysely";
import type { DB } from "./types.js";

export function createPgPool(databaseUrl: string): pg.Pool {
  return new pg.Pool({ connectionString: databaseUrl });
}

export function createDb(pool: pg.Pool): Kysely<DB> {
  return new Kysely<DB>({
    dialect: new PostgresDialect({ pool })
  });
}
