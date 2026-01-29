import type { JsonObject } from "../core/json.js";

export function envSnapshot(): JsonObject {
  return {
    node: process.version,
    mode: process.env.DATABASE_URL ? "postgres" : "pg-mem",
    object_store: process.env.OBJECT_STORE_DIR ?? "var/objects",
    runs_dir: process.env.RUNS_DIR ?? "var/runs"
  };
}

