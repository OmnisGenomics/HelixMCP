import { readFile } from "node:fs/promises";
import { parseArgs } from "node:util";
import { compareDependencyGraph } from "../src/security/dependencyGraph.js";

try {
  const { values } = parseArgs({ options: { sbom: { type: "string" }, lock: { type: "string", default: "package-lock.json" } } });
  if (!values.sbom) throw new Error("Usage: npm run security:graph -- --sbom github-sbom.json [--lock package-lock.json]");
  const parse = async (file: string) => JSON.parse((await readFile(file, "utf8")).replace(/^\uFEFF/, ""));
  const report = compareDependencyGraph(await parse(values.lock!), await parse(values.sbom));
  console.log(JSON.stringify(report, null, 2));
  process.exitCode = report.status === "matched" ? 0 : 1;
} catch (error) {
  console.error(JSON.stringify({ status: "error", error: String(error) }));
  process.exitCode = 2;
}
