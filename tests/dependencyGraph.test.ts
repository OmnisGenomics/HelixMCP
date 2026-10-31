import { describe, expect, it } from "vitest";
import { compareDependencyGraph } from "../src/security/dependencyGraph.js";

const lock = { lockfileVersion: 3, packages: {
  "": { name: "test-project" },
  "node_modules/@example/sdk": { version: "2.0.0" },
  "node_modules/optional": { version: "1.0.0", optional: true },
  "node_modules/tool/node_modules/optional": { version: "0.9.0", dev: true },
} };
const pkg = (name: string, version: string) => ({ versionInfo: version, externalRefs: [
  { referenceType: "purl", referenceLocator: `pkg:npm/${name.replace("@", "%40")}@${version}` },
] });
const sbom = (packages: unknown[]) => ({ sbom: { spdxVersion: "SPDX-2.3", packages } });
const current = [pkg("@example/sdk", "2.0.0"), pkg("optional", "1.0.0"), pkg("optional", "0.9.0")];

describe("GitHub dependency graph reconciliation", () => {
  it("accepts all locked versions regardless of order, duplicates, or unrelated ecosystems", () => {
    const other = { externalRefs: [{ referenceType: "purl", referenceLocator: "pkg:github/owner/repo@master" }] };
    const a = compareDependencyGraph(lock, sbom([...current, current[0], other]));
    expect(a.status).toBe("matched");
    expect(a.locked_instances).toBe(3);
    expect(a.expected_package_versions).toBe(3);
    expect(compareDependencyGraph(lock, sbom([...current].reverse()))).toEqual(a);
  });
  it("rejects the stale SDK while retaining both optional/nested versions", () => {
    const result = compareDependencyGraph(lock, sbom([pkg("@example/sdk", "1.0.0"), ...current.slice(1)]));
    expect(result.status).toBe("mismatch");
    expect(result.missing).toEqual(["@example/sdk@2.0.0"]);
    expect(result.unexpected).toEqual(["@example/sdk@1.0.0"]);
  });
  it("rejects an empty graph rather than treating zero detected findings as coverage", () => {
    expect(compareDependencyGraph(lock, sbom([])).missing).toHaveLength(3);
    expect(() => compareDependencyGraph(lock, { message: "Not Found" })).toThrow("SPDX");
  });
  it("rejects incomplete locks and inconsistent or malformed npm package URLs", () => {
    expect(() => compareDependencyGraph({ lockfileVersion: 2 }, sbom(current))).toThrow("version 3");
    expect(() => compareDependencyGraph({ lockfileVersion: 3, packages: {} }, sbom(current))).toThrow("no resolved");
    expect(() => compareDependencyGraph({ lockfileVersion: 3, packages: { "node_modules/link": { link: true } } }, sbom(current))).toThrow("Unsupported");
    expect(() => compareDependencyGraph(lock, sbom([{ ...pkg("optional", "1.0.0"), versionInfo: "2.0.0" }]))).toThrow("differs");
    expect(() => compareDependencyGraph(lock, sbom([{ externalRefs: [{ referenceType: "purl", referenceLocator: "pkg:npm/optional" }] }]))).toThrow("Invalid npm");
  });
});
