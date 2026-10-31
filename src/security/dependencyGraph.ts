/** Compare all resolved npm package versions, including development/optional ones. */
export function compareDependencyGraph(lock: unknown, exported: unknown) {
  const object = (value: unknown): Record<string, unknown> => {
    if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("Expected JSON object");
    return value as Record<string, unknown>;
  };
  const locked = object(lock);
  if (locked.lockfileVersion !== 3) throw new Error("Expected npm lockfile version 3");
  const entries = object(locked.packages);
  const expected = new Set<string>();
  let instances = 0;
  for (const [location, raw] of Object.entries(entries)) {
    if (!location) continue;
    const entry = object(raw);
    if (entry.link || typeof entry.version !== "string" || !location.includes("node_modules/"))
      throw new Error(`Unsupported lockfile entry: ${location}`);
    const name = entry.name ?? location.split("node_modules/").at(-1);
    if (typeof name !== "string" || !name) throw new Error(`Missing package name: ${location}`);
    expected.add(`${name}@${entry.version}`);
    instances++;
  }
  if (!expected.size) throw new Error("Lockfile has no resolved dependencies");

  const wrapper = object(exported);
  const sbom = object(wrapper.sbom ?? wrapper);
  if (typeof sbom.spdxVersion !== "string" || !sbom.spdxVersion.startsWith("SPDX-2.") || !Array.isArray(sbom.packages))
    throw new Error("Expected GitHub SPDX 2.x SBOM packages");
  const actual = new Set<string>();
  for (const raw of sbom.packages) {
    const pkg = object(raw);
    if (!Array.isArray(pkg.externalRefs)) continue;
    for (const rawRef of pkg.externalRefs) {
      const ref = object(rawRef);
      if (ref.referenceType !== "purl" || typeof ref.referenceLocator !== "string" || !ref.referenceLocator.startsWith("pkg:npm/")) continue;
      const match = /^pkg:npm\/(.+)@([^?#]+)(?:[?#].*)?$/.exec(ref.referenceLocator);
      if (!match) throw new Error(`Invalid npm package URL: ${ref.referenceLocator}`);
      const name = decodeURIComponent(match[1]!), version = decodeURIComponent(match[2]!);
      if (pkg.versionInfo !== undefined && pkg.versionInfo !== version)
        throw new Error(`SBOM version differs from its package URL: ${name}`);
      actual.add(`${name}@${version}`);
    }
  }
  const missing = [...expected].filter(key => !actual.has(key)).sort();
  const unexpected = [...actual].filter(key => !expected.has(key)).sort();
  return {
    status: missing.length || unexpected.length ? "mismatch" : "matched",
    scope: "resolved npm package versions; all lockfile scopes and platforms",
    locked_instances: instances,
    expected_package_versions: expected.size,
    github_package_versions: actual.size,
    missing,
    unexpected,
  };
}
