# Verify GitHub's resolved npm inventory

`npm audit` checks the current local dependency tree. It cannot establish that
GitHub has imported that tree. Use the separate read-only graph check after
dependency updates, especially when alert status differs from local audit results.

```powershell
gh api repos/OmnisGenomics/HelixMCP/dependency-graph/sbom > github-sbom.json
npm run security:graph -- --sbom github-sbom.json
```

Run from the repository root with the locked development dependencies installed.
The verifier accepts the REST response wrapper or its raw SPDX 2.x document,
including a UTF-8 BOM from Windows PowerShell. `--lock` selects another lockfile;
the default is `package-lock.json`. It makes no network requests or mutations.

The check compares the complete set of resolved **npm name/version pairs**:
runtime, development, optional and cross-platform entries, including nested
versions. Duplicate installations of the same pair collapse to one graph entry;
the original installation count is also reported. Non-npm SBOM entries, such as
the repository identity or Actions, are outside this comparison.

- Exit 0: every expected pair is present and no unexpected npm pair exists.
- Exit 1: stale, missing or unexpected pairs. An empty exported graph fails.
- Exit 2: malformed input, unsupported lock entries or inconsistent SBOM identity.

Missing graph coverage never qualifies as a clean result. The check validates
package identity/coverage, not all dependency edges, license information or
vulnerability status. Pair it with the remote open/fixed/dismissed alert inventory
and npm audit. No alert is dismissed and no incomplete snapshot is submitted.

The current lock contains 236 installation entries and 233 unique name/version
pairs. On 2026-10-31 the initial enabled graph contained 227 old npm pairs; this
check correctly exited 1 for that stale export. See
[recovery evidence](dependency_graph_recovery_2026-10-31.md) for the cause and
final remote verification status.
