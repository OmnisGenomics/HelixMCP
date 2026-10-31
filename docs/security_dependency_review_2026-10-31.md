# MCP dependency remediation — 2026-10-31

Current status, verified at 16:53 UTC: **zero open GitHub alerts; all 102 original
alerts marked fixed, none dismissed.** The dependency graph still matches the
lockfile (233 npm pairs). GitHub's recorded fixes occurred around 15:07 UTC;
the signed-in browser already showed no open alerts, so no manual refresh was
needed. See [verified clearance](github_alert_clearance_2026-10-31.md).
The earlier pending-alert observations and
[per-alert investigation](github_alert_recheck_2026-10-31.md) are preserved below
as historical evidence, not the current status.

Scope: the locked HelixMCP JavaScript dependency tree, including development
tools. Before remediation, npm audit reported 22 dependency findings (2 critical,
15 high, 5 moderate); production-only audit reported 12 (1 critical, 8 high,
3 moderate). These are dependency findings, not a count of independently
demonstrated exploitable application paths.

| Direct package | Before | After |
| --- | --- | --- |
| @modelcontextprotocol/sdk | 1.25.3 | 1.32.2 |
| ajv | 8.17.1 | 8.20.0 |
| kysely | 0.28.10 | 0.28.18 |
| yaml | 2.8.2 | 2.9.1 |
| zod | 4.3.6 | 4.6.7 |
| vitest (development) | 4.0.18 | 4.1.12 |
| pg-mem (development) | 3.0.5 | 3.0.14 |
| tsx (development) | 4.21.0 | 4.23.15 |

Compatible transitive fixes are captured in package-lock.json, including the
Express/Hono request handling and address/path parsing dependency chains.
No force upgrade, suppression or audit exception was used. Exact direct pins
remain explicit. Node 24.15.0 and npm 11.6.2 were used for locked installation;
the minimum Node line now reflects the test toolchain's 20.19 requirement.
The host's npm 9 incremental installer failed with an Arborist edgesOut error;
the preserved failed attempt did not qualify the final lock. npm 11 completed
installation and a fresh npm ci validated it.

## Applicability and boundaries

- SDK advisories include shared-server/transport response leakage and HTTP/OAuth
  input, authentication and redirect boundaries. The gateway's exercised path is
  one local stdio server per process, with separate server/transport instances in
  tests. HTTP/OAuth or multi-tenant deployment is not qualified by this update.
  [Upstream SDK advisory](https://github.com/modelcontextprotocol/typescript-sdk/security/advisories/GHSA-345p-7cg4-v4c7).
- AJV, YAML and Zod findings concern parser/validation resource or input handling;
  all are updated, including dependencies used for actual tool and policy inputs.
- Kysely JSON-path/literal advisories are addressed by the compatible patched
  dependency. Existing parameterized store/database tests passed; no additional
  database deployment or arbitrary SQL execution is authorized by this review.
- Vitest's affected UI/browser server is a development surface. The test command
  uses run mode; UI/browser serving is not enabled. The package and its transitive
  dependencies were patched regardless.
  [Upstream Vitest advisory](https://github.com/vitest-dev/vitest/security/advisories/GHSA-5xrq-8626-4rwp).

## Verification

Fresh locked install, TypeScript typecheck, build and all 47 enabled tests passed;
4 opt-in integration tests remained skipped. Production and complete npm audits
both reported **zero known findings**. Run `npm run security:audit` to repeat the
complete audit. A zero registry audit is time-scoped dependency evidence, not a
claim of complete source, service or operational security.

The native Windows Prime assistant proof also passed with the patched SDK:
166 stdio MCP calls, four generated edit types, candidate revision/comparison,
manual override and the 236-case validation benchmark. The standalone comparison
verified offline and prior generated evidence remained unchanged. Receipt:
`D:/Helix/artifacts/mcp-security-windows-proof-20261031/receipt.json`.

Raw before/after reports and test logs are preserved under
`D:/Helix/artifacts/mcp-security-*`. This review does not assert that unrelated
GitHub alerts, Python/native dependencies or optional service deployments are
cleared; those require their own exact inventories.

The final 2026-10-31 recheck again found zero known findings in both audits.
The patched gateway also passed against the frozen Windows candidate; see
[frozen Prime qualification](windows_frozen_prime_qualification_2026-10-31.md)
for the 163-call result and its distinct runtime and scientific boundaries.

## GitHub alert status after sync

GitHub's push response and authenticated Dependabot API still reported 102 open
alerts (3 critical, 38 high, 55 medium, 6 low): 89 reference package-lock.json
and 13 package.json. They cover 21 package names. This remains an open GitHub
status, despite the zero-finding npm audits; no alerts were dismissed.

The remote lockfile blob `6e89485ceefc01021080aff378876424bf8cec01` matched
the pushed local lockfile at gateway qualification commit `23104e9`. Every alert's
reported vulnerable range was compared with every currently locked version of
its package using npm's semver implementation (GitHub comma-separated bounds
normalized as conjunctions). None of the 102 reported ranges matched a locked
version; one referenced package was absent. Examples: SDK 1.32.2, Vitest mocker
4.1.12, Vite 8.3.5 and body-parser 2.4.0 are outside their reported ranges.
This supports a dependency-graph reevaluation discrepancy; it does not establish
that GitHub has cleared the alerts or that all security surfaces are qualified.

API evidence and range comparison are retained as
`D:/Helix/artifacts/mcp-github-alerts-after-sync-20261031.json` and
`D:/Helix/artifacts/mcp-github-alerts-range-check-20261031.json`.

Follow-up investigation confirmed that **Dependency graph was disabled** on the
repository, despite alerts remaining enabled. GitHub's public graph page states
this explicitly; its API returns no manifests and no SBOM. See
[graph recovery](dependency_graph_recovery_2026-10-31.md) for the evidence and
remote acceptance checks. The earlier reevaluation-discrepancy inference is now
grounded in that setting; enabling and verifying the graph is the next gate.

The graph recovery follow-up has now enabled the graph and verified all 233
resolved npm name/version pairs against the unchanged dependency resolutions.
Both manifests are current. The 102 historical alert records still await
reevaluation; they have not been dismissed or claimed fixed. The
[recovery record](dependency_graph_recovery_2026-10-31.md#verified-updated-graph)
contains the exact SBOM digest, repair commits, checks and remaining gate.
