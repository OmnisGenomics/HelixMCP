# GitHub dependency graph recovery — 2026-10-31

**Resolved:** verification at 16:53 UTC confirmed all 102 original alerts fixed,
zero open alerts and the complete graph still matching the lockfile. See
[clearance evidence](github_alert_clearance_2026-10-31.md). The intermediate
pending states below are retained as historical observations.

## Confirmed cause

The public dependency page for `OmnisGenomics/HelixMCP` explicitly reported
**Dependency graph is disabled**. It stated that the graph had not been enabled
by an organization owner or repository administrator. This explains the mismatch
between the patched default-branch manifests and historical Dependabot alerts.
Who disabled the graph, and when, have not been established.

The authenticated API independently returned zero dependency manifests and no
SBOM (404), while Dependabot alerts remained enabled (204 from the repository
vulnerability-alerts endpoint). There were 102 open alerts: 89 for package-lock.json
and 13 for package.json. Their recorded dependency requirements still include
SDK 1.25.3 and body-parser 2.2.2, whereas the pushed lockfile contains SDK 1.32.2
and body-parser 2.4.0. This is not evidence of another unresolved npm package
upgrade. All 102 reported vulnerable ranges were already checked against the
patched lockfile in the [security review](security_dependency_review_2026-10-31.md).

## Repair and acceptance checks

The repair is to enable Dependency graph for **this repository** under Settings
→ Advanced Security, and allow GitHub to parse the default-branch manifests.
Do not disable/re-enable alerts, dismiss the historical alerts, apply an
organization-wide security configuration, or submit an incomplete dependency
snapshot merely to remove the count.

GitHub documents that enabling the graph parses supported manifests immediately
and that population normally takes minutes. If needed after population, use
the Dependabot page's **Refresh Dependabot alerts** action, limited to once an
hour. See [enabling the graph](https://docs.github.com/en/code-security/how-tos/secure-your-supply-chain/secure-your-dependencies/enable-dependency-graph)
and [troubleshooting](https://docs.github.com/en/code-security/reference/supply-chain-security/troubleshoot-dependabot/dependency-graph-errors).

After the setting is enabled, acceptance requires remote evidence:

1. A populated, parseable dependency graph for the actual default branch.
2. GitHub-reported resolved npm versions consistent with package-lock.json,
   including all locked versions, rather than only a few upgraded direct packages.
3. A fresh complete alert inventory. Historical alerts should become `fixed`,
   with no manual dismissal. Any remaining or newly detected alert must be
   reconciled with its current graph dependency and advisory range.
4. Unchanged package/lockfile identities, passing npm audits, and a committed
   record of the final outcome and any unverified boundary.

The browser used for this investigation was signed out. Its sign-in page was
opened for the operator; authenticated settings repair and the acceptance checks
are still pending at the time of this initial findings commit. The existing CLI
token can read repository/alert data but does not have the organization-write
scope required to create a code-security configuration. No credential scope or
organization configuration was changed.

## Evidence

Initial authenticated snapshots are retained under `D:/Helix/artifacts/`:

- `mcp-alerts-graph-before-20261031.json`: complete open-alert pages, including
  each recorded dependency requirement.
- `mcp-graph-graphql-before-20261031.json`: zero dependency manifests.
- `mcp-graph-sbom-before-20261031.json`: 404 from SBOM export.
- `mcp-security-config-before-20261031.json`: no attached security configuration.

These observations establish the disabled-graph cause. A zero local audit alone
does not establish successful remote graph recovery or alert clearance.

## Repository-scoped API repair

After the initial findings commit, the documented
`PUT /repos/OmnisGenomics/HelixMCP/vulnerability-alerts` operation returned 204.
GitHub documents this as enabling both alerts and the dependency graph. It used
the existing repository-admin CLI access, requiring neither browser sign-in nor
organization-write scope. No disable operation, alert dismissal, manual snapshot,
organization configuration or credential change was performed.

The graph immediately became available: two parseable manifests appeared and
SBOM export returned 200. Its first inventory was still cached at the old versions
(227 npm pairs, including SDK 1.25.3). Thus enabling the setting alone did not yet
establish ingestion of the patched lockfile. The new
[graph verification command](dependency_graph_verification.md) rejects that stale
export and compares all 233 current npm pairs, including optional/development
entries. A default-branch manifest update also provides GitHub its documented
automatic reparse trigger. Final graph and alert evidence must follow that push.

The subsequent package.json push updated GitHub's package.json graph to SDK
1.32.2, while its unchanged package-lock.json graph still reported SDK 1.25.3.
This distinguished a stale per-manifest inventory from a missing package fix.
A single blank separator was added before the lockfile's `packages` map to
trigger its reparse. Parsed JSON was compared with the prior committed lock:
all 236 installation entries, dependency edges, integrity hashes and metadata
were identical. No package version, package-manager setting or lock schema was
changed. This is a one-time ingestion repair, not a new lockfile convention.

## Verified updated graph

After lockfile refresh commit `84bab88`, GitHub's SBOM exported at
`2026-10-31T04:30:31Z` matched **all 233 unique npm name/version pairs** from
236 locked installation entries. `security:graph` exited 0 with no missing or
unexpected pairs. A separate GraphQL query reported two parseable manifests:
package-lock.json with 233 dependencies and package.json with 13. SDK 1.32.2,
Vitest 4.1.12 and the patched transitive versions are now in the actual remote
graph. No dependency-submission snapshot or replacement inventory was used.

Preserved final graph evidence under `D:/Helix/artifacts/`:

- `mcp-graph-sbom-after-lock-refresh-20261031.json`, SHA-256
  `d84241aec1a1f9d58f7e6f7cba17fa92cc9d948a7c220c35e107abcc6e5521bc`.
- `mcp-graph-lock-refresh-check-20261031.log`: complete successful comparison.
- `mcp-graph-upgrade-diff-20261031.json`: GitHub's dependency review independently
  identifies the removed old SDK and added SDK 1.32.2 without reported findings.

The malformed/disabled-graph export exits 2; the old 227-pair graph exits 1.
Typecheck, build and all 51 enabled tests passed, with four opt-in tests skipped.
Both refreshed npm audits reported zero findings. Manifest blob identities were
verified remotely, and the lockfile reparse change preserves parsed JSON exactly.

**Alert status remains a separate gate.** After graph reconciliation the API
still listed the original 102 alerts as open, not fixed. No alerts were dismissed.
`mcp-alerts-reevaluation-poll2-20261031.json` preserves that observation.

## Final API-only status

The user selected API-only verification and reporting of the remaining alert
status. No authenticated browser refresh was performed. The final SBOM, exported
at `2026-10-31T04:34:53Z`, again passed complete comparison: 233 expected and
233 GitHub npm pairs, zero missing and zero unexpected. Its SHA-256 is
`c044e2559e7d3ddccfe98cc5e35dd62297f0461172d3c49a05d1fd026f9305c3`.

The final REST alert inventories still show **102 open alerts**, comprising
3 critical, 38 high, 55 medium and 6 low. All are from the original inventory;
none of those 102 have become `fixed`. Their reported vulnerable versions are
absent from the now-current graph. The downstream alert records have not yet
reconciled; this is not a claim that the alerts are cleared. No additional
dependency upgrade is justified by those historical ranges.

Final evidence under `D:/Helix/artifacts/`:

- `mcp-graph-api-final-20261031.json` and
  `mcp-graph-api-final-check-20261031.log`.
- `mcp-alerts-api-final-20261031.json` and
  `mcp-alerts-fixed-api-final-20261031.json`.

If the alert status persists, the remaining supported escalation is the browser's
Refresh Dependabot alerts control, or GitHub Support with the exact manifest
blobs, repaired setting, SBOM digest and alert IDs. Neither action was performed
under the selected API-only scope. Nothing was suppressed or manually closed.

## Subsequent API recheck

At 05:18 UTC on 2026-10-31, all 102 original alerts were still open, with none
marked fixed and no new open alerts. A newly exported graph still matched all
233 current npm pairs. Each of the 102 advisory ranges was compared against the
live graph and exact direct requirements: zero current versions matched.
See the [complete investigation and alert ledger](github_alert_recheck_2026-10-31.md)
for all 21 package groups, 102 alert IDs, raw snapshot identities and the remaining
GitHub alert-status gate. No additional dependency or security-setting change
was justified or made during that recheck.

## Alert-status gate closed

GitHub recorded fixes for all 102 original alerts at 15:06:59–15:07:32 UTC.
Authenticated verification at 16:53 UTC reconciled every original ID with a fixed
record, non-null fixed timestamp and null dismissed timestamp. No open alerts
remain. The signed-in browser showed this result before any refresh action, and
the API independently confirmed it. All 233 npm pairs still match the lockfile.
See the [clearance record](github_alert_clearance_2026-10-31.md) for exact evidence
digests and scope. No manual dismissal or further dependency change was needed.
