# GitHub dependency graph recovery — 2026-10-31

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
