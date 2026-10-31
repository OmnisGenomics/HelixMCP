# Frozen Windows Prime MCP qualification — 2026-10-31

Scope: the patched stdio gateway driving the actual frozen Omnis Helix
1.1.6rc5 GUI on Windows 11 x64, with synthetic in-silico data at 150% Qt scaling.
Application source: `e9ab0be7d35981ef0a2a43c289e85ceaec0ab7bc`.
The source application launcher is not used for this qualification.

## Smoke-helper support

`scripts/studio_prime_assistant_smoke.ts` accepts optional `--launcher` and
`--cli` executable paths. Without them it retains the source-launcher route.
The explicit GUI launcher must be displayed: passing Windows SW_HIDE to the
frozen executable hid its first main window and prevented native screenshots.
The helper now hides only the source console launcher. It starts outside the
checkout with fresh profile/settings/recovery paths, Windows-only PATH and no
PYTHONPATH/PYTHONHOME. `--python` reads the pinned expected fixture inventory;
it is not the frozen application's interpreter or offline verifier.

Startup retries are limited to the read-only state request after a bridge
timeout, at most three attempts. Native captures also allow three attempts with
distinct filenames. Every response is retained, and a widget-render fallback
never qualifies as a native capture. Writes and simulation actions are not
retried. Failed output directories remain intact.

Example from PowerShell, using a NEW output directory:

```powershell
node --import tsx scripts/studio_prime_assistant_smoke.ts `
  --helix-root 'D:\RC\app' `
  --python D:\RC\builder\Scripts\python.exe `
  --launcher 'D:\RC\app\OmnisHelix.exe' `
  --cli 'D:\RC\app\helix.exe' `
  --outdir D:\RC\new-mcp-proof
```

## Final result

TypeScript typecheck, build and all 47 enabled tests passed; four opt-in tests
remained skipped. Refreshed full and production npm audits both reported zero
known findings. Dependency remediation details are in
[the security review](security_dependency_review_2026-10-31.md).

The final frozen proof passed **163 stdio MCP calls**, with no tool errors:

- Four wizard runs generated substitution, insertion, deletion and replacement
  candidates from a synthetic reference and desired edits. Selection was explicit.
- Empty searches explained missing context without inventing design sequences;
  equivalent searches were deterministic. Candidate provenance survived navigation
  and was bound into each simulation manifest.
- A revised generated candidate retained previous evidence and produced an
  equivalent edited sequence. Comparison export was byte-stable and verified
  with the frozen standalone CLI.
- A manual override cleared generated-candidate provenance and retained an
  incompatible, unresolved sequence instead of fabricating an edited sequence.
- All **12 captures were native windows**. Results Review and its comparison
  footer remained accessible. All **55 historical files** stayed unchanged.
- The 236-case validation benchmark passed without changing the active session.

Final receipt and full call responses:
`D:/HelixReleaseCandidates/windows-rc-20261031/frozen-mcp-final-20261031/`.
Logs: `D:/Helix/artifacts/windows-rc-frozen-mcp-final-20261031.log`.
Earlier hidden-window capture failures and the startup-timeout attempt are
preserved in the sibling `frozen-mcp-proof*` directories; they did not qualify.

## Boundaries

The GUI was exercised on this Windows host, not a fresh Windows installation.
Wizard cases used the plus strand; the independent benchmark covers both
strands. The separate Python-free clean-machine kit checks frozen import/runtime
identity, normal close/reopen and installed-payload preservation. This SDK proof
additionally qualifies gateway integration; its cleanup stops only its owned
test process. Neither result establishes biological accuracy, calibration,
publisher authentication, optional-service deployment security or complete
product readiness. Conditional sequence and uncalibrated-frequency labels remain.
