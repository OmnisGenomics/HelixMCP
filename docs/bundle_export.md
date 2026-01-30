# Audit Bundle Export (v0.9.5)

HelixMCP ships a deterministic audit bundle exporter as a CLI script.

## Export

Directory output:

```bash
DATABASE_URL=... OBJECT_STORE_DIR=var/objects \
  npm run bundle:export -- --run-id run_... --out /tmp/bundle_dir --include-blobs selected --max-bytes 100000000 --verify-after-write
```

Deterministic tar output:

```bash
DATABASE_URL=... OBJECT_STORE_DIR=var/objects \
  npm run bundle:export -- --run-id run_... --out /tmp/bundle.tar --include-blobs selected --max-bytes 100000000 --verify-after-write
```

Notes:
- `include-blobs=selected` includes the root run’s input/output/log blobs only.
- `include-blobs=all` includes all blobs referenced by the exported run graph (including subruns in `result_json.graph`).
- `max-bytes` is a per-blob cap; blobs larger than the cap are skipped with a warning.

## Verify

```bash
npm run bundle:verify -- --bundle /tmp/bundle_dir
npm run bundle:verify -- --bundle /tmp/bundle.tar
```

For `.tar` bundles, if a sibling digest file exists (`/tmp/bundle.tar.sha256`), verification will also validate it.

