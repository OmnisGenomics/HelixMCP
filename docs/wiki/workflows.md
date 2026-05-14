---
page_id: workflows
page_type: workflows
generation_mode: inferred
freshness_status: new
updated_at: 2026-04-18T05:42:41.555Z
---

<details>
<summary>Build metadata</summary>

```json
{
  "freshnessKey": "3ef7353f122794800454e6278bf237e64ae5355c",
  "plannerReason": "Generated because workflows are one of the primary agent interaction surfaces.",
  "changedPaths": [
    "package.json"
  ],
  "dependencyPaths": [
    "package.json"
  ],
  "dependencyEvidenceIds": [
    "workflow:package.json"
  ],
  "evidenceIds": [
    "workflow:package.json"
  ],
  "qualityWarnings": []
}

```
</details>

# Workflows

Workflow guide for HelixMCP.

## Related Pages

- [testing](testing.md)
- [architecture](architecture.md)

## Workflow Inventory

- `pnpm build` (build, confidence high) | prerequisites: pnpm install
- `pnpm bundle:export` (bundle:export, confidence high) | prerequisites: pnpm install
- `pnpm bundle:verify` (bundle:verify, confidence high) | prerequisites: pnpm install
- `pnpm dev` (dev, confidence high) | prerequisites: pnpm install
- `pnpm test` (test, confidence high) | prerequisites: pnpm install
- `pnpm test:watch` (test:watch, confidence high) | prerequisites: pnpm install
- `pnpm typecheck` (typecheck, confidence high) | prerequisites: pnpm install

<details>
<summary>Related files:</summary>

- `package.json`
</details>

<details>
<summary>Citations:</summary>

- `package.json`
</details>

## Testing and Validation

- `pnpm test`
- `pnpm test:watch`

<details>
<summary>Related files:</summary>

- `package.json`
</details>

<details>
<summary>Citations:</summary>

- `package.json`
</details>

## Runtime Entrypoints

- `pnpm dev`

<details>
<summary>Related files:</summary>

- `package.json`
</details>

<details>
<summary>Citations:</summary>

- `package.json`
</details>

## Citations

<details>
<summary>Citations:</summary>

- `package.json`
</details>
