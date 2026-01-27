import { sha256Prefixed, stableJsonStringify } from "../core/canonicalJson.js";
import type { RunId } from "../core/ids.js";
import { deriveRunIdFromParts } from "../core/ids.js";

export interface RunIdentityInput {
  toolName: string;
  contractVersion: string;
  policyHash: `sha256:${string}`;
  canonicalParams: unknown;
}

export function deriveCanonicalParamsHash(canonicalParams: unknown): `sha256:${string}` {
  return sha256Prefixed(stableJsonStringify(canonicalParams));
}

export function deriveRunId(input: RunIdentityInput): { runId: RunId; paramsHash: `sha256:${string}` } {
  const paramsHash = deriveCanonicalParamsHash(input.canonicalParams);
  const runId = deriveRunIdFromParts([
    `tool=${input.toolName}`,
    `contract=${input.contractVersion}`,
    `policy=${input.policyHash}`,
    `params=${paramsHash}`
  ]);
  return { runId, paramsHash };
}

