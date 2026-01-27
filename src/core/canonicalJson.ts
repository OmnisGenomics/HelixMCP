import { createHash } from "crypto";

const CROCKFORD_BASE32_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";

export function sha256Hex(data: string | Buffer): string {
  return createHash("sha256").update(data).digest("hex");
}

export function sha256Prefixed(data: string | Buffer): `sha256:${string}` {
  return `sha256:${sha256Hex(data)}` as const;
}

export function encodeCrockfordBase32_128bits(bytes: Uint8Array): string {
  if (bytes.byteLength !== 16) throw new Error(`expected 16 bytes, got ${bytes.byteLength}`);
  let value = 0n;
  for (const b of bytes) value = (value << 8n) | BigInt(b);

  let out = "";
  for (let i = 0; i < 26; i++) {
    const idx = Number(value & 31n);
    out = CROCKFORD_BASE32_ALPHABET[idx] + out;
    value >>= 5n;
  }
  return out;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return (
    typeof value === "object" &&
    value !== null &&
    (Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null)
  );
}

export function canonicalizeJson(value: unknown): unknown {
  if (value === undefined) return undefined;
  if (value === null) return null;

  if (typeof value === "number") {
    if (!Number.isFinite(value)) return null;
    if (Object.is(value, -0)) return 0;
    return value;
  }

  if (typeof value === "string" || typeof value === "boolean") return value;

  if (typeof value === "bigint") return value.toString();

  if (value instanceof Date) return value.toISOString();

  if (Array.isArray(value)) {
    return value.map((v) => {
      const c = canonicalizeJson(v);
      return c === undefined ? null : c;
    });
  }

  if (isPlainObject(value)) {
    const out: Record<string, unknown> = {};
    const keys = Object.keys(value).sort();
    for (const key of keys) {
      const c = canonicalizeJson(value[key]);
      if (c !== undefined) out[key] = c;
    }
    return out;
  }

  // Fallback: try to stringify unknown objects deterministically by their JSON representation.
  // If it cannot be stringified, throw to avoid non-deterministic hashes.
  try {
    const json = JSON.parse(JSON.stringify(value)) as unknown;
    return canonicalizeJson(json);
  } catch {
    throw new Error("value is not JSON-serializable");
  }
}

export function stableJsonStringify(value: unknown): string {
  return JSON.stringify(canonicalizeJson(value));
}

