import { createHash } from "crypto";

export function seedFrom(parts: string[]): Buffer {
  const h = createHash("sha256");
  for (const p of parts) h.update(p).update("|");
  return h.digest();
}

export function floatBetween(seed: Buffer, offset: number, min: number, max: number): number {
  const idx = offset % Math.max(1, seed.byteLength - 4);
  const n = seed.readUInt32BE(idx);
  const unit = n / 0xffffffff;
  return min + unit * (max - min);
}

export function intBetween(seed: Buffer, offset: number, min: number, max: number): number {
  const f = floatBetween(seed, offset, 0, 1);
  return Math.floor(min + f * (max - min + 1));
}

