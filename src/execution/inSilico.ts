import type { ArtifactRecord, ArtifactType } from "../core/artifact.js";
import type { JsonObject } from "../core/json.js";
import { floatBetween, intBetween, seedFrom } from "./deterministic.js";

export interface OutputArtifactSpec {
  role: string;
  type: ArtifactType;
  label: string;
  contentText: string;
}

export interface SimulateQcFastqResult {
  outputs: OutputArtifactSpec[];
  qc: {
    reads_estimated: number;
    q30_estimated: number;
    gc_pct_estimated: number;
  };
}

export function simulateQcFastq(job: {
  toolName: string;
  reads1: ArtifactRecord;
  reads2: ArtifactRecord | null;
  threads: number;
}): SimulateQcFastqResult {
  const seed = seedFrom([
    job.toolName,
    job.reads1.checksumSha256,
    job.reads2?.checksumSha256 ?? "",
    `threads=${job.threads}`
  ]);

  const readsEstimated = intBetween(seed, 0, 1_000_000, 50_000_000);
  const q30 = Number(floatBetween(seed, 4, 0.7, 0.95).toFixed(4));
  const gc = Number(floatBetween(seed, 8, 0.35, 0.65).toFixed(4));

  const report = `# Simulated QC Report\n\n- reads_1_sha256: ${job.reads1.checksumSha256}\n- reads_2_sha256: ${job.reads2?.checksumSha256 ?? "(none)"}\n- reads_estimated: ${readsEstimated}\n- q30_estimated: ${q30}\n- gc_pct_estimated: ${gc}\n`;

  return {
    qc: { reads_estimated: readsEstimated, q30_estimated: q30, gc_pct_estimated: gc },
    outputs: [
      {
        role: "report",
        type: "TEXT",
        label: "qc_report.md",
        contentText: report
      }
    ]
  };
}

export interface SimulateAlignReadsResult {
  outputs: OutputArtifactSpec[];
  qc: {
    mapped_pct: number;
    duplication_pct: number;
    insert_size_median: number;
  };
}

export function simulateAlignReads(job: {
  toolName: string;
  reads1: ArtifactRecord;
  reads2: ArtifactRecord | null;
  referenceAlias: string;
  threads: number;
  sort: boolean;
  markDuplicates: boolean;
}): SimulateAlignReadsResult {
  const seed = seedFrom([
    job.toolName,
    job.reads1.checksumSha256,
    job.reads2?.checksumSha256 ?? "",
    `ref=${job.referenceAlias}`,
    `threads=${job.threads}`,
    `sort=${job.sort}`,
    `mark_duplicates=${job.markDuplicates}`
  ]);

  const mappedPct = Number(floatBetween(seed, 0, 80, 99.9).toFixed(3));
  const dupPct = Number(floatBetween(seed, 4, 0, 30).toFixed(3));
  const insertMedian = intBetween(seed, 8, 150, 550);

  const bamPayload: JsonObject = {
    simulated: true,
    tool: job.toolName,
    reference: job.referenceAlias,
    inputs: {
      reads_1_sha256: job.reads1.checksumSha256,
      reads_2_sha256: job.reads2?.checksumSha256 ?? null
    },
    params: {
      threads: job.threads,
      sort: job.sort,
      mark_duplicates: job.markDuplicates
    },
    qc: {
      mapped_pct: mappedPct,
      duplication_pct: dupPct,
      insert_size_median: insertMedian
    }
  };

  const bamText = JSON.stringify(bamPayload, null, 2) + "\n";

  return {
    qc: { mapped_pct: mappedPct, duplication_pct: dupPct, insert_size_median: insertMedian },
    outputs: [
      {
        role: "bam_sorted",
        type: "BAM",
        label: "aligned.sorted.bam",
        contentText: bamText
      },
      {
        role: "bai",
        type: "BAI",
        label: "aligned.sorted.bam.bai",
        contentText: `SIMULATED_BAI sha256:${job.reads1.checksumSha256.slice("sha256:".length)}\n`
      }
    ]
  };
}

