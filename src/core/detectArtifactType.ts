import type { ArtifactType } from "./artifact.js";

export function detectArtifactType(filenameOrLabel: string): ArtifactType {
  const s = filenameOrLabel.toLowerCase();
  if (s.endsWith(".fastq.gz") || s.endsWith(".fq.gz")) return "FASTQ_GZ";
  if (s.endsWith(".bam")) return "BAM";
  if (s.endsWith(".bai")) return "BAI";
  if (s.endsWith(".vcf") || s.endsWith(".vcf.gz")) return "VCF";
  if (s.endsWith(".h5ad")) return "H5AD";
  if (s.endsWith(".tsv")) return "TSV";
  if (s.endsWith(".csv")) return "CSV";
  if (s.endsWith(".json")) return "JSON";
  if (s.endsWith(".html") || s.endsWith(".htm")) return "HTML";
  if (s.endsWith(".pdf")) return "PDF";
  if (s.endsWith(".log")) return "LOG";
  if (s.endsWith(".txt")) return "TEXT";
  return "UNKNOWN";
}

