import type { ArtifactType } from "./artifact.js";

export function mimeTypeForArtifactType(type: ArtifactType): string {
  switch (type) {
    case "FASTQ_GZ":
      return "application/gzip";
    case "JSON":
      return "application/json";
    case "HTML":
      return "text/html";
    case "PDF":
      return "application/pdf";
    case "ZIP":
      return "application/zip";
    case "MD":
      return "text/markdown";
    case "TSV":
    case "CSV":
    case "TEXT":
    case "LOG":
      return "text/plain";
    case "BAM":
    case "BAI":
    case "VCF":
    case "H5AD":
    case "UNKNOWN":
    default:
      return "application/octet-stream";
  }
}
