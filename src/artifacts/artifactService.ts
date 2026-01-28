import type { ArtifactRecord, ArtifactType } from "../core/artifact.js";
import { detectArtifactType } from "../core/detectArtifactType.js";
import type { ArtifactId, ProjectId, RunId } from "../core/ids.js";
import { newArtifactId } from "../core/ids.js";
import type { JsonObject } from "../core/json.js";
import { mimeTypeForArtifactType } from "../core/mimeType.js";
import type { PostgresStore } from "../store/postgresStore.js";
import type { LocalObjectStore } from "./localObjectStore.js";

export type ArtifactImportSource =
  | { kind: "inline_text"; text: string }
  | { kind: "local_path"; path: string };

export class ArtifactService {
  constructor(
    private readonly store: PostgresStore,
    private readonly objects: LocalObjectStore
  ) {}

  async importArtifact(input: {
    projectId: ProjectId;
    source: ArtifactImportSource;
    typeHint: ArtifactType | null;
    label: string | null;
    createdByRunId: RunId | null;
    maxBytes: bigint | null;
  }): Promise<ArtifactRecord> {
    const artifactId = newArtifactId();
    const inferredType =
      input.typeHint ??
      detectArtifactType(
        input.label ??
          (input.source.kind === "local_path" ? input.source.path : "inline_text")
      );

    const putResult =
      input.source.kind === "inline_text"
        ? await this.objects.putInlineText(artifactId, input.source.text)
        : await this.objects.putFromLocalPath(artifactId, input.source.path, input.maxBytes);

    const mimeType = mimeTypeForArtifactType(inferredType);

    const metadata: JsonObject = {
      import: {
        kind: input.source.kind,
        label: input.label
      }
    };

    return this.store.createArtifact({
      artifactId,
      projectId: input.projectId,
      type: inferredType,
      uri: putResult.uri,
      mimeType,
      sizeBytes: putResult.sizeBytes,
      checksumSha256: putResult.checksumSha256,
      label: input.label,
      createdByRunId: input.createdByRunId,
      metadata
    });
  }

  async getArtifact(artifactId: ArtifactId): Promise<ArtifactRecord | null> {
    return this.store.getArtifact(artifactId);
  }

  async listArtifacts(projectId: ProjectId, limit: number, asOfCreatedAt?: string | null): Promise<ArtifactRecord[]> {
    return this.store.listArtifacts(projectId, limit, asOfCreatedAt);
  }

  async previewText(artifactId: ArtifactId, opts: { maxBytes: number; maxLines: number }): Promise<{ preview: string; truncated: boolean }> {
    return this.objects.readTextPreview(artifactId, opts);
  }

  async materializeToPath(artifactId: ArtifactId, destPath: string): Promise<void> {
    await this.objects.materializeToPath(artifactId, destPath);
  }
}
