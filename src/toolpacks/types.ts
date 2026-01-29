import type { ZodType } from "zod/v4";
import type { ArtifactService } from "../artifacts/artifactService.js";
import type { ArtifactId, ProjectId, RunId } from "../core/ids.js";
import type { JsonObject } from "../core/json.js";
import type { PolicyEngine } from "../policy/policy.js";
import type { ToolRun } from "../runs/toolRun.js";
import type { PostgresStore } from "../store/postgresStore.js";
import type { ArtifactType } from "../core/artifact.js";
import type { SlurmSubmitter } from "../execution/slurm/submitter.js";

export interface ToolContext {
  policy: PolicyEngine;
  store: PostgresStore;
  artifacts: ArtifactService;
  runsDir: string;
  slurmSubmitter?: SlurmSubmitter;
}

export type ToolPlanKind = "docker" | "slurm" | "hybrid";
export type SelectedPlanKind = Exclude<ToolPlanKind, "hybrid">;

export interface ToolDeclaredOutput {
  role: string;
  type: ArtifactType;
  label: string;
  // For slurm toolpacks: relative path under the run workspace where the output will be collected from.
  // Example: "out/report.txt"
  srcRelpath?: string;
}

export interface ToolExecutionResult {
  summary: string;
  result: JsonObject;
}

export interface PreparedToolRun<TPlan> {
  projectId: ProjectId;
  canonicalParams: JsonObject;
  toolVersion: string;
  plan: TPlan;
  selectedPlanKind: SelectedPlanKind;
  inputsToLink: Array<{ artifactId: ArtifactId; role: string }>;
}

export interface ToolDefinition<TArgs, TPlan> {
  toolName: string;
  contractVersion: string;
  planKind: ToolPlanKind;
  description: string;
  inputSchema: ZodType<TArgs>;
  outputSchema: ZodType<unknown>;
  declaredOutputs: ToolDeclaredOutput[];
  canonicalize(args: TArgs, ctx: ToolContext): Promise<PreparedToolRun<TPlan>>;
  run(args: {
    runId: RunId;
    toolRun: ToolRun;
    prepared: PreparedToolRun<TPlan>;
    ctx: ToolContext;
  }): Promise<ToolExecutionResult>;
}
