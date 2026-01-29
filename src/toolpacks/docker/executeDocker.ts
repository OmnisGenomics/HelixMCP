import { DockerRunner } from "../../execution/backends/dockerRunner.js";
import type { RunnerBackend } from "../../execution/backends/types.js";
import { createRunWorkspace } from "../../execution/workspace.js";
import type { ArtifactRecord } from "../../core/artifact.js";
import type { RunId } from "../../core/ids.js";
import type { PolicyEngine } from "../../policy/policy.js";

export interface DockerExecutionPlan {
  image: string;
  argv: string[];
  workdir: string;
  env?: Record<string, string>;
  inputs: Array<{ role: string; artifact: ArtifactRecord; destName: string }>;
  resources: { threads: number; runtimeSeconds: number };
}

export interface DockerExecutionOutcome {
  stdout: string;
  stderr: string;
  exitCode: number;
  startedAt: string;
  finishedAt: string;
}

const docker: RunnerBackend<"docker"> = new DockerRunner();

export async function executeDockerPlan(input: {
  policy: PolicyEngine;
  runsDir: string;
  runId: RunId;
  toolName: string;
  plan: DockerExecutionPlan;
  materializeToPath: (artifactId: string, destPath: string) => Promise<void>;
}): Promise<DockerExecutionOutcome> {
  const { policy, runsDir, runId, toolName, plan } = input;

  policy.assertDockerNetworkNone();
  policy.assertDockerImagePinned(plan.image);
  policy.assertDockerImageAllowed(plan.image);
  policy.enforceThreads(toolName, plan.resources.threads);
  policy.enforceRuntimeSeconds(plan.resources.runtimeSeconds);

  const ws = await createRunWorkspace(runsDir, runId);
  for (const inp of plan.inputs) {
    const destPath = ws.inPath(inp.destName);
    await input.materializeToPath(inp.artifact.artifactId, destPath);
  }

  const uid = typeof (process as any).getuid === "function" ? (process as any).getuid() : null;
  const gid = typeof (process as any).getgid === "function" ? (process as any).getgid() : null;
  const user = uid !== null && gid !== null ? `${uid}:${gid}` : undefined;

  const env: Record<string, string> = { ...(plan.env ?? {}) };
  const threads = String(plan.resources.threads);
  env.OMP_NUM_THREADS = threads;
  env.OPENBLAS_NUM_THREADS = threads;
  env.MKL_NUM_THREADS = threads;
  env.NUMEXPR_NUM_THREADS = threads;

  const dockerSpec: Parameters<RunnerBackend<"docker">["execute"]>[0] = {
    kind: "docker",
    image: plan.image,
    argv: plan.argv,
    workdir: plan.workdir,
    network: policy.dockerNetworkMode(),
    readOnlyRootFs: true,
    containerName: `helixmcp_${runId}`,
    mounts: [{ hostPath: ws.rootDir, containerPath: "/work", readOnly: false }],
    env
  };
  if (user) dockerSpec.user = user;

  const result = await docker.execute(dockerSpec, plan.resources);
  return result;
}
