import path from "path";

export interface SlurmJobSpecV1 {
  version: 1;
  resources: {
    time_limit_seconds: number;
    cpus: number;
    mem_mb: number;
    gpus?: number | null;
    gpu_type?: string | null;
  };
  placement: {
    partition: string;
    account: string;
    qos?: string | null;
    constraint?: string | null;
  };
  execution: {
    kind: "container";
    container: {
      engine: "apptainer";
      image: string;
      network_mode: "none";
      readonly_rootfs: true;
    };
    command: {
      argv: string[];
      workdir?: string;
      env: Record<string, string>;
    };
  };
  io: {
    inputs: Array<{
      role: string;
      artifact_id: string;
      checksum_sha256: string;
      dest_relpath: string;
    }>;
    outputs: Array<{
      role: string;
      src_relpath: string;
      type: string;
      label: string;
    }>;
  };
}

function bashSingleQuote(value: string): string {
  return `'${value.replace(/'/g, `'\"'\"'`)}'`;
}

function formatSlurmTimeLimit(seconds: number): string {
  if (!Number.isInteger(seconds) || seconds < 1) throw new Error(`invalid time_limit_seconds: ${seconds}`);

  const days = Math.floor(seconds / 86400);
  const rem = seconds - days * 86400;
  const hours = Math.floor(rem / 3600);
  const rem2 = rem - hours * 3600;
  const minutes = Math.floor(rem2 / 60);
  const secs = rem2 - minutes * 60;

  const hh = String(hours).padStart(2, "0");
  const mm = String(minutes).padStart(2, "0");
  const ss = String(secs).padStart(2, "0");

  if (days > 0) return `${days}-${hh}:${mm}:${ss}`;
  return `${hh}:${mm}:${ss}`;
}

function assertEnvKey(key: string): void {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(key)) {
    throw new Error(`invalid env var name: ${key}`);
  }
}

export function renderSlurmScriptV1(input: {
  workspaceRootDir: string;
  jobName: string;
  spec: SlurmJobSpecV1;
}): string {
  const workspaceRoot = path.resolve(input.workspaceRootDir);
  const jobName = input.jobName.slice(0, 128);
  const spec = input.spec;

  const workdir = spec.execution.command.workdir ?? "/work";
  const envKeys = Object.keys(spec.execution.command.env ?? {}).sort();
  for (const k of envKeys) assertEnvKey(k);

  const directives: string[] = [];
  directives.push(`#!/usr/bin/env bash`);
  directives.push(`#SBATCH --job-name=${jobName}`);
  directives.push(`#SBATCH --partition=${spec.placement.partition}`);
  directives.push(`#SBATCH --account=${spec.placement.account}`);
  if (spec.placement.qos) directives.push(`#SBATCH --qos=${spec.placement.qos}`);
  if (spec.placement.constraint) directives.push(`#SBATCH --constraint=${spec.placement.constraint}`);
  directives.push(`#SBATCH --time=${formatSlurmTimeLimit(spec.resources.time_limit_seconds)}`);
  directives.push(`#SBATCH --cpus-per-task=${spec.resources.cpus}`);
  directives.push(`#SBATCH --mem=${spec.resources.mem_mb}M`);
  if (spec.resources.gpus !== null && spec.resources.gpus !== undefined) {
    const count = spec.resources.gpus;
    const gpuType = spec.resources.gpu_type ?? null;
    directives.push(`#SBATCH --gres=gpu${gpuType ? `:${gpuType}` : ""}:${count}`);
  }
  directives.push(`#SBATCH --output=${path.join(workspaceRoot, "meta", "slurm.out")}`);
  directives.push(`#SBATCH --error=${path.join(workspaceRoot, "meta", "slurm.err")}`);

  const lines: string[] = [];
  lines.push(...directives);
  lines.push("");
  lines.push("set -euo pipefail");
  lines.push("");
  lines.push(`WORKSPACE_ROOT=${bashSingleQuote(workspaceRoot)}`);
  lines.push(`STDOUT_PATH=${bashSingleQuote(path.join(workspaceRoot, "meta", "stdout.txt"))}`);
  lines.push(`STDERR_PATH=${bashSingleQuote(path.join(workspaceRoot, "meta", "stderr.txt"))}`);
  lines.push(`EXIT_CODE_PATH=${bashSingleQuote(path.join(workspaceRoot, "meta", "exit_code.txt"))}`);
  lines.push("");
  lines.push(`mkdir -p \"$WORKSPACE_ROOT/in\" \"$WORKSPACE_ROOT/out\" \"$WORKSPACE_ROOT/meta\"`);
  lines.push("");

  for (const k of envKeys) {
    const v = String(spec.execution.command.env[k] ?? "");
    lines.push(`export APPTAINERENV_${k}=${bashSingleQuote(v)}`);
  }
  if (envKeys.length > 0) lines.push("");

  const bindArg = `${workspaceRoot}:/work`;
  const argv = spec.execution.command.argv;
  if (!Array.isArray(argv) || argv.length < 1) throw new Error("command.argv must be non-empty");

  const cmdParts: string[] = [
    "apptainer",
    "exec",
    "--cleanenv",
    "--pwd",
    workdir,
    "--bind",
    bindArg,
    spec.execution.container.image,
    "--",
    ...argv
  ];

  const cmdLine = cmdParts.map((p) => bashSingleQuote(String(p))).join(" ");

  lines.push("set +e");
  lines.push(`${cmdLine} >\"$STDOUT_PATH\" 2>\"$STDERR_PATH\"`);
  lines.push("exit_code=$?");
  lines.push("set -e");
  lines.push("printf '%s' \"$exit_code\" >\"$EXIT_CODE_PATH\"");
  lines.push("exit \"$exit_code\"");
  lines.push("");

  return lines.join("\n");
}

