# Slurm cluster smoke (apptainer-only, network none)

This is a minimal “does it run” check for the `slurm_submit` + `slurm_job_collect` flow.

## Preconditions

- Gateway configured to use Postgres: `DATABASE_URL` is set.
- Policy contains a non-empty `runtime.instance_id` (gateway refuses to start without it when `DATABASE_URL` is set).
- `sbatch` is available on the gateway host (or wherever the gateway submits from).
- `apptainer` is available on the compute nodes.
- `RUNS_DIR` points at a filesystem visible to the compute nodes (shared FS).

## Required policy fields (minimum)

Add `slurm_submit` and `slurm_job_collect` to `tool_allowlist` and configure a Slurm section.

```yaml
runtime:
  instance_id: "prod_cluster_a"

tool_allowlist:
  - artifact_import
  - artifact_get
  - slurm_submit
  - slurm_job_collect

slurm:
  partitions_allowlist: ["short"]
  accounts_allowlist: ["bio"]
  qos_allowlist: ["normal"]          # optional
  constraints_allowlist: [""]        # optional; treat "" as “no constraint”

  max_time_limit_seconds: 7200
  max_cpus: 32
  max_mem_mb: 262144
  max_gpus: 2
  max_collect_output_bytes: 1073741824   # 1 GiB
  max_collect_log_bytes: 268435456       # 256 MiB

  gpu_types_allowlist: ["A100", "H100"]  # optional

  apptainer:
    image_allowlist:
      - "docker://quay.io/biocontainers/samtools@sha256:bf80e07e650becfd084db1abde0fe932b50f990a07fa56421ea647b552b5a406"

  network_mode_required: "none"
```

## Example `slurm_submit` payload (samtools_flagstat)

1) Import a BAM as an artifact (or use an existing `artifact_id`).

2) Submit the job:

```json
{
  "name": "slurm_submit",
  "arguments": {
    "project_id": "proj_01HZZZZZZZZZZZZZZZZZZZZZZZ",
    "job_spec": {
      "version": 1,
      "resources": { "time_limit_seconds": 900, "cpus": 1, "mem_mb": 2048, "gpus": null, "gpu_type": null },
      "placement": { "partition": "short", "account": "bio", "qos": "normal", "constraint": null },
      "execution": {
        "kind": "container",
        "container": {
          "engine": "apptainer",
          "image": "docker://quay.io/biocontainers/samtools@sha256:bf80e07e650becfd084db1abde0fe932b50f990a07fa56421ea647b552b5a406",
          "network_mode": "none",
          "readonly_rootfs": true
        },
        "command": {
          "argv": ["sh", "-c", "set -euo pipefail; samtools flagstat /work/in/input.bam > /work/out/samtools_flagstat.txt"],
          "workdir": "/work",
          "env": {}
        }
      },
      "io": {
        "inputs": [
          {
            "role": "bam",
            "artifact_id": "art_01HXXXXXXXXXXXXXXXXXXXXXXXXX",
            "checksum_sha256": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "dest_relpath": "in/input.bam"
          }
        ],
        "outputs": [
          {
            "role": "report",
            "src_relpath": "out/samtools_flagstat.txt",
            "type": "TEXT",
            "label": "samtools_flagstat.txt"
          }
        ]
      }
    }
  }
}
```

Expected `slurm_submit` output:
- `provenance_run_id` (the HelixMCP `run_id`; deterministic for the spec + policy)
- `slurm_job_id` (cluster allocated; not part of identity)
- `slurm_script_artifact_id` (exact script content captured as an artifact)

## Workspace layout

For a submitted run `run_...`, workspace root is:

`$RUNS_DIR/<run_id>/`

Expected paths:
- `in/` materialized input artifacts (per `dest_relpath`)
- `out/` tool outputs (per declared output `src_relpath`)
- `meta/slurm_script.sbatch` script written for submission
- `meta/slurm_job_id.txt` job id recorded after submission
- `meta/stdout.txt`, `meta/stderr.txt`, `meta/exit_code.txt` produced by the script
- `meta/slurm.out`, `meta/slurm.err` Slurm scheduler logs (from `#SBATCH --output/--error`)

## Collecting outputs

After the Slurm job completes, call:

```json
{
  "name": "slurm_job_collect",
  "arguments": {
    "run_id": "run_01HYYYYYYYYYYYYYYYYYYYYYYYYY"
  }
}
```

Expected behavior:
- Declared outputs are imported as artifacts (with `slurm.max_collect_output_bytes` enforced).
- `stdout`/`stderr` are imported as log artifacts if present (with `slurm.max_collect_log_bytes` enforced).
- Target run is finalized exactly once; repeat collects are idempotent and do not rewrite `finishedAt`.
- `artifacts_by_role` includes declared roles plus `stdout`, `stderr`, and `slurm_script` when available.
