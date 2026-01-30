# Versioning and Stability

HelixMCP uses semantic versioning.

## v1.x guarantees

In the v1.x line, the following are **stable**:

1) Toolpack ABI v1  
- Toolpacks that declare `abiVersion: "v1"` and conform to `TOOLPACK_ABI_V1.md` will continue to load and run.
- Any breaking toolpack changes require a new ABI version (`"v2"`).

2) Run identity semantics  
- `run_id` derivation inputs and canonicalization rules remain stable.
- Canonical params are forced-canonical (stable JSON) before hashing and storage.

3) Artifact model contract  
- Artifacts remain checksum-addressed and role-linked.
- Artifact type semantics remain stable (types may be added; existing types keep meaning).

4) Replay semantics  
- Successful runs may be replayed without re-execution when identity matches.
- Queued Slurm submissions may replay queued checkpoints without resubmission.

5) Event envelope v1  
- The event envelope and existing kind meanings remain stable.

## What may evolve in v1.x (additive only)

- New tools and toolpacks
- New artifact types
- New event kinds
- New optional fields in schemas and outputs
- Additional validations that tighten previously-undefined behavior (fail-closed)

## What requires v2.0.0

Any breaking change to:

- Toolpack ABI v1 surface or semantics
- Run id derivation semantics
- Artifact schema meaning for an existing type or field
- Event envelope fields or meanings
- Removing or changing a public tool contract without versioning

