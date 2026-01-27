-- HelixMCP BioMCP Fabric (Postgres) - current schema
-- Keep this file in sync with db/migrations.

CREATE TABLE IF NOT EXISTS projects (
  project_id TEXT PRIMARY KEY,
  name TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS artifacts (
  artifact_id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
  type TEXT NOT NULL,
  uri TEXT NOT NULL,
  mime_type TEXT NOT NULL,
  size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
  checksum_sha256 TEXT NOT NULL,
  label TEXT,
  created_by_run_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS artifacts_project_created_idx ON artifacts(project_id, created_at DESC);
CREATE INDEX IF NOT EXISTS artifacts_checksum_idx ON artifacts(checksum_sha256);

CREATE TABLE IF NOT EXISTS param_sets (
  params_hash TEXT PRIMARY KEY,
  canonical_json JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS runs (
  run_id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
  tool_name TEXT NOT NULL,
  contract_version TEXT NOT NULL,
  tool_version TEXT,
  params_hash TEXT NOT NULL REFERENCES param_sets(params_hash) ON DELETE RESTRICT,
  policy_hash TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'blocked')),
  requested_by TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  policy_snapshot JSONB,
  environment JSONB,
  exit_code INTEGER,
  error TEXT,
  result_json JSONB,
  log_artifact_id TEXT REFERENCES artifacts(artifact_id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS runs_project_created_idx ON runs(project_id, created_at DESC);
CREATE INDEX IF NOT EXISTS runs_tool_idx ON runs(tool_name);

CREATE TABLE IF NOT EXISTS run_inputs (
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
  artifact_id TEXT NOT NULL REFERENCES artifacts(artifact_id) ON DELETE RESTRICT,
  role TEXT NOT NULL,
  PRIMARY KEY (run_id, artifact_id, role)
);

CREATE TABLE IF NOT EXISTS run_outputs (
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
  artifact_id TEXT NOT NULL REFERENCES artifacts(artifact_id) ON DELETE RESTRICT,
  role TEXT NOT NULL,
  PRIMARY KEY (run_id, artifact_id, role)
);

CREATE TABLE IF NOT EXISTS run_events (
  event_id BIGSERIAL PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
  ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  kind TEXT NOT NULL,
  message TEXT,
  data JSONB
);

CREATE INDEX IF NOT EXISTS run_events_run_ts_idx ON run_events(run_id, ts);
