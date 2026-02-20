-- 001_epic1_core.sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS ref_instrument_types (
  instrument_type TEXT PRIMARY KEY,
  jurisdiction TEXT NOT NULL,
  is_binding BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ref_primary_axis (
  primary_axis TEXT PRIMARY KEY,
  description TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ref_rules (
  rule_key TEXT PRIMARY KEY,
  rule_desc TEXT NOT NULL,
  rule_json JSONB NOT NULL,
  is_active BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS documents (
  document_id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  jurisdiction TEXT NOT NULL,
  regulation_family TEXT NOT NULL,
  instrument_type TEXT NOT NULL,
  primary_axis TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ,
  CONSTRAINT fk_docs_instrument FOREIGN KEY (instrument_type) REFERENCES ref_instrument_types(instrument_type),
  CONSTRAINT fk_docs_primary_axis FOREIGN KEY (primary_axis) REFERENCES ref_primary_axis(primary_axis)
);

CREATE TABLE IF NOT EXISTS document_versions (
  version_id TEXT PRIMARY KEY,
  document_id TEXT NOT NULL REFERENCES documents(document_id),
  version_label TEXT,
  effective_date DATE,
  status TEXT NOT NULL,
  parent_version_id TEXT NULL REFERENCES document_versions(version_id),
  tenant_id TEXT NOT NULL,
  effective_year INT NOT NULL,
  uploaded_by TEXT NOT NULL,
  uploaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  raw_sha256 TEXT NOT NULL,
  file_id TEXT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_versions_doc ON document_versions(document_id);
CREATE INDEX IF NOT EXISTS idx_versions_parent ON document_versions(parent_version_id);
CREATE INDEX IF NOT EXISTS idx_versions_raw_sha ON document_versions(raw_sha256);

CREATE TABLE IF NOT EXISTS evidence_files (
  file_id TEXT PRIMARY KEY,
  sha256 TEXT NOT NULL,
  mime_type TEXT NOT NULL,
  size_bytes BIGINT NOT NULL,
  storage_uri TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_evidence_sha ON evidence_files(sha256);

CREATE TABLE IF NOT EXISTS derived_artifacts (
  artifact_id TEXT PRIMARY KEY,
  version_id TEXT NOT NULL REFERENCES document_versions(version_id),
  kind TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  storage_uri TEXT NOT NULL,
  generator_name TEXT NOT NULL,
  generator_version TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_artifacts_version ON derived_artifacts(version_id);
CREATE INDEX IF NOT EXISTS idx_artifacts_kind ON derived_artifacts(kind);

CREATE TABLE IF NOT EXISTS audit_log (
  event_id TEXT PRIMARY KEY,
  entity_type TEXT NOT NULL,
  entity_id TEXT NOT NULL,
  action TEXT NOT NULL,
  actor TEXT NOT NULL,
  at TIMESTAMPTZ NOT NULL DEFAULT now(),
  prev_event_hash TEXT,
  event_hash TEXT,
  correlation_id TEXT,
  details_json JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_audit_entity ON audit_log(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_audit_action ON audit_log(action);
CREATE INDEX IF NOT EXISTS idx_audit_correlation ON audit_log(correlation_id);

CREATE TABLE IF NOT EXISTS chunks (
  chunk_id TEXT PRIMARY KEY,
  version_id TEXT NOT NULL REFERENCES document_versions(version_id),
  chunk_set_artifact_id TEXT NOT NULL,
  chunk_schema_version TEXT NOT NULL,
  start_char INT NOT NULL,
  end_char INT NOT NULL,
  page_start INT,
  page_end INT,
  bbox_refs JSONB,
  text_sha256 TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_chunks_version ON chunks(version_id);

CREATE TABLE IF NOT EXISTS prompts (
  prompt_hash TEXT PRIMARY KEY,
  prompt_template TEXT NOT NULL,
  prompt_version TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS llm_runs (
  run_id TEXT PRIMARY KEY,
  version_id TEXT NOT NULL REFERENCES document_versions(version_id),
  purpose TEXT NOT NULL,
  model_name TEXT NOT NULL,
  model_version TEXT NOT NULL,
  prompt_hash TEXT NOT NULL REFERENCES prompts(prompt_hash),
  tools_used JSONB NOT NULL DEFAULT '{}'::jsonb,
  input_fingerprint TEXT NOT NULL,
  output_artifact_id TEXT REFERENCES derived_artifacts(artifact_id),
  status TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS soi_documents (
  document_id TEXT PRIMARY KEY,
  title TEXT,
  jurisdiction TEXT,
  regulation_family TEXT,
  instrument_type TEXT,
  primary_axis TEXT,
  latest_version_id TEXT,
  latest_status TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS soi_versions (
  version_id TEXT PRIMARY KEY,
  document_id TEXT,
  status TEXT,
  version_label TEXT,
  effective_date DATE,
  uploaded_by TEXT,
  uploaded_at TIMESTAMPTZ,
  raw_sha256 TEXT,
  artifact_count INT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS soi_artifacts (
  artifact_id TEXT PRIMARY KEY,
  version_id TEXT,
  kind TEXT,
  sha256 TEXT,
  storage_uri TEXT,
  created_at TIMESTAMPTZ
);

INSERT INTO ref_primary_axis(primary_axis, description)
VALUES
 ('jurisdiction','Grouped primarily by jurisdiction'),
 ('theme','Grouped by theme/domain'),
 ('product_scope','Grouped by product/material scope'),
 ('sector','Grouped by industry/sector')
ON CONFLICT DO NOTHING;

INSERT INTO ref_instrument_types(instrument_type, jurisdiction, is_binding)
VALUES
 ('Regulation','EU', true),
 ('Directive','EU', true),
 ('Act','India', true),
 ('Rule','India', true),
 ('Guidance','EU', false),
 ('Framework','Global', false)
ON CONFLICT DO NOTHING;
