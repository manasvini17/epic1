-- EPIC-1: track where primary_axis came from, and store derived-only suggestions.

ALTER TABLE documents
  ADD COLUMN IF NOT EXISTS primary_axis_source TEXT NOT NULL DEFAULT 'UPLOAD';

-- Derived-only suggestion table for primary_axis (LLM or other analytics).
-- IMPORTANT: This table does not affect the source-of-truth primary_axis stored in documents.

CREATE TABLE IF NOT EXISTS primary_axis_suggestions (
  suggestion_id UUID PRIMARY KEY,
  version_id UUID UNIQUE NOT NULL REFERENCES document_versions(version_id) ON DELETE CASCADE,
  suggested_axis TEXT NOT NULL,
  model_name TEXT NOT NULL,
  model_version TEXT NOT NULL,
  confidence DOUBLE PRECISION NOT NULL,
  details_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS idx_primary_axis_suggestions_version_id
  ON primary_axis_suggestions(version_id);
