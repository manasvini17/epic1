-- Store a compact artifacts map in document_versions for UI/consumers.

ALTER TABLE document_versions
  ADD COLUMN IF NOT EXISTS artifacts_json JSONB NOT NULL DEFAULT '{}'::jsonb;

-- Optional FK: version references its primary evidence file.
-- Not enforced in some minimal setups; uncomment if you want strict integrity.
-- ALTER TABLE document_versions
--   ADD CONSTRAINT fk_document_versions_file_id
--   FOREIGN KEY (file_id) REFERENCES evidence_files(file_id);
