-- Reference data tables for EPIC-1 business rules / controlled vocab.
-- This file is safe to run multiple times (IF NOT EXISTS).

CREATE TABLE IF NOT EXISTS ref_instrument_types (
  jurisdiction TEXT NOT NULL,
  instrument_type TEXT NOT NULL,
  PRIMARY KEY (jurisdiction, instrument_type)
);

CREATE TABLE IF NOT EXISTS ref_primary_axes (
  axis TEXT PRIMARY KEY
);

-- Generic rules registry (simple key/value JSON rules used by services)
CREATE TABLE IF NOT EXISTS ref_rules (
  rule_key TEXT PRIMARY KEY,
  rule_value JSONB NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Seed minimal controlled vocab
INSERT INTO ref_primary_axes(axis) VALUES
  ('jurisdiction'),('industry'),('theme'),('product_scope')
ON CONFLICT DO NOTHING;

-- EU instrument types
INSERT INTO ref_instrument_types(jurisdiction, instrument_type) VALUES
  ('EU','Regulation'),('EU','Directive'),('EU','Decision'),('EU','Delegated Act'),('EU','Implementing Act'),('EU','Guidance')
ON CONFLICT DO NOTHING;

-- India instrument types
INSERT INTO ref_instrument_types(jurisdiction, instrument_type) VALUES
  ('IN','Act'),('IN','Rule'),('IN','Gazette Notification'),('IN','Circular'),('IN','Order'),('IN','Amendment')
ON CONFLICT DO NOTHING;
