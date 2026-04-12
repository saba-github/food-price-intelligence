-- 026_align_raw_idempotency_with_run_scope.sql

-- 1) raw_hash should never be null going forward
ALTER TABLE raw_price_events
ALTER COLUMN raw_hash SET NOT NULL;

-- 2) Drop old raw uniqueness/index shapes if they exist
DROP INDEX IF EXISTS uniq_raw_event;
DROP INDEX IF EXISTS uniq_raw_event_hash;
DROP INDEX IF EXISTS idx_raw_price_events_source_hash;
DROP INDEX IF EXISTS uniq_raw_event_per_run;

ALTER TABLE raw_price_events
DROP CONSTRAINT IF EXISTS uniq_raw_event_hash_constraint;

-- 3) Keep a normal lookup index for source_name + raw_hash
CREATE INDEX IF NOT EXISTS idx_raw_price_events_source_hash
ON raw_price_events (source_name, raw_hash);

-- 4) Enforce idempotency at run level, not globally across all history
CREATE UNIQUE INDEX IF NOT EXISTS uniq_raw_event_per_run
ON raw_price_events (run_id, source_name, raw_hash);