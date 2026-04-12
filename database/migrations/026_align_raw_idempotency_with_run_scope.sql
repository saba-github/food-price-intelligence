-- 026_align_raw_idempotency_with_run_scope.sql

-- --------------------------------------------------
-- STEP 0: DROP constraint FIRST (critical fix)
-- --------------------------------------------------
ALTER TABLE raw_price_events
DROP CONSTRAINT IF EXISTS uniq_raw_event_hash_constraint;


-- --------------------------------------------------
-- STEP 1: Fill NULL raw_hash
-- --------------------------------------------------
UPDATE raw_price_events
SET raw_hash = md5(
    coalesce(source_name, '') ||
    coalesce(product_name, '') ||
    coalesce(price::text, '') ||
    coalesce(category_slug, '')
)
WHERE raw_hash IS NULL;


-- --------------------------------------------------
-- STEP 2: Remove duplicates (global)
-- --------------------------------------------------
DELETE FROM raw_price_events a
USING raw_price_events b
WHERE a.event_id < b.event_id
  AND a.source_name = b.source_name
  AND a.raw_hash = b.raw_hash;


-- --------------------------------------------------
-- STEP 3: Drop old indexes
-- --------------------------------------------------
DROP INDEX IF EXISTS uniq_raw_event;
DROP INDEX IF EXISTS uniq_raw_event_hash;
DROP INDEX IF EXISTS idx_raw_price_events_source_hash;
DROP INDEX IF EXISTS uniq_raw_event_per_run;


-- --------------------------------------------------
-- STEP 4: Enforce NOT NULL
-- --------------------------------------------------
ALTER TABLE raw_price_events
ALTER COLUMN raw_hash SET NOT NULL;


-- --------------------------------------------------
-- STEP 5: Create lookup index
-- --------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_raw_price_events_source_hash
ON raw_price_events (source_name, raw_hash);


-- --------------------------------------------------
-- STEP 6: New idempotency (CORRECT)
-- --------------------------------------------------
CREATE UNIQUE INDEX IF NOT EXISTS uniq_raw_event_per_run
ON raw_price_events (run_id, source_name, raw_hash);
