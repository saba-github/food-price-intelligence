-- 026_align_raw_idempotency_with_run_scope.sql

-- --------------------------------------------------
-- STEP 0: Drop old global uniqueness first
-- --------------------------------------------------
ALTER TABLE raw_price_events
DROP CONSTRAINT IF EXISTS uniq_raw_event_hash_constraint;

DROP INDEX IF EXISTS uniq_raw_event;
DROP INDEX IF EXISTS uniq_raw_event_hash;
DROP INDEX IF EXISTS idx_raw_price_events_source_hash;
DROP INDEX IF EXISTS uniq_raw_event_per_run;

-- --------------------------------------------------
-- STEP 1: Fill NULL raw_hash
-- --------------------------------------------------
UPDATE raw_price_events
SET raw_hash = md5(
    coalesce(source_name, '') || '|' ||
    coalesce(source_product_id, '') || '|' ||
    coalesce(source_sku, '') || '|' ||
    coalesce(product_name, '') || '|' ||
    coalesce(price::text, '') || '|' ||
    coalesce(category_slug, '')
)
WHERE raw_hash IS NULL;

-- --------------------------------------------------
-- STEP 2: Repoint child tables to canonical raw row
-- Canonical grain = (run_id, source_name, raw_hash)
-- --------------------------------------------------

-- stg_source_products
UPDATE stg_source_products sp
SET event_id = x.canonical_event_id
FROM (
    SELECT
        event_id,
        MIN(event_id) OVER (
            PARTITION BY run_id, source_name, raw_hash
        ) AS canonical_event_id
    FROM raw_price_events
    WHERE raw_hash IS NOT NULL
) x
WHERE sp.event_id = x.event_id
  AND x.event_id <> x.canonical_event_id;

-- stg_price_observations
UPDATE stg_price_observations spo
SET event_id = x.canonical_event_id
FROM (
    SELECT
        event_id,
        MIN(event_id) OVER (
            PARTITION BY run_id, source_name, raw_hash
        ) AS canonical_event_id
    FROM raw_price_events
    WHERE raw_hash IS NOT NULL
) x
WHERE spo.event_id = x.event_id
  AND x.event_id <> x.canonical_event_id;

-- stg_normalized_observations
UPDATE stg_normalized_observations sno
SET event_id = x.canonical_event_id
FROM (
    SELECT
        event_id,
        MIN(event_id) OVER (
            PARTITION BY run_id, source_name, raw_hash
        ) AS canonical_event_id
    FROM raw_price_events
    WHERE raw_hash IS NOT NULL
) x
WHERE sno.event_id = x.event_id
  AND x.event_id <> x.canonical_event_id;

-- --------------------------------------------------
-- STEP 3: Delete duplicate raw rows safely
-- --------------------------------------------------
DELETE FROM raw_price_events r
USING (
    SELECT
        event_id,
        MIN(event_id) OVER (
            PARTITION BY run_id, source_name, raw_hash
        ) AS canonical_event_id
    FROM raw_price_events
    WHERE raw_hash IS NOT NULL
) x
WHERE r.event_id = x.event_id
  AND x.event_id <> x.canonical_event_id;

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
-- STEP 6: Run-scoped idempotency
-- --------------------------------------------------
CREATE UNIQUE INDEX IF NOT EXISTS uniq_raw_event_per_run
ON raw_price_events (run_id, source_name, raw_hash);
