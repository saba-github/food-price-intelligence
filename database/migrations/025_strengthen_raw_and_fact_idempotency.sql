-- 025_strengthen_raw_and_fact_idempotency.sql

-- RAW: move idempotency key from timestamp-based uniqueness to deterministic content hash.
DROP INDEX IF EXISTS uniq_raw_event;

-- Build a canonical mapping for duplicated raw events.
WITH raw_canonical AS (
    SELECT
        event_id,
        source_name,
        raw_hash,
        MIN(event_id) OVER (
            PARTITION BY source_name, raw_hash
        ) AS canonical_event_id
    FROM raw_price_events
    WHERE raw_hash IS NOT NULL
),

-- Re-point child tables to the canonical raw event first.
updated_stg_source_products AS (
    UPDATE stg_source_products sp
    SET event_id = rc.canonical_event_id
    FROM raw_canonical rc
    WHERE sp.event_id = rc.event_id
      AND rc.event_id <> rc.canonical_event_id
    RETURNING sp.event_id
),

updated_stg_price_observations AS (
    UPDATE stg_price_observations s
    SET event_id = rc.canonical_event_id
    FROM raw_canonical rc
    WHERE s.event_id = rc.event_id
      AND rc.event_id <> rc.canonical_event_id
    RETURNING s.observation_id
)

-- Now delete only non-canonical duplicate raw rows.
DELETE FROM raw_price_events r
USING raw_canonical rc
WHERE r.event_id = rc.event_id
  AND rc.event_id <> rc.canonical_event_id;

CREATE UNIQUE INDEX IF NOT EXISTS uniq_raw_event_hash
ON raw_price_events (
    source_name,
    raw_hash
);

-- FACT: add event_id lineage key and enforce one fact row per raw event.
ALTER TABLE fact_price_observations
ADD COLUMN IF NOT EXISTS event_id INTEGER;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fk_fact_event'
    ) THEN
        ALTER TABLE fact_price_observations
        ADD CONSTRAINT fk_fact_event
        FOREIGN KEY (event_id)
        REFERENCES raw_price_events(event_id);
    END IF;
END $$;

-- Backfill event_id from staging linkage (observation_id -> stg_price_observations.event_id).
UPDATE fact_price_observations f
SET event_id = s.event_id
FROM stg_price_observations s
WHERE f.observation_id = s.observation_id
  AND f.event_id IS NULL;

-- If historical duplicates exist for the same event_id, keep earliest fact row.
WITH ranked AS (
    SELECT
        fact_id,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY fact_id
        ) AS rn
    FROM fact_price_observations
    WHERE event_id IS NOT NULL
)
DELETE FROM fact_price_observations f
USING ranked r
WHERE f.fact_id = r.fact_id
  AND r.rn > 1;

DROP INDEX IF EXISTS uniq_fact_event;

ALTER TABLE fact_price_observations
ALTER COLUMN event_id SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uniq_fact_event_id
ON fact_price_observations (
    event_id
);