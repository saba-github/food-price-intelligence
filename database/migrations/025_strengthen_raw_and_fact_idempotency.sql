-- 025_strengthen_raw_and_fact_idempotency.sql

-- RAW: move idempotency key from timestamp-based uniqueness to deterministic content hash.

DROP INDEX IF EXISTS uniq_raw_event;

-- If historical duplicates exist for the same raw business event, keep earliest row.
WITH ranked_raw AS (
    SELECT
        event_id,
        ROW_NUMBER() OVER (
            PARTITION BY source_name, raw_hash
            ORDER BY event_id
        ) AS rn
    FROM raw_price_events
    WHERE raw_hash IS NOT NULL
)
DELETE FROM raw_price_events r
USING ranked_raw rr
WHERE r.event_id = rr.event_id
  AND rr.rn > 1;

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