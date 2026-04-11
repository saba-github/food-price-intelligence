-- 025_strengthen_raw_and_fact_idempotency.sql

-- RAW
DROP INDEX IF EXISTS uniq_raw_event;

-- 1️⃣ stg_source_products update
UPDATE stg_source_products sp
SET event_id = rc.canonical_event_id
FROM (
    SELECT
        event_id,
        MIN(event_id) OVER (
            PARTITION BY source_name, raw_hash
        ) AS canonical_event_id
    FROM raw_price_events
    WHERE raw_hash IS NOT NULL
) rc
WHERE sp.event_id = rc.event_id
  AND rc.event_id <> rc.canonical_event_id;

-- 2️⃣ stg_price_observations update
UPDATE stg_price_observations s
SET event_id = rc.canonical_event_id
FROM (
    SELECT
        event_id,
        MIN(event_id) OVER (
            PARTITION BY source_name, raw_hash
        ) AS canonical_event_id
    FROM raw_price_events
    WHERE raw_hash IS NOT NULL
) rc
WHERE s.event_id = rc.event_id
  AND rc.event_id <> rc.canonical_event_id;

-- 3️⃣ stg_normalized_observations update
UPDATE stg_normalized_observations s
SET event_id = rc.canonical_event_id
FROM (
    SELECT
        event_id,
        MIN(event_id) OVER (
            PARTITION BY source_name, raw_hash
        ) AS canonical_event_id
    FROM raw_price_events
    WHERE raw_hash IS NOT NULL
) rc
WHERE s.event_id = rc.event_id
  AND rc.event_id <> rc.canonical_event_id;

-- 4️⃣ raw duplicate sil
DELETE FROM raw_price_events r
USING (
    SELECT
        event_id,
        MIN(event_id) OVER (
            PARTITION BY source_name, raw_hash
        ) AS canonical_event_id
    FROM raw_price_events
    WHERE raw_hash IS NOT NULL
) rc
WHERE r.event_id = rc.event_id
  AND rc.event_id <> rc.canonical_event_id;

-- 5️⃣ unique index
CREATE UNIQUE INDEX IF NOT EXISTS uniq_raw_event_hash
ON raw_price_events (source_name, raw_hash);

-- FACT
ALTER TABLE fact_price_observations
ADD COLUMN IF NOT EXISTS event_id INTEGER;

-- backfill
UPDATE fact_price_observations f
SET event_id = s.event_id
FROM stg_price_observations s
WHERE f.observation_id = s.observation_id
  AND f.event_id IS NULL;

-- fact duplicate sil
DELETE FROM fact_price_observations f
USING (
    SELECT
        fact_id,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY fact_id
        ) AS rn
    FROM fact_price_observations
    WHERE event_id IS NOT NULL
) r
WHERE f.fact_id = r.fact_id
  AND r.rn > 1;

-- constraint
ALTER TABLE fact_price_observations
ALTER COLUMN event_id SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uniq_fact_event_id
ON fact_price_observations (event_id);