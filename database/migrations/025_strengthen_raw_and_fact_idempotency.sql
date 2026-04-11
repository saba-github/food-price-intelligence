-- RAW: move idempotency key from timestamp-based uniqueness to deterministic content hash.
DROP INDEX IF EXISTS uniq_raw_event;

-- 1️⃣ Canonical mapping oluştur
CREATE TEMP TABLE tmp_raw_canonical AS
SELECT
    event_id,
    source_name,
    raw_hash,
    MIN(event_id) OVER (
        PARTITION BY source_name, raw_hash
    ) AS canonical_event_id
FROM raw_price_events
WHERE raw_hash IS NOT NULL;

-- 2️⃣ TÜM child tabloları update et

UPDATE stg_source_products sp
SET event_id = rc.canonical_event_id
FROM tmp_raw_canonical rc
WHERE sp.event_id = rc.event_id
  AND rc.event_id <> rc.canonical_event_id;

UPDATE stg_price_observations s
SET event_id = rc.canonical_event_id
FROM tmp_raw_canonical rc
WHERE s.event_id = rc.event_id
  AND rc.event_id <> rc.canonical_event_id;

-- 🔥 YENİ EKLENEN (KRİTİK)
UPDATE stg_normalized_observations s
SET event_id = rc.canonical_event_id
FROM tmp_raw_canonical rc
WHERE s.event_id = rc.event_id
  AND rc.event_id <> rc.canonical_event_id;


-- 3️⃣ ŞİMDİ raw duplicate sil

DELETE FROM raw_price_events r
USING tmp_raw_canonical rc
WHERE r.event_id = rc.event_id
  AND rc.event_id <> rc.canonical_event_id;

-- 4️⃣ Index oluştur

CREATE UNIQUE INDEX IF NOT EXISTS uniq_raw_event_hash
ON raw_price_events (
    source_name,
    raw_hash
);

-- cleanup
DROP TABLE tmp_raw_canonical;