-- 019_fix_raw_idempotency.sql

CREATE UNIQUE INDEX IF NOT EXISTS uniq_raw_event
ON raw_price_events (
    source_name,
    source_product_id,
    scraped_at
);
