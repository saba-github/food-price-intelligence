-- 020_fix_fact_idempotency.sql

CREATE UNIQUE INDEX IF NOT EXISTS uniq_fact_event
ON fact_price_observations (
    source_name,
    source_product_id,
    observed_at
);
