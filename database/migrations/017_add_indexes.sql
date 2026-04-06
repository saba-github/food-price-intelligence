CREATE INDEX IF NOT EXISTS idx_fact_price_observations_run_id
    ON fact_price_observations(run_id);

CREATE INDEX IF NOT EXISTS idx_fact_price_observations_observed_at
    ON fact_price_observations(observed_at);

CREATE INDEX IF NOT EXISTS idx_fact_price_observations_product_name
    ON fact_price_observations(standardized_product_name);

CREATE INDEX IF NOT EXISTS idx_fact_price_observations_product_id
    ON fact_price_observations(product_id);

CREATE INDEX IF NOT EXISTS idx_stg_source_products_run_id
    ON stg_source_products(run_id);

CREATE INDEX IF NOT EXISTS idx_stg_source_products_source_product
    ON stg_source_products(source_name, source_product_id);

CREATE INDEX IF NOT EXISTS idx_raw_price_events_run_id
    ON raw_price_events(run_id);

CREATE INDEX IF NOT EXISTS idx_raw_price_events_raw_hash
    ON raw_price_events(raw_hash);

CREATE INDEX IF NOT EXISTS idx_raw_price_events_raw_hash
    ON raw_price_events(raw_hash);
