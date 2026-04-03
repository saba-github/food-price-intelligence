CREATE TABLE IF NOT EXISTS stg_normalized_observations (
    observation_id SERIAL PRIMARY KEY,

    event_id INTEGER REFERENCES raw_price_events(event_id),
    run_id INTEGER REFERENCES scrape_runs(run_id),

    source_name TEXT,
    source_product_id TEXT,

    raw_product_name TEXT,
    standardized_product_name TEXT,

    normalized_unit TEXT,
    normalized_quantity NUMERIC,

    price NUMERIC,
    price_per_unit NUMERIC,
    unit_price_label TEXT,

    brand_name TEXT,
    category_name TEXT,

    is_suspicious BOOLEAN,
    suspicious_reason TEXT,

    observed_at TIMESTAMP DEFAULT NOW()
);
