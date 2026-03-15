CREATE TABLE IF NOT EXISTS stg_price_observations (

    observation_id SERIAL PRIMARY KEY,

    event_id INTEGER REFERENCES raw_price_events(event_id),

    run_id INTEGER REFERENCES scrape_runs(run_id),

    source_name TEXT NOT NULL,

    source_product_id TEXT,
    source_sku TEXT,

    product_name TEXT,
    product_url TEXT,

    price NUMERIC,
    currency TEXT,

    normalized_unit TEXT,
    normalized_quantity NUMERIC,

    standardized_product_name TEXT,

    observed_at TIMESTAMP NOT NULL DEFAULT NOW()

);
