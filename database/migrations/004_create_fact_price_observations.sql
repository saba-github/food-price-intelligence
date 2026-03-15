CREATE TABLE IF NOT EXISTS fact_price_observations (

    fact_id SERIAL PRIMARY KEY,

    observation_id INTEGER REFERENCES stg_price_observations(observation_id),

    run_id INTEGER REFERENCES scrape_runs(run_id),

    source_name TEXT NOT NULL,

    source_product_id TEXT,
    source_sku TEXT,

    product_name TEXT,
    standardized_product_name TEXT,

    product_url TEXT,

    normalized_unit TEXT,
    normalized_quantity NUMERIC,

    price NUMERIC CHECK (price >= 0),
    currency TEXT,

    observed_at TIMESTAMP NOT NULL,

    loaded_at TIMESTAMP NOT NULL DEFAULT NOW()

);
