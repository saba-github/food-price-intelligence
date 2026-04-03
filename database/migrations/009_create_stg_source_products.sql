CREATE TABLE IF NOT EXISTS stg_source_products (
    source_id SERIAL PRIMARY KEY,

    event_id INTEGER REFERENCES raw_price_events(event_id),
    run_id INTEGER REFERENCES scrape_runs(run_id),

    source_name TEXT,

    source_product_id TEXT,
    source_sku TEXT,

    raw_product_name TEXT,
    raw_category_name TEXT,

    product_url TEXT,

    shown_price NUMERIC,
    regular_price NUMERIC,
    discount_rate NUMERIC,

    unit TEXT,
    unit_amount NUMERIC,

    observed_at TIMESTAMP DEFAULT NOW()
);
