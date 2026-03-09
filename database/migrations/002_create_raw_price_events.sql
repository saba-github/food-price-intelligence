CREATE TABLE raw_price_events (

    event_id SERIAL PRIMARY KEY,

    run_id INTEGER REFERENCES scrape_runs(run_id),

    source_name TEXT NOT NULL,

    product_name TEXT,

    product_url TEXT,

    price NUMERIC,

    currency TEXT,

    scraped_at TIMESTAMP DEFAULT NOW(),

    raw_payload JSONB

);
