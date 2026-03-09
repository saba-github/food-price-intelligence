CREATE TABLE scrape_runs (

    run_id SERIAL PRIMARY KEY,

    source_name TEXT NOT NULL,

    started_at TIMESTAMP NOT NULL DEFAULT NOW(),

    finished_at TIMESTAMP,

    status TEXT,

    records_scraped INTEGER,

    error_message TEXT

);
