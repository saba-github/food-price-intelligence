CREATE TABLE IF NOT EXISTS ops_data_quality_results (
    quality_id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES scrape_runs(run_id),
    check_name TEXT NOT NULL,
    check_status TEXT NOT NULL CHECK (check_status IN ('pass', 'fail')),
    observed_value NUMERIC,
    threshold_value NUMERIC,
    details TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
