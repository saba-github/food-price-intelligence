CREATE TABLE fact_price_observations (

    fact_id SERIAL PRIMARY KEY,

    observation_id INTEGER REFERENCES stg_price_observations(observation_id),

    product_name TEXT NOT NULL,

    source_name TEXT NOT NULL,

    price NUMERIC NOT NULL,

    currency TEXT,

    normalized_unit TEXT,

    normalized_quantity NUMERIC,

    observed_at TIMESTAMP,

    created_at TIMESTAMP DEFAULT NOW()

);
