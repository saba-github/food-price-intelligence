DROP MATERIALIZED VIEW IF EXISTS mart_top_movers CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mart_price_anomalies CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mart_pipeline_health CASCADE;

DROP MATERIALIZED VIEW IF EXISTS mart_daily_prices CASCADE;
DROP TABLE IF EXISTS mart_daily_prices CASCADE;

CREATE MATERIALIZED VIEW mart_daily_prices AS
SELECT
    DATE(observed_at) AS date,
    standardized_product_name,
    category_name,
    normalized_unit,
    AVG(price_per_unit) AS avg_price,
    MIN(price_per_unit) AS min_price,
    MAX(price_per_unit) AS max_price,
    COUNT(*) AS observation_count
FROM fact_price_observations
GROUP BY 1,2,3,4;

CREATE INDEX IF NOT EXISTS idx_mart_daily_prices_date
    ON mart_daily_prices(date);

CREATE INDEX IF NOT EXISTS idx_mart_daily_prices_product
    ON mart_daily_prices(standardized_product_name);
