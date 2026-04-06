DROP MATERIALIZED VIEW IF EXISTS mart_daily_prices;

CREATE MATERIALIZED VIEW mart_daily_prices AS
SELECT
    DATE(observed_at) as date,
    standardized_product_name,
    category_name,
    normalized_unit,
    AVG(price_per_unit) as avg_price,
    MIN(price_per_unit) as min_price,
    MAX(price_per_unit) as max_price,
    COUNT(*) as observation_count
FROM fact_price_observations
GROUP BY 1,2,3,4;

CREATE INDEX IF NOT EXISTS idx_mart_daily_prices_date
ON mart_daily_prices(date);
