DROP MATERIALIZED VIEW IF EXISTS mart_daily_prices_by_retailer;

CREATE MATERIALIZED VIEW mart_daily_prices_by_retailer AS
SELECT
    DATE(observed_at) AS date,
    source_name,
    standardized_product_name,
    category_name,
    normalized_unit,
    AVG(price_per_unit) AS avg_price,
    MIN(price_per_unit) AS min_price,
    MAX(price_per_unit) AS max_price,
    COUNT(*) AS observation_count
FROM fact_price_observations
WHERE price_per_unit IS NOT NULL
GROUP BY 1,2,3,4,5;

CREATE INDEX IF NOT EXISTS idx_mart_daily_prices_by_retailer_date
    ON mart_daily_prices_by_retailer(date);

CREATE INDEX IF NOT EXISTS idx_mart_daily_prices_by_retailer_product
    ON mart_daily_prices_by_retailer(standardized_product_name);

CREATE INDEX IF NOT EXISTS idx_mart_daily_prices_by_retailer_source
    ON mart_daily_prices_by_retailer(source_name);