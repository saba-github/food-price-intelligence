DROP MATERIALIZED VIEW IF EXISTS mart_latest_prices;

CREATE MATERIALIZED VIEW mart_latest_prices AS
WITH ranked AS (
    SELECT
        f.source_name,
        f.source_product_id,
        f.standardized_product_name,
        f.product_id,
        f.price,
        f.price_per_unit,
        f.normalized_unit,
        f.observed_at,
        ROW_NUMBER() OVER (
            PARTITION BY f.source_name, f.source_product_id
            ORDER BY f.observed_at DESC, f.run_id DESC, f.fact_id DESC
        ) AS rn
    FROM fact_price_observations f
    JOIN mart_pipeline_health mph
        ON f.run_id = mph.run_id
    WHERE mph.pipeline_health_status IN ('healthy', 'warning')
      AND f.source_product_id IS NOT NULL
      AND f.standardized_product_name IS NOT NULL
)
SELECT
    source_name,
    source_product_id,
    standardized_product_name,
    product_id,
    price,
    price_per_unit,
    normalized_unit,
    observed_at
FROM ranked
WHERE rn = 1;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mart_latest_prices_source_product
    ON mart_latest_prices(source_name, source_product_id);

CREATE INDEX IF NOT EXISTS idx_mart_latest_prices_source_name
    ON mart_latest_prices(source_name);

CREATE INDEX IF NOT EXISTS idx_mart_latest_prices_standardized_product_name
    ON mart_latest_prices(standardized_product_name);
