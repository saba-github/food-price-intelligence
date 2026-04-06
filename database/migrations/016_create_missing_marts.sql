DROP MATERIALIZED VIEW IF EXISTS mart_top_movers;
DROP MATERIALIZED VIEW IF EXISTS mart_price_anomalies;
DROP MATERIALIZED VIEW IF EXISTS mart_pipeline_health;

CREATE MATERIALIZED VIEW mart_top_movers AS
WITH daily_product_prices AS (
    SELECT
        DATE(observed_at) AS date,
        standardized_product_name,
        category_name,
        AVG(price_per_unit) AS avg_price
    FROM fact_price_observations
    WHERE price_per_unit IS NOT NULL
    GROUP BY 1,2,3
),
ranked AS (
    SELECT
        date,
        standardized_product_name,
        category_name,
        avg_price AS latest_price,
        LAG(avg_price) OVER (
            PARTITION BY standardized_product_name
            ORDER BY date
        ) AS previous_price
    FROM daily_product_prices
)
SELECT
    standardized_product_name,
    category_name,
    date,
    latest_price,
    previous_price,
    (latest_price - previous_price) AS abs_change,
    CASE
        WHEN previous_price IS NULL OR previous_price = 0 THEN NULL
        ELSE ROUND(((latest_price - previous_price) / previous_price) * 100, 2)
    END AS pct_change
FROM ranked
WHERE previous_price IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_mart_top_movers_pct_change
    ON mart_top_movers(pct_change);

CREATE INDEX IF NOT EXISTS idx_mart_top_movers_product
    ON mart_top_movers(standardized_product_name);


CREATE MATERIALIZED VIEW mart_price_anomalies AS
SELECT
    standardized_product_name,
    category_name,
    AVG(price_per_unit) AS avg_price_per_unit,
    STDDEV_POP(price_per_unit) AS volatility,
    COUNT(*) AS observation_count,
    CASE
        WHEN STDDEV_POP(price_per_unit) >= 50 THEN 'high'
        WHEN STDDEV_POP(price_per_unit) >= 20 THEN 'medium'
        ELSE 'low'
    END AS volatility_level
FROM fact_price_observations
WHERE price_per_unit IS NOT NULL
GROUP BY 1,2
HAVING COUNT(*) >= 2;

CREATE INDEX IF NOT EXISTS idx_mart_price_anomalies_volatility
    ON mart_price_anomalies(volatility DESC);

CREATE INDEX IF NOT EXISTS idx_mart_price_anomalies_product
    ON mart_price_anomalies(standardized_product_name);


CREATE MATERIALIZED VIEW mart_pipeline_health AS
SELECT
    sr.run_id,
    sr.source_name,
    sr.started_at,
    sr.finished_at,
    sr.status,
    sr.records_scraped,
    COUNT(dq.quality_id) AS total_checks,
    COUNT(*) FILTER (WHERE dq.check_status = 'pass') AS passed_checks,
    COUNT(*) FILTER (WHERE dq.check_status = 'fail') AS failed_checks,
    CASE
        WHEN sr.status = 'failed' THEN 'failed'
        WHEN COUNT(*) FILTER (WHERE dq.check_status = 'fail') > 0 THEN 'warning'
        WHEN sr.status = 'success' THEN 'healthy'
        ELSE 'unknown'
    END AS pipeline_health_status
FROM scrape_runs sr
LEFT JOIN ops_data_quality_results dq
    ON sr.run_id = dq.run_id
GROUP BY
    sr.run_id,
    sr.source_name,
    sr.started_at,
    sr.finished_at,
    sr.status,
    sr.records_scraped;

CREATE INDEX IF NOT EXISTS idx_mart_pipeline_health_run_id
    ON mart_pipeline_health(run_id DESC);
