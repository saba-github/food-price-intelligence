DROP MATERIALIZED VIEW IF EXISTS mart_latest_prices;
DROP MATERIALIZED VIEW IF EXISTS mart_pipeline_health;

CREATE MATERIALIZED VIEW mart_pipeline_health AS
WITH failed_checks AS (
    SELECT
        run_id,
        check_name,
        details,
        created_at,
        ROW_NUMBER() OVER (
            PARTITION BY run_id
            ORDER BY created_at DESC, quality_id DESC
        ) AS rn
    FROM ops_data_quality_results
    WHERE check_status = 'fail'
)
SELECT
    sr.run_id,
    sr.source_name,
    sr.started_at,
    sr.finished_at,
    sr.status,
    sr.records_scraped,
    sr.records_raw,
    sr.records_stg,
    sr.records_fact,
    sr.records_suspicious,
    sr.records_failed,
    CASE
        WHEN sr.finished_at IS NOT NULL
        THEN EXTRACT(EPOCH FROM (sr.finished_at - sr.started_at))
        ELSE NULL
    END AS run_duration_seconds,
    COUNT(dq.quality_id) AS total_checks,
    COUNT(*) FILTER (WHERE dq.check_status = 'pass') AS passed_checks,
    COUNT(*) FILTER (WHERE dq.check_status = 'fail') AS failed_checks,
    fc.check_name AS last_failed_check_name,
    fc.details AS last_failed_check_details,
    CASE
        WHEN sr.status = 'failed' THEN 'failed'
        WHEN sr.status = 'running' THEN 'running'
        WHEN COUNT(*) FILTER (WHERE dq.check_status = 'fail') > 0 THEN 'warning'
        WHEN sr.status = 'success' THEN 'healthy'
        ELSE 'unknown'
    END AS pipeline_health_status
FROM scrape_runs sr
LEFT JOIN ops_data_quality_results dq
    ON sr.run_id = dq.run_id
LEFT JOIN failed_checks fc
    ON sr.run_id = fc.run_id
   AND fc.rn = 1
GROUP BY
    sr.run_id,
    sr.source_name,
    sr.started_at,
    sr.finished_at,
    sr.status,
    sr.records_scraped,
    sr.records_raw,
    sr.records_stg,
    sr.records_fact,
    sr.records_suspicious,
    sr.records_failed,
    fc.check_name,
    fc.details;

CREATE INDEX IF NOT EXISTS idx_mart_pipeline_health_run_id
    ON mart_pipeline_health(run_id DESC);

CREATE INDEX IF NOT EXISTS idx_mart_pipeline_health_status
    ON mart_pipeline_health(pipeline_health_status);
