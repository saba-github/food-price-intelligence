DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_views
        WHERE schemaname = 'public' AND viewname = 'mart_top_movers'
    ) THEN
        EXECUTE 'DROP VIEW public.mart_top_movers CASCADE';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_matviews
        WHERE schemaname = 'public' AND matviewname = 'mart_top_movers'
    ) THEN
        EXECUTE 'DROP MATERIALIZED VIEW public.mart_top_movers CASCADE';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_views
        WHERE schemaname = 'public' AND viewname = 'mart_price_anomalies'
    ) THEN
        EXECUTE 'DROP VIEW public.mart_price_anomalies CASCADE';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_matviews
        WHERE schemaname = 'public' AND matviewname = 'mart_price_anomalies'
    ) THEN
        EXECUTE 'DROP MATERIALIZED VIEW public.mart_price_anomalies CASCADE';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_views
        WHERE schemaname = 'public' AND viewname = 'mart_pipeline_health'
    ) THEN
        EXECUTE 'DROP VIEW public.mart_pipeline_health CASCADE';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_matviews
        WHERE schemaname = 'public' AND matviewname = 'mart_pipeline_health'
    ) THEN
        EXECUTE 'DROP MATERIALIZED VIEW public.mart_pipeline_health CASCADE';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_views
        WHERE schemaname = 'public' AND viewname = 'mart_daily_prices'
    ) THEN
        EXECUTE 'DROP VIEW public.mart_daily_prices CASCADE';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_matviews
        WHERE schemaname = 'public' AND matviewname = 'mart_daily_prices'
    ) THEN
        EXECUTE 'DROP MATERIALIZED VIEW public.mart_daily_prices CASCADE';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'mart_daily_prices'
          AND table_type = 'BASE TABLE'
    ) THEN
        EXECUTE 'DROP TABLE public.mart_daily_prices CASCADE';
    END IF;
END $$;

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
