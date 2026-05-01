-- Repair existing normalized names that contain a lowercase i plus combining dot
-- from Turkish uppercase I-with-dot lowercasing, then make product aliases
-- self-healing for product matching and expose a live price history view.

UPDATE dim_products
SET
    standardized_product_name = replace(standardized_product_name, U&'\0069\0307', 'i'),
    canonical_name = replace(canonical_name, U&'\0069\0307', 'i')
WHERE standardized_product_name LIKE '%' || U&'\0069\0307' || '%'
   OR canonical_name LIKE '%' || U&'\0069\0307' || '%';

UPDATE fact_price_observations
SET standardized_product_name = replace(standardized_product_name, U&'\0069\0307', 'i')
WHERE standardized_product_name LIKE '%' || U&'\0069\0307' || '%';

UPDATE stg_price_observations
SET standardized_product_name = replace(standardized_product_name, U&'\0069\0307', 'i')
WHERE standardized_product_name LIKE '%' || U&'\0069\0307' || '%';

UPDATE stg_normalized_observations
SET standardized_product_name = replace(standardized_product_name, U&'\0069\0307', 'i')
WHERE standardized_product_name LIKE '%' || U&'\0069\0307' || '%';

UPDATE dim_product_aliases
SET
    alias_text = replace(alias_text, U&'\0069\0307', 'i'),
    normalized_alias = replace(normalized_alias, U&'\0069\0307', 'i')
WHERE alias_text LIKE '%' || U&'\0069\0307' || '%'
   OR normalized_alias LIKE '%' || U&'\0069\0307' || '%';

INSERT INTO dim_product_aliases (
    product_id,
    alias_text,
    normalized_alias
)
SELECT
    product_id,
    standardized_product_name AS alias_text,
    standardized_product_name AS normalized_alias
FROM dim_products
WHERE standardized_product_name IS NOT NULL
  AND standardized_product_name <> ''
ON CONFLICT (normalized_alias) DO NOTHING;

WITH alias_rules(alias_text, normalized_alias, match_pattern) AS (
    VALUES
        ('muz', 'muz', '%muz%'),
        ('domates', 'domates', '%domates%'),
        ('elma', 'elma', '%elma%'),
        ('hiyar', 'hiyar', '%hiyar%'),
        ('salatalik', 'salatalik', '%hiyar%'),
        ('limon', 'limon', '%limon%'),
        ('patates', 'patates', '%patates%'),
        ('sogan', 'sogan', '%sogan%')
),
alias_candidates AS (
    SELECT DISTINCT ON (ar.normalized_alias)
        dp.product_id,
        ar.alias_text,
        ar.normalized_alias
    FROM alias_rules ar
    JOIN dim_products dp
      ON dp.standardized_product_name LIKE ar.match_pattern
    ORDER BY
        ar.normalized_alias,
        CASE
            WHEN dp.standardized_product_name = ar.normalized_alias THEN 1
            WHEN dp.standardized_product_name LIKE ar.normalized_alias || ' %' THEN 2
            ELSE 3
        END,
        dp.product_id
)
INSERT INTO dim_product_aliases (
    product_id,
    alias_text,
    normalized_alias
)
SELECT
    product_id,
    alias_text,
    normalized_alias
FROM alias_candidates
ON CONFLICT (normalized_alias) DO NOTHING;

CREATE OR REPLACE VIEW price_history AS
SELECT
    f.fact_id AS price_observation_id,
    f.observed_at::date AS price_date,
    f.observed_at,
    f.run_id,
    f.source_name,
    f.source_product_id,
    f.source_sku,
    f.product_id,
    dp.canonical_name,
    f.standardized_product_name,
    f.product_name AS source_product_name,
    f.product_url,
    f.brand_name,
    f.category_name,
    f.normalized_unit,
    f.normalized_quantity,
    f.currency,
    f.price,
    f.regular_price,
    f.discount_rate,
    f.price_per_unit,
    f.unit_price_label,
    (
        COALESCE(f.discount_rate, 0) > 0
        OR (
            f.regular_price IS NOT NULL
            AND f.price IS NOT NULL
            AND f.regular_price > f.price
        )
    ) AS is_promotion,
    sr.status AS run_status
FROM fact_price_observations f
LEFT JOIN dim_products dp
    ON f.product_id = dp.product_id
LEFT JOIN scrape_runs sr
    ON f.run_id = sr.run_id;

CREATE OR REPLACE VIEW mart_cross_compare AS
WITH latest AS (
    SELECT
        ph.*,
        ROW_NUMBER() OVER (
            PARTITION BY ph.standardized_product_name, ph.source_name
            ORDER BY ph.observed_at DESC, ph.price_observation_id DESC
        ) AS rn
    FROM price_history ph
    WHERE ph.standardized_product_name IS NOT NULL
      AND ph.price IS NOT NULL
),
filtered AS (
    SELECT *
    FROM latest
    WHERE rn = 1
),
pivoted AS (
    SELECT
        standardized_product_name,
        COALESCE(
            MAX(CASE WHEN source_name = 'a101' THEN canonical_name END),
            MAX(CASE WHEN source_name = 'migros' THEN canonical_name END)
        ) AS canonical_name,
        MAX(CASE WHEN source_name = 'a101' THEN source_product_name END) AS a101_source_product_name,
        MAX(CASE WHEN source_name = 'migros' THEN source_product_name END) AS migros_source_product_name,
        MAX(CASE WHEN source_name = 'a101' THEN normalized_unit END) AS a101_normalized_unit,
        MAX(CASE WHEN source_name = 'migros' THEN normalized_unit END) AS migros_normalized_unit,
        MAX(CASE WHEN source_name = 'a101' THEN normalized_quantity END) AS a101_normalized_quantity,
        MAX(CASE WHEN source_name = 'migros' THEN normalized_quantity END) AS migros_normalized_quantity,
        MAX(CASE WHEN source_name = 'a101' THEN price END) AS a101_price,
        MAX(CASE WHEN source_name = 'migros' THEN price END) AS migros_price,
        MAX(CASE WHEN source_name = 'a101' THEN price_per_unit END) AS a101_price_per_unit,
        MAX(CASE WHEN source_name = 'migros' THEN price_per_unit END) AS migros_price_per_unit,
        MAX(CASE WHEN source_name = 'a101' THEN observed_at END) AS a101_observed_at,
        MAX(CASE WHEN source_name = 'migros' THEN observed_at END) AS migros_observed_at,
        MAX(observed_at) AS compared_at
    FROM filtered
    GROUP BY standardized_product_name
    HAVING COUNT(DISTINCT source_name) = 2
),
safety AS (
    SELECT
        *,
        (
            a101_normalized_unit IS NOT NULL
            AND migros_normalized_unit IS NOT NULL
            AND a101_normalized_unit = migros_normalized_unit
        ) AS same_unit_flag,
        (
            a101_normalized_quantity IS NOT NULL
            AND migros_normalized_quantity IS NOT NULL
            AND a101_normalized_quantity = migros_normalized_quantity
        ) AS same_quantity_flag
    FROM pivoted
)
SELECT
    standardized_product_name,
    canonical_name,
    a101_price,
    migros_price,
    ROUND(migros_price - a101_price, 2) AS price_diff,
    ROUND((migros_price - a101_price) / NULLIF(a101_price, 0) * 100, 2) AS price_diff_pct,
    CASE
        WHEN a101_price < migros_price THEN 'a101'
        WHEN a101_price > migros_price THEN 'migros'
        ELSE 'same'
    END AS cheaper_source,
    compared_at,
    a101_source_product_name,
    migros_source_product_name,
    a101_normalized_unit,
    migros_normalized_unit,
    a101_normalized_quantity,
    migros_normalized_quantity,
    a101_price_per_unit,
    migros_price_per_unit,
    a101_observed_at,
    migros_observed_at,
    same_unit_flag,
    same_quantity_flag,
    CASE
        WHEN same_unit_flag AND same_quantity_flag THEN 'high'
        WHEN same_unit_flag THEN 'medium'
        ELSE 'low'
    END AS comparison_confidence
FROM safety;
