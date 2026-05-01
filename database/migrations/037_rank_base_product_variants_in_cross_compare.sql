CREATE OR REPLACE VIEW mart_cross_compare AS
WITH latest_source_base AS (
    SELECT
        ph.*,
        ROW_NUMBER() OVER (
            PARTITION BY ph.source_name, ph.source_product_name
            ORDER BY ph.observed_at DESC, ph.price_observation_id DESC
        ) AS source_rn
    FROM price_history ph
    WHERE ph.standardized_product_name IS NOT NULL
      AND ph.price IS NOT NULL
),
latest_source AS (
    SELECT
        *,
        COALESCE(
            (
                SELECT STRING_AGG(token, ' ' ORDER BY token)
                FROM UNNEST(
                    REGEXP_SPLIT_TO_ARRAY(LOWER(source_product_name), E'[[:space:]]+')
                ) AS parts(token)
                WHERE token <> ''
                  AND token !~ E'^[0-9]+(?:[.,][0-9]+)?$'
                  AND token NOT IN ('kg', 'g', 'gram', 'ml', 'l', 'lt', 'adet', 'demet')
            ),
            LOWER(source_product_name)
        ) AS source_base_name,
        (
            SELECT COUNT(*)
            FROM UNNEST(
                REGEXP_SPLIT_TO_ARRAY(LOWER(source_product_name), E'[[:space:]]+')
            ) AS parts(token)
            WHERE token <> ''
              AND token !~ E'^[0-9]+(?:[.,][0-9]+)?$'
              AND token NOT IN ('kg', 'g', 'gram', 'ml', 'l', 'lt', 'adet', 'demet')
        ) AS source_token_count,
        (
            SELECT COUNT(*)
            FROM UNNEST(
                REGEXP_SPLIT_TO_ARRAY(LOWER(source_product_name), E'[[:space:]]+')
            ) AS parts(token)
            WHERE token IN (
                'yerli', 'kokteyl', 'salkim', 'salkım', 'pembe', 'cherry', 'mini',
                'organik', 'ithal', 'paket', 'özel', 'ozel', 'sosluk',
                'salçalık', 'salcalik', 'şeker', 'seker',
                'çengelköy', 'cengelkoy', 'kestane', 'istiridye', 'istridye', 'shiitake'
            )
              AND NOT (
                  token = ANY(
                      REGEXP_SPLIT_TO_ARRAY(COALESCE(standardized_product_name, ''), E'[[:space:]]+')
                  )
              )
        ) AS variant_penalty,
        (
            SELECT COUNT(*)
            FROM UNNEST(
                REGEXP_SPLIT_TO_ARRAY(LOWER(source_product_name), E'[[:space:]]+')
            ) AS parts(token)
            WHERE token <> ''
              AND token !~ E'^[0-9]+(?:[.,][0-9]+)?$'
              AND token NOT IN ('kg', 'g', 'gram', 'ml', 'l', 'lt', 'adet', 'demet')
              AND NOT (
                  token = ANY(
                      REGEXP_SPLIT_TO_ARRAY(COALESCE(standardized_product_name, ''), E'[[:space:]]+')
                  )
              )
        ) AS extra_token_count
    FROM latest_source_base
    WHERE source_rn = 1
),
ranked AS (
    SELECT
        ls.*,
        ROW_NUMBER() OVER (
            PARTITION BY ls.standardized_product_name, ls.source_name
            ORDER BY
                CASE
                    WHEN ls.source_base_name = ls.standardized_product_name THEN 0
                    ELSE 1
                END,
                ls.variant_penalty,
                ls.extra_token_count,
                ls.source_token_count,
                ls.observed_at DESC,
                ls.price_observation_id DESC
        ) AS selection_rn
    FROM latest_source ls
),
filtered AS (
    SELECT *
    FROM ranked
    WHERE selection_rn = 1
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
