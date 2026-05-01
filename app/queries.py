LATEST_DATES_QUERY = """
select distinct date
from mart_daily_prices
order by date desc
"""

GLOBAL_FRESHNESS_QUERY = """
with latest_data as (
    select max(date) as latest_data_date
    from mart_daily_prices
),
latest_success_run as (
    select max(started_at) as latest_success_started_at
    from scrape_runs
    where status = 'success'
),
latest_attempt as (
    select
        source_name as latest_run_source_name,
        status as latest_run_status,
        started_at as latest_run_started_at,
        error_message as latest_run_error_message
    from scrape_runs
    order by run_id desc
    limit 1
)
select
    ld.latest_data_date,
    lsr.latest_success_started_at,
    la.latest_run_source_name,
    la.latest_run_status,
    la.latest_run_started_at,
    la.latest_run_error_message
from latest_data ld
cross join latest_success_run lsr
cross join latest_attempt la
"""

CATEGORY_LIST_QUERY = """
select distinct category_name
from mart_daily_prices
where category_name is not null
order by category_name
"""

TOP_EXPENSIVE_QUERY = """
select
    standardized_product_name,
    category_name,
    avg_price
from mart_daily_prices
where date = %(selected_date)s
  and avg_price is not null
order by avg_price desc
limit 10
"""

TOP_CHEAPEST_QUERY = """
select
    standardized_product_name,
    category_name,
    avg_price
from mart_daily_prices
where date = %(selected_date)s
  and avg_price is not null
order by avg_price asc
limit 10
"""

TOP_VOLATILE_QUERY = """
select
    standardized_product_name,
    category_name,
    avg_price_per_unit,
    volatility,
    observation_count,
    volatility_level
from mart_price_anomalies
order by volatility desc
limit 10
"""

PRICE_TREND_QUERY = """
select
    date,
    standardized_product_name,
    avg_price
from mart_daily_prices
where standardized_product_name = %(product_name)s
order by date
"""


HOME_TOP_MOVER_CARD_QUERY = """
select
    standardized_product_name,
    category_name,
    date,
    previous_price,
    latest_price,
    pct_change
from mart_top_movers
where pct_change is not null
order by pct_change desc
limit 3;
"""

HOME_TOP_DECLINER_CARD_QUERY = """
select
    standardized_product_name,
    category_name,
    date,
    previous_price,
    latest_price,
    pct_change
from mart_top_movers
where pct_change is not null
order by pct_change asc
limit 3;
"""

HOME_RECENT_TRENDS_QUERY = """
with latest_prices as (
    select
        standardized_product_name,
        avg_price,
        date,
        row_number() over (
            partition by standardized_product_name
            order by date desc
        ) as rn
    from mart_daily_prices
    where avg_price is not null
),
trend_base as (
    select
        standardized_product_name
    from mart_top_movers
    where pct_change is not null
    order by abs(pct_change) desc
    limit 5
)
select
    mdp.date,
    mdp.standardized_product_name,
    mdp.avg_price
from mart_daily_prices mdp
join trend_base tb
    on mdp.standardized_product_name = tb.standardized_product_name
where mdp.avg_price is not null
order by mdp.standardized_product_name, mdp.date;
"""

TOP_MOVERS_QUERY = """
select
    standardized_product_name,
    category_name,
    date,
    latest_price,
    previous_price,
    abs_change,
    pct_change
from mart_top_movers
order by pct_change desc
limit 10
"""

TOP_DECLINERS_QUERY = """
select
    standardized_product_name,
    category_name,
    date,
    latest_price,
    previous_price,
    abs_change,
    pct_change
from mart_top_movers
order by pct_change asc
limit 10
"""

PIPELINE_HEALTH_QUERY = """
select
    run_id,
    source_name,
    started_at,
    finished_at,
    status,
    records_scraped,
    records_raw,
    records_stg,
    records_fact,
    records_suspicious,
    records_failed,
    run_duration_seconds,
    total_checks,
    passed_checks,
    failed_checks,
    last_failed_check_name,
    last_failed_check_details,
    pipeline_health_status
from mart_pipeline_health
order by run_id desc
limit 20
"""

QUALITY_RESULTS_QUERY = """
select
    run_id,
    check_name,
    check_status,
    observed_value,
    threshold_value,
    details,
    created_at
from ops_data_quality_results
order by quality_id desc
limit 20
"""


CROSS_RETAILER_PRODUCTS_QUERY = """
WITH latest AS (
    SELECT
        standardized_product_name,
        source_name,
        source_product_name,
        normalized_unit,
        normalized_quantity,
        ROW_NUMBER() OVER (
            PARTITION BY standardized_product_name, source_name
            ORDER BY observed_at DESC, price_observation_id DESC
        ) AS rn
    FROM price_history
    WHERE standardized_product_name IS NOT NULL
      AND price IS NOT NULL
),
safety AS (
    SELECT
        standardized_product_name,
        MAX(CASE WHEN source_name = 'a101' THEN source_product_name END) AS a101_source_product_name,
        MAX(CASE WHEN source_name = 'migros' THEN source_product_name END) AS migros_source_product_name,
        MAX(CASE WHEN source_name = 'a101' THEN normalized_unit END) AS a101_normalized_unit,
        MAX(CASE WHEN source_name = 'migros' THEN normalized_unit END) AS migros_normalized_unit,
        MAX(CASE WHEN source_name = 'a101' THEN normalized_quantity END) AS a101_normalized_quantity,
        MAX(CASE WHEN source_name = 'migros' THEN normalized_quantity END) AS migros_normalized_quantity,
        (
            COUNT(DISTINCT source_name) = 2
            AND MAX(CASE WHEN source_name = 'a101' THEN normalized_unit END) IS NOT NULL
            AND MAX(CASE WHEN source_name = 'migros' THEN normalized_unit END) IS NOT NULL
            AND MAX(CASE WHEN source_name = 'a101' THEN normalized_unit END)
                = MAX(CASE WHEN source_name = 'migros' THEN normalized_unit END)
        ) AS same_unit_flag,
        (
            COUNT(DISTINCT source_name) = 2
            AND MAX(CASE WHEN source_name = 'a101' THEN normalized_quantity END) IS NOT NULL
            AND MAX(CASE WHEN source_name = 'migros' THEN normalized_quantity END) IS NOT NULL
            AND MAX(CASE WHEN source_name = 'a101' THEN normalized_quantity END)
                = MAX(CASE WHEN source_name = 'migros' THEN normalized_quantity END)
        ) AS same_quantity_flag
    FROM latest
    WHERE rn = 1
    GROUP BY standardized_product_name
)
SELECT
    m.standardized_product_name,
    s.same_unit_flag,
    s.same_quantity_flag,
    s.a101_source_product_name,
    s.migros_source_product_name,
    s.a101_normalized_unit,
    s.migros_normalized_unit,
    s.a101_normalized_quantity,
    s.migros_normalized_quantity,
    CASE
        WHEN s.same_unit_flag AND s.same_quantity_flag
         AND COALESCE(tq.is_toilet_paper_pair, FALSE) = FALSE
        THEN 'high'
        WHEN s.same_unit_flag THEN 'medium'
        ELSE 'low'
    END AS comparison_confidence
FROM mart_cross_compare m
JOIN safety s
    ON m.standardized_product_name = s.standardized_product_name
WHERE m.standardized_product_name IS NOT NULL
ORDER BY
    CASE WHEN s.same_unit_flag AND s.same_quantity_flag THEN 0 ELSE 1 END,
    m.standardized_product_name;
"""

CROSS_RETAILER_COMPARISON_QUERY = """
SELECT
    date,
    source_name,
    standardized_product_name,
    avg_price,
    observation_count
FROM mart_daily_prices_by_retailer
WHERE standardized_product_name = %s
ORDER BY date, source_name;
"""

CHEAPEST_RETAILER_TODAY_QUERY = """
WITH latest AS (
    SELECT
        standardized_product_name,
        source_name,
        normalized_unit,
        normalized_quantity,
        ROW_NUMBER() OVER (
            PARTITION BY standardized_product_name, source_name
            ORDER BY observed_at DESC, price_observation_id DESC
        ) AS rn
    FROM price_history
    WHERE standardized_product_name IS NOT NULL
      AND price IS NOT NULL
),
safety AS (
    SELECT
        standardized_product_name,
        (
            COUNT(DISTINCT source_name) = 2
            AND MAX(CASE WHEN source_name = 'a101' THEN normalized_unit END) IS NOT NULL
            AND MAX(CASE WHEN source_name = 'migros' THEN normalized_unit END) IS NOT NULL
            AND MAX(CASE WHEN source_name = 'a101' THEN normalized_unit END)
                = MAX(CASE WHEN source_name = 'migros' THEN normalized_unit END)
        ) AS same_unit_flag,
        (
            COUNT(DISTINCT source_name) = 2
            AND MAX(CASE WHEN source_name = 'a101' THEN normalized_quantity END) IS NOT NULL
            AND MAX(CASE WHEN source_name = 'migros' THEN normalized_quantity END) IS NOT NULL
            AND MAX(CASE WHEN source_name = 'a101' THEN normalized_quantity END)
                = MAX(CASE WHEN source_name = 'migros' THEN normalized_quantity END)
        ) AS same_quantity_flag
    FROM latest
    WHERE rn = 1
    GROUP BY standardized_product_name
)
SELECT
    m.compared_at::date AS date,
    m.standardized_product_name,
    m.cheaper_source AS source_name,
    CASE
        WHEN m.cheaper_source = 'a101' THEN m.a101_price
        WHEN m.cheaper_source = 'migros' THEN m.migros_price
        ELSE m.a101_price
    END AS avg_price,
    'high' AS comparison_confidence
FROM mart_cross_compare m
JOIN safety s
    ON m.standardized_product_name = s.standardized_product_name
WHERE s.same_unit_flag
  AND s.same_quantity_flag
ORDER BY m.standardized_product_name;
"""


PUBLIC_PRODUCT_CATALOG_QUERY = """
WITH latest_source_base AS (
    SELECT
        standardized_product_name,
        source_name,
        source_product_name,
        brand_name,
        price,
        observed_at,
        price_observation_id,
        CASE
            WHEN standardized_product_name ILIKE '%uno%'
             AND standardized_product_name ILIKE '%bugday%'
             AND standardized_product_name ILIKE '%tava%'
             AND standardized_product_name ILIKE '%ekmeg%'
             AND normalized_unit = 'kg'
             AND normalized_quantity = 0.45
            THEN 'tava ekmek'
            WHEN (
                (
                    standardized_product_name = 'su'
                    OR standardized_product_name LIKE 'su %'
                    OR standardized_product_name LIKE '% su'
                    OR standardized_product_name LIKE '% suyu%'
                    OR standardized_product_name LIKE '% kaynak suyu%'
                )
                AND standardized_product_name NOT ILIKE '%sut%'
                AND standardized_product_name NOT ILIKE '%yogurt%'
                AND standardized_product_name NOT ILIKE '%susam%'
                AND standardized_product_name NOT ILIKE '%sos%'
                AND standardized_product_name NOT ILIKE '%maden%'
                AND standardized_product_name NOT ILIKE '%mineral%'
                AND standardized_product_name NOT ILIKE '%soda%'
                AND standardized_product_name NOT ILIKE '%gazli%'
                AND standardized_product_name NOT ILIKE '%aromali%'
                AND standardized_product_name NOT ILIKE '%12x%'
                AND standardized_product_name NOT ILIKE '%6x%'
                AND standardized_product_name NOT ILIKE '%4x%'
            )
            THEN 'su'
            WHEN (
                standardized_product_name LIKE '%tuz%'
                AND standardized_product_name NOT ILIKE '%tuzlu%'
                AND standardized_product_name NOT ILIKE '%zeytin%'
                AND standardized_product_name NOT ILIKE '%tereyagi%'
                AND standardized_product_name NOT ILIKE '%limon tuzu%'
                AND standardized_product_name NOT ILIKE '%himalaya%'
                AND standardized_product_name NOT ILIKE '%kaya%'
                AND standardized_product_name NOT ILIKE '%salamura%'
                AND standardized_product_name NOT ILIKE '%sarimsakli%'
                AND standardized_product_name NOT ILIKE '%truf%'
                AND standardized_product_name NOT ILIKE '%mantarli%'
            )
            THEN 'tuz'
            WHEN (
                standardized_product_name LIKE '%kola pepsi%'
                AND standardized_product_name NOT ILIKE '%4x%'
                AND standardized_product_name NOT ILIKE '%6x%'
                AND standardized_product_name NOT ILIKE '%kutu%'
                AND standardized_product_name NOT ILIKE '%zero%'
                AND standardized_product_name NOT ILIKE '%sekersiz%'
                AND standardized_product_name NOT ILIKE '%light%'
                AND standardized_product_name NOT ILIKE '%diet%'
                AND standardized_product_name NOT ILIKE '%sugar%'
                AND standardized_product_name NOT ILIKE '%free%'
                AND standardized_product_name NOT ILIKE '%lime%'
                AND standardized_product_name NOT ILIKE '%lemon%'
                AND standardized_product_name NOT ILIKE '%vanilla%'
                AND standardized_product_name NOT ILIKE '%cherry%'
            )
            THEN 'kola pepsi'
            WHEN (
                (
                    standardized_product_name LIKE '%coca-cola%'
                    OR standardized_product_name LIKE '%coca cola%'
                )
                AND standardized_product_name NOT ILIKE '%zero%'
                AND standardized_product_name NOT ILIKE '%sekersiz%'
                AND standardized_product_name NOT ILIKE '%light%'
                AND standardized_product_name NOT ILIKE '%diet%'
                AND standardized_product_name NOT ILIKE '%sugar%'
                AND standardized_product_name NOT ILIKE '%free%'
                AND standardized_product_name NOT ILIKE '%lime%'
                AND standardized_product_name NOT ILIKE '%lemon%'
                AND standardized_product_name NOT ILIKE '%vanilla%'
                AND standardized_product_name NOT ILIKE '%cherry%'
            )
            THEN 'kola coca-cola'
            WHEN (
                standardized_product_name ILIKE '%aycicek%'
                AND standardized_product_name ILIKE '%yag%'
                AND standardized_product_name NOT ILIKE '%zeytin%'
            )
            THEN 'aycicek yagi'
            WHEN (
                (
                    standardized_product_name ILIKE '%zeytinyagi%'
                    OR (
                        standardized_product_name ILIKE '%zeytin%'
                        AND standardized_product_name ILIKE '%yag%'
                    )
                )
                AND standardized_product_name NOT ILIKE '%aycicek%'
            )
            THEN 'zeytinyagi'
            WHEN (
                (
                    standardized_product_name ILIKE '%misirozu%'
                    OR (
                        standardized_product_name ILIKE '%misir%'
                        AND standardized_product_name ILIKE '%yag%'
                    )
                )
                AND standardized_product_name NOT ILIKE '%zeytin%'
            )
            THEN 'misir yagi'
            WHEN (
                standardized_product_name ILIKE '%findik%'
                AND standardized_product_name ILIKE '%yag%'
            )
            THEN 'findik yagi'
            WHEN (
                standardized_product_name ILIKE '%tuvalet%'
                AND standardized_product_name ILIKE '%kagid%'
                AND standardized_product_name NOT ILIKE '%islak%'
                AND standardized_product_name NOT ILIKE '%mendil%'
                AND standardized_product_name NOT ILIKE '%havlu%'
                AND standardized_product_name NOT ILIKE '%pecete%'
            )
            THEN COALESCE(NULLIF(TRIM(LOWER(SPLIT_PART(source_product_name, ' ', 1))), '') || ' tuvalet kagidi', 'tuvalet kagidi')
            WHEN (
                (
                    (
                        standardized_product_name ILIKE '%kagit%'
                        AND standardized_product_name ILIKE '%havlu%'
                    )
                    OR (
                        standardized_product_name ILIKE '%havlu%'
                        AND standardized_product_name ILIKE '%kagidi%'
                    )
                )
                AND standardized_product_name NOT ILIKE '%tuvalet%'
                AND standardized_product_name NOT ILIKE '%islak%'
                AND standardized_product_name NOT ILIKE '%mendil%'
                AND standardized_product_name NOT ILIKE '%pecete%'
                AND standardized_product_name NOT ILIKE '%el havlu%'
                AND standardized_product_name NOT ILIKE '%yuz havlu%'
            )
            THEN COALESCE(NULLIF(TRIM(LOWER(SPLIT_PART(source_product_name, ' ', 1))), '') || ' kagit havlu', 'kagit havlu')
            WHEN (
                standardized_product_name ILIKE '%arpacik%'
                AND standardized_product_name ILIKE '%sogan%'
            )
            THEN 'arpacik sogan'
            WHEN (
                standardized_product_name ILIKE '%sogan%'
                AND standardized_product_name NOT ILIKE '%soganli%'
                AND (
                    source_product_name ILIKE '%taze%'
                    OR source_product_name ILIKE '%demet%'
                    OR source_product_name ILIKE '%frenk%'
                )
            )
            THEN 'sogan taze'
            WHEN (
                standardized_product_name ILIKE '%sogan%'
                AND standardized_product_name NOT ILIKE '%soganli%'
                AND standardized_product_name NOT ILIKE '%arpacik%'
            )
            THEN 'sogan'
            WHEN (
                standardized_product_name ILIKE '%bildircin%'
                AND standardized_product_name ILIKE '%yumurta%'
            )
            THEN 'bildircin yumurta'
            WHEN standardized_product_name ILIKE '%yumurta%' THEN 'yumurta'
            WHEN standardized_product_name IN ('ekmek', 'ekmek sofra') THEN 'ekmek'
            WHEN standardized_product_name IN ('salatalik', 'hiyar') THEN 'salatalik'
            WHEN standardized_product_name ILIKE '%shiitake%' THEN 'shiitake mantar'
            WHEN standardized_product_name ILIKE '%istiridye%'
              OR standardized_product_name ILIKE '%istridye%' THEN 'istiridye mantar'
            WHEN standardized_product_name ILIKE '%kestane%' THEN 'kestane mantar'
            WHEN standardized_product_name ILIKE '%izgaralik%' THEN 'izgaralik mantar'
            WHEN standardized_product_name ILIKE '%mantar%' THEN 'mantar'
            ELSE COALESCE(
                (
                    SELECT STRING_AGG(token, ' ' ORDER BY ordinality)
                    FROM UNNEST(
                        REGEXP_SPLIT_TO_ARRAY(standardized_product_name, E'[[:space:]]+')
                    ) WITH ORDINALITY AS parts(token, ordinality)
                    WHERE token <> ''
                      AND token !~ E'^[0-9]+(?:[.,][0-9]+)?$'
                      AND token NOT IN (
                          'kg', 'g', 'gram', 'ml', 'l', 'lt',
                          'adet', 'demet', 'paket',
                          'dokme', 'kuru', 'taze', 'yerli', 'ithal', 'salkim'
                      )
                ),
                standardized_product_name
            )
        END AS canonical_search_name,
        CASE
            WHEN standardized_product_name ILIKE '%yumurta%'
             AND (
                 SUBSTRING(LOWER(standardized_product_name) FROM '([1-9][0-9]?)\s*[''’]') IS NOT NULL
                 OR SUBSTRING(LOWER(standardized_product_name) FROM '([1-9][0-9]?)\s*adet') IS NOT NULL
             ) THEN 'piece'
            WHEN SUBSTRING(LOWER(source_product_name) FROM '([0-9]+(?:[.,][0-9]+)?) *kg') IS NOT NULL THEN 'kg'
            WHEN SUBSTRING(LOWER(source_product_name) FROM '([0-9]+(?:[.,][0-9]+)?) *g(ram)?') IS NOT NULL THEN 'kg'
            WHEN SUBSTRING(LOWER(source_product_name) FROM '([0-9]+(?:[.,][0-9]+)?) *ml') IS NOT NULL THEN 'liter'
            WHEN SUBSTRING(LOWER(source_product_name) FROM '([0-9]+(?:[.,][0-9]+)?) *l') IS NOT NULL THEN 'liter'
            ELSE normalized_unit
        END AS comparison_unit,
        CASE
            WHEN standardized_product_name ILIKE '%yumurta%'
             AND (
                 SUBSTRING(LOWER(standardized_product_name) FROM '([1-9][0-9]?)\s*[''’]') IS NOT NULL
                 OR SUBSTRING(LOWER(standardized_product_name) FROM '([1-9][0-9]?)\s*adet') IS NOT NULL
             ) THEN COALESCE(
                REPLACE(
                    SUBSTRING(
                        LOWER(standardized_product_name)
                        FROM '([1-9][0-9]?)\s*[''’]'
                    ),
                    ',',
                    '.'
                )::numeric,
                REPLACE(
                    SUBSTRING(
                        LOWER(standardized_product_name)
                        FROM '([1-9][0-9]?)\s*adet'
                    ),
                    ',',
                    '.'
                )::numeric
            )
            WHEN SUBSTRING(LOWER(source_product_name) FROM '([0-9]+(?:[.,][0-9]+)?) *kg') IS NOT NULL THEN
                REPLACE(
                    SUBSTRING(LOWER(source_product_name) FROM '([0-9]+(?:[.,][0-9]+)?) *kg'),
                    ',',
                    '.'
                )::numeric
            WHEN SUBSTRING(LOWER(source_product_name) FROM '([0-9]+(?:[.,][0-9]+)?) *g(ram)?') IS NOT NULL THEN
                REPLACE(
                    SUBSTRING(LOWER(source_product_name) FROM '([0-9]+(?:[.,][0-9]+)?) *g(ram)?'),
                    ',',
                    '.'
                )::numeric / 1000
            WHEN SUBSTRING(LOWER(source_product_name) FROM '([0-9]+(?:[.,][0-9]+)?) *ml') IS NOT NULL THEN
                REPLACE(
                    SUBSTRING(LOWER(source_product_name) FROM '([0-9]+(?:[.,][0-9]+)?) *ml'),
                    ',',
                    '.'
                )::numeric / 1000
            WHEN SUBSTRING(LOWER(source_product_name) FROM '([0-9]+(?:[.,][0-9]+)?) *l') IS NOT NULL THEN
                REPLACE(
                    SUBSTRING(LOWER(source_product_name) FROM '([0-9]+(?:[.,][0-9]+)?) *l'),
                    ',',
                    '.'
                )::numeric
            ELSE normalized_quantity
        END AS comparison_quantity,
        ROW_NUMBER() OVER (
            PARTITION BY source_name, source_product_name
            ORDER BY observed_at DESC, price_observation_id DESC
        ) AS source_rn
    FROM price_history
    WHERE standardized_product_name IS NOT NULL
      AND price IS NOT NULL
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
                'çengelköy', 'cengelkoy', 'kestane', 'istiridye', 'istridye', 'shiitake',
                'maden', 'mineral', 'soda', 'gazli', 'aromali',
                'himalaya', 'kaya', 'salamura', 'sarimsakli', 'truf', 'mantarli',
                '4x1', '6x200', '6x330', '12x330', 'kutu', 'zero', 'sekersiz',
                'light', 'diet', 'sugar', 'free', 'lime', 'lemon', 'vanilla', 'cherry'
            )
              AND NOT (
                  token = ANY(
                      REGEXP_SPLIT_TO_ARRAY(COALESCE(canonical_search_name, ''), E'[[:space:]]+')
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
                      REGEXP_SPLIT_TO_ARRAY(COALESCE(canonical_search_name, ''), E'[[:space:]]+')
                  )
              )
        ) AS extra_token_count
    FROM latest_source_base
    WHERE source_rn = 1
),
latest AS (
    SELECT
        *,
        CASE
            WHEN canonical_search_name = 'ekmek'
             AND standardized_product_name IN ('ekmek', 'ekmek sofra')
            THEN NULL
            ELSE comparison_unit
        END AS grouping_unit,
        CASE
            WHEN canonical_search_name = 'ekmek'
             AND standardized_product_name IN ('ekmek', 'ekmek sofra')
            THEN NULL
            WHEN canonical_search_name = 'mantar'
             AND comparison_unit = 'kg'
            THEN NULL
            ELSE comparison_quantity
        END AS grouping_quantity,
        CASE
            WHEN comparison_unit = 'kg'
             AND comparison_quantity IS NOT NULL
             AND comparison_quantity > 0
            THEN ROUND(price / comparison_quantity, 4)
            ELSE price
        END AS comparison_price
    FROM latest_source
),
latest_group_keys AS (
    SELECT
        canonical_search_name,
        grouping_unit,
        grouping_quantity,
        CASE
            WHEN canonical_search_name IN ('sogan', 'sogan taze', 'arpacik sogan')
            THEN canonical_search_name
            WHEN grouping_unit = 'roll'
             AND grouping_quantity IS NOT NULL
             AND canonical_search_name LIKE '%tuvalet kagidi'
            THEN canonical_search_name || ' ' || grouping_quantity::int::text || ' roll'
            WHEN grouping_unit = 'roll'
             AND grouping_quantity IS NOT NULL
             AND canonical_search_name LIKE '%kagit havlu'
            THEN canonical_search_name || ' ' || grouping_quantity::int::text || ' roll'
            WHEN canonical_search_name IN (
                'su',
                'tuz',
                'kola pepsi',
                'kola coca-cola',
                'yumurta',
                'bildircin yumurta',
                'aycicek yagi',
                'zeytinyagi',
                'misir yagi',
                'findik yagi'
            )
             AND grouping_unit IS NOT NULL
             AND grouping_quantity IS NOT NULL
            THEN
                canonical_search_name || ' ' ||
                CASE
                    WHEN grouping_unit = 'kg'
                    THEN REGEXP_REPLACE((grouping_quantity * 1000)::text, '\.?0+$', '') || ' g'
                    WHEN grouping_unit = 'liter'
                    THEN REGEXP_REPLACE(grouping_quantity::text, '\.?0+$', '') || ' l'
                    WHEN grouping_unit = 'piece'
                    THEN grouping_quantity::int::text || ' adet'
                    ELSE REGEXP_REPLACE(grouping_quantity::text, '\.?0+$', '') || ' ' || grouping_unit
                END
            WHEN COUNT(DISTINCT standardized_product_name) > 1 THEN canonical_search_name
            ELSE MAX(standardized_product_name)
        END AS display_product_name
    FROM latest
    GROUP BY canonical_search_name, grouping_unit, grouping_quantity
),
grouped_latest AS (
    SELECT
        standardized_product_name,
        source_name,
        source_product_name,
        normalized_unit,
        normalized_quantity,
        raw_price,
        comparison_price
    FROM (
        SELECT
            lgk.display_product_name AS standardized_product_name,
            l.source_name,
            l.source_product_name,
            l.comparison_unit AS normalized_unit,
            l.comparison_quantity AS normalized_quantity,
            l.price AS raw_price,
            l.comparison_price,
            l.source_base_name,
            l.source_token_count,
            l.variant_penalty,
            l.extra_token_count,
            l.observed_at,
            l.price_observation_id,
            ROW_NUMBER() OVER (
                PARTITION BY lgk.display_product_name, l.source_name
                ORDER BY
                    CASE
                        WHEN l.source_base_name = lgk.display_product_name THEN 0
                        ELSE 1
                    END,
                    l.variant_penalty,
                    l.extra_token_count,
                    l.source_token_count,
                    l.observed_at DESC,
                    l.price_observation_id DESC
            ) AS group_rn
        FROM latest l
        JOIN latest_group_keys lgk
            ON l.canonical_search_name = lgk.canonical_search_name
           AND l.grouping_unit IS NOT DISTINCT FROM lgk.grouping_unit
           AND l.grouping_quantity IS NOT DISTINCT FROM lgk.grouping_quantity
    ) ranked_grouped
    WHERE group_rn = 1
),
coverage AS (
    SELECT
        standardized_product_name,
        COUNT(DISTINCT source_name) AS source_count,
        STRING_AGG(DISTINCT source_name, ', ' ORDER BY source_name) AS available_retailers,
        MAX(CASE WHEN source_name = 'a101' THEN source_product_name END) AS a101_source_product_name,
        MAX(CASE WHEN source_name = 'migros' THEN source_product_name END) AS migros_source_product_name,
        MAX(CASE WHEN source_name = 'a101' THEN normalized_unit END) AS a101_normalized_unit,
        MAX(CASE WHEN source_name = 'migros' THEN normalized_unit END) AS migros_normalized_unit,
        MAX(CASE WHEN source_name = 'a101' THEN normalized_quantity END) AS a101_normalized_quantity,
        MAX(CASE WHEN source_name = 'migros' THEN normalized_quantity END) AS migros_normalized_quantity,
        MAX(CASE WHEN source_name = 'a101' THEN raw_price END) AS a101_raw_price,
        MAX(CASE WHEN source_name = 'migros' THEN raw_price END) AS migros_raw_price,
        MAX(CASE WHEN source_name = 'a101' THEN comparison_price END) AS a101_comparison_price,
        MAX(CASE WHEN source_name = 'migros' THEN comparison_price END) AS migros_comparison_price
    FROM grouped_latest
    GROUP BY standardized_product_name
),
safety AS (
    SELECT
        standardized_product_name,
        (
            COUNT(DISTINCT source_name) = 2
            AND MAX(CASE WHEN source_name = 'a101' THEN normalized_unit END) IS NOT NULL
            AND MAX(CASE WHEN source_name = 'migros' THEN normalized_unit END) IS NOT NULL
            AND MAX(CASE WHEN source_name = 'a101' THEN normalized_unit END)
                = MAX(CASE WHEN source_name = 'migros' THEN normalized_unit END)
        ) AS same_unit_flag,
        (
            COUNT(DISTINCT source_name) = 2
            AND MAX(CASE WHEN source_name = 'a101' THEN normalized_quantity END) IS NOT NULL
            AND MAX(CASE WHEN source_name = 'migros' THEN normalized_quantity END) IS NOT NULL
            AND (
                MAX(CASE WHEN source_name = 'a101' THEN normalized_quantity END)
                    = MAX(CASE WHEN source_name = 'migros' THEN normalized_quantity END)
                OR (
                    standardized_product_name = 'mantar'
                    AND MAX(CASE WHEN source_name = 'a101' THEN normalized_unit END) = 'kg'
                    AND MAX(CASE WHEN source_name = 'migros' THEN normalized_unit END) = 'kg'
                )
            )
        ) AS same_quantity_flag
    FROM grouped_latest
    GROUP BY standardized_product_name
),
toilet_paper_quality AS (
    SELECT
        c.*,
        (
            TRANSLATE(LOWER(c.a101_source_product_name), 'ıçğöşüİÇĞÖŞÜ', 'icgosuicgosu') LIKE '%tuvalet%'
            AND TRANSLATE(LOWER(c.a101_source_product_name), 'ıçğöşüİÇĞÖŞÜ', 'icgosuicgosu') LIKE '%kagid%'
            AND TRANSLATE(LOWER(c.migros_source_product_name), 'ıçğöşüİÇĞÖŞÜ', 'icgosuicgosu') LIKE '%tuvalet%'
            AND TRANSLATE(LOWER(c.migros_source_product_name), 'ıçğöşüİÇĞÖŞÜ', 'icgosuicgosu') LIKE '%kagid%'
        ) AS is_toilet_paper_pair,
        SPLIT_PART(TRANSLATE(LOWER(c.a101_source_product_name), 'ıçğöşüİÇĞÖŞÜ', 'icgosuicgosu'), ' ', 1) AS a101_brand_token,
        SPLIT_PART(TRANSLATE(LOWER(c.migros_source_product_name), 'ıçğöşüİÇĞÖŞÜ', 'icgosuicgosu'), ' ', 1) AS migros_brand_token,
        CASE
            WHEN TRANSLATE(LOWER(c.a101_source_product_name), 'ıçğöşüİÇĞÖŞÜ', 'icgosuicgosu') LIKE '%platinum%' THEN 'platinum'
            WHEN TRANSLATE(LOWER(c.a101_source_product_name), 'ıçğöşüİÇĞÖŞÜ', 'icgosuicgosu') LIKE '%egzotik%' THEN 'egzotik'
            WHEN TRANSLATE(LOWER(c.a101_source_product_name), 'ıçğöşüİÇĞÖŞÜ', 'icgosuicgosu') LIKE '%bambu%' OR TRANSLATE(LOWER(c.a101_source_product_name), 'ıçğöşüİÇĞÖŞÜ', 'icgosuicgosu') LIKE '%bamboo%' THEN 'bambu'
            WHEN TRANSLATE(LOWER(c.a101_source_product_name), 'ıçğöşüİÇĞÖŞÜ', 'icgosuicgosu') LIKE '%deluxe%' THEN 'deluxe'
            WHEN TRANSLATE(LOWER(c.a101_source_product_name), 'ıçğöşüİÇĞÖŞÜ', 'icgosuicgosu') LIKE '%natural%' THEN 'natural'
            WHEN TRANSLATE(LOWER(c.a101_source_product_name), 'ıçğöşüİÇĞÖŞÜ', 'icgosuicgosu') LIKE '%soft%' THEN 'soft'
            ELSE NULL
        END AS a101_product_line,
        CASE
            WHEN TRANSLATE(LOWER(c.migros_source_product_name), 'ıçğöşüİÇĞÖŞÜ', 'icgosuicgosu') LIKE '%platinum%' THEN 'platinum'
            WHEN TRANSLATE(LOWER(c.migros_source_product_name), 'ıçğöşüİÇĞÖŞÜ', 'icgosuicgosu') LIKE '%egzotik%' THEN 'egzotik'
            WHEN TRANSLATE(LOWER(c.migros_source_product_name), 'ıçğöşüİÇĞÖŞÜ', 'icgosuicgosu') LIKE '%bambu%' OR TRANSLATE(LOWER(c.migros_source_product_name), 'ıçğöşüİÇĞÖŞÜ', 'icgosuicgosu') LIKE '%bamboo%' THEN 'bambu'
            WHEN TRANSLATE(LOWER(c.migros_source_product_name), 'ıçğöşüİÇĞÖŞÜ', 'icgosuicgosu') LIKE '%deluxe%' THEN 'deluxe'
            WHEN TRANSLATE(LOWER(c.migros_source_product_name), 'ıçğöşüİÇĞÖŞÜ', 'icgosuicgosu') LIKE '%natural%' THEN 'natural'
            WHEN TRANSLATE(LOWER(c.migros_source_product_name), 'ıçğöşüİÇĞÖŞÜ', 'icgosuicgosu') LIKE '%soft%' THEN 'soft'
            ELSE NULL
        END AS migros_product_line
    FROM coverage c
)
SELECT
    tq.standardized_product_name,
    tq.source_count,
    tq.available_retailers,
    tq.a101_source_product_name,
    tq.migros_source_product_name,
    tq.a101_normalized_unit,
    tq.migros_normalized_unit,
    tq.a101_normalized_quantity,
    tq.migros_normalized_quantity,
    tq.a101_raw_price,
    tq.migros_raw_price,
    tq.a101_comparison_price,
    tq.migros_comparison_price,
    CASE
        WHEN tq.source_count = 2
         AND s.same_unit_flag
         AND s.same_quantity_flag
        THEN tq.a101_normalized_unit
        WHEN tq.source_count = 2
         AND tq.a101_normalized_unit = tq.migros_normalized_unit
         AND tq.a101_normalized_quantity IS NOT NULL
         AND tq.migros_normalized_quantity IS NOT NULL
         AND tq.a101_normalized_quantity <> tq.migros_normalized_quantity
        THEN tq.a101_normalized_unit
        ELSE NULL
    END AS comparison_price_unit,
    s.same_unit_flag,
    s.same_quantity_flag,
    CASE
        WHEN tq.source_count < 2 THEN 'single_source'
        WHEN tq.is_toilet_paper_pair
         AND s.same_unit_flag
         AND s.same_quantity_flag
         AND tq.a101_brand_token = tq.migros_brand_token
         AND tq.a101_product_line IS NOT NULL
         AND tq.a101_product_line = tq.migros_product_line
        THEN 'high'
        WHEN tq.is_toilet_paper_pair
         AND s.same_unit_flag
         AND s.same_quantity_flag
         AND tq.a101_brand_token = tq.migros_brand_token
         AND tq.a101_product_line IS NULL
         AND tq.migros_product_line IS NULL
        THEN 'medium'
        WHEN s.same_unit_flag AND s.same_quantity_flag THEN 'high'
        WHEN s.same_unit_flag THEN 'medium'
        ELSE 'low'
    END AS comparison_confidence,
    CASE
        WHEN tq.source_count = 2
         AND tq.is_toilet_paper_pair
         AND s.same_unit_flag
         AND s.same_quantity_flag
         AND tq.a101_brand_token = tq.migros_brand_token
         AND tq.a101_product_line IS NOT NULL
         AND tq.a101_product_line = tq.migros_product_line
        THEN 'comparable'
        WHEN tq.source_count = 2
         AND s.same_unit_flag
         AND s.same_quantity_flag
         AND COALESCE(tq.is_toilet_paper_pair, FALSE) = FALSE
        THEN 'comparable'
        WHEN tq.source_count = 2 THEN 'comparison_review_required'
        WHEN tq.source_count = 1 AND tq.available_retailers = 'a101' THEN 'only_a101'
        WHEN tq.source_count = 1 AND tq.available_retailers = 'migros' THEN 'only_migros'
        ELSE 'unavailable'
    END AS coverage_status
FROM toilet_paper_quality tq
LEFT JOIN safety s
    ON tq.standardized_product_name = s.standardized_product_name
ORDER BY standardized_product_name;
"""
