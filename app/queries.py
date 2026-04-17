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
)
select
    ld.latest_data_date,
    lsr.latest_success_started_at
from latest_data ld
cross join latest_success_run lsr
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
SELECT DISTINCT standardized_product_name
FROM mart_daily_prices_by_retailer
WHERE standardized_product_name IS NOT NULL
ORDER BY standardized_product_name;
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
WITH latest_date AS (
    SELECT MAX(date) AS max_date
    FROM mart_daily_prices_by_retailer
),
ranked AS (
    SELECT
        m.date,
        m.standardized_product_name,
        m.source_name,
        m.avg_price,
        RANK() OVER (
            PARTITION BY m.date, m.standardized_product_name
            ORDER BY m.avg_price ASC
        ) AS price_rank
    FROM mart_daily_prices_by_retailer m
    JOIN latest_date ld
      ON m.date = ld.max_date
)
SELECT
    date,
    standardized_product_name,
    source_name,
    avg_price
FROM ranked
WHERE price_rank = 1
ORDER BY standardized_product_name;
"""
