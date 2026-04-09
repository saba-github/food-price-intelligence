LATEST_DATES_QUERY = """
select distinct date
from mart_daily_prices
order by date desc
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
