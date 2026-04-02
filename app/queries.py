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
    total_checks,
    passed_checks,
    failed_checks,
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
