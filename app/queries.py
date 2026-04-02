
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

PRICE_INTELLIGENCE_BASE_QUERY = """
select
    date,
    standardized_product_name,
    category_name,
    normalized_unit,
    avg_price,
    min_price,
    max_price,
    observation_count
from mart_daily_prices
where date = %(selected_date)s
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
    avg_price,
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

PIPELINE_RUNS_QUERY = """
select
    run_id,
    source_name,
    started_at,
    finished_at,
    status,
    records_scraped,
    error_message
from scrape_runs
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
