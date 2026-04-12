def refresh_materialized_views(cursor):
    cursor.execute("REFRESH MATERIALIZED VIEW mart_daily_prices")
    cursor.execute("REFRESH MATERIALIZED VIEW mart_top_movers")
    cursor.execute("REFRESH MATERIALIZED VIEW mart_price_anomalies")
    cursor.execute("REFRESH MATERIALIZED VIEW mart_pipeline_health")
    cursor.execute("REFRESH MATERIALIZED VIEW mart_latest_prices")
    cursor.execute("REFRESH MATERIALIZED VIEW mart_daily_prices_by_retailer")
