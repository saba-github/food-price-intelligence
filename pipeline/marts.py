def refresh_materialized_views(cursor):
    views = [
        "mart_daily_prices",
        "mart_top_movers",
        "mart_price_anomalies",
        "mart_pipeline_health",
        "mart_latest_prices",
        "mart_daily_prices_by_retailer",
    ]

    for view_name in views:
        cursor.execute(
            """
            SELECT 1
            FROM pg_matviews
            WHERE schemaname = 'public'
              AND matviewname = %s
            """,
            (view_name,),
        )
        exists = cursor.fetchone()

        if exists:
            cursor.execute(f"REFRESH MATERIALIZED VIEW {view_name}")
