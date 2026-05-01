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
        # check existence
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

        if not exists:
            continue

        savepoint_name = f"refresh_{view_name}"
        cursor.execute(f"SAVEPOINT {savepoint_name}")

        try:
            cursor.execute(f"REFRESH MATERIALIZED VIEW {view_name}")
        except Exception as e:
            cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
            # do NOT crash pipeline
            print(f"[WARN] Failed to refresh {view_name}: {e}")
        finally:
            cursor.execute(f"RELEASE SAVEPOINT {savepoint_name}")
