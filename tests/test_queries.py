from app.queries import GLOBAL_FRESHNESS_QUERY


def test_global_freshness_query_exposes_scrape_and_price_observation_timestamps():
    lowered_query = GLOBAL_FRESHNESS_QUERY.lower()

    assert "latest_success_started_at" in lowered_query
    assert "latest_price_observed_at" in lowered_query
    assert "latest_data_date" in lowered_query
