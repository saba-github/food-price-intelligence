import inspect

from pipeline.optimizer import pricing


def test_latest_price_history_query_uses_latest_grouped_row_instead_of_max_price():
    source = inspect.getsource(pricing.get_latest_price_history_prices)

    assert "ROW_NUMBER() OVER (" in source
    assert "PARTITION BY lgk.display_product_name, l.source_name" in source
    assert "l.observed_at DESC" in source
    assert "l.price_observation_id DESC" in source
    assert "WHERE group_rn = 1" in source
    assert "MAX(l.price) AS price" not in source


def test_latest_price_history_query_canonicalizes_hiyar_with_salatalik():
    source = inspect.getsource(pricing.get_latest_price_history_prices)

    assert "standardized_product_name IN ('salatalik', 'hiyar')" in source


def test_latest_price_history_query_groups_base_ekmek_with_sofra_ekmek():
    source = inspect.getsource(pricing.get_latest_price_history_prices)

    assert "standardized_product_name IN ('ekmek', 'ekmek sofra')" in source
    assert "WHEN canonical_search_name = 'ekmek'" in source
    assert "grouping_unit" in source


def test_latest_price_history_query_groups_tava_ekmek_variants():
    source = inspect.getsource(pricing.get_latest_price_history_prices)

    assert "standardized_product_name ILIKE '%%uno%%'" in source
    assert "standardized_product_name ILIKE '%%bugday%%'" in source
    assert "standardized_product_name ILIKE '%%tava%%'" in source
    assert "standardized_product_name ILIKE '%%ekmeg%%'" in source
    assert "normalized_quantity = 0.45" in source
    assert "THEN 'tava ekmek'" in source


def test_latest_price_history_query_canonicalizes_water_salt_and_cola_groups():
    source = inspect.getsource(pricing.get_latest_price_history_prices)

    assert "THEN 'su'" in source
    assert "standardized_product_name ILIKE '%%camasir%%'" in source
    assert "standardized_product_name ILIKE '%%suyu%%'" in source
    assert "THEN 'tuz'" in source
    assert "THEN 'kola pepsi'" in source
    assert "THEN 'kola coca-cola'" in source
    assert "THEN 'aycicek yagi'" in source
    assert "THEN 'zeytinyagi'" in source
    assert "THEN 'misir yagi'" in source
    assert "THEN 'findik yagi'" in source
    assert "tuvalet kagidi" in source
    assert "kagit havlu" in source


def test_latest_price_history_query_groups_eggs_by_count_and_onions_by_base_family():
    source = inspect.getsource(pricing.get_latest_price_history_prices)

    assert "THEN 'yumurta'" in source
    assert "THEN 'bildircin yumurta'" in source
    assert "THEN 'sogan taze'" in source
    assert "THEN 'arpacik sogan'" in source
    assert "THEN 'piece'" in source
    assert "|| ' adet'" in source
    assert "canonical_search_name IN ('sogan', 'sogan taze', 'arpacik sogan')" in source


def test_latest_price_history_query_excludes_light_zero_and_flavored_cola_variants_from_generic_groups():
    source = inspect.getsource(pricing.get_latest_price_history_prices)

    assert "standardized_product_name NOT ILIKE '%%light%%'" in source
    assert "standardized_product_name NOT ILIKE '%%diet%%'" in source
    assert "standardized_product_name NOT ILIKE '%%sugar%%'" in source
    assert "standardized_product_name NOT ILIKE '%%free%%'" in source
    assert "standardized_product_name NOT ILIKE '%%vanilla%%'" in source
    assert "standardized_product_name NOT ILIKE '%%cherry%%'" in source


def test_latest_price_history_query_extracts_liter_and_ml_comparison_units():
    source = inspect.getsource(pricing.get_latest_price_history_prices)

    assert "*ml') IS NOT NULL THEN 'liter'" in source
    assert "*l') IS NOT NULL THEN 'liter'" in source


def test_latest_price_history_query_builds_size_specific_display_names_for_staples():
    source = inspect.getsource(pricing.get_latest_price_history_prices)

    assert "canonical_search_name IN (" in source
    assert "'su'" in source
    assert "'tuz'" in source
    assert "'kola pepsi'" in source
    assert "'kola coca-cola'" in source
    assert "'aycicek yagi'" in source
    assert "'zeytinyagi'" in source
    assert "|| ' g'" in source
    assert "|| ' l'" in source
    assert "|| ' roll'" in source


def test_latest_price_history_query_ranks_base_product_ahead_of_variants_within_group():
    source = inspect.getsource(pricing.get_latest_price_history_prices)

    assert "PARTITION BY source_name, source_product_name" in source
    assert "WHEN l.source_base_name = lgk.display_product_name THEN 0" in source
    assert "l.variant_penalty" in source
    assert "l.extra_token_count" in source


def test_latest_price_history_query_penalizes_light_zero_and_flavored_cola_tokens_within_group():
    source = inspect.getsource(pricing.get_latest_price_history_prices)

    assert "'light'" in source
    assert "'diet'" in source
    assert "'sugar'" in source
    assert "'free'" in source
    assert "'vanilla'" in source
    assert "'cherry'" in source
