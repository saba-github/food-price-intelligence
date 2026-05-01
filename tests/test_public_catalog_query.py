from app.queries import PUBLIC_PRODUCT_CATALOG_QUERY


def test_public_catalog_query_uses_latest_grouped_row_instead_of_max_price():
    assert "ROW_NUMBER() OVER (" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "PARTITION BY lgk.display_product_name, l.source_name" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "l.observed_at DESC" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "l.price_observation_id DESC" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "WHERE group_rn = 1" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "MAX(l.price) AS raw_price" not in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "MAX(l.comparison_price) AS comparison_price" not in PUBLIC_PRODUCT_CATALOG_QUERY


def test_public_catalog_query_canonicalizes_hiyar_with_salatalik():
    assert "standardized_product_name IN ('salatalik', 'hiyar')" in PUBLIC_PRODUCT_CATALOG_QUERY


def test_public_catalog_query_groups_base_ekmek_with_sofra_ekmek():
    assert "standardized_product_name IN ('ekmek', 'ekmek sofra')" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "WHEN canonical_search_name = 'ekmek'" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "THEN NULL" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "grouping_unit" in PUBLIC_PRODUCT_CATALOG_QUERY


def test_public_catalog_query_groups_tava_ekmek_variants():
    assert "standardized_product_name ILIKE '%uno%'" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "standardized_product_name ILIKE '%bugday%'" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "standardized_product_name ILIKE '%tava%'" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "standardized_product_name ILIKE '%ekmeg%'" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "normalized_quantity = 0.45" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "THEN 'tava ekmek'" in PUBLIC_PRODUCT_CATALOG_QUERY


def test_public_catalog_query_canonicalizes_water_salt_and_cola_groups():
    assert "THEN 'su'" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "THEN 'tuz'" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "THEN 'kola pepsi'" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "THEN 'kola coca-cola'" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "THEN 'aycicek yagi'" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "THEN 'zeytinyagi'" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "THEN 'misir yagi'" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "THEN 'findik yagi'" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "tuvalet kagidi" in PUBLIC_PRODUCT_CATALOG_QUERY


def test_public_catalog_query_contains_toilet_paper_product_line_quality_rules():
    assert "toilet_paper_quality AS" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "platinum" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "egzotik" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "bambu" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "deluxe" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "natural" in PUBLIC_PRODUCT_CATALOG_QUERY


def test_public_catalog_query_groups_paper_towel_into_roll_based_brand_rows():
    assert "kagit havlu" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "canonical_search_name LIKE '%kagit havlu'" in PUBLIC_PRODUCT_CATALOG_QUERY


def test_public_catalog_query_groups_eggs_by_count_and_onions_by_base_family():
    assert "THEN 'yumurta'" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "THEN 'bildircin yumurta'" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "THEN 'sogan taze'" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "THEN 'arpacik sogan'" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "THEN 'piece'" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "|| ' adet'" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "canonical_search_name IN ('sogan', 'sogan taze', 'arpacik sogan')" in PUBLIC_PRODUCT_CATALOG_QUERY


def test_public_catalog_query_excludes_light_zero_and_flavored_cola_variants_from_generic_groups():
    assert "standardized_product_name NOT ILIKE '%light%'" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "standardized_product_name NOT ILIKE '%diet%'" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "standardized_product_name NOT ILIKE '%sugar%'" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "standardized_product_name NOT ILIKE '%free%'" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "standardized_product_name NOT ILIKE '%vanilla%'" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "standardized_product_name NOT ILIKE '%cherry%'" in PUBLIC_PRODUCT_CATALOG_QUERY


def test_public_catalog_query_extracts_liter_and_ml_comparison_units():
    assert "*ml') IS NOT NULL THEN 'liter'" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "*l') IS NOT NULL THEN 'liter'" in PUBLIC_PRODUCT_CATALOG_QUERY


def test_public_catalog_query_builds_size_specific_display_names_for_staples():
    assert "canonical_search_name IN (" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "'su'" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "'tuz'" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "'kola pepsi'" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "'kola coca-cola'" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "'aycicek yagi'" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "'zeytinyagi'" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "|| ' g'" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "|| ' l'" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "|| ' roll'" in PUBLIC_PRODUCT_CATALOG_QUERY


def test_public_catalog_query_exposes_comparison_unit_for_same_quantity_kg_products():
    assert "WHEN tq.source_count = 2" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "AND s.same_unit_flag" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "AND s.same_quantity_flag" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "THEN tq.a101_normalized_unit" in PUBLIC_PRODUCT_CATALOG_QUERY


def test_public_catalog_query_ranks_base_product_ahead_of_variants_within_group():
    assert "PARTITION BY source_name, source_product_name" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "WHEN l.source_base_name = lgk.display_product_name THEN 0" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "l.variant_penalty" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "l.extra_token_count" in PUBLIC_PRODUCT_CATALOG_QUERY


def test_public_catalog_query_penalizes_light_zero_and_flavored_cola_tokens_within_group():
    assert "'light'" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "'diet'" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "'sugar'" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "'free'" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "'vanilla'" in PUBLIC_PRODUCT_CATALOG_QUERY
    assert "'cherry'" in PUBLIC_PRODUCT_CATALOG_QUERY
