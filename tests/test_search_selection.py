import pandas as pd
import pytest

from app.search_selection import (
    build_brand_filter_options,
    build_category_row_price_model,
    build_group_result_rows,
    CLEANING_SEARCH_MODE_BRAND,
    CLEANING_SEARCH_MODE_CATEGORY,
    CLEANING_SEARCH_MODE_SPECIFIC,
    build_category_product_results,
    build_category_result_sections,
    build_unified_category_results,
    build_cleaning_selection_button_key,
    build_cleaning_family_product_groups,
    build_cleaning_sibling_product_groups,
    build_brand_only_cleaning_groups,
    combine_selection_groups,
    detect_search_mode,
    filter_category_results,
    format_compact_category_display_name,
    has_hidden_brand_result_rows,
    has_hidden_cleaning_family_product_groups,
    is_brand_only_cleaning_query,
    limit_brand_result_rows,
    limit_cleaning_family_product_groups,
    normalize_result_status,
    preserve_or_reset_cleaning_product_selection,
    resolve_category_filter_selection,
    resolve_category_result_selection,
    resolved_cleaning_brand_token,
    split_category_rows_by_status,
    sort_brand_result_rows,
    sort_specific_result_rows,
    select_brand_only_cleaning_default_group,
)
from pipeline.optimizer.product_search import build_search_group_sections


def test_brand_only_cleaning_query_detection():
    assert is_brand_only_cleaning_query("fairy") is True
    assert is_brand_only_cleaning_query("pril") is True
    assert is_brand_only_cleaning_query("finish") is True
    assert is_brand_only_cleaning_query("fairy 1500") is False
    assert is_brand_only_cleaning_query("fairy elma") is False
    assert is_brand_only_cleaning_query("fairy tablet") is False


def test_detect_search_mode_distinguishes_brand_category_and_specific_queries():
    assert detect_search_mode("fairy") == CLEANING_SEARCH_MODE_BRAND
    assert detect_search_mode("bulaşık deterjanı") == CLEANING_SEARCH_MODE_CATEGORY
    assert detect_search_mode("bulasik tableti") == CLEANING_SEARCH_MODE_CATEGORY
    assert detect_search_mode("bulaşık makinesi tableti") == CLEANING_SEARCH_MODE_CATEGORY
    assert detect_search_mode("yüzey temizleyici") == CLEANING_SEARCH_MODE_CATEGORY
    assert detect_search_mode("çamaşır suyu") == CLEANING_SEARCH_MODE_CATEGORY
    assert detect_search_mode("fairy 650") == CLEANING_SEARCH_MODE_SPECIFIC
    assert detect_search_mode("fairy elma") == CLEANING_SEARCH_MODE_SPECIFIC
    assert detect_search_mode("finish tuz") == CLEANING_SEARCH_MODE_SPECIFIC
    assert detect_search_mode("su") == CLEANING_SEARCH_MODE_SPECIFIC
    assert detect_search_mode("fairy tablet") == CLEANING_SEARCH_MODE_SPECIFIC
    assert detect_search_mode("fairy elma 1500") == CLEANING_SEARCH_MODE_SPECIFIC


def test_build_category_product_results_sorts_by_best_unit_price_and_marks_cheapest():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "fairy bulasik deterjani 0.65 l",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "comparison_status_label": "Güvenli karşılaştırma",
                "a101_source_product_name": "Fairy Sıvı Bulaşık Deterjanı 650 ml",
                "migros_source_product_name": "Fairy Sıvı Bulaşık Deterjanı Limon 650 Ml",
                "a101_raw_price": 99.5,
                "migros_raw_price": 99.95,
                "a101_normalized_unit": "liter",
                "migros_normalized_unit": "liter",
                "a101_normalized_quantity": 0.65,
                "migros_normalized_quantity": 0.65,
                "a101_comparison_price": None,
                "migros_comparison_price": None,
                "comparison_price_unit": None,
            },
            {
                "standardized_product_name": "pril bulasik deterjani 0.653 l",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "comparison_status_label": "Güvenli karşılaştırma",
                "a101_source_product_name": "Pril Bulaşık Deterjanı 653 ml",
                "migros_source_product_name": "Pril Power 5 Etki Limon 653 Ml",
                "a101_raw_price": 89.9,
                "migros_raw_price": 64.95,
                "a101_normalized_unit": "liter",
                "migros_normalized_unit": "liter",
                "a101_normalized_quantity": 0.653,
                "migros_normalized_quantity": 0.653,
                "a101_comparison_price": None,
                "migros_comparison_price": None,
                "comparison_price_unit": None,
            },
            {
                "standardized_product_name": "bingo bulasik deterjani 1.5 l",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "comparison_status_label": "Güvenli karşılaştırma",
                "a101_source_product_name": "Bingo Bulaşık Deterjanı 1.5 L",
                "migros_source_product_name": "Bingo Bulaşık Deterjanı 1.5 L",
                "a101_raw_price": 169.0,
                "migros_raw_price": 109.95,
                "a101_normalized_unit": "liter",
                "migros_normalized_unit": "liter",
                "a101_normalized_quantity": 1.5,
                "migros_normalized_quantity": 1.5,
                "a101_comparison_price": None,
                "migros_comparison_price": None,
                "comparison_price_unit": None,
            },
        ]
    )

    category_results = build_category_product_results(catalog_df, "bulaşık deterjanı")

    assert [result["group"]["product_names"][0] for result in category_results[:3]] == [
        "bingo bulasik deterjani 1.5 l",
        "pril bulasik deterjani 0.653 l",
        "fairy bulasik deterjani 0.65 l",
    ]
    assert category_results[0]["is_cheapest"] is True
    assert category_results[0]["best_retailer"] == "migros"


def test_build_category_product_results_gives_small_comparable_boost_when_prices_are_close():
    catalog_df = pd.DataFrame(
        [
                {
                    "standardized_product_name": "single market bulasik deterjani 1 l",
                "source_count": 1,
                "available_retailers": "a101",
                "coverage_status": "only_a101",
                "comparison_status_label": "Tek markette",
                    "a101_source_product_name": "Single Market Bulaşık Deterjanı 1 L",
                "migros_source_product_name": None,
                "a101_raw_price": 100.0,
                "migros_raw_price": None,
                "a101_normalized_unit": "liter",
                "migros_normalized_unit": None,
                "a101_normalized_quantity": 1.0,
                "migros_normalized_quantity": None,
                "a101_comparison_price": None,
                "migros_comparison_price": None,
                "comparison_price_unit": None,
            },
                {
                    "standardized_product_name": "comparable bulasik deterjani 1 l",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "comparison_status_label": "Karşılaştırılabilir",
                    "a101_source_product_name": "Comparable Bulaşık Deterjanı 1 L",
                    "migros_source_product_name": "Comparable Bulaşık Deterjanı 1 L",
                "a101_raw_price": 100.8,
                "migros_raw_price": 100.8,
                "a101_normalized_unit": "liter",
                "migros_normalized_unit": "liter",
                "a101_normalized_quantity": 1.0,
                "migros_normalized_quantity": 1.0,
                "a101_comparison_price": None,
                "migros_comparison_price": None,
                "comparison_price_unit": None,
            },
        ]
        )

    category_results = build_category_product_results(catalog_df, "bulaşık deterjanı")

    assert [result["group"]["product_names"][0] for result in category_results[:2]] == [
        "comparable bulasik deterjani 1 l",
        "single market bulasik deterjani 1 l",
    ]


def test_build_category_result_sections_excludes_kg_rows_from_primary_liter_list():
    category_results = [
        {
            "group": {"selection_id": "product:bingo bulasik deterjani 1.5 l", "product_names": ["bingo bulasik deterjani 1.5 l"]},
            "display_name": "Bingo Bulaşık Deterjanı 1.5 L",
            "coverage_status": "comparable",
            "best_retailer": "migros",
            "best_price": 109.95,
            "best_unit_price": 73.3,
            "best_unit_label": "litre",
            "brand_token": "bingo",
            "subtype": "dishwashing_liquid",
            "variant_token": None,
        },
        {
            "group": {"selection_id": "product:fairy bulasik deterjani 0.65 l", "product_names": ["fairy bulasik deterjani 0.65 l"]},
            "display_name": "Fairy Sıvı Bulaşık Deterjanı 650 ml",
            "coverage_status": "comparable",
            "best_retailer": "a101",
            "best_price": 99.5,
            "best_unit_price": 153.08,
            "best_unit_label": "litre",
            "brand_token": "fairy",
            "subtype": "dishwashing_liquid",
            "variant_token": None,
        },
        {
            "group": {"selection_id": "product:sio bulasik deterjani 4 kg", "product_names": ["sio bulasik deterjani 4 kg"]},
            "display_name": "Sio Bulaşık Deterjanı 4 Kg",
            "coverage_status": "only_a101",
            "best_retailer": "a101",
            "best_price": 72.5,
            "best_unit_price": 18.12,
            "best_unit_label": "kg",
            "brand_token": "sio",
            "subtype": "dishwashing_liquid",
            "variant_token": None,
        },
    ]

    sections = build_category_result_sections(category_results, "bulaşık deterjanı")

    assert [result["group"]["selection_id"] for result in sections["primary_results"]] == [
        "product:bingo bulasik deterjani 1.5 l",
        "product:fairy bulasik deterjani 0.65 l",
    ]
    assert [result["group"]["selection_id"] for result in sections["secondary_results"]] == [
        "product:sio bulasik deterjani 4 kg",
    ]


def test_category_result_sections_build_brand_filters_and_hide_empty_ones():
    category_results = [
        {
            "group": {"selection_id": "product:fairy bulasik deterjani 0.65 l", "product_names": ["fairy bulasik deterjani 0.65 l"]},
            "display_name": "Fairy Sıvı Bulaşık Deterjanı 650 ml",
            "coverage_status": "comparable",
            "best_retailer": "a101",
            "best_price": 99.5,
            "best_unit_price": 153.08,
            "best_unit_label": "litre",
            "brand_token": "fairy",
            "subtype": "dishwashing_liquid",
            "variant_token": None,
        },
        {
            "group": {"selection_id": "product:pril bulasik deterjani 0.653 l", "product_names": ["pril bulasik deterjani 0.653 l"]},
            "display_name": "Pril Bulaşık Deterjanı 653 ml",
            "coverage_status": "comparable",
            "best_retailer": "migros",
            "best_price": 64.95,
            "best_unit_price": 99.46,
            "best_unit_label": "litre",
            "brand_token": "pril",
            "subtype": "dishwashing_liquid",
            "variant_token": None,
        },
        {
            "group": {"selection_id": "product:bingo bulasik deterjani 1.5 l", "product_names": ["bingo bulasik deterjani 1.5 l"]},
            "display_name": "Bingo Bulaşık Deterjanı 1.5 L",
            "coverage_status": "comparable",
            "best_retailer": "migros",
            "best_price": 109.95,
            "best_unit_price": 73.3,
            "best_unit_label": "litre",
            "brand_token": "bingo",
            "subtype": "dishwashing_liquid",
            "variant_token": None,
        },
        {
            "group": {"selection_id": "product:30lu tablet", "product_names": ["30lu tablet"]},
            "display_name": "Finish Bulaşık Tableti 30'lu",
            "coverage_status": "only_migros",
            "best_retailer": "migros",
            "best_price": 239.95,
            "best_unit_price": 8.0,
            "best_unit_label": "adet",
            "brand_token": "finish",
            "subtype": "dishwasher_tablet",
            "variant_token": None,
        },
    ]

    sections = build_category_result_sections(category_results, "bulaşık deterjanı")
    labels = [filter_option["label"] for filter_option in sections["filters"]]

    assert labels == ["Tümü", "Fairy", "Pril", "Bingo"]


def test_filter_category_results_marks_first_visible_row_as_cheapest():
    category_results = [
        {
            "group": {"selection_id": "product:bingo bulasik deterjani 1.5 l", "product_names": ["bingo bulasik deterjani 1.5 l"]},
            "display_name": "Bingo Bulaşık Deterjanı 1.5 L",
            "coverage_status": "comparable",
            "best_retailer": "migros",
            "best_price": 109.95,
            "best_unit_price": 73.3,
            "best_unit_label": "litre",
            "brand_token": "bingo",
            "subtype": "dishwashing_liquid",
            "variant_token": None,
        },
        {
            "group": {"selection_id": "product:fairy bulasik deterjani 0.65 l", "product_names": ["fairy bulasik deterjani 0.65 l"]},
            "display_name": "Fairy Sıvı Bulaşık Deterjanı 650 ml",
            "coverage_status": "comparable",
            "best_retailer": "a101",
            "best_price": 99.5,
            "best_unit_price": 153.08,
            "best_unit_label": "litre",
            "brand_token": "fairy",
            "subtype": "dishwashing_liquid",
            "variant_token": None,
        },
    ]

    sections = build_category_result_sections(category_results, "bulaşık deterjanı")
    filtered_results = filter_category_results(sections, "all")

    assert filtered_results[0]["is_visible_cheapest"] is True
    assert filtered_results[1]["is_visible_cheapest"] is False


def test_filter_category_results_supports_brand_chips():
    category_results = [
        {
            "group": {"selection_id": "product:fairy bulasik deterjani 0.65 l", "product_names": ["fairy bulasik deterjani 0.65 l"]},
            "display_name": "Fairy Sıvı Bulaşık Deterjanı 650 ml",
            "coverage_status": "comparable",
            "best_retailer": "a101",
            "best_price": 99.5,
            "best_unit_price": 153.08,
            "best_unit_label": "litre",
            "brand_token": "fairy",
            "subtype": "dishwashing_liquid",
            "variant_token": None,
        },
        {
            "group": {"selection_id": "product:pril bulasik deterjani 0.653 l", "product_names": ["pril bulasik deterjani 0.653 l"]},
            "display_name": "Pril Bulaşık Deterjanı 653 ml",
            "coverage_status": "comparable",
            "best_retailer": "migros",
            "best_price": 64.95,
            "best_unit_price": 99.46,
            "best_unit_label": "litre",
            "brand_token": "pril",
            "subtype": "dishwashing_liquid",
            "variant_token": None,
        },
    ]

    sections = build_category_result_sections(category_results, "bulaşık deterjanı")
    fairy_results = filter_category_results(sections, "fairy")

    assert [result["group"]["selection_id"] for result in fairy_results] == [
        "product:fairy bulasik deterjani 0.65 l",
    ]


def test_bingo_filter_includes_comparable_and_a101_only_bingo_rows():
    category_results = [
        {
            "group": {
                "selection_id": "product:bingo bulasik deterjani 1.5 l",
                "product_names": ["bingo bulasik deterjani 1.5 l"],
            },
            "display_name": "Bingo Mandalina Kokulu Bulaşık Deterjanı 1,5 L",
            "coverage_status": "comparable",
            "best_retailer": "migros",
            "best_price": 109.95,
            "best_unit_price": 73.3,
            "best_unit_label": "litre",
            "brand_token": "bingo",
            "subtype": "dishwashing_liquid",
            "variant_token": None,
        },
        {
            "group": {
                "selection_id": "product:bingo limon bulasik deterjani 0.75 l",
                "product_names": ["bingo limon bulasik deterjani 0.75 l"],
            },
            "display_name": "Bingo Limon Kokulu Bulaşık Deterjanı 750 ml",
            "coverage_status": "only_a101",
            "best_retailer": "a101",
            "best_price": 39.5,
            "best_unit_price": 52.67,
            "best_unit_label": "litre",
            "brand_token": "bingo",
            "subtype": "dishwashing_liquid",
            "variant_token": "limon",
        },
        {
            "group": {
                "selection_id": "product:fairy bulasik deterjani 0.65 l",
                "product_names": ["fairy bulasik deterjani 0.65 l"],
            },
            "display_name": "Fairy Sıvı Bulaşık Deterjanı 650 ml",
            "coverage_status": "comparable",
            "best_retailer": "migros",
            "best_price": 99.5,
            "best_unit_price": 153.08,
            "best_unit_label": "litre",
            "brand_token": "fairy",
            "subtype": "dishwashing_liquid",
            "variant_token": None,
        },
    ]

    sections = build_category_result_sections(category_results, "bulaşık deterjanı")
    bingo_results = filter_category_results(sections, "bingo")
    comparable_rows, single_market_rows, _ = split_category_rows_by_status(bingo_results)

    assert [result["group"]["selection_id"] for result in comparable_rows] == [
        "product:bingo bulasik deterjani 1.5 l",
    ]
    assert [result["group"]["selection_id"] for result in single_market_rows] == [
        "product:bingo limon bulasik deterjani 0.75 l",
    ]


def test_split_category_rows_by_status_separates_comparable_single_market_and_review():
    rows = [
        {"coverage_status": "comparable", "group": {"selection_id": "product:fairy"}},
        {"coverage_status": "only_a101", "group": {"selection_id": "product:bingo"}},
        {
            "coverage_status": "comparison_review_required",
            "group": {"selection_id": "product:kg-row"},
        },
    ]

    comparable_rows, single_market_rows, incompatible_rows = split_category_rows_by_status(rows)

    assert [row["group"]["selection_id"] for row in comparable_rows] == ["product:fairy"]
    assert [row["group"]["selection_id"] for row in single_market_rows] == ["product:bingo"]
    assert [row["group"]["selection_id"] for row in incompatible_rows] == ["product:kg-row"]


def test_build_unified_category_results_keeps_one_ranked_list_with_secondary_rows_at_bottom():
    category_results = [
        {
            "group": {"selection_id": "product:bingo bulasik deterjani 1.5 l", "product_names": ["bingo bulasik deterjani 1.5 l"]},
            "display_name": "Bingo Bulaşık Deterjanı 1.5 L",
            "coverage_status": "comparable",
            "best_retailer": "migros",
            "best_price": 109.95,
            "best_unit_price": 73.3,
            "best_unit_label": "litre",
            "brand_token": "bingo",
            "subtype": "dishwashing_liquid",
            "variant_token": None,
        },
        {
            "group": {"selection_id": "product:fairy bulasik deterjani 0.65 l", "product_names": ["fairy bulasik deterjani 0.65 l"]},
            "display_name": "Fairy Sıvı Bulaşık Deterjanı 650 ml",
            "coverage_status": "only_a101",
            "best_retailer": "a101",
            "best_price": 99.5,
            "best_unit_price": 153.08,
            "best_unit_label": "litre",
            "brand_token": "fairy",
            "subtype": "dishwashing_liquid",
            "variant_token": None,
        },
        {
            "group": {"selection_id": "product:sio bulasik deterjani 4 kg", "product_names": ["sio bulasik deterjani 4 kg"]},
            "display_name": "Sio Bulaşık Deterjanı 4 Kg",
            "coverage_status": "only_a101",
            "best_retailer": "a101",
            "best_price": 72.5,
            "best_unit_price": 18.12,
            "best_unit_label": "kg",
            "brand_token": "sio",
            "subtype": "dishwashing_liquid",
            "variant_token": None,
        },
    ]

    sections = build_category_result_sections(category_results, "bulaşık deterjanı")
    unified_results = build_unified_category_results(sections, "all", limit=None)

    assert [result["group"]["selection_id"] for result in unified_results] == [
        "product:bingo bulasik deterjani 1.5 l",
        "product:fairy bulasik deterjani 0.65 l",
        "product:sio bulasik deterjani 4 kg",
    ]
    assert unified_results[0]["is_visible_cheapest"] is True
    assert unified_results[2]["category_display_section"] == "secondary"


def test_category_row_price_model_prefers_unit_price_and_keeps_shelf_price_secondary():
    litre_result = {
        "best_price": 29.5,
        "best_unit_price": 39.33,
        "best_unit_label": "litre",
    }
    kg_result = {
        "best_price": 72.5,
        "best_unit_price": 18.12,
        "best_unit_label": "kg",
    }
    raw_only_result = {
        "best_price": 24.9,
        "best_unit_price": None,
        "best_unit_label": None,
    }

    litre_model = build_category_row_price_model(litre_result)
    kg_model = build_category_row_price_model(kg_result)
    raw_model = build_category_row_price_model(raw_only_result)

    assert litre_model["primary_kind"] == "unit_price"
    assert litre_model["secondary_kind"] == "shelf_price"
    assert litre_model["primary_unit_label"] == "litre"
    assert kg_model["primary_kind"] == "unit_price"
    assert kg_model["primary_unit_label"] == "kg"
    assert raw_model["primary_kind"] == "shelf_price"
    assert raw_model["secondary_kind"] is None


def test_category_row_price_model_uses_adet_as_primary_for_tablets():
    tablet_result = {
        "best_price": 239.95,
        "best_unit_price": 8.0,
        "best_unit_label": "adet",
    }

    tablet_model = build_category_row_price_model(tablet_result)

    assert tablet_model["primary_kind"] == "unit_price"
    assert tablet_model["primary_unit_label"] == "adet"
    assert tablet_model["secondary_kind"] == "shelf_price"


def test_build_brand_filter_options_never_returns_blank_labels():
    family_groups = [
        {
            "family_id": "cleaning_family:fairy:dishwashing_liquid",
            "family_label": "Fairy Sıvı Bulaşık Deterjanı",
        },
        {
            "family_id": "cleaning_family:fairy:dishwashing_spray",
            "family_label": "",
        },
        {
            "family_id": "cleaning_family:fairy:dishwasher_tablet",
            "family_label": None,
        },
        {
            "family_id": "cleaning_family:fairy:cleaning_wipes",
            "family_label": "Fairy Temizlik Havlusu",
        },
    ]

    options = build_brand_filter_options(family_groups)

    assert [option["id"] for option in options] == [
        "all",
        "dishwashing_liquid",
        "dishwashing_spray",
        "dishwasher_tablet",
        "cleaning_wipes",
    ]
    assert options[0]["label"] == "Tümü"
    assert all(str(option["label"]).strip() for option in options)


def test_normalize_result_status_maps_to_single_source_of_truth():
    assert normalize_result_status("comparable") == "safe"
    assert normalize_result_status("only_a101") == "single_market"
    assert normalize_result_status("only_available_at_migros") == "single_market"
    assert normalize_result_status("comparison_review_required") == "review_required"


def test_build_group_result_rows_preserves_group_order_and_family_override():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "fairy bulasik deterjani 0.65 l",
                "a101_source_product_name": "Fairy Sıvı Bulaşık Deterjanı 650 ml",
                "migros_source_product_name": "Fairy Sıvı Bulaşık Deterjanı Limon 650 Ml",
                "a101_raw_price": 99.5,
                "migros_raw_price": 99.95,
                "a101_normalized_unit": "liter",
                "migros_normalized_unit": "liter",
                "a101_normalized_quantity": 0.65,
                "migros_normalized_quantity": 0.65,
                "coverage_status": "comparable",
                "comparison_status_label": "Karşılaştırılabilir",
            },
            {
                "standardized_product_name": "fairy sprey 0.5 l",
                "a101_source_product_name": "Fairy Power Sprey 500 ml",
                "migros_source_product_name": "Fairy Power Sprey 500 Ml",
                "a101_raw_price": 89.5,
                "migros_raw_price": 79.95,
                "a101_normalized_unit": "liter",
                "migros_normalized_unit": "liter",
                "a101_normalized_quantity": 0.5,
                "migros_normalized_quantity": 0.5,
                "coverage_status": "comparable",
                "comparison_status_label": "Karşılaştırılabilir",
            },
        ]
    )
    groups = [
        {
            "selection_id": "cleaning_family:fairy:dishwashing_spray",
            "selection_type": "product_family",
            "family_id": "cleaning_family:fairy:dishwashing_spray",
            "family_label": "Fairy Sprey",
            "product_names": ["fairy sprey 0.5 l"],
            "force_review": True,
        },
        {
            "selection_id": "product:fairy bulasik deterjani 0.65 l",
            "selection_type": "product",
            "family_id": None,
            "family_label": None,
            "product_names": ["fairy bulasik deterjani 0.65 l"],
        },
    ]

    rows = build_group_result_rows(catalog_df, groups)

    assert [row["group"]["selection_id"] for row in rows] == [
        "cleaning_family:fairy:dishwashing_spray",
        "product:fairy bulasik deterjani 0.65 l",
    ]
    assert rows[0]["display_name"] == "Fairy Sprey"
    assert rows[0]["coverage_status"] == "comparison_review_required"


def test_limit_cleaning_family_product_groups_shows_top_three_until_expanded():
    product_groups = [
        {"selection_id": f"product:fairy-{index}", "product_names": [f"fairy-{index}"]}
        for index in range(1, 6)
    ]

    collapsed = limit_cleaning_family_product_groups(
        product_groups,
        selected_product_id=None,
        limit=3,
        expanded=False,
    )
    assert [group["selection_id"] for group in collapsed] == [
        "product:fairy-1",
        "product:fairy-2",
        "product:fairy-3",
    ]
    assert has_hidden_cleaning_family_product_groups(product_groups, collapsed) is True

    selected_visible = limit_cleaning_family_product_groups(
        product_groups,
        selected_product_id="product:fairy-5",
        limit=3,
        expanded=False,
    )
    assert selected_visible[0]["selection_id"] == "product:fairy-5"

    expanded = limit_cleaning_family_product_groups(
        product_groups,
        selected_product_id="product:fairy-5",
        limit=3,
        expanded=True,
    )
    assert len(expanded) == 5


def test_build_category_product_results_uses_total_pack_quantity_for_water_multipack():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "hayat su 6x1.5 l",
                "a101_source_product_name": "Hayat Su 6x1.5 L",
                "migros_source_product_name": None,
                "a101_raw_price": 60.0,
                "migros_raw_price": None,
                "a101_normalized_unit": "liter",
                "migros_normalized_unit": None,
                "a101_normalized_quantity": 1.5,
                "migros_normalized_quantity": None,
                "a101_comparison_price": None,
                "migros_comparison_price": None,
                "coverage_status": "only_a101",
            }
        ]
    )

    results = build_category_product_results(catalog_df, "su")

    assert len(results) == 1
    assert round(results[0]["best_unit_price"], 2) == 6.67
    assert results[0]["best_unit_label"] == "litre"


def test_compact_category_display_name_shortens_cleaning_rows():
    fairy_result = {
        "display_name": "Fairy Sıvı Bulaşık Deterjanı 650 ml",
        "best_unit_label": "litre",
        "brand_token": "fairy",
        "subtype": "dishwashing_liquid",
        "variant_token": None,
        "group": {"product_names": ["fairy bulasik deterjani 0.65 l"]},
    }
    pril_result = {
        "display_name": "Pril Bulaşık Deterjanı 653 ml",
        "best_unit_label": "litre",
        "brand_token": "pril",
        "subtype": "dishwashing_liquid",
        "variant_token": None,
        "group": {"product_names": ["pril bulasik deterjani 0.653 l"]},
    }

    assert format_compact_category_display_name(fairy_result) == "Fairy 650 ml"
    assert format_compact_category_display_name(pril_result) == "Pril 653 ml"


def test_compact_category_display_name_handles_tablet_and_adet_names_without_crashing():
    finish_result = {
        "display_name": "Finish Bulaşık Makinesi Tableti 101'li",
        "best_unit_label": "adet",
        "brand_token": "finish",
        "subtype": "dishwasher_tablet",
        "variant_token": None,
        "group": {"product_names": ["finish bulasik tableti 101li"]},
    }
    migros_result = {
        "display_name": "Migros Ultra Bulaşık Makinesi Tableti 33 Adet 627G",
        "best_unit_label": "adet",
        "brand_token": None,
        "subtype": "dishwasher_tablet",
        "variant_token": None,
        "group": {"product_names": ["migros ultra bulasik tableti 33 adet 627 g"]},
    }
    fairy_result = {
        "display_name": "Fairy Platinum Fresh Tablet 30'lu 447 G",
        "best_unit_label": "adet",
        "brand_token": "fairy",
        "subtype": "dishwasher_tablet",
        "variant_token": "platinum",
        "group": {"product_names": ["fairy platinum fresh tablet 30lu 447 g"]},
    }

    assert format_compact_category_display_name(finish_result) == "Finish Tablet 101'li"
    assert format_compact_category_display_name(migros_result) == "Migros Ultra Bulaşık Makinesi Tableti 33 Adet 627G"
    assert format_compact_category_display_name(fairy_result) == "Fairy Tablet Platinum 30'li"


def test_resolve_category_result_selection_prefers_clicked_row_when_valid():
    category_results = [
        {"group": {"selection_id": "product:fairy bulasik deterjani 0.65 l"}},
        {"group": {"selection_id": "product:pril bulasik deterjani 0.653 l"}},
    ]

    assert (
        resolve_category_result_selection(
            category_results,
            "product:fairy bulasik deterjani 0.65 l",
            "product:pril bulasik deterjani 0.653 l",
        )
        == "product:pril bulasik deterjani 0.653 l"
    )
    assert (
        resolve_category_result_selection(
            category_results,
            "product:fairy bulasik deterjani 0.65 l",
            None,
        )
        == "product:fairy bulasik deterjani 0.65 l"
    )


def test_resolve_category_result_selection_clears_when_rebuilt_rows_do_not_contain_key():
    category_results = [
        {"group": {"selection_id": "product:pril bulasik deterjani 0.653 l"}},
        {"group": {"selection_id": "product:bingo bulasik deterjani 1.5 l"}},
    ]

    assert (
        resolve_category_result_selection(
            category_results,
            "product:fairy bulasik deterjani 0.65 l",
            None,
        )
        is None
    )


def test_resolve_category_filter_selection_prefers_requested_filter_when_valid():
    filter_ids = ["all", "fairy", "pril"]

    assert (
        resolve_category_filter_selection(
            filter_ids,
            "all",
            "fairy",
            "all",
        )
        == "fairy"
    )


def test_resolve_category_filter_selection_keeps_current_or_default_when_requested_invalid():
    filter_ids = ["all", "fairy", "pril"]

    assert (
        resolve_category_filter_selection(
            filter_ids,
            "pril",
            "bingo",
            "all",
        )
        == "pril"
    )
    assert (
        resolve_category_filter_selection(
            filter_ids,
            None,
            "bingo",
            "all",
        )
        == "all"
    )


def test_filter_change_keeps_filter_and_clears_invalid_selected_row():
    filter_ids = ["all", "fairy", "pril"]
    category_results = [
        {"group": {"selection_id": "product:pril bulasik deterjani 0.653 l"}},
    ]

    selected_filter = resolve_category_filter_selection(
        filter_ids,
        "all",
        "pril",
        "all",
    )
    selected_row = resolve_category_result_selection(
        category_results,
        "product:fairy bulasik deterjani 0.65 l",
        None,
    )

    assert selected_filter == "pril"
    assert selected_row is None


def test_combine_selection_groups_keeps_safe_groups_first_and_dedupes():
    safe_groups = [
        {
            "selection_id": "product:fairy elma bulasik deterjani 1.5 l",
            "selection_type": "product",
            "product_names": ["fairy elma bulasik deterjani 1.5 l"],
        }
    ]
    related_groups = [
        {
            "selection_id": "product:fairy elma bulasik deterjani 1.5 l",
            "selection_type": "product",
            "product_names": ["fairy elma bulasik deterjani 1.5 l"],
        },
        {
            "selection_id": "product:fairy bulasik deterjani 0.65 l",
            "selection_type": "product",
            "product_names": ["fairy bulasik deterjani 0.65 l"],
        },
    ]

    combined = combine_selection_groups(safe_groups, related_groups)

    assert [group["selection_id"] for group in combined] == [
        "product:fairy elma bulasik deterjani 1.5 l",
        "product:fairy bulasik deterjani 0.65 l",
    ]


def test_cleaning_sku_button_key_is_stable_and_distinct():
    key = build_cleaning_selection_button_key(
        "cleaning_sku",
        "product:fairy bulasik deterjani 0.65 l",
        "cleaning_family:fairy:dishwashing_liquid",
    )

    assert key.startswith("cleaning_sku_cleaning_family_fairy_dishwashing_liquid")
    assert "product_fairy_bulasik_deterjani_0_65_l" in key


def test_preserve_or_reset_cleaning_product_selection_only_resets_when_groups_are_known():
    selected_product_id = "product:fairy bulasik deterjani 0.65 l"

    assert (
        preserve_or_reset_cleaning_product_selection(selected_product_id, None)
        == selected_product_id
    )
    assert (
        preserve_or_reset_cleaning_product_selection(
            selected_product_id,
            [{"selection_id": selected_product_id}],
        )
        == selected_product_id
    )
    assert (
        preserve_or_reset_cleaning_product_selection(
            selected_product_id,
            [{"selection_id": "product:fairy elma bulasik deterjani 1.5 l"}],
        )
        is None
    )


def test_brand_only_fairy_exploratory_list_keeps_comparable_group_ahead_of_single_market():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "fairy bulasik deterjani 0.65 l",
                "source_count": 1,
                "available_retailers": "a101",
                "coverage_status": "only_a101",
                "a101_source_product_name": "Fairy Sıvı Bulaşık Deterjanı 650 ml",
                "migros_source_product_name": None,
            },
            {
                "standardized_product_name": "fairy elma bulasik deterjani 1.5 l",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Fairy Elma Bulaşık Deterjanı 1500 Ml",
                "migros_source_product_name": "Fairy Temiz & Ferah Elma Kokulu Elde Yıkama 1.5 L",
            },
        ]
    )

    sections = build_search_group_sections(catalog_df, "fairy")
    combined = combine_selection_groups(sections["safe_groups"], sections["related_groups"])

    assert combined[0]["selection_id"] == "product:fairy elma bulasik deterjani 1.5 l"


def test_brand_only_fairy_selects_best_comparable_default_group_when_available():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "fairy bulasik deterjani 0.65 l",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Fairy Sıvı Bulaşık Deterjanı 650 ml",
                "migros_source_product_name": "Fairy Sıvı Bulaşık Deterjanı Limon 650 Ml",
            },
            {
                "standardized_product_name": "fairy sprey 0.5 l",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Fairy Power Sprey 500 ml",
                "migros_source_product_name": "Fairy Power Sprey 500 Ml",
            },
            {
                "standardized_product_name": "fairy elma bulasik deterjani 1.5 l",
                "source_count": 1,
                "available_retailers": "a101",
                "coverage_status": "only_a101",
                "a101_source_product_name": "Fairy Elma Bulaşık Deterjanı 1500 Ml",
                "migros_source_product_name": None,
            },
        ]
    )

    sections = build_search_group_sections(catalog_df, "fairy")
    selected_group = select_brand_only_cleaning_default_group(
        catalog_df,
        sections["safe_groups"],
        "fairy",
    )

    assert selected_group is not None
    assert selected_group["selection_id"] == "product:fairy bulasik deterjani 0.65 l"


def test_build_brand_only_cleaning_groups_for_fairy_returns_family_labels():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "fairy elma bulasik deterjani 1.5 l",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Fairy Elma Bulaşık Deterjanı 1500 Ml",
                "migros_source_product_name": "Fairy Temiz & Ferah Elma Kokulu Elde Yıkama 1.5 L",
            },
            {
                "standardized_product_name": "30'lu fairy platinum",
                "source_count": 1,
                "available_retailers": "migros",
                "coverage_status": "only_migros",
                "a101_source_product_name": None,
                "migros_source_product_name": "Fairy Platinum Bulaşık Makinesi Tableti 30'lu 447 G",
            },
            {
                "standardized_product_name": "fairy power sprey",
                "source_count": 1,
                "available_retailers": "a101",
                "coverage_status": "only_a101",
                "a101_source_product_name": "Fairy Power Sprey 500 Ml",
                "migros_source_product_name": None,
            },
            {
                "standardized_product_name": "100'lu fairy havlusu temizleme yuzey",
                "source_count": 1,
                "available_retailers": "a101",
                "coverage_status": "only_a101",
                "a101_source_product_name": "Fairy Yüzey Temizleme Havlusu 100'lü",
                "migros_source_product_name": None,
            },
        ]
    )

    sections = build_search_group_sections(catalog_df, "fairy")
    groups = build_brand_only_cleaning_groups(
        catalog_df,
        sections["safe_groups"],
        sections["related_groups"],
        "fairy",
    )

    assert [group["family_label"] for group in groups] == [
        "Fairy Sıvı Bulaşık Deterjanı",
        "Fairy Sprey",
        "Fairy Bulaşık Tableti",
        "Fairy Temizlik Havlusu",
    ]


def test_build_brand_only_cleaning_groups_for_pril_and_omo_return_subtype_choices():
    pril_catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "pril bulasik deterjani 0.653 l",
                "source_count": 1,
                "available_retailers": "a101",
                "coverage_status": "only_a101",
                "a101_source_product_name": "Pril Bulaşık Deterjanı 653 ml",
                "migros_source_product_name": None,
            },
            {
                "standardized_product_name": "pril makine parlaticisi 0.5 l",
                "source_count": 1,
                "available_retailers": "migros",
                "coverage_status": "only_migros",
                "a101_source_product_name": None,
                "migros_source_product_name": "Pril Bulaşık Makinesi Parlatıcısı 500 Ml",
            },
        ]
    )
    pril_sections = build_search_group_sections(pril_catalog_df, "pril")
    pril_groups = build_brand_only_cleaning_groups(
        pril_catalog_df,
        pril_sections["safe_groups"],
        pril_sections["related_groups"],
        "pril",
    )
    assert [group["family_label"] for group in pril_groups] == [
        "Pril Sıvı Bulaşık Deterjanı",
        "Pril Parlatıcı",
    ]

    omo_catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "deterjan omo sivi",
                "source_count": 1,
                "available_retailers": "a101",
                "coverage_status": "only_a101",
                "a101_source_product_name": "Omo Sıvı Deterjan 1690 Ml",
                "migros_source_product_name": None,
            },
            {
                "standardized_product_name": "20 active beyazlar camasir deterjani fresh omo toz yikama",
                "source_count": 1,
                "available_retailers": "migros",
                "coverage_status": "only_migros",
                "a101_source_product_name": None,
                "migros_source_product_name": "Omo Active Fresh Beyazlar Toz Çamaşır Deterjanı 3 KG 20 yıkama",
            },
            {
                "standardized_product_name": "omo kapsul deterjan",
                "source_count": 1,
                "available_retailers": "migros",
                "coverage_status": "only_migros",
                "a101_source_product_name": None,
                "migros_source_product_name": "Omo Kapsül Deterjan 24'lü",
            },
            {
                "standardized_product_name": "sut homojenize",
                "source_count": 1,
                "available_retailers": "a101",
                "coverage_status": "only_a101",
                "a101_source_product_name": "Süt Homojenize 1 L",
                "migros_source_product_name": None,
            },
        ]
    )
    omo_sections = build_search_group_sections(omo_catalog_df, "omo")
    omo_groups = build_brand_only_cleaning_groups(
        omo_catalog_df,
        omo_sections["safe_groups"],
        omo_sections["related_groups"],
        "omo",
    )
    assert [group["family_label"] for group in omo_groups] == [
        "Omo Sıvı Deterjan",
        "Omo Toz Deterjan",
        "Omo Kapsül Deterjan",
    ]


def test_build_cleaning_family_product_groups_for_fairy_liquid_returns_multiple_skus():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "fairy bulasik deterjani 0.65 l",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Fairy Sıvı Bulaşık Deterjanı 650 ml",
                "migros_source_product_name": "Fairy Sıvı Bulaşık Deterjanı Limon 650 Ml",
            },
            {
                "standardized_product_name": "fairy elma bulasik deterjani 1.5 l",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Fairy Elma Bulaşık Deterjanı 1500 Ml",
                "migros_source_product_name": "Fairy Temiz & Ferah Elma Kokulu Elde Yıkama 1.5 L",
            },
            {
                "standardized_product_name": "fairy bulasik deterjani 1 l",
                "source_count": 1,
                "available_retailers": "a101",
                "coverage_status": "only_a101",
                "a101_source_product_name": "Fairy Sıvı Bulaşık Deterjanı 1 L",
                "migros_source_product_name": None,
            },
            {
                "standardized_product_name": "fairy bulasik deterjani 2.6 l",
                "source_count": 1,
                "available_retailers": "a101",
                "coverage_status": "only_a101",
                "a101_source_product_name": "Fairy Sıvı Bulaşık Deterjanı 2600 Ml",
                "migros_source_product_name": None,
            },
            {
                "standardized_product_name": "2x1500 bulasik deterjani fairy",
                "source_count": 1,
                "available_retailers": "a101",
                "coverage_status": "only_a101",
                "a101_source_product_name": "Fairy Bulaşık Deterjanı 2x1500 Ml",
                "migros_source_product_name": None,
            },
            {
                "standardized_product_name": "6x650 bulasik deterjani fairy",
                "source_count": 1,
                "available_retailers": "a101",
                "coverage_status": "only_a101",
                "a101_source_product_name": "Fairy Bulaşık Deterjanı 6x650 Ml",
                "migros_source_product_name": None,
            },
            {
                "standardized_product_name": "fairy power sprey 500 ml",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Fairy Power Sprey 500 Ml",
                "migros_source_product_name": "Fairy Power Sprey 500 Ml",
            },
            {
                "standardized_product_name": "30'lu fairy platinum tablet",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Fairy Platinum Tablet 30'lu",
                "migros_source_product_name": "Fairy Platinum Bulaşık Makinesi Tableti 30'lu",
            },
        ]
    )

    sections = build_search_group_sections(catalog_df, "fairy")
    family_groups = build_brand_only_cleaning_groups(
        catalog_df,
        sections["safe_groups"],
        sections["related_groups"],
        "fairy",
    )
    liquid_family = next(
        group
        for group in family_groups
        if group["family_label"] == "Fairy Sıvı Bulaşık Deterjanı"
    )

    product_groups = build_cleaning_family_product_groups(
        sections["search_results"],
        liquid_family,
    )

    assert [group["product_names"][0] for group in product_groups] == [
        "fairy bulasik deterjani 0.65 l",
        "fairy elma bulasik deterjani 1.5 l",
        "fairy bulasik deterjani 1 l",
        "fairy bulasik deterjani 2.6 l",
        "2x1500 bulasik deterjani fairy",
        "6x650 bulasik deterjani fairy",
    ]


def test_build_cleaning_family_product_groups_for_fairy_spray_and_tablets_return_multiple_options():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "fairy power sprey 500 ml",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Fairy Power Sprey 500 Ml",
                "migros_source_product_name": "Fairy Power Sprey 500 Ml",
            },
            {
                "standardized_product_name": "fairy power sprey portakal 800 ml",
                "source_count": 1,
                "available_retailers": "migros",
                "coverage_status": "only_migros",
                "a101_source_product_name": None,
                "migros_source_product_name": "Fairy Power Sprey Portakal 800 Ml",
            },
            {
                "standardized_product_name": "30'lu fairy platinum tablet",
                "source_count": 2,
                "available_retailers": "a101, migros",
                "coverage_status": "comparable",
                "a101_source_product_name": "Fairy Platinum Tablet 30'lu",
                "migros_source_product_name": "Fairy Platinum Bulaşık Makinesi Tableti 30'lu",
            },
            {
                "standardized_product_name": "40'li fairy kapsul tablet",
                "source_count": 1,
                "available_retailers": "a101",
                "coverage_status": "only_a101",
                "a101_source_product_name": "Fairy Platinum Plus Bulaşık Makinesi Kapsülü 40'lı",
                "migros_source_product_name": None,
            },
        ]
    )

    sections = build_search_group_sections(catalog_df, "fairy")
    family_groups = build_brand_only_cleaning_groups(
        catalog_df,
        sections["safe_groups"],
        sections["related_groups"],
        "fairy",
    )

    spray_family = next(
        group for group in family_groups if group["family_label"] == "Fairy Sprey"
    )
    spray_groups = build_cleaning_family_product_groups(
        sections["search_results"],
        spray_family,
    )
    assert [group["product_names"][0] for group in spray_groups] == [
        "fairy power sprey 500 ml",
        "fairy power sprey portakal 800 ml",
    ]

    tablet_family = next(
        group
        for group in family_groups
        if group["family_label"] == "Fairy Bulaşık Tableti"
    )
    tablet_groups = build_cleaning_family_product_groups(
        sections["search_results"],
        tablet_family,
    )
    assert [group["product_names"][0] for group in tablet_groups] == [
        "30'lu fairy platinum tablet",
        "40'li fairy kapsul tablet",
    ]


def test_build_cleaning_sibling_product_groups_excludes_selected_sku_and_keeps_same_subtype_options():
    product_groups = [
        {
            "selection_id": "product:fairy sprey 0.5 l",
            "selection_type": "product",
            "product_names": ["fairy sprey 0.5 l"],
        },
        {
            "selection_id": "product:fairy sprey 0.8 l",
            "selection_type": "product",
            "product_names": ["fairy sprey 0.8 l"],
        },
        {
            "selection_id": "product:fairy sprey portakal 0.5 l",
            "selection_type": "product",
            "product_names": ["fairy sprey portakal 0.5 l"],
        },
        {
            "selection_id": "product:fairy sprey portakal 0.8 l",
            "selection_type": "product",
            "product_names": ["fairy sprey portakal 0.8 l"],
        },
    ]

    siblings = build_cleaning_sibling_product_groups(
        product_groups,
        "product:fairy sprey 0.5 l",
    )

    assert [group["selection_id"] for group in siblings] == [
        "product:fairy sprey 0.8 l",
        "product:fairy sprey portakal 0.5 l",
        "product:fairy sprey portakal 0.8 l",
    ]


def test_resolved_cleaning_brand_token_handles_common_typos():
    assert resolved_cleaning_brand_token("fary") == "fairy"
    assert resolved_cleaning_brand_token("frinish") == "finish"
    assert resolved_cleaning_brand_token("pring") == "pril"
    assert resolved_cleaning_brand_token("bingooo") == "bingo"


def test_sort_brand_result_rows_prefers_fairy_liquid_before_spray():
    results = [
        {
            "display_name": "Fairy Power Sprey 500 Ml",
            "coverage_status": "comparable",
            "best_unit_price": 239.9,
            "best_price": 119.95,
            "brand_token": "fairy",
            "subtype": "dishwashing_spray",
        },
        {
            "display_name": "Fairy Sıvı Bulaşık Deterjanı 650 ml",
            "coverage_status": "comparable",
            "best_unit_price": 153.08,
            "best_price": 99.5,
            "brand_token": "fairy",
            "subtype": "dishwashing_liquid",
        },
    ]

    ranked = sort_brand_result_rows("fairy", results)

    assert [row["display_name"] for row in ranked] == [
        "Fairy Sıvı Bulaşık Deterjanı 650 ml",
        "Fairy Power Sprey 500 Ml",
    ]


def test_limit_brand_result_rows_shows_top_eight_until_expanded():
    result_rows = [
        {
            "group": {"selection_id": f"product:fairy-{index}"},
            "display_name": f"Fairy {index}",
        }
        for index in range(1, 11)
    ]

    collapsed_rows = limit_brand_result_rows(
        result_rows,
        selected_selection_id=None,
        limit=8,
        expanded=False,
    )
    assert [row["group"]["selection_id"] for row in collapsed_rows] == [
        "product:fairy-1",
        "product:fairy-2",
        "product:fairy-3",
        "product:fairy-4",
        "product:fairy-5",
        "product:fairy-6",
        "product:fairy-7",
        "product:fairy-8",
    ]
    assert has_hidden_brand_result_rows(result_rows, collapsed_rows) is True

    selected_rows = limit_brand_result_rows(
        result_rows,
        selected_selection_id="product:fairy-10",
        limit=8,
        expanded=False,
    )
    assert selected_rows[0]["group"]["selection_id"] == "product:fairy-10"

    expanded_rows = limit_brand_result_rows(
        result_rows,
        selected_selection_id="product:fairy-10",
        limit=8,
        expanded=True,
    )
    assert len(expanded_rows) == 10


def test_sort_specific_result_rows_prefers_finish_power_variants():
    results = [
        {
            "display_name": "Finish Bulaşık Makinesi Tableti 101'li",
            "coverage_status": "only_a101",
            "best_unit_price": 2.99,
            "best_price": 329.0,
            "brand_token": "finish",
            "subtype": "dishwasher_tablet",
            "group": {"product_names": ["101li finish tableti"]},
        },
        {
            "display_name": "Finish Quantum Powerball Bulaşık Tableti 40'lı",
            "coverage_status": "only_a101",
            "best_unit_price": 8.22,
            "best_price": 329.0,
            "brand_token": "finish",
            "subtype": "dishwasher_tablet",
            "group": {"product_names": ["finish power bulasik tableti 40 li"]},
        },
    ]

    ranked = sort_specific_result_rows("finish power", results)

    assert ranked[0]["display_name"] == "Finish Quantum Powerball Bulaşık Tableti 40'lı"
def test_sort_specific_result_rows_demotes_generic_finish_rows_below_power_family_rows():
    results = [
        {
            "display_name": "Finish Bulaşık Makinesi Tableti 101'li",
            "coverage_status": "only_a101",
            "best_unit_price": 2.99,
            "best_price": 329.0,
            "brand_token": "finish",
            "subtype": "dishwasher_tablet",
            "group": {"product_names": ["101li finish tableti"]},
        },
        {
            "display_name": "Finish Ultimate Plus Bulaşık Tableti 50'li",
            "coverage_status": "only_migros",
            "best_unit_price": 8.75,
            "best_price": 437.5,
            "brand_token": "finish",
            "subtype": "dishwasher_tablet",
            "group": {"product_names": ["finish ultimate bulasik tableti 50 li"]},
        },
        {
            "display_name": "Finish Quantum Powerball Bulaşık Tableti 40'lı",
            "coverage_status": "only_a101",
            "best_unit_price": 8.22,
            "best_price": 329.0,
            "brand_token": "finish",
            "subtype": "dishwasher_tablet",
            "group": {"product_names": ["finish power bulasik tableti 40 li"]},
        },
    ]

    ranked = sort_specific_result_rows("finish power", results)

    assert [row["display_name"] for row in ranked[:2]] == [
        "Finish Quantum Powerball Bulaşık Tableti 40'lı",
        "Finish Ultimate Plus Bulaşık Tableti 50'li",
    ]


def test_brand_only_cleaning_query_detection_does_not_misclassify_domates_or_typos():
    assert is_brand_only_cleaning_query("domates") is False
    assert is_brand_only_cleaning_query("fary") is False


def test_detect_search_mode_keeps_domates_and_brand_typos_out_of_brand_mode():
    assert detect_search_mode("domates") == CLEANING_SEARCH_MODE_SPECIFIC
    assert detect_search_mode("fary") == CLEANING_SEARCH_MODE_SPECIFIC


@pytest.mark.parametrize(
    ("query", "expected_mode"),
    [
        ("fairy", CLEANING_SEARCH_MODE_BRAND),
        ("finish", CLEANING_SEARCH_MODE_BRAND),
        ("domestos", CLEANING_SEARCH_MODE_BRAND),
        ("fary", CLEANING_SEARCH_MODE_SPECIFIC),
        ("domates", CLEANING_SEARCH_MODE_SPECIFIC),
        ("salatalik", CLEANING_SEARCH_MODE_SPECIFIC),
        ("hiyar", CLEANING_SEARCH_MODE_SPECIFIC),
        ("sut", CLEANING_SEARCH_MODE_SPECIFIC),
        ("su", CLEANING_SEARCH_MODE_SPECIFIC),
        ("tuvalet kagidi", CLEANING_SEARCH_MODE_SPECIFIC),
        ("bulasik tableti", CLEANING_SEARCH_MODE_CATEGORY),
    ],
)
def test_golden_query_search_modes_stay_stable(query, expected_mode):
    assert detect_search_mode(query) == expected_mode


@pytest.mark.parametrize(
    ("query", "expected_brand_token"),
    [
        ("fairy", "fairy"),
        ("fary", "fairy"),
        ("domestos", "domestos"),
    ],
)
def test_golden_brand_and_typo_queries_resolve_to_expected_cleaning_brand(
    query, expected_brand_token
):
    assert resolved_cleaning_brand_token(query) == expected_brand_token
