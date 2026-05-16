from decimal import Decimal

import pandas as pd

from pipeline.optimizer.cleaning_products import (
    augment_catalog_with_cleaning_rows,
    infer_cleaning_product_profile,
    synthesize_cleaning_price_rows,
)
from pipeline.optimizer.product_search import search_product_catalog


def test_augment_catalog_with_cleaning_rows_builds_fairy_650_comparable_row():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "fairy bulasik deterjani 0.65 l",
                "source_count": 1,
                "available_retailers": "a101",
                "coverage_status": "only_a101",
                "a101_source_product_name": "Fairy Sıvı Bulaşık Deterjanı 650 ml",
                "migros_source_product_name": None,
                "a101_normalized_unit": "liter",
                "migros_normalized_unit": None,
                "a101_normalized_quantity": Decimal("0.65"),
                "migros_normalized_quantity": None,
                "a101_raw_price": Decimal("54.90"),
                "migros_raw_price": None,
                "a101_comparison_price": Decimal("54.90"),
                "migros_comparison_price": None,
                "comparison_price_unit": "liter",
                "same_unit_flag": False,
                "same_quantity_flag": False,
                "comparison_confidence": "single_source",
                "comparison_review_reason": None,
            },
            {
                "standardized_product_name": "fairy limon bulasik deterjani 0.65 l",
                "source_count": 1,
                "available_retailers": "migros",
                "coverage_status": "only_migros",
                "a101_source_product_name": None,
                "migros_source_product_name": "Fairy Sıvı Bulaşık Deterjanı Limon 650 Ml",
                "a101_normalized_unit": None,
                "migros_normalized_unit": "liter",
                "a101_normalized_quantity": None,
                "migros_normalized_quantity": Decimal("0.65"),
                "a101_raw_price": None,
                "migros_raw_price": Decimal("62.95"),
                "a101_comparison_price": None,
                "migros_comparison_price": Decimal("62.95"),
                "comparison_price_unit": "liter",
                "same_unit_flag": False,
                "same_quantity_flag": False,
                "comparison_confidence": "single_source",
                "comparison_review_reason": None,
            },
        ]
    )

    augmented_df = augment_catalog_with_cleaning_rows(catalog_df)
    row = augmented_df.loc[
        augmented_df["standardized_product_name"] == "fairy bulasik deterjani 0.65 l"
    ].iloc[0]

    assert row["coverage_status"] == "comparable"
    assert row["a101_source_product_name"] == "Fairy Sıvı Bulaşık Deterjanı 650 ml"
    assert row["migros_source_product_name"] == "Fairy Sıvı Bulaşık Deterjanı Limon 650 Ml"


def test_explicit_fairy_limon_650_does_not_select_generic_synthesized_pair():
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
                "standardized_product_name": "fairy limon bulasik deterjani 0.65 l",
                "source_count": 1,
                "available_retailers": "migros",
                "coverage_status": "only_migros",
                "a101_source_product_name": None,
                "migros_source_product_name": "Fairy Sıvı Bulaşık Deterjanı Limon 650 Ml",
            },
        ]
    )

    assert search_product_catalog(catalog_df, "fairy limon 650")[
        "standardized_product_name"
    ].tolist()[0] == "fairy limon bulasik deterjani 0.65 l"


def test_synthesize_cleaning_price_rows_builds_fairy_650_comparable_row():
    latest_price_rows = [
        {
            "standardized_product_name": "fairy bulasik deterjani 0.65 l",
            "canonical_name": "fairy bulasik deterjani 0.65 l",
            "a101_price": Decimal("54.90"),
            "migros_price": None,
            "comparison_confidence": "single_source",
            "a101_source_product_name": "Fairy Sıvı Bulaşık Deterjanı 650 ml",
            "migros_source_product_name": None,
            "a101_normalized_unit": "liter",
            "migros_normalized_unit": None,
            "a101_normalized_quantity": Decimal("0.65"),
            "migros_normalized_quantity": None,
        },
        {
            "standardized_product_name": "fairy limon bulasik deterjani 0.65 l",
            "canonical_name": "fairy limon bulasik deterjani 0.65 l",
            "a101_price": None,
            "migros_price": Decimal("62.95"),
            "comparison_confidence": "single_source",
            "a101_source_product_name": None,
            "migros_source_product_name": "Fairy Sıvı Bulaşık Deterjanı Limon 650 Ml",
            "a101_normalized_unit": None,
            "migros_normalized_unit": "liter",
            "a101_normalized_quantity": None,
            "migros_normalized_quantity": Decimal("0.65"),
        },
    ]

    synthesized = synthesize_cleaning_price_rows(latest_price_rows)
    row = next(
        row
        for row in synthesized
        if row["standardized_product_name"] == "fairy bulasik deterjani 0.65 l"
    )

    assert row["comparison_confidence"] == "high"
    assert row["a101_source_product_name"] == "Fairy Sıvı Bulaşık Deterjanı 650 ml"
    assert row["migros_source_product_name"] == "Fairy Sıvı Bulaşık Deterjanı Limon 650 Ml"


def test_infer_cleaning_product_profile_treats_fairy_count_weight_rows_as_tablets():
    profile = infer_cleaning_product_profile("Fairy Platinum 30'lu 447 G")

    assert profile["subtype"] == "dishwasher_tablet"
    assert profile["package_format"] == "tablet_capsule"


def test_infer_cleaning_product_profile_treats_spray_and_sprey_as_same_cleaning_package():
    profile_sprey = infer_cleaning_product_profile("Fairy Power Sprey 500 Ml")
    profile_spray = infer_cleaning_product_profile("Fairy Power Spray Portakal 800 Ml")

    assert profile_sprey["subtype"] == "dishwashing_liquid"
    assert profile_sprey["package_format"] == "spray"
    assert profile_spray["subtype"] == "dishwashing_liquid"
    assert profile_spray["package_format"] == "spray"


def test_augment_catalog_with_cleaning_rows_builds_fairy_spray_comparable_row_and_keeps_800_separate():
    catalog_df = pd.DataFrame(
        [
            {
                "standardized_product_name": "fairy power sprey",
                "source_count": 1,
                "available_retailers": "a101",
                "coverage_status": "only_a101",
                "a101_source_product_name": "Fairy Power Sprey 500 Ml",
                "migros_source_product_name": None,
                "a101_normalized_unit": None,
                "migros_normalized_unit": None,
                "a101_normalized_quantity": None,
                "migros_normalized_quantity": None,
                "a101_raw_price": Decimal("84.90"),
                "migros_raw_price": None,
                "a101_comparison_price": None,
                "migros_comparison_price": None,
                "comparison_price_unit": None,
                "same_unit_flag": False,
                "same_quantity_flag": False,
                "comparison_confidence": "single_source",
                "comparison_review_reason": None,
            },
            {
                "standardized_product_name": "fairy power sprey",
                "source_count": 1,
                "available_retailers": "migros",
                "coverage_status": "only_migros",
                "a101_source_product_name": None,
                "migros_source_product_name": "Fairy Power Sprey 500 Ml",
                "a101_normalized_unit": None,
                "migros_normalized_unit": None,
                "a101_normalized_quantity": None,
                "migros_normalized_quantity": None,
                "a101_raw_price": None,
                "migros_raw_price": Decimal("89.95"),
                "a101_comparison_price": None,
                "migros_comparison_price": None,
                "comparison_price_unit": None,
                "same_unit_flag": False,
                "same_quantity_flag": False,
                "comparison_confidence": "single_source",
                "comparison_review_reason": None,
            },
            {
                "standardized_product_name": "fairy portakal power spray",
                "source_count": 1,
                "available_retailers": "migros",
                "coverage_status": "only_migros",
                "a101_source_product_name": None,
                "migros_source_product_name": "Fairy Power Spray Portakal 800 Ml",
                "a101_normalized_unit": None,
                "migros_normalized_unit": None,
                "a101_normalized_quantity": None,
                "migros_normalized_quantity": None,
                "a101_raw_price": None,
                "migros_raw_price": Decimal("119.95"),
                "a101_comparison_price": None,
                "migros_comparison_price": None,
                "comparison_price_unit": None,
                "same_unit_flag": False,
                "same_quantity_flag": False,
                "comparison_confidence": "single_source",
                "comparison_review_reason": None,
            },
        ]
    )

    augmented_df = augment_catalog_with_cleaning_rows(catalog_df)

    assert "fairy sprey 0.5 l" in augmented_df["standardized_product_name"].tolist()
    assert "fairy sprey portakal 0.8 l" in augmented_df["standardized_product_name"].tolist()

    comparable_row = augmented_df.loc[
        augmented_df["standardized_product_name"] == "fairy sprey 0.5 l"
    ].iloc[0]
    single_row = augmented_df.loc[
        augmented_df["standardized_product_name"] == "fairy sprey portakal 0.8 l"
    ].iloc[0]

    assert comparable_row["coverage_status"] == "comparable"
    assert comparable_row["a101_source_product_name"] == "Fairy Power Sprey 500 Ml"
    assert comparable_row["migros_source_product_name"] == "Fairy Power Sprey 500 Ml"
    assert single_row["coverage_status"] == "only_migros"
    assert single_row["migros_source_product_name"] == "Fairy Power Spray Portakal 800 Ml"


def test_synthesize_cleaning_price_rows_builds_fairy_spray_comparable_row_and_keeps_800_separate():
    latest_price_rows = [
        {
            "standardized_product_name": "fairy power sprey",
            "canonical_name": "fairy power sprey",
            "a101_price": Decimal("84.90"),
            "migros_price": None,
            "comparison_confidence": "single_source",
            "a101_source_product_name": "Fairy Power Sprey 500 Ml",
            "migros_source_product_name": None,
            "a101_normalized_unit": None,
            "migros_normalized_unit": None,
            "a101_normalized_quantity": None,
            "migros_normalized_quantity": None,
        },
        {
            "standardized_product_name": "fairy power sprey",
            "canonical_name": "fairy power sprey",
            "a101_price": None,
            "migros_price": Decimal("89.95"),
            "comparison_confidence": "single_source",
            "a101_source_product_name": None,
            "migros_source_product_name": "Fairy Power Sprey 500 Ml",
            "a101_normalized_unit": None,
            "migros_normalized_unit": None,
            "a101_normalized_quantity": None,
            "migros_normalized_quantity": None,
        },
        {
            "standardized_product_name": "fairy portakal power spray",
            "canonical_name": "fairy portakal power spray",
            "a101_price": None,
            "migros_price": Decimal("119.95"),
            "comparison_confidence": "single_source",
            "a101_source_product_name": None,
            "migros_source_product_name": "Fairy Power Spray Portakal 800 Ml",
            "a101_normalized_unit": None,
            "migros_normalized_unit": None,
            "a101_normalized_quantity": None,
            "migros_normalized_quantity": None,
        },
    ]

    synthesized = synthesize_cleaning_price_rows(latest_price_rows)

    comparable_row = next(
        row
        for row in synthesized
        if row["standardized_product_name"] == "fairy sprey 0.5 l"
    )
    single_row = next(
        row
        for row in synthesized
        if row["standardized_product_name"] == "fairy sprey portakal 0.8 l"
    )

    assert comparable_row["comparison_confidence"] == "high"
    assert comparable_row["a101_source_product_name"] == "Fairy Power Sprey 500 Ml"
    assert comparable_row["migros_source_product_name"] == "Fairy Power Sprey 500 Ml"
    assert single_row["comparison_confidence"] == "single_source"
    assert single_row["migros_source_product_name"] == "Fairy Power Spray Portakal 800 Ml"
