import pandas as pd

from pipeline.optimizer.paper_products import (
    augment_catalog_with_paper_towel_rows,
    synthesize_paper_towel_price_rows,
)
from pipeline.optimizer.product_search import build_search_group_sections


def _catalog_row(**kwargs):
    base = {
        "standardized_product_name": kwargs.get("standardized_product_name"),
        "source_count": kwargs.get("source_count", 1),
        "available_retailers": kwargs.get("available_retailers"),
        "a101_source_product_name": kwargs.get("a101_source_product_name"),
        "migros_source_product_name": kwargs.get("migros_source_product_name"),
        "a101_normalized_unit": kwargs.get("a101_normalized_unit"),
        "migros_normalized_unit": kwargs.get("migros_normalized_unit"),
        "a101_normalized_quantity": kwargs.get("a101_normalized_quantity"),
        "migros_normalized_quantity": kwargs.get("migros_normalized_quantity"),
        "a101_raw_price": kwargs.get("a101_raw_price"),
        "migros_raw_price": kwargs.get("migros_raw_price"),
        "a101_comparison_price": kwargs.get("a101_comparison_price"),
        "migros_comparison_price": kwargs.get("migros_comparison_price"),
        "comparison_price_unit": kwargs.get("comparison_price_unit"),
        "same_unit_flag": kwargs.get("same_unit_flag", False),
        "same_quantity_flag": kwargs.get("same_quantity_flag", False),
        "comparison_confidence": kwargs.get("comparison_confidence", "single_source"),
        "coverage_status": kwargs.get("coverage_status", "single_source"),
    }
    return base


def _latest_row(**kwargs):
    return {
        "standardized_product_name": kwargs["standardized_product_name"],
        "canonical_name": kwargs.get("canonical_name", kwargs["standardized_product_name"]),
        "a101_price": kwargs.get("a101_price"),
        "migros_price": kwargs.get("migros_price"),
        "cheaper_source": kwargs.get("cheaper_source"),
        "compared_at": kwargs.get("compared_at"),
        "same_unit_flag": kwargs.get("same_unit_flag", False),
        "same_quantity_flag": kwargs.get("same_quantity_flag", False),
        "comparison_confidence": kwargs.get("comparison_confidence", "single_source"),
        "a101_source_product_name": kwargs.get("a101_source_product_name"),
        "migros_source_product_name": kwargs.get("migros_source_product_name"),
        "a101_normalized_unit": kwargs.get("a101_normalized_unit"),
        "migros_normalized_unit": kwargs.get("migros_normalized_unit"),
        "a101_normalized_quantity": kwargs.get("a101_normalized_quantity"),
        "migros_normalized_quantity": kwargs.get("migros_normalized_quantity"),
    }


def test_augment_catalog_with_paper_towel_rows_builds_comparable_pair_from_single_source_rows():
    catalog_df = pd.DataFrame(
        [
            _catalog_row(
                standardized_product_name="solo kagit havlu 6 roll",
                available_retailers="a101",
                a101_source_product_name="Solo Bambu Kağıt Havlu 2 Katlı 6'lı",
                a101_raw_price=99.9,
                a101_normalized_unit="roll",
                a101_normalized_quantity=6.0,
                coverage_status="only_a101",
            ),
            _catalog_row(
                standardized_product_name="6'li bambu havlu katkili solo",
                available_retailers="migros",
                migros_source_product_name="Solo Bambu Katkılı Havlu 6'lı",
                migros_raw_price=109.95,
                migros_normalized_unit="piece",
                migros_normalized_quantity=1.0,
                coverage_status="only_migros",
            ),
        ]
    )

    augmented_df = augment_catalog_with_paper_towel_rows(catalog_df)
    row = augmented_df.loc[
        augmented_df["standardized_product_name"] == "solo kagit havlu 6 roll"
    ].iloc[0]

    assert row["coverage_status"] == "comparable"
    assert row["a101_source_product_name"] == "Solo Bambu Kağıt Havlu 2 Katlı 6'lı"
    assert row["migros_source_product_name"] == "Solo Bambu Katkılı Havlu 6'lı"
    assert row["a101_normalized_quantity"] == 6.0
    assert row["migros_normalized_quantity"] == 6.0


def test_synthesize_paper_towel_price_rows_downgrades_same_brand_close_roll_count():
    latest_rows = [
        _latest_row(
            standardized_product_name="solo kagit havlu 6 roll",
            a101_price=99.9,
            a101_source_product_name="Solo Bambu Kağıt Havlu 2 Katlı 6'lı",
            a101_normalized_unit="roll",
            a101_normalized_quantity=6.0,
        ),
        _latest_row(
            standardized_product_name="solo asilabilir havlu 1=7 rulo 325 yaprak",
            migros_price=239.95,
            migros_source_product_name="Solo Asılabilir Havlu 1=7 Rulo 325 Yaprak",
            migros_normalized_unit="piece",
            migros_normalized_quantity=1.0,
        ),
    ]

    synthesized_rows = synthesize_paper_towel_price_rows(latest_rows)
    review_row = next(
        row for row in synthesized_rows if row["standardized_product_name"] == "solo kagit havlu"
    )

    assert review_row["coverage_status"] == "comparison_review_required"
    assert review_row["comparison_review_reason"] == "product_line_unknown"


def test_synthesize_paper_towel_price_rows_downgrades_same_brand_same_roll_when_product_line_differs():
    latest_rows = [
        _latest_row(
            standardized_product_name="papia kagit havlu 6 roll",
            a101_price=129.9,
            a101_source_product_name="Papia 3 Katlı Kağıt Havlu 6'lı",
            a101_normalized_unit="roll",
            a101_normalized_quantity=6.0,
        ),
        _latest_row(
            standardized_product_name="papia kagit havlu 6 roll",
            migros_price=149.95,
            migros_source_product_name="Papia Biocare Kağıt Havlu 6'lı",
            migros_normalized_unit="roll",
            migros_normalized_quantity=6.0,
        ),
    ]

    synthesized_rows = synthesize_paper_towel_price_rows(latest_rows)
    review_row = next(
        row for row in synthesized_rows if row["standardized_product_name"] == "papia kagit havlu 6 roll"
    )

    assert review_row["coverage_status"] == "comparison_review_required"
    assert review_row["comparison_review_reason"] == "product_line_unknown"


def test_synthesize_paper_towel_price_rows_downgrades_different_brands_same_roll_count():
    latest_rows = [
        _latest_row(
            standardized_product_name="papia kagit havlu 8 roll",
            a101_price=210.0,
            a101_source_product_name="Papia Inova 3 Katlı Kağıt Havlu 8'li",
            a101_normalized_unit="roll",
            a101_normalized_quantity=8.0,
        ),
        _latest_row(
            standardized_product_name="super kagit havlu 8 roll",
            migros_price=149.95,
            migros_source_product_name="Süper Maylo Bambu Katkılı Kağıt Havlu 8'li",
            migros_normalized_unit="piece",
            migros_normalized_quantity=1.0,
        ),
    ]

    synthesized_rows = synthesize_paper_towel_price_rows(latest_rows)
    review_row = next(
        row for row in synthesized_rows if row["standardized_product_name"] == "kagit havlu 8 roll"
    )

    assert review_row["coverage_status"] == "comparison_review_required"
    assert review_row["comparison_review_reason"] == "brand_mismatch"


def test_generic_paper_towel_search_returns_groups_when_candidates_exist():
    catalog_df = pd.DataFrame(
        [
            _catalog_row(
                standardized_product_name="solo kagit havlu 6 roll",
                available_retailers="a101",
                a101_source_product_name="Solo Bambu Kağıt Havlu 2 Katlı 6'lı",
                a101_raw_price=99.9,
                a101_normalized_unit="roll",
                a101_normalized_quantity=6.0,
                coverage_status="only_a101",
            ),
            _catalog_row(
                standardized_product_name="6'li bambu havlu katkili solo",
                available_retailers="migros",
                migros_source_product_name="Solo Bambu Katkılı Havlu 6'lı",
                migros_raw_price=109.95,
                migros_normalized_unit="piece",
                migros_normalized_quantity=1.0,
                coverage_status="only_migros",
            ),
            _catalog_row(
                standardized_product_name="solo tuvalet kagidi 6 roll",
                available_retailers="a101, migros",
                a101_source_product_name="Solo Tuvalet Kagidi 6'lı",
                migros_source_product_name="Solo Tuvalet Kagidi 6'lı",
                a101_raw_price=89.9,
                migros_raw_price=95.0,
                a101_normalized_unit="roll",
                migros_normalized_unit="roll",
                a101_normalized_quantity=6.0,
                migros_normalized_quantity=6.0,
                coverage_status="comparable",
            ),
        ]
    )

    augmented_df = augment_catalog_with_paper_towel_rows(catalog_df)
    sections = build_search_group_sections(augmented_df, "kağıt havlu")

    assert sections["safe_groups"] or sections["related_groups"]
    first_group = (sections["safe_groups"] or sections["related_groups"])[0]
    assert first_group["selection_id"] == "product:solo kagit havlu 6 roll"
