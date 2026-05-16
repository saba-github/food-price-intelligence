from decimal import Decimal

from pipeline.optimizer.public_compare import build_public_result_display


def test_build_public_result_display_prefers_normalized_unit_price_for_base_mantar():
    recommendation = {
        "standardized_product_name": "mantar",
        "recommended_retailer": "a101",
        "recommended_price": Decimal("64.90"),
        "a101_price": Decimal("64.90"),
        "migros_price": Decimal("87.95"),
        "comparison_confidence": "high",
    }
    catalog_row = {
        "comparison_price_unit": "kg",
        "a101_raw_price": Decimal("64.90"),
        "migros_raw_price": Decimal("87.95"),
        "a101_comparison_price": Decimal("216.3333"),
        "migros_comparison_price": Decimal("219.8750"),
    }

    display = build_public_result_display(recommendation, catalog_row)

    assert display["uses_normalized_price_compare"] is True
    assert display["price_display_unit"] == "kg"
    assert display["a101_shelf_price"] == Decimal("64.90")
    assert display["migros_shelf_price"] == Decimal("87.95")
    assert display["a101_display_price"] == Decimal("216.3333")
    assert display["migros_display_price"] == Decimal("219.8750")
    assert display["display_recommended_retailer"] == "a101"
    assert display["display_recommended_price"] == Decimal("216.3333")


def test_build_public_result_display_uses_kg_compare_for_same_quantity_cucumber():
    recommendation = {
        "standardized_product_name": "salatalik",
        "recommended_retailer": "a101",
        "recommended_price": Decimal("32.90"),
        "a101_price": Decimal("32.90"),
        "migros_price": Decimal("39.95"),
        "comparison_confidence": "high",
    }
    catalog_row = {
        "comparison_price_unit": "kg",
        "a101_raw_price": Decimal("32.90"),
        "migros_raw_price": Decimal("39.95"),
        "a101_comparison_price": Decimal("32.90"),
        "migros_comparison_price": Decimal("39.95"),
    }

    display = build_public_result_display(recommendation, catalog_row)

    assert display["uses_normalized_price_compare"] is True
    assert display["price_display_unit"] == "kg"
    assert display["a101_display_price"] == Decimal("32.90")
    assert display["migros_display_price"] == Decimal("39.95")


def test_build_public_result_display_keeps_raw_prices_without_normalized_compare():
    recommendation = {
        "standardized_product_name": "kestane mantar",
        "recommended_retailer": "migros",
        "recommended_price": Decimal("119.95"),
        "a101_price": None,
        "migros_price": Decimal("119.95"),
        "comparison_confidence": "single_source",
    }

    display = build_public_result_display(recommendation, None)

    assert display["uses_normalized_price_compare"] is False
    assert display["price_display_unit"] is None
    assert display["migros_shelf_price"] == Decimal("119.95")
    assert display["migros_display_price"] == Decimal("119.95")
    assert display["display_recommended_retailer"] == "migros"


def test_build_public_result_display_prefers_recommendation_shelf_price_over_catalog_fallback():
    recommendation = {
        "standardized_product_name": "domates",
        "recommended_retailer": "migros",
        "recommended_price": Decimal("109.95"),
        "a101_price": Decimal("119.50"),
        "migros_price": Decimal("109.95"),
        "comparison_confidence": "high",
    }
    catalog_row = {
        "a101_raw_price": Decimal("119.50"),
        "migros_raw_price": Decimal("139.95"),
    }

    display = build_public_result_display(recommendation, catalog_row)

    assert display["a101_shelf_price"] == Decimal("119.50")
    assert display["migros_shelf_price"] == Decimal("109.95")


def test_build_public_result_display_derives_litre_unit_price_from_shelf_price_and_quantity():
    recommendation = {
        "standardized_product_name": "fairy elma bulasik deterjani 1.5 l",
        "recommended_retailer": "a101",
        "recommended_price": Decimal("89.00"),
        "a101_price": Decimal("89.00"),
        "migros_price": Decimal("149.95"),
        "comparison_confidence": "high",
        "a101_normalized_unit": "liter",
        "migros_normalized_unit": "liter",
        "a101_normalized_quantity": Decimal("1.5"),
        "migros_normalized_quantity": Decimal("1.5"),
    }
    catalog_row = {
        "comparison_price_unit": "liter",
        "a101_raw_price": Decimal("89.00"),
        "migros_raw_price": Decimal("149.95"),
        "a101_normalized_unit": "liter",
        "migros_normalized_unit": "liter",
        "a101_normalized_quantity": Decimal("1.5"),
        "migros_normalized_quantity": Decimal("1.5"),
        # Existing bad catalog values should not win.
        "a101_comparison_price": Decimal("89.00"),
        "migros_comparison_price": Decimal("149.95"),
    }

    display = build_public_result_display(recommendation, catalog_row)

    assert display["uses_normalized_price_compare"] is True
    assert display["price_display_unit"] == "litre"
    assert round(display["a101_display_price"], 2) == 59.33
    assert round(display["migros_display_price"], 2) == 99.97
    assert display["a101_display_price"] != Decimal("89.00")
    assert display["display_recommended_retailer"] == "a101"


def test_build_public_result_display_uses_total_pack_quantity_for_water_multipack():
    recommendation = {
        "standardized_product_name": "hayat su 6x1.5 l",
        "recommended_retailer": "a101",
        "recommended_price": Decimal("60.00"),
        "a101_price": Decimal("60.00"),
        "migros_price": None,
        "comparison_confidence": "single_source",
        "a101_normalized_unit": "liter",
        "a101_normalized_quantity": Decimal("1.5"),
    }
    catalog_row = {
        "comparison_price_unit": "liter",
        "a101_raw_price": Decimal("60.00"),
        "a101_normalized_unit": "liter",
        "a101_normalized_quantity": Decimal("1.5"),
        "a101_source_product_name": "Hayat Su 6x1.5 L",
    }

    display = build_public_result_display(recommendation, catalog_row)

    assert display["uses_normalized_price_compare"] is True
    assert display["price_display_unit"] == "litre"
    assert round(display["a101_display_price"], 2) == 6.67
