import math


def _is_missing(value) -> bool:
    if value is None:
        return True

    try:
        return math.isnan(value)
    except (TypeError, ValueError):
        return False


def _infer_cheaper_source(a101_price, migros_price) -> str | None:
    if _is_missing(a101_price) and _is_missing(migros_price):
        return None
    if _is_missing(a101_price):
        return "migros"
    if _is_missing(migros_price):
        return "a101"
    if a101_price < migros_price:
        return "a101"
    if migros_price < a101_price:
        return "migros"
    return "same"


def build_public_result_display(
    recommendation: dict,
    catalog_row: dict | None = None,
) -> dict:
    display = {
        **recommendation,
        "display_recommended_retailer": recommendation.get("recommended_retailer"),
        "display_recommended_price": recommendation.get("recommended_price"),
        "a101_shelf_price": recommendation.get("a101_price"),
        "migros_shelf_price": recommendation.get("migros_price"),
        "a101_display_price": recommendation.get("a101_price"),
        "migros_display_price": recommendation.get("migros_price"),
        "price_display_unit": None,
        "uses_normalized_price_compare": False,
    }

    if not catalog_row:
        return display

    if recommendation.get("force_review"):
        return display

    comparison_unit = catalog_row.get("comparison_price_unit")
    a101_raw_price = catalog_row.get("a101_raw_price")
    migros_raw_price = catalog_row.get("migros_raw_price")
    a101_comparison_price = catalog_row.get("a101_comparison_price")
    migros_comparison_price = catalog_row.get("migros_comparison_price")

    if _is_missing(display.get("a101_shelf_price")) and not _is_missing(a101_raw_price):
        display["a101_shelf_price"] = a101_raw_price
    if _is_missing(display.get("migros_shelf_price")) and not _is_missing(migros_raw_price):
        display["migros_shelf_price"] = migros_raw_price

    if (
        _is_missing(comparison_unit)
        or _is_missing(a101_comparison_price)
        or _is_missing(migros_comparison_price)
    ):
        return display

    display["price_display_unit"] = comparison_unit
    display["a101_display_price"] = a101_comparison_price
    display["migros_display_price"] = migros_comparison_price
    display["uses_normalized_price_compare"] = True

    cheaper_source = _infer_cheaper_source(
        a101_comparison_price,
        migros_comparison_price,
    )
    display["display_recommended_retailer"] = cheaper_source
    if cheaper_source == "a101":
        display["display_recommended_price"] = a101_comparison_price
    elif cheaper_source == "migros":
        display["display_recommended_price"] = migros_comparison_price
    else:
        display["display_recommended_price"] = a101_comparison_price

    return display
