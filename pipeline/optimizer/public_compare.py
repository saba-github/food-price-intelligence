import math
import re


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


_DISPLAY_UNIT_LABELS = {
    "liter": "litre",
    "litre": "litre",
    "kg": "kg",
    "piece": "adet",
    "roll": "rulo",
}
_PACK_MEASUREMENT_PATTERN = re.compile(
    r"\b(?P<count>\d+)\s*x\s*(?P<size>\d+(?:[.,]\d+)?)\s*(?P<unit>ml|l|kg|g)\b",
    flags=re.IGNORECASE,
)


def _to_float(value):
    if _is_missing(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_display_unit(unit: str | None) -> str | None:
    if _is_missing(unit):
        return None
    return _DISPLAY_UNIT_LABELS.get(str(unit), str(unit))


def _effective_pack_quantity(
    source_name,
    normalized_unit,
    normalized_quantity,
):
    if _is_missing(source_name):
        return normalized_unit, normalized_quantity

    match = _PACK_MEASUREMENT_PATTERN.search(str(source_name))
    if not match:
        return normalized_unit, normalized_quantity

    pack_count = int(match.group("count"))
    pack_size = float(match.group("size").replace(",", "."))
    pack_unit = match.group("unit").lower()

    total_unit = None
    total_quantity = None
    if pack_unit == "ml":
        total_unit = "liter"
        total_quantity = pack_count * pack_size / 1000.0
    elif pack_unit == "l":
        total_unit = "liter"
        total_quantity = pack_count * pack_size
    elif pack_unit == "g":
        total_unit = "kg"
        total_quantity = pack_count * pack_size / 1000.0
    elif pack_unit == "kg":
        total_unit = "kg"
        total_quantity = pack_count * pack_size

    quantity_value = _to_float(normalized_quantity)
    if (
        total_unit
        and total_quantity is not None
        and normalized_unit == total_unit
        and (quantity_value is None or total_quantity > quantity_value)
    ):
        return total_unit, total_quantity

    return normalized_unit, normalized_quantity


def _derived_unit_price(raw_price, normalized_unit, normalized_quantity):
    raw_price_value = _to_float(raw_price)
    quantity_value = _to_float(normalized_quantity)
    display_unit = _normalize_display_unit(normalized_unit)
    if raw_price_value is None or quantity_value is None or quantity_value <= 0 or not display_unit:
        return None, None
    return raw_price_value / quantity_value, display_unit


def _select_display_price(
    raw_price,
    normalized_unit,
    normalized_quantity,
    comparison_price,
    comparison_unit,
):
    derived_price, derived_unit = _derived_unit_price(
        raw_price,
        normalized_unit,
        normalized_quantity,
    )
    if derived_price is not None and derived_unit is not None:
        return derived_price, derived_unit

    fallback_unit = _normalize_display_unit(comparison_unit)
    if comparison_price is None or not fallback_unit:
        return None, None
    return comparison_price, fallback_unit


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
    a101_normalized_unit = catalog_row.get("a101_normalized_unit") or recommendation.get("a101_normalized_unit")
    migros_normalized_unit = catalog_row.get("migros_normalized_unit") or recommendation.get("migros_normalized_unit")
    a101_normalized_quantity = catalog_row.get("a101_normalized_quantity") or recommendation.get("a101_normalized_quantity")
    migros_normalized_quantity = catalog_row.get("migros_normalized_quantity") or recommendation.get("migros_normalized_quantity")
    a101_source_name = catalog_row.get("a101_source_product_name") or recommendation.get("a101_source_product_name")
    migros_source_name = catalog_row.get("migros_source_product_name") or recommendation.get("migros_source_product_name")

    a101_normalized_unit, a101_normalized_quantity = _effective_pack_quantity(
        a101_source_name,
        a101_normalized_unit,
        a101_normalized_quantity,
    )
    migros_normalized_unit, migros_normalized_quantity = _effective_pack_quantity(
        migros_source_name,
        migros_normalized_unit,
        migros_normalized_quantity,
    )

    if _is_missing(display.get("a101_shelf_price")) and not _is_missing(a101_raw_price):
        display["a101_shelf_price"] = a101_raw_price
    if _is_missing(display.get("migros_shelf_price")) and not _is_missing(migros_raw_price):
        display["migros_shelf_price"] = migros_raw_price

    a101_display_price, a101_display_unit = _select_display_price(
        display.get("a101_shelf_price"),
        a101_normalized_unit,
        a101_normalized_quantity,
        a101_comparison_price,
        comparison_unit,
    )
    migros_display_price, migros_display_unit = _select_display_price(
        display.get("migros_shelf_price"),
        migros_normalized_unit,
        migros_normalized_quantity,
        migros_comparison_price,
        comparison_unit,
    )

    price_display_unit = None
    if a101_display_unit and migros_display_unit and a101_display_unit == migros_display_unit:
        price_display_unit = a101_display_unit
    elif a101_display_unit and _is_missing(display.get("migros_shelf_price")):
        price_display_unit = a101_display_unit
    elif migros_display_unit and _is_missing(display.get("a101_shelf_price")):
        price_display_unit = migros_display_unit

    if not price_display_unit:
        return display

    display["price_display_unit"] = price_display_unit
    if a101_display_price is not None and a101_display_unit == price_display_unit:
        display["a101_display_price"] = a101_display_price
    if migros_display_price is not None and migros_display_unit == price_display_unit:
        display["migros_display_price"] = migros_display_price
    display["uses_normalized_price_compare"] = True

    cheaper_source = _infer_cheaper_source(
        display.get("a101_display_price"),
        display.get("migros_display_price"),
    )
    display["display_recommended_retailer"] = cheaper_source
    if cheaper_source == "a101":
        display["display_recommended_price"] = display.get("a101_display_price")
    elif cheaper_source == "migros":
        display["display_recommended_price"] = display.get("migros_display_price")
    else:
        display["display_recommended_price"] = display.get("a101_display_price")

    return display
