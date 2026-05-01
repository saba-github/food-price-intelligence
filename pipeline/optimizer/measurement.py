import math
from decimal import Decimal, InvalidOperation


COMPARISON_STATUS_LABELS = {
    "safe": "Güvenli karşılaştırma",
    "unit_mismatch": "Ölçü uyumsuz",
    "quantity_mismatch": "Kontrol gerekli",
    "review": "Kontrol gerekli",
    "single_source": "Tek markette var",
}


def _format_number(value) -> str:
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return "-"

    if numeric_value.is_integer():
        return str(int(numeric_value))

    return f"{numeric_value:.3f}".rstrip("0").rstrip(".")


def _is_missing(value) -> bool:
    if value is None:
        return True

    try:
        if math.isnan(value):
            return True
    except (TypeError, ValueError):
        pass

    return str(value).strip().lower() in {"", "nan", "none", "<na>", "nat"}


def _is_true(value) -> bool:
    if _is_missing(value):
        return False
    return value is True or value == True or str(value).strip().lower() == "true"


def _is_false(value) -> bool:
    if _is_missing(value):
        return False
    return value is False or value == False or str(value).strip().lower() == "false"


def _has_package_hint(source_product_name: str | None) -> bool:
    if _is_missing(source_product_name):
        return False

    name = str(source_product_name).lower()
    return any(hint in name for hint in ["paket", "file", "tabak"])


def format_measurement_label(
    normalized_quantity,
    normalized_unit: str | None,
    source_product_name: str | None = None,
) -> str:
    if _is_missing(normalized_quantity) or _is_missing(normalized_unit):
        return "-"

    unit = str(normalized_unit).strip().lower()

    try:
        quantity = Decimal(str(normalized_quantity))
    except (InvalidOperation, ValueError):
        return "-"

    if quantity.is_nan():
        return "-"

    package_suffix = " paket" if _has_package_hint(source_product_name) else ""

    if unit == "kg" and quantity < 1:
        grams = quantity * Decimal("1000")
        return f"{_format_number(grams)} g{package_suffix}"

    if unit in {"g", "gram"}:
        return f"{_format_number(quantity)} g{package_suffix}"

    if unit == "kg":
        return f"{_format_number(quantity)} kg{package_suffix}"

    if unit in {"piece", "adet", "ad"}:
        return f"{_format_number(quantity)} adet"

    if unit == "roll":
        return f"{_format_number(quantity)} rulo"

    if unit in {"demet", "bunch"}:
        return f"{_format_number(quantity)} demet"

    if unit in {"l", "lt", "liter", "litre"}:
        return f"{_format_number(quantity)} l"

    if unit == "ml":
        return f"{_format_number(quantity)} ml"

    return f"{_format_number(quantity)} {unit}"


def add_measurement_labels(row: dict) -> dict:
    return {
        **row,
        "a101_measurement_label": format_measurement_label(
            row.get("a101_normalized_quantity"),
            row.get("a101_normalized_unit"),
            row.get("a101_source_product_name"),
        ),
        "migros_measurement_label": format_measurement_label(
            row.get("migros_normalized_quantity"),
            row.get("migros_normalized_unit"),
            row.get("migros_source_product_name"),
        ),
    }


def get_comparison_status_label(row: dict) -> str:
    if row.get("comparison_confidence") == "single_source":
        return COMPARISON_STATUS_LABELS["single_source"]

    if _is_true(row.get("force_review")):
        return COMPARISON_STATUS_LABELS["review"]

    if (
        row.get("comparison_confidence") == "high"
        and _is_true(row.get("same_unit_flag"))
        and _is_true(row.get("same_quantity_flag"))
    ):
        return COMPARISON_STATUS_LABELS["safe"]

    if _is_false(row.get("same_unit_flag")):
        return COMPARISON_STATUS_LABELS["unit_mismatch"]

    if _is_false(row.get("same_quantity_flag")):
        return COMPARISON_STATUS_LABELS["quantity_mismatch"]

    return COMPARISON_STATUS_LABELS["review"]


def get_measurement_mismatch_label(row: dict) -> str | None:
    if str(row.get("coverage_status")).strip().lower() == "comparable":
        return None

    if _is_true(row.get("comparison_safe")):
        return None

    if (
        row.get("comparison_confidence") == "high"
        and _is_true(row.get("same_unit_flag"))
        and _is_true(row.get("same_quantity_flag"))
    ):
        return None

    a101_label = row.get("a101_measurement_label") or format_measurement_label(
        row.get("a101_normalized_quantity"),
        row.get("a101_normalized_unit"),
        row.get("a101_source_product_name"),
    )
    migros_label = row.get("migros_measurement_label") or format_measurement_label(
        row.get("migros_normalized_quantity"),
        row.get("migros_normalized_unit"),
        row.get("migros_source_product_name"),
    )

    if a101_label == "-" and migros_label == "-":
        return None

    reason = row.get("comparison_review_reason")
    reason_suffix = f" ({reason})" if reason else ""
    return f"{a101_label} vs {migros_label}{reason_suffix}"
