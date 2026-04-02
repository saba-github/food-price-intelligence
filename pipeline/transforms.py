import re
from typing import Any, Optional, Tuple


def normalize_unit(
    unit: Optional[str], quantity: Any
) -> Tuple[Optional[str], Optional[float]]:
    if unit is None:
        return None, None

    unit_upper = str(unit).strip().upper()

    qty: Optional[float] = None
    if quantity is not None:
        try:
            qty = float(quantity)
        except (TypeError, ValueError):
            qty = None

    if unit_upper == "GRAM":
        if qty is None:
            return "kg", None
        return "kg", round(qty / 1000, 4)

    if unit_upper == "PIECE":
        return "piece", qty if qty is not None else 1.0

    return unit.lower(), qty


def standardize_product_name(product_name: Optional[str]) -> Optional[str]:
    if not product_name:
        return None

    name = product_name.lower().strip()

    tr_map = {"ı": "i", "ğ": "g", "ü": "u", "ş": "s", "ö": "o", "ç": "c"}
    for old, new in tr_map.items():
        name = name.replace(old, new)

    name = re.sub(r"\b\d+\s*(?:kg|g|gram|ml|l|lt|adet|demet|paket)\b", " ", name)

    standalone_units = r"\b(?:kg|gram|adet|demet|paket)\b"
    name = re.sub(standalone_units, " ", name)

    name = re.sub(r"\s+", " ", name).strip()

    return name or None


def calculate_price_per_unit(
    price: Optional[float], normalized_quantity: Optional[float]
) -> Optional[float]:
    if price is None or normalized_quantity is None:
        return None

    try:
        price_value = float(price)
        quantity_value = float(normalized_quantity)
    except (TypeError, ValueError):
        return None

    if quantity_value <= 0:
        return None

    return round(price_value / quantity_value, 4)


def build_unit_price_label(normalized_unit: Optional[str]) -> Optional[str]:
    if normalized_unit is None:
        return None
    return f"TRY/{normalized_unit}"


def detect_suspicious(
    product_name: Optional[str], price: Optional[float]
) -> Tuple[bool, Optional[str]]:
    name = (product_name or "").lower()

    if price is None:
        return True, "price_null"

    if price <= 0:
        return True, "price_invalid"

    if price > 500:
        return True, "price_too_high"

    has_small_package_hint = (
        " gr" in name
        or "g paket" in name
        or " g paket" in name
        or "paket" in name
    )

    if has_small_package_hint and price > 200:
        return True, "small_package_price_too_high"

    return False, None


def transform_product(product: dict[str, Any]) -> dict[str, Any]:
    price = product.get("shown_price_tl")
    regular_price = product.get("regular_price_tl")

    unit = product.get("unit")
    unit_amount = product.get("unit_amount")

    normalized_unit, normalized_quantity = normalize_unit(unit, unit_amount)
    price_per_unit = calculate_price_per_unit(price, normalized_quantity)
    unit_price_label = build_unit_price_label(normalized_unit)
    standardized_product_name = standardize_product_name(product.get("product_name"))

    is_suspicious, suspicious_reason = detect_suspicious(
        product.get("product_name"),
        price,
    )

    discount_rate = product.get("discount_rate")
    brand_name = product.get("brand_name")
    category_name = product.get("category_name")

    return {
        "price": price,
        "regular_price": regular_price,
        "currency": "TRY",
        "normalized_unit": normalized_unit,
        "normalized_quantity": normalized_quantity,
        "price_per_unit": price_per_unit,
        "unit_price_label": unit_price_label,
        "standardized_product_name": standardized_product_name,
        "is_suspicious": is_suspicious,
        "suspicious_reason": suspicious_reason,
        "brand_name": brand_name,
        "category_name": category_name,
        "discount_rate": discount_rate,
    }
