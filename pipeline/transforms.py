import re
from typing import Optional, Tuple, Any


# --------------------------------------------------
# UNIT NORMALIZATION
# --------------------------------------------------
def normalize_unit(
    unit: Optional[str], quantity: Any
) -> Tuple[Optional[str], Optional[float]]:
    if unit is None:
        return None, None

    unit_upper = str(unit).strip().upper()

    qty = None
    try:
        if quantity is not None:
            qty = float(quantity)
    except:
        qty = None

    if unit_upper == "GRAM":
        if qty is None:
            return "kg", None
        return "kg", round(qty / 1000, 4)

    if unit_upper == "PIECE":
        return "piece", qty if qty else 1.0

    return unit.lower(), qty


# --------------------------------------------------
# PRODUCT NAME STANDARDIZATION
# --------------------------------------------------
def standardize_product_name(name: str) -> str:
    if not name:
        return ""

    name = name.lower()

    replacements = {
        "ç": "c",
        "ğ": "g",
        "ı": "i",
        "ö": "o",
        "ş": "s",
        "ü": "u",
    }

    for k, v in replacements.items():
        name = name.replace(k, v)

    name = re.sub(r"\d+", "", name)
    name = re.sub(r"[^\w\s]", "", name)

    stopwords = [
        "kg", "g", "gram", "adet", "paket", "demet",
        "file", "kutu"
    ]

    words = [
        w for w in name.split()
        if w not in stopwords
    ]

    return " ".join(words).strip()


# --------------------------------------------------
# PRICE PER UNIT
# --------------------------------------------------
def calculate_price_per_unit(price: float, quantity: float) -> Optional[float]:
    if price is None or quantity in (None, 0):
        return None
    return round(price / quantity, 4)


# --------------------------------------------------
# MAIN TRANSFORM
# --------------------------------------------------
def transform_product(product: dict) -> dict:

    unit = product.get("unit")
    quantity = product.get("quantity")
    price = product.get("price")
    name = product.get("product_name")

    normalized_unit, normalized_quantity = normalize_unit(unit, quantity)

    standardized_name = standardize_product_name(name)

    price_per_unit = calculate_price_per_unit(price, normalized_quantity)

    return {
        "standardized_product_name": standardized_name,
        "normalized_unit": normalized_unit,
        "normalized_quantity": normalized_quantity,
        "price_per_unit": price_per_unit,
    }
