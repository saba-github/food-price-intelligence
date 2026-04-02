from pipeline.transforms import (
    normalize_unit,
    standardize_product_name,
    calculate_price_per_unit,
    detect_suspicious,
    transform_product,
)


def test_normalize_unit_gram_to_kg():
    unit, qty = normalize_unit("GRAM", 400)
    assert unit == "kg"
    assert qty == 0.4


def test_normalize_unit_piece_default():
    unit, qty = normalize_unit("PIECE", None)
    assert unit == "piece"
    assert qty == 1.0


def test_standardize_product_name():
    result = standardize_product_name("Kültür Mantarı 400 G Paket")
    assert result == "kultur mantari"


def test_calculate_price_per_unit():
    result = calculate_price_per_unit(80.0, 0.4)
    assert result == 200.0


def test_detect_suspicious_price_too_high():
    is_suspicious, reason = detect_suspicious("Domates", 600)
    assert is_suspicious is True
    assert reason == "price_too_high"


def test_detect_suspicious_valid_product():
    is_suspicious, reason = detect_suspicious("Domates", 50)
    assert is_suspicious is False
    assert reason is None


def test_transform_product():
    product = {
        "product_name": "Kültür Mantarı 400 G Paket",
        "shown_price_tl": 80.0,
        "regular_price_tl": 90.0,
        "unit": "GRAM",
        "unit_amount": 400,
        "discount_rate": 0.1111,
        "brand_name": "Migros",
        "category_name": "Mantar",
    }

    transformed = transform_product(product)

    assert transformed["standardized_product_name"] == "kultur mantari"
    assert transformed["normalized_unit"] == "kg"
    assert transformed["normalized_quantity"] == 0.4
    assert transformed["price_per_unit"] == 200.0
    assert transformed["unit_price_label"] == "TRY/kg"
    assert transformed["brand_name"] == "Migros"
    assert transformed["category_name"] == "Mantar"
