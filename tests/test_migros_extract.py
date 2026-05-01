from scraper.migros.extract import parse_migros_products
from pipeline.transforms import transform_product


def test_parse_migros_kg_fresh_produce_preserves_unit_fields():
    api_json = {
        "data": [
            {
                "id": "123",
                "storeId": "20000000000607",
                "sku": "123",
                "prettyName": "hiyar-kg-p-123",
                "name": "Hiyar Kg",
                "unit": "GRAM",
                "unitAmount": 1000,
                "shownPrice": 3995,
                "regularPrice": 3995,
                "discountRate": 0,
                "alternativeUnit": "PIECE",
                "alternativeUnitValue": 100,
                "category": {"id": "1", "name": "Sebze", "prettyName": "sebze"},
                "categoryAscendants": [{"id": "1", "name": "Sebze", "prettyName": "sebze"}],
                "brand": {},
                "images": [],
                "badges": [],
                "socialProofInfo": {},
            }
        ]
    }

    products = parse_migros_products(api_json)

    assert len(products) == 1
    assert products[0]["product_name"] == "Hiyar Kg"
    assert products[0]["shown_price_tl"] == 39.95
    assert products[0]["unit"] == "GRAM"
    assert products[0]["unit_amount"] == 1000


def test_transform_product_normalizes_migros_hiyar_kg_to_salatalik_kg():
    product = {
        "product_name": "Hiyar Kg",
        "shown_price_tl": 39.95,
        "regular_price_tl": 39.95,
        "unit": "GRAM",
        "unit_amount": 1000,
        "discount_rate": 0,
        "brand_name": "Migros",
        "category_name": "Sebze",
    }

    transformed = transform_product(product)

    assert transformed["standardized_product_name"] == "salatalik"
    assert transformed["normalized_unit"] == "kg"
    assert transformed["normalized_quantity"] == 1.0
    assert transformed["price_per_unit"] == 39.95
