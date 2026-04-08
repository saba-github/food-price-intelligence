from typing import Any


def insert_stg_source_product(
    cursor,
    event_id: int,
    run_id: int,
    product: dict[str, Any],
    source_name: str,
):
    cursor.execute(
        """
        INSERT INTO stg_source_products
            (event_id, run_id, source_name,
             source_product_id, source_sku,
             raw_product_name, raw_category_name,
             product_url,
             shown_price, regular_price, discount_rate,
             unit, unit_amount)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            event_id,
            run_id,
            source_name,
            str(product["product_id"]) if product.get("product_id") else None,
            product.get("sku"),
            product.get("product_name"),
            product.get("category_name"),
            product.get("product_url"),
            product.get("shown_price_tl"),
            product.get("regular_price_tl"),
            product.get("discount_rate"),
            product.get("unit"),
            product.get("unit_amount"),
        ),
    )


def insert_stg_observation(
    cursor,
    event_id: int,
    run_id: int,
    product: dict[str, Any],
    transformed: dict[str, Any],
    source_name: str,
    currency: str,
) -> int:
    price = transformed["price"]
    normalized_unit = transformed["normalized_unit"]
    normalized_quantity = transformed["normalized_quantity"]
    price_per_unit = transformed["price_per_unit"]
    unit_price_label = transformed["unit_price_label"]
    standardized_name = transformed["standardized_product_name"]
    regular_price = transformed["regular_price"]
    discount_rate = transformed["discount_rate"]
    brand_name = transformed["brand_name"]
    category_name = transformed["category_name"]
    is_suspicious = transformed["is_suspicious"]
    suspicious_reason = transformed["suspicious_reason"]

    cursor.execute(
        """
        INSERT INTO stg_price_observations
            (event_id, run_id, source_name, source_product_id, source_sku,
             product_name, product_url, price, currency,
             normalized_unit, normalized_quantity, price_per_unit, unit_price_label,
             standardized_product_name,
             regular_price, discount_rate, brand_name, category_name,
             is_suspicious, suspicious_reason,
             observed_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        RETURNING observation_id
        """,
        (
            event_id,
            run_id,
            source_name,
            str(product["product_id"]) if product.get("product_id") is not None else None,
            product.get("sku"),
            product.get("product_name"),
            product.get("product_url"),
            price,
            currency,
            normalized_unit,
            normalized_quantity,
            price_per_unit,
            unit_price_label,
            standardized_name,
            regular_price,
            discount_rate,
            brand_name,
            category_name,
            is_suspicious,
            suspicious_reason,
        ),
    )
    return cursor.fetchone()[0]


def insert_stg_normalized_observation(
    cursor,
    event_id: int,
    run_id: int,
    product: dict[str, Any],
    transformed: dict[str, Any],
    source_name: str,
):
    cursor.execute(
        """
        INSERT INTO stg_normalized_observations
            (event_id, run_id, source_name, source_product_id,
             raw_product_name, standardized_product_name,
             normalized_unit, normalized_quantity,
             price, price_per_unit, unit_price_label,
             brand_name, category_name,
             is_suspicious, suspicious_reason)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            event_id,
            run_id,
            source_name,
            str(product["product_id"]) if product.get("product_id") else None,
            product.get("product_name"),
            transformed.get("standardized_product_name"),
            transformed.get("normalized_unit"),
            transformed.get("normalized_quantity"),
            transformed.get("price"),
            transformed.get("price_per_unit"),
            transformed.get("unit_price_label"),
            transformed.get("brand_name"),
            transformed.get("category_name"),
            transformed.get("is_suspicious"),
            transformed.get("suspicious_reason"),
        ),
    )
