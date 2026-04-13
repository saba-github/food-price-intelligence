import logging
from typing import Any, Optional, Tuple

logger = logging.getLogger(__name__)


def can_insert_to_fact(transformed: dict[str, Any]) -> Tuple[bool, Optional[str]]:
    if transformed.get("is_suspicious"):
        return False, "suspicious_record"

    required_fields = {
        "price": transformed.get("price"),
        "normalized_unit": transformed.get("normalized_unit"),
        "normalized_quantity": transformed.get("normalized_quantity"),
        "price_per_unit": transformed.get("price_per_unit"),
        "standardized_product_name": transformed.get("standardized_product_name"),
        "category_name": transformed.get("category_name"),
    }

    for field_name, value in required_fields.items():
        if value is None:
            return False, f"missing_{field_name}"

    if transformed["price"] <= 0:
        return False, "invalid_price"

    if transformed["normalized_quantity"] <= 0:
        return False, "invalid_normalized_quantity"

    return True, None


def insert_fact_observation(
    cursor,
    observation_id: int,
    run_id: int,
    product: dict[str, Any],
    transformed: dict[str, Any],
    product_id: int,
    source_name: str,
) -> bool:
    can_insert, reason = can_insert_to_fact(transformed)

    if not can_insert:
        logger.info(
            (
                "DEBUG - FACT SKIP | product=%r | reason=%s | "
                "price=%r | normalized_unit=%r | normalized_quantity=%r | "
                "price_per_unit=%r | standardized_product_name=%r | category_name=%r"
            ),
            product.get("product_name"),
            reason,
            transformed.get("price"),
            transformed.get("normalized_unit"),
            transformed.get("normalized_quantity"),
            transformed.get("price_per_unit"),
            transformed.get("standardized_product_name"),
            transformed.get("category_name"),
        )
        return False

    observed_at = product.get("scraped_at")
    if observed_at is None:
        cursor.execute("SELECT NOW()")
        observed_at = cursor.fetchone()[0]

    source_product_id = (
        str(product["product_id"]) if product.get("product_id") is not None else None
    )

    cursor.execute(
        """
        SELECT event_id
        FROM stg_price_observations
        WHERE observation_id = %s
        LIMIT 1
        """,
        (observation_id,),
    )
    event_row = cursor.fetchone()

    if not event_row or event_row[0] is None:
        raise ValueError(
            f"Could not resolve event_id for observation_id={observation_id}"
        )

    event_id = event_row[0]

    cursor.execute(
        """
        INSERT INTO fact_price_observations (
            event_id,
            observation_id,
            run_id,
            source_name,
            source_product_id,
            product_id,
            product_name,
            standardized_product_name,
            product_url,
            price,
            regular_price,
            discount_rate,
            currency,
            normalized_unit,
            normalized_quantity,
            price_per_unit,
            unit_price_label,
            brand_name,
            category_name,
            observed_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING
        """,
        (
            event_id,
            observation_id,
            run_id,
            source_name,
            source_product_id,
            product_id,
            product.get("product_name"),
            transformed["standardized_product_name"],
            product.get("product_url"),
            transformed["price"],
            transformed["regular_price"],
            transformed["discount_rate"],
            transformed["currency"],
            transformed["normalized_unit"],
            transformed["normalized_quantity"],
            transformed["price_per_unit"],
            transformed["unit_price_label"],
            transformed["brand_name"],
            transformed["category_name"],
            observed_at,
        ),
    )

    if cursor.rowcount != 1:
        logger.info(
            "DEBUG - FACT NOT INSERTED | product=%r | reason=conflict_or_no_insert | event_id=%r",
            product.get("product_name"),
            event_id,
        )

    return cursor.rowcount == 1
