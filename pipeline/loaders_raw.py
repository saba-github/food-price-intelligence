import hashlib
from typing import Any

import psycopg2.extras


def insert_raw_event(
    cursor,
    run_id: int,
    product: dict[str, Any],
    category_slug: str,
    source_name: str,
    currency: str,
) -> int:
    raw_payload = {"category_slug": category_slug, **product}

    raw_hash_source = (
        f"{product.get('product_id')}|"
        f"{product.get('sku')}|"
        f"{product.get('product_name')}|"
        f"{product.get('shown_price_tl')}|"
        f"{category_slug}"
    )
    raw_hash = hashlib.md5(raw_hash_source.encode("utf-8")).hexdigest()

    scraped_at = product.get("scraped_at")
    if scraped_at is None:
        cursor.execute("SELECT NOW()")
        scraped_at = cursor.fetchone()[0]

    source_product_id = (
        str(product["product_id"]) if product.get("product_id") is not None else None
    )

    cursor.execute(
        """
        INSERT INTO raw_price_events
            (run_id, source_name, source_product_id, source_sku, category_slug,
             product_name, product_url, price, currency, scraped_at, raw_hash, raw_payload)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING event_id
        """,
        (
            run_id,
            source_name,
            source_product_id,
            product.get("sku"),
            category_slug,
            product.get("product_name"),
            product.get("product_url"),
            product.get("shown_price_tl"),
            currency,
            scraped_at,
            raw_hash,
            psycopg2.extras.Json(raw_payload),
        ),
    )
    row = cursor.fetchone()
    if row:
        return row[0]

    cursor.execute(
        """
        SELECT event_id
        FROM raw_price_events
        WHERE source_name = %s
          AND raw_hash = %s
        LIMIT 1
        """,
        (
            source_name,
            raw_hash,
        ),
    )
    existing_row = cursor.fetchone()
    if not existing_row:
        raise ValueError("Could not get event_id from raw_price_events after conflict.")
    return existing_row[0]
