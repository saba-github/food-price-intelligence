import os
import re
from typing import Any

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

from scraper.migros.categories import get_migros_category_products

load_dotenv()


DEFAULT_CATEGORY_SLUG = "meyve-sebze-c-2"
SOURCE_NAME = "migros"
CURRENCY = "TRY"


def get_connection():
    return psycopg2.connect(
        host=os.getenv("DATABASE_HOST"),
        database=os.getenv("DATABASE_NAME"),
        user=os.getenv("DATABASE_USER"),
        password=os.getenv("DATABASE_PASSWORD"),
        port=os.getenv("DATABASE_PORT", "5432")
    )


def start_run(cursor):
    cursor.execute("""
        INSERT INTO scrape_runs (source_name, status)
        VALUES (%s, 'running')
        RETURNING run_id
    """, (SOURCE_NAME,))
    return cursor.fetchone()[0]


def finish_run(cursor, run_id: int, records: int):
    cursor.execute("""
        UPDATE scrape_runs
        SET finished_at = NOW(),
            status = 'success',
            records_scraped = %s
        WHERE run_id = %s
    """, (records, run_id))


def fail_run(cursor, run_id: int, error_message: str):
    cursor.execute("""
        UPDATE scrape_runs
        SET finished_at = NOW(),
            status = 'failed',
            error_message = %s
        WHERE run_id = %s
    """, (error_message[:5000], run_id))


def normalize_unit(unit: str | None, quantity: Any) -> tuple[str | None, float | None]:
    """
    Kaynak birimini daha analize uygun hale getirir.

    Örnek:
    - GRAM + 1000 -> ("kg", 1.0)
    - GRAM + 400  -> ("g", 400.0)
    - PIECE + 1   -> ("piece", 1.0)
    """
    if unit is None:
        return None, None

    unit_upper = str(unit).strip().upper()

    try:
        qty = float(quantity) if quantity is not None else None
    except (TypeError, ValueError):
        qty = None

    if unit_upper == "GRAM":
        if qty == 1000:
            return "kg", 1.0
        return "g", qty

    if unit_upper == "PIECE":
        return "piece", qty if qty is not None else 1.0

    return unit.lower(), qty


def standardize_product_name(product_name: str | None) -> str | None:
    """
    Ürün adını karşılaştırma ve grouping için sadeleştirir.
    Tam mükemmel canonicalization değil; ilk sağlam katman.
    """
    if not product_name:
        return None

    name = product_name.lower().strip()

    replacements = {
        "ı": "i",
        "ğ": "g",
        "ü": "u",
        "ş": "s",
        "ö": "o",
        "ç": "c",
    }

    for old, new in replacements.items():
        name = name.replace(old, new)

    # Yaygın birim/ambalaj kelimelerini sadeleştir
    removable_patterns = [
        r"\bkg\b",
        r"\bgram\b",
        r"\bg\b",
        r"\badet\b",
        r"\bdemet\b",
        r"\bpaket\b",
    ]

    for pattern in removable_patterns:
        name = re.sub(pattern, " ", name)

    # Çoklu boşlukları temizle
    name = re.sub(r"\s+", " ", name).strip()

    return name or None


def insert_raw_event(cursor, run_id: int, product: dict[str, Any], category_slug: str) -> int:
    """
    Scraper'dan gelen ham ürünü raw tabloya yazar.
    event_id döndürür.
    """
    raw_payload = {
        "category_slug": category_slug,
        **product
    }

    cursor.execute("""
        INSERT INTO raw_price_events
        (
            run_id,
            source_name,
            product_name,
            product_url,
            price,
            currency,
            raw_payload
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING event_id
    """, (
        run_id,
        SOURCE_NAME,
        product.get("product_name"),
        product.get("product_url"),
        product.get("shown_price_tl"),
        CURRENCY,
        psycopg2.extras.Json(raw_payload)
    ))

    return cursor.fetchone()[0]


def insert_stg_observation(
    cursor,
    event_id: int,
    run_id: int,
    product: dict[str, Any],
) -> int:
    """
    Raw event'ten normalize edilmiş staging kaydını oluşturur.
    observation_id döndürür.
    """
    normalized_unit, normalized_quantity = normalize_unit(
        product.get("unit"),
        product.get("unit_amount")
    )

    standardized_name = standardize_product_name(product.get("product_name"))

    cursor.execute("""
        INSERT INTO stg_price_observations
        (
            event_id,
            run_id,
            source_name,
            source_product_id,
            source_sku,
            product_name,
            product_url,
            price,
            currency,
            normalized_unit,
            normalized_quantity,
            standardized_product_name,
            observed_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        RETURNING observation_id
    """, (
        event_id,
        run_id,
        SOURCE_NAME,
        str(product.get("product_id")) if product.get("product_id") is not None else None,
        product.get("sku"),
        product.get("product_name"),
        product.get("product_url"),
        product.get("shown_price_tl"),
        CURRENCY,
        normalized_unit,
        normalized_quantity,
        standardized_name,
    ))

    return cursor.fetchone()[0]


def insert_fact_observation(
    cursor,
    observation_id: int,
    run_id: int,
    product: dict[str, Any],
):
    """
    Analize hazır fact kaydını oluşturur.
    """
    normalized_unit, normalized_quantity = normalize_unit(
        product.get("unit"),
        product.get("unit_amount")
    )

    standardized_name = standardize_product_name(product.get("product_name"))

    cursor.execute("""
        INSERT INTO fact_price_observations
        (
            observation_id,
            run_id,
            source_name,
            source_product_id,
            source_sku,
            product_name,
            standardized_product_name,
            product_url,
            normalized_unit,
            normalized_quantity,
            price,
            currency,
            observed_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
    """, (
        observation_id,
        run_id,
        SOURCE_NAME,
        str(product.get("product_id")) if product.get("product_id") is not None else None,
        product.get("sku"),
        product.get("product_name"),
        standardized_name,
        product.get("product_url"),
        normalized_unit,
        normalized_quantity,
        product.get("shown_price_tl"),
        CURRENCY,
    ))


def run_pipeline(category_slug: str = DEFAULT_CATEGORY_SLUG):
    conn = get_connection()
    cursor = conn.cursor()

    run_id = None

    try:
        run_id = start_run(cursor)
        conn.commit()

        scraped_products = get_migros_category_products(category_slug)

        raw_count = 0
        stg_count = 0
        fact_count = 0

        for product in scraped_products:
            event_id = insert_raw_event(
                cursor=cursor,
                run_id=run_id,
                product=product,
                category_slug=category_slug
            )
            raw_count += 1

            observation_id = insert_stg_observation(
                cursor=cursor,
                event_id=event_id,
                run_id=run_id,
                product=product
            )
            stg_count += 1

            insert_fact_observation(
                cursor=cursor,
                observation_id=observation_id,
                run_id=run_id,
                product=product
            )
            fact_count += 1

        finish_run(cursor, run_id, raw_count)
        conn.commit()

        print(f"Pipeline finished successfully.")
        print(f"Run ID: {run_id}")
        print(f"Category: {category_slug}")
        print(f"Raw rows inserted: {raw_count}")
        print(f"Staging rows inserted: {stg_count}")
        print(f"Fact rows inserted: {fact_count}")

    except Exception as e:
        conn.rollback()

        if run_id is not None:
            try:
                fail_run(cursor, run_id, str(e))
                conn.commit()
            except Exception:
                conn.rollback()

        print(f"Pipeline failed: {e}")

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    run_pipeline(DEFAULT_CATEGORY_SLUG)
