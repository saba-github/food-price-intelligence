import hashlib
import logging
import os
from typing import Any

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

from scraper.migros.categories import get_migros_category_products

from pipeline.transforms import transform_product

load_dotenv()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEFAULT_CATEGORY_SLUG = "meyve-sebze-c-2"
SOURCE_NAME = "migros"
CURRENCY = "TRY"


# ---------------------------------------------------------------------------
# DB connection
# ---------------------------------------------------------------------------
def get_connection():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is not set.")
    return psycopg2.connect(database_url)


# ---------------------------------------------------------------------------
# Run lifecycle
# ---------------------------------------------------------------------------
def start_run(cursor, category_slug: str) -> int:
    cursor.execute(
        """
        INSERT INTO scrape_runs (
            source_name,
            category_slug,
            status,
            triggered_by,
            pipeline_version
        )
        VALUES (%s, %s, 'running', %s, %s)
        RETURNING run_id
        """,
        (
            SOURCE_NAME,
            category_slug,
            "github_actions",
            "v2-prep-001",
        ),
    )
    run_id = cursor.fetchone()[0]
    logger.info("Run started — run_id=%s  category=%s", run_id, category_slug)
    return run_id

def finish_run(
    cursor,
    run_id: int,
    records_scraped: int,
    records_raw: int,
    records_stg: int,
    records_fact: int,
    records_suspicious: int,
    records_failed: int,
):
    cursor.execute(
        """
        UPDATE scrape_runs
        SET finished_at = NOW(),
            status = 'success',
            records_scraped = %s,
            records_raw = %s,
            records_stg = %s,
            records_fact = %s,
            records_suspicious = %s,
            records_failed = %s
        WHERE run_id = %s
        """,
        (
            records_scraped,
            records_raw,
            records_stg,
            records_fact,
            records_suspicious,
            records_failed,
            run_id,
        ),
    )


def fail_run(cursor, run_id: int, error_message: str):
    cursor.execute(
        """
        UPDATE scrape_runs
        SET finished_at = NOW(),
            status = 'failed',
            error_message = %s
        WHERE run_id = %s
        """,
        (error_message[:5000], run_id),
    )
    logger.error("Run failed — run_id=%s  error=%s", run_id, error_message[:200])

def log_quality_check(
    cursor,
    run_id: int,
    check_name: str,
    check_status: str,
    observed_value: float | int | None = None,
    threshold_value: float | int | None = None,
    details: str | None = None,
):
    cursor.execute(
        """
        insert into ops_data_quality_results
            (run_id, check_name, check_status, observed_value, threshold_value, details)
        values (%s, %s, %s, %s, %s, %s)
        """,
        (run_id, check_name, check_status, observed_value, threshold_value, details),
    )

# ---------------------------------------------------------------------------
# Insert helpers
# ---------------------------------------------------------------------------
def insert_raw_event(
    cursor, run_id: int, product: dict[str, Any], category_slug: str
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

    cursor.execute(
        """
        INSERT INTO raw_price_events
            (run_id, source_name, source_product_id, source_sku, category_slug,
             product_name, product_url, price, currency, raw_hash, raw_payload)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING event_id
        """,
        (
            run_id,
            SOURCE_NAME,
            str(product["product_id"]) if product.get("product_id") is not None else None,
            product.get("sku"),
            category_slug,
            product.get("product_name"),
            product.get("product_url"),
            product.get("shown_price_tl"),
            CURRENCY,
            raw_hash,
            psycopg2.extras.Json(raw_payload),
        ),
    )
    return cursor.fetchone()[0]

def insert_stg_source_product(
    cursor,
    event_id: int,
    run_id: int,
    product: dict[str, Any],
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
            SOURCE_NAME,
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
    cursor, event_id: int, run_id: int, product: dict[str, Any], transformed: dict[str, Any]
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
            SOURCE_NAME,
            str(product["product_id"]) if product.get("product_id") is not None else None,
            product.get("sku"),
            product.get("product_name"),
            product.get("product_url"),
            price,
            CURRENCY,
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
            SOURCE_NAME,
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
    
def can_insert_to_fact(transformed: dict[str, Any]) -> tuple[bool, str | None]:
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
    observation_id,
    run_id,
    product,
    transformed,
    product_id,
):
    can_insert, reason = True, None

#    if not can_insert:
#        logger.info(
#            "Skipping fact insert — product=%r reason=%s",
#            product.get("product_name"),
#            reason,
#        )
#        return False

    cursor.execute(
        """
        INSERT INTO fact_price_observations (
            observation_id,
            run_id,
            product_id,
            standardized_product_name,
            price,
            regular_price,
            currency,
            normalized_unit,
            normalized_quantity,
            price_per_unit,
            unit_price_label,
            observed_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """,
        (
            observation_id,
            run_id,
            product_id,
            transformed["standardized_product_name"],
            transformed["price"],
            transformed["regular_price"],
            transformed["currency"],
            transformed["normalized_unit"],
            transformed["normalized_quantity"],
            transformed["price_per_unit"],
            transformed["unit_price_label"],
        ),
    )

    return True

# ---------------------------------------------------------------------------
# Per-product insert (kendi transaction'ı var)
# ---------------------------------------------------------------------------


def process_product(
    conn,
    run_id: int,
    product: dict[str, Any],
    category_slug: str,
) -> dict[str, Any]:
    """
    Tek bir ürünü raw → stg → fact olarak yazar.
    Her ürün kendi transaction'ında çalışır.
    """
    cursor = conn.cursor()
    transformed = None

    try:
        # 1️⃣ RAW
        event_id = insert_raw_event(cursor, run_id, product, category_slug)

        # 2️⃣ STG SOURCE
        insert_stg_source_product(cursor, event_id, run_id, product)

        # 3️⃣ TRANSFORM
        transformed = transform_product(product)

        # 4️⃣ DIM PRODUCT (NEW 🔥)
        product_id = get_or_create_product_id(
            cursor,
            transformed["standardized_product_name"],
            transformed.get("category_name"),
        )

        # 5️⃣ STG NORMALIZED (SADECE 1 KEZ!)
        insert_stg_normalized_observation(
            cursor, event_id, run_id, product, transformed
        )

        # 6️⃣ STG OBSERVATION
        observation_id = insert_stg_observation(
            cursor, event_id, run_id, product, transformed
        )

        # 7️⃣ FACT
        fact_inserted = insert_fact_observation(
            cursor,
            observation_id,
            run_id,
            product,
            transformed,
            product_id,   # ⭐ kritik yeni alan
        )

        # 8️⃣ COMMIT
        conn.commit()

        return {
            "ok": True,
            "inserted_raw": 1,
            "inserted_stg": 1,
            "inserted_fact": 1 if fact_inserted else 0,
            "is_suspicious": bool(transformed.get("is_suspicious")),
        }

    except Exception as e:
        conn.rollback()

        logger.exception(
            "FAILED INSERT — run_id=%s product_name=%r product_id=%r sku=%r transformed=%r error=%s",
            run_id,
            product.get("product_name"),
            product.get("product_id"),
            product.get("sku"),
            transformed,
            str(e),
        )

        return {
            "ok": False,
            "inserted_raw": 0,
            "inserted_stg": 0,
            "inserted_fact": 0,
            "is_suspicious": False,
        }

    finally:
        cursor.close()

def get_or_create_product_id(
    cursor,
    standardized_product_name: str,
    category_name: str | None,
) -> int:
    if not standardized_product_name:
        raise ValueError("standardized_product_name cannot be empty")
    # önce var mı bak
    cursor.execute(
        """
        SELECT product_id
        FROM dim_products
        WHERE standardized_product_name = %s
        """,
        (standardized_product_name,),
    )
    row = cursor.fetchone()

    if row:
        return row[0]

    # yoksa insert et
    cursor.execute(
        """
        INSERT INTO dim_products (
            standardized_product_name,
            canonical_name,
            category_level_1
        )
        VALUES (%s, %s, %s)
        RETURNING product_id
        """,
        (
            standardized_product_name,
            standardized_product_name,
            category_name,
        ),
    )

    return cursor.fetchone()[0]

# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------
def run_pipeline(category_slug: str = DEFAULT_CATEGORY_SLUG):
    conn = None
    run_id = None

    try:
        conn = get_connection()

        with conn.cursor() as cur:
            run_id = start_run(cur, category_slug)
            conn.commit()

        scraped_products = get_migros_category_products(category_slug)
        logger.info(
            "Scraped %d products from category=%s",
            len(scraped_products),
            category_slug,
        )

        success_count = 0
        failed_products: list[dict] = []
        raw_count = 0
        stg_count = 0
        fact_count = 0
        suspicious_count = 0
        failed_count = 0

        for product in scraped_products:
            result = process_product(conn, run_id, product, category_slug)

            if result["ok"]:
                success_count += 1
                raw_count += result["inserted_raw"]
                stg_count += result["inserted_stg"]
                fact_count += result["inserted_fact"]

                if result["is_suspicious"]:
                    suspicious_count += 1
            else:
                failed_products.append(product)
                failed_count += 1

        with conn.cursor() as cur:
            cur.execute(
                """
                select count(*)::int,
                       count(price_per_unit)::int,
                       count(category_name)::int
                from fact_price_observations
                where run_id = %s
                """,
                (run_id,),
            )
            total_rows, non_null_price_per_unit_rows, non_null_category_rows = cur.fetchone()

            price_check_status = "pass" if total_rows == non_null_price_per_unit_rows else "fail"
            category_check_status = "pass" if total_rows == non_null_category_rows else "fail"

            log_quality_check(
                cur,
                run_id,
                "fact_price_per_unit_completeness",
                price_check_status,
                non_null_price_per_unit_rows,
                total_rows,
                "price_per_unit should be populated for all fact rows in the run",
            )

            log_quality_check(
                cur,
                run_id,
                "fact_category_name_completeness",
                category_check_status,
                non_null_category_rows,
                total_rows,
                "category_name should be populated for all fact rows in the run",
            )

            if price_check_status == "fail" or category_check_status == "fail":
                raise RuntimeError("Data quality checks failed for current run.")

            finish_run(
                cur,
                run_id,
                records_scraped=len(scraped_products),
                records_raw=raw_count,
                records_stg=stg_count,
                records_fact=fact_count,
                records_suspicious=suspicious_count,
                records_failed=failed_count,
            )
            conn.commit()

        logger.info("=" * 50)
        logger.info("Run ID       : %s", run_id)
        logger.info("Category     : %s", category_slug)
        logger.info("Success      : %d", success_count)
        logger.info("Failed       : %d", len(failed_products))
        logger.info("Raw inserted : %d", raw_count)
        logger.info("Stg inserted : %d", stg_count)
        logger.info("Fact inserted: %d", fact_count)
        logger.info("Suspicious   : %d", suspicious_count)

        if failed_products:
            logger.warning("Failed products:")
            for p in failed_products:
                logger.warning("  - %s", p.get("product_name"))

        if success_count == 0 and scraped_products:
            raise RuntimeError("All products failed to insert — check logs above.")

    except Exception as e:
        if conn is not None:
            conn.rollback()

        if run_id is not None and conn is not None:
            try:
                with conn.cursor() as cur:
                    fail_run(cur, run_id, str(e))
                    conn.commit()
            except Exception:
                conn.rollback()

        logger.exception("Pipeline failed: %s", e)
        raise

    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    run_pipeline(DEFAULT_CATEGORY_SLUG)
