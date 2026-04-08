import logging
import os
from typing import Any

import psycopg2

from dotenv import load_dotenv
from pipeline.loaders_raw import insert_raw_event
from scraper.migros.categories import get_migros_category_products
from pipeline.marts import refresh_materialized_views
from pipeline.quality import log_quality_check
from pipeline.run_lifecycle import start_run, finish_run, fail_run
from pipeline.dimensions import get_or_create_product_id

from pipeline.loaders_staging import (
    insert_stg_source_product,
    insert_stg_observation,
    insert_stg_normalized_observation,
)

from pipeline.transforms import transform_product
from pipeline.loaders_fact import insert_fact_observation

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
# Insert helpers
# ---------------------------------------------------------------------------

 
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
        event_id = insert_raw_event(
            cursor,
            run_id,
            product,
            category_slug,
            source_name=SOURCE_NAME,
            currency=CURRENCY,
        )

        # 2️⃣ STG SOURCE
        insert_stg_source_product(
            cursor,
            event_id,
            run_id,
            product,
            source_name=SOURCE_NAME,
        )

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
            cursor,
            event_id,
            run_id,
            product,
            transformed,
            source_name=SOURCE_NAME,
        )

        # 6️⃣ STG OBSERVATION
        observation_id = insert_stg_observation(
            cursor,
            event_id,
            run_id,
            product,
            transformed,
            source_name=SOURCE_NAME,
            currency=CURRENCY,
        )

        # 7️⃣ FACT
        fact_inserted = insert_fact_observation(
            cursor,
            observation_id,
            run_id,
            product,
            transformed,
            product_id,
            source_name=SOURCE_NAME,
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

# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------
def run_pipeline(category_slug: str = DEFAULT_CATEGORY_SLUG):
    conn = None
    run_id = None

    try:
        conn = get_connection()

        with conn.cursor() as cur:
            run_id = start_run(
                cur,
                source_name=SOURCE_NAME,
                category_slug=category_slug,
                triggered_by="github_actions",
                pipeline_version="v2-prep-001",
            )
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

            refresh_materialized_views(cur)
            # ---------------------------
            # MART DATA QUALITY CHECKS
            # ---------------------------


            # Check 1: latest mart date için avg_price null olmamalı
            cur.execute(
                """
                select max(date)
                from mart_daily_prices
                """
            )
            latest_mart_date = cur.fetchone()[0]

            cur.execute(
                """
                select count(*)
                from mart_daily_prices
                where date = %s
                  and avg_price is null
                """,
                (latest_mart_date,),
            )
            null_avg_count = cur.fetchone()[0]

            mart_avg_status = "pass" if null_avg_count == 0 else "fail"

            log_quality_check(
                cur,
                run_id,
                "mart_avg_price_not_null_latest_date",
                mart_avg_status,
                null_avg_count,
                0,
                "avg_price should not be null in mart_daily_prices for the latest mart date",
            )

            # Check 2: full grain duplicate kontrolü
            cur.execute(
                """
                select count(*)
                from (
                    select
                        date,
                        standardized_product_name,
                        category_name,
                        normalized_unit,
                        count(*)
                    from mart_daily_prices
                    group by 1,2,3,4
                    having count(*) > 1
                ) t
                """
            )
            duplicate_count = cur.fetchone()[0]

            mart_dup_status = "pass" if duplicate_count == 0 else "fail"

            log_quality_check(
                cur,
                run_id,
                "mart_no_duplicates_full_grain",
                mart_dup_status,
                duplicate_count,
                0,
                "mart_daily_prices should not have duplicate rows at (date, product, category, unit) grain",
            )

            if mart_avg_status == "fail" or mart_dup_status == "fail":
                raise RuntimeError("Mart data quality checks failed.")

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
