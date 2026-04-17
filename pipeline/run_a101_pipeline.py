import os
import logging
from typing import Any

from config.retailers import RETAILER_CONFIG
from pipeline.db import get_connection
from pipeline.dimensions import get_or_create_product_id
from pipeline.loaders_fact import insert_fact_observation
from pipeline.loaders_raw import insert_raw_event
from pipeline.loaders_staging import (
    insert_stg_normalized_observation,
    insert_stg_observation,
    insert_stg_source_product,
)
from pipeline.run_lifecycle import start_run, finish_run, fail_run
from pipeline.transforms import transform_product
from scraper.a101.categories import get_a101_category_products

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

source_name = RETAILER_CONFIG["a101"]["source_name"]
currency = RETAILER_CONFIG["a101"]["currency"]
PIPELINE_VERSION = "v2-a101-001"


def resolve_triggered_by() -> str:
    if os.getenv("GITHUB_ACTIONS") == "true":
        return "github_actions"
    return "manual"


def run_pipeline(category_key: str):
    category_slug = RETAILER_CONFIG["a101"]["categories"][category_key]

    conn = None
    run_id = None

    try:
        # -------------------------
        # 1) SCRAPE
        # -------------------------
        products = get_a101_category_products(category_slug)
        logger.info("A101 scraped %d products", len(products))

        # -------------------------
        # 2) DB CONNECT
        # -------------------------
        conn = get_connection()

        with conn.cursor() as cur:
            run_id = start_run(
                cur,
                source_name=source_name,
                category_key=category_key,
                category_slug=category_slug,
                triggered_by=resolve_triggered_by(),
                pipeline_version=PIPELINE_VERSION,
            )
            conn.commit()

        raw_count = 0
        stg_count = 0
        fact_count = 0
        failed_count = 0

        # -------------------------
        # 3) LOOP PRODUCTS
        # -------------------------
        for product in products:
            try:
                with conn.cursor() as cur:
                    # RAW
                    event_id = insert_raw_event(
                        cur,
                        run_id=run_id,
                        product=product,
                        category_slug=category_slug,
                        source_name=source_name,
                        currency=currency,
                    )

                    # STG SOURCE
                    insert_stg_source_product(
                        cur,
                        event_id=event_id,
                        run_id=run_id,
                        product=product,
                        source_name=source_name,
                    )

                    # TRANSFORM
                    transformed = transform_product(product)

                    # DIM
                    product_id = get_or_create_product_id(
                        cur,
                        transformed["standardized_product_name"],
                        transformed.get("category_name"),
                    )

                    # STG NORMALIZED
                    insert_stg_normalized_observation(
                        cur,
                        event_id,
                        run_id,
                        product,
                        transformed,
                        source_name=source_name,
                    )

                    # STG OBS
                    observation_id = insert_stg_observation(
                        cur,
                        event_id,
                        run_id,
                        product,
                        transformed,
                        source_name=source_name,
                        currency=currency,
                    )

                    # FACT
                    inserted = insert_fact_observation(
                        cur,
                        observation_id,
                        run_id,
                        product,
                        transformed,
                        product_id,
                        source_name=source_name,
                    )

                    conn.commit()

                    raw_count += 1
                    stg_count += 1

                    if inserted:
                        fact_count += 1

            except Exception as e:
                failed_count += 1
                try:
                    conn.rollback()
                except Exception:
                    pass

                logger.exception("A101 product failed: %s", e)

        # -------------------------
        # 4) FINISH RUN
        # -------------------------
        with conn.cursor() as cur:
            finish_run(
                cur,
                run_id=run_id,
                records_scraped=len(products),
                records_raw=raw_count,
                records_stg=stg_count,
                records_fact=fact_count,
                records_suspicious=0,
                records_failed=failed_count,
            )
            conn.commit()

        logger.info("=" * 40)
        logger.info("A101 RUN COMPLETED")
        logger.info("Products scraped : %d", len(products))
        logger.info("Raw inserted     : %d", raw_count)
        logger.info("Stg inserted     : %d", stg_count)
        logger.info("Fact inserted    : %d", fact_count)

    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except:
                pass

        if conn and run_id:
            try:
                with conn.cursor() as cur:
                    fail_run(cur, run_id, str(e))
                    conn.commit()
            except:
                pass

        logger.exception("A101 pipeline failed: %s", e)
        raise

    finally:
        if conn:
            conn.close()
