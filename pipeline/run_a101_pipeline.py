import logging

from config.retailers import RETAILER_CONFIG
from pipeline.db import get_connection
from pipeline.loaders_raw import insert_raw_event
from pipeline.loaders_staging import insert_stg_source_product
from pipeline.run_lifecycle import start_run, finish_run, fail_run
from scraper.a101.categories import get_a101_category_products

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

source_name = RETAILER_CONFIG["a101"]["source_name"]
currency = RETAILER_CONFIG["a101"]["currency"]


def run_pipeline(category_key: str):
    category_slug = RETAILER_CONFIG["a101"]["categories"][category_key]

    conn = None
    run_id = None

    try:
        conn = get_connection()

        # ---------------------------
        # START RUN
        # ---------------------------
        with conn.cursor() as cur:
            run_id = start_run(
                cur,
                source_name=source_name,
                category_key=category_key,
                category_slug=category_slug,
                triggered_by="github_actions",
                pipeline_version="v1-a101",
            )
            conn.commit()

        # ---------------------------
        # SCRAPE
        # ---------------------------
        products = get_a101_category_products(category_slug)

        logger.info("A101 scraped %d products", len(products))

        raw_count = 0
        stg_count = 0
        failed_count = 0

        # ---------------------------
        # PROCESS PRODUCTS
        # ---------------------------
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
                        event_id,
                        run_id=run_id,
                        product=product,
                        source_name=source_name,
                    )

                    conn.commit()

                    raw_count += 1
                    stg_count += 1

            except Exception as e:
                conn.rollback()
                failed_count += 1
                logger.error("Failed A101 product: %s", e)

        # ---------------------------
        # FINISH RUN
        # ---------------------------
        with conn.cursor() as cur:
            finish_run(
                cur,
                run_id,
                records_scraped=len(products),
                records_raw=raw_count,
                records_stg=stg_count,
                records_fact=0,
                records_suspicious=0,
                records_failed=failed_count,
            )
            conn.commit()

        logger.info("A101 pipeline completed successfully")

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

        logger.exception("A101 pipeline failed: %s", e)
        raise

    finally:
        if conn is not None:
            conn.close()
