import logging
from typing import Any

from config.retailers import RETAILER_CONFIG
from pipeline.db import get_connection
from pipeline.loaders_raw import insert_raw_event
from pipeline.loaders_staging import insert_stg_source_product
from pipeline.transforms import transform_product
from scraper.a101.categories import get_a101_category_products

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

source_name = RETAILER_CONFIG["a101"]["source_name"]
currency = RETAILER_CONFIG["a101"]["currency"]


def run_pipeline(category_key: str):
    category_slug = RETAILER_CONFIG["a101"]["categories"][category_key]

    conn = get_connection()
    cursor = conn.cursor()

    products = get_a101_category_products(category_slug)

    logger.info("A101 scraped %d products", len(products))

    for product in products:
        try:
            # RAW
            event_id = insert_raw_event(
                cursor,
                run_id=999,  # geçici (sonra düzelteceğiz)
                product=product,
                category_slug=category_slug,
                source_name=source_name,
                currency=currency,
            )

            # STG SOURCE
            insert_stg_source_product(
                cursor,
                event_id,
                run_id=999,
                product=product,
                source_name=source_name,
            )

            conn.commit()

        except Exception as e:
            conn.rollback()
            logger.error("Failed A101 product: %s", e)

    cursor.close()
    conn.close()
