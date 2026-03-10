import os
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

from scraper.migros.categories import get_migros_category_products

load_dotenv()


def get_connection():
    return psycopg2.connect(
        host=os.getenv("DATABASE_HOST"),
        database=os.getenv("DATABASE_NAME"),
        user=os.getenv("DATABASE_USER"),
        password=os.getenv("DATABASE_PASSWORD"),
        port=os.getenv("DATABASE_PORT", "5432")
    )


def start_run(cursor, category_slug: str):
    cursor.execute("""
        INSERT INTO scrape_runs (source_name, status, notes)
        VALUES ('migros', 'running', %s)
        RETURNING run_id
    """, (f"category_slug={category_slug}",))
    return cursor.fetchone()[0]


def finish_run(cursor, run_id, records):
    cursor.execute("""
        UPDATE scrape_runs
        SET finished_at = NOW(),
            status = 'success',
            records_scraped = %s
        WHERE run_id = %s
    """, (records, run_id))


def fail_run(cursor, run_id, error_message):
    cursor.execute("""
        UPDATE scrape_runs
        SET finished_at = NOW(),
            status = 'failed',
            error_message = %s
        WHERE run_id = %s
    """, (error_message, run_id))


def insert_raw_event(cursor, run_id, product, category_slug):
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
        VALUES (%s, 'migros', %s, %s, %s, 'TRY', %s)
    """, (
        run_id,
        product.get("product_name"),
        product.get("product_url"),
        product.get("shown_price_tl"),
        psycopg2.extras.Json({
            "category_slug": category_slug,
            **product
        })
    ))


def run_pipeline(category_slug: str = "meyve-sebze-c-2"):

    conn = get_connection()
    cursor = conn.cursor()

    run_id = None

    try:
        run_id = start_run(cursor, category_slug)
        conn.commit()

        scraped_products = get_migros_category_products(category_slug)

        for product in scraped_products:
            insert_raw_event(
                cursor=cursor,
                run_id=run_id,
                product=product,
                category_slug=category_slug
            )

        finish_run(cursor, run_id, len(scraped_products))
        conn.commit()

        print(f"Pipeline finished successfully. Run ID: {run_id}")
        print(f"Category: {category_slug}")
        print(f"Records scraped: {len(scraped_products)}")

    except Exception as e:
        if run_id is not None:
            fail_run(cursor, run_id, str(e))
            conn.commit()

        print(f"Pipeline failed: {e}")

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    run_pipeline("meyve-sebze-c-2")
