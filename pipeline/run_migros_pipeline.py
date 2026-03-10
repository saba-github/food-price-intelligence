import os
import re
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

from scraper.migros.extract import extract_migros_products

load_dotenv()


def get_connection():
    return psycopg2.connect(
        host=os.getenv("DATABASE_HOST"),
        database=os.getenv("DATABASE_NAME"),
        user=os.getenv("DATABASE_USER"),
        password=os.getenv("DATABASE_PASSWORD"),
        port=os.getenv("DATABASE_PORT", "5432")
    )


def parse_price(price_text):
    if not price_text:
        return None

    cleaned = price_text.replace("TL", "").replace("₺", "").strip()
    cleaned = cleaned.replace(".", "").replace(",", ".")

    match = re.search(r"\d+(\.\d+)?", cleaned)
    if match:
        return float(match.group(0))

    return None


def start_run(cursor):
    cursor.execute("""
        INSERT INTO scrape_runs (source_name, status)
        VALUES ('migros', 'running')
        RETURNING run_id
    """)
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


def insert_raw_event(cursor, run_id, product_name, price, url, raw_payload):
    cursor.execute("""
        INSERT INTO raw_price_events
        (run_id, source_name, product_name, product_url, price, currency, raw_payload)
        VALUES (%s, 'migros', %s, %s, %s, 'TRY', %s)
    """, (run_id, product_name, url, price, psycopg2.extras.Json(raw_payload)))


def run_pipeline():

    conn = get_connection()
    cursor = conn.cursor()

    run_id = None

    try:

        run_id = start_run(cursor)
        conn.commit()

        scraped_products = extract_migros_products()

        for product in scraped_products:

            product_name = product.get("product_name")
            price_text = product.get("price_text")
            product_url = product.get("product_url")

            price = parse_price(price_text)

            insert_raw_event(
                cursor,
                run_id,
                product_name,
                price,
                product_url,
                product
            )

        finish_run(cursor, run_id, len(scraped_products))
        conn.commit()

        print(f"Pipeline finished successfully. Run ID: {run_id}")

    except Exception as e:

        if run_id is not None:
            fail_run(cursor, run_id, str(e))
            conn.commit()

        print(f"Pipeline failed: {e}")

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    run_pipeline()
