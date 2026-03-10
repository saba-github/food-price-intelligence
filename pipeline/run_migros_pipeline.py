import psycopg2

# Database connection
conn = psycopg2.connect(
    host="YOUR_NEON_HOST",
    database="YOUR_DATABASE",
    user="YOUR_USER",
    password="YOUR_PASSWORD"
)

cursor = conn.cursor()


def start_run():
    cursor.execute("""
        INSERT INTO scrape_runs (source_name, status)
        VALUES ('migros', 'running')
        RETURNING run_id
    """)
    run_id = cursor.fetchone()[0]
    conn.commit()
    return run_id


def finish_run(run_id, records):
    cursor.execute("""
        UPDATE scrape_runs
        SET finished_at = NOW(),
            status = 'success',
            records_scraped = %s
        WHERE run_id = %s
    """, (records, run_id))
    conn.commit()


def insert_raw_event(run_id, product_name, price, url):
    cursor.execute("""
        INSERT INTO raw_price_events
        (run_id, source_name, product_name, price, product_url)
        VALUES (%s, 'migros', %s, %s, %s)
    """, (run_id, product_name, price, url))


def run_pipeline():

    run_id = start_run()

    # temporary example data
    scraped_products = [
        ("Reis Baldo Pirinç 2.5kg", 189.90, "https://migros.com/pirinc"),
        ("Sütaş Süt 1L", 32.50, "https://migros.com/sut"),
        ("Yumurta 10'lu", 65.00, "https://migros.com/yumurta"),
    ]

    for product in scraped_products:
        insert_raw_event(run_id, product[0], product[1], product[2])

    conn.commit()

    finish_run(run_id, len(scraped_products))

    print("Pipeline finished successfully")


if __name__ == "__main__":
    run_pipeline()
