import os

import psycopg2

from pipeline.optimizer.engine import optimize_basket


def main():
    database_url = os.getenv("DATABASE_URL")
    conn = psycopg2.connect(database_url)

    try:
        cursor = conn.cursor()
        try:
            print(optimize_basket(cursor, ["domates", "muz"]))
        finally:
            cursor.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
