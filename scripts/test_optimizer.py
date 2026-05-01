from database.connection import get_connection
from pipeline.optimizer.engine import optimize_basket


def main():
    conn = get_connection(application_name="test-optimizer")

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
