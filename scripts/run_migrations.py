import os

import psycopg2


def main():
    database_url = os.getenv("DATABASE_URL")
    conn = psycopg2.connect(database_url)
    migrations_dir = os.path.join("database", "migrations")

    try:
        sql_files = sorted(
            file_name
            for file_name in os.listdir(migrations_dir)
            if file_name.endswith(".sql")
        )

        for file_name in sql_files:
            file_path = os.path.join(migrations_dir, file_name)

            try:
                with open(file_path, "r", encoding="utf-8") as file:
                    sql = file.read()

                with conn.cursor() as cursor:
                    cursor.execute(sql)

                conn.commit()
                print(f"Ran migration: {file_name}")

            except Exception as exc:
                conn.rollback()
                print(f"Failed migration: {file_name}")
                print(exc)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
