import hashlib
import os
import re
import sys

import psycopg2


MIGRATION_NAME_PATTERN = re.compile(r"^\d{3}_.+\.sql$")


def _sha256(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def main():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL is required.")

    migrations_dir = os.path.join("database", "migrations")
    if not os.path.isdir(migrations_dir):
        raise FileNotFoundError(f"Missing migrations directory: {migrations_dir}")

    conn = psycopg2.connect(database_url)

    try:
        conn.autocommit = False

        with conn.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    filename TEXT PRIMARY KEY,
                    checksum TEXT NOT NULL,
                    applied_at TIMESTAMP NOT NULL DEFAULT NOW()
                )
                """
            )
            conn.commit()

        sql_files = sorted(
            file_name
            for file_name in os.listdir(migrations_dir)
            if file_name.endswith(".sql")
        )

        invalid_names = [name for name in sql_files if not MIGRATION_NAME_PATTERN.match(name)]
        if invalid_names:
            raise ValueError(f"Invalid migration filename(s): {invalid_names}")

        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM schema_migrations")
            migration_log_count = cursor.fetchone()[0]

            cursor.execute("SELECT to_regclass('public.scrape_runs') IS NOT NULL")
            has_legacy_tables = bool(cursor.fetchone()[0])

        should_bootstrap_existing_db = migration_log_count == 0 and has_legacy_tables
        if should_bootstrap_existing_db:
            print("Detected existing migrated database. Bootstrapping schema_migrations ledger.")

        for file_name in sql_files:
            file_path = os.path.join(migrations_dir, file_name)

            try:
                with open(file_path, "r", encoding="utf-8") as file:
                    sql = file.read()
                checksum = _sha256(sql)

                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT checksum
                        FROM schema_migrations
                        WHERE filename = %s
                        """,
                        (file_name,),
                    )
                    row = cursor.fetchone()

                    if row is not None:
                        applied_checksum = row[0]
                        if applied_checksum != checksum:
                            raise ValueError(
                                f"Checksum mismatch for {file_name}. "
                                f"Applied={applied_checksum}, current={checksum}"
                            )
                        print(f"Skipping already applied migration: {file_name}")
                        continue

                    if should_bootstrap_existing_db:
                        cursor.execute(
                            """
                            INSERT INTO schema_migrations (filename, checksum)
                            VALUES (%s, %s)
                            """,
                            (file_name, checksum),
                        )
                        conn.commit()
                        print(f"Bootstrapped migration record: {file_name}")
                        continue

                    cursor.execute(sql)
                    cursor.execute(
                        """
                        INSERT INTO schema_migrations (filename, checksum)
                        VALUES (%s, %s)
                        """,
                        (file_name, checksum),
                    )

                conn.commit()
                print(f"Ran migration: {file_name}")

            except Exception as exc:
                conn.rollback()
                print(f"Failed migration: {file_name}")
                print(exc)
                raise

    finally:
        conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Migration runner failed: {exc}")
        sys.exit(1)
