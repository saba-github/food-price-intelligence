import argparse
import json
from pathlib import Path

from psycopg2 import sql

from database.connection import get_connection
from pipeline.marts import refresh_materialized_views


IMPORT_TABLES = [
    "scrape_runs",
    "dim_products",
    "dim_product_aliases",
    "raw_price_events",
    "stg_source_products",
    "stg_normalized_observations",
    "stg_price_observations",
    "fact_price_observations",
    "ops_data_quality_results",
]

PRIMARY_KEYS = {
    "scrape_runs": "run_id",
    "dim_products": "product_id",
    "dim_product_aliases": "alias_id",
    "raw_price_events": "event_id",
    "stg_source_products": "source_id",
    "stg_normalized_observations": "observation_id",
    "stg_price_observations": "observation_id",
    "fact_price_observations": "fact_id",
    "ops_data_quality_results": "quality_id",
}


def _load_metadata(bundle_dir: Path) -> dict:
    metadata_path = bundle_dir / "metadata.json"
    if not metadata_path.exists():
        raise FileNotFoundError(f"Missing bundle metadata: {metadata_path}")
    return json.loads(metadata_path.read_text(encoding="utf-8"))


def _truncate_tables(cursor) -> None:
    identifiers = sql.SQL(", ").join(sql.Identifier(name) for name in IMPORT_TABLES)
    cursor.execute(sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY CASCADE").format(identifiers))


def _import_table(cursor, bundle_dir: Path, table_name: str) -> None:
    csv_path = bundle_dir / f"{table_name}.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing export file for {table_name}: {csv_path}")

    copy_query = sql.SQL("COPY {table} FROM STDIN WITH CSV HEADER").format(
        table=sql.Identifier(table_name)
    )

    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        cursor.copy_expert(copy_query.as_string(cursor), handle)


def _reset_sequences(cursor) -> None:
    for table_name, column_name in PRIMARY_KEYS.items():
        cursor.execute(
            """
            SELECT pg_get_serial_sequence(%s, %s)
            """,
            (table_name, column_name),
        )
        sequence_name = cursor.fetchone()[0]
        if not sequence_name:
            continue

        cursor.execute(
            sql.SQL(
                """
                SELECT setval(
                    %s,
                    COALESCE((SELECT MAX({column}) FROM {table}), 1),
                    (SELECT COUNT(*) > 0 FROM {table})
                )
                """
            ).format(
                table=sql.Identifier(table_name),
                column=sql.Identifier(column_name),
            ),
            (sequence_name,),
        )


def main():
    parser = argparse.ArgumentParser(
        description="Import a CSV bundle into an already-migrated PostgreSQL database."
    )
    parser.add_argument("--bundle-dir", required=True, help="Directory created by export_db_bundle.py")
    parser.add_argument(
        "--keep-existing",
        action="store_true",
        help="Skip truncation and append onto existing rows instead.",
    )
    args = parser.parse_args()

    bundle_dir = Path(args.bundle_dir)
    metadata = _load_metadata(bundle_dir)

    conn = get_connection(application_name="import-db-bundle")
    try:
        conn.autocommit = False
        with conn.cursor() as cursor:
            if not args.keep_existing:
                _truncate_tables(cursor)

            for table_name in IMPORT_TABLES:
                _import_table(cursor, bundle_dir, table_name)

            _reset_sequences(cursor)
            refresh_materialized_views(cursor)
            conn.commit()

        print(f"Imported bundle from {bundle_dir}")
        for table in metadata.get("tables", []):
            print(f"- {table['table']}: {table['rows']} rows expected")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
