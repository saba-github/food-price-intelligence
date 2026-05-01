import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from psycopg2 import sql

from database.connection import get_connection, resolve_database_url


EXPORT_TABLES = [
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


def export_table(cursor, output_dir: Path, table_name: str) -> int:
    csv_path = output_dir / f"{table_name}.csv"
    order_column = PRIMARY_KEYS.get(table_name)

    if order_column:
        copy_query = sql.SQL(
            "COPY (SELECT * FROM {table} ORDER BY {column}) TO STDOUT WITH CSV HEADER"
        ).format(
            table=sql.Identifier(table_name),
            column=sql.Identifier(order_column),
        )
    else:
        copy_query = sql.SQL(
            "COPY (SELECT * FROM {table}) TO STDOUT WITH CSV HEADER"
        ).format(table=sql.Identifier(table_name))

    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        cursor.copy_expert(copy_query.as_string(cursor), handle)

    cursor.execute(
        sql.SQL("SELECT COUNT(*) FROM {table}").format(table=sql.Identifier(table_name))
    )
    return int(cursor.fetchone()[0])


def main():
    parser = argparse.ArgumentParser(
        description="Export PostgreSQL base tables into a reusable CSV bundle."
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory to write the bundle into. Defaults to exports/db_bundle_<timestamp>.",
    )
    args = parser.parse_args()

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_dir = Path(args.output_dir or f"exports/db_bundle_{timestamp}")
    output_dir.mkdir(parents=True, exist_ok=True)

    metadata = {
        "exported_at_utc": datetime.now(timezone.utc).isoformat(),
        "database_url_present": bool(resolve_database_url()),
        "tables": [],
    }

    conn = get_connection(application_name="export-db-bundle")
    try:
        with conn.cursor() as cursor:
            for table_name in EXPORT_TABLES:
                row_count = export_table(cursor, output_dir, table_name)
                metadata["tables"].append(
                    {
                        "table": table_name,
                        "file": f"{table_name}.csv",
                        "rows": row_count,
                    }
                )

        metadata_path = output_dir / "metadata.json"
        metadata_path.write_text(
            json.dumps(metadata, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        print(f"Exported bundle to {output_dir}")
        for table in metadata["tables"]:
            print(f"- {table['table']}: {table['rows']} rows")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
