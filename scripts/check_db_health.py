from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pandas as pd

from app.queries import PUBLIC_PRODUCT_CATALOG_QUERY
from database.connection import get_connection
from pipeline.optimizer.engine import optimize_basket
from pipeline.optimizer.product_search import (
    build_optimizer_input_from_group,
    build_search_group_sections,
)


REQUIRED_RELATIONS = {
    "scrape_runs": "table",
    "raw_price_events": "table",
    "stg_source_products": "table",
    "stg_normalized_observations": "table",
    "stg_price_observations": "table",
    "dim_products": "table",
    "dim_product_aliases": "table",
    "fact_price_observations": "table",
    "ops_data_quality_results": "table",
    "price_history": "view",
    "mart_daily_prices": "materialized_view",
}
SMOKE_QUERIES = ["domates", "süt", "ekmek"]


@dataclass
class HealthCheckResult:
    name: str
    ok: bool
    details: str


def _print_section(title: str) -> None:
    print()
    print(title)
    print("-" * len(title))


def _print_result(result: HealthCheckResult) -> None:
    status = "PASS" if result.ok else "FAIL"
    print(f"[{status}] {result.name}: {result.details}")


def _rows_to_frame(cursor) -> pd.DataFrame:
    columns = [description[0] for description in cursor.description]
    return pd.DataFrame(cursor.fetchall(), columns=columns)


def _fetch_catalog_df(conn) -> pd.DataFrame:
    with conn.cursor() as cursor:
        cursor.execute(PUBLIC_PRODUCT_CATALOG_QUERY)
        return _rows_to_frame(cursor)


def _check_connection_identity(conn) -> HealthCheckResult:
    with conn.cursor() as cursor:
        cursor.execute("SELECT current_database() AS db_name")
        db_name = cursor.fetchone()[0]

    dsn = conn.get_dsn_parameters()
    host = dsn.get("host") or "local_socket"
    port = dsn.get("port") or "default"
    return HealthCheckResult(
        name="database_connection",
        ok=True,
        details=f"connected to database='{db_name}' host='{host}' port='{port}'",
    )


def _check_required_relations(conn) -> HealthCheckResult:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT c.relname, c.relkind
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public'
              AND c.relname = ANY(%s)
            """,
            (list(REQUIRED_RELATIONS.keys()),),
        )
        found = {name: relkind for name, relkind in cursor.fetchall()}

    missing = [
        relation_name
        for relation_name in REQUIRED_RELATIONS
        if relation_name not in found
    ]

    if missing:
        return HealthCheckResult(
            name="required_relations",
            ok=False,
            details="missing: " + ", ".join(missing),
        )

    return HealthCheckResult(
        name="required_relations",
        ok=True,
        details=f"all {len(REQUIRED_RELATIONS)} required relations are present",
    )


def _check_latest_success_runs(conn) -> HealthCheckResult:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT source_name, MAX(started_at) AS latest_success_started_at
            FROM scrape_runs
            WHERE status = 'success'
            GROUP BY source_name
            ORDER BY source_name
            """
        )
        rows = cursor.fetchall()

    if not rows:
        return HealthCheckResult(
            name="latest_successful_scrapes",
            ok=False,
            details="no successful scrape_runs found",
        )

    formatted = ", ".join(
        f"{source_name}={started_at:%Y-%m-%d %H:%M}"
        for source_name, started_at in rows
        if started_at is not None
    )

    if not formatted:
        return HealthCheckResult(
            name="latest_successful_scrapes",
            ok=False,
            details="successful runs exist but timestamps are missing",
        )

    return HealthCheckResult(
        name="latest_successful_scrapes",
        ok=True,
        details=formatted,
    )


def _check_price_history_rows(conn) -> HealthCheckResult:
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM price_history")
        row_count = int(cursor.fetchone()[0])

    return HealthCheckResult(
        name="price_history_rows",
        ok=row_count > 0,
        details=f"{row_count} rows",
    )


def _select_group_for_smoke_test(search_sections: dict[str, object]) -> dict | None:
    safe_groups = search_sections.get("safe_groups") or []
    if safe_groups:
        return safe_groups[0]

    related_groups = search_sections.get("related_groups") or []
    if related_groups:
        return related_groups[0]

    return None


def _smoke_test_query(conn, catalog_df: pd.DataFrame, query: str) -> HealthCheckResult:
    search_sections = build_search_group_sections(catalog_df, query)
    selected_group = _select_group_for_smoke_test(search_sections)

    if not selected_group:
        return HealthCheckResult(
            name=f"app_smoke:{query}",
            ok=False,
            details="no search group returned by app search path",
        )

    optimizer_input = build_optimizer_input_from_group(selected_group)
    with conn.cursor() as cursor:
        result = optimize_basket(cursor, [optimizer_input])

    recommendations = result.get("per_product_recommendations") or []
    if not recommendations:
        return HealthCheckResult(
            name=f"app_smoke:{query}",
            ok=False,
            details=(
                "search group exists but optimize_basket returned no displayable "
                "recommendation"
            ),
        )

    recommendation = recommendations[0]
    standardized_name = recommendation.get("standardized_product_name") or "-"
    a101_name = recommendation.get("a101_source_product_name") or "-"
    migros_name = recommendation.get("migros_source_product_name") or "-"
    coverage_status = recommendation.get("coverage_status") or "-"

    return HealthCheckResult(
        name=f"app_smoke:{query}",
        ok=True,
        details=(
            f"selected='{standardized_name}', "
            f"a101='{a101_name}', migros='{migros_name}', "
            f"status='{coverage_status}'"
        ),
    )


def _summarize_failures(results: Iterable[HealthCheckResult]) -> int:
    return sum(1 for result in results if not result.ok)


def main() -> None:
    all_results: list[HealthCheckResult] = []

    try:
        conn = get_connection(application_name="check-db-health")
    except Exception as exc:
        _print_result(
            HealthCheckResult(
                name="database_connection",
                ok=False,
                details=str(exc),
            )
        )
        raise SystemExit(1) from exc

    try:
        _print_section("Connection")
        connection_result = _check_connection_identity(conn)
        all_results.append(connection_result)
        _print_result(connection_result)

        _print_section("Schema")
        relation_result = _check_required_relations(conn)
        all_results.append(relation_result)
        _print_result(relation_result)

        _print_section("Freshness")
        freshness_result = _check_latest_success_runs(conn)
        all_results.append(freshness_result)
        _print_result(freshness_result)

        _print_section("Data Volume")
        price_history_result = _check_price_history_rows(conn)
        all_results.append(price_history_result)
        _print_result(price_history_result)

        _print_section("App Smoke Tests")
        catalog_df = _fetch_catalog_df(conn)
        for query in SMOKE_QUERIES:
            smoke_result = _smoke_test_query(conn, catalog_df, query)
            all_results.append(smoke_result)
            _print_result(smoke_result)

    finally:
        conn.close()

    failure_count = _summarize_failures(all_results)
    _print_section("Summary")
    if failure_count:
        print(f"DB health check finished with {failure_count} failing check(s).")
        raise SystemExit(1)

    print(f"DB health check passed ({len(all_results)} checks).")


if __name__ == "__main__":
    main()
