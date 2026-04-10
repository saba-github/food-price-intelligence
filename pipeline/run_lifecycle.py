import logging

logger = logging.getLogger(__name__)


def start_run(
    cursor,
    source_name: str,
    category_key: str,
    category_slug: str,
    triggered_by: str,
    pipeline_version: str,
) -> int:
    cursor.execute(
        """
        INSERT INTO scrape_runs (
            source_name,
            category_key,
            category_slug,
            status,
            triggered_by,
            pipeline_version
        )
        VALUES (%s, %s, %s, 'running', %s, %s)
        RETURNING run_id
        """,
        (
            source_name,
            category_key,
            category_slug,
            triggered_by,
            pipeline_version,
        ),
    )
    run_id = cursor.fetchone()[0]
    logger.info(
        "Run started — run_id=%s category_key=%s category_slug=%s",
        run_id,
        category_key,
        category_slug,
    )
    return run_id


def finish_run(
    cursor,
    run_id: int,
    records_scraped: int,
    records_raw: int,
    records_stg: int,
    records_fact: int,
    records_suspicious: int,
    records_failed: int,
):
    cursor.execute(
        """
        UPDATE scrape_runs
        SET finished_at = NOW(),
            status = 'success',
            records_scraped = %s,
            records_raw = %s,
            records_stg = %s,
            records_fact = %s,
            records_suspicious = %s,
            records_failed = %s
        WHERE run_id = %s
        """,
        (
            records_scraped,
            records_raw,
            records_stg,
            records_fact,
            records_suspicious,
            records_failed,
            run_id,
        ),
    )


def fail_run(cursor, run_id: int, error_message: str):
    cursor.execute(
        """
        UPDATE scrape_runs
        SET finished_at = NOW(),
            status = 'failed',
            error_message = %s
        WHERE run_id = %s
        """,
        (error_message[:5000], run_id),
    )
    logger.error("Run failed — run_id=%s error=%s", run_id, error_message[:200])
