from typing import Optional


def log_quality_check(
    cursor,
    run_id: int,
    check_name: str,
    check_status: str,
    observed_value=None,
    threshold_value=None,
    details: Optional[str] = None,
):
    cursor.execute(
        """
        insert into ops_data_quality_results
            (run_id, check_name, check_status, observed_value, threshold_value, details)
        values (%s, %s, %s, %s, %s, %s)
        """,
        (run_id, check_name, check_status, observed_value, threshold_value, details),
    )