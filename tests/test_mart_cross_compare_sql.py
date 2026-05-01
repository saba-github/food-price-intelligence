from pathlib import Path


def test_mart_cross_compare_is_created_by_repo_migration():
    migration_sql = Path(
        "database/migrations/036_prepare_product_matching_and_price_history.sql"
    ).read_text(encoding="utf-8")

    assert "CREATE OR REPLACE VIEW mart_cross_compare AS" in migration_sql
    assert "same_unit_flag" in migration_sql
    assert "same_quantity_flag" in migration_sql
    assert "comparison_confidence" in migration_sql
