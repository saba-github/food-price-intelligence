from typing import Optional


def ensure_product_alias(cursor, product_id: int, alias_text: str) -> None:
    if not alias_text:
        return

    cursor.execute(
        """
        INSERT INTO dim_product_aliases (
            product_id,
            alias_text,
            normalized_alias
        )
        VALUES (%s, %s, %s)
        ON CONFLICT (normalized_alias) DO NOTHING
        """,
        (
            product_id,
            alias_text,
            alias_text,
        ),
    )


def get_or_create_product_id(
    cursor,
    standardized_product_name: str,
    category_name: Optional[str],
) -> int:
    if not standardized_product_name:
        raise ValueError("standardized_product_name cannot be empty")

    cursor.execute(
        """
        SELECT product_id
        FROM dim_products
        WHERE standardized_product_name = %s
        """,
        (standardized_product_name,),
    )
    row = cursor.fetchone()

    if row:
        product_id = row[0]
        ensure_product_alias(cursor, product_id, standardized_product_name)
        return product_id

    cursor.execute(
        """
        INSERT INTO dim_products (
            standardized_product_name,
            canonical_name,
            category_level_1
        )
        VALUES (%s, %s, %s)
        RETURNING product_id
        """,
        (
            standardized_product_name,
            standardized_product_name,
            category_name,
        ),
    )

    product_id = cursor.fetchone()[0]
    ensure_product_alias(cursor, product_id, standardized_product_name)
    return product_id
