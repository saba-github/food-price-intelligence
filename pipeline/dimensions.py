def get_or_create_product_id(
    cursor,
    standardized_product_name: str,
    category_name: str | None,
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
        return row[0]

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

    return cursor.fetchone()[0]
