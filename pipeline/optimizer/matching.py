from pipeline.transforms import standardize_product_name


def normalize_input(text: str) -> str:
    return standardize_product_name(text) or ""


def find_product_match(cursor, user_input: str) -> dict:
    normalized_input = normalize_input(user_input)

    try:
        cursor.execute(
            """
            SELECT product_id
            FROM dim_products
            WHERE standardized_product_name = %s
            LIMIT 1;
            """,
            (normalized_input,),
        )
        row = cursor.fetchone()

        if row:
            return {
                "product_id": row[0],
                "found": True,
                "match_type": "product_exact",
                "normalized_input": normalized_input,
            }

        cursor.execute(
            """
            SELECT product_id
            FROM dim_product_aliases
            WHERE normalized_alias = %s
            LIMIT 1;
            """,
            (normalized_input,),
        )
        row = cursor.fetchone()

        if row:
            return {
                "product_id": row[0],
                "found": True,
                "match_type": "alias_exact",
                "normalized_input": normalized_input,
            }

    except Exception:
        return {
            "product_id": None,
            "found": False,
            "match_type": "lookup_error",
            "normalized_input": normalized_input,
        }

    return {
        "product_id": None,
        "found": False,
        "match_type": "no_match",
        "normalized_input": normalized_input,
    }


def find_product_id(cursor, user_input: str) -> int | None:
    result = find_product_match(cursor, user_input)
    return result["product_id"]
