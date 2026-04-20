def normalize_input(text: str) -> str:
    normalized = text.lower()

    tr_map = {
        "ı": "i",
        "ğ": "g",
        "ü": "u",
        "ş": "s",
        "ö": "o",
        "ç": "c",
    }

    for old, new in tr_map.items():
        normalized = normalized.replace(old, new)

    return " ".join(normalized.split())


def find_product_match(cursor, user_input: str) -> dict:
    normalized_input = normalize_input(user_input)

    try:
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
