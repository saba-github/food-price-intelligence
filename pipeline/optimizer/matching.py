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


def find_product_id(cursor, user_input: str) -> int | None:
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
    except Exception:
        return None

    if not row:
        return None

    return row[0]
