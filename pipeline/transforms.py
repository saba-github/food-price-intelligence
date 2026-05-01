import re
import unicodedata
from typing import Any, Optional, Tuple


TR_CHAR_MAP = str.maketrans(
    {
        "\u0131": "i",
        "\u011f": "g",
        "\u00fc": "u",
        "\u015f": "s",
        "\u00f6": "o",
        "\u00e7": "c",
    }
)

MEASUREMENT_PATTERN = re.compile(
    r"\b\d+(?:[.,]\d+)?\s*(kg|g|gram|ml|l|lt|adet|demet|paket)\b"
)
STANDALONE_MEASUREMENT_PATTERN = re.compile(
    r"\b(kg|g|gram|ml|l|lt|adet|demet|paket)\b"
)
PRODUCE_BASE_TOKENS = {
    "biber",
    "domates",
    "elma",
    "hiyar",
    "muz",
    "patates",
    "salatalik",
    "sogan",
}
PRODUCE_DESCRIPTOR_TOKENS = {
    "adet",
    "dokme",
    "ithal",
    "kuru",
    "paket",
    "salkim",
    "taze",
    "yerli",
}
PRODUCE_BASE_CANONICAL_MAP = {
    "hiyar": "salatalik",
}
MUSHROOM_BASE_TOKENS = {"mantar", "mantari"}
MUSHROOM_VARIANT_CANONICAL_MAP = {
    "istiridye": "istiridye mantar",
    "istridye": "istiridye mantar",
    "kestane": "kestane mantar",
    "shiitake": "shiitake mantar",
    "izgaralik": "izgaralik mantar",
}
MUSHROOM_REGULAR_DESCRIPTOR_TOKENS = {
    "kultur",
    "paket",
    "tabak",
}
WEIGHT_IN_NAME_PATTERN = re.compile(
    r"(?P<qty>\d+(?:[.,]\d+)?)\s*(?P<unit>kg|g|gram)\b"
)
ROLL_COUNT_PATTERN = re.compile(
    r"\b(?P<count>\d+)\s*(?:['’]?\s*(?:li|lu)\b|rulo\b|roll\b)"
)
MULTIPACK_ROLL_PATTERN = re.compile(
    r"\b(?P<outer>\d+)\s*x\s*(?P<inner>\d+)\b"
)
EQUIVALENT_ROLL_PATTERN = re.compile(r"\b1\s*=\s*(?P<count>\d+)\b")
TOKEN_PATTERN = re.compile(r"[a-z0-9]+")
TOILET_PAPER_TOKENS = {"tuvalet", "kagidi"}
TOILET_PAPER_EXCLUDE_TOKENS = {"islak", "mendil", "havlu", "pecete"}
PAPER_TOWEL_TOKEN_SETS = (
    {"kagit", "havlu"},
    {"havlu", "kagit"},
    {"havlu", "kagidi"},
)
PAPER_TOWEL_EXCLUDE_TOKENS = {
    "tuvalet",
    "islak",
    "mendil",
    "pecete",
}
PAPER_PRODUCT_LINE_ALIASES = {
    "platinum": "platinum",
    "egzotik": "egzotik",
    "biocare": "biocare",
    "deluxe": "deluxe",
    "bamboo": "bambu",
    "bambu": "bambu",
    "natural": "natural",
    "inova": "inova",
    "klasik": "klasik",
    "plus": "plus",
    "soft": "soft",
}


def normalize_text(value: str) -> str:
    if value is None:
        return ""

    if isinstance(value, float) and value != value:
        return ""

    if not isinstance(value, str):
        value = str(value)

    normalized = value.lower().strip().translate(TR_CHAR_MAP)
    normalized = (
        normalized.replace("’", "'")
        .replace("`", "'")
        .replace("´", "'")
        .replace("–", "-")
        .replace("—", "-")
    )
    normalized = unicodedata.normalize("NFKD", normalized)
    normalized = "".join(
        char for char in normalized if not unicodedata.combining(char)
    )
    return " ".join(normalized.split())


def normalize_unit(
    unit: Optional[str], quantity: Any
) -> Tuple[Optional[str], Optional[float]]:
    if unit is None:
        return None, None

    unit_upper = str(unit).strip().upper()

    qty: Optional[float] = None
    if quantity is not None:
        try:
            qty = float(quantity)
        except (TypeError, ValueError):
            qty = None

    if unit_upper == "GRAM":
        if qty is None:
            return "kg", None
        return "kg", round(qty / 1000, 4)

    if unit_upper == "PIECE":
        return "piece", qty if qty is not None else 1.0

    if unit_upper == "ROLL":
        return "roll", qty if qty is not None else 1.0

    return unit.lower(), qty


def _strip_measurement_tokens(name: str) -> str:
    name = MEASUREMENT_PATTERN.sub(" ", name)
    name = STANDALONE_MEASUREMENT_PATTERN.sub(" ", name)
    return re.sub(r"\s+", " ", name).strip()


def _canonicalize_mushroom_words(words: list[str]) -> Optional[str]:
    if not any(word in MUSHROOM_BASE_TOKENS for word in words):
        return None

    for token, canonical_name in MUSHROOM_VARIANT_CANONICAL_MAP.items():
        if token in words:
            return canonical_name

    filtered_words = [
        word
        for word in words
        if word not in MUSHROOM_REGULAR_DESCRIPTOR_TOKENS
    ]
    if any(word in MUSHROOM_BASE_TOKENS for word in filtered_words):
        return "mantar"

    return "mantar"


def infer_weight_measurement_from_name(
    product_name: Optional[str],
) -> Tuple[Optional[str], Optional[float]]:
    if not product_name:
        return None, None

    normalized_name = normalize_text(product_name)
    match = WEIGHT_IN_NAME_PATTERN.search(normalized_name)
    if match is None:
        return None, None

    quantity_value = float(match.group("qty").replace(",", "."))
    unit = match.group("unit")

    if unit in {"g", "gram"}:
        return "kg", round(quantity_value / 1000, 4)

    return "kg", quantity_value


def _token_list(value: str) -> list[str]:
    return TOKEN_PATTERN.findall(value)


def infer_paper_product_profile(
    product_name: Optional[str],
    category_name: Optional[str] = None,
) -> dict[str, Any]:
    normalized_name = normalize_text(product_name or "")
    normalized_category = normalize_text(category_name or "")
    tokens = _token_list(normalized_name.replace("-", " "))
    token_set = set(tokens)

    if not normalized_name and not normalized_category:
        return {
            "kind": None,
            "brand_token": None,
            "product_line": None,
            "product_line_tokens": (),
            "roll_count": None,
            "standard_roll_pack": False,
            "dev_rulo": False,
            "asilabilir": False,
            "el_yuz_havlusu": False,
            "yaprak_based": False,
            "multipack_equivalent": False,
            "is_special_format": False,
        }

    is_toilet_paper = (
        "tuvalet" in token_set
        and ("kagidi" in token_set or "kagit" in token_set)
        and not any(token in token_set for token in TOILET_PAPER_EXCLUDE_TOKENS)
    ) or ("tuvalet" in normalized_category and "kagit" in normalized_category)

    is_el_yuz_havlusu = (
        ("havlu" in token_set and "el" in token_set)
        or ("havlu" in token_set and "yuz" in token_set)
        or "elhavlusu" in token_set
        or "yuzhavlusu" in token_set
    )
    is_tissue_like = any(
        token in token_set for token in {"islak", "mendil", "pecete"}
    )
    is_asilabilir = any(
        token in token_set for token in {"asilabilir", "asmali", "cekmeli"}
    )
    is_dev_rulo = any(token in token_set for token in {"dev", "jumbo"})
    is_yaprak_based = "yaprak" in token_set
    is_multipack_equivalent = (
        EQUIVALENT_ROLL_PATTERN.search(normalized_name) is not None
    )
    product_line_tokens = tuple(
        sorted(
            {
                canonical
                for token, canonical in PAPER_PRODUCT_LINE_ALIASES.items()
                if token in token_set
            }
        )
    )

    is_paper_towel = (
        not is_toilet_paper
        and (
            "havlu" in token_set
            or ("kagit" in normalized_category and "havlu" in normalized_category)
        )
        and not is_tissue_like
    )

    roll_count: Optional[float] = None
    multipack_match = MULTIPACK_ROLL_PATTERN.search(normalized_name)
    equivalent_match = EQUIVALENT_ROLL_PATTERN.search(normalized_name)
    explicit_rulo_match = re.search(
        r"\b(?P<count>\d+)\s*(?:rulo|roll)\b",
        normalized_name,
    )
    li_match = re.search(r"\b(?P<count>\d+)\s*['â€™]?\s*(?:li|lu)\b", normalized_name)

    if is_toilet_paper or is_paper_towel:
        if multipack_match is not None:
            outer = int(multipack_match.group("outer"))
            inner = int(multipack_match.group("inner"))
            if outer > 0 and inner > 0:
                roll_count = float(outer * inner)
        elif equivalent_match is not None:
            roll_count = float(equivalent_match.group("count"))
        elif explicit_rulo_match is not None:
            roll_count = float(explicit_rulo_match.group("count"))
        elif li_match is not None:
            count_value = int(li_match.group("count"))
            blocked_count_only = (
                is_el_yuz_havlusu
                or is_tissue_like
                or (
                    (is_asilabilir or is_yaprak_based)
                    and explicit_rulo_match is None
                    and equivalent_match is None
                )
                or count_value >= 50
            )
            if not blocked_count_only:
                roll_count = float(count_value)

    kind = None
    if is_toilet_paper:
        kind = "toilet_paper"
    elif is_paper_towel:
        kind = "paper_towel"

    standard_roll_pack = (
        kind in {"toilet_paper", "paper_towel"}
        and roll_count is not None
        and not is_dev_rulo
        and not is_asilabilir
        and not is_el_yuz_havlusu
        and not is_yaprak_based
        and not is_multipack_equivalent
    )

    is_special_format = (
        is_dev_rulo
        or is_asilabilir
        or is_el_yuz_havlusu
        or is_yaprak_based
        or is_multipack_equivalent
    )

    return {
        "kind": kind,
        "brand_token": tokens[0] if tokens else None,
        "product_line": " ".join(product_line_tokens) if product_line_tokens else None,
        "product_line_tokens": product_line_tokens,
        "roll_count": roll_count,
        "standard_roll_pack": standard_roll_pack,
        "dev_rulo": is_dev_rulo,
        "asilabilir": is_asilabilir,
        "el_yuz_havlusu": is_el_yuz_havlusu,
        "yaprak_based": is_yaprak_based,
        "multipack_equivalent": is_multipack_equivalent,
        "is_special_format": is_special_format,
    }


def _looks_like_roll_paper_product(
    product_name: Optional[str],
    category_name: Optional[str] = None,
) -> bool:
    profile = infer_paper_product_profile(product_name, category_name)
    return profile.get("kind") in {"toilet_paper", "paper_towel"}


def infer_roll_measurement_from_name(
    product_name: Optional[str],
    category_name: Optional[str] = None,
) -> Tuple[Optional[str], Optional[float]]:
    if not _looks_like_roll_paper_product(product_name, category_name):
        return None, None

    profile = infer_paper_product_profile(product_name, category_name)
    roll_count = profile.get("roll_count")
    if roll_count is not None:
        return "roll", float(roll_count)

    return None, None


def canonicalize_produce_name(product_name: Optional[str]) -> Optional[str]:
    if not product_name:
        return None

    name = _strip_measurement_tokens(normalize_text(product_name))
    if not name:
        return None

    words = name.split()
    mushroom_name = _canonicalize_mushroom_words(words)
    if mushroom_name is not None:
        return mushroom_name

    if not any(word in PRODUCE_BASE_TOKENS for word in words):
        return " ".join(words) or None

    canonical_words = [
        word for word in words if word not in PRODUCE_DESCRIPTOR_TOKENS
    ]
    if len(canonical_words) == 1:
        canonical_words = [
            PRODUCE_BASE_CANONICAL_MAP.get(canonical_words[0], canonical_words[0])
        ]
    return " ".join(canonical_words) or None


def standardize_product_name(product_name: Optional[str]) -> Optional[str]:
    name = canonicalize_produce_name(product_name)
    if not name:
        return None

    words = name.split()
    words.sort()
    return " ".join(words) or None


def calculate_price_per_unit(
    price: Optional[float], normalized_quantity: Optional[float]
) -> Optional[float]:
    if price is None or normalized_quantity is None:
        return None

    try:
        price_value = float(price)
        quantity_value = float(normalized_quantity)
    except (TypeError, ValueError):
        return None

    if quantity_value <= 0:
        return None

    return round(price_value / quantity_value, 4)


def build_unit_price_label(normalized_unit: Optional[str]) -> Optional[str]:
    if normalized_unit is None:
        return None
    return f"TRY/{normalized_unit}"


def detect_suspicious(
    product_name: Optional[str], price: Optional[float]
) -> Tuple[bool, Optional[str]]:
    name = (product_name or "").lower()

    if price is None:
        return True, "price_null"

    if price <= 0:
        return True, "price_invalid"

    if price > 500:
        return True, "price_too_high"

    has_small_package_hint = (
        " gr" in name
        or "g paket" in name
        or " g paket" in name
        or "paket" in name
    )

    if has_small_package_hint and price > 200:
        return True, "small_package_price_too_high"

    return False, None


def transform_product(product: dict[str, Any]) -> dict[str, Any]:
    price = product.get("shown_price_tl")
    regular_price = product.get("regular_price_tl")

    unit = product.get("unit")
    unit_amount = product.get("unit_amount")

    normalized_unit, normalized_quantity = normalize_unit(unit, unit_amount)
    inferred_roll_unit, inferred_roll_quantity = infer_roll_measurement_from_name(
        product.get("product_name"),
        product.get("category_name"),
    )
    if inferred_roll_unit is not None and inferred_roll_quantity is not None:
        normalized_unit = inferred_roll_unit
        normalized_quantity = inferred_roll_quantity
    elif normalized_unit in {None, "piece", "adet", "ad"}:
        inferred_unit, inferred_quantity = infer_weight_measurement_from_name(
            product.get("product_name")
        )
        if inferred_unit is not None and inferred_quantity is not None:
            normalized_unit = inferred_unit
            normalized_quantity = inferred_quantity
    price_per_unit = calculate_price_per_unit(price, normalized_quantity)
    unit_price_label = build_unit_price_label(normalized_unit)

    canonical_product_name = canonicalize_produce_name(product.get("product_name"))
    standardized_product_name = standardize_product_name(canonical_product_name)

    is_suspicious, suspicious_reason = detect_suspicious(
        product.get("product_name"),
        price,
    )

    discount_rate = product.get("discount_rate")
    brand_name = product.get("brand_name")
    category_name = product.get("category_name")

    return {
        "price": price,
        "regular_price": regular_price,
        "currency": "TRY",
        "normalized_unit": normalized_unit,
        "normalized_quantity": normalized_quantity,
        "price_per_unit": price_per_unit,
        "unit_price_label": unit_price_label,
        "canonical_product_name": canonical_product_name,
        "standardized_product_name": standardized_product_name,
        "is_suspicious": is_suspicious,
        "suspicious_reason": suspicious_reason,
        "brand_name": brand_name,
        "category_name": category_name,
        "discount_rate": discount_rate,
    }
