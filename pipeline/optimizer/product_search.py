from difflib import SequenceMatcher
import re

import pandas as pd

from pipeline.optimizer.cleaning_products import (
    analyze_cleaning_pair,
    cleaning_brand_from_query_tokens,
    cleaning_variant_from_query_tokens,
    explicit_cleaning_query_subtype,
    extract_cleaning_bundle_signature,
    infer_cleaning_product_profile,
    is_cleaning_context_text,
    preferred_cleaning_subtypes,
)
from pipeline.transforms import (
    PRODUCE_BASE_TOKENS,
    canonicalize_produce_name,
    infer_paper_product_profile,
    normalize_text,
    standardize_product_name,
)


PRODUCT_SEARCH_SYNONYMS = {
    "salatalik": ["hiyar"],
    "cucumber": ["hiyar"],
    "kahve": ["kahvesi"],
    "kahvesi": ["kahve"],
    "bulasik tablet": ["bulasik tableti"],
    "bulasik tableti": ["bulasik tablet"],
    "kagit havlu": ["havlu kagit", "havlu kagidi"],
    "havlu kagit": ["kagit havlu", "havlu kagidi"],
    "havlu kagidi": ["kagit havlu", "havlu kagit"],
    "taze sogan": ["sogan taze"],
    "sogan taze": ["sogan taze", "taze sogan"],
    "kuru sogan": ["sogan kuru"],
    "sogan kuru": ["sogan kuru", "kuru sogan"],
    "muz": ["muz yerli", "ithal muz"],
}
PRODUCT_FAMILY_DEFINITIONS = {
    "salatalik": {
        "label": "Salatalık",
        "terms": [
            "salatalik",
            "hiyar",
            "badem hiyar",
            "cengelkoy salatalik",
            "çengelköy salatalık",
        ],
    },
    "muz": {
        "label": "Muz",
        "terms": [
            "muz",
            "muz yerli",
            "ithal muz",
        ],
    },
    "elma": {
        "label": "Elma",
        "terms": [
            "elma",
            "starking",
            "granny smith",
        ],
    },
    "oil_aycicek": {
        "label": "Aycicek yagi",
        "terms": [
            "aycicek yagi",
        ],
    },
    "oil_zeytinyagi": {
        "label": "Zeytinyagi",
        "terms": [
            "zeytinyagi",
            "zeytin yagi",
        ],
    },
    "oil_misir": {
        "label": "Misirozu yagi",
        "terms": [
            "misir yagi",
            "misirozu yagi",
        ],
    },
    "oil_findik": {
        "label": "Findik yagi",
        "terms": [
            "findik yagi",
        ],
    },
    "coffee_turk": {
        "label": "Turk kahvesi",
        "terms": [
            "turk kahvesi",
            "turk kahve",
        ],
    },
    "coffee_filter": {
        "label": "Filtre kahve",
        "terms": [
            "filtre kahve",
        ],
    },
    "coffee_instant": {
        "label": "Instant kahve",
        "terms": [
            "instant kahve",
            "granul kahve",
            "gold kahve",
            "classic kahve",
            "cozunebilir kahve",
            "2'si arada kahve",
            "3'u arada kahve",
            "3u arada kahve",
        ],
    },
    "coffee_capsule": {
        "label": "Kapsul kahve",
        "terms": [
            "kapsul kahve",
        ],
    },
}
PRODUCT_SELECTION_PREFIX = "product:"
FAMILY_SELECTION_PREFIX = "family:"
SAFE_SEARCH_COVERAGE_STATUSES = {"comparable"}
TOILET_PAPER_QUERY_TOKENS = {"tuvalet", "kagidi"}
TOILET_PAPER_EXCLUDE_TOKENS = {"islak", "mendil", "havlu", "pecete"}
TOILET_PAPER_PREFERRED_ROLL_COUNTS = {12, 16}
PAPER_TOWEL_QUERY_TOKEN_SETS = (
    {"kagit", "havlu"},
    {"havlu", "kagit"},
    {"havlu", "kagidi"},
)
PAPER_TOWEL_EXCLUDE_TOKENS = {
    "tuvalet",
    "islak",
    "mendil",
    "pecete",
    "el",
    "yuz",
}
PAPER_TOWEL_PENALTY_TOKENS = {
    "asilabilir": 6,
    "asmali": 6,
    "cekmeli": 6,
    "dev": 5,
    "jumbo": 5,
    "yaprak": 6,
    "7": 3,
}
PAPER_TOWEL_PREFERRED_ROLL_COUNTS = {6, 8, 12}
PAPER_QUERY_GENERIC_TOKENS = {
    "tuvalet",
    "kagit",
    "kagidi",
    "havlu",
    "rulo",
    "roll",
    "li",
    "lu",
}
PRODUCE_FAMILY_IDS = {"salatalik", "muz", "elma"}

FUZZY_MATCH_THRESHOLD = 0.78
FUZZY_SYNONYM_THRESHOLD = 0.84
GENERIC_QUERY_PENALTY_RULES = {
    "pirinc": {
        "penalty_tokens": {
            "gevrek": 8,
            "crunch": 8,
            "nesfit": 8,
            "special": 7,
            "tahil": 6,
            "cikolatali": 6,
            "balli": 6,
            "bademli": 6,
            "kremasi": 7,
            "patlamis": 7,
            "granola": 7,
        },
    },
    "su": {
        "penalty_tokens": {
            "maden": 4,
            "mineral": 4,
            "soda": 4,
            "gazli": 4,
            "aromali": 3,
            "meyveli": 3,
        },
    },
    "sut": {
        "penalty_tokens": {
            "sutlac": 6,
            "sutlu": 5,
            "tatli": 4,
            "tereyagi": 6,
            "ari": 6,
            "krema": 4,
            "puding": 4,
            "cikolatali": 3,
            "cilekli": 3,
            "muzlu": 3,
            "vanilyali": 3,
            "vanilya": 3,
            "aromali": 3,
            "protein": 2,
            "proteinli": 2,
            "laktozsuz": 1,
        },
    },
    "yumurta": {
        "penalty_tokens": {
            "bildircin": 8,
            "organik": 3,
            "omega": 3,
            "omega-3": 3,
            "gezen": 3,
            "koy": 3,
            "naturakoy": 3,
            "jumbo": 3,
            "kucuk": 3,
            "buyuk": 3,
            "xl": 3,
        },
    },
    "kahve": {
        "penalty_tokens": {
            "latte": 8,
            "kahveli": 8,
            "sutlu": 8,
            "icecek": 7,
            "hazir": 7,
            "krema": 7,
            "kremasi": 7,
            "protein": 5,
            "granola": 6,
            "seker": 6,
            "kahverengi": 6,
        },
    },
    "ekmek": {
        "penalty_tokens": {
            "cavdarli": 4,
            "kepekli": 4,
            "eksi": 3,
            "mayali": 3,
            "glutensiz": 4,
            "tava": 4,
            "tost": 4,
            "sandvic": 4,
            "hamburger": 4,
            "lavas": 4,
            "bazlama": 4,
            "tortilla": 4,
            "tam": 2,
            "bugday": 2,
            "cok": 2,
            "tahilli": 2,
            "susamli": 3,
        },
    },
    "tuz": {
        "penalty_tokens": {
            "himalaya": 4,
            "kaya": 4,
            "salamura": 4,
            "limon": 4,
            "sarimsakli": 3,
            "truf": 3,
            "mantarli": 3,
            "iyotsuz": 2,
            "tuzluklu": 2,
            "azaltilmis": 2,
            "sodyumu": 2,
        },
    },
    "sogan": {
        "penalty_tokens": {
            "taze": 7,
            "demet": 7,
            "frenk": 6,
            "arpacik": 5,
        },
    },
    "kola": {
        "penalty_tokens": {
            "zero": 2,
            "sekersiz": 2,
            "sugar": 2,
            "free": 2,
            "light": 2,
            "diet": 2,
            "max": 2,
            "lime": 2,
            "lemon": 2,
            "vanilla": 2,
            "cherry": 2,
        },
    },
}

STRICT_SINGLE_TOKEN_QUERY_RULES = {
    "un": {
        "allowed_tokens": {"un", "unu"},
        "exclude_tokens": {"peynir", "unal", "unlu"},
    },
    "pirinc": {
        "allowed_tokens": {"pirinc"},
        "exclude_tokens": {
            "gevrek",
            "crunch",
            "nesfit",
            "special",
            "tahil",
            "cikolatali",
            "balli",
            "bademli",
            "kremasi",
            "patlamis",
            "granola",
        },
    },
    "su": {
        "allowed_tokens": {"su", "suyu"},
        "exclude_tokens": {
            "sut",
            "yogurt",
            "susam",
            "sos",
            "maden",
            "meyve",
            "kola",
            "gazoz",
            "soda",
            "enerji",
            "icecek",
            "icecegi",
            "limonata",
            "tonik",
            "aromali",
        },
    },
    "yag": {
        "required_any_token_sets": [
            {"zeytinyagi"},
            {"zeytin", "yagi"},
            {"aycicek", "yagi"},
            {"misir", "yagi"},
            {"misirozu", "yagi"},
            {"findik", "yagi"},
            {"sivi", "yag"},
            {"sivi", "yagi"},
        ],
        "exclude_tokens": set(),
    },
    "kahve": {
        "allowed_tokens": {"kahve", "kahvesi"},
        "exclude_tokens": {
            "latte",
            "kahveli",
            "sutlu",
            "icecek",
            "hazir",
            "krema",
            "kremasi",
            "protein",
            "granola",
            "seker",
            "kahverengi",
        },
    },
    "tuz": {
        "allowed_tokens": {"tuz", "tuzu"},
        "exclude_tokens": {"tuzlu", "tuzsuz"},
    },
    "seker": {
        "allowed_tokens": {"seker", "sekeri"},
        "exclude_tokens": {"domates"},
    },
    "kola": {
        "allowed_tokens": {"kola"},
        "exclude_tokens": set(),
    },
    "omo": {
        "allowed_tokens": {"omo"},
        "exclude_tokens": set(),
    },
}
GENERIC_REVIEW_QUERY_GROUPS = {
    "yag": [
        "oil_aycicek",
        "oil_zeytinyagi",
        "oil_misir",
        "oil_findik",
    ],
    "kahve": [
        "coffee_turk",
        "coffee_filter",
        "coffee_instant",
        "coffee_capsule",
    ],
}


def _standardized(value: str) -> str:
    return standardize_product_name(value) or normalize_text(value)


def _token_set(value: str) -> set[str]:
    return set(re.findall(r"[a-z0-9]+", _standardized(value)))


def _token_list(value: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", _standardized(value))


def _raw_token_set(value: str) -> set[str]:
    return set(re.findall(r"[a-z0-9]+", normalize_text(value)))


def _contains_token_sequence(haystack: list[str], needle: list[str]) -> bool:
    if not haystack or not needle or len(needle) > len(haystack):
        return False

    window_size = len(needle)
    return any(
        haystack[index:index + window_size] == needle
        for index in range(len(haystack) - window_size + 1)
    )


def _family_terms(family_id: str) -> list[str]:
    return [
        _standardized(term)
        for term in PRODUCT_FAMILY_DEFINITIONS[family_id]["terms"]
    ]


def _term_matches_product(product_name: str, term: str) -> bool:
    standardized_product = _standardized(product_name)
    if standardized_product == term:
        return True

    term_tokens = _token_set(term)
    product_tokens = _token_set(product_name)
    return bool(term_tokens) and term_tokens.issubset(product_tokens)


def _sequence_ratio(left: str, right: str) -> float:
    return SequenceMatcher(None, left, right).ratio()


def _query_variants(search_text: str) -> tuple[str, list[str]]:
    normalized_query = normalize_text(search_text)
    standardized_query = standardize_product_name(search_text) or normalized_query
    variants = [normalized_query, standardized_query]

    synonym_keys = [normalized_query, standardized_query]
    synonym_keys.extend(
        synonym_key
        for synonym_key in PRODUCT_SEARCH_SYNONYMS
        if any(
            _sequence_ratio(query, synonym_key) >= FUZZY_SYNONYM_THRESHOLD
            for query in [normalized_query, standardized_query]
        )
    )

    for synonym_key in synonym_keys:
        variants.extend(PRODUCT_SEARCH_SYNONYMS.get(synonym_key, []))

    normalized_variants = []
    for variant in variants:
        normalized_variant = normalize_text(variant)
        standardized_variant = standardize_product_name(variant) or normalized_variant
        normalized_variants.extend([normalized_variant, standardized_variant])

    return standardized_query, list(dict.fromkeys(normalized_variants))


def _strict_single_token_query(exact_query: str, variants: list[str]) -> str | None:
    query_tokens = _token_set(exact_query)
    if len(query_tokens) != 1:
        return None

    token = next(iter(query_tokens))
    if token in STRICT_SINGLE_TOKEN_QUERY_RULES:
        return token

    return None


def _matches_strict_single_token_query(
    product_name: str,
    strict_query_token: str,
) -> bool:
    product_tokens = _token_set(product_name)
    rule = STRICT_SINGLE_TOKEN_QUERY_RULES[strict_query_token]
    allowed_tokens = rule.get("allowed_tokens") or set()
    if allowed_tokens and not any(token in product_tokens for token in allowed_tokens):
        return False

    required_any_token_sets = rule.get("required_any_token_sets") or []
    if required_any_token_sets and not any(
        required_tokens.issubset(product_tokens)
        for required_tokens in required_any_token_sets
    ):
        return False

    excluded_tokens = rule.get("exclude_tokens", set())
    return not any(token in product_tokens for token in excluded_tokens)


def _match_product(product_name_or_row, exact_query: str, variants: list[str]):
    product_name, candidate_values = _search_values(product_name_or_row)
    normalized_product = normalize_text(product_name)
    standardized_product = standardize_product_name(product_name) or normalized_product
    product_variants = {
        standardize_product_name(value) or normalize_text(value)
        for value in candidate_values
        if value
    }
    query_tokens = _query_token_set(exact_query, variants)
    candidate_profiles = [
        infer_cleaning_product_profile(value)
        for value in candidate_values
        if value
    ]
    if _query_mentions_towel(query_tokens) and _is_water_candidate(candidate_values):
        return None

    explicit_subtype_family = _explicit_subtype_family_id(query_tokens)
    if explicit_subtype_family:
        product_family_id = get_product_family_id(product_name)
        if product_family_id != explicit_subtype_family:
            return None

    cleaning_brand_query = cleaning_brand_from_query_tokens(query_tokens)
    if cleaning_brand_query:
        if not any(
            profile.get("brand_token") == cleaning_brand_query
            for profile in candidate_profiles
        ):
            return None

    explicit_cleaning_subtype = explicit_cleaning_query_subtype(query_tokens)
    if explicit_cleaning_subtype:
        candidate_subtypes = {
            profile.get("subtype")
            for profile in candidate_profiles
            if profile.get("subtype")
        }
        if candidate_subtypes and explicit_cleaning_subtype not in candidate_subtypes:
            return None
        if not candidate_subtypes and any(
            profile.get("is_cleaning") for profile in candidate_profiles
        ):
            return None

    explicit_cleaning_variant = cleaning_variant_from_query_tokens(query_tokens)
    if explicit_cleaning_variant:
        relevant_profiles = [
            profile
            for profile in candidate_profiles
            if profile.get("is_cleaning")
            and (
                not cleaning_brand_query
                or profile.get("brand_token") == cleaning_brand_query
            )
            and (
                not explicit_cleaning_subtype
                or profile.get("subtype") == explicit_cleaning_subtype
            )
        ]
        if relevant_profiles:
            if any(
                profile.get("variant_token") != explicit_cleaning_variant
                for profile in relevant_profiles
            ):
                return None

    size_matched_cleaning_query = False
    cleaning_query_volume = _explicit_cleaning_size_query_liters(
        exact_query,
        variants,
        query_tokens,
    )
    if cleaning_query_volume is not None:
        matched_volume = any(
            _cleaning_volume_matches_query(value, cleaning_query_volume)
            for value in candidate_values
        )
        if any(profile.get("is_cleaning") for profile in candidate_profiles):
            if not matched_volume:
                return None
            size_matched_cleaning_query = True

    strict_query_token = _strict_single_token_query(exact_query, variants)
    if strict_query_token:
        combined_tokens: set[str] = set()
        for value in candidate_values:
            combined_tokens.update(_token_set(value))

        rule = STRICT_SINGLE_TOKEN_QUERY_RULES[strict_query_token]
        allowed_tokens = rule.get("allowed_tokens") or set()
        if allowed_tokens and not any(token in combined_tokens for token in allowed_tokens):
            return None

        required_any_token_sets = rule.get("required_any_token_sets") or []
        if required_any_token_sets and not any(
            required_tokens.issubset(combined_tokens)
            for required_tokens in required_any_token_sets
        ):
            return None

        excluded_tokens = rule.get("exclude_tokens", set())
        if any(token in combined_tokens for token in excluded_tokens):
            return None

    strict_produce_token = _strict_produce_query_token(exact_query, variants)
    if strict_produce_token:
        produce_family_id = get_product_family_id(strict_produce_token)
        family_raw_tokens: set[str] = set()
        if produce_family_id and produce_family_id in PRODUCT_FAMILY_DEFINITIONS:
            for term in PRODUCT_FAMILY_DEFINITIONS[produce_family_id]["terms"]:
                family_raw_tokens.update(_raw_token_set(term))
        matched_produce_candidate = False
        for value in candidate_values:
            if not value:
                continue
            raw_tokens = _raw_token_set(value)
            canonical_tokens = set(_token_list(canonicalize_produce_name(value) or ""))
            candidate_family_id = get_product_family_id(value)
            if (
                strict_produce_token in raw_tokens
                or strict_produce_token in canonical_tokens
                or bool(family_raw_tokens & raw_tokens)
                or (produce_family_id is not None and candidate_family_id == produce_family_id)
            ):
                matched_produce_candidate = True
                break

        if candidate_values and not matched_produce_candidate:
            return None

    requested_roll_count = _explicit_roll_count_query(exact_query, variants)
    if requested_roll_count is not None:
        candidate_roll_counts: set[float] = set()
        candidate_kinds: set[str] = set()
        for value in candidate_values:
            profile = infer_paper_product_profile(value)
            kind = profile.get("kind")
            if kind:
                candidate_kinds.add(kind)
            roll_count = profile.get("roll_count")
            if roll_count is not None:
                candidate_roll_counts.add(float(roll_count))

        if candidate_roll_counts:
            if requested_roll_count not in candidate_roll_counts:
                return None
        elif candidate_kinds:
            return None

    if exact_query in product_variants:
        return 0, 1.0

    if any(
        _term_matches_product(candidate_value, variant)
        for candidate_value in candidate_values
        for variant in variants
        if candidate_value and variant
    ):
        return 1, 0.99

    if any(variant in product_variants for variant in variants):
        return 1, 0.98

    best_partial_ratio = 0.0
    for variant in variants:
        if not variant:
            continue

        variant_tokens = _token_list(variant)
        for product_variant in product_variants:
            product_variant_tokens = _token_list(product_variant)
            forward_partial_match = (
                variant in product_variant
                or _contains_token_sequence(product_variant_tokens, variant_tokens)
            )
            reverse_partial_match = _contains_token_sequence(
                variant_tokens,
                product_variant_tokens,
            )
            if forward_partial_match or reverse_partial_match:
                ratio = len(variant) / max(len(product_variant), 1)
                best_partial_ratio = max(best_partial_ratio, ratio)

    if best_partial_ratio:
        return 2, best_partial_ratio

    if size_matched_cleaning_query:
        return 2, 0.97

    best_fuzzy_ratio = max(
        (
            _sequence_ratio(variant, product_variant)
            for variant in variants
            for product_variant in product_variants
            if variant and product_variant
        ),
        default=0.0,
    )
    if best_fuzzy_ratio >= FUZZY_MATCH_THRESHOLD:
        return 3, best_fuzzy_ratio

    return None


def _query_token_set(exact_query: str, variants: list[str]) -> set[str]:
    query_tokens: set[str] = set()
    for value in [exact_query, *variants]:
        if not value:
            continue
        query_tokens.update(_token_set(value))
    return query_tokens


def _strict_produce_query_token(exact_query: str, variants: list[str]) -> str | None:
    query_candidates = [exact_query, *variants]
    for value in query_candidates:
        if not value:
            continue
        canonical_name = canonicalize_produce_name(value)
        if not canonical_name:
            continue
        canonical_tokens = _token_list(canonical_name)
        if len(canonical_tokens) == 1 and canonical_tokens[0] in PRODUCE_BASE_TOKENS:
            return canonical_tokens[0]
    return None


def _explicit_subtype_family_id(query_tokens: set[str]) -> str | None:
    if "aycicek" in query_tokens and "yagi" in query_tokens:
        return "oil_aycicek"
    if "zeytinyagi" in query_tokens or {"zeytin", "yagi"}.issubset(query_tokens):
        return "oil_zeytinyagi"
    if "misirozu" in query_tokens or {"misir", "yagi"}.issubset(query_tokens):
        return "oil_misir"
    if "findik" in query_tokens and "yagi" in query_tokens:
        return "oil_findik"
    if "turk" in query_tokens and ("kahve" in query_tokens or "kahvesi" in query_tokens):
        return "coffee_turk"
    if "filtre" in query_tokens and ("kahve" in query_tokens or "kahvesi" in query_tokens):
        return "coffee_filter"
    if "kapsul" in query_tokens and ("kahve" in query_tokens or "kahvesi" in query_tokens):
        return "coffee_capsule"
    if (
        {"instant", "kahve"}.issubset(query_tokens)
        or {"granul", "kahve"}.issubset(query_tokens)
        or {"gold", "kahve"}.issubset(query_tokens)
        or {"classic", "kahve"}.issubset(query_tokens)
        or {"cozunebilir", "kahve"}.issubset(query_tokens)
    ):
        return "coffee_instant"
    return None


def _generic_review_query_key(query_tokens: set[str]) -> str | None:
    if query_tokens <= {"yag", "yagi"} and "yag" in query_tokens:
        return "yag"
    if query_tokens <= {"kahve", "kahvesi"} and "kahve" in query_tokens:
        return "kahve"
    return None


def _group_sort_key(group: dict, review_query_key: str | None = None) -> tuple:
    family_order = GENERIC_REVIEW_QUERY_GROUPS.get(review_query_key, [])
    family_rank = 999
    if group.get("selection_type") == "product_family":
        family_id = group.get("family_id")
        if family_id in family_order:
            family_rank = family_order.index(family_id)
        return (0, family_rank, format_product_family_group(group))

    product_names = group.get("product_names") or []
    return (1, family_rank, product_names[0] if product_names else group.get("selection_id", ""))


def _row_text_values(row: pd.Series) -> list[str]:
    values = []
    for field_name in (
        "standardized_product_name",
        "a101_source_product_name",
        "migros_source_product_name",
    ):
        value = row.get(field_name)
        if value:
            values.append(str(value))
    return values


def _search_values(product_name_or_row) -> tuple[str, list[str]]:
    if isinstance(product_name_or_row, pd.Series):
        primary_value = str(product_name_or_row.get("standardized_product_name") or "")
        values = _row_text_values(product_name_or_row)
        if primary_value and primary_value not in values:
            values.insert(0, primary_value)
        return primary_value, values

    primary_value = str(product_name_or_row)
    return primary_value, [primary_value]


def _query_mentions_towel(query_tokens: set[str]) -> bool:
    return any(
        token in query_tokens
        for token in {"havlu", "havlusu", "elhavlusu", "yuzhavlusu"}
    )


def _is_water_candidate(candidate_values: list[str]) -> bool:
    beverage_exclude_tokens = {
        "maden",
        "meyve",
        "kola",
        "gazoz",
        "soda",
        "enerji",
        "icecek",
        "icecegi",
        "limonata",
        "tonik",
        "aromali",
    }
    for value in candidate_values:
        tokens = _token_set(value)
        if beverage_exclude_tokens & tokens:
            continue
        if "su" in tokens or "suyu" in tokens:
            return True
        if {"kaynak", "suyu"}.issubset(tokens):
            return True
    return False


def _row_tokens(row: pd.Series) -> set[str]:
    tokens: set[str] = set()
    for value in _row_text_values(row):
        tokens.update(_token_set(value))
    return tokens


def _row_cleaning_profiles(row: pd.Series) -> list[dict[str, object]]:
    profiles = []
    for value in _row_text_values(row):
        profile = infer_cleaning_product_profile(value)
        if profile.get("is_cleaning"):
            profiles.append(profile)
    return profiles


def _row_cleaning_subtypes(row: pd.Series) -> set[str]:
    return {
        str(profile["subtype"])
        for profile in _row_cleaning_profiles(row)
        if profile.get("subtype")
    }


def _row_cleaning_package_formats(row: pd.Series) -> set[str]:
    return {
        str(profile["package_format"])
        for profile in _row_cleaning_profiles(row)
        if profile.get("package_format")
    }


def _row_cleaning_bundle_signatures(row: pd.Series) -> set[str]:
    return {
        str(profile["bundle_signature"])
        for profile in _row_cleaning_profiles(row)
        if profile.get("bundle_signature")
    }


def _effective_coverage_status(row: pd.Series) -> str:
    coverage_status = row.get("coverage_status")

    pair_info = analyze_cleaning_pair(
        row.get("a101_source_product_name"),
        row.get("migros_source_product_name"),
        row.get("a101_normalized_unit"),
        row.get("migros_normalized_unit"),
        row.get("a101_normalized_quantity"),
        row.get("migros_normalized_quantity"),
    )
    if not pair_info.get("is_cleaning_pair"):
        return coverage_status

    if pair_info.get("soft_equivalent"):
        return "comparable"

    if coverage_status != "comparable":
        return coverage_status

    if not pair_info.get("same_brand") or not pair_info.get("same_subtype"):
        return "comparison_review_required"

    if (
        pair_info.get("a101_variant_token")
        and pair_info.get("migros_variant_token")
        and not pair_info.get("same_variant")
    ):
        return "comparison_review_required"

    if (
        pair_info.get("a101_package_format")
        and pair_info.get("migros_package_format")
        and not pair_info.get("same_package_format")
    ):
        return "comparison_review_required"

    return coverage_status


def _is_cola_query(query_tokens: set[str]) -> bool:
    return (
        "kola" in query_tokens
        or "pepsi" in query_tokens
        or {"coca", "cola"}.issubset(query_tokens)
    )


def _is_generic_toilet_paper_query(query_tokens: set[str]) -> bool:
    return TOILET_PAPER_QUERY_TOKENS.issubset(query_tokens)


def _is_generic_paper_towel_query(query_tokens: set[str]) -> bool:
    return any(token_set.issubset(query_tokens) for token_set in PAPER_TOWEL_QUERY_TOKEN_SETS)


def _extract_liter_value(value: str) -> float | None:
    normalized_value = normalize_text(value)
    match_ml = re.search(r"(\d+(?:[.,]\d+)?)\s*ml\b", normalized_value)
    if match_ml:
        return float(match_ml.group(1).replace(",", ".")) / 1000

    match_liter = re.search(r"(\d+(?:[.,]\d+)?)\s*l\b", normalized_value)
    if match_liter:
        return float(match_liter.group(1).replace(",", "."))

    return None


def _explicit_cleaning_size_query_liters(
    exact_query: str,
    variants: list[str],
    query_tokens: set[str],
) -> float | None:
    if _explicit_cleaning_bundle_signature(exact_query, variants):
        return None

    cleaning_brand_query = cleaning_brand_from_query_tokens(query_tokens)
    cleaning_context_query = bool(
        cleaning_brand_query
        or explicit_cleaning_query_subtype(query_tokens)
        or {"bulasik", "camasir", "deterjan", "deterjani"} & query_tokens
    )
    if not cleaning_context_query:
        return None

    query_values = [exact_query, *variants]
    for value in query_values:
        volume = _extract_liter_value(value)
        if volume is not None:
            return volume

    numeric_tokens = [
        int(token)
        for token in query_tokens
        if token.isdigit() and 100 <= int(token) <= 5000
    ]
    if len(numeric_tokens) != 1:
        return None

    return numeric_tokens[0] / 1000.0


def _cleaning_volume_matches_query(value: str, query_volume_liters: float) -> bool:
    candidate_volume = _extract_liter_value(value)
    if candidate_volume is None:
        return False

    tolerance = 0.1 if query_volume_liters >= 1 else 0.03
    return abs(candidate_volume - query_volume_liters) <= tolerance


def _extract_piece_count(value: str) -> float | None:
    normalized_value = normalize_text(value)
    match = re.search(
        r"\b(\d+)\s*['’]?\s*(?:adet|li|lu)\b",
        normalized_value,
    )
    if match:
        return float(match.group(1))

    return None


def _extract_roll_count(value: str) -> float | None:
    normalized_value = normalize_text(value)

    multipack_match = re.search(r"\b(\d+)\s*x\s*(\d+)\b", normalized_value)
    if multipack_match:
        return float(int(multipack_match.group(1)) * int(multipack_match.group(2)))

    roll_match = re.search(
        r"\b(\d+)\s*(?:['’]?\s*(?:li|lu)\b|rulo\b)",
        normalized_value,
    )
    if roll_match:
        return float(roll_match.group(1))

    return None


def _extract_roll_count(value: str) -> float | None:
    profile = infer_paper_product_profile(value)
    roll_count = profile.get("roll_count")
    if roll_count is None:
        return None
    return float(roll_count)


def _query_has_pack_quantity(exact_query: str, variants: list[str]) -> bool:
    pattern = r"\b(?:4|6|12|24)\s*x\s*\d"
    return any(
        re.search(pattern, normalize_text(value))
        for value in [exact_query, *variants]
        if value
    )


def _explicit_roll_count_query(exact_query: str, variants: list[str]) -> float | None:
    for value in [exact_query, *variants]:
        if not value:
            continue

        profile = infer_paper_product_profile(value)
        if profile.get("kind") not in {"paper_towel", "toilet_paper"}:
            continue

        roll_count = profile.get("roll_count")
        if roll_count is not None:
            return float(roll_count)

    return None


def _cola_query_volume_target(
    exact_query: str,
    variants: list[str],
    query_tokens: set[str],
) -> float | None:
    for value in [exact_query, *variants]:
        if not value:
            continue

        volume = _extract_liter_value(value)
        if volume is not None:
            return volume

    if "kutu" in query_tokens:
        return 0.33

    if _is_cola_query(query_tokens):
        return 1.0

    return None


def _cola_brand_rank(row: pd.Series, query_tokens: set[str]) -> int:
    if not _is_cola_query(query_tokens):
        return 0

    row_tokens = _row_tokens(row)

    if "kola" in query_tokens and "pepsi" not in query_tokens and not {
        "coca",
        "cola",
    }.issubset(query_tokens):
        if {"coca", "cola"}.issubset(row_tokens):
            return 0
        if "pepsi" in row_tokens:
            return 1
        return 2

    if "pepsi" in query_tokens:
        return 0 if "pepsi" in row_tokens else 1

    if {"coca", "cola"}.issubset(query_tokens):
        return 0 if {"coca", "cola"}.issubset(row_tokens) else 1

    return 0


def _cola_package_rank(
    row: pd.Series,
    exact_query: str,
    variants: list[str],
) -> int:
    query_tokens = _query_token_set(exact_query, variants)
    if not _is_cola_query(query_tokens):
        return 0

    row_tokens = _row_tokens(row)
    normalized_row_text = " ".join(
        normalize_text(value) for value in _row_text_values(row)
    )

    if _query_has_pack_quantity(exact_query, variants):
        return 0

    if "kutu" in query_tokens:
        return 0 if "kutu" in row_tokens else 1

    if re.search(r"\b(?:4|6|12|24)\s*x\s*\d", normalized_row_text):
        return 2

    if "kutu" in row_tokens:
        return 1

    return 0


def _cola_row_penalty(
    row: pd.Series,
    exact_query: str,
    variants: list[str],
) -> int:
    query_tokens = _query_token_set(exact_query, variants)
    if not _is_cola_query(query_tokens):
        return 0

    row_tokens = _row_tokens(row)
    normalized_row_text = " ".join(normalize_text(value) for value in _row_text_values(row))
    penalty = 0

    penalty_tokens = {
        "light": 6,
        "zero": 6,
        "max": 6,
        "sekersiz": 6,
        "sugar": 4,
        "free": 4,
        "diet": 6,
        "lime": 4,
        "lemon": 4,
        "vanilla": 4,
        "cherry": 4,
    }
    for token, weight in penalty_tokens.items():
        if token in row_tokens and token not in query_tokens:
            penalty += weight

    if "kutu" in query_tokens:
        if "kutu" not in row_tokens:
            penalty += 3
    elif "kutu" in row_tokens:
        penalty += 1

    if not _query_has_pack_quantity(exact_query, variants):
        if re.search(r"\b(?:4|6|12|24)\s*x\s*\d", normalized_row_text):
            penalty += 3

    return penalty


def _cola_price_rank(
    row: pd.Series,
    exact_query: str,
    variants: list[str],
) -> float:
    query_tokens = _query_token_set(exact_query, variants)
    if not _is_cola_query(query_tokens):
        return float("inf")

    price_candidates = []
    for field_name in ("a101_comparison_price", "migros_comparison_price"):
        value = row.get(field_name)
        if value is None or pd.isna(value):
            continue
        price_candidates.append(float(value))

    if not price_candidates:
        return float("inf")

    return min(price_candidates)


def _generic_query_penalty(
    product_name_or_row,
    exact_query: str,
    variants: list[str],
) -> int:
    query_tokens = _query_token_set(exact_query, variants)
    if _is_generic_toilet_paper_query(query_tokens):
        if isinstance(product_name_or_row, pd.Series):
            product_tokens = _row_tokens(product_name_or_row)
        else:
            product_tokens = _token_set(str(product_name_or_row))

        penalty = 0
        for token in TOILET_PAPER_EXCLUDE_TOKENS:
            if token in product_tokens and token not in query_tokens:
                penalty += 8
        return penalty

    if _is_generic_paper_towel_query(query_tokens):
        if isinstance(product_name_or_row, pd.Series):
            product_tokens = _row_tokens(product_name_or_row)
            normalized_row_text = " ".join(
                normalize_text(value) for value in _row_text_values(product_name_or_row)
            )
        else:
            product_tokens = _token_set(str(product_name_or_row))
            normalized_row_text = normalize_text(str(product_name_or_row))

        penalty = 0
        for token in PAPER_TOWEL_EXCLUDE_TOKENS:
            if token in product_tokens and token not in query_tokens:
                penalty += 8
        for token, weight in PAPER_TOWEL_PENALTY_TOKENS.items():
            if token in product_tokens and token not in query_tokens:
                penalty += weight
        if "1=7" in normalized_row_text and "1=7" not in exact_query:
            penalty += 6
        return penalty

    active_rule = next(
        (
            rule_name
            for rule_name in GENERIC_QUERY_PENALTY_RULES
            if rule_name in query_tokens
        ),
        None,
    )
    if not active_rule:
        return 0

    penalty_tokens = GENERIC_QUERY_PENALTY_RULES[active_rule]["penalty_tokens"]
    if isinstance(product_name_or_row, pd.Series):
        product_tokens = _row_tokens(product_name_or_row)
    else:
        product_tokens = _token_set(str(product_name_or_row))
    penalty = 0
    for token, weight in penalty_tokens.items():
        if token in product_tokens and token not in query_tokens:
            penalty += weight
    return penalty


def _generic_query_size_rank(
    row: pd.Series,
    exact_query: str,
    variants: list[str],
) -> float:
    query_tokens = _query_token_set(exact_query, variants)
    product_name = row["standardized_product_name"]
    normalized_product = normalize_text(product_name)

    if "su" in query_tokens:
        match = re.search(r"(\d+(?:[.]\d+)?)\s*l\b", normalized_product)
        if match:
            return abs(float(match.group(1)) - 0.5)

    if "tuz" in query_tokens:
        match_g = re.search(r"(\d+(?:[.]\d+)?)\s*g\b", normalized_product)
        if match_g:
            return abs(float(match_g.group(1)) - 750)

        match_kg = re.search(r"(\d+(?:[.]\d+)?)\s*kg\b", normalized_product)
        if match_kg:
            return abs(float(match_kg.group(1)) * 1000 - 750)

    if "yumurta" in query_tokens:
        for value in _row_text_values(row):
            piece_count = _extract_piece_count(value)
            if piece_count is None:
                continue
            if piece_count in {10, 15}:
                return 0
            if piece_count == 12:
                return 1
            if piece_count == 20:
                return 2
            if piece_count == 6:
                return 3
            if piece_count >= 30:
                return 4
            return abs(piece_count - 12.5)

    if _is_generic_toilet_paper_query(query_tokens):
        requested_roll_count = _explicit_roll_count_query(exact_query, variants)

        for value in _row_text_values(row):
            roll_count = _extract_roll_count(value)
            if roll_count is None:
                continue

            if requested_roll_count is not None:
                return abs(roll_count - requested_roll_count)

            if roll_count in TOILET_PAPER_PREFERRED_ROLL_COUNTS:
                return 0
            if roll_count == 8:
                return 1
            if roll_count in {32, 40}:
                return 2
            return 3 + abs(roll_count - 14)

    if _is_generic_paper_towel_query(query_tokens):
        requested_roll_count = _explicit_roll_count_query(exact_query, variants)

        for value in _row_text_values(row):
            roll_count = _extract_roll_count(value)
            if roll_count is None:
                continue

            if requested_roll_count is not None:
                return abs(roll_count - requested_roll_count)

            if roll_count in PAPER_TOWEL_PREFERRED_ROLL_COUNTS:
                return 0
            if roll_count == 16:
                return 1
            return 2 + abs(roll_count - 8)

    if _is_cola_query(query_tokens):
        target_volume = _cola_query_volume_target(exact_query, variants, query_tokens)
        if target_volume is None:
            return float("inf")

        for value in _row_text_values(row):
            volume = _extract_liter_value(value)
            if volume is not None:
                return abs(volume - target_volume)

    return float("inf")


def _generic_query_primary_rank(
    row: pd.Series,
    exact_query: str,
    variants: list[str],
) -> int:
    query_tokens = _query_token_set(exact_query, variants)

    if "yumurta" in query_tokens:
        for value in _row_text_values(row):
            piece_count = _extract_piece_count(value)
            if piece_count is None:
                continue
            if piece_count in {10, 15}:
                return 0
            if piece_count == 20:
                return 1
            if piece_count >= 30:
                return 2
            if piece_count in {6, 8, 12}:
                return 3
            return 4

    return 0


def _paper_roll_selection_rank(
    row: pd.Series,
    exact_query: str,
    variants: list[str],
) -> int:
    query_tokens = _query_token_set(exact_query, variants)
    if not (
        _is_generic_toilet_paper_query(query_tokens)
        or _is_generic_paper_towel_query(query_tokens)
    ):
        return 0

    requested_roll_count = _explicit_roll_count_query(exact_query, variants)
    row_roll_count = _extract_roll_count(row["standardized_product_name"])

    if requested_roll_count is not None:
        if row_roll_count is None:
            return 2
        if row_roll_count == requested_roll_count:
            return 0
        return 1

    if row_roll_count is not None:
        return 0

    return 1


def _paper_brand_selection_rank(
    row: pd.Series,
    exact_query: str,
    variants: list[str],
) -> int:
    query_tokens = _query_token_set(exact_query, variants)
    if not (
        _is_generic_toilet_paper_query(query_tokens)
        or _is_generic_paper_towel_query(query_tokens)
    ):
        return 0

    brand_query_tokens = {
        token
        for token in query_tokens
        if token not in PAPER_QUERY_GENERIC_TOKENS and not token.isdigit()
    }
    if not brand_query_tokens:
        return 0

    standardized_tokens = _token_set(row["standardized_product_name"])
    return 0 if brand_query_tokens & standardized_tokens else 1


def _paper_brand_pair_rank(
    row: pd.Series,
    exact_query: str,
    variants: list[str],
) -> int:
    query_tokens = _query_token_set(exact_query, variants)
    if not _is_generic_paper_towel_query(query_tokens):
        return 0

    if _explicit_roll_count_query(exact_query, variants) is not None:
        return 0

    brand_query_tokens = {
        token
        for token in query_tokens
        if token not in PAPER_QUERY_GENERIC_TOKENS and not token.isdigit()
    }
    if not brand_query_tokens:
        return 0

    standardized_tokens = _token_set(row["standardized_product_name"])
    if not brand_query_tokens & standardized_tokens:
        return 2

    coverage_status = row.get("coverage_status")
    if coverage_status in {"comparable", "comparison_review_required"}:
        return 0 if _extract_roll_count(row["standardized_product_name"]) is None else 1

    return 2


def _generic_query_preference_rank(
    row: pd.Series,
    exact_query: str,
    variants: list[str],
) -> int:
    query_tokens = _query_token_set(exact_query, variants)
    row_tokens = _row_tokens(row)

    if query_tokens == {"kahve"}:
        if "turk" in row_tokens and "kahvesi" in row_tokens:
            return 0
        if "filtre" in row_tokens and ("kahve" in row_tokens or "kahvesi" in row_tokens):
            return 1
        if "cekirdek" in row_tokens and ("kahve" in row_tokens or "kahvesi" in row_tokens):
            return 2
        return 3

    return 0


def _cleaning_query_preference_rank(
    row: pd.Series,
    exact_query: str,
    variants: list[str],
) -> int:
    query_tokens = _query_token_set(exact_query, variants)
    subtype_order = preferred_cleaning_subtypes(query_tokens)
    if not subtype_order:
        return 0

    row_subtypes = _row_cleaning_subtypes(row)
    if not row_subtypes:
        return len(subtype_order) + 3

    for index, subtype in enumerate(subtype_order):
        if subtype in row_subtypes:
            return index

    return len(subtype_order) + 1


def _cleaning_brand_coverage_rank(
    row: pd.Series,
    exact_query: str,
    variants: list[str],
    coverage_rank: int,
) -> int:
    query_tokens = _query_token_set(exact_query, variants)
    brand_query = cleaning_brand_from_query_tokens(query_tokens)
    if not brand_query:
        return 0

    row_profiles = _row_cleaning_profiles(row)
    if not row_profiles:
        return 0

    if not any(profile.get("brand_token") == brand_query for profile in row_profiles):
        return 0

    return coverage_rank


def _explicit_cleaning_bundle_signature(
    exact_query: str,
    variants: list[str],
) -> str | None:
    for value in [exact_query, *variants]:
        signature = extract_cleaning_bundle_signature(value)
        if signature:
            return signature
    return None


def _cleaning_package_rank(
    row: pd.Series,
    exact_query: str,
    variants: list[str],
) -> int:
    query_tokens = _query_token_set(exact_query, variants)
    preferred_subtypes = preferred_cleaning_subtypes(query_tokens)
    if not preferred_subtypes:
        return 0

    row_subtypes = _row_cleaning_subtypes(row)
    if not row_subtypes:
        return 0

    primary_subtype = preferred_subtypes[0]
    if primary_subtype not in row_subtypes:
        return 0

    row_formats = _row_cleaning_package_formats(row)
    explicit_bundle_signature = _explicit_cleaning_bundle_signature(
        exact_query,
        variants,
    )
    if explicit_bundle_signature:
        row_bundle_signatures = _row_cleaning_bundle_signatures(row)
        return 0 if explicit_bundle_signature in row_bundle_signatures else 1

    if "sprey" in query_tokens:
        return 0 if "spray" in row_formats else 1

    if primary_subtype == "dishwashing_liquid":
        if "spray" in row_formats:
            return 2
        if "multipack_bundle" in row_formats:
            return 1
        if "standard_single_pack" in row_formats:
            return 0

    return 0


def _cleaning_variant_rank(
    row: pd.Series,
    exact_query: str,
    variants: list[str],
) -> int:
    query_tokens = _query_token_set(exact_query, variants)
    brand_query = cleaning_brand_from_query_tokens(query_tokens)
    if brand_query not in {"fairy", "pril"}:
        return 0

    if len(query_tokens - {brand_query}) > 0:
        return 0

    row_subtypes = _row_cleaning_subtypes(row)
    if "dishwashing_liquid" not in row_subtypes:
        return 0

    row_tokens = _row_tokens(row)
    scent_tokens = {
        "elma",
        "limon",
        "portakal",
        "nar",
        "bergamot",
        "lavanta",
        "yasemin",
        "okyanus",
    }
    has_scent_token = bool(scent_tokens & row_tokens)
    has_sivi_token = bool({"sivi"} & row_tokens)

    if has_sivi_token and not has_scent_token:
        return 0
    if not has_scent_token:
        return 1
    if has_sivi_token:
        return 2
    return 3


def _cleaning_context_rank(
    row: pd.Series,
    exact_query: str,
    variants: list[str],
) -> int:
    query_tokens = _query_token_set(exact_query, variants)
    subtype_order = preferred_cleaning_subtypes(query_tokens)
    if not subtype_order:
        return 0

    primary_subtype = subtype_order[0]
    row_subtypes = _row_cleaning_subtypes(row)
    if primary_subtype not in row_subtypes:
        return 0

    row_tokens = _row_tokens(row)
    required_tokens_by_subtype = {
        "dishwashing_liquid": ({"bulasik"}, {"deterjan", "deterjani"}),
        "dishwasher_tablet": ({"tablet", "tableti", "kapsul", "kapsulu", "pods"},),
        "dishwasher_salt": ({"tuz", "tuzu"}, {"bulasik", "makinesi", "makine"}),
        "rinse_aid": ({"parlatici", "parlaticisi"},),
        "dishwasher_cleaner": ({"temizleyici"}, {"bulasik", "makinesi", "makine"}),
        "laundry_liquid": ({"camasir"}, {"deterjan", "deterjani"}, {"sivi", "jel"}),
        "laundry_powder": ({"camasir"}, {"deterjan", "deterjani"}, {"toz"}),
        "laundry_capsule": ({"camasir"}, {"kapsul", "kapsulu", "pods"}),
        "bleach": ({"camasir"}, {"suyu"}),
        "surface_cleaner": ({"yuzey", "temizleyici"},),
    }

    required_groups = required_tokens_by_subtype.get(primary_subtype)
    if not required_groups:
        return 0

    return 0 if all(group & row_tokens for group in required_groups) else 1


def _should_suppress_generic_variant(
    product_name: str,
    exact_query: str,
    variants: list[str],
    query_penalty: int,
) -> bool:
    query_tokens = _query_token_set(exact_query, variants)
    if query_tokens != {"ekmek"}:
        return False

    product_tokens = _token_set(product_name)
    return "ekmek" in product_tokens and query_penalty > 0


def _demote_generic_safe_variants(
    search_results: pd.DataFrame,
    exact_query: str,
    variants: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if search_results.empty:
        return search_results.copy(), search_results.copy()

    query_tokens = _query_token_set(exact_query, variants)
    if query_tokens != {"ekmek"}:
        return split_search_results_by_safety(search_results)

    safe_mask = search_results["coverage_status"].isin(SAFE_SEARCH_COVERAGE_STATUSES)
    if not safe_mask.any():
        return split_search_results_by_safety(search_results)

    demoted_indexes = []
    for index, row in search_results[safe_mask].iterrows():
        product_name = row["standardized_product_name"]
        query_penalty = _generic_query_penalty(product_name, exact_query, variants)
        if _should_suppress_generic_variant(
            product_name,
            exact_query,
            variants,
            query_penalty,
        ):
            demoted_indexes.append(index)

    if not demoted_indexes:
        return split_search_results_by_safety(search_results)

    demoted_mask = search_results.index.isin(demoted_indexes)
    safe_results = search_results[safe_mask & ~demoted_mask].reset_index(drop=True)
    related_results = search_results[~safe_mask | demoted_mask].reset_index(drop=True)
    return safe_results, related_results


def search_product_catalog(
    catalog_df: pd.DataFrame,
    search_text: str,
) -> pd.DataFrame:
    if not search_text.strip() or catalog_df.empty:
        return catalog_df.copy()

    exact_query, variants = _query_variants(search_text)
    ranked_rows = []

    for index, row in catalog_df.iterrows():
        product_name = row["standardized_product_name"]
        match = _match_product(row, exact_query, variants)
        if match is None:
            continue

        match_rank, match_score = match
        source_count = int(row.get("source_count") or 0)
        coverage_status = _effective_coverage_status(row)
        if coverage_status == "comparable":
            coverage_rank = 0
        elif coverage_status == "comparison_review_required":
            coverage_rank = 2
        else:
            coverage_rank = 1 if source_count > 1 else 2
        cleaning_brand_coverage_rank = _cleaning_brand_coverage_rank(
            row,
            exact_query,
            variants,
            coverage_rank,
        )
        query_penalty = _generic_query_penalty(row, exact_query, variants)
        cola_penalty = _cola_row_penalty(row, exact_query, variants)
        cola_package_rank = _cola_package_rank(row, exact_query, variants)
        cola_brand_rank = _cola_brand_rank(row, _query_token_set(exact_query, variants))
        ranked_rows.append(
            (
                _paper_roll_selection_rank(row, exact_query, variants),
                _paper_brand_selection_rank(row, exact_query, variants),
                _paper_brand_pair_rank(row, exact_query, variants),
                _cleaning_query_preference_rank(row, exact_query, variants),
                cleaning_brand_coverage_rank,
                _cleaning_variant_rank(row, exact_query, variants),
                _cleaning_package_rank(row, exact_query, variants),
                _cleaning_context_rank(row, exact_query, variants),
                match_rank,
                _generic_query_primary_rank(row, exact_query, variants),
                query_penalty + cola_penalty,
                coverage_rank,
                -match_score,
                cola_package_rank,
                _generic_query_size_rank(row, exact_query, variants),
                _generic_query_preference_rank(row, exact_query, variants),
                cola_brand_rank,
                _cola_price_rank(row, exact_query, variants),
                len(_token_set(product_name)),
                product_name,
                index,
            )
        )

    if not ranked_rows:
        return catalog_df.iloc[0:0].copy()

    ranked_rows.sort()
    ranked_indexes = [row[-1] for row in ranked_rows]
    return catalog_df.loc[ranked_indexes].reset_index(drop=True)


def split_search_results_by_safety(
    search_results: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if search_results.empty:
        return search_results.copy(), search_results.copy()

    effective_statuses = search_results.apply(_effective_coverage_status, axis=1)
    safe_mask = effective_statuses.isin(SAFE_SEARCH_COVERAGE_STATUSES)
    safe_results = search_results[safe_mask].reset_index(drop=True)
    related_results = search_results[~safe_mask].reset_index(drop=True)
    return safe_results, related_results


def build_search_group_sections(
    catalog_df: pd.DataFrame,
    search_text: str,
) -> dict[str, object]:
    search_results = search_product_catalog(catalog_df, search_text)
    exact_query, variants = _query_variants(search_text)
    query_tokens = _query_token_set(exact_query, variants)
    review_query_key = _generic_review_query_key(query_tokens)
    if review_query_key:
        related_groups = build_product_family_groups(search_results)
        family_groups = [
            group
            for group in related_groups
            if group.get("selection_type") == "product_family"
        ]
        if family_groups:
            related_groups = family_groups
        normalized_groups = []
        for group in related_groups:
            updated_group = dict(group)
            if updated_group.get("selection_type") == "product_family":
                updated_group["force_review"] = True
            normalized_groups.append(updated_group)

        normalized_groups.sort(
            key=lambda group: _group_sort_key(group, review_query_key)
        )
        return {
            "search_results": search_results,
            "safe_groups": [],
            "related_groups": normalized_groups,
        }

    safe_results, related_results = _demote_generic_safe_variants(
        search_results,
        exact_query,
        variants,
    )
    return {
        "search_results": search_results,
        "safe_groups": build_product_family_groups(safe_results),
        "related_groups": build_product_family_groups(related_results),
    }


def get_product_family_id(product_name: str) -> str | None:
    for family_id in PRODUCT_FAMILY_DEFINITIONS:
        if family_id in PRODUCE_FAMILY_IDS and is_cleaning_context_text(product_name):
            continue
        if any(
            _term_matches_product(product_name, term)
            for term in _family_terms(family_id)
        ):
            return family_id

    return None


def get_product_family_label(family_id: str) -> str:
    family = PRODUCT_FAMILY_DEFINITIONS.get(family_id)
    if not family:
        return family_id
    return family["label"]


def product_selection_id(product_name: str) -> str:
    return f"{PRODUCT_SELECTION_PREFIX}{product_name}"


def family_selection_id(family_id: str) -> str:
    return f"{FAMILY_SELECTION_PREFIX}{family_id}"


def build_product_family_groups(catalog_df: pd.DataFrame) -> list[dict]:
    groups: dict[str, dict] = {}

    if catalog_df.empty:
        return []

    for _, row in catalog_df.iterrows():
        product_name = row["standardized_product_name"]
        family_id = get_product_family_id(product_name)

        if family_id:
            selection_id = family_selection_id(family_id)
            group = groups.setdefault(
                selection_id,
                {
                    "selection_id": selection_id,
                    "selection_type": "product_family",
                    "family_id": family_id,
                    "family_label": get_product_family_label(family_id),
                    "product_names": [],
                },
            )
        else:
            selection_id = product_selection_id(product_name)
            group = groups.setdefault(
                selection_id,
                {
                    "selection_id": selection_id,
                    "selection_type": "product",
                    "family_id": None,
                    "family_label": None,
                    "product_names": [],
                },
            )

        if product_name not in group["product_names"]:
            group["product_names"].append(product_name)

    return list(groups.values())


def format_product_family_group(group: dict) -> str:
    option_count = len(group.get("product_names") or [])
    if group.get("selection_type") == "product_family":
        return f"{group['family_label']} ({option_count} seçenek)"

    product_names = group.get("product_names") or []
    return product_names[0] if product_names else group["selection_id"]


def build_optimizer_input_from_group(group: dict) -> dict | str:
    product_names = group.get("product_names") or []
    if group.get("selection_type") != "product_family":
        return product_names[0] if product_names else ""

    optimizer_input = {
        "type": "product_family",
        "family_id": group["family_id"],
        "family_label": group["family_label"],
        "product_names": product_names,
    }
    if group.get("force_review"):
        optimizer_input["force_review"] = True
    return optimizer_input
