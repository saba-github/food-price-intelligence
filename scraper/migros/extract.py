from __future__ import annotations

from typing import Any, Optional


BASE_URL = "https://www.migros.com.tr"


def _safe_get(dct: Optional[dict[str, Any]], key: str, default: Any = None) -> Any:
    if not isinstance(dct, dict):
        return default
    return dct.get(key, default)


def _price_to_tl(value: Any) -> Optional[float]:
    if value is None:
        return None

    try:
        return round(float(value) / 100, 2)
    except (TypeError, ValueError):
        return None


def _extract_category_hierarchy(category_ascendants: Optional[list[dict[str, Any]]]) -> dict[str, Optional[str]]:
    if not category_ascendants:
        return {
            "parent_category_name": None,
            "top_category_name": None,
            "parent_category_id": None,
            "top_category_id": None,
            "parent_category_pretty_name": None,
            "top_category_pretty_name": None,
        }

    parent = category_ascendants[0] if len(category_ascendants) >= 1 else {}
    top = category_ascendants[-1] if len(category_ascendants) >= 1 else {}

    return {
        "parent_category_name": _safe_get(parent, "name"),
        "top_category_name": _safe_get(top, "name"),
        "parent_category_id": _safe_get(parent, "id"),
        "top_category_id": _safe_get(top, "id"),
        "parent_category_pretty_name": _safe_get(parent, "prettyName"),
        "top_category_pretty_name": _safe_get(top, "prettyName"),
    }


def _extract_image_urls(images: Optional[list[dict[str, Any]]]) -> dict[str, Optional[str]]:
    if not images:
        return {
            "image_product_list": None,
            "image_product_detail": None,
            "image_product_hd": None,
            "image_cart": None,
        }

    first_image = images[0] if images else {}
    urls = _safe_get(first_image, "urls", {}) or {}

    return {
        "image_product_list": _safe_get(urls, "PRODUCT_LIST"),
        "image_product_detail": _safe_get(urls, "PRODUCT_DETAIL"),
        "image_product_hd": _safe_get(urls, "PRODUCT_HD"),
        "image_cart": _safe_get(urls, "CART"),
    }


def _extract_badges(badges: Optional[list[dict[str, Any]]]) -> list[str]:
    if not badges:
        return []

    cleaned_badges: list[str] = []
    for badge in badges:
        value = _safe_get(badge, "value")
        if value:
            cleaned_badges.append(str(value).strip())

    return cleaned_badges


def _build_product_url(pretty_name: Optional[str]) -> Optional[str]:
    if not pretty_name:
        return None
    return f"{BASE_URL}/{pretty_name}"


def parse_migros_products(api_json: dict[str, Any]) -> list[dict[str, Any]]:
    products: list[dict[str, Any]] = []

    items: list[dict[str, Any]] = []

    data = api_json.get("data", [])

    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        if isinstance(data.get("products"), list):
            items = data.get("products", [])
        elif isinstance(data.get("items"), list):
            items = data.get("items", [])
        elif isinstance(data.get("results"), list):
            items = data.get("results", [])
        elif isinstance(data.get("searchInfo"), dict):
            search_info = data.get("searchInfo", {})
            if isinstance(search_info.get("storeProductInfos"), list):
                items = search_info.get("storeProductInfos", [])
    else:
        items = []

    if not items:
        return products

    for item in items:
        if not isinstance(item, dict):
            continue

        brand = _safe_get(item, "brand", {}) or {}
        category = _safe_get(item, "category", {}) or {}
        category_ascendants = _safe_get(item, "categoryAscendants", []) or []
        images = _safe_get(item, "images", []) or []
        badges = _safe_get(item, "badges", []) or []
        social = _safe_get(item, "socialProofInfo", {}) or {}

        category_info = _extract_category_hierarchy(category_ascendants)
        image_info = _extract_image_urls(images)
        badge_values = _extract_badges(badges)

        pretty_name = _safe_get(item, "prettyName")
        product_url = _build_product_url(pretty_name)

        record = {
            "product_id": _safe_get(item, "id"),
            "store_id": _safe_get(item, "storeId"),
            "sku": _safe_get(item, "sku"),
            "pretty_name": pretty_name,
            "product_url": product_url,

            "product_name": _safe_get(item, "name"),
            "status": _safe_get(item, "status"),
            "sponsored": _safe_get(item, "sponsored"),
            "buy_now_applicable": _safe_get(item, "buyNowApplicable"),
            "product_note_appendable": _safe_get(item, "productNoteAppendable"),

            "brand_id": _safe_get(brand, "id"),
            "brand_name": _safe_get(brand, "name"),
            "brand_pretty_name": _safe_get(brand, "prettyName"),

            "category_id": _safe_get(category, "id"),
            "category_name": _safe_get(category, "name"),
            "category_pretty_name": _safe_get(category, "prettyName"),

            **category_info,

            "unit": _safe_get(item, "unit"),
            "unit_amount": _safe_get(item, "unitAmount"),
            "initial_increment_amount": _safe_get(item, "initialIncrementAmount"),
            "increment_amount": _safe_get(item, "incrementAmount"),
            "max_amount": _safe_get(item, "maxAmount"),

            "alternative_unit": _safe_get(item, "alternativeUnit"),
            "alternative_unit_value": _safe_get(item, "alternativeUnitValue"),
            "alternative_unit_initial_increment_amount": _safe_get(item, "alternativeUnitInitialIncrementAmount"),
            "alternative_unit_increment_amount": _safe_get(item, "alternativeUnitIncrementAmount"),
            "alternative_unit_max_amount": _safe_get(item, "alternativeUnitMaxAmount"),
            "use_only_alternative_unit": _safe_get(item, "useOnlyAlternativeUnit"),

            "regular_price_raw": _safe_get(item, "regularPrice"),
            "shown_price_raw": _safe_get(item, "shownPrice"),
            "regular_price_tl": _price_to_tl(_safe_get(item, "regularPrice")),
            "shown_price_tl": _price_to_tl(_safe_get(item, "shownPrice")),
            "discount_rate": _safe_get(item, "discountRate"),
            "unit_price_text": _safe_get(item, "unitPrice"),

            "badges": badge_values,
            "social_proof_priority": _safe_get(social, "socialProofPriority"),
            "social_proof_category_id": _safe_get(social, "categoryId"),
            "social_proof_category_name": _safe_get(social, "categoryName"),
            "social_proof_description": _safe_get(social, "description"),

            **image_info,

            "referrer_event_id": _safe_get(item, "referrerEventId"),
            "crm_discount_tags": _safe_get(item, "crmDiscountTags", []),
            "group_badge_map": _safe_get(item, "groupBadgeMap", {}),
        }

        products.append(record)

    return products