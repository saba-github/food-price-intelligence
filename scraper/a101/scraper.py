import logging
import os
import re

from playwright.sync_api import sync_playwright

logger = logging.getLogger(__name__)

AUTOMATION_ENV_VARS = ("CI", "GITHUB_ACTIONS")
HEADLESS_TRUTHY_VALUES = {"1", "true", "yes", "on"}


def repair_text(text: str) -> str:
    if not text:
        return ""

    if not any(marker in text for marker in ("Ã", "Å", "Ä", "â", "Â")):
        return text

    for source_encoding in ("cp1252", "latin1"):
        try:
            return text.encode(source_encoding).decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            continue

    return text


def normalize_text(text: str) -> str:
    return " ".join(repair_text(text).split())


def should_run_headless() -> bool:
    override = os.getenv("A101_HEADLESS")
    if override is not None:
        return override.strip().lower() in HEADLESS_TRUTHY_VALUES

    return any(os.getenv(env_var) for env_var in AUTOMATION_ENV_VARS)


def extract_unit_info(product_name: str):
    if not product_name:
        return None, None

    normalized_name = normalize_text(product_name)

    patterns = [
        (r"(\d+(?:[.,]\d+)?)\s*(kg|KG|Kg)", "KG"),
        (r"(\d+(?:[.,]\d+)?)\s*(g|G|gr|GR)", "GRAM"),
        (r"(\d+(?:[.,]\d+)?)\s*(l|L|lt|LT)", "LITER"),
        (r"(\d+(?:[.,]\d+)?)\s*(ml|ML)", "ML"),
    ]

    for pattern, unit in patterns:
        match = re.search(pattern, normalized_name)
        if match:
            amount = match.group(1).replace(",", ".")
            try:
                return unit, float(amount)
            except ValueError:
                return unit, None

    return None, None


def is_valid_product_name(name: str) -> bool:
    if not name:
        return False

    normalized_name = normalize_text(name)

    invalid_fragments = [
        "Popüler Ürünler",
        "Çerez Kullanımı",
        "Kampanyalar",
        "Giriş Yap",
        "Sepetim",
        "Aramak istediğin ürünü yaz",
        "Böyle bir sayfa bulamadık",
        "A101 Hep Ucuz",
    ]

    for fragment in invalid_fragments:
        if fragment.lower() in normalized_name.lower():
            return False

    return True


def parse_price_from_lines(lines):
    for line in lines:
        normalized_line = normalize_text(line)
        upper_line = normalized_line.upper()

        if "₺" not in normalized_line and "TL" not in upper_line and "TRY" not in upper_line:
            continue

        price_candidates = re.findall(r"\d[\d.,]*", normalized_line)

        for candidate in price_candidates:
            raw_price = candidate.replace(".", "").replace(",", ".").strip()
            try:
                return float(raw_price)
            except ValueError:
                continue

    return None


def get_a101_products(category_slug: str):
    url = f"https://www.a101.com.tr/kapida/{category_slug}/"
    products = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=should_run_headless())
        page = browser.new_page()
        page.goto(url, timeout=60000)

        for text in [
            "Bu defalık izin ver",
            "Siteyi ziyaret ederken izin ver",
            "Hiçbir zaman izin verme",
        ]:
            try:
                page.get_by_text(text, exact=True).click(timeout=3000)
                break
            except Exception:
                pass

        for text in ["KABUL ET", "Kabul Et", "Tümünü Kabul Et"]:
            try:
                page.get_by_text(text, exact=True).click(timeout=3000)
                break
            except Exception:
                pass

        page.wait_for_timeout(4000)

        for _ in range(8):
            page.mouse.wheel(0, 2500)
            page.wait_for_timeout(1200)

        cards = page.locator("div[class*='product']").all()

        if not cards:
            logger.warning("No A101 product cards found for category_slug=%s", category_slug)

        for i, card in enumerate(cards):
            try:
                text_blob = repair_text(card.inner_text()).strip()
                if not text_blob:
                    continue

                lines = [
                    normalize_text(line)
                    for line in text_blob.splitlines()
                    if normalize_text(line)
                ]
                if not lines:
                    continue

                price = parse_price_from_lines(lines)
                if price is None:
                    logger.warning(
                        "Skipping A101 card index=%s reason=price_not_found",
                        i,
                    )
                    continue

                name = None
                for line in lines:
                    upper_line = line.upper()
                    if "₺" not in line and "TL" not in upper_line and "TRY" not in upper_line and len(line) > 2:
                        if is_valid_product_name(line):
                            name = line
                            break

                if not name:
                    logger.warning(
                        "Skipping A101 card index=%s reason=name_not_found",
                        i,
                    )
                    continue

                extracted_unit, extracted_amount = extract_unit_info(name)

                products.append(
                    {
                        "product_id": f"a101_{i}",
                        "product_name": name,
                        "sku": f"a101_{i}",
                        "shown_price_tl": price,
                        "regular_price_tl": price,
                        "discount_rate": None,
                        "product_url": url,
                        "brand_name": None,
                        "category_name": "Meyve ve Sebze",
                        "unit": extracted_unit,
                        "unit_amount": extracted_amount,
                    }
                )

            except Exception as exc:
                logger.warning(
                    "Skipping A101 card index=%s error=%s",
                    i,
                    str(exc),
                )
                continue

        browser.close()

    return products
