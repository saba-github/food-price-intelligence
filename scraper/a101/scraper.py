import logging
import os
import re
from urllib.parse import urljoin

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

logger = logging.getLogger(__name__)

AUTOMATION_ENV_VARS = ("CI", "GITHUB_ACTIONS")
HEADLESS_TRUTHY_VALUES = {"1", "true", "yes", "on"}
TEXT_REPAIR_MARKERS = ("Ãƒ", "Ã„", "Ã…", "Ã¢", "Ã‚")
POPUP_TEXTS = (
    "Bu defalÄ±k izin ver",
    "Siteyi ziyaret ederken izin ver",
    "HiÃ§bir zaman izin verme",
)
COOKIE_TEXTS = (
    "TÃ¼mÃ¼nÃ¼ Kabul Et",
    "TÃ¼mÃ¼nÃ¼ kabul et",
    "KABUL ET",
    "Kabul Et",
)
PRODUCT_CARD_SELECTORS = (
    "a[href*='_p-']",
    "article:has(a[href*='_p-'])",
    "div[class*='product']",
)
DECIMAL_PRICE_PATTERN = re.compile(r"(?<!\d)(\d{1,4})[.,](\d{2})(?!\d)")
WHOLE_PRICE_LINE_PATTERN = re.compile(r"^\d{1,4}$")
FRACTION_PRICE_LINE_PATTERN = re.compile(r"^\d{2}$")


def repair_text(text: str) -> str:
    if not text:
        return ""

    repaired = text
    for _ in range(2):
        if not any(marker in repaired for marker in TEXT_REPAIR_MARKERS):
            break

        for source_encoding in ("cp1252", "latin1"):
            try:
                candidate = repaired.encode(source_encoding).decode("utf-8")
            except (UnicodeEncodeError, UnicodeDecodeError):
                continue

            if candidate != repaired:
                repaired = candidate
                break
        else:
            break

    return repaired


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


def line_has_price_indicator(line: str) -> bool:
    normalized_line = normalize_text(line)
    upper_line = normalized_line.upper()
    return (
        "₺" in normalized_line
        or "â‚º" in normalized_line
        or "TL" in upper_line
        or "TRY" in upper_line
    )


def parse_decimal_price(text: str):
    normalized_text = normalize_text(text)
    match = DECIMAL_PRICE_PATTERN.search(normalized_text)
    if not match:
        return None

    try:
        return float(f"{match.group(1)}.{match.group(2)}")
    except ValueError:
        return None


def parse_price_from_lines(lines):
    normalized_lines = [normalize_text(line) for line in lines if normalize_text(line)]

    for normalized_line in normalized_lines:
        if not line_has_price_indicator(normalized_line):
            continue

        price = parse_decimal_price(normalized_line)
        if price is not None:
            return price

        price_candidates = re.findall(r"\d[\d.,]*", normalized_line)

        for candidate in price_candidates:
            raw_price = candidate.replace(".", "").replace(",", ".").strip()
            try:
                return float(raw_price)
            except ValueError:
                continue

    for normalized_line in normalized_lines:
        price = parse_decimal_price(normalized_line)
        if price is not None:
            return price

    for current_line, next_line in zip(normalized_lines, normalized_lines[1:]):
        if WHOLE_PRICE_LINE_PATTERN.fullmatch(current_line) and FRACTION_PRICE_LINE_PATTERN.fullmatch(next_line):
            try:
                return float(f"{current_line}.{next_line}")
            except ValueError:
                continue

    return None


def dismiss_optional_overlay(page, texts) -> bool:
    for text in texts:
        for locator in (
            page.get_by_role("button", name=text, exact=False),
            page.get_by_text(text, exact=False),
        ):
            try:
                locator.first.click(timeout=2500)
                page.wait_for_timeout(500)
                return True
            except Exception:
                continue

    return False


def wait_for_catalog_page(page) -> None:
    try:
        page.wait_for_load_state("domcontentloaded", timeout=10000)
    except PlaywrightTimeoutError:
        pass

    try:
        page.wait_for_load_state("networkidle", timeout=10000)
    except PlaywrightTimeoutError:
        pass

    page.wait_for_timeout(1500)


def scroll_catalog(page, passes: int = 8) -> None:
    last_height = 0

    for _ in range(passes):
        try:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(1000)
            current_height = page.evaluate("document.body.scrollHeight")
        except Exception:
            page.wait_for_timeout(1000)
            continue

        if current_height == last_height:
            page.wait_for_timeout(500)
        last_height = current_height


def get_product_cards(page):
    for selector in PRODUCT_CARD_SELECTORS:
        locator = page.locator(selector)

        try:
            count = locator.count()
        except Exception:
            continue

        if count:
            logger.info(
                "Found A101 product cards selector=%s count=%s",
                selector,
                count,
            )
            return locator.all()

    return []


def prepare_catalog_page(page) -> None:
    wait_for_catalog_page(page)
    dismiss_optional_overlay(page, POPUP_TEXTS)
    dismiss_optional_overlay(page, COOKIE_TEXTS)
    wait_for_catalog_page(page)
    scroll_catalog(page)
    wait_for_catalog_page(page)


def extract_product_url(card, fallback_url: str) -> str:
    try:
        href = card.get_attribute("href")
    except Exception:
        href = None

    if not href:
        try:
            href = card.locator("a[href]").first.get_attribute("href")
        except Exception:
            href = None

    if href:
        return urljoin(fallback_url, href)

    return fallback_url


def get_a101_products(category_slug: str):
    url = f"https://www.a101.com.tr/kapida/{category_slug}/"
    products = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=should_run_headless())
        page = browser.new_page(viewport={"width": 1440, "height": 2200})
        page.goto(url, wait_until="domcontentloaded", timeout=60000)

        prepare_catalog_page(page)
        cards = get_product_cards(page)

        if not cards:
            logger.warning(
                "Retrying A101 catalog load category_slug=%s url=%s",
                category_slug,
                page.url,
            )
            page.reload(wait_until="domcontentloaded", timeout=60000)
            prepare_catalog_page(page)
            cards = get_product_cards(page)

        if not cards:
            logger.warning(
                "No A101 product cards found for category_slug=%s url=%s title=%s",
                category_slug,
                page.url,
                page.title(),
            )

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
                    if not line_has_price_indicator(line) and len(line) > 2:
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
                        "product_url": extract_product_url(card, url),
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
