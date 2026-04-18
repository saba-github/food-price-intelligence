import logging
import os
import re
import unicodedata
from urllib.parse import urljoin

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

logger = logging.getLogger(__name__)

AUTOMATION_ENV_VARS = ("CI", "GITHUB_ACTIONS")
HEADLESS_TRUTHY_VALUES = {"1", "true", "yes", "on"}
TEXT_REPAIR_MARKERS = ("ÃƒÆ’", "Ãƒâ€", "Ãƒâ€¦", "ÃƒÂ¢", "Ãƒâ€š")
POPUP_TEXTS = (
    "Bu defalÃ„Â±k izin ver",
    "Siteyi ziyaret ederken izin ver",
    "HiÃƒÂ§bir zaman izin verme",
)
COOKIE_TEXTS = (
    "TÃƒÂ¼mÃƒÂ¼nÃƒÂ¼ Kabul Et",
    "TÃƒÂ¼mÃƒÂ¼nÃƒÂ¼ kabul et",
    "KABUL ET",
    "Kabul Et",
)
PRODUCT_CARD_SELECTORS = (
    "a[href*='_p-']",
    "article:has(a[href*='_p-'])",
    "div[class*='product']",
)
CARD_CONTEXT_SELECTORS = (
    "xpath=ancestor::article[1]",
    "xpath=ancestor::li[1]",
    "xpath=ancestor::div[contains(translate(@class,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'product')][1]",
    "xpath=ancestor::div[contains(translate(@class,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'item')][1]",
)
NAME_ATTRIBUTE_SELECTORS = (
    ("img[alt]", "alt"),
    ("img[title]", "title"),
    ("[title]", "title"),
)
NAME_TEXT_SELECTORS = (
    "[class*='name']",
    "[class*='Name']",
    "[class*='title']",
    "[class*='Title']",
    "[class*='description']",
    "[class*='Description']",
)
PRICE_TEXT_SELECTORS = (
    "[data-testid*='price']",
    "[class*='currentPrice']",
    "[class*='CurrentPrice']",
    "[class*='salePrice']",
    "[class*='SalePrice']",
    "[class*='newPrice']",
    "[class*='NewPrice']",
    "[class*='finalPrice']",
    "[class*='FinalPrice']",
    "[class*='priceText']",
    "[class*='PriceText']",
    "[class*='price']",
    "[class*='Price']",
)
MAX_PRICE_FAILURE_DIAGNOSTICS = 3
MAX_HTML_SNIPPET_LENGTH = 400
DECIMAL_PRICE_PATTERN = re.compile(r"(?<!\d)(\d{1,4})[.,](\d{2})(?!\d)")
INLINE_SPLIT_PRICE_PATTERN = re.compile(r"(?<!\d)(\d{1,4})\s+(\d{2})(?!\d)")
WHOLE_PRICE_LINE_PATTERN = re.compile(r"^\d{1,4}$")
FRACTION_PRICE_LINE_PATTERN = re.compile(r"^\d{2}$")
IGNORED_PRICE_TEXT_MARKERS = ("indirim", "%")
IGNORED_PRICE_METADATA_MARKERS = ("badge", "campaign", "old", "strike", "cross", "percent")


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


def split_text_lines(text: str) -> list[str]:
    if not text:
        return []

    return [normalize_text(line) for line in repair_text(text).splitlines() if normalize_text(line)]


def fold_for_match(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", normalize_text(text).casefold())
    return "".join(char for char in normalized if not unicodedata.combining(char))


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

    normalized_name = fold_for_match(name)

    invalid_fragments = [
        "populer urunler",
        "cerez kullanimi",
        "kampanyalar",
        "giris yap",
        "sepetim",
        "aramak istedigin urunu yaz",
        "boyle bir sayfa bulamadik",
        "a101 hep ucuz",
    ]

    for fragment in invalid_fragments:
        if fragment in normalized_name:
            return False

    return True


def line_has_price_indicator(line: str) -> bool:
    normalized_line = normalize_text(line)
    upper_line = normalized_line.upper()
    return (
        "â‚º" in normalized_line
        or "Ã¢â€šÂº" in normalized_line
        or "TL" in upper_line
        or "TRY" in upper_line
    )


def parse_decimal_price(text: str):
    normalized_text = normalize_text(text)

    for pattern in (DECIMAL_PRICE_PATTERN, INLINE_SPLIT_PRICE_PATTERN):
        match = pattern.search(normalized_text)
        if not match:
            continue

        try:
            return float(f"{match.group(1)}.{match.group(2)}")
        except ValueError:
            continue

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


def sanitize_html_snippet(html: str) -> str:
    if not html:
        return ""

    sanitized = re.sub(r"\s+", " ", repair_text(html)).strip()
    if len(sanitized) > MAX_HTML_SNIPPET_LENGTH:
        return sanitized[:MAX_HTML_SNIPPET_LENGTH] + "..."

    return sanitized


def safe_locator_count(locator) -> int:
    try:
        return locator.count()
    except Exception:
        return 0


def get_product_cards(page):
    for selector in PRODUCT_CARD_SELECTORS:
        locator = page.locator(selector)
        count = safe_locator_count(locator)

        if count:
            logger.info(
                "Found A101 product cards selector=%s count=%s",
                selector,
                count,
            )
            return locator.all(), selector

    return [], None


def prepare_catalog_page(page) -> None:
    wait_for_catalog_page(page)
    dismiss_optional_overlay(page, POPUP_TEXTS)
    dismiss_optional_overlay(page, COOKIE_TEXTS)
    wait_for_catalog_page(page)
    scroll_catalog(page)
    wait_for_catalog_page(page)


def context_has_product_details(context) -> bool:
    detail_selectors = (
        "img[alt]",
        "img[title]",
        "[data-testid*='price']",
        "[class*='price']",
        "[class*='Price']",
        "[class*='name']",
        "[class*='title']",
    )
    return any(safe_locator_count(context.locator(selector)) for selector in detail_selectors)


def get_card_context(card, matched_selector: str | None):
    if matched_selector != "a[href*='_p-']":
        return card

    for selector in CARD_CONTEXT_SELECTORS:
        locator = card.locator(selector)
        if not safe_locator_count(locator):
            continue

        candidate = locator.first
        if context_has_product_details(candidate):
            return candidate

    return card


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


def should_ignore_price_text(text: str, metadata: str = "") -> bool:
    folded_text = fold_for_match(text)
    if not folded_text:
        return True

    if any(marker in folded_text for marker in IGNORED_PRICE_TEXT_MARKERS):
        return True

    if metadata and any(marker in metadata for marker in IGNORED_PRICE_METADATA_MARKERS):
        return True

    return False


def extract_locator_lines(locator) -> list[str]:
    try:
        return split_text_lines(locator.inner_text())
    except Exception:
        return []


def extract_locator_attribute(locator, attribute: str) -> str:
    try:
        value = locator.get_attribute(attribute)
    except Exception:
        value = None

    return normalize_text(value) if value else ""


def collect_node_metadata(locator) -> str:
    attributes = []
    for attribute in ("class", "id", "data-testid", "aria-label"):
        value = extract_locator_attribute(locator, attribute)
        if value:
            attributes.append(value)
    return fold_for_match(" ".join(attributes))


def extract_price_from_context(context):
    for selector in PRICE_TEXT_SELECTORS:
        locator = context.locator(selector)
        count = min(safe_locator_count(locator), 12)

        for idx in range(count):
            node = locator.nth(idx)
            metadata = collect_node_metadata(node)
            lines = [line for line in extract_locator_lines(node) if not should_ignore_price_text(line, metadata)]
            if not lines:
                continue

            price = parse_price_from_lines(lines)
            if price is not None:
                return price

    return None


def is_candidate_name(text: str) -> bool:
    if not text or len(text) <= 2:
        return False

    if parse_decimal_price(text) is not None:
        return False

    if line_has_price_indicator(text):
        return False

    if should_ignore_price_text(text):
        return False

    return is_valid_product_name(text)


def extract_name_from_context(context, fallback_lines: list[str]):
    for selector, attribute in NAME_ATTRIBUTE_SELECTORS:
        locator = context.locator(selector)
        count = min(safe_locator_count(locator), 6)

        for idx in range(count):
            candidate = extract_locator_attribute(locator.nth(idx), attribute)
            if is_candidate_name(candidate):
                return candidate

    for selector in NAME_TEXT_SELECTORS:
        locator = context.locator(selector)
        count = min(safe_locator_count(locator), 6)

        for idx in range(count):
            for candidate in extract_locator_lines(locator.nth(idx)):
                if is_candidate_name(candidate):
                    return candidate

    for line in fallback_lines:
        if is_candidate_name(line):
            return line

    return None


def log_price_failure_diagnostic(context, index: int, lines: list[str], matched_selector: str | None) -> None:
    try:
        html_snippet = sanitize_html_snippet(context.inner_html())
    except Exception as exc:
        html_snippet = f"<html_unavailable:{exc}>"

    logger.warning(
        "A101 price parse diagnostic index=%s selector=%s lines=%s html=%s",
        index,
        matched_selector or "unknown",
        lines,
        html_snippet,
    )


def get_a101_products(category_slug: str):
    url = f"https://www.a101.com.tr/kapida/{category_slug}/"
    products = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=should_run_headless())
        page = browser.new_page(viewport={"width": 1440, "height": 2200})
        page.goto(url, wait_until="domcontentloaded", timeout=60000)

        prepare_catalog_page(page)
        cards, matched_selector = get_product_cards(page)

        if not cards:
            logger.warning(
                "Retrying A101 catalog load category_slug=%s url=%s",
                category_slug,
                page.url,
            )
            page.reload(wait_until="domcontentloaded", timeout=60000)
            prepare_catalog_page(page)
            cards, matched_selector = get_product_cards(page)

        if not cards:
            logger.warning(
                "No A101 product cards found for category_slug=%s url=%s title=%s",
                category_slug,
                page.url,
                page.title(),
            )

        price_failure_diagnostics = 0

        for i, card in enumerate(cards):
            try:
                context = get_card_context(card, matched_selector)

                try:
                    context_text_blob = repair_text(context.inner_text()).strip()
                except Exception:
                    context_text_blob = repair_text(card.inner_text()).strip()

                if not context_text_blob:
                    continue

                context_lines = split_text_lines(context_text_blob)
                if not context_lines:
                    continue

                price = extract_price_from_context(context)
                if price is None:
                    price = parse_price_from_lines(context_lines)

                if price is None:
                    if price_failure_diagnostics < MAX_PRICE_FAILURE_DIAGNOSTICS:
                        log_price_failure_diagnostic(context, i, context_lines, matched_selector)
                        price_failure_diagnostics += 1
                    logger.warning(
                        "Skipping A101 card index=%s reason=price_not_found",
                        i,
                    )
                    continue

                name = extract_name_from_context(context, context_lines)
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
