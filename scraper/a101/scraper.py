import re
from typing import Any

from playwright.sync_api import sync_playwright

from scraper.a101.http import build_session


A101_CATEGORY_API_URL = "https://rio.a101.com.tr/dbmk89vnr/CALL/Store/getProductsByCategory/VS032"
A101_CATEGORY_IDS = {
    "meyve-sebze": "C01",
    "meyve-sebze/meyve": "C0101",
    "meyve-sebze/sebze": "C0102",
    "meyve-sebze/yesillik": "C0103",
    "firindan": "C02",
    "firindan/ekmek": "C0201",
    "kagit-urunleri": "C13",
    "kagit-urunleri/tuvalet-kagidi": "C1301",
    "kagit-urunleri/kagit-havlu": "C1303",
    "temel-gida": "C07",
    "temel-gida/sivi-yaglar": "C0701",
    "temel-gida/bakliyat": "C0702",
    "temel-gida/seker": "C0703",
    "temel-gida/konserve": "C0704",
    "temel-gida/un": "C0705",
    "temel-gida/makarna-noodle": "C0706",
    "temel-gida/ketcap-mayonez-soslar-sirkeler": "C0707",
    "temel-gida/salca": "C0708",
    "temel-gida/tuz-baharat-harc": "C0709",
    "temel-gida/hazir-corbalar": "C0710",
    "temel-gida/tursu": "C0711",
    "temel-gida/bulyonlar": "C0712",
    "temel-gida/eriste-manti": "C0713",
    "su-icecek": "C08",
    "su-icecek/gazli-icecekler": "C0801",
    "su-icecek/gazsiz-icecekler": "C0802",
    "su-icecek/cay": "C0803",
    "su-icecek/maden-suyu": "C0804",
    "su-icecek/su": "C0805",
    "su-icecek/bitki-caylari": "C0806",
    "su-icecek/ayran-kefir": "C0807",
    "su-icecek/enerji-icecegi": "C0808",
    "su-icecek/kahve": "C0809",
    "sut-urunleri-kahvaltilik": "C05",
    "sut-urunleri-kahvaltilik/sut": "C0502",
    "sut-urunleri-kahvaltilik/yumurta": "C0503",
}


def extract_unit_info(product_name: str):
    if not product_name:
        return None, None

    patterns = [
        (r"(\d+(?:[.,]\d+)?)\s*(kg|KG|Kg)\b", "KG"),
        (r"(\d+(?:[.,]\d+)?)\s*(g|G|gr|GR)\b", "GRAM"),
        (r"(\d+(?:[.,]\d+)?)\s*(l|L|lt|LT)\b", "LITER"),
        (r"(\d+(?:[.,]\d+)?)\s*(ml|ML)\b", "ML"),
        (r"(\d+(?:[.,]\d+)?)\s*(adet|Adet|ADET)\b", "PIECE"),
        (r"(\d+(?:[.,]\d+)?)\s*(li|LI|'lu|'lü)\b", "PIECE"),
    ]

    for pattern, unit in patterns:
        match = re.search(pattern, product_name)
        if match:
            amount = match.group(1).replace(",", ".")
            try:
                return unit, float(amount)
            except ValueError:
                return unit, None

    return None, None


def _price_to_tl(value: Any):
    if value is None:
        return None

    try:
        return round(float(value) / 100, 2)
    except (TypeError, ValueError):
        return None


def _unit_from_api_product(product: dict[str, Any]):
    attributes = product.get("attributes") or {}
    name = attributes.get("name")

    unit, amount = extract_unit_info(name or "")
    if unit is not None:
        return unit, amount

    raw_unit = (
        attributes.get("salesUnitOfMeasure")
        or attributes.get("baseUnitOfMeasure")
        or ""
    ).upper()

    if raw_unit in {"KG", "KGM"}:
        return "KG", 1.0

    if raw_unit in {"G", "GR", "GRAM"}:
        return "GRAM", attributes.get("netWeight")

    if raw_unit in {"AD", "ADET", "PIECE"}:
        return "PIECE", 1.0

    return raw_unit or None, 1.0 if raw_unit else None


def _walk_category_products(category: dict[str, Any]):
    category_name = category.get("name")

    for product in category.get("products") or []:
        yield category_name, product

    for child in category.get("children") or []:
        yield from _walk_category_products(child)


def parse_a101_api_products(api_json: dict[str, Any]) -> list[dict[str, Any]]:
    products = []
    seen_product_ids = set()

    for category_name, product in _walk_category_products(api_json):
        product_id = product.get("id")
        if product_id in seen_product_ids:
            continue
        seen_product_ids.add(product_id)

        attributes = product.get("attributes") or {}
        price_info = product.get("price") or {}
        name = attributes.get("name")
        shown_price = _price_to_tl(price_info.get("discounted"))
        regular_price = _price_to_tl(price_info.get("normal"))

        if not name or shown_price is None:
            continue

        unit, unit_amount = _unit_from_api_product(product)
        discount_rate = None
        if regular_price and regular_price > shown_price:
            discount_rate = round((regular_price - shown_price) / regular_price, 4)

        products.append(
            {
                "product_id": str(product_id) if product_id is not None else None,
                "product_name": name,
                "sku": str(product_id) if product_id is not None else None,
                "shown_price_tl": shown_price,
                "regular_price_tl": regular_price if regular_price is not None else shown_price,
                "discount_rate": discount_rate,
                "product_url": attributes.get("seoUrl"),
                "brand_name": attributes.get("brand"),
                "category_name": category_name,
                "unit": unit,
                "unit_amount": unit_amount,
            }
        )

    return products


def _request_a101_category(category_id: str, category_slug: str) -> dict[str, Any]:
    with build_session() as session:
        response = session.get(
            A101_CATEGORY_API_URL,
            params={
                "id": category_id,
                "channel": "SLOT",
                "__culture": "tr-TR",
                "__platform": "web",
                "data": "e30=",
                "__isbase64": "true",
            },
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json",
                "Referer": f"https://www.a101.com.tr/kapida/{category_slug}",
            },
            timeout=30,
        )
        response.raise_for_status()
        return response.json()


def _find_category_by_id(category: dict[str, Any], category_id: str) -> dict[str, Any] | None:
    if category.get("id") == category_id:
        return category

    for child in category.get("children") or []:
        found = _find_category_by_id(child, category_id)
        if found is not None:
            return found

    return None


def _resolve_nested_category_from_parent(category_slug: str) -> dict[str, Any] | None:
    if "/" not in category_slug:
        return None

    parent_slug, _ = category_slug.rsplit("/", 1)
    parent_category_id = A101_CATEGORY_IDS.get(parent_slug)
    child_category_id = A101_CATEGORY_IDS.get(category_slug)

    if not parent_category_id or not child_category_id:
        return None

    parent_json = _request_a101_category(parent_category_id, parent_slug)
    category_json = _find_category_by_id(parent_json, child_category_id)

    if category_json is None:
        raise ValueError(f"A101 category not found in parent tree: {category_slug}")

    return category_json


def fetch_a101_category_api(category_slug: str) -> dict[str, Any]:
    category_id = A101_CATEGORY_IDS.get(category_slug, category_slug)
    nested_category = _resolve_nested_category_from_parent(category_slug)
    if nested_category is not None:
        return nested_category

    return _request_a101_category(category_id, category_slug)


def normalize_category_name(category_slug: str) -> str:
    if category_slug.endswith("/meyve"):
        return "Meyve"
    if category_slug.endswith("/sebze"):
        return "Sebze"
    if category_slug.endswith("/yesillik"):
        return "Yesillik"
    return category_slug.replace("-", " ").title()


def get_section_header(category_slug: str) -> str | None:
    if category_slug.endswith("/meyve"):
        return "Meyve"
    if category_slug.endswith("/sebze"):
        return "Sebze"
    if category_slug.endswith("/yesillik"):
        return "Yeşillik"
    return None


def is_price_line(text: str) -> bool:
    return bool(re.match(r"^₺\s*[\d\.]+(?:,\d{1,2})?$", text))


def find_best_section_start(lines: list[str], section_header: str) -> int | None:
    candidate_indices = [i for i, line in enumerate(lines) if line == section_header]

    if not candidate_indices:
        return None

    best_idx = None
    best_score = -1

    for idx in candidate_indices:
        window = lines[idx : idx + 80]
        score = sum(1 for line in window if is_price_line(line))

        if score > best_score:
            best_score = score
            best_idx = idx

    if best_score <= 0:
        return None

    return best_idx


def parse_products_from_body_text(body_text: str, category_slug: str):
    lines = [line.strip() for line in body_text.splitlines() if line.strip()]

    section_header = get_section_header(category_slug)
    if not section_header:
        print(f"DEBUG - section header missing for category_slug={category_slug}")
        return []

    start_idx = find_best_section_start(lines, section_header)
    if start_idx is None:
        print(f"DEBUG - section header not found or no priced rows nearby: {section_header}")
        return []

    stop_headers = {"Meyve", "Sebze", "Yeşillik", "Sepetim", "Giriş Yap", "Site Haritası"}

    section_lines = []
    for line in lines[start_idx + 1 :]:
        if line in stop_headers and line != section_header:
            break
        section_lines.append(line)

    print(f"DEBUG - section_header={section_header}")
    print(f"DEBUG - section_start_idx={start_idx}")
    print(f"DEBUG - section_lines_sample={section_lines[:40]}")

    products = []
    seen_names = set()

    i = 0
    while i < len(section_lines) - 1:
        name = section_lines[i]
        next_line = section_lines[i + 1]

        if is_price_line(next_line):
            lowered = name.lower()

            if any(
                x in lowered
                for x in [
                    "kampanyalar",
                    "giriş yap",
                    "sepetim",
                    "anasayfa",
                    "site haritası",
                    "yardım",
                    "iletişim",
                    "kategoriler",
                ]
            ):
                i += 1
                continue

            raw_price = next_line.replace("₺", "").replace(".", "").replace(",", ".").strip()

            try:
                price = float(raw_price)
            except ValueError:
                i += 1
                continue

            normalized_name = name.lower().strip()
            if normalized_name in seen_names:
                i += 2
                continue

            seen_names.add(normalized_name)

            extracted_unit, extracted_amount = extract_unit_info(name)

            products.append(
                {
                    "product_id": f"a101_{category_slug}_{len(products)}",
                    "product_name": name,
                    "sku": f"a101_{category_slug}_{len(products)}",
                    "shown_price_tl": price,
                    "regular_price_tl": price,
                    "discount_rate": None,
                    "product_url": f"https://www.a101.com.tr/kapida/{category_slug}/",
                    "brand_name": None,
                    "category_name": normalize_category_name(category_slug),
                    "unit": extracted_unit,
                    "unit_amount": extracted_amount,
                }
            )

            i += 2
            continue

        i += 1

    return products


def get_a101_products(category_slug: str):
    api_json = fetch_a101_category_api(category_slug)
    products = parse_a101_api_products(api_json)

    if products:
        return products

    url = f"https://www.a101.com.tr/kapida/{category_slug}/"
    products = []

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )

        context = browser.new_context(
            viewport={"width": 1440, "height": 2200},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            locale="tr-TR",
        )

        page = context.new_page()

        page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)

        print(f"DEBUG - CATEGORY URL: {url}")

        page.goto(url, timeout=60000, wait_until="domcontentloaded")
        page.wait_for_timeout(5000)

        for text in [
            "Bu defalık izin ver",
            "Siteyi ziyaret ederken izin ver",
            "Hiçbir zaman izin verme",
        ]:
            try:
                page.get_by_text(text, exact=True).click(timeout=3000)
                print(f"DEBUG - clicked location popup button: {text}")
                break
            except Exception:
                pass

        for text in ["KABUL ET", "Kabul Et", "Tümünü Kabul Et"]:
            try:
                page.get_by_text(text, exact=True).click(timeout=3000)
                print(f"DEBUG - clicked cookie popup button: {text}")
                break
            except Exception:
                pass

        page.wait_for_timeout(4000)

        for i in range(10):
            page.mouse.wheel(0, 3000)
            page.wait_for_timeout(1200)
            print(f"DEBUG - scroll step {i + 1}")

        body_text = page.locator("body").inner_text()
        print(f"DEBUG - BODY TEXT SAMPLE: {body_text[:2000]}")

        products = parse_products_from_body_text(body_text, category_slug)

        print(f"DEBUG - TOTAL PRODUCTS RETURNED: {len(products)}")
        if products:
            print(f"DEBUG - FIRST 10 PRODUCTS: {[p['product_name'] for p in products[:10]]}")

        context.close()
        browser.close()

    return products
