import re
from playwright.sync_api import sync_playwright


def extract_unit_info(product_name: str):
    if not product_name:
        return None, None

    patterns = [
        (r"(\d+(?:[.,]\d+)?)\s*(kg|KG|Kg)\b", "KG"),
        (r"(\d+(?:[.,]\d+)?)\s*(g|G|gr|GR)\b", "GRAM"),
        (r"(\d+(?:[.,]\d+)?)\s*(l|L|lt|LT)\b", "LITER"),
        (r"(\d+(?:[.,]\d+)?)\s*(ml|ML)\b", "ML"),
        (r"(\d+(?:[.,]\d+)?)\s*(adet|Adet|ADET)\b", "PIECE"),
        (r"(\d+(?:[.,]\d+)?)\s*(li|LI)\b", "PIECE"),
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


def is_valid_product_name(name: str) -> bool:
    if not name:
        return False

    invalid_fragments = [
        "Popüler Ürünler",
        "Çerez Kullanımı",
        "Kampanyalar",
        "Giriş Yap",
        "Sepetim",
        "Aramak istediğin ürünü yaz",
        "Böyle bir sayfa bulamadık",
        "A101 Hep Ucuz",
        "Kapıda",
        "Teslimat",
        "Seçtiğin mağaza",
    ]

    lowered = name.lower().strip()

    if len(lowered) < 3:
        return False

    for fragment in invalid_fragments:
        if fragment.lower() in lowered:
            return False

    if "₺" in lowered:
        return False

    return True


def parse_price_from_lines(lines):
    price_pattern = re.compile(r"₺\s*([\d\.]+(?:,\d{1,2})?)")

    prices = []
    for line in lines:
        matches = price_pattern.findall(line)
        for match in matches:
            raw_price = match.replace(".", "").replace(",", ".").strip()
            try:
                prices.append(float(raw_price))
            except ValueError:
                continue

    if not prices:
        return None

    return min(prices)


def clean_product_name(lines):
    candidates = []

    for line in lines:
        line = line.strip()

        if not is_valid_product_name(line):
            continue

        if re.search(r"\d+\s*(?:kg|g|gr|l|lt|ml|adet|li)\b", line, flags=re.IGNORECASE):
            candidates.append(line)
            continue

        if not re.search(r"^\d+[.,]?\d*$", line) and "₺" not in line:
            candidates.append(line)

    if not candidates:
        return None

    return max(candidates, key=len).strip()


def get_a101_products(category_slug: str):
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

        selectors = [
            "div[class*='product']",
            "article",
            "a[href*='/kapida/']",
            "div[class*='grid'] > div",
        ]

        best_cards = []
        best_selector = None

        for selector in selectors:
            try:
                current_cards = page.locator(selector).all()
                print(f"DEBUG - SELECTOR {selector} -> {len(current_cards)} cards")

                if len(current_cards) > len(best_cards):
                    best_cards = current_cards
                    best_selector = selector
            except Exception as e:
                print(f"DEBUG - SELECTOR ERROR {selector}: {e}")

        print(f"DEBUG - BEST SELECTOR: {best_selector}")
        print(f"DEBUG - TOTAL CARDS FOUND: {len(best_cards)}")

        seen_names = set()

        for i, card in enumerate(best_cards):
            try:
                text_blob = card.inner_text().strip()

                if i < 20:
                    print(f"DEBUG - RAW CARD {i}: {text_blob[:400]}")

                if not text_blob:
                    continue

                lines = [x.strip() for x in text_blob.splitlines() if x.strip()]
                if not lines:
                    continue

                price = parse_price_from_lines(lines)
                name = clean_product_name(lines)

                if i < 20:
                    print(f"DEBUG - PARSED CARD {i}: name={name}, price={price}, lines={lines[:10]}")

                if price is None and name is None:
                    continue

                if not name:
                    continue

                if price is None:
                    continue

                normalized_name = name.lower().strip()
                if normalized_name in seen_names:
                    continue

                seen_names.add(normalized_name)

                extracted_unit, extracted_amount = extract_unit_info(name)

                products.append(
                    {
                        "product_id": f"a101_{category_slug}_{i}",
                        "product_name": name,
                        "sku": f"a101_{category_slug}_{i}",
                        "shown_price_tl": price,
                        "regular_price_tl": price,
                        "discount_rate": None,
                        "product_url": url,
                        "brand_name": None,
                        "category_name": category_slug.replace("-", " ").title(),
                        "unit": extracted_unit,
                        "unit_amount": extracted_amount,
                    }
                )

            except Exception as e:
                print(f"DEBUG - CARD ERROR {i}: {e}")
                continue

        print(f"DEBUG - TOTAL PRODUCTS RETURNED: {len(products)}")

        context.close()
        browser.close()

    return products
