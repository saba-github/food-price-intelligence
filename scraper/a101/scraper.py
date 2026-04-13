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
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, timeout=60000, wait_until="domcontentloaded")

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

        page.wait_for_timeout(3000)

        for _ in range(10):
            page.mouse.wheel(0, 3000)
            page.wait_for_timeout(1000)

        cards = page.locator("div[class*='product'], article, a[href*='/kapida/']").all()

        seen_names = set()

        for i, card in enumerate(cards):
            try:
                text_blob = card.inner_text().strip()
                if not text_blob:
                    continue

                lines = [x.strip() for x in text_blob.splitlines() if x.strip()]
                if not lines:
                    continue

                price = parse_price_from_lines(lines)
                if price is None:
                    continue

                name = clean_product_name(lines)
                if not name:
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

            except Exception:
                continue

        browser.close()

    return products
