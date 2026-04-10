import re
from playwright.sync_api import sync_playwright


def extract_unit_info(product_name: str):
    if not product_name:
        return None, None

    patterns = [
        (r"(\d+(?:[.,]\d+)?)\s*(kg|KG|Kg)", "KG"),
        (r"(\d+(?:[.,]\d+)?)\s*(g|G|gr|GR)", "GRAM"),
        (r"(\d+(?:[.,]\d+)?)\s*(l|L|lt|LT)", "LITER"),
        (r"(\d+(?:[.,]\d+)?)\s*(ml|ML)", "ML"),
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
    ]

    for fragment in invalid_fragments:
        if fragment.lower() in name.lower():
            return False

    return True


def parse_price_from_lines(lines):
    for line in lines:
        if "₺" in line:
            raw_price = (
                line.replace("₺", "")
                .replace(".", "")
                .replace(",", ".")
                .strip()
            )
            try:
                return float(raw_price)
            except ValueError:
                continue
    return None


def get_a101_products(category_slug: str):
    url = f"https://www.a101.com.tr/kapida/{category_slug}/"
    products = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto(url, timeout=60000)

        # Konum popup
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

        # Cookie popup
        for text in ["KABUL ET", "Kabul Et", "Tümünü Kabul Et"]:
            try:
                page.get_by_text(text, exact=True).click(timeout=3000)
                break
            except Exception:
                pass

        page.wait_for_timeout(4000)

        # Lazy loading için scroll
        for _ in range(8):
            page.mouse.wheel(0, 2500)
            page.wait_for_timeout(1200)

        cards = page.locator("div[class*='product']").all()

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

                name = None
                for line in lines:
                    if "₺" not in line and len(line) > 2:
                        if is_valid_product_name(line):
                            name = line
                            break

                if not name:
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

            except Exception:
                continue

        browser.close()

    return products