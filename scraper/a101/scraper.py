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

        cards = page.locator("div[data-testid='product-card']").all()

        print("DEBUG - USING SELECTOR: div[data-testid='product-card']")
        print(f"DEBUG - TOTAL CARDS FOUND: {len(cards)}")

        seen_names = set()

        for i, card in enumerate(cards):
            try:
                name = card.locator("h3").inner_text().strip()

                # Meyve-sebze dışı ürünleri ele
                if any(
                    x in name.lower()
                    for x in [
                        "süt",
                        "peynir",
                        "çikolata",
                        "deterjan",
                        "pirinç",
                        "yumurta",
                    ]
                ):
                    continue

                price = None
                candidate_texts = []

                try:
                    spans = card.locator("span").all_inner_texts()
                    candidate_texts.extend(spans)
                except Exception:
                    pass

                try:
                    card_text = card.inner_text()
                    candidate_texts.append(card_text)
                except Exception:
                    pass

                for text in candidate_texts:
                    matches = re.findall(r"₺\s*([\d\.]+(?:,\d{1,2})?)", text)
                    if matches:
                        raw_price = matches[0].replace(".", "").replace(",", ".").strip()
                        try:
                            price = float(raw_price)
                            break
                        except ValueError:
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
