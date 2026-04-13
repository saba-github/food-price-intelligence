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
