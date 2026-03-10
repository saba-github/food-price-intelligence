from playwright.sync_api import sync_playwright


def extract_migros_products():
    products = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        page.goto("https://www.migros.com.tr/arama?q=pirin%C3%A7", timeout=60000)
        page.wait_for_timeout(5000)

        product_cards = page.locator("div[class*='mdc-card']").all()

        for card in product_cards[:10]:
            try:
                name = card.locator("h5, h6, p").first.text_content()
            except:
                name = None

            try:
                price_text = card.locator("span").nth(0).text_content()
            except:
                price_text = None

            try:
                url = card.locator("a").first.get_attribute("href")
                if url and not url.startswith("http"):
                    url = "https://www.migros.com.tr" + url
            except:
                url = None

            if name:
                products.append({
                    "product_name": name.strip() if name else None,
                    "price_text": price_text.strip() if price_text else None,
                    "product_url": url
                })

        browser.close()

    return products
