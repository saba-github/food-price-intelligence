## UcuzSepet – Food Price Intelligence

A system for comparing grocery prices across retailers in Turkey.

Goal:  
Answer a simple question: “En ucuz nerede?”

---

## What it does

- Collects prices from multiple retailers (Migros, A101)
- Normalizes product names and units (kg, liter, roll)
- Matches equivalent products safely
- Calculates comparable unit prices (e.g., TL/kg, TL/roll)
- Returns the cheapest option with a confidence level

---

## Key Features

- Safe matching  
  Avoids incorrect comparisons using brand, size and product-line awareness

- Smart search  
  Handles real queries like:  
  tuvalet kağıdı, kagit havlu, 8'li kağıt havlu

- Unit-aware pricing  
  Ensures fair comparison across different package sizes

- Confidence system  
  Results are labeled as comparable or review_required

---

## Stack

- Python (data pipeline)
- PostgreSQL (Neon)
- Streamlit (UI)
- Playwright (scraping)
- GitHub Actions (CI/CD)

---

## Example

Query: tuvalet kağıdı  
Result: A101 is cheaper than Migros based on TL/roll comparison

---

## Roadmap

- Cleaning products (detergents)
- Basket optimization
- Price history tracking

---

## Author

Sabahat Sengezer
