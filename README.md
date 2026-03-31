#  Food Price Intelligence System

*A production-grade layered data pipeline for supermarket price intelligence.*

Food Price Intelligence is an end-to-end data engineering project that captures, processes, and analyzes supermarket price data with a structured, layered architecture.

The system is designed to produce **historical, analytics-ready datasets** while ensuring traceability, data quality, and reproducibility.

---

#  What This Project Does

* Scrapes real-time food prices from supermarkets (currently Migros)
* Stores raw data for full traceability
* Applies structured transformations in layered architecture
* Filters unreliable data using rule-based quality checks
* Produces a clean **fact table for analytics**
* Computes **unit-normalized prices (TRY/kg, TRY/piece)**
* Runs automatically via **GitHub Actions + Neon PostgreSQL**

---

#  Architecture

```
Source (Migros API)
        │
        ▼
Raw Layer (raw_price_events)
        │
        ▼
Staging Layer (stg_price_observations)
        │
        ▼
Fact Layer (fact_price_observations)
        │
        ▼
Serving Layer (Analysis / Dashboard / Forecasting)
```

---

#  Data Model

## Raw Layer — `raw_price_events`

* Stores raw scraped JSON payload
* No transformations applied
* Enables full reproducibility and debugging

## Staging Layer — `stg_price_observations`

* Normalizes:

  * product name
  * unit (kg / piece)
* Computes:

  * `normalized_quantity`
  * `price_per_unit`
  * `unit_price_label` (TRY/kg, TRY/piece)
* Applies data quality rules
* Flags suspicious records:

  * `is_suspicious`
  * `suspicious_reason`

## Fact Layer — `fact_price_observations`

* Stores only **clean, trusted data**
* Used for analytics and downstream systems
* Suspicious records are excluded

---

#  Data Quality System

The pipeline includes rule-based validation:

### Detects:

* Missing or null prices
* Invalid prices (≤ 0)
* Extreme outliers
* Inconsistent unit-price relationships

### Behavior:

* Suspicious records → kept in staging
* Clean records → promoted to fact layer

---

# ⚙️ Pipeline Execution

The pipeline is fully automated:

* Runs via **GitHub Actions**
* Uses **Neon PostgreSQL** as cloud database
* Each run is tracked in `scrape_runs`
* Data is stored as **append-only observations**

---

#  Example Output

Each observation includes:

* `product_name`
* `price`
* `normalized_unit`
* `price_per_unit`
* `unit_price_label` (e.g. TRY/kg)

This enables direct comparison across products and time.

---

#  Tech Stack

* Python
* PostgreSQL (Neon)
* Requests / API scraping
* Pandas
* GitHub Actions (CI/CD)

---

#  Repository Structure

```
food-price-intelligence
│
├── scraper/
│   └── migros/
│
├── pipeline/
│   └── run_migros_pipeline.py
│
├── database/
│   └── migrations/
│
├── docs/
│   └── architecture.md
│
├── .github/workflows/
│
└── README.md
```

---

#  Current Status

✔ Migros scraping implemented
✔ Layered pipeline (raw → staging → fact)
✔ Data quality validation
✔ Unit normalization (price per kg / piece)
✔ Automated pipeline via GitHub Actions
✔ Cloud storage with Neon PostgreSQL

---

#  Next Steps

* Add Carrefour & A101 data sources
* Build cross-market price comparison
* Improve anomaly detection (statistical)
* Develop analytical queries & dashboards
* Add forecasting models

---

#  Why This Project Matters

This project demonstrates:

* Real-world data pipeline architecture
* Data quality engineering
* Cloud-based data workflows
* Reproducible analytics datasets
* End-to-end data system design

---

#  Vision

To evolve into a **Food Price Intelligence Platform** that:

* Tracks food prices across markets
* Detects anomalies and trends
* Supports inflation analysis
* Enables data-driven insights for consumers and researchers
