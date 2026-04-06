#  Food Price Intelligence

> An end-to-end data platform for collecting, standardizing, and analyzing supermarket price data.

---

##  Overview

Food Price Intelligence is a production-style data pipeline that transforms messy retail price data into clean, analytics-ready datasets.

It goes beyond scraping — the system focuses on **data quality, normalization, and reliable analytics**, enabling price comparison, trend analysis, and anomaly detection.

---

##  What problem does it solve?

Retail price data is:

- inconsistent (units, naming, packaging)
- noisy (duplicates, missing fields, invalid values)
- hard to compare across products

This project solves that by:

- standardizing product names and units
- calculating comparable **price per unit**
- filtering invalid and suspicious data
- building trusted datasets for analytics

---

##  Architecture

Source (Migros API)
↓
RAW (raw_price_events)
↓
STAGING (stg_source_products, stg_price_observations)
↓
FACT (fact_price_observations)
↓
MART (analytics-ready tables)
↓
Streamlit Dashboard

---


---

## ⚙️ Key Features

###  End-to-End Data Pipeline
- Automated ingestion → transformation → loading
- Fully orchestrated with GitHub Actions

###  Product & Unit Standardization
- Normalizes units (e.g. gram → kg, piece handling)
- Cleans and standardizes product names
- Enables apples-to-apples price comparison

###  Price Intelligence Layer
- Computes **price_per_unit**
- Tracks **discounts and regular prices**
- Aggregates daily product-level insights

###  Data Quality Framework
Each pipeline run validates:

- `price_per_unit completeness`
- `category_name completeness`

Runs fail automatically if data is unreliable.

###  Suspicious Data Detection
Flags:
- invalid prices (≤ 0)
- extreme prices (> 500 TRY)
- small-package price anomalies

###  Observability & Run Tracking
Each run tracks:

- records scraped / inserted
- suspicious records
- failed inserts
- quality check results
- pipeline health status

###  Analytics Layer (MART)
Materialized views provide:

- daily average prices
- top movers (price changes)
- anomaly detection
- pipeline health monitoring

###  Interactive Dashboard (Streamlit)
- Trend analysis
- Cheapest / most expensive products
- Price change tracking
- Anomaly insights
- Pipeline monitoring

---

##  Data Model

### RAW
- `raw_price_events`
- Stores unprocessed API responses

### STAGING
- `stg_source_products`
- `stg_price_observations`
- `stg_normalized_observations`

Handles cleaning, normalization, and enrichment.

### FACT
- `fact_price_observations`
- Trusted, analytics-ready dataset

### MART
- `mart_daily_prices`
- `mart_top_movers`
- `mart_price_anomalies`
- `mart_pipeline_health`

---

##  Example Transformations

- `"Domates Kokteyl 500 g"` → `domates kokteyl`
- `500 gram` → `0.5 kg`
- `price_per_unit = price / normalized_quantity`
- `"TRY/kg"` labeling

---

##  Data Quality Logic

A record is inserted into FACT only if:

- not suspicious
- price is valid
- normalized unit & quantity exist
- standardized product name exists
- category is populated

Otherwise → excluded from analytics layer

---

##  Pipeline Flow

1. Start run (tracked in `scrape_runs`)
2. Scrape products from source
3. Insert RAW data
4. Transform (normalize + enrich)
5. Insert STAGING tables
6. Validate data quality
7. Insert FACT data
8. Refresh MART views
9. Mark run as success / failure

---

##  Tech Stack

- **Python**
- **PostgreSQL (Neon)**
- **Streamlit**
- **GitHub Actions (CI/CD)**
- **psycopg2**
- **dotenv**

---

##  How to Run

```bash
git clone https://github.com/your-username/food-price-intelligence.git
cd food-price-intelligence

pip install -r requirements.txt

# set env
export DATABASE_URL=your_neon_connection

# run pipeline
python pipeline/run_migros_pipeline.py
