# 📊 Food Price Intelligence System

*A layered data engineering pipeline for supermarket food price intelligence.*

Food Price Intelligence is a layered data engineering project that captures, standardizes, models, and serves supermarket price observations for historical analysis and market intelligence.

The system is designed as a reproducible data pipeline where raw data is retained, transformations are layered, and analytical datasets are built on top of reliable historical observations.

---

# Architecture


```
Source Websites (Migros / Carrefour / A101)
        │
        ▼
Raw Price Event Capture
        │
        ▼
Staging Layer (Normalization + Data Quality Checks)
        │
        ▼
Canonical Product Mapping
        │
        ▼
Fact Layer (Trusted Observations)
        │
        ▼
Serving Layer (Analysis / Dashboard / Forecasting)
```

---

## Data Pipeline Layers

The pipeline is organized into three main layers:

### 1. Raw Layer — `raw_price_events`
- Stores raw scraped product data
- No transformation applied
- Used for debugging and traceability

### 2. Staging Layer — `stg_price_observations`
- Normalizes product structure
- Standardizes product names and units
- Applies data quality validation rules
- Adds quality flags:
  - `is_suspicious`
  - `suspicious_reason`

### 3. Fact Layer — `fact_price_observations`
- Stores trusted, analytics-ready data
- Only clean records are inserted
- Suspicious records are excluded

---

## Data Quality System

The pipeline includes a built-in data quality layer to ensure reliability.

### Rule-based validation

The system detects:

- Null or missing prices
- Invalid prices (≤ 0)
- Extremely high prices
- Inconsistent package-price relationships

### Suspicious record handling

Suspicious records:

- Are stored in staging layer
- Are marked with:
  - `is_suspicious = true`
  - `suspicious_reason`
- Are excluded from fact layer

This ensures that downstream analysis uses only reliable data.

---

## Design Principles

The system follows modern data engineering best practices:

- Raw data is always retained
- Data transformations are layered
- Pipeline runs are traceable
- Observations are append-only
- Analytical datasets are reproducible
- Data quality validation is enforced
- Clean and trusted data is separated from raw data

---

## Data Flow

1. Scrapers extract product data from supermarket websites
2. Raw events are stored without modification
3. Data is normalized in staging layer
4. Data quality checks are applied
5. Clean data is written to fact tables
6. Analytical and forecasting layers consume fact data

---


# Tech Stack

- Python
- Playwright
- PostgreSQL (Neon)
- Pandas
- Streamlit
- GitHub

---

# Repository Structure

food-price-intelligence
│
├── scraper
│ └── migros
│ ├── extract.py
│ ├── parse.py
│ └── selectors.py
│
├── pipeline
│ └── run_migros_pipeline.py
│
├── database
│ └── migrations
│ ├── 001_create_scrape_runs.sql
│ ├── 002_create_raw_price_events.sql
│ ├── 003_create_stg_price_observations.sql
│ └── 004_create_fact_price_observations.sql
│
├── analysis
│ └── price_analysis.py
│
├── dashboard
│ └── streamlit_app.py
│
└── README.md

---

## Current Features

- Scrapes food prices from Migros
- Stores data in PostgreSQL (Neon)
- Tracks historical price observations
- Implements layered data pipeline
- Includes data quality validation system
- Separates raw, staging, and fact layers
- Filters suspicious data before analysis

---

## Planned Improvements

- Add Carrefour and A101 scraping
- Implement statistical anomaly detection (p95-based)
- Normalize package sizes and compute price per unit
- Build forecasting models
- Create Streamlit dashboard
- Automate daily scraping via GitHub Actions

---

## Project Roadmap

### Phase 1 — Data Collection
Scrape food prices from major supermarkets.

### Phase 2 — Data Storage
Build structured and historical price datasets.

### Phase 3 — Data Quality
Introduce validation and anomaly detection.

### Phase 4 — Data Analysis
Analyze price trends and market differences.

### Phase 5 — Forecasting
Build time series prediction models.

### Phase 6 — Dashboard
Visualize insights via Streamlit.

---

## Why This Project Matters

This project demonstrates:

- End-to-end data pipeline design
- Data quality engineering
- Cloud database usage
- Time series readiness
- Analytical data modeling

---

## Future Vision

The long-term goal is to build a **Food Price Intelligence Platform** capable of:

- Tracking supermarket prices automatically
- Detecting abnormal price changes
- Estimating food inflation trends
- Supporting economic analysis

---

## Development Status

Current progress:

- Migros scraping implemented
- PostgreSQL cloud database connected
- Layered pipeline architecture established
- Data quality validation implemented
- Suspicious data filtering active

Next steps:

- Statistical anomaly detection (p95)
- Unit normalization (price per kg / liter)
- Dashboard development
