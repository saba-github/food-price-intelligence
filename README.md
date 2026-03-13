# Food Price Intelligence System

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
Standardized Staging Observations
        │
        ▼
Canonical Product Mapping
        │
        ▼
Fact Price Observations
        │
        ▼
Serving Layer (Analysis / Dashboard / Forecasting)
```

---

# Design Principles

The system follows modern data engineering design patterns:

- Raw data is always retained
- Data transformations are layered
- Pipeline runs are traceable
- Observations are append-only
- Analytical datasets are reproducible
- Product identities are canonicalized across sources

---

# Data Flow

The data pipeline follows a layered structure:

1. Scrapers extract raw product data from supermarket websites.
2. Raw events are stored in the database without modification.
3. Parsing and normalization produce standardized observations.
4. Canonical product identities are mapped across sources.
5. Analytical fact tables are built for price analysis and forecasting.

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
│ ├── migros
│ │ ├── extract.py
│ │ ├── parse.py
│ │ └── selectors.py
│
├── pipeline
│ ├── run_migros_pipeline.py
│ ├── load_raw.py
│ ├── build_staging.py
│ ├── build_fact.py
│ └── quality_checks.py
│
├── database
│ ├── migrations
│ │ ├── 001_create_scrape_runs.sql
│ │ ├── 002_create_raw_price_events.sql
│ │ ├── 003_create_stg_price_observations.sql
│ │ └── 004_create_fact_price_observations.sql
│
├── analysis
│ └── price_analysis.py
│
├── dashboard
│ └── streamlit_app.py
│
├── docs
│ └── architecture.md
│
└── README.md

---

# Example Analysis

Planned analyses include:

- Market price comparison
- Cheapest market detection
- Product price trends
- Food price inflation index

Example question:

> Which supermarket consistently offers the cheapest rice prices?

The project will analyze historical price observations to answer these questions.

---

# Current Features

- Scrapes food prices from Migros
- Stores product and price observations in PostgreSQL
- Supports historical price tracking
- Forecast-ready database schema

---

# Planned Improvements

- Add Carrefour and A101 scraping
- Normalize product names and package sizes
- Build forecasting models
- Create a Streamlit dashboard
- Automate daily scraping with GitHub Actions

---

# Project Roadmap

## Phase 1 — Data Collection

Scrape food prices from major supermarkets.

Markets:
- Migros
- CarrefourSA
- A101

Tracked products:
- Rice
- Milk
- Eggs
- Bread
- Chicken

Goal: collect daily price observations for essential food products.

---

## Phase 2 — Data Storage

Store and organize price observations in a cloud PostgreSQL database.

Tasks:

- Store price observations
- Build historical price dataset
- Normalize product names and units
- Handle duplicate products across markets

---

## Phase 3 — Data Analysis

Analyze food price trends and market differences.

Planned analyses:

- Market price comparison
- Cheapest market detection
- Product price trends
- Food price inflation index

Example analysis questions:

- Which supermarket offers the cheapest prices?
- How do food prices change over time?
- Which products show the highest price volatility?

---

## Phase 4 — Forecasting

Build time series forecasting models.

Planned models:

- Prophet
- ARIMA (optional)

Goals:

- Predict food prices for the next 14 days
- Identify potential price spikes
- Detect price anomalies

---

## Phase 5 — Dashboard

Build an interactive dashboard using Streamlit.

Dashboard features:

- Price trend visualization
- Market comparison charts
- Forecast visualization
- Food inflation index tracking

---

# Why This Project Matters

This project demonstrates key data engineering and data science skills:

- Web scraping
- Data pipeline design
- Cloud database usage
- Time series analysis
- Forecasting
- Dashboard development

---

# Future Vision

The long-term goal is to evolve the system into a **Food Price Intelligence Platform** capable of:

- Automatically tracking supermarket prices
- Building a historical food price dataset
- Detecting abnormal price changes
- Estimating food inflation trends
- Supporting economic and consumer price analysis

---

# Development Status

Current progress:

- Migros scraping implemented
- PostgreSQL cloud database connected
- Historical price observation storage available
- Layered data pipeline architecture in progress
- Run-level pipeline tracking being added

Planned next steps:

- Implement run-level pipeline metadata
- Introduce raw price event capture
- Build staging layer for standardized observations
- Implement canonical product mapping
- Build analytical fact tables
