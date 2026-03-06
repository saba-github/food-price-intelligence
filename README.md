# Food Price Intelligence System

An end-to-end data pipeline that collects supermarket food prices, stores them in a cloud PostgreSQL database, and analyzes price trends to support forecasting and market comparison.

This project aims to monitor food prices across multiple supermarkets and build a dataset for economic and food price analysis.

---
        Web Scraping
    (Migros / Carrefour / A101)
                │
                ▼
         Data Cleaning
                │
                ▼
        Cloud PostgreSQL
                │
                ▼
         Data Analysis
                │
                ▼
       Time Series Forecast
                │
                ▼
        Streamlit Dashboard

---

# Tech Stack

Python  
Playwright  
PostgreSQL (Neon)  
Pandas  
Streamlit  
GitHub  

---

# Repository Structure
food-price-intelligence
│
├── scraper
│   ├── migros_scraper.py
│   ├── carrefour_scraper.py
│   └── a101_scraper.py
│
├── database
│   └── schema.sql
│
├── pipeline
│   └── run_pipeline.py
│
├── analysis
│   └── price_analysis.py
│
├── models
│   └── forecast_model.py
│
├── dashboard
│   └── streamlit_app.py
│
└── README.md

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

The goal is to build a real-world **data product that tracks and analyzes food price dynamics**.
