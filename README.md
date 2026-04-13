# Food Price Intelligence

A production-style data pipeline that collects, processes, and analyzes grocery price data.

## What it does

- Scrapes real product prices (Migros)
- Stores raw data in PostgreSQL (Neon)
- Transforms into structured datasets (raw → staging → fact)
- Applies data quality checks & anomaly detection
- Serves insights via a Streamlit dashboard

## Architecture

Source → Raw → Staging → Fact → Mart → Dashboard

## Tech Stack

- Python (ETL)
- PostgreSQL (Neon)
- GitHub Actions (CI/CD)
- Streamlit

## Key Features

- Idempotent data ingestion
- Data quality validation
- Automated pipelines
- Analytical marts
- Price trend tracking

## Why this project

Built to simulate real-world data engineering systems  
with a focus on reliability and data quality.
