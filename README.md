# Food Price Intelligence System

An end-to-end data pipeline that collects market food prices from online supermarkets, stores them in a cloud PostgreSQL database, and prepares the data for price trend analysis and forecasting.

## Architecture

Web Scraping  
↓  
Cloud PostgreSQL  
↓  
Data Cleaning  
↓  
Time Series Forecasting  
↓  
Dashboard  

## Tech Stack

- Python
- Playwright
- PostgreSQL (Neon)
- Streamlit
- GitHub

## Current Features

- Scrapes food prices from Migros
- Stores product and price history in PostgreSQL
- Supports repeated price observations over time
- Forecast-ready database schema

## Planned Improvements

- Add Carrefour and A101 scraping
- Normalize product names and units
- Add baseline forecasting
- Build Streamlit dashboard
- Automate daily scraping with GitHub Actions

## Why This Project Matters

This project demonstrates:
- web scraping
- cloud database usage
- ETL thinking
- historical price tracking
- end-to-end data product design
