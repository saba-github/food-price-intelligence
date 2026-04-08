#  Food Price Intelligence

A production-style data system that collects, processes, validates, and analyzes food price data to generate reliable insights.

---

##  Dashboard

<!-- buraya kendi streamlit screenshot'ını koy -->
![Dashboard](./assets/dashboard.png)

---

##  What it does

- Scrapes real-time product prices (Migros)
- Transforms raw data into structured datasets
- Applies data quality checks & anomaly detection
- Stores data in a cloud PostgreSQL database
- Serves analytics via a Streamlit dashboard

---

##  Architecture

Source → Raw → Staging → Fact → Mart → Dashboard  

- **Raw**: unprocessed data  
- **Staging**: normalization + quality checks  
- **Fact**: trusted dataset  
- **Mart**: aggregated analytics  

---

##  Tech Stack

- Python (data pipeline)
- PostgreSQL (Neon)
- SQL (transformations)
- GitHub Actions (automation)
- Streamlit (dashboard)

---

##  Why this project

- Demonstrates end-to-end data engineering
- Focuses on data quality & reliability
- Built with a production mindset

---

##  Author

**Sabahat Sengezer**  
Data Analyst → Data Scientist  

---

## Note

This is not just a scraper — it's a **data product**.
