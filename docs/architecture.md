# Architecture

## Purpose

The system collects supermarket food prices and stores them as historical price observations.

## Design Principles

- Raw data is always retained
- Data transformations are layered
- Pipeline runs are traceable
- Observations are append-only
- Analytical datasets are reproducible

## Data Layers

1. Raw Price Events  
Raw records captured from supermarket websites.

2. Staging Observations  
Parsed and standardized product price records.

3. Canonical Products  
Normalized product identities across sources.

4. Fact Price Observations  
Analytical dataset used for analysis and forecasting.

## Data Flow

Scraper → Raw Events → Staging → Product Mapping → Fact Tables → Analytics
