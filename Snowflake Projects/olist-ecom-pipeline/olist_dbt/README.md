# Olist E-Commerce Analytics Pipeline

A production-style data pipeline built on Snowflake, transforming raw
Brazilian e-commerce data into analytics-ready fact and dimension tables.

## Architecture

Raw CSV (Bronze) → dbt Staging (Silver) → dbt Marts (Gold)

## Dataset

Olist Brazilian E-Commerce · 9 tables · ~100k orders · 2016–2018

## Key Technical Decisions

- All VARCHAR in raw layer to preserve source fidelity
- TRY*TO*\* casts in staging to handle dirty data gracefully
- Star schema in marts for BI-tool compatibility
- dbt tests at every layer to catch data quality regressions

## Stack

Snowflake (Enterprise trial) · dbt Core 1.x · VS Code · Git

## Key Metrics Produced

- Monthly revenue trend with running totals
- Customer cohort retention analysis
- Top 20 categories by revenue and satisfaction

## How to Run

1. Configure ~/.dbt/profiles.yml with your Snowflake credentials
2. dbt build
3. Query ecommerce_db.marts.\*
