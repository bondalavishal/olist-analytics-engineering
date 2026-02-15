# Olist Analytics Engineering Pipeline

End-to-end analytics engineering project simulating a production-grade
data warehouse using:

- Python (incremental ingestion engine)
- PostgreSQL (warehouse)
- dbt (staging + star schema modeling)
- Apache Airflow (orchestration)

---

## Architecture

CSV → Python Incremental Loader → Postgres (raw)
→ dbt (staging → mart)
→ Data Quality Tests
→ Airflow Orchestration

---

## Dataset

Source:
Olist Brazilian E-Commerce Public Dataset (Kaggle)

https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

Download and place CSV files inside:

ingestion/raw_files/

Raw CSV files are not included in this repository.

---

## 1️⃣ Prerequisites

- Python 3.12+
- PostgreSQL 14+
- dbt-core + dbt-postgres
- Apache Airflow 3.x

---

## 2️⃣ PostgreSQL Setup

Create database:

CREATE DATABASE olist_dw;

Create schemas:

CREATE SCHEMA raw;
CREATE SCHEMA analytics_staging;
CREATE SCHEMA analytics_mart;

---

## 3️⃣ Python Ingestion Setup

cd ingestion
python -m venv venv
source venv/bin/activate
pip install psycopg2-binary pandas

Run incremental loader:

python incremental_loader.py

The loader:
- Processes one business date at a time
- Inserts new records into raw layer
- Advances watermark in pipeline_progress table

---

## 4️⃣ dbt Setup

cd dbt/olist_project
source ~/.venv/bin/activate  (your dbt venv)
dbt run
dbt test
dbt docs generate
dbt docs serve

Models include:
- Staging layer
- Fact tables
- Dimension tables
- Incremental fact logic

---

## 5️⃣ Airflow Setup

cd ~/Applications/Softwares/airflow
source airflow_venv/bin/activate
airflow standalone

DAG:
olist_analytics_pipeline

Pipeline flow:
1. Incremental ingestion
2. dbt run
3. dbt test

---

## Design Decisions

- Fact grain: 1 row per order_item
- Deterministic surrogate keys using MD5
- Incremental build strategy in fact table
- Watermark-driven ingestion control
- Referential integrity enforced via dbt tests

---

## Author

Vishal Bondala