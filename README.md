# 🎟️ End-to-End ELT Pipeline
![Python](https://img.shields.io/badge/Python-3.12-blue.svg)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-Orchestration-red.svg)
![RabbitMQ](https://img.shields.io/badge/RabbitMQ-Message%20Broker-FF6600.svg)
![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-blue.svg)
![dbt](https://img.shields.io/badge/dbt-Transformation-orange.svg)
![Docker](https://img.shields.io/badge/Docker-Containerization-2496ED.svg)
![Ticketmaster API](https://img.shields.io/badge/API-Ticketmaster-purple.svg)
![ELT Pipeline](https://img.shields.io/badge/Pipeline-ELT-green.svg)
![Architecture](https://img.shields.io/badge/Architecture-Star%20Schema-yellow.svg)
![Data Modeling](https://img.shields.io/badge/Modeling-Bronze%2FSilver%2FGold-lightgrey.svg)
---
## 📌 Project Overview

This project focuses on building a robust ELT (Extract, Load, Transform) data pipeline. The objective is to collect global event data via the Ticketmaster API for the years 2026 and 2027, enabling decision-making analysis and strategic planning.

The architecture relies on Airflow, which serves as the orchestrator of the pipelines, a message broker (RabbitMQ) to ensure reliable ingestion, a cloud data warehouse (Snowflake) for storage, and dbt (Data Build Tool) for transforming and modeling data into a star schema.
---

## 🏗️ Technical Architecture

![Architecture](img/architecture.png)

The data flow follows these steps:

- **Extraction (Python Producers):**  
  Retrieve JSON data from the Ticketmaster API with filtering for the years 2026–2027.

- **Transit (RabbitMQ):**  
  Data is sent to a queue (`events_queue`) to decouple extraction from ingestion.

- **Loading (Python Consumer):**  
  Reads messages from RabbitMQ and inserts raw data into the `RAW_DATA` table in Snowflake.

- **Transformation (dbt):**  
  Cleans, types, and models the data in Snowflake to create analytical tables (Dimensions and Facts).

---

## 🛠️ Technology Stack

- **Language:** Python 3.12  
- **Orchestrator:** Apache Airflow
- **Message Broker:** RabbitMQ  
- **Data Warehouse:** Snowflake  
- **Transformation:** dbt (Data Build Tool)  
- **Containerization:** Docker & Docker Compose  
- **API Source:** Ticketmaster Discovery API  

---

## 📊 Data Explanation

The pipeline collects and processes **event-related data** from the Ticketmaster API. In this project, we only collect the following fields:

- **Event ID and Name**  
- **City and Venue**  
- **Event Segment** (category/segment of the event)

A known issue with this data source is the presence of **duplicate rows** and **missing values** for the event segment field.

The raw JSON data is flattened into the `EVENTS_RAW` table and later cleaned and structured into dimension and fact tables for analytical purposes.
---

## 🚀 Installation and Usage

### 1. Prerequisites

- Docker and Docker Compose installed  
- An active Snowflake account  
- A Ticketmaster API key  

---

### 2. Configuration

Create a `.env` file at the root of the project with your credentials:

API_KEY=your_api_key
API_SECRET=your_api_secret
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASS=your_password
SNOWFLAKE_WAREHOUSE=your_datawarhouse
SNOWFLAKE_DATABASE=your_db
SNOWFLAKE_ROLE=your_role
SNOWFLAKE_SCHEMA=your_schema_initial

---

### 3. Start Services

Launch the full stack :

docker compose -f Docker/docker-compose.yml up --build  

---

### 4. Run the Pipeline (Manual Mode)

You can still execute extraction and ingestion manually:

- Start the Consumer:

python3 scripts/consumer.py  

- Start the Producers:

python3 scripts/producer2026.py  
python3 scripts/producer2027.py  

---

### 5. dbt Transformation

Once data is loaded into Snowflake, run the transformations:

docker compose exec dbt bash  
cd project_ticketmaster  
dbt run  

---

## 🌬️ Airflow Orchestration 

![alt text](img/dag.png)
The pipeline is now orchestrated with Airflow through the DAG `elt_dag` located in `dags/elt_dag.py`.

### DAG behavior

- **Schedule:** `@daily`
- **Catchup:** `False`
- **Task flow:**
  - `run_producer2026` and `run_producer2027` run in parallel
  - then `run_consumer`
  - then `run_dbt`

In short:

`[run_producer2026, run_producer2027] >> run_consumer >> run_dbt`

### Access Airflow UI

- URL: `http://localhost:8080`
- Username: `admin`
- Password: `admin`

### Trigger the DAG

1. Open Airflow UI.
2. Enable `elt_dag`.
3. Click **Trigger DAG** for an immediate run.

### Optional: trigger from CLI

docker compose -f Docker/docker-compose.yml exec airflow-webserver airflow dags trigger elt_dag

### Monitor execution

Use the **Graph** and **Grid** views in Airflow to track each task execution and quickly identify failures/retries.

---

## 📊 Data Modeling (Star Schema)

The project uses dbt to transform raw data into a BI-optimized schema:

### 🟤 Bronze (Source)
- `EVENTS_RAW` (Flattened JSON data)

### ⚪ Silver (Cleaning)
- Date formatting  
- Handling missing values  

### 🟡 Gold (Analytics)

- `DIM_LOCATION`: Geographic details  
- `DIM_DATE`: Calendar for 2026–2027  
- `DIM_CATEGORIES`: Event categories (e.g., sports, music)  
- `FACT_EVENTS`: Central fact table linking all dimensions  

---

## 🎯 Conclusion

This project demonstrates full mastery of the data lifecycle, from asynchronous ingestion to advanced cloud-based data modeling. Docker ensures portability, while dbt guarantees data quality, consistency, and transformation traceability.
