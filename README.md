# 🌏 UrbanPulse VN

**Vietnam Environmental & Urban Analytics Platform**

[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/)
[![dbt](https://img.shields.io/badge/dbt-1.11-orange.svg)](https://www.getdbt.com/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.x-E25A1C.svg)](https://spark.apache.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-017CEE.svg)](https://airflow.apache.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg)](https://www.docker.com/)

> A **production-grade data platform** analyzing Vietnam's environment & urban data from **7 sources** (5 REST APIs + 2 custom web crawlers). Built with a **Medallion Open Lakehouse** architecture. Fully containerized with Docker Compose — zero cloud cost.

---

## 🏗️ Architecture

```
DATA SOURCES (7)     INGESTION          BRONZE (Raw)         SILVER (Clean)       GOLD (Business)      SERVING
─────────────────    ─────────────────  ──────────────────   ──────────────────   ──────────────────   ──────────
• OpenAQ API    ──►  Python pipeline    PostgreSQL           Apache Iceberg       PostgreSQL           Streamlit
• Open-Meteo   ──►  (7 extractors)  ──► (bronze schema)  ──► (MinIO + Nessie) ──► (gold schema)  ──►  Grafana
• NASA FIRMS   ──►  Airflow DAG         MinIO (raw/)         PySpark cleaning     dbt marts            JupyterLab
• OSM Overpass ──►  (daily 06:00 ICT)                        (ACID guaranteed)    (19 models)          Trino SQL
• Open-Meteo   ──►
• CEM.gov.vn   ──►  (crawler)
• NCHMF.gov.vn ──►  (crawler)
```

**Orchestration:** Apache Airflow schedules ingestion daily and triggers dbt + Spark transformations.  
**Query Engine:** Trino provides unified SQL across Iceberg (MinIO) and PostgreSQL.

---

## 🛠️ Tech Stack

| Layer | Technology | Status |
|-------|-----------|--------|
| **Ingestion** | Python (httpx), Scrapy | ✅ Done |
| **Data Lake** | MinIO + Apache Iceberg (Nessie Catalog) | ✅ Done |
| **Query Engine** | Trino | ✅ Done |
| **Warehouse** | PostgreSQL | ✅ Done |
| **Transform (Heavy)** | PySpark — Bronze→Silver cleaning & partitioning | ✅ Done |
| **Transform (SQL)** | dbt — Silver→Gold (19 models, 60 tests) | ✅ Done |
| **Orchestration** | Apache Airflow (2 DAGs) | ✅ Done |
| **Cache** | Redis | ✅ Done |
| **Data Quality** | Great Expectations | 📋 Sprint 6 |
| **ML Tracking** | MLflow | 📋 Sprint 4 |
| **Dashboard** | Streamlit, Grafana | 📋 Sprint 4 |
| **Sandbox** | JupyterLab | 📋 Sprint 4 |
| **Streaming** | Kafka + Debezium CDC | 📋 Sprint 5 |
| **Infrastructure** | Docker Compose, Kubernetes (Kind), Terraform | 📋 Sprint 7 |
| **Cloud** | AWS S3 + GCP BigQuery | 📋 Sprint 7 |

---

## 🚀 Quick Start

### Prerequisites

- **Docker Desktop** (with Docker Compose v2 + WSL2 backend)
- **Python 3.12+**
- 16GB RAM recommended

### 1. Clone & Setup

```bash
git clone https://github.com/HoangLongIT1/UrbanPulseVN.git
cd UrbanPulseVN
cp .env.example .env
python -m venv .venv
.venv\Scripts\activate        # Windows
pip install -r requirements.txt
```

### 2. Start Core Infrastructure

```bash
# Start: PostgreSQL, MinIO, Trino, Redis, Nessie (~2.1GB RAM)
docker compose -f docker-compose.yaml up -d

# Wait ~30s for services to be healthy, then verify
docker compose ps
```

### 3. Run Ingestion Pipeline

```bash
# Seed historical data (first time)
python -m ingestion.pipeline --mode seed

# Or daily incremental run
python -m ingestion.pipeline --mode daily
```

### 4. Run dbt Transformation

```bash
cd dbt_transform
dbt seed        # Load static tables (cities, rivers, pollutant standards)
dbt run         # Build 19 models: Bronze → Silver → Gold
dbt test        # Run 60 data quality tests
cd ..
```

### 5. Run Spark Processing (optional — for large datasets)

```bash
# Bronze → Silver cleaning (with ACID via Iceberg)
spark-submit spark_jobs/bronze_to_silver_cleaning.py

# Silver → Gold aggregation (daily AQI, monthly risk index)
spark-submit spark_jobs/silver_to_gold_partitioning.py
```

> ⚠️ **RAM Warning:** Never run all services simultaneously on a 16GB machine.  
> See [warnings.md](warnings.md) for safe service grouping per sprint.

---

## 📁 Project Structure

```
UrbanPulseVN/
├── airflow/
│   └── dags/
│       ├── ingestion_dag.py          # Daily batch ingestion (06:00 ICT)
│       └── dbt_transformation_dag.py # dbt seed → run → test pipeline
├── dbt_transform/
│   └── models/
│       ├── staging/     # 7 sources → cleaned views (Silver)
│       ├── intermediate/ # Joins & business logic
│       └── marts/       # Final Gold tables (facts & dimensions)
├── ingestion/
│   ├── extractors/      # 5 API extractors (OpenAQ, Weather, Flood, Fire, Geo)
│   ├── crawlers/        # 2 web crawlers (CEM, NCHMF)
│   └── pipeline.py      # Main orchestrator
├── spark_jobs/
│   ├── bronze_to_silver_cleaning.py  # PySpark cleaning → Iceberg Silver
│   └── silver_to_gold_partitioning.py # PySpark aggregation → Iceberg Gold
├── docs/
│   ├── PRD.md           # Product Requirements Document
│   ├── ADR.md           # Architecture Decision Records (7 decisions)
│   ├── architecture.md  # Detailed architecture overview
│   └── sprint_*_report.md
├── docker-compose.yaml          # Core: Postgres, MinIO, Trino, Redis, Nessie
├── docker-compose.kafka.yaml    # Streaming: Kafka, Debezium (Sprint 5)
├── docker-compose.monitoring.yaml # Prometheus, Grafana (Sprint 4)
├── implementation_plan.md       # Full sprint roadmap
└── warnings.md                  # ⚠️ Must-read before developing
```

---

## 📊 Sprint Progress

| Sprint | Scope | Status |
|--------|-------|--------|
| Sprint 0 | Infrastructure (Docker, Postgres, MinIO, Nessie, Trino) | ✅ Done |
| Sprint 1 | Batch Ingestion (7 sources → Bronze layer) | ✅ Done |
| Sprint 2 | dbt Transformation (19 models, 60 tests) | ✅ Done |
| Sprint 3 | Airflow Orchestration + PySpark Processing | ✅ Done |
| Sprint 4 | Dashboard (Streamlit) + MLflow + JupyterLab | 📋 Planned |
| Sprint 5 | Kafka Streaming + CDC (Debezium) | 📋 Planned |
| Sprint 6 | Great Expectations Data Quality | 📋 Planned |
| Sprint 7 | Kubernetes + Cloud Migration (AWS S3 / GCP BigQuery) | 📋 Planned |

---

## 📖 Documentation

- [Product Requirements (PRD)](docs/PRD.md)
- [Architecture Decisions (ADR)](docs/ADR.md)
- [Architecture Overview](docs/architecture.md)
- [Implementation Plan](implementation_plan.md)
- [Warnings & Guidelines](warnings.md)

---

## 📄 License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

---

## 🏗️ Architecture

```
DATA SOURCES (7)  →  INGESTION  →  BRONZE (Raw)  →  SILVER (Clean)  →  GOLD (Business)  →  DASHBOARD
  • OpenAQ API        Batch &       MinIO +          PostgreSQL /       PostgreSQL /       Streamlit
  • Open-Meteo        Streaming     Apache Iceberg   BigQuery           BigQuery           Grafana
  • NASA FIRMS        (Airflow +    (Nessie Catalog) (dbt staging)      (dbt marts)        Looker Studio
  • 🕷️ CEM.gov.vn     Kafka)                                                              JupyterLab
  • 🕷️ NCHMF.gov.vn
  • OSM Overpass
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| **Ingestion** | Python (httpx), Scrapy, Kafka + Debezium CDC |
| **Data Lake** | MinIO + Apache Iceberg (Nessie Catalog) |
| **Query Engine** | Trino |
| **Warehouse** | PostgreSQL / GCP BigQuery |
| **Transform** | Spark (Bronze→Silver), dbt (Silver→Gold) |
| **Orchestration** | Apache Airflow |
| **Data Quality** | Great Expectations |
| **Cache** | Redis |
| **Secrets** | HashiCorp Vault |
| **ML Tracking** | MLflow |
| **Data Lineage** | Marquez (OpenLineage) |
| **Dashboard** | Streamlit, Grafana, Looker Studio |
| **Sandbox** | JupyterLab |
| **Infrastructure** | Docker Compose, Kubernetes (Kind + Helm), Terraform |
| **Cloud** | AWS (S3, Lambda) + GCP (BigQuery, Pub/Sub) |
| **CI/CD** | GitHub Actions |

---

## 🚀 Quick Start

### Prerequisites

- Docker Desktop (with Docker Compose v2)
- Python 3.12+
- Make (optional, for convenience)

### 1. Clone & Setup

```bash
git clone https://github.com/HoangLongIT1/UrbanPulseVN.git
cd UrbanPulseVN
cp .env.example .env
```

### 2. Start Core Services

```bash
make up-core
# Starts: PostgreSQL, MinIO, Trino, Redis, Nessie
```

### 3. Verify

```bash
make health
```

> ⚠️ **RAM Warning**: This project is designed for a **16GB RAM** machine. Never run all services simultaneously. See [warnings.md](warnings.md) for safe service grouping.

---

## 📁 Project Structure

See [implementation_plan.md](implementation_plan.md) for the complete project structure and sprint planning.

---

## 📖 Documentation

- [Architecture](docs/architecture.md)
- [Implementation Plan](implementation_plan.md)
- [Warnings & Guidelines](warnings.md)

---

## 📄 License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.
