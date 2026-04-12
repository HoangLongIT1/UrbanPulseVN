# 🌏 UrbanPulse VN

**Vietnam Environmental & Urban Analytics Platform**

[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg)](https://www.docker.com/)

> A **production-grade data platform** analyzing Vietnam's environment & urban data from **7 sources** (5 REST APIs + 2 custom web crawlers). Built with a **Medallion Open Lakehouse** architecture and **Hybrid-Cloud** deployment (AWS + GCP).

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
