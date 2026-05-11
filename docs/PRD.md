# Product Requirements Document (PRD)
# UrbanPulse VN — Environmental & Urban Data Platform

**Version:** 1.0  
**Status:** In Development  
**Author:** Luong Hoang Long  
**Created:** January 2026  
**Last Updated:** May 2026

---

## 1. Problem Statement

Vietnam faces escalating urban environmental challenges — rising air pollution in major cities (AQI exceeding 150 on 30%+ of days in Hanoi and HCMC), seasonal flooding affecting millions of households in the Mekong Delta, and wildfires in the Central Highlands — yet no unified, publicly accessible data platform aggregates these signals in real-time.

Existing government portals (cem.gov.vn, nchmf.gov.vn) provide fragmented, non-machine-readable data. Researchers, journalists, and city planners lack the infrastructure to correlate multi-hazard events across time and geography.

---

## 2. Goals & Non-Goals

### Goals
- Build a **Medallion Data Lakehouse** (Bronze → Silver → Gold) that ingests 7 environmental data sources covering all 63 Vietnamese provinces.
- Provide **clean, queryable Gold-layer tables** via dbt models, consumable by dashboards and ML models.
- Automate daily ingestion and transformation using **Apache Airflow** with fault-tolerant retry logic.
- Implement **distributed processing** with PySpark to handle multi-year historical datasets.
- Enforce **ACID transactions** on the data lake using Apache Iceberg, enabling time-travel queries and schema evolution.
- Keep total infrastructure cost at **$0** (local Docker + free-tier cloud APIs).

### Non-Goals (v1.0)
- Real-time streaming (Kafka integration planned for v2.0).
- Public user-facing API (internal use and research only).
- Coverage of countries other than Vietnam.

---

## 3. Data Sources

| # | Source | Type | Data | Update Frequency |
|---|--------|------|------|-----------------|
| 1 | OpenAQ API | REST API | Air quality (PM2.5, PM10, NO2) | Daily |
| 2 | Open-Meteo Weather | REST API | Temperature, humidity, wind | Daily |
| 3 | Open-Meteo Flood | REST API | River discharge (12 monitoring points) | Daily |
| 4 | NASA FIRMS | REST API | Fire hotspots (VIIRS satellite) | Daily |
| 5 | OpenStreetMap (Overpass) | REST API | Geographic features (roads, water bodies) | Weekly |
| 6 | CEM (cem.gov.vn) | Web Crawler | Vietnam AQI station data | Daily |
| 7 | NCHMF (nchmf.gov.vn) | Web Crawler | Disaster warnings (floods, storms) | Daily |

---

## 4. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        BRONZE LAYER                                   │
│  PostgreSQL (bronze schema) + MinIO (raw/ bucket)                    │
│  Raw, unprocessed data — append-only, never modified                 │
└───────────────────────────┬─────────────────────────────────────────┘
                            │ PySpark (bronze_to_silver_cleaning.py)
┌───────────────────────────▼─────────────────────────────────────────┐
│                        SILVER LAYER                                   │
│  Apache Iceberg on MinIO (iceberg/ bucket) via Nessie Catalog        │
│  Cleaned, typed, deduplicated — ACID guaranteed                      │
└───────────────────────────┬─────────────────────────────────────────┘
                            │ dbt (staging → intermediate → marts)
                            │ PySpark (silver_to_gold_partitioning.py)
┌───────────────────────────▼─────────────────────────────────────────┐
│                         GOLD LAYER                                    │
│  PostgreSQL (gold schema) + Iceberg Gold tables                      │
│  Business-ready aggregations — daily AQI, monthly risk index         │
└─────────────────────────────────────────────────────────────────────┘
```

Orchestration: **Apache Airflow** schedules ingestion (daily) and triggers dbt + Spark transformations.  
Query Engine: **Trino** provides unified SQL access across Iceberg and PostgreSQL.

---

## 5. Key Metrics for Success

| Metric | Target |
|--------|--------|
| Source coverage | 63/63 Vietnamese provinces |
| Data freshness (air quality) | < 24 hours lag |
| dbt test pass rate | 100% (60 tests) |
| Pipeline uptime | > 95% (daily runs) |
| RAM usage | < 12GB (16GB machine limit) |
| Cloud cost | $0 (local-first, free APIs) |

---

## 6. Sprint Roadmap

| Sprint | Scope | Status |
|--------|-------|--------|
| Sprint 0 | Infrastructure (Docker, Postgres, MinIO, Nessie, Trino) | ✅ Done |
| Sprint 1 | Ingestion (7 extractors + 2 crawlers → Bronze) | ✅ Done |
| Sprint 2 | dbt Transformation (19 models, 60 tests) | ✅ Done |
| Sprint 3 | Airflow Orchestration + Spark Processing | 🔄 In Progress |
| Sprint 4 | Streamlit Dashboard + MLflow + JupyterLab | 📋 Planned |
| Sprint 5 | Kafka Streaming + CDC (Debezium) | 📋 Planned |
| Sprint 6 | Great Expectations Data Quality | 📋 Planned |
| Sprint 7 | Kubernetes + Cloud Migration (AWS S3 / GCP BigQuery) | 📋 Planned |

---

## 7. Constraints

- **RAM:** Maximum 16GB on local machine. Spark executors capped at 1GB each.
- **Cost:** Zero spend — all tools must have a free tier or be open-source.
- **Data privacy:** No PII collected. All data is publicly available environmental data.
- **Windows compatibility:** Docker Compose must run on Docker Desktop for Windows with WSL2 backend.
