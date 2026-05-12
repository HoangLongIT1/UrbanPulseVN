# Sprint 3 Report: Airflow Orchestration & PySpark Processing
# UrbanPulse VN — Environmental & Urban Data Platform

**Period:** April 17 – May 11, 2026  
**Status:** ✅ Completed  
**Owner:** Luong Hoang Long (Data Engineer)

---

## 1. Sprint Objectives
The primary goal of Sprint 3 was to transition from manual script execution to a fully automated, scalable, and fault-tolerant data pipeline. This involved:
- Implementing **Apache Airflow** for end-to-end orchestration.
- Developing **PySpark** jobs to handle heavy data cleaning and partitioning.
- Integrating **Apache Iceberg** for ACID transactions and time-travel capabilities in the Silver layer.

---

## 2. Key Deliverables

### 2.1 Orchestration (Airflow)
Two production-grade DAGs were implemented in `airflow/dags/`:
- **`urbanpulse_ingestion`**: Handles daily batch ingestion from 7 sources (APIs & Crawlers). Features: retry logic (3 attempts), failure notifications, and dynamic task generation.
- **`urbanpulse_dbt_transformation`**: A trigger-based DAG that executes the dbt suite (`seed` -> `run` -> `test`) ensuring data consistency after each ingestion cycle.

### 2.2 Distributed Processing (PySpark)
Two Spark jobs were developed in `spark_jobs/` to replace Pandas-based processing for scalability:
- **`bronze_to_silver_cleaning.py`**: Reads raw data from PostgreSQL Bronze, performs schema validation, type casting, UTC normalization, and writes to **Apache Iceberg** (Silver layer).
- **`silver_to_gold_partitioning.py`**: Aggregates environmental metrics (AQI, Risk Index) and writes partitioned Gold tables, optimized for Trino queries.

### 2.3 Data Lakehouse Integration
Successfully integrated **Project Nessie** as a catalog for Iceberg tables, allowing for:
- ACID transactions on MinIO (local S3).
- Schema evolution without breaking downstream dbt models.
- Future support for data branching and merging (Git-for-data).

---

## 3. Technical Challenges & Solutions

| Challenge | Solution |
|-----------|----------|
| **Resource Constraints (16GB RAM)** | Configured Spark executors to `1g` memory and limited Airflow worker concurrency to 2 to prevent OOM errors on the local machine. |
| **Spark-Iceberg Compatibility** | Resolved JAR dependency issues by including `iceberg-spark-runtime-3.x` and `nessie-spark-extensions` in the Spark submit configuration. |
| **Data Consistency** | Implemented `dbt test` as a blocking step in the Airflow DAG to prevent corrupted data from reaching the Gold layer. |

---

## 4. Verification & Metrics

- **Pipeline Uptime:** 100% during the final verification week.
- **Data Freshness:** < 15 minutes for the entire E2E run (from API to Gold tables).
- **Test Coverage:** 60 automated tests passing across 19 models.
- **Storage Efficiency:** Parquet compression reduced raw JSON data size by ~70% in the Silver layer.

---

## 5. Conclusion
Sprint 3 successfully professionalized the UrbanPulse platform. The architecture now meets industry standards for Data Engineering, specifically matching the requirements for high-availability and performant data lakehouse systems.

**Next Sprint:** Sprint 4 — Dashboard & Sandbox (Streamlit, MLflow, JupyterLab).
