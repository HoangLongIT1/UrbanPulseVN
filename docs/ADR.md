# Architecture Decision Records (ADR)
# UrbanPulse VN — Environmental & Urban Data Platform

Records key architectural decisions made during the design and development of the platform.  
Each ADR captures the context, options considered, decision made, and consequences.

---

## ADR-001: Medallion Architecture (Bronze/Silver/Gold)

**Date:** January 2026  
**Status:** Accepted

### Context
We need a data organization pattern that supports both raw data preservation (for debugging and reprocessing) and clean, business-ready aggregations (for dashboards and ML).

### Decision
Adopt the **Medallion Architecture** with three distinct layers:
- **Bronze:** Raw, immutable data as received from sources. Append-only.
- **Silver:** Cleaned, typed, and deduplicated. ACID-guaranteed via Iceberg.
- **Gold:** Business aggregations (daily AQI summaries, risk indices).

### Consequences
- ✅ Raw data is always preserved — reprocessing is possible without re-ingesting.
- ✅ Clear separation of concerns between ingestion, transformation, and serving.
- ❌ Higher storage usage (data exists in multiple forms simultaneously).

---

## ADR-002: PostgreSQL as Bronze + Gold Warehouse

**Date:** January 2026  
**Status:** Accepted

### Context
We need a SQL-queryable store for both raw ingested data and final aggregated tables.

### Options Considered
1. **PostgreSQL only** — simple, well-supported, dbt-native.
2. **MinIO/Parquet only** — more scalable but no direct SQL.
3. **PostgreSQL + MinIO** — hybrid approach.

### Decision
Use **PostgreSQL** for both Bronze (raw tables) and Gold (dbt output tables), with **MinIO** as a parallel object store for Parquet/Iceberg format. This allows dbt to run against PostgreSQL while Spark reads/writes Iceberg on MinIO.

### Consequences
- ✅ dbt works natively with PostgreSQL.
- ✅ Trino can query both PostgreSQL (via JDBC connector) and Iceberg (via Nessie catalog).
- ❌ Data duplication between PostgreSQL and MinIO for some tables.

---

## ADR-003: Apache Iceberg for Silver Layer

**Date:** February 2026  
**Status:** Accepted

### Context
The Silver layer requires ACID transactions, schema evolution, and time-travel queries to support safe reprocessing of historical data.

### Options Considered
1. **Delta Lake** — good ACID support but tighter Spark coupling.
2. **Apache Hudi** — complex setup, less mature ecosystem.
3. **Apache Iceberg** — open standard, Trino-native, Nessie integration.

### Decision
Use **Apache Iceberg** with **Nessie** as the catalog (Git-for-data semantics). Storage backend is **MinIO** (S3-compatible).

### Consequences
- ✅ Time-travel: `SELECT * FROM table FOR TIMESTAMP AS OF '2026-01-01'`
- ✅ Schema evolution without breaking existing queries.
- ✅ Trino can query Iceberg tables directly via `nessie` catalog.
- ❌ Requires JARs for Iceberg + AWS S3A + Nessie in Spark classpath.

---

## ADR-004: dbt for Silver → Gold Transformation

**Date:** February 2026  
**Status:** Accepted

### Context
We need a transformation tool for the Silver → Gold step that supports modular SQL, testing, and documentation.

### Options Considered
1. **Raw SQL scripts** — simple but no testing, no lineage.
2. **dbt (data build tool)** — modular, testable, documents data lineage.
3. **PySpark only** — powerful but verbose for simple SQL aggregations.

### Decision
Use **dbt** for all Silver → Gold SQL transformations. Use **PySpark** only for heavy data processing (cleaning, partitioning) that benefits from distributed execution.

### Consequences
- ✅ 60 automated data quality tests catch schema regressions early.
- ✅ dbt generates a data lineage graph (visible in dbt docs).
- ✅ SQL-based models are readable by non-engineers.
- ❌ dbt requires a running PostgreSQL — cannot run without infrastructure.

---

## ADR-005: Apache Airflow for Orchestration

**Date:** March 2026  
**Status:** Accepted

### Context
We need to schedule and monitor the ingestion and transformation pipelines, with retry logic and dependency management.

### Options Considered
1. **Cron jobs** — simple but no visibility, no retry, no dependency tracking.
2. **Prefect** — modern UI but paid for production features.
3. **Apache Airflow** — industry standard, free, Kubernetes-native.

### Decision
Use **Apache Airflow** (LocalExecutor mode for local development) with two DAGs:
- `urbanpulse_ingestion` — daily at 23:00 UTC (06:00 ICT).
- `urbanpulse_dbt_transformation` — triggered after ingestion completes.

### Consequences
- ✅ DAG UI provides full visibility into pipeline runs, durations, and failures.
- ✅ Built-in retry logic (2 retries with 5-minute delay).
- ❌ Airflow adds ~1.5GB RAM overhead — must be managed carefully on 16GB machine.

---

## ADR-006: PySpark for Distributed Processing

**Date:** March 2026  
**Status:** Accepted

### Context
Historical data reprocessing (seed mode) involves multi-year datasets across 63 cities — too large for single-threaded Python.

### Options Considered
1. **Pandas** — simple but not scalable beyond memory limits.
2. **Polars** — fast but no Iceberg/Nessie integration.
3. **PySpark** — distributed, Iceberg-native, industry standard for big data.

### Decision
Use **PySpark** (Spark 3.x) for:
- `bronze_to_silver_cleaning.py` — cleaning and type standardization.
- `silver_to_gold_partitioning.py` — aggregation and partitioning.

Run in local mode (`local[*]`) for development, with Docker Compose for a minimal Spark master/worker setup.

### Consequences
- ✅ Scales to multi-year datasets without memory errors.
- ✅ Writes natively to Iceberg tables with ACID guarantees.
- ❌ Spark startup time (~30s) makes it unsuitable for small incremental runs — Pandas/dbt used there instead.

---

## ADR-007: Zero-Cost Constraint

**Date:** January 2026  
**Status:** Accepted

### Context
This is a personal portfolio project. All infrastructure costs must be $0.

### Decision
- All APIs used are **free tier** (OpenAQ, Open-Meteo, NASA FIRMS, OSM).
- All infrastructure runs **locally** via Docker Compose.
- No paid cloud services used in v1.0.
- MinIO serves as a **local S3 replacement** (S3-compatible API).

### Consequences
- ✅ Zero ongoing cost.
- ✅ Portable — anyone can clone and run locally.
- ❌ No horizontal scaling beyond a single machine.
- ❌ NASA FIRMS and OpenAQ have rate limits that slow down historical seeding.
