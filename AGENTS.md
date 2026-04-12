# 🚨 UrbanPulse VN - AI Agent Master Guideline

**Welcome to UrbanPulse VN (v8.1) - A Hybrid-Cloud Data Platform handling Vietnam's environmental & urban data.**

## 1. Project Context
This project utilizes a **Medallion Open Lakehouse** architecture:
- **Infra/Cloud**: Kubernetes (Kind), AWS S3 (Free Tier), GCP BigQuery, Terraform
- **Ingestion**: Scrapy, Python httpx, Kafka + Debezium CDC
- **Data Lake**: MinIO + Apache Iceberg (via Nessie Catalog)
- **Transformation**: Spark (Bronze->Silver), dbt (Silver->Gold)
- **Orchestration & Quality**: Airflow, Great Expectations
- **Dashboard & ML**: Streamlit, JupyterLab, MLflow
- **Constraints**: **16GB RAM MAX**, **Zero-Cost ($0) Strategy**.

## 2. Agentic Workflow Instructions
We have migrated to an **Agentic Workflow** ruleset to optimize context window and prevent hallucinations.
- **DO NOT** read all rules at once.
- Global rules are found in `.cursor/rules/00-core-constraints.mdc`.
- Context-specific rules (Airflow, Spark, dbt, Python Crawlers) will be automatically loaded based on the `globs` configuration when you are working on specific directories.
- To execute common tasks, check `.agents/workflows/` for available scripts (e.g., verifying a Sprint).

Before making modifications or suggesting architecture changes, ALWAYS review the `implementation_plan.md` and `warnings.md` documents.
