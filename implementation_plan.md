# 🌏 UrbanPulse VN — Vietnam Environmental & Urban Analytics Platform

> **Version**: 8.1 — **Hybrid-Cloud (AWS + GCP)** + K8s + Sandbox + **Iceberg lakehouse** (Trino + Nessie) + **Data Lineage (Marquez) + Secret Mgmt (Vault) + ML Tracking (MLflow) + Cache (Redis)**

## Mục tiêu

Xây dựng một **Data Platform cấp production** thu thập, xử lý và phân tích dữ liệu môi trường & đô thị Việt Nam từ **7 nguồn dữ liệu đa dạng** — thể hiện kỹ năng Data Engineering ở mức **Fresher+ đến Junior**.

---

## Nguồn dữ liệu — 7 Sources, 100% phạm vi Việt Nam

| # | Source | Loại | Dữ liệu | VN Filter | Tần suất |
|---|--------|------|---------|-----------|----------|
| 1 | **OpenAQ** | API (free) | Chất lượng không khí (PM2.5, PM10, O3, NO2, SO2, CO) — trạm đo VN | `country=VN` | Mỗi giờ |
| 2 | **Open-Meteo Weather** | API (free) | Thời tiết 63 tỉnh thành (nhiệt độ, độ ẩm, mưa, gió, UV) | lat/lon VN cities | Mỗi giờ |
| 3 | **Open-Meteo Flood** | API (free) | Lưu lượng sông, dự báo lũ (sông Hồng, Mekong, Đà, Mã) | lat/lon VN rivers | Hàng ngày |
| 4 | **🕷️ Crawler: cem.gov.vn** | Web Scraper | Chỉ số AQI chính thức của Bộ TN&MT, cảnh báo ô nhiễm theo thành phố | Chỉ có VN | Mỗi giờ |
| 5 | **🕷️ Crawler: nchmf.gov.vn** | Web Scraper | Cảnh báo bão, lũ, động đất, thời tiết nguy hiểm (TTKTTVQG) | Chỉ có VN | Real-time |
| 6 | **NASA FIRMS** | API (free EDL) | Điểm nóng cháy rừng vệ tinh MODIS/VIIRS, focus VN | `country=VNM` | Mỗi 3 giờ |
| 7 | **OpenStreetMap** | Overpass API | Hạ tầng đô thị VN (bệnh viện, trường, KCN, giao thông) | VN bounding box | Tĩnh (static) |

### Tại sao 7 sources?
- **100% Việt Nam**: mọi source đều lọc hoặc chỉ có dữ liệu VN
- **Đa dạng kỹ thuật**: API REST (1-3,6), Web Scraping/Crawling (4-5), Geospatial (7) — thể hiện nhiều phương pháp ingestion
- **Đa dạng tần suất**: real-time → hourly → daily → static
- **Phạm vi bao phủ**: không khí → thời tiết → lũ lụt → thiên tai → cháy rừng → hạ tầng
- **Cross-analysis**: thời tiết ↔ ô nhiễm ↔ thiên tai → insights giá trị cho chính sách môi trường VN

> [!TIP]
> 2 Crawlers (cem.gov.vn, nchmf.gov.vn) thể hiện kỹ năng **Web Scraping + Data Engineering** — skill rất được nhà tuyển dụng đánh giá cao vì thực tế nhiều nguồn data không có API sẵn.

---

## Bài học rút ra từ 3 Reference Projects

| Repo | Điểm hay được áp dụng |
|------|----------------------|
| **NYC_Taxi_Pipeline** | Apache Iceberg format, Nessie Catalog, Debezium CDC, Trino query engine, Makefile targets, Great Expectations validation → notebook, multi docker-compose files |
| **Customer-Purchase ML** | Schema validation layer (valid/invalid routing), bronze→silver→gold trong PostgreSQL, multi docker-compose split by concern, Kafka Control Center, NGINX proxy |
| **ChatOpsLLM** | Terraform IaC, Prometheus + Grafana monitoring stack, ELK logging, proper Helm-style deployment |

---

## Kiến trúc tổng quan — Hybrid-Cloud: AWS + GCP + K8s (v8.1)

```
                         ┌──────────────────────────────────────┐
                         │         ORCHESTRATION (Airflow)       │
                         └──────────┬───────────────┬───────────┘
                                    │               │
 ┌──────────────┐  ┌────────────────▼──┐  ┌────────▼────────┐  ┌──────────────────┐
 │ DATA SOURCES │  │     BRONZE        │  │     SILVER       │  │      GOLD        │
 │  (7 VN src)  │  │   (Raw / Landing) │  │   (Cleaned)      │  │   (Business)     │
 │• OpenAQ      │  │                   │  │                  │  │                  │
 │• Open-Meteo  │─▶│  AWS S3 / MinIO   │─▶│  PostgreSQL /    │─▶│  PostgreSQL /    │
 │• 🕷CEM.gov   │  │  Parquet / JSON   │  │  BigQuery        │  │  BigQuery        │
 │• 🕷NCHMF.gov │  │  Apache Iceberg  │  │  dbt staging     │  │  dbt marts       │
 │• NASA FIRMS  │  │   (via Nessie)    │  │                  │  │                  │
 │• OSM         │  │                   │  │                  │  │                  │
 └──────┬───────┘  └───────────────────┘  └──────────────────┘  └────────┬─────────┘
        │                                                                │
 ┌──────▼───────┐  ┌──────────────────┐  ┌──────────────┐  ┌────────────▼─────────┐
 │  STREAMING   │  │ SCHEMA VALIDATION│  │  DATA QUALITY│  │     SERVING LAYER    │
 │  Kafka /     │─▶│ Valid → process  │  │• Great Expect│  │• Streamlit Dashboard │
 │  GCP Pub/Sub │  │ Invalid → DLQ    │  │• dbt tests   │  │• Grafana Monitoring  │
 └──────────────┘  └──────────────────┘  └──────────────┘  │• Trino / BigQuery   │
                                                           │• Looker Studio      │
 ┌──────────────────────────────────────────────────────────│• 🧪 Sandbox (Jupyter)│
 │  SANDBOX LAYER                                          └──────────────────────┘
 │  JupyterLab → Read Gold (SELECT only) → Write sandbox schema (read-write)    │
 │  EDA / ML experiments / cross-source correlation analysis                      │
 └────────────────────────────────────────────────────────────────────────────────┘
 ┌────────────────────────────────────────────────────────────────────────────────┐
 │                           INFRASTRUCTURE                                      │
 │  DEV:   Docker Compose │ Makefile │ Bash Scripts │ GitHub Actions             │
 │  K8S:   Kind cluster   │ Helm charts │ kubectl                               │
 │  LINUX: Crontab │ Health checks │ Log management                              │
 │  CLOUD: Hybrid (AWS Lambda + S3) + (GCP BigQuery + Pub/Sub + Looker)          │
 └────────────────────────────────────────────────────────────────────────────────┘
```

### 🔀 Hybrid-Cloud Architecture — Tại sao?

| Cloud | Dịch vụ chọn | Lý do (Best of both worlds) |
|------|-------------|----------------------------|
| **AWS** | **S3 & Lambda** | AWS S3 là tiêu chuẩn ngành DE. Lambda (Serverless) cực kỳ phổ biến. |
| **GCP** | **BigQuery & Pub/Sub** | BigQuery có gói **Always Free 1TB/tháng** — an toàn tuyệt đối về chi phí. |
| **Both** | **Terraform** | Quản lý hạ tầng đa đám mây bằng code (IaC), thể hiện trình độ Senior. |

### 🔀 Tri-Mode Architecture — Tại sao?

| Mode | Khi nào dùng | Lợi ích |
|------|-------------|--------|
| **🖥️ Docker Compose** | Development, quick testing | Không cần internet, `make run-all`, toàn quyền kiểm soát |
| **☸️ Kubernetes (Kind)** | Production simulation, container orchestration | Thể hiện K8s skills, auto-scaling, self-healing |
| **☁️ GCP Cloud** | Cloud deployment, CV showcase | Free-tier, luôn chạy 24/7, cloud-native |

---

## Tech Stack — Hybrid Production-Grade (v8.1)

| Layer | Docker Compose | Kubernetes (Kind) | Hybrid Cloud (Free Tier) |
|-------|---------------|-------------------|--------------------------|
| **Ingestion (Batch)** | Python + httpx | K8s CronJob | **AWS Lambda** |
| **Ingestion (Crawl)** | Scrapy/BeautifulSoup | K8s Job | **AWS Lambda** |
| **Streaming** | Kafka + Debezium | Kafka on K8s | **GCP Pub/Sub** |
| **Data Lake** | MinIO + Apache Iceberg + Nessie | MinIO on K8s | **Amazon S3** |
| **Warehouse** | PostgreSQL | PostgreSQL on K8s | **GCP BigQuery** |
| **Transform** | dbt-postgres | dbt-postgres | **dbt-bigquery** |
| **Quality** | Great Expectations | ← same | ← same |
| **Orchestration** | Airflow | Airflow on K8s | **EventBridge** (AWS) |
| **Dashboard** | Streamlit | Streamlit on K8s | Looker Studio (GCP) |
| **Sandbox** | JupyterLab | JupyterLab on K8s | — |
| **Cache/NoSQL** | **Redis** | Redis on K8s | Redis (self-hosted) |
| **Secret Mgmt** | **HashiCorp Vault** (dev) | Vault on K8s | Docker Secrets / Vault |
| **Data Lineage** | **Marquez** (OpenLineage) | Marquez on K8s | — |
| **ML Tracking** | **MLflow** | MLflow on K8s | — |
| **Infra** | Docker Compose | Kind + Helm | **Terraform (AWS + GCP)** |
| **CI/CD** | GitHub Actions | ← same | ← same |

---

## Hybrid Cloud Free Tier — Zero Cost Strategy

| Cloud | Dịch vụ | Always-Free / 12mo | Dùng cho |
|-------|---------|-------------------|----------|
| **AWS** | **Amazon S3** | 5GB (12 months free) | Data Lake (Bronze) |
| **AWS** | **AWS Lambda** | 1M calls (Always Free) | Serverless Ingestion |
| **AWS** | **EventBridge** | Rất rẻ/Miễn phí | Scheduler trigger |
| **GCP** | **BigQuery** | 10GB + 1TB query (Always Free) | Data Warehouse |
| **GCP** | **Pub/Sub** | 10GB traffic (Always Free) | Streaming messaging |
| **GCP** | **Looker Studio**| 100% Free | BI Dashboard |
| **Both**| **Terraform** | Open-source | Infrastructure as Code |

> [!IMPORTANT]
> **Anti-bill strategy**: Set budget alert $0 → email notification ngay khi phát sinh phí. Project này sẽ KHÔNG vượt free tier nếu chạy đúng hướng dẫn.

---

## Project Structure (v6)

```
urbanpulse-vn/
├── docker-compose.yaml              # Core: Postgres, MinIO, Trino
├── docker-compose.kafka.yaml        # Kafka, Zookeeper, Schema Registry, Control Center
├── docker-compose.spark.yaml        # Spark master + workers
├── docker-compose.airflow.yaml      # Airflow webserver + scheduler + init
├── docker-compose.monitoring.yaml   # Prometheus, Grafana
├── docker-compose.app.yaml          # Streamlit dashboard + JupyterLab sandbox
├── .env.example
├── Makefile                          # make up-all, make up-kafka, make down, etc.
├── README.md
├── .github/workflows/
│   ├── ci.yml
│   └── dbt-test.yml
│
├── ingestion/                        # ── BATCH INGESTION (7 VN sources) ──
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── config.py
│   ├── extractors/                   # API-based extractors
│   │   ├── base.py                   # Abstract BaseExtractor
│   │   ├── air_quality.py            # OpenAQ (country=VN)
│   │   ├── weather.py                # Open-Meteo Weather (VN cities)
│   │   ├── flood.py                  # Open-Meteo Flood (VN rivers)
│   │   ├── fire_hotspot.py           # NASA FIRMS (country=VNM)
│   │   └── geo_features.py           # OSM Overpass (VN bbox)
│   ├── crawlers/                     # Web scraper for VN govt sites
│   │   ├── base_crawler.py           # Abstract BaseCrawler (retry, rate-limit)
│   │   ├── cem_aqi_crawler.py        # cem.gov.vn — AQI chính thức Bộ TN&MT
│   │   └── nchmf_disaster_crawler.py # nchmf.gov.vn — cảnh báo bão/lũ/động đất
│   ├── loaders/
│   │   ├── minio_loader.py           # → MinIO Bronze
│   │   └── postgres_loader.py        # → PostgreSQL
│   └── utils/
│       ├── retry.py
│       └── logging_config.py
│
├── streaming/                        # ── STREAMING + CDC ──
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── producer/
│   │   └── air_quality_producer.py
│   ├── consumer/
│   │   └── spark_streaming_consumer.py
│   ├── schema_validation/
│   │   └── validator.py              # Valid → process, Invalid → DLQ
│   ├── debezium/
│   │   ├── configs/
│   │   │   └── air-quality-cdc.json  # CDC connector config
│   │   └── run.sh                    # Register connector script
│   └── schemas/
│       └── air_quality_event.avro
│
├── spark_jobs/                       # ── SPARK PROCESSING ──
│   ├── Dockerfile
│   ├── jars/                         # JAR files (aws-sdk, hadoop-aws, iceberg, nessie)
│   ├── bronze_to_silver.py
│   ├── silver_to_gold.py
│   └── streaming_to_datalake.py      # Stream → MinIO raw bucket
│
├── dbt_transform/                    # ── dbt (Medallion) ──
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   ├── models/
│   │   ├── staging/
│   │   │   ├── _staging.yml
│   │   │   ├── stg_air_quality.sql
│   │   │   ├── stg_weather.sql
│   │   │   ├── stg_flood.sql
│   │   │   ├── stg_cem_aqi.sql              # CEM crawler data
│   │   │   ├── stg_nchmf_disaster.sql       # NCHMF crawler data
│   │   │   ├── stg_fire_hotspot.sql
│   │   │   └── stg_geo_features.sql
│   │   ├── intermediate/
│   │   │   ├── int_daily_aqi_city.sql
│   │   │   ├── int_weather_hourly.sql
│   │   │   ├── int_disaster_impact.sql
│   │   │   └── int_environmental_risk.sql  # cross-source risk scoring
│   │   └── marts/
│   │       ├── _marts.yml
│   │       ├── fact_air_measurements.sql
│   │       ├── fact_weather_observations.sql
│   │       ├── fact_natural_disasters.sql   # flood + fire + NCHMF warnings
│   │       ├── dim_stations.sql
│   │       ├── dim_cities.sql
│   │       ├── dim_date.sql
│   │       ├── dim_pollutants.sql
│   │       └── dim_disaster_types.sql
│   ├── tests/
│   ├── macros/
│   │   ├── calculate_aqi.sql
│   │   └── calculate_risk_score.sql
│   └── seeds/
│       ├── vietnam_cities.csv
│       ├── vietnam_rivers.csv
│       └── pollutant_standards.csv
│
├── data_quality/                     # ── GREAT EXPECTATIONS ──
│   ├── great_expectations.yml
│   ├── expectations/
│   ├── checkpoints/
│   └── notebooks/
│       └── full_flow.ipynb           # GE validation notebook
│
├── trino/                            # ── QUERY ENGINE ──
│   ├── catalog/
│   │   └── datalake.properties       # MinIO catalog
│   └── etc/
│       ├── config.properties
│       └── node.properties
│
├── airflow/
│   ├── Dockerfile
│   ├── dags/
│   │   ├── batch_ingestion_dag.py
│   │   ├── streaming_monitor_dag.py
│   │   ├── data_quality_dag.py
│   │   └── dbt_dag.py
│   └── plugins/custom_operators/
│
├── dashboard/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── app.py
│   └── components/
│       ├── aqi_heatmap.py
│       ├── trend_charts.py
│       ├── weather_correlation.py
│       └── alerts_panel.py
│
├── sandbox/                          # ── DATA SANDBOX (JupyterLab) ──
│   ├── Dockerfile                    # JupyterLab + pandas, scikit-learn, matplotlib
│   ├── requirements.txt
│   └── notebooks/
│       ├── 01_eda_air_quality.ipynb   # EDA: PM2.5 trends theo thành phố
│       ├── 02_weather_correlation.ipynb # Correlation: thời tiết ↔ ô nhiễm
│       └── 03_disaster_risk_analysis.ipynb # Cross-source: thiên tai + AQI
│
├── monitoring/
│   ├── grafana/
│   │   ├── provisioning/dashboards/
│   │   └── datasources.yml
│   └── prometheus/
│       └── prometheus.yml
│
├── tests/
│   ├── unit/
│   ├── integration/
│   └── conftest.py
│
├── scripts/                          # ── AUTOMATION ──
│   ├── seed_historical_data.py
│   ├── generate_fake_stream.py
│   ├── convert_to_iceberg.py         # Parquet → Apache Iceberg via Nessie
│   └── bash/                         # ── LINUX/UNIX SHELL SCRIPTS ──
│       ├── setup_env.sh              # Bootstrap: install deps, create dirs, set permissions
│       ├── health_check.sh           # Check all services: Docker, DB, MinIO, Kafka
│       ├── backup_data.sh            # Backup PostgreSQL + MinIO data (pg_dump + mc mirror)
│       ├── rotate_logs.sh            # Log rotation: compress old, delete >7 days
│       ├── monitor_pipeline.sh       # Pipeline watchdog: alert on failure via webhook
│       ├── crontab_setup.sh          # Register crontab jobs tự động
│       └── deploy.sh                 # One-click deployment script
│
├── utils/                            # Shared utilities
│   ├── create_schema.py
│   ├── create_table.py
│   ├── postgresql_client.py
│   ├── minio_utils.py
│   └── helper.py
│
├── configs/
│   ├── spark.yaml
│   ├── datalake.yaml
│   └── crontab.conf                  # Linux crontab schedule file
│
├── cloud/                            # ── HYBRID CLOUD DEPLOYMENT ──
│   ├── terraform/
│   │   ├── aws/                       # AWS Resources (S3, Lambda, IAM)
│   │   │   ├── s3.tf
│   │   │   ├── lambda.tf
│   │   │   └── variables.tf
│   │   ├── gcp/                       # GCP Resources (BigQuery, Pub/Sub)
│   │   │   ├── bigquery.tf
│   │   │   ├── pubsub.tf
│   │   │   └── variables.tf
│   │   ├── main.tf                    # Multi-provider entrypoint
│   │   └── provider.tf                # AWS & Google providers
│   ├── aws_lambda/
│   │   ├── ingest_to_s3/              # Lambda: API/Crawl → S3
│   │   └── s3_to_bigquery_trigger/    # Lambda: S3 upload → notify BigQuery
│   ├── dbt_bigquery/                  # dbt on GCP BigQuery
│   └── README.md                      # Hybrid deployment guide
│
├── k8s/                              # ── KUBERNETES DEPLOYMENT ──
│   ├── kind-config.yaml              # Kind cluster config (multi-node)
│   ├── namespaces.yaml               # urbanpulse-data, urbanpulse-app, monitoring
│   ├── helm/
│   │   ├── urbanpulse/                # Custom Helm chart
│   │   │   ├── Chart.yaml
│   │   │   ├── values.yaml
│   │   │   ├── values-local.yaml
│   │   │   └── templates/
│   │   │       ├── postgres.yaml
│   │   │       ├── minio.yaml
│   │   │       ├── kafka.yaml
│   │   │       ├── airflow.yaml
│   │   │       ├── spark.yaml
│   │   │       ├── streamlit.yaml
│   │   │       ├── jupyter.yaml
│   │   │       ├── ingress.yaml
│   │   │       └── configmap.yaml
│   │   └── monitoring/                # Helm chart for Prometheus + Grafana
│   │       ├── Chart.yaml
│   │       └── values.yaml
│   ├── manifests/                     # Raw K8s manifests (alternative to Helm)
│   │   ├── cronjob-ingestion.yaml     # K8s CronJob: run ingestion every 6h
│   │   └── job-crawler.yaml           # K8s Job: run crawlers on-demand
│   └── README.md                      # K8s deployment guide
│
└── docs/
    ├── architecture.md
    ├── data_dictionary.md
    ├── runbook.md
    ├── cloud_deployment_guide.md
    └── k8s_deployment_guide.md        # Kind + Helm deployment guide
```

---

## Thay đổi chính — Evolution qua các version

| Hạng mục | v1 | v2 | v3 | v4 | v5 | v6 |
|----------|----|----|----|----|----|----|
| **Data** | 3 (global) | = | + Cloud | 7 sources | 7 VN + crawlers | = |
| **Deploy** | Docker | Multi-compose | + GCP | = | + K8s (Kind+Helm) | = |
| **Lake** | MinIO | + Iceberg | + GCS | = | = | = |
| **DWH** | PostgreSQL | bronze→gold | + BigQuery | = | = | **+ Sandbox schema** |
| **Infra** | Makefile | = | + Terraform | + Bash | + Kind + Helm | = |
| **Extra** | — | CDC + Trino | Scheduler | Linux | K8s + Scraping | **🧪 JupyterLab** |

---

## Makefile — Quản lý project (học từ NYC_Taxi + Customer-Purchase)

```makefile
# ── NETWORK ──
up-network:
	docker network create urbanpulse-net || true

# ── SERVICES ──
up-core:
	docker compose -f docker-compose.yaml up -d

up-kafka:
	docker compose -f docker-compose.kafka.yaml up -d

up-spark:
	docker compose -f docker-compose.spark.yaml up -d

up-airflow:
	docker compose -f docker-compose.airflow.yaml up -d

up-monitoring:
	docker compose -f docker-compose.monitoring.yaml up -d

up-app:
	docker compose -f docker-compose.app.yaml up -d

# ── ALL ──
run-all: up-network up-core up-kafka up-spark up-airflow up-monitoring up-app
	@echo "✅ All services started!"

down:
	docker compose -f docker-compose.yaml \
	               -f docker-compose.kafka.yaml \
	               -f docker-compose.spark.yaml \
	               -f docker-compose.airflow.yaml \
	               -f docker-compose.monitoring.yaml \
	               -f docker-compose.app.yaml \
	               down

# ── DATA ──
seed:
	python scripts/seed_historical_data.py

convert-iceberg:
	python scripts/convert_to_iceberg.py

# ── TESTING ──
test-unit:
	pytest tests/unit/ -v

test-e2e:
	pytest tests/integration/ -v

dbt-test:
	cd dbt_transform && dbt test

quality-check:
	cd data_quality && great_expectations checkpoint run air_quality_suite

lint:
	flake8 . && black --check .

# ── KUBERNETES ──
k8s-create:
	kind create cluster --config k8s/kind-config.yaml --name urbanpulse

k8s-deploy:
	helm install urbanpulse k8s/helm/urbanpulse -f k8s/helm/urbanpulse/values-local.yaml

k8s-monitoring:
	helm install monitoring k8s/helm/monitoring

k8s-status:
	kubectl get pods -A | grep urbanpulse

k8s-delete:
	kind delete cluster --name urbanpulse

# ── CLOUD (GCP) ──
cloud-init:
	cd cloud/terraform && terraform init

cloud-plan:
	cd cloud/terraform && terraform plan

cloud-apply:
	cd cloud/terraform && terraform apply -auto-approve

cloud-destroy:
	cd cloud/terraform && terraform destroy -auto-approve

cloud-deploy-aws:
	# Lệnh deploy AWS Lambda code bằng CLI
	zip -j lambda.zip cloud/aws_lambda/ingest_to_s3/*
	aws lambda update-function-code --function-name urbanpulse-ingest --zip-file fileb://lambda.zip

cloud-deploy-gcp:
	# BigQuery schemas & PubSub setup via Terraform
	@echo "Deployed via Terraform apply"
```

---

## Docker Compose Services (15 services, 6 files)

| File | Service | Image | Port |
|------|---------|-------|------|
| `docker-compose.yaml` | `postgres` | postgres:15 | 5432 |
| | `minio` | minio/minio | 9000/9001 |
| | `trino` | trinodb/trino | 8090 |
| | `redis` | redis:alpine | 6379 |
| | `nessie` | projectnessie/nessie | 19120 |
| `docker-compose.kafka.yaml` | `zookeeper` | confluentinc/cp-zookeeper | 2181 |
| | `kafka` | confluentinc/cp-kafka | 9092 |
| | `kafka-control-center` | confluentinc/cp-enterprise-control-center | 9021 |
| | `debezium` | debezium/connect | 8083 |
| `docker-compose.spark.yaml` | `spark-master` | bitnami/spark | 7077/8091 |
| | `spark-worker` | bitnami/spark | — |
| `docker-compose.airflow.yaml` | `airflow-webserver` | apache/airflow | 8080 |
| | `airflow-scheduler` | apache/airflow | — |
| `docker-compose.monitoring.yaml` | `prometheus` | prom/prometheus | 9090 |
| | `grafana` | grafana/grafana | 3000 |
| | `vault` | hashicorp/vault | 8200 |
| `docker-compose.lineage.yaml` | `marquez` | marquezproject/marquez | 5000 |
| | `marquez-web` | marquezproject/marquez-web | 3001 |
| `docker-compose.app.yaml` | `streamlit` | custom | 8501 |
| | `jupyter` | jupyter/scipy-notebook | 8888 |
| | `mlflow` | ghcr.io/mlflow/mlflow | 5001 |

---

# 📋 Sprint Planning — Quy trình Scrum chuẩn doanh nghiệp

## Tổng quan Agile Framework

| Item | Detail |
|------|--------|
| **Methodology** | Scrum |
| **Sprint Duration** | 1 tuần / sprint |
| **Total Sprints** | 8 sprints (~8 tuần) |
| **Team Size** | 1 person (solo developer) |
| **Ceremonies** | Sprint Planning → Daily Log → Sprint Review → Sprint Retro |
| **Estimation** | Story Points (Fibonacci: 1, 2, 3, 5, 8, 13) |
| **Velocity Target** | ~20-25 SP/sprint |
| **Board** | GitHub Projects (Kanban) |
| **Branching** | `main` ← `develop` ← `feature/*` (Git Flow) |

### Definition of Done (DoD)
- [ ] Code hoạt động đúng chức năng
- [ ] Có unit test (nếu applicable)
- [ ] Docker service start thành công + health check pass
- [ ] README/docs cập nhật
- [ ] Code pushed lên branch, PR created
- [ ] Không có linting errors

### Story Point Guide
| SP | Effort | Ví dụ |
|----|--------|-------|
| 1 | ~1-2 giờ | Viết config file, seed CSV |
| 2 | ~2-4 giờ | Viết 1 extractor đơn giản |
| 3 | ~4-6 giờ | Setup Docker Compose + health check |
| 5 | ~1 ngày | Viết Spark job hoàn chỉnh |
| 8 | ~1.5-2 ngày | Airflow DAG phức tạp + test |
| 13 | ~2-3 ngày | Full streaming pipeline end-to-end |

---

## Sprint 0: Project Setup & Foundation *(Pre-sprint)*
> **Goal**: Repo sẵn sàng, infra core chạy được, team (mình) nắm rõ kiến trúc

| # | User Story | Tasks | SP | Priority |
|---|-----------|-------|-----|----------|
| 0.1 | Setup GitHub repo + branching strategy | Init repo, .gitignore, branch protection, README skeleton, LICENSE | 2 | 🔴 Must |
| 0.2 | Setup project structure | Tạo toàn bộ folder structure như spec ở trên | 2 | 🔴 Must |
| 0.3 | Setup core Docker Compose | `docker-compose.yaml`: PostgreSQL + MinIO + Trino, verify health | 3 | 🔴 Must |
| 0.4 | Setup .env.example + configs | `.env.example`, `configs/spark.yaml`, `configs/datalake.yaml` | 1 | 🔴 Must |
| 0.5 | Setup Makefile | Tất cả targets: up-core, up-kafka, run-all, down, test, lint | 2 | 🔴 Must |
| 0.6 | Setup shared utils | `utils/`: postgresql_client.py, minio_utils.py, helper.py | 3 | 🟡 Should |
| 0.7 | Setup Trino catalog | `trino/catalog/datalake.properties`, test query trên MinIO | 2 | 🟡 Should |
| 0.8 | Design architecture diagram | Vẽ diagram cho `docs/architecture.md` | 1 | 🟡 Should |
| 0.9 | 🗄️ Setup Redis (Cache Layer) | Thêm `redis:alpine` vào `docker-compose.yaml` (Core group, ~64MB). Viết `utils/redis_client.py` — dùng làm crawler fallback cache + API rate-limit buffer | 1 | 🔴 Must |
| 0.10 | 🔐 Setup HashiCorp Vault (Dev Mode) | Thêm `hashicorp/vault` vào `docker-compose.monitoring.yaml`. Seed secrets: DB passwords, API keys (NASA, OpenAQ). Update `.env.example` → hướng dẫn dùng Vault. **Dev mode: không cần license, 100% free** | 2 | 🟡 Should |
| 0.11 | 🗂️ Setup Nessie (Iceberg Catalog) | Thêm `projectnessie/nessie` vào `docker-compose.yaml` (Core group, port 19120). Đây là catalog layer quản lý Iceberg table phiên bản (GIT cho Dữ liệu) | 2 | 🔴 Must |

**Total: 21 SP** · **Acceptance Criteria**: `make up-core` → 6 services healthy (postgres, minio, trino, redis, nessie, + vault), `psql` connect OK, MinIO console accessible, Trino CLI query OK, Redis PING→PONG, Vault/Nessie UI accessible.

---

## Sprint 1: Batch Ingestion — 7 Vietnam Sources *(Week 1-2)*
> **Goal**: Data flows từ 5 APIs + 2 Crawlers → MinIO (Bronze) → PostgreSQL (Raw)

| # | User Story | Tasks | SP | Priority |
|---|-----------|-------|-----|----------|
| 1.1 | Viết BaseExtractor + BaseCrawler | `base.py`: extract/validate/save_raw, `base_crawler.py`: retry, rate-limit, parse HTML | 3 | 🔴 Must |
| 1.2 | Viết Air Quality Extractor | `air_quality.py`: OpenAQ API (country=VN) → Parquet | 3 | 🔴 Must |
| 1.3 | Viết Weather Extractor | `weather.py`: Open-Meteo (63 VN cities lat/lon) → Parquet | 3 | 🔴 Must |
| 1.4 | Viết Flood Extractor | `flood.py`: Open-Meteo Flood (VN rivers) → Parquet | 3 | 🔴 Must |
| 1.5 | Viết CEM AQI Crawler | `cem_aqi_crawler.py`: Scrape cem.gov.vn (Scrapy/BS4) → Parquet | 5 | 🔴 Must |
| 1.6 | Viết NCHMF Disaster Crawler | `nchmf_disaster_crawler.py`: Scrape nchmf.gov.vn warnings → Parquet | 5 | 🔴 Must |
| 1.7 | Viết Fire Hotspot Extractor | `fire_hotspot.py`: NASA FIRMS (country=VNM) → Parquet | 3 | 🟡 Should |
| 1.8 | Viết Geo Features Extractor | `geo_features.py`: OSM Overpass (VN bbox) → GeoJSON | 2 | 🟡 Should |
| 1.9 | Viết MinIO + PostgreSQL Loaders | Upload to `raw/` bucket + insert `bronze` schema | 5 | 🔴 Must |
| 1.10 | Incremental loading + seed data | Track timestamps + backfill 3-6 tháng lịch sử | 3 | 🔴 Must |
| 1.11 | Unit tests | `test_extractors.py`, `test_crawlers.py`, `test_loaders.py` | 2 | 🟡 Should |

**Total: 37 SP** · **Acceptance Criteria**: 5 extractors + 2 crawlers chạy thành công, data VN-only xuất hiện trong MinIO + PostgreSQL. Crawlers có retry + rate-limit. Unit tests pass.

---

## Sprint 2: Streaming + Kafka + dbt Transformation *(Week 2)*
> **Goal**: Real-time streaming hoạt động + dbt model star schema hoàn chỉnh

| # | User Story | Tasks | SP | Priority |
|---|-----------|-------|-----|----------|
| 2.1 | Setup Kafka Docker Compose | `docker-compose.kafka.yaml`: Zookeeper, Kafka, Control Center, Debezium | 5 | 🔴 Must |
| 2.2 | Viết Kafka Producer | `air_quality_producer.py`: poll API → produce to `raw-air-quality` topic | 3 | 🔴 Must |
| 2.3 | Viết Schema Validator | `schema_validation/validator.py`: valid→process, invalid→DLQ topic | 3 | 🔴 Must |
| 2.4 | Setup Debezium CDC | `debezium/configs/` + `run.sh`: capture PostgreSQL changes → Kafka | 3 | 🟡 Should |
| 2.5 | Setup dbt project | `dbt_project.yml`, `profiles.yml`, `packages.yml`, connection test | 2 | 🔴 Must |
| 2.6 | Viết dbt staging models (7 sources) | `stg_air_quality`, `stg_weather`, `stg_flood`, `stg_cem_aqi`, `stg_nchmf_disaster`, `stg_fire_hotspot`, `stg_geo_features` | 5 | 🔴 Must |
| 2.7 | Viết dbt marts models (Star Schema) | `fact_air_measurements`, `fact_weather_observations`, `fact_natural_disasters`, `dim_*` tables | 5 | 🔴 Must |
| 2.8 | Viết dbt intermediate models | `int_daily_aqi_city`, `int_weather_hourly`, `int_disaster_impact`, `int_environmental_risk` | 3 | 🟡 Should |
| 2.9 | Viết dbt tests + macros | Custom tests, `calculate_aqi.sql`, `calculate_risk_score.sql` macro | 2 | 🟡 Should |
| 2.10 | Viết dbt seeds | `vietnam_cities.csv`, `vietnam_rivers.csv`, `pollutant_standards.csv` | 1 | 🟡 Should |

**Total: 31 SP** · **Acceptance Criteria**: Kafka topic có messages, Schema Validator routing đúng, `dbt run && dbt test` pass, star schema tables (7 staging + 4 intermediate + 3 fact + 5 dim) populated trong PostgreSQL.

---

## Sprint 3: Spark Processing + Airflow Orchestration *(Week 3)*
> **Goal**: Spark jobs chạy batch transforms + Airflow DAGs orchestrate toàn bộ pipeline

| # | User Story | Tasks | SP | Priority |
|---|-----------|-------|-----|----------|
| 3.1 | Setup Spark Docker Compose | `docker-compose.spark.yaml`: master + worker, JAR files | 3 | 🔴 Must |
| 3.2 | Viết bronze_to_silver Spark job | Read MinIO Parquet → clean, deduplicate → PostgreSQL silver | 5 | 🔴 Must |
| 3.3 | Viết silver_to_gold Spark job | Join air quality + weather → compute metrics → PostgreSQL gold | 5 | 🔴 Must |
| 3.4 | Viết Spark Streaming consumer | Consume Kafka → window aggregation → alert logic → PostgreSQL | 8 | 🔴 Must |
| 3.5 | Viết convert_to_iceberg.py | Convert Parquet → Apache Iceberg format trong MinIO qua Nessie | 2 | 🟡 Should |
| 3.6 | Setup Airflow Docker Compose | `docker-compose.airflow.yaml`: webserver + scheduler + init | 3 | 🔴 Must |
| 3.7 | Viết batch_ingestion DAG | extract → upload_bronze → spark_transform → trigger_dbt | 5 | 🔴 Must |
| 3.8 | Viết dbt_dag | dbt run → dbt test → dbt docs generate | 3 | 🔴 Must |
| 3.9 | 🗂️ Setup Marquez Data Lineage | Deploy `marquezproject/marquez` + `marquez-web` via `docker-compose.lineage.yaml`. Cài `openlineage-airflow` pip package vào Airflow. Set `AIRFLOW__LINEAGE__BACKEND=openlineage` | 3 | 🟡 Should |
| 3.10 | 🗂️ DAG Lineage Metadata | Annotate toàn bộ DAGs với OpenLineage events. Marquez UI tại `localhost:3001` hiển thị data flow graph: API → Bronze → Silver → Gold → Dashboard | 2 | 🟡 Should |

**Total: 39 SP** *(stretch sprint — có thể move 3.4 sang Sprint 4)*

**Acceptance Criteria**: Spark submit jobs thành công từ Airflow, DAGs hiện trạng thái "success" (xanh lá) trên UI, data flows end-to-end từ APIs → Bronze → Silver → Gold. Marquez UI hiển thị lineage graph đầy đủ.

---

## Sprint 4: Data Quality + Dashboard + Monitoring + Sandbox *(Week 4)*
> **Goal**: GE validates data, Streamlit + JupyterLab Sandbox, Grafana monitors pipeline

| # | User Story | Tasks | SP | Priority |
|---|-----------|-------|-----|----------|
| 4.1 | Setup Great Expectations | `great_expectations.yml`, suites, checkpoints | 3 | 🔴 Must |
| 4.2 | Viết GE validation notebook | `full_flow.ipynb`: interactive validation + profiling | 2 | 🟡 Should |
| 4.3 | Viết data_quality DAG | Airflow: run GE → evaluate → alert on failure | 3 | 🔴 Must |
| 4.4 | Viết streaming_monitor DAG | Check Kafka lag, Spark job health, data freshness | 3 | 🟡 Should |
| 4.5 | Viết Streamlit app — AQI Map page | Vietnam map color-coded by AQI level, responsive | 5 | 🔴 Must |
| 4.6 | Viết Streamlit app — Trend Analysis | Time-series charts: PM2.5, temperature, humidity by city | 3 | 🔴 Must |
| 4.7 | Viết Streamlit app — Correlation Analysis | Scatter/heatmap: AQI vs Weather correlation | 3 | 🟡 Should |
| 4.8 | Viết Streamlit app — Alerts Panel | Real-time pollution alerts, threshold breaches | 2 | 🟡 Should |
| 4.9 | Setup Monitoring Docker Compose | `docker-compose.monitoring.yaml`: Prometheus + Grafana | 3 | 🔴 Must |
| 4.10 | Viết Grafana dashboards | Pipeline health, data freshness, data quality score | 3 | 🔴 Must |
| 4.11 | 🧪 Setup JupyterLab Sandbox | `docker-compose.app.yaml` thêm Jupyter service, tạo `sandbox` schema (read-write), PostgreSQL `gold` schema (read-only cho Jupyter user) | 3 | 🔴 Must |
| 4.12 | 🧪 Viết Sandbox notebooks | `01_eda_air_quality.ipynb`, `02_weather_correlation.ipynb`, `03_disaster_risk_analysis.ipynb` | 2 | 🟡 Should |
| 4.13 | 🧪 Setup MLflow Tracking Server | Thêm `mlflow` service vào `docker-compose.app.yaml` (port 5001). MLflow dùng PostgreSQL làm backend store, MinIO làm artifact store. **100% free, open-source** | 2 | 🟡 Should |
| 4.14 | 🧪 Integrate MLflow vào Sandbox | Log metrics/params/artifacts từ 3 sandbox notebooks → MLflow UI. Tạo experiment `UrbanPulse-EDA`. Thể hiện ML Experiment Tracking workflow | 2 | 🟡 Should |
| 4.15 | 📖 dbt Docs as Data Catalog | `dbt docs generate && dbt docs serve` → expose port 8085. Document toàn bộ models, sources, tests. Dùng như lightweight **Data Catalog** thay DataHub (quá nặng cho 16GB RAM) | 1 | 🟡 Should |

**Total: 40 SP**

**Acceptance Criteria**: GE checkpoint pass, Streamlit ≥3 pages, Grafana dashboards, JupyterLab accessible tại `localhost:8888`, `sandbox` schema tồn tại, Jupyter user chỉ SELECT được Gold nhưng INSERT/CREATE được Sandbox. MLflow UI tại `localhost:5001` hiển thị experiments. dbt docs tại `localhost:8085`.

---

## Sprint 5: Linux/Unix Automation + CI/CD *(Week 5)*
> **Goal**: Bash scripts cho pipeline automation, CI/CD, security hardening

| # | User Story | Tasks | SP | Priority |
|---|-----------|-------|-----|----------|
| 5.1 | Viết setup_env.sh | Bootstrap script: check dependencies, create dirs, set permissions (chmod), generate .env | 3 | 🔴 Must |
| 5.2 | Viết health_check.sh | Check Docker containers, DB connection, MinIO, Kafka — exit codes + color output | 3 | 🔴 Must |
| 5.3 | Viết backup_data.sh | `pg_dump` PostgreSQL + `mc mirror` MinIO → local backup folder, compressed (tar.gz) | 2 | 🟡 Should |
| 5.4 | Viết rotate_logs.sh | Find logs >7 days → compress (gzip), >30 days → delete. Uses `find`, `xargs`, `cron` | 2 | 🟡 Should |
| 5.5 | Viết monitor_pipeline.sh | Watchdog: check data freshness, Kafka lag, disk usage → alert via webhook on failure | 3 | 🔴 Must |
| 5.6 | Viết crontab_setup.sh + crontab.conf | Register cron jobs: ingestion mỗi 6h, health check mỗi 5 phút, backup hàng ngày, log rotation hàng tuần | 2 | 🔴 Must |
| 5.7 | Viết deploy.sh | One-click: pull latest → build images → run migrations → start services → health check | 3 | 🔴 Must |
| 5.8 | Setup GitHub Actions CI | `ci.yml`: lint + pytest + shellcheck (lint bash scripts) on every PR | 3 | 🔴 Must |
| 5.9 | Setup GitHub Actions dbt test | `dbt-test.yml`: dbt compile + test on merge to main | 2 | 🔴 Must |
| 5.10 | Viết comprehensive README | Architecture diagram, quick start, tech stack, screenshots | 5 | 🔴 Must |
| 5.11 | Viết data dictionary + runbook | `docs/data_dictionary.md` + `docs/runbook.md` | 3 | 🟡 Should |
| 5.12 | 🔐 Security Hardening Doc | Viết `docs/security.md`: IAM least privilege, secret rotation policy, encryption at rest (MinIO/S3 SSE), data masking strategy cho PII fields | 2 | 🟡 Should |
| 5.13 | 🔐 Vault secrets integration script | `scripts/bash/vault_seed.sh`: tự động seed secrets vào Vault khi bootstrap. Update `setup_env.sh` để pull secrets từ Vault thay vì `.env` | 2 | 🟡 Should |

**Total: 36 SP**

**Acceptance Criteria**: `bash scripts/bash/setup_env.sh` bootstraps project, `bash scripts/bash/health_check.sh` trả exit 0, crontab đã registered, CI/CD green trên GitHub, shellcheck pass tất cả .sh files. `docs/security.md` tồn tại. Vault seeded với đủ secrets.

---

## Sprint 6: ☁️ Hybrid Cloud Deployment (AWS + GCP) *(Week 6)*
> **Goal**: Pipeline chạy trên AWS (S3/Lambda) & GCP (BigQuery) — Maximize CV impact

| # | User Story | Tasks | SP | Priority |
|---|-----------|-------|-----|----------|
| 6.1 | Setup AWS & GCP accounts | Register AWS (Free Tier), GCP project, set budget alerts cho CẢ HAI | 2 | 🔴 Must |
| 6.2 | Hybrid Terraform IaC | Provision S3 (AWS) & BigQuery + Pub/Sub (GCP) dùng Terraform | 5 | 🔴 Must |
| 6.3 | AWS Lambda Ingestion | Viết Python Lambda: Crawl data → Lưu vào Amazon S3 (Bronze) | 5 | 🔴 Must |
| 6.4 | S3 to BigQuery Bridge | Python script/Lambda để chuyển data từ Amazon S3 sang GCP BigQuery | 5 | 🔴 Must |
| 6.5 | dbt on BigQuery | Chạy dbt models (Silver/Gold) trên BigQuery data | 3 | 🔴 Must |
| 6.6 | Looker Studio (GCP) | Thiết kế Dashboard kết nối trực tiếp BigQuery Gold tables | 3 | 🟡 Should |
| 6.7 | Hybrid Architecture Doc | Vẽ lại và tài liệu hóa cách kết nối AWS & GCP | 2 | 🟡 Should |
| 6.8 | Verify Cloud E2E | Trigger Lambda → S3 → BigQuery → Looker Studio works | 3 | 🔴 Must |

**Total: 28 SP**

**Acceptance Criteria**: `terraform apply` tạo đủ tài nguyên trên cả 2 cloud, data tự động chảy từ AWS Lambda về S3, sau đó sang BigQuery. Looker Studio hiển thị dữ liệu thực từ Cloud. Hóa đơn vẫn ở mức $0.

---

## Sprint 7: ☸️ Kubernetes Deployment *(Week 8)*
> **Goal**: Pipeline chạy trên K8s local (Kind) — thể hiện container orchestration skills

| # | User Story | Tasks | SP | Priority |
|---|-----------|-------|-----|----------|
| 7.1 | Setup Kind cluster | `kind-config.yaml`: multi-node (1 control + 2 workers), port mappings | 3 | 🔴 Must |
| 7.2 | Viết K8s namespaces | `urbanpulse-data`, `urbanpulse-app`, `monitoring` | 1 | 🔴 Must |
| 7.3 | Viết Helm chart — Data services | `helm/urbanpulse/templates/`: PostgreSQL (StatefulSet), MinIO, Kafka | 5 | 🔴 Must |
| 7.4 | Viết Helm chart — App services | Airflow, Spark, Streamlit (Deployments), Ingress | 5 | 🔴 Must |
| 7.5 | Viết Helm chart — Monitoring | Prometheus + Grafana on K8s, ServiceMonitor | 3 | 🟡 Should |
| 7.6 | Configmap + Secrets | Externalize configs, DB passwords, API keys via K8s native | 2 | 🔴 Must |
| 7.7 | Viết CronJob + Job manifests | `cronjob-ingestion.yaml` (mỗi 6h), `job-crawler.yaml` (on-demand) | 3 | 🔴 Must |
| 7.8 | Viết K8s deployment guide | `docs/k8s_deployment_guide.md`: step-by-step, screenshots | 2 | 🟡 Should |
| 7.9 | Verify K8s E2E | `make k8s-create && make k8s-deploy` → all pods Running, pipeline works | 2 | 🔴 Must |

**Total: 26 SP**

**Acceptance Criteria**: `make k8s-create` tạo cluster, `make k8s-deploy` deploy tất cả services, `kubectl get pods -A` hiển thị all Running, pipeline ingestion → transform → dashboard hoạt động trên K8s.

---

## Sprint Summary — Velocity Tracker

| Sprint | Goal | SP Planned | Gap Fills | Status |
|--------|------|-----------|-----------|--------|
| 0 | Project Setup & Foundation | 19 | Redis + Vault | ⬜ Not Started |
| 1 | Batch Ingestion — 7 VN Sources + Crawlers | 37 | — | ⬜ Not Started |
| 2 | Streaming + Kafka + dbt | 31 | — | ⬜ Not Started |
| 3 | Spark + Airflow + 🗂️ Data Lineage | 39 | Marquez + OpenLineage | ⬜ Not Started |
| 4 | Data Quality + Dashboard + 🧪 Sandbox + MLflow | 40 | MLflow + dbt Catalog | ⬜ Not Started |
| 5 | 🐧 Linux Automation + CI/CD + 🔐 Security | 36 | Vault Script + Security Doc | ⬜ Not Started |
| 6 | ☁️ Hybrid Cloud Deployment (AWS+GCP) | 28 | — | ⬜ Not Started |
| 7 | ☸️ Kubernetes Deployment | 26 | — | ⬜ Not Started |
| **TOTAL** | | **256 SP (~8 tuần)** | | |

---

## Burndown Tracking Template

```
Sprint X — [Goal]
Start: YYYY-MM-DD  |  End: YYYY-MM-DD

Day 1: ██████████░░░░░░░░░░  XX/YY SP done
Day 2: ████████████░░░░░░░░  XX/YY SP done
Day 3: ██████████████░░░░░░  XX/YY SP done
...
Day 7: ████████████████████  YY/YY SP done ✅

Blockers:
- [List any blockers encountered]

Retrospective:
- What went well:
- What to improve:
- Action items for next sprint:
```

---

## Verification Plan

### Access Points

| URL | Service | Verify |
|-----|---------|--------|
| http://localhost:8080 | Airflow | DAGs visible, trigger → all green |
| http://localhost:9001 | MinIO Console | Buckets: `raw/`, `processed/`, `iceberg/` |
| http://localhost:19120 | 🗂️ Nessie | Catalog REST API hoạt động bình thường |
| http://localhost:9021 | Kafka Control Center | Topics có messages, consumer lag = 0 |
| http://localhost:8083 | Debezium | Connector status: RUNNING |
| http://localhost:8090 | Trino | `SELECT * FROM datalake.raw.air_quality LIMIT 10` |
| http://localhost:8091 | Spark UI | Jobs completed successfully |
| http://localhost:8501 | Streamlit | Dashboard hiển thị charts với real data |
| http://localhost:8888 | 🧪 JupyterLab | Sandbox notebooks chạy được, query Gold OK |
| http://localhost:5001 | 🧪 MLflow | Experiments visible, metrics logged |
| http://localhost:8085 | 📖 dbt Docs | Data catalog: models, sources, tests documented |
| http://localhost:3000 | Grafana | Pipeline health dashboards |
| http://localhost:3001 | 🗂️ Marquez UI | Lineage graph: API→Bronze→Silver→Gold visible |
| http://localhost:8200 | 🔐 Vault UI | Secrets seeded, `kv/urbanpulse` path exists |
| http://localhost:9090 | Prometheus | Metrics being scraped |
| console.cloud.google.com | GCP Console | BigQuery tables populated, GCS files exist |
| lookerstudio.google.com | Looker Studio | Cloud dashboard hiển thị charts |

### End-to-End Test Flow

```bash
# 1. Start all services
make run-all

# 2. Seed historical data
make seed

# 3. Convert to Apache Iceberg
make convert-iceberg

# 4. Verify Trino can query lake
docker exec -it trino trino --execute "SELECT count(*) FROM datalake.raw.air_quality"

# 5. Trigger Airflow DAG
# → Open localhost:8080 → trigger batch_ingestion_dag → wait for green

# 6. Verify dbt models
make dbt-test

# 7. Verify data quality
make quality-check

# 8. Check dashboard
# → Open localhost:8501 → verify charts populated

# 9. Check monitoring
# → Open localhost:3000 → verify Grafana dashboards
```

---

## Linux/Unix Skills thể hiện trong project

| Skill | Ở đâu trong project |
|-------|---------------------|
| **Bash scripting** | 7 shell scripts tự động hóa pipeline (`scripts/bash/`) |
| **Crontab scheduling** | `crontab.conf`: lên lịch ingestion, backup, health check |
| **Process management** | `health_check.sh`: kiểm tra Docker containers, DB connections |
| **File system ops** | `rotate_logs.sh`: find, xargs, gzip, tar |
| **Networking** | Docker networks, port mapping, service discovery |
| **Permissions** | chmod, chown trong `setup_env.sh` |
| **Package management** | apt-get trong Dockerfiles (Linux-based images) |
| **Text processing** | grep, awk, sed trong monitoring scripts |
| **Environment variables** | .env files, export, envsubst |
| **SSH/SCP** | Remote deployment script `deploy.sh` |

---

## Kubernetes Skills thể hiện trong project

| Skill | Ở đâu trong project |
|-------|---------------------|
| **Cluster management** | Kind cluster create/delete, multi-node config |
| **Helm charts** | Custom chart `urbanpulse/` + monitoring chart |
| **Workload types** | Deployment, StatefulSet, CronJob, Job |
| **Networking** | Service, Ingress, port-forward |
| **Config management** | ConfigMap, Secret, values.yaml |
| **Namespaces** | urbanpulse-data, urbanpulse-app, monitoring |
| **Observability** | Prometheus + Grafana native on K8s |
| **kubectl** | logs, exec, describe, get, apply, rollout |

---

## Mô tả trên CV (gợi ý)

> **UrbanPulse VN** — Vietnam Environmental & Urban Analytics Platform
>
> Built a **production-grade data platform** analyzing Vietnam's environment & urban data from **7 sources** (5 REST APIs + 2 custom **web crawlers**). Architecture follows **Medallion Lakehouse** (Bronze→Silver→Gold→**Sandbox**) with a **Hybrid-Cloud** deployment: **AWS** (S3, Lambda) for serverless ingestion and **GCP** (BigQuery, Pub/Sub) for analytics.
>
> **Batch pipeline**: Airflow (with **OpenLineage** data lineage) orchestrates Python extractors/Scrapy crawlers → Amazon S3 (Bronze) → GCP BigQuery. **Kubernetes** deployment via Kind + Helm charts. **IaC** using Terraform for multi-cloud. Data modeled in **star schema** with dbt (serves as **Data Catalog** via dbt docs), validated with Great Expectations. **Open Lakehouse**: Built with Apache Iceberg and Nessie catalog queried by Trino. **Redis** cache layer protects crawler rate limits. **HashiCorp Vault** manages all secrets. **MLflow** tracks ML experiments in JupyterLab sandbox. Visualized with Streamlit + **Looker Studio**. Security hardened with IAM least-privilege + encryption at rest. Fully automated via **Linux Bash** (health checks, crontab, log rotation).
>
> **Tech**: Python · AWS (S3, Lambda) · GCP (BigQuery, Pub/Sub) · Airflow · Kafka · Spark · dbt · Trino · Apache Iceberg · Nessie · Redis · HashiCorp Vault · MLflow · Marquez (OpenLineage) · Kubernetes · Helm · Docker · Terraform · Linux/Bash · Scrapy · Great Expectations · Grafana · Looker Studio

