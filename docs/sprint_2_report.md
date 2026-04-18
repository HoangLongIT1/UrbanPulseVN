# 🚀 Sprint 2 — Code Complete Report

## Status: ✅ ALL CODE WRITTEN (chưa test với data thật)

---

## Tổng kết: 40+ files được tạo

### Mảng B — dbt Transformation (28 files)

| Layer | Files | Chi tiết |
|-------|-------|----------|
| **Config** (3) | `dbt_project.yml`, `profiles.yml`, `packages.yml` | Kết nối PostgreSQL, Medallion schema mapping |
| **Seeds** (3) | `vietnam_cities.csv`, `vietnam_rivers.csv`, `pollutant_standards.csv` | 63 tỉnh, 12 điểm sông, 6 chất ô nhiễm |
| **Sources** (1) | `sources.yml` | 7 bronze tables mapped |
| **Staging** (7+1) | `stg_air_quality`, `stg_weather`, `stg_flood`, `stg_cem_aqi`, `stg_nchmf_disaster`, `stg_fire_hotspot`, `stg_geo_features` + `schema.yml` | Clean, cast, COALESCE, filter outliers |
| **Intermediate** (4+1) | `int_daily_aqi_city`, `int_weather_hourly`, `int_disaster_impact`, `int_environmental_risk` + `schema.yml` | AQI aggregation, heat index, risk scoring |
| **Marts** (8+1) | 5 dims + 3 facts + `schema.yml` | Star Schema hoàn chỉnh |
| **Macros** (2) | `calculate_aqi.sql`, `calculate_risk_score.sql` | EPA AQI formula, composite risk 0-100 |
| **Tests** (1) | `assert_aqi_range.sql` | Custom AQI range validation |

#### Star Schema Design:
```
                    dim_time ──────────┐
                    dim_location ──────┤
                    dim_pollutant ─────┼── fact_air_measurements
                    dim_station ───────┘
                    
                    dim_time ──────────┐
                    dim_location ──────┼── fact_weather_observations
                                       
                    dim_time ──────────┐
                    dim_disaster_type ─┼── fact_natural_disasters
```

### Mảng A — Kafka Streaming (8 files)

| Component | Files | Chi tiết |
|-----------|-------|----------|
| **Infrastructure** | `docker-compose.kafka.yaml` | Zookeeper (512MB), Kafka (1GB), Debezium (512MB), Control Center (1GB) |
| **Topics** | Auto-created via `kafka-init` | `raw-air-quality`, `processed-air-quality`, `dlq-air-quality`, `cdc-postgres` |
| **Schema** | `air_quality.avsc` | Avro schema with pollutant enum |
| **Producer** | `air_quality_producer.py` | Poll OpenAQ → publish to Kafka (5min interval) |
| **Validator** | `validator.py` | Validate schema+range+geo → route valid/DLQ |
| **CDC** | `postgres-connector.json`, `register.sh` | Debezium CDC tracking 6 bronze tables |

### Phụ trợ
- `requirements.txt` — thêm `kafka-python`, `dbt-postgres`
- `.env` — uncomment Kafka ports
- `__init__.py` × 3 — package inits

---

## ⏭️ Khi anh sẵn sàng test

### Bước 1: Nạp data (chưa chạy)
```powershell
# Start Docker
make up-core

# Seed data vào Bronze
$env:PYTHONIOENCODING='utf-8'
.venv\Scripts\python -m ingestion.pipeline --mode seed
```

### Bước 2: Test dbt
```powershell
cd dbt_transform
dbt deps          # Install dbt-utils package
dbt seed          # Load CSV seeds
dbt run           # Build all models
dbt test          # Run all tests
```

### Bước 3: Test Kafka
```powershell
# Start Kafka services
make up-kafka

# Register Debezium connector
bash streaming/debezium/configs/register.sh

# Start producer (terminal 1)
.venv\Scripts\python -m streaming.producer.air_quality_producer

# Start validator (terminal 2)
.venv\Scripts\python -m streaming.schema_validation.validator
```

---

## Acceptance Criteria Checklist
- [x] `docker-compose.kafka.yaml` tạo xong (4 services)
- [ ] `make up-kafka` → tất cả services xanh lá *(cần Docker chạy)*
- [ ] Kafka topic nhận message *(cần Producer chạy)*
- [ ] Schema Validator routing valid/DLQ *(cần Kafka chạy)*
- [ ] Debezium CDC capture events *(cần PostgreSQL WAL=logical)*
- [ ] `dbt run` thành công *(cần data trong bronze)*
- [ ] `dbt test` pass 100% *(cần data trong bronze)*
- [x] Star schema: 7 staging + 4 intermediate + 3 fact + 5 dim = **19 models** tạo xong
