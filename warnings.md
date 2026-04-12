# ⚠️ UrbanPulse VN — Cảnh báo & Hướng dẫn khi Build

> File này PHẢI được đọc trước khi thực hiện BẤT KỲ task nào trong project.

---

## 🔴 1. Giới hạn phần cứng — RAM 16GB

Máy chỉ có **16GB RAM**. Chạy cả 15 Docker services cùng lúc **SẼ TREO MÁY**.

### Quy tắc bắt buộc
- **KHÔNG BAO GIỜ** chạy `make run-all` trên máy 16GB
- Chỉ bật **nhóm services đang làm việc**, tắt phần còn lại
- Luôn kiểm tra RAM trước khi start thêm service: `docker stats --no-stream`
- Nếu RAM >80% → **DỪNG LẠI**, tắt bớt services

### Nhóm services theo Sprint (chạy từng nhóm)

| Nhóm | Services | RAM ước tính | Khi nào bật |
|------|----------|-------------|-------------|
| **Core** | postgres, minio, trino, **redis** | ~2.1GB | Luôn bật khi dev (Redis chỉ ~64MB) |
| **Kafka** | zookeeper, kafka, control-center, debezium | ~3-4GB | Sprint 2 (streaming) |
| **Spark** | spark-master, spark-worker | ~2-3GB | Sprint 3 (processing) |
| **Airflow** | airflow-webserver, airflow-scheduler | ~1.5GB | Sprint 3 (orchestration) |
| **Lineage** | marquez, marquez-web | ~512MB | Sprint 3 (data lineage) |
| **App** | streamlit, jupyter, **mlflow** | ~1.5GB | Sprint 4 (dashboard/sandbox/ml) |
| **Monitoring** | prometheus, grafana, **vault** | ~0.8GB | Sprint 4 (monitoring + secrets) |

### Kịch bản RAM an toàn

```bash
# Sprint 0-1: Chỉ cần Core (ingestion không cần Kafka/Spark)
make up-core                          # ~2.1GB → an toàn (Redis thêm ~64MB)

# Sprint 2: Core + Kafka (tắt Spark)
make up-core && make up-kafka         # ~5-6GB → an toàn

# Sprint 3: Core + Spark + Airflow + Lineage (tắt Kafka nếu không test streaming)
make up-core && make up-spark && make up-airflow && make up-lineage   # ~6.5-7.5GB → chú ý RAM

# Sprint 4: Core + App + Monitoring (tắt Kafka + Spark)
make up-core && make up-app && make up-monitoring  # ~4.5GB → an toàn

# Full test E2E: Tắt control-center + spark-worker (tiết kiệm ~2GB)
# Chỉ bật khi cần verify cuối sprint
```

### Mẹo tiết kiệm RAM
- Giới hạn RAM cho mỗi container trong docker-compose:
  ```yaml
  deploy:
    resources:
      limits:
        memory: 512M   # hoặc 1G tùy service
  ```
- Dùng `docker compose ... --scale spark-worker=1` (chỉ 1 worker thay vì 2)
- Tắt Kafka Control Center khi không cần UI (tiết kiệm ~500MB)
- **K8s (Kind)**: chỉ chạy khi đã tắt hết Docker Compose services

---

## 🟠 2. Rủi ro Web Crawler — cem.gov.vn & nchmf.gov.vn

2 crawlers là **thành phần rủi ro cao nhất** của project.

### Các tình huống phải dự phòng

| Rủi ro | Xác suất | Hậu quả | Biện pháp |
|--------|----------|---------|-----------|
| **Website sập/bảo trì** | Trung bình | Crawler fail, pipeline thiếu data | Retry 3 lần + exponential backoff (5s → 30s → 120s). Nếu vẫn fail → ghi log WARNING, skip và dùng data cache cũ |
| **Bị chặn (rate-limit/ban IP)** | Cao | HTTP 403/429, crawler bị block | Rate-limit: tối đa 1 request/5 giây. Random delay 2-8s giữa các request. Rotate User-Agent. **KHÔNG dùng proxy** (giữ project đơn giản) |
| **Website đổi giao diện/HTML** | Cao | Parser trả về data rỗng/sai | CSS selector PHẢI được cấu hình qua `config.py`, KHÔNG hardcode. Viết test kiểm tra output schema. Nếu parse fail → alert + fallback sang cache |
| **Data không nhất quán** | Trung bình | Giá trị AQI bất thường, missing fields | Great Expectations validation sau mỗi lần crawl. Schema check trước khi load vào Bronze |
| **Website yêu cầu CAPTCHA/JS** | Thấp | Crawler không lấy được data | Nếu xảy ra → chuyển sang dùng Selenium headless. Ghi vào backlog, không block pipeline |

### Thiết kế crawler bắt buộc

```python
class BaseCrawler:
    MAX_RETRIES = 3
    BACKOFF_FACTOR = 2         # exponential: 5s, 10s, 20s
    REQUEST_DELAY = (2, 8)     # random delay giữa requests (giây)
    CACHE_TTL = 3600           # cache data 1 giờ khi source fail
    
    def crawl(self):
        # 1. Check cache trước (fallback nếu source down)
        # 2. Request với retry + backoff
        # 3. Parse HTML với configurable selectors
        # 4. Validate output schema
        # 5. Save to cache + return data
```

### Fallback strategy
- Mỗi crawler **PHẢI** có file cache (`.json` hoặc `.parquet`) chứa data gần nhất
- Nếu crawl fail sau 3 retries → dùng cache, pipeline KHÔNG bị break
- Alert qua log/webhook để biết cần sửa parser

---

## 🟡 3. Pre-commit — Tự động format code

### Cài đặt ngay từ Sprint 0

```bash
pip install pre-commit
pre-commit install
```

### File `.pre-commit-config.yaml`

```yaml
repos:
  # Python formatting
  - repo: https://github.com/psf/black
    rev: 24.4.2
    hooks:
      - id: black
        language_version: python3

  # Python import sorting
  - repo: https://github.com/pycqa/isort
    rev: 5.13.2
    hooks:
      - id: isort
        args: ["--profile", "black"]

  # SQL formatting (dbt models)
  - repo: https://github.com/sqlfluff/sqlfluff
    rev: 3.0.7
    hooks:
      - id: sqlfluff-fix
        args: ["--dialect", "postgres"]
        files: "dbt_transform/.*\\.sql$"

  # Shell script linting
  - repo: https://github.com/shellcheck-py/shellcheck-py
    rev: v0.10.0.1
    hooks:
      - id: shellcheck
        files: "scripts/bash/.*\\.sh$"

  # General
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v4.6.0
    hooks:
      - id: trailing-whitespace
      - id: end-of-file-fixer
      - id: check-yaml
      - id: check-json
      - id: check-added-large-files
        args: ["--maxkb=5000"]
```

### File `pyproject.toml` (cấu hình Black + isort)

```toml
[tool.black]
line-length = 88
target-version = ["py311"]

[tool.isort]
profile = "black"
line_length = 88
```

### File `.sqlfluff`

```ini
[sqlfluff]
dialect = postgres
templater = dbt
max_line_length = 120

[sqlfluff:rules:capitalisation.keywords]
capitalisation_policy = upper
```

### Lưu ý
- Pre-commit chạy **tự động** mỗi lần `git commit`
- Nếu format code fail → commit bị chặn → sửa → commit lại
- CI/CD (GitHub Actions) cũng chạy `pre-commit run --all-files` để double-check

---

## 🟡 4. Dự đoán tình huống & Cảnh báo sớm theo Sprint

### Sprint 0: Project Setup
| Tình huống | Cảnh báo |
|------------|---------|
| Docker Compose conflict port | Luôn kiểm tra `netstat -ano | findstr :PORT` trước khi start |
| MinIO không start được | Cần tạo thư mục data trước: `mkdir -p ./data/minio` |
| Trino connection timeout | Trino cần ~30s để khởi động, đợi health check pass |

### Sprint 1: Batch Ingestion
| Tình huống | Cảnh báo |
|------------|---------|
| OpenAQ API rate limit | Giới hạn 100 requests/phút (free tier). Dùng batch query, không loop từng station |
| Open-Meteo trả data rỗng cho 1 số tỉnh | Một số tỉnh nhỏ không có weather station gần → interpolate hoặc skip |
| NASA FIRMS cần EDL token | Phải đăng ký account trước tại earthdata.nasa.gov (mất 5-10 phút approve) |
| cem.gov.vn đổi HTML structure | Viết integration test chạy hàng tuần kiểm tra parser |
| Seed historical data chạy lâu | 3-6 tháng × 7 sources có thể mất 30-60 phút. Chạy background + tee log |

### Sprint 2: Streaming + dbt
| Tình huống | Cảnh báo |
|------------|---------|
| Kafka + Zookeeper ăn RAM khủng | Giới hạn memory trong compose: Kafka 1GB, Zookeeper 512MB |
| Debezium connector fail | Phải start PostgreSQL TRƯỚC Debezium. Kiểm tra WAL level = logical |
| dbt test fail do null values | Crawler data thường có null → cần `coalesce()` trong staging models |
| Schema Registry conflict | Xóa schema cache nếu thay đổi Avro schema: `docker volume rm ...` |

### Sprint 3: Spark + Airflow + Lineage
| Tình huống | Cảnh báo |
|------------|---------|
| Spark OOM (Out of Memory) | Trên 16GB máy, set `spark.driver.memory=1g`, `spark.executor.memory=1g` |
| Airflow DAG import error | Mọi file trong `dags/` đều bị Airflow scan. KHÔNG để file test ở đây |
| Spark không đọc được MinIO | Cần JAR files: `hadoop-aws`, `aws-java-sdk-bundle`. Đặt trong `spark_jobs/jars/` |
| PySpark + Apache Iceberg version | Cần add `iceberg-spark-runtime` jar và `nessie-spark-extensions` để tương thích Spark 3.x |
| Marquez + Airflow không gửi được lineage | Kiểm tra biến môi trường `OPENLINEAGE_URL=http://marquez:5000`. Airflow phải có `openlineage-airflow` package trong image |
| Marquez ăn RAM Sprint 3 | Nếu RAM đầy: tắt `marquez-web` (UI, ~200MB), chỉ giữ `marquez` API (~300MB) |

### Sprint 4: Quality + Dashboard + Sandbox + MLflow
| Tình huống | Cảnh báo |
|------------|---------|
| Great Expectations quá chậm | Chạy trên sample data (10%) khi dev, full data chỉ khi ci/cd |
| Streamlit map render chậm | Dùng `st.cache_data` + giới hạn points trên map (<5000) |
| JupyterLab không connect được PostgreSQL | Cần cài `psycopg2-binary` trong Jupyter Dockerfile |
| Sandbox user vô tình DROP table Gold | **BẮT BUỘC**: tạo PostgreSQL user riêng với `GRANT SELECT ON SCHEMA gold` |
| MLflow không kết nối được MinIO (artifact store) | Cấu hình `MLFLOW_S3_ENDPOINT_URL=http://minio:9000` trong docker-compose. Dùng MinIO bucket `mlflow-artifacts` |
| MLflow + PostgreSQL backend conflict | Dùng DB riêng: tạo database `mlflow` trong PostgreSQL, không dùng chung schema |

### Sprint 5: Linux + CI/CD
| Tình huống | Cảnh báo |
|------------|---------|
| Shell script chạy trên Windows | Tất cả .sh scripts chạy TRONG Docker container (Linux). KHÔNG chạy trên PowerShell |
| Crontab không hoạt động trên host Windows | Crontab chạy bên trong container hoặc WSL, không phải Windows native |
| GitHub Actions timeout | Set `timeout-minutes: 15` cho mỗi job. Docker build cache để tăng tốc |
| Vault token hết hạn trong dev mode | **Dev mode Vault reset khi restart container** — phải re-seed secrets sau mỗi `docker compose down`. Dùng `vault_seed.sh` để tự động hoá |

### Sprint 6: Hybrid Cloud (AWS + GCP)
| Tình huống | Cảnh báo |
|------------|---------|
| Phát sinh phí ngoài ý muốn | **ĐẶT BUDGET ALERT $0 TRÊN CẢ AWS & GCP**. AWS rất nghiêm khắc về billing |
| AWS S3 hết 12 tháng free | S3 chỉ miễn phí 12 tháng. Sau đó sẽ tính phí lưu trữ. Check ngày tạo account |
| AWS Athena quét quá nhiều data | Athena tính $5/TB. LUÔN dùng partition (year/month/day) và limit query |
| AWS Lambda timeout | Crawl web có thể lâu, set timeout Lambda ≥ 180s |
| Bảo mật IAM Keys | KHÔNG bao giờ commit AWS Access Keys lên GitHub. Dùng `.env` và `gitignore` |
| BigQuery scan toàn bộ table | LUÔN dùng `WHERE` clause + partition. 1TB free/tháng hết rất nhanh nếu full scan |
| Terraform state conflict | Dùng local backend khi dev. KHÔNG commit `.tfstate` lên GitHub |

### Sprint 7: Kubernetes
| Tình huống | Cảnh báo |
|------------|---------|
| Kind cluster ăn thêm RAM | Kind cluster cần ~1.5-2GB riêng. **TẮT Docker Compose trước** khi chạy K8s |
| Helm chart values sai | Luôn dùng `helm template ... --dry-run` trước khi `helm install` |
| ImagePullBackOff | Kind không pull được image từ local. Cần `kind load docker-image IMAGE` |
| PersistentVolumeClaim stuck Pending | Kind cần StorageClass mặc định. Kiểm tra `kubectl get sc` |

---

## 🔵 6. Lưu ý đặc biệt cho Hybrid-Cloud (AWS + GCP)

- **Quản lý Credentials**: Bạn sẽ cần cả `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` và GCP `service-account.json`. Hãy đặt tên biến môi trường rõ ràng trong `.env`.
- **Latency**: Truyền dữ liệu giữa AWS (ví dụ Singapore region) và GCP (ví dụ Iowa region) có thể bị chậm. Hãy cố gắng chọn Region gần nhau nhất có thể (ví dụ: `ap-southeast-1` cho AWS và `asia-southeast1` cho GCP).
- **Service Account Permissions**: Gán quyền tối thiểu (Least Privilege). Lambda chỉ cần quyền `S3FullAccess`, dbt chỉ cần quyền `BigQuery Data Editor`.
- **Kiểm tra Billing**: Vào bảng điều khiển Billing của cả 2 cloud ít nhất 1 lần/tuần.

---

## 🟢 5. Checklist trước mỗi lần code

```
□ Kiểm tra RAM hiện tại (docker stats / Task Manager)
□ Chỉ bật services cần thiết cho task hiện tại
□ Pre-commit hooks đã cài (pre-commit install)
□ Branch đúng (feature/* từ develop)
□ .env file đã copy từ .env.example
□ Đọc lại warnings cho Sprint hiện tại (mục 4 phía trên)
```

---

## 📌 Quick Reference — Lệnh hữu ích

```bash
# Kiểm tra RAM Docker đang dùng
docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}"

# Tắt tất cả containers
docker compose -f docker-compose.yaml -f docker-compose.kafka.yaml -f docker-compose.spark.yaml -f docker-compose.airflow.yaml -f docker-compose.monitoring.yaml -f docker-compose.app.yaml down

# Xóa volumes (reset data) — ⚠️ MẤT HẾT DATA
docker volume prune -f

# Kiểm tra port đang dùng
netstat -ano | findstr :8080

# Pre-commit chạy thủ công
pre-commit run --all-files
```
