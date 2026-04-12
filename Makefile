# =============================================
# UrbanPulse VN — Makefile
# =============================================
# ⚠️  RAM WARNING: NEVER run 'make run-all' on 16GB machine!
#     See warnings.md for safe service grouping.
#
# Quick start:
#   cp .env.example .env
#   make up-core      # Start core services (~2.1GB RAM)
#   make health       # Verify all services healthy
#   make down         # Stop everything

.PHONY: help up-network up-core up-monitoring up-kafka up-spark up-airflow up-lineage up-app \
        down status logs health clean \
        test-unit test-e2e dbt-test quality-check lint \
        k8s-create k8s-deploy k8s-monitoring k8s-status k8s-delete \
        cloud-init cloud-plan cloud-apply cloud-destroy

# Default target
help:
	@echo ""
	@echo "  🌏 UrbanPulse VN — Available Commands"
	@echo "  ======================================"
	@echo ""
	@echo "  Services:"
	@echo "    make up-core         Start core (PostgreSQL, MinIO, Trino, Redis, Nessie)"
	@echo "    make up-monitoring   Start monitoring (Vault, Prometheus, Grafana)"
	@echo "    make up-kafka        Start streaming (Kafka, Zookeeper, Debezium)"
	@echo "    make up-spark        Start processing (Spark master + worker)"
	@echo "    make up-airflow      Start orchestration (Airflow webserver + scheduler)"
	@echo "    make up-lineage      Start lineage (Marquez + Marquez Web)"
	@echo "    make up-app          Start apps (Streamlit, JupyterLab, MLflow)"
	@echo "    make down            Stop ALL services"
	@echo ""
	@echo "  Operations:"
	@echo "    make status          Show container status"
	@echo "    make logs            Tail logs (all running services)"
	@echo "    make health          Check health of core services"
	@echo "    make clean           Stop + remove volumes (⚠️  DELETES DATA)"
	@echo ""
	@echo "  Testing:"
	@echo "    make test-unit       Run unit tests"
	@echo "    make test-e2e        Run integration tests"
	@echo "    make dbt-test        Run dbt tests"
	@echo "    make quality-check   Run Great Expectations"
	@echo "    make lint            Run linters (flake8, black)"
	@echo ""
	@echo "  Kubernetes:"
	@echo "    make k8s-create      Create Kind cluster"
	@echo "    make k8s-deploy      Deploy with Helm"
	@echo "    make k8s-status      Show pod status"
	@echo "    make k8s-delete      Delete Kind cluster"
	@echo ""
	@echo "  Cloud:"
	@echo "    make cloud-init      Terraform init"
	@echo "    make cloud-plan      Terraform plan"
	@echo "    make cloud-apply     Terraform apply"
	@echo "    make cloud-destroy   Terraform destroy"
	@echo ""

# ── NETWORK ──────────────────────────────────────────
up-network:
	@docker network create urbanpulse-net 2>/dev/null || true
	@echo "✅ Network urbanpulse-net ready"

# ── CORE SERVICES (Sprint 0) ────────────────────────
# PostgreSQL + MinIO + Trino + Redis + Nessie (~2.1GB RAM)
up-core: up-network
	@echo "🚀 Starting core services..."
	docker compose -f docker-compose.yaml up -d
	@echo "✅ Core services started! Check: make status"

# ── MONITORING (Sprint 0: Vault / Sprint 4: Prometheus+Grafana) ──
up-monitoring: up-network
	@echo "🚀 Starting monitoring services..."
	docker compose -f docker-compose.monitoring.yaml up -d
	@echo "✅ Monitoring services started!"

# ── KAFKA & STREAMING (Sprint 2) ────────────────────
up-kafka: up-network
	@echo "🚀 Starting Kafka services..."
	docker compose -f docker-compose.kafka.yaml up -d
	@echo "✅ Kafka services started!"

# ── SPARK (Sprint 3) ────────────────────────────────
up-spark: up-network
	@echo "🚀 Starting Spark services..."
	docker compose -f docker-compose.spark.yaml up -d
	@echo "✅ Spark services started!"

# ── AIRFLOW (Sprint 3) ──────────────────────────────
up-airflow: up-network
	@echo "🚀 Starting Airflow services..."
	docker compose -f docker-compose.airflow.yaml up -d
	@echo "✅ Airflow services started!"

# ── DATA LINEAGE (Sprint 3) ─────────────────────────
up-lineage: up-network
	@echo "🚀 Starting Marquez lineage services..."
	docker compose -f docker-compose.lineage.yaml up -d
	@echo "✅ Lineage services started!"

# ── APPLICATIONS (Sprint 4) ─────────────────────────
up-app: up-network
	@echo "🚀 Starting application services..."
	docker compose -f docker-compose.app.yaml up -d
	@echo "✅ Application services started!"

# ── ALL SERVICES ─────────────────────────────────────
# ⚠️  WARNING: DO NOT USE on machines with ≤16GB RAM!
# This will start ~20 containers consuming ~12-14GB RAM.
run-all: up-network up-core up-kafka up-spark up-airflow up-monitoring up-lineage up-app
	@echo "⚠️  ALL services started — monitor RAM with: docker stats"

# ── STOP ─────────────────────────────────────────────
down:
	@echo "🛑 Stopping all services..."
	-docker compose -f docker-compose.yaml down 2>/dev/null
	-docker compose -f docker-compose.monitoring.yaml down 2>/dev/null
	-docker compose -f docker-compose.kafka.yaml down 2>/dev/null
	-docker compose -f docker-compose.spark.yaml down 2>/dev/null
	-docker compose -f docker-compose.airflow.yaml down 2>/dev/null
	-docker compose -f docker-compose.lineage.yaml down 2>/dev/null
	-docker compose -f docker-compose.app.yaml down 2>/dev/null
	@echo "✅ All services stopped"

# ── STATUS & LOGS ────────────────────────────────────
status:
	@echo "📊 Container Status:"
	@docker compose -f docker-compose.yaml ps 2>/dev/null
	@docker compose -f docker-compose.monitoring.yaml ps 2>/dev/null

logs:
	docker compose -f docker-compose.yaml logs -f --tail=50

# ── HEALTH CHECK ─────────────────────────────────────
health:
	@echo ""
	@echo "🏥 UrbanPulse VN — Health Check"
	@echo "================================"
	@echo ""
	@echo "PostgreSQL:" && docker exec urbanpulse-postgres pg_isready -U urbanpulse 2>/dev/null && echo "  ✅ Healthy" || echo "  ❌ Not running"
	@echo ""
	@echo "MinIO:" && curl -sf http://localhost:9000/minio/health/live > /dev/null 2>&1 && echo "  ✅ Healthy (Console: http://localhost:9001)" || echo "  ❌ Not running"
	@echo ""
	@echo "Redis:" && docker exec urbanpulse-redis redis-cli ping 2>/dev/null | grep -q PONG && echo "  ✅ Healthy (PONG)" || echo "  ❌ Not running"
	@echo ""
	@echo "Nessie:" && curl -sf http://localhost:19120/api/v2/config > /dev/null 2>&1 && echo "  ✅ Healthy (API: http://localhost:19120)" || echo "  ❌ Not running"
	@echo ""
	@echo "Trino:" && curl -sf http://localhost:8090/v1/info > /dev/null 2>&1 && echo "  ✅ Healthy (UI: http://localhost:8090)" || echo "  ❌ Not running (may need ~30s to start)"
	@echo ""

# ── CLEAN (DANGER) ───────────────────────────────────
clean:
	@echo "⚠️  WARNING: This will DELETE all data volumes!"
	@echo "Press Ctrl+C to cancel..."
	@sleep 3
	docker compose -f docker-compose.yaml down -v
	-docker compose -f docker-compose.monitoring.yaml down -v 2>/dev/null
	@echo "🗑️  All volumes removed"

# ── DATA OPERATIONS ──────────────────────────────────
seed:
	python scripts/seed_historical_data.py

convert-iceberg:
	python scripts/convert_to_iceberg.py

# ── TESTING ──────────────────────────────────────────
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

# ── KUBERNETES (Sprint 7) ────────────────────────────
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

# ── CLOUD (Sprint 6) ────────────────────────────────
cloud-init:
	cd cloud/terraform && terraform init

cloud-plan:
	cd cloud/terraform && terraform plan

cloud-apply:
	cd cloud/terraform && terraform apply -auto-approve

cloud-destroy:
	cd cloud/terraform && terraform destroy -auto-approve
