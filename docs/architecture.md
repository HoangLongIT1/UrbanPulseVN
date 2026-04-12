# Sơ đồ Kiến trúc UrbanPulse VN (Hybrid-Cloud & Medallion)

Kiến trúc bên dưới thể hiện luồng chạy dữ liệu từ lúc Ingestion (Batch/Streaming) qua các tầng Bronze, Silver, Gold và cuối cùng đưa lên hệ thống Serving và Dashboard theo kiến trúc Medallion kết hợp Open Data Lakehouse (Iceberg + Nessie + Trino).

```mermaid
flowchart TD
    %% Define Styles
    classDef aws fill:#FF9900,stroke:#232F3E,stroke-width:2px,color:#232F3E;
    classDef gcp fill:#4285F4,stroke:#0F9D58,stroke-width:2px,color:white;
    classDef tool fill:#f9f9f9,stroke:#333,stroke-width:1px;
    classDef data fill:#e1f5fe,stroke:#01579b,stroke-width:1px;
    
    %% Tầng Data Sources
    subgraph DataSources ["🌍 7 Vietnam Data Sources"]
        API1["OpenAQ (API)"]
        API2["Open-Meteo Weather"]
        API3["Open-Meteo Flood"]
        Crawl1["🕷 cem.gov.vn (Scrapy)"]
        Crawl2["🕷 nchmf.gov.vn (Scrapy)"]
        API4["NASA FIRMS (API)"]
        API5["OSM (Overpass API)"]
    end

    %% Tầng Ingestion & Streaming
    subgraph Ingestion ["🚀 Ingestion Layer (Python + K8s CronJob/Lambda)"]
        Batch["Batch Extractors"]
        StreamP["Kafka Producer"]
        Debezium["Debezium CDC"]
    end

    %% Streaming Broker
    subgraph Streaming ["⚡ Streaming Broker (Kafka)"]
        Kafka["Kafka Topics"]
        SchemaRegistry["Schema Validation (Avro)<br>Valid ➜ Process / Invalid ➜ DLQ"]
    end

    %% Tầng Data Lake (Bronze & Silver)
    subgraph DataLake ["🗄️ Open Data Lakehouse (MinIO + S3)"]
        Bronze[("🥉 Bronze<br>(MinIO / S3 Raw)")]
        Nessie{"🗂️ Nessie<br>(Iceberg Catalog)"}
        BronzeIce[("🧊 Bronze Iceberg Format")]
    end

    %% Tầng Data Warehouse (Silver & Gold)
    subgraph DWH ["🏢 Data Warehouse (PostgreSQL / GCP BigQuery)"]
        Silver[("🥈 Silver<br>(Cleaned & Enriched)")]
        Gold[("🥇 Gold<br>(Fact & Dim - Star Schema)")]
        Sandbox[("🧪 Sandbox<br>(Read-Write cho EDA)")]
    end

    %% Tầng Transform & Orchestration
    subgraph Transform ["⚙️ Transform & Orchestration"]
        Spark("Apache Spark<br>(Bronze ➜ Silver)")
        dbt("dbt<br>(Silver ➜ Gold)")
        Airflow{"Apache Airflow<br>(DAG Orchestration)"}
    end

    %% Tầng Serving & Dashboard
    subgraph Serving ["📊 Serving & Serving Layer"]
        Trino("🟢 Trino<br>(Federated Query Engine)")
        Streamlit("📈 Streamlit Dashboard")
        Jupyter("📓 JupyterLab Sandbox")
    end

    %% Tầng Monitoring & Quality
    subgraph Monitoring ["🛡️ Quality & Monitoring"]
        GE("Great Expectations (Data Quality)")
        PromGraf("Prometheus + Grafana (Infra)")
        MLflow("MLflow (Model Tracking)")
        Marquez("Marquez (Data Lineage)")
        Vault("HashiCorp Vault (Secrets)")
    end

    %% Define connections
    DataSources --> Batch
    API1 -.-> StreamP
    
    Batch --> Bronze
    StreamP --> Kafka
    Debezium -.-> Kafka
    Kafka --> SchemaRegistry
    SchemaRegistry --> Bronze
    
    Bronze --> Spark
    Spark --> Silver
    Spark -.-> BronzeIce
    BronzeIce -.-> Nessie
    
    Silver --> dbt
    dbt --> Gold
    dbt -.-> GE
    
    Gold -.-> Sandbox
    Gold --> Trino
    BronzeIce --> Trino
    
    Trino --> Streamlit
    Jupyter --> Sandbox
    Jupyter -.-> MLflow
    
    Airflow --> Batch
    Airflow --> Spark
    Airflow --> dbt
    
    %% Style applies
    class Batch,StreamP,Spark,dbt,Trino,Streamlit,Jupyter tool;
    class Bronze,Silver,Gold,BronzeIce,Sandbox data;
    class Bronze,Kafka aws;
    class Silver,Gold,Streamlit gcp;
```

## Giải thích Luồng Dữ liệu (Data Flow Diagram)

1. **Ingestion Layer:** 
   Các API và Crawler của Python sẽ thu thập dữ liệu về chất lượng không khí, thời tiết, thiên tai từ 7 nguồn tập trung vào Việt Nam. Dữ liệu batch được tải thẳng vào MinIO (Bronze Layer).

2. **Streaming & CDC:** 
   Các luồng stream API được bắn vào Kafka qua các schema đã định nghĩa (Schema Registry). Thông tin thay đổi từ PostgreSQL được bắt thông qua Debezium.

3. **Data Lakehouse (Nessie + Iceberg + MinIO):**
   Thay vì quản lý Data Lake thô, dữ liệu sau Ingestion được đẩy lên MinIO và chuyển sang định dạng Apache Iceberg được quản lý phiên bản bởi Project Nessie (cho hiệu suất đọc-ghi và time-travel tốt hơn).

4. **Transforming (Medallion Architecture):**
   * **Bronze ➔ Silver (Apache Spark):** Spark lấy nội dung gốc, làm sạch, xoá trùng lặp, chuẩn hoá kiểu dữ liệu rồi đẩy vào PostgreSQL (đóng vai trò như DWH nhỏ) / BigQuery sau này.
   * **Silver ➔ Gold (dbt):** dbt xây dựng các mô hình dữ liệu (Data Modeling) Star Schema gồm Fact và Dim tables trên Silver data để chuyển đổi thành Gold data phục vụ cho BI và Report.

5. **Data Quality & Sandbox:**
   * Dữ liệu xuyên suốt quá trình được Great Expectations validate. 
   * ML Engineers/Data Analysts có Schema `Sandbox` riêng biệt kết nối qua JupyterLab để gọi Model EDA hoặc query thử mô hình Gold, đồng thời track ML qua MLflow.

6. **Serving & Query Engine:**
   Sử dụng Trino làm federated query engine đứng ranh giới giữa Data Lake (MinIO) và Data Warehouse (Postgres) cho phép query liên thông (cross-query) mà không cần di chuyển dữ liệu, phục vụ Dashboard Streamlit hiển thị.

7. **Orchestration & Lineage:**
   Toàn bộ luồng được lập lịch bởi Apache Airflow, giám sát sự cố bởi Grafana và Lineage biểu diễn trực quan bởi OpenLineage (Marquez).
