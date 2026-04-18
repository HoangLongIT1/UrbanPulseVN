# Báo cáo Tổng kết Sprint 1: Batch Ingestion (Hệ thống Thu thập Dữ liệu)

## 🎯 Mục đích của Sprint 1
Sprint 1 tập trung vào xây dựng **Ingestion Layer (Tầng Thu Thập)** cho kiến trúc Medallion Open Lakehouse của UrbanPulse VN. 

Mục tiêu chính là tạo ra một hệ thống thu thập dữ liệu (Extract) cực kỳ bền bỉ (resilient) từ 7 nguồn dữ liệu ngoại vi khác nhau của Việt Nam (API và Website). Hệ thống này phải tuân thủ nghiêm ngặt các quy tắc:
- Chịu lỗi tốt (tự động thử lại khi rớt mạng, tự động dùng dữ liệu dự phòng khi server nguồn sập).
- Tôn trọng giới hạn máy chủ (quay vòng User-Agent, nghỉ ngẫu nhiên để tránh bị block, gộp tọa độ để giảm số lần gọi).
- Kết nối thông suốt với DB: Lưu file chuẩn Parquet vào tệp cấu trúc Date-Partition trên MinIO (Bronze Layer) và ghi nhật ký tự động vào PostgreSQL.

---

## 📂 Danh sách các File đã tạo & Vai trò chi tiết

Dưới đây là sơ đồ cấu trúc các tệp tin đã được xây dựng và triển khai trong Sprint 1 kèm giải thích chức năng từng file:

### 1. Cấu hình Trung tâm
- **`ingestion/config.py`**
  - **Vai trò**: Là bộ não lưu trữ toàn bộ các thông số cố định để tránh tình trạng "hardcode". Chứa danh sách API Endpoints, CSS Selectors (để cào web), danh sách tọa độ 63 tỉnh thành, 12 điểm sông trọng yếu, và cấu hình thông số chuẩn (thời gian retry, số lần chờ). Nếu nguồn dữ liệu thay đổi giao diện, anh chỉ cần sửa CSS Selector ở file này là xong.

### 2. Bộ Extractors (Kéo API)
- **`ingestion/extractors/base.py`**
  - **Vai trò**: Lớp trừu tượng (`BaseExtractor`) khai báo sẵn sức mạnh "chống chịu lỗi". Hỗ trợ tự động Exponential Backoff (chờ lâu dần sau mỗi lần lấy lỗi), lưu cache xuống local disk để khi rớt API vẫn có sẵn dữ liệu cũ trả về cho Pipeline không bị gãy.
- **`ingestion/extractors/air_quality.py`**
  - **Vai trò**: Máy bơm dữ liệu Chất lượng không khí từ OpenAQ v3 API, quét tự động tất cả trạm đo ở Việt Nam (PM2.5, PM10, v.v).
- **`ingestion/extractors/weather.py`**
  - **Vai trò**: Máy bơm Thời tiết (Nhiệt độ, mưa, gió) từ Open-Meteo. Được tối ưu gom nhóm 15 tỉnh/lần gọi để tiết kiệm tài nguyên.
- **`ingestion/extractors/flood.py`**
  - **Vai trò**: Máy bơm Lưu lượng xả 12 điểm sông từ Open-Meteo Flood API (Dự báo 7 ngày). 
- **`ingestion/extractors/fire_hotspot.py`**
  - **Vai trò**: Quét dữ liệu cháy rừng/điểm nóng từ vệ tinh NASA FIRMS EarthData bằng token cấp phép.
- **`ingestion/extractors/geo_features.py`**
  - **Vai trò**: Quét API OpenStreetMap (OSM Overpass) tìm vị trí Bệnh viện, Trạm cứu hỏa trong khung hình chữ nhật địa lý (Bounding Box) của Việt Nam.

### 3. Bộ Crawlers (Cào Website Chính Phủ)
- **`ingestion/crawlers/base_crawler.py`**
  - **Vai trò**: Lớp trừu tượng (`BaseCrawler`) chuyên cho Scraping. Tự động đổi Browser (User-Agent), chống block qua việc Sleep nghỉ ngẫu nhiên, hỗ trợ cơ chế song song 2 lớp Cache (Redis và JSON file disk).
- **`ingestion/crawlers/cem_aqi_crawler.py`**
  - **Vai trò**: Kéo bảng dữ liệu AQI (Chất lượng không khí) trực tiếp bằng mã HTML quét từ Tổng cục Môi trường `cem.gov.vn`. Tự phát hiện và "lấy nội dung thông minh" ngay cả khi Cục Môi trường đổi thuộc tính css của bảng.
- **`ingestion/crawlers/nchmf_disaster_crawler.py`**
  - **Vai trò**: Cào liên tục các bản tin cảnh báo thảm họa/thời tiết nguy hiểm mới nhất ở trang chủ của `nchmf.gov.vn`. 

### 4. Bộ Loaders (Nạp Dữ liệu xuống Data Lake)
- **`ingestion/loaders/base_loader.py`** / **`__init__.py`**
  - **Vai trò**: Khai báo Sub-package và Abstract interface cho Loader.
- **`ingestion/loaders/minio_loader.py`**
  - **Vai trò**: Súng bắn Data. Ép sạch dữ liệu vừa cào sang tịnh dạng `.parquet`, sau đó phân thư mục giống hệt Hive Hadoop (`/year=2026/month=04/day=14/...`) và đẩy thẳng lên MinIO S3 (Bronze Bucket).
- **`ingestion/loaders/postgres_loader.py`**
  - **Vai trò**: Hệ thống viết sổ nhật ký. Ghi nhận ai chạy, lúc mấy giờ, rút được bao nhiêu dòng, kết quả Thành Công hay Thất Bại vào DB PostgreSQL với bộ thư viện siêu nhẹ `psycopg2`.

### 5. Bộ Vi xử lý trung tâm (Orchestrator) 
- **`ingestion/pipeline.py`**
  - **Vai trò**: Tổng chỉ huy kết nối tất cả các tay sai phía trên. Khởi động vòng lặp kích hoạt 7 con Crawler/Extractor -> Đưa Data cho MinIO Loader đổ xuống Hồ MinIO -> Báo cáo về Postgres Loader.
  - Hỗ trợ chạy bằng lệnh CLI với cờ `--mode daily` (Hằng ngày) và `--mode seed` (Vét lịch sử).

### 6. Validation & Testing
- **`tests/unit/test_crawlers.py`** và **`test_loaders.py`**
  - **Vai trò**: Giả lập môi trường rớt mạng, lỗi DB, mất kết nối Minio. Đảm bảo toàn bộ những Logic chống lỗi của `BaseCrawler`, `BaseExtractor` chạy 100% tỷ lệ an toàn.

### 7. File Cập nhật chung
- **`requirements.txt`**
  - **Vai trò**: Bổ sung thêm các thư viện xử lý cào Web (`beautifulsoup4`, `lxml`) và xử lý test code tự động (`pytest`, `pytest-mock`).

---

🔹 **Tình Trạng:** Hoàn Tất 100%. Code đã được đẩy đủ lên nhánh `main`. Dự án UrbanPulse VN chính thức kết thúc Sprint 1 và sẵn sàng vận hành công tác rút ruột Ingestion chạy 24/7.
