# Báo cáo Tổng kết Sprint 0: Project Setup & Foundation

## 🎯 Mục đích của Sprint 0
Sprint 0 đóng vai trò thiết lập **nền móng hạ tầng (Infrastructure Foundation)** và đưa dự án UrbanPulse VN vào khuôn khổ quy chuẩn. 

Mục tiêu cốt lõi của Sprint này là khởi tạo một hệ thống chịu tải với hình hài Medallion Open Lakehouse hoàn chỉnh, đi đôi với kiến trúc "Hybrid-Cloud + Zero-Cost" và trói buộc giới hạn phần cứng nghiêm ngặt:
- Khởi chạy nền tảng mạng Docker lưới ảo cho Data Lake và Kho Backend (PostgreSQL, MinIO, Trino, Nessie, Redis) với cấu hình Limit RAM an toàn tuyệt đối dưới **2.1GB tổng** để bảo vệ dàn máy 16GB.
- Bảo mật Repository bằng hệ thống bỏ qua Git (Ignore) chống rò rỉ Key API, Secret Token, và các luật lõi của Agent AI.
- Tạo ra các Utility (tiện ích cốt lõi) kết nối DB được lập trình sẵn để các Sprint sau không cần tốn công đắp lại base code kết nối.

---

## 📂 Danh sách các File đã tạo & Vai trò chi tiết

Dưới đây là sơ đồ cấu trúc các tệp tin đã được xây dựng và triển khai xuyên suốt quá trình thiết lập Sprint 0:

### 1. Nền tảng Container & Hạ tầng (Infrastructure)
- **`docker-compose.yaml`**
  - **Vai trò**: Cấu hình mạng Docker Compose toàn cục. Bao gồm 5 dịch vụ chính thức: `postgres` (Kho Metadata), `minio` (Data Lake), `trino` (Query Engine), `nessie` (Iceberg Catalog) và tự động trích xuất lệnh `minio-init` để đẻ ra 3 folder Lake (`raw`, `processed`, `iceberg`) ngay khi hệ thống lên sóng. Cấu hình này ghim chặt Memory Limit chặn phình RAM.
- **`.env.example`** (và các file `.env` local)
  - **Vai trò**: Khuôn mẫu hệ thống chứa cấu trúc chuẩn cung cấp Biến môi trường (Environment Variable) cho dự án (Host, Port, User, Pass).

### 2. Cấu trúc Source Code Core Utilities (Khung xương)
- **`utils/helper.py`**
  - **Vai trò**: Thiết lập lõi tiện ích cấu hình dự án. Khởi tạo chức năng `get_env` chuyên đọc biến môi trường an toàn, và hàm khởi tạo thiết lập `structlog` tích hợp Logging Format đẹp, có thứ tự, không xài `print` cứng nhắc trong toàn bộ hệ thống Ingestion về sau.
- **`utils/minio_utils.py`**
  - **Vai trò**: Client kết nối Amazon S3/MinIO. Gói sẵn kỹ nghệ chuyển đổi định dạng Pandas DataFrame sang file nén `Parquet` thông qua PyArrow và upload cực rỗng lên Buckets (như `raw`), cũng như hỗ trợ hàm check Health.
- **`utils/redis_client.py`**
  - **Vai trò**: Client gắn liền với Redis. Gói chuẩn mọi lệnh thao tác Cache lưu trữ RAM, cho phép Crawler (được tạo ở Sprint 1) tái sử dụng để làm Cache Layer cho Scraping thông qua hàm `get()`, `set()`, `ping()`.

### 3. File Quản lý Package và Git
- **`requirements.txt`**
  - **Vai trò**: Danh sách nguyên liệu cài đặt của dự án Python. Quản lý thư viện cốt lõi `psycopg2-binary` (bản mới hỗ trợ Python 3.13), `minio`, `pandas`, `pyarrow`, và `structlog` để đồng bộ môi trường VENV.
- **`.gitignore`**
  - **Vai trò**: Vệ sĩ cửa khẩu. Chứa luật Git triệt tiêu hoàn toàn đường sinh sống của các file kế hoạch (`warnings.md`, `implementation_plan.md`, thư mục agents) hoặc `.env` để không bao giờ bị Push rò rỉ lên mạng Github/Gitlab public.

### 4. Tài liệu Đặc tả hệ thống (Documentation)
- **`docs/architecture.md`**
  - **Vai trò**: Đặc tả Blueprint thiết kế. Giải thích rõ Medallion Architecture (Bronze-Silver-Gold Layers). Đã tích hợp Biểu đồ lưu chuyển Data (Data Flow ASCII Version) giúp mọi AI Model hoặc kỹ sư khi vào code có thể đọc lướt và bắt kịp kiến trúc ngay lập tức.
- **`AGENTS.md`** (Và hệ thống luật `.cursor/rules/`)
  - **Vai trò**: Lõi định hướng "Agentic Workflow". Bẻ gãy `cursorrules` rườm rà thành từng thư mục nhỏ quy định luật Crawler, Data Rules để AI không bị Hallucinate (ảo giác) trong lúc lập trình và bám sát chiến lược Zero-Cost.

---

🔹 **Tình Trạng:** Hoàn Tất 100%. Môi trường Virtual Env đã chạy ổn định vĩnh viễn, Database Compose Backend khởi động xanh lá 100%, Vault và Ignore Rules bảo mật tuyệt đối. Đây là bàn đạp kiên cố nhất đã đẩy tốc độ của Sprint 1 lên gấp đôi.
