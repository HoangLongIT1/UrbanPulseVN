---
description: "Workflow verification tự động cho mỗi Sprint. Chạy '/verify-sprint [sprint_number]' để bắt đầu."
---

# Verify Sprint Workflow

Khi User gọi lệnh kiểm tra Sprint (ví dụ: `/verify-sprint 0`), hãy tự động thực hiện các bước sau:

1. **Đọc mục tiêu Sprint**:
   Tự động đọc các "Acceptance Criteria" của Sprint tương ứng trong `implementation_plan.md`.

2. **Kiểm tra Infrastracture Status**:
   - Tự động chạy lệnh shell (Docker) để kiểm tra các container yêu cầu tương ứng với Sprint có trạng thái `Up` và `Healthy` không.
   - Ví dụ Sprint 0 yêu cầu: `postgres`, `minio`, `trino`, `redis`, `vault`, `nessie`. 

3. **Kiểm tra Logs & Health Endpoints**:
   - Truy xuất API hoặc Logs ngắn của các service chính để đảm bảo không có ERROR loop.
   - (VD: Kiểm tra log của Nessie hoặc gửi request tới Trino coordinator port 8090).

4. **Báo cáo**:
   - Trả về kết quả Pass / Fail dưới dạng bảng Markdown.
   - Liệt kê lỗi và gợi ý cách fix nếu có bất kỳ service nào không chạy.
