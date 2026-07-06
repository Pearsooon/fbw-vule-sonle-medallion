# Hướng Dẫn Test Toàn Diện (End-to-End Test Guide)

Tài liệu này hướng dẫn chi tiết từng bước để test toàn bộ 2 flow trong hệ thống Integration, đảm bảo không bỏ sót bất kỳ component nào theo đúng thiết kế kiến trúc.

---

## 🟢 FLOW 1: DATA INGESTION FLOW
**Mục tiêu:** Đảm bảo file `.xlsx` được upload từ SharePoint đi qua Logic App, Event Grid, phân nhánh sang Logging và APIM -> Data Lake Adapter, cuối cùng được convert sang `.csv` (staging) và backup `.xlsx` (archive).

### Bước 1: Kích hoạt luồng (Trigger)
1. Truy cập vào **SharePoint** (thư mục đã cấu hình cho Logic App).
2. Upload một file Excel mẫu (ví dụ: `sample_data.xlsx`).
3. Đợi tối đa **3 phút** (do Logic App trigger chạy định kỳ mỗi 3 phút).

### Bước 2: Kiểm tra Logic App (Ingestion)
1. Mở Azure Portal -> Truy cập vào resource **Logic App (Ingestion)**.
2. Mở tab **Run history**.
3. **Cần check:** 
   - Có một luồng chạy mới với trạng thái **Succeeded**.
   - Mở chi tiết luồng chạy:
     - Action **Get file content** thành công (đọc được file từ SharePoint).
     - Action **Create blob** thành công (ghi file vào Storage Account: container `training`, thư mục `pre-raw`).
     - Action **Publish Event Grid event** thành công.

### Bước 3: Kiểm tra Logging Adapter (Audit Trail)
1. Mở Azure Portal -> Truy cập vào **Azure Function App** chứa `LoggingAdapter`.
2. Mở **Log stream** hoặc **Application Insights** của function `LoggingAdapter`.
3. **Cần check:**
   - Function được trigger thành công từ Event Grid.
   - Có dòng log báo hiệu đã nhận event: `Audit trail: Ingestion event received. Event type: ..., Event subject: ..., Event data: ...`.

### Bước 4: Kiểm tra APIM & Data Lake Adapter (Xử lý Data)
1. Mở Azure Portal -> Truy cập vào **Azure Function App** chứa `DataLakeAdapter`.
2. Mở **Log stream** hoặc **Application Insights** của function `DataLakeAdapter`.
3. **Cần check:**
   - Bắt được log: `DataLakeAdapter received request.` (chứng tỏ APIM đã forward thành công từ Event Grid sang Function).
   - Bắt được log bóc tách payload: `EventGrid payload: type=...`
   - Bắt được log bắt đầu xử lý file: `DataLakeAdapter processing blob...`
   - Bắt được log hoàn tất: `Hoàn tất: Đã convert sample_data.xlsx → sample_data.csv, đẩy vào staging, backup ở archive và xóa file cũ.`

### Bước 5: Nghiệm thu kết quả cuối tại Azure Storage
1. Mở Azure Portal -> Truy cập vào **Storage Account** -> Container `training`.
2. **Cần check 3 thư mục:**
   - **`pre-raw/`**: File `sample_data.xlsx` ban đầu **đã biến mất** (bị xóa sau khi xử lý xong để dọn dẹp).
   - **`staging/`**: Xuất hiện file mới tên `sample_data_{timestamp}.csv`. Tải về và mở ra xem có đúng định dạng CSV chuẩn (các cột tách bằng dấu phẩy) không.
   - **`archive/`**: Xuất hiện file mới tên `sample_data_{timestamp}.xlsx`. Đây là bản backup gốc không bị chỉnh sửa.

---

## 🔵 FLOW 2: QUERY & NOTIFICATION FLOW
**Mục tiêu:** Đảm bảo request query từ bên ngoài gọi qua APIM, được đẩy vào Service Bus thông qua Logic App, sau đó Function Consumer lấy ra, chạy query trên Fabric Lakehouse SQL Endpoint và gửi kết quả qua email.

### Bước 1: Kích hoạt luồng (Gửi Request)
1. Mở **Postman** (hoặc dùng `curl`).
2. Gửi một request **POST** tới endpoint của **APIM** (Query API endpoint).
3. Payload JSON mẫu:
   ```json
   {
     "query": "SELECT TOP 10 * FROM gold.fact_sales",
     "recipientEmail": "email_cua_ban@example.com",
     "requestId": "test-req-001"
   }
   ```
4. **Cần check:** Response trả về từ APIM (thường là `202 Accepted` hoặc `200 OK` tùy cấu hình Logic App), báo hiệu hệ thống đã tiếp nhận yêu cầu thành công.

### Bước 2: Kiểm tra Publisher (Logic App)
1. Mở Azure Portal -> Truy cập vào resource **Logic App (Publisher)**.
2. Mở tab **Run history**.
3. **Cần check:**
   - Có một luồng chạy mới với trạng thái **Succeeded**.
   - Luồng này đã nhận được HTTP request từ APIM.
   - Action **Send message** (gửi payload vào Service Bus Queue) chạy thành công.

### Bước 3: Kiểm tra Service Bus Queue
1. Mở Azure Portal -> Truy cập vào **Service Bus Namespace** -> **Queues**.
2. Chọn Queue bạn đang dùng (ví dụ: `nrn-academy-sbq-query-son`).
3. **Cần check:** 
   - Tab **Metrics** hiển thị có Message đi vào (Incoming Messages).
   - Thông số **Active messages** phải = `0` (nghĩa là Function Consumer đã ngay lập tức kéo message ra để xử lý).
   - Thông số **Dead-letter messages** phải = `0`. (Nếu > 0, chứng tỏ Function Consumer chạy bị lỗi và nhả message vào thùng rác).

### Bước 4: Kiểm tra Consumer (Azure Function)
1. Mở Azure Portal -> Truy cập vào **Azure Function App** chứa `Consumer`.
2. Mở **Log stream** hoặc **Application Insights** của function `Consumer`.
3. **Cần check các log theo thứ tự:**
   - `[Consumer] Started processing Message ID: ...`
   - `[Consumer] Executing query for requestId=test-req-001: SELECT TOP 10 * ...`
   - `[Consumer] Successfully connected to Fabric Lakehouse.` (Đảm bảo kết nối AD token hoạt động tốt)
   - `[Consumer] Query returned 10 rows`
   - `[Consumer] Email sent successfully.`
   - Nếu có lỗi xảy ra, kiểm tra các dòng `[Consumer] ERROR...` (ví dụ: lỗi SQL syntax, thiếu quyền Fabric, sai email...).

### Bước 5: Nghiệm thu kết quả cuối tại Email
1. Mở hộp thư cá nhân (email bạn đã điền trong payload Postman).
2. **Cần check:**
   - Nhận được email có tiêu đề: `Fabric Lakehouse Query Results [test-req-001]` (hoặc `Query Results — nrn-academy Gold Layer [test-req-001]`).
   - Nội dung email hiển thị một khối JSON gọn gàng chứa: `requestId`, `query`, `status: "processed"`, `rowCount: 10`, và mảng `data` chứa 10 dòng kết quả được select từ database của Microsoft Fabric.
   - *(Tùy chọn: Nếu có làm thêm Push Notifications theo kiến trúc thì check log/màn hình Push Notification subscriber).*

---
**🎉 CHÚC MỪNG! Nếu tất cả các bước trên đều vượt qua, hệ thống Integration của bạn đã hoạt động hoàn hảo 100% từ End-to-End đúng như bản vẽ Architecture.**
