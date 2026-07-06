# Giải Thích Code Chi Tiết (Code Walkthrough)

Tài liệu này giải thích chi tiết logic, vai trò và ý nghĩa từng nhóm dòng code trong hai file nòng cốt của hệ thống Integration: `DataLakeAdapter.cs` và `Consumer.cs`.

---

## 1. `DataLakeAdapter.cs`
**Vai trò:** Đây là "Cỗ máy chuyển đổi". Nó nhận sự kiện báo có file mới từ Event Grid (xuyên qua APIM), tải file Excel gốc từ thư mục `pre-raw` trên Storage Account xuống, đọc và chuyển đổi dữ liệu sang dạng `.csv` lưu vào thư mục `staging`, sao chép bản gốc sang `archive`, và cuối cùng dọn dẹp file cũ.

### Group 1: Khởi tạo và Injection (Dòng 1-19)
- Gồm các thư viện quan trọng: `Azure.Storage.Blobs` (xử lý file), `ClosedXML.Excel` (đọc file Excel).
- `public DataLakeAdapter(ILogger logger)`: Inject `ILogger` để Function có thể in log ra console hoặc Application Insights.

### Group 2: Khai báo Trigger & Bắt tay Event Grid (Dòng 21-48)
- `[HttpTrigger(AuthorizationLevel.Function, "post", Route = "datalakeadapter")]`: Đánh dấu API này bắt buộc phải có Function Key thì mới gọi được, nhận method POST qua đường dẫn `/datalakeadapter`.
- `TryGetSubscriptionValidationResponse(...)`: Đây là bước **"Bắt tay" (Handshake)** của Event Grid. Lần đầu tiên bạn dán URL vào Event Grid Subscription, nó sẽ bắn thử 1 cục data có type là `SubscriptionValidationEvent`. Đoạn code này phát hiện ra điều đó và trả về chữ ký `validationCode` bằng HTTP 200 OK để báo "Tôi là hàng real, cứ gửi event cho tôi".

### Group 3: Định vị File trên Storage (Dòng 50-82)
- Lấy `blobUrl` từ payload của Event Grid.
- Đọc biến môi trường `AzureStorageDataPull` hoặc `CUSTOMCONNSTR_AzureStorageDataPull` để lấy Connection String tới Azure Storage.
- Cắt chuỗi `blobUrl` ra để xác định xem file nằm ở Container nào, tên file là gì, để khởi tạo `sourceBlobClient`. Nếu lỗi ở đây thường do thiếu Connection String.

### Group 4: Tải file vào RAM & Convert (Dòng 84-114)
- `sourceBlobClient.DownloadToAsync(sourceStream)`: Tải toàn bộ nội dung file Excel từ đám mây xuống bộ nhớ RAM (MemoryStream) của máy chủ Function.
- `extension == ".csv" ? ... : ConvertExcelToCsv(sourceStream)`: Nếu lỡ là CSV thì đọc luôn, nếu là `.xlsx` thì đưa vào hàm convert.
  - **Hàm ConvertExcelToCsv (Dòng 165-180):** Dùng thư viện `ClosedXML` mở file, tìm đến phần vùng có chứa dữ liệu (`RangeUsed`). Duyệt qua từng dòng, duyệt từng ô.
  - **Hàm EscapeCsvField (Dòng 182-188):** Xử lý đặc biệt: nếu nội dung một ô Excel có dấu phẩy `,` hoặc dấu nháy kép `"`, nó sẽ tự động bọc trong ngoặc kép theo chuẩn quốc tế của định dạng CSV để không bị vỡ cột. Nối bằng `StringBuilder`.

### Group 5: Phân phối và Dọn dẹp (Dòng 116-138)
- Tính toán tên file mới có đuôi `.csv`.
- `WriteToStagingAsync`: Tạo/mở container `staging`, đổ chuỗi CSV dạng Byte Array lên file mới.
- `ArchiveOriginalFileAsync`: Tạo/mở container `archive`, ghi lại cái luồng file `.xlsx` gốc lên đó coi như là cất vào kho.
- `sourceBlobClient.DeleteIfExistsAsync()`: Lệnh dọn dẹp, **xóa sổ** file `.xlsx` ở bên `pre-raw`. 
- Trả về HTTP 200 báo xử lý thành công.

---

## 2. `Consumer.cs`
**Vai trò:** Đọc message từ Service Bus Queue, lấy ra câu truy vấn SQL, kết nối vào kho dữ liệu Microsoft Fabric (bằng chứng chỉ Azure AD), chạy truy vấn, đóng gói dữ liệu thành chuẩn JSON rồi soạn email gửi cho người dùng thông qua SendGrid.

### Group 1: Setup kết nối và ServiceBus Trigger (Dòng 15-34)
- Khai báo 2 biến để sẵn sàng: `_lakehouseConnectionString` (đường dẫn tới Fabric SQL Endpoint) và `_tenantId` (Id của công ty trên Azure). Lấy thẳng từ App Settings.
- `[ServiceBusTrigger("nrn-academy-sbq-query-son", Connection = "ServiceBusConnection")]`: Điểm nhấn của hàm! Cấu hình báo cho Function biết: "Mày không phải API web, mày phải kết nối vào cái Queue kia. Cứ có message nào rớt vào thì tự động bị đánh thức và lôi message đó ra xử lý (đối tượng `message`)".

### Group 2: Bóc tách Message Queue (Dòng 35-69)
- Lấy `message.Body` biến thành chuỗi JSON.
- Phân tích cú pháp (`JsonDocument.Parse`) để bóc ra đúng 3 thông tin then chốt: `query`, `recipientEmail` (email người nhận), và `requestId`. 

### Group 3: Kết nối Fabric & Chạy Query (Dòng 71-80 & 117-190)
Đoạn này xảy ra trong hàm `QueryGoldLayerAsync`:
- `builder.Remove("Authentication")` ...: Do chuỗi kết nối Fabric thường không hỗ trợ User/Pass truyền thống, ta bóc các khóa đó ra.
- **Dòng cực kỳ quan trọng (`builder.ConnectTimeout = 120`):** Do Serverless SQL của Fabric thỉnh thoảng hay bị "tắt ngủ đông" để tiết kiệm điện, khi bị gọi lần đầu nó mất khoảng 30-40s để khởi động lên. Chỉnh lên 120s giúp Function không bị crash vì timeout.
- `ClientSecretCredential(...)`: Dùng cơ chế **Service Principal (App Registration)** của Azure AD để xin Token truy cập vào data.
- Gắn `connection.AccessToken = tokenResult.Token`, mở kết nối, gọi `ExecuteReaderAsync()`.
- Vòng lặp lấy `reader.GetName(i)` để gom tên cột, sau đó đọc từng hàng nhét vào list kiểu `Dictionary` (Map key-value) để lát nữa xuất JSON có tên biến rõ ràng.

### Group 4: Xây dựng Payload và Gửi Email (Dòng 81-98 & 192-258)
- Gộp requestId, query gốc, số lượng rowCount và data trả về vào một `resultPayload` duy nhất.
- Đẩy xuống hàm `SendEmailViaSendGridAsync`:
  - Đọc API Key của tài khoản SendGrid từ cấu hình.
  - Ép `resultPayload` thành chuỗi JSON định dạng đẹp đẽ (`WriteIndented = true`).
  - Lắp ráp JSON đó vào giữa một cục `htmlContent` được design bằng code (thẻ `<pre>` nền xám viền xanh chèn chữ HTML).
  - Dùng `SendGridClient` gọi API gửi mail đi.
- Gửi xong chạy `await messageActions.CompleteMessageAsync(message)`. Việc này báo cho Service Bus: "Tích V xanh, xóa vĩnh viễn message này khỏi Queue nhé!".

### Group 5: Xử lý ngoại lệ (Dead-letter) (Dòng 99-114)
- Được bọc trong block `try...catch`. Nếu có rủi ro xảy ra (sai câu query, rớt mạng SendGrid, token hết hạn...). Code nhảy vào `catch`.
- `messageActions.DeadLetterMessageAsync(...)`: Bắn message lỗi vào Dead-letter Queue (tạm gọi là Khu cách ly). Đây là Best Practice siêu kinh điển. Nếu bạn không gọi hàm này, Function bị lỗi xong nó sẽ nhả message lại vào hàng đợi, rồi lúc sau lại chạy lại, lại lỗi, lại nhả -> Tạo thành vòng lặp vô tận làm tốn tiền server. Dead-letter chặn đứng việc đó. Mở ra cơ hội cho dân Dev vào xem chi tiết lỗi và có thể Replay (chạy lại) sau khi đã sửa bug.
