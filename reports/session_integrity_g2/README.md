# GĐ2 – Session Integrity / Gold V2

Báo cáo này độc lập với `reports/root_cause_analysis/` của GĐ1. Không chỉnh sửa
Bronze, Silver hoặc Gold V1; không publish Gold V2 lên bucket chính thức.

Gold V2 giữ nguyên 11 trường Gold V1 và thêm `session_key` nullable ở cuối schema.
Key được tạo trong Gold writer từ JSON array UTF-8 của
`[source_dataset, user_id, session_id]`, SHA-256 và prefix `nutee-session-v1:`.
ID không được trim hoặc đổi chữ hoa/thường trong hàm tạo key; `None` và chuỗi
rỗng tạo key null. Cơ chế tạo `event_id` không đổi.

Object key V2:

```text
gold/v2/<dataset>/<source_version>/events.parquet
```

`minio_integration.json` ghi kết quả đối chiếu V1/V2, kiểm tra mapping/collision
trên toàn snapshot và inventory MinIO chính thức trước/sau test. Dữ liệu test
được giữ lại trong bucket riêng; script không xóa bucket hoặc object. Mỗi lần
chạy cần một bucket test rỗng, khác bucket chính thức.

Lệnh unit/regression test:

```powershell
python -m unittest discover -s test
```

Lệnh integration test MinIO cần bucket test riêng và source versions của snapshot:

```powershell
python -B -m test.common.verify_gold_v2_minio `
  --test-bucket <empty-test-bucket> `
  --rees46-source-version <rees46-source-sha256> `
  --merrec-source-version <merrec-source-sha256>
```

MerRec Silver hiện trim/cast ID trong adapter. GĐ2 không thay đổi bước này; key
phản ánh chính xác `user_id` và `session_id` đang có tại Gold.
