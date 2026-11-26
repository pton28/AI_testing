# Root Cause Analysis: REES46 Electronics và MerRec

## Phạm vi

Điều tra read-only snapshot hiện tại trong MinIO bucket `nutee-recsys` cho `rees46_electronics` và `merrec`, truy vết Bronze → Silver → Gold. Audit tải object về thư mục tạm để đọc; không ghi/xóa object MinIO, không sửa pipeline hay dữ liệu.

Snapshot được kiểm tra ngày 2026-11-26 UTC. Mỗi dataset có một object hiện hành cho mỗi tầng. `report.json` ghi rõ object key, kích thước, ETag, thời gian sửa đổi và trạng thái versioning. SHA-256 của Bronze khớp với source version trong object key.

## Kết quả chính

- REES46: 885.129 Bronze rows được map đầy đủ qua Silver và Gold.
- MerRec: 605.269 Bronze rows; Silver/Gold giữ 515.601 rows. 89.668 rows `item_like`, `offer_make`, `buy_start` bị loại theo mapping hiện hành.
- Signature multiset `(event_time UTC, event_type, user_id, product_id, session_id)` khớp toàn bộ các rows được map giữa các tầng. Dữ liệu lặp có thể làm nhiều rows cùng signature; không dùng row number để kết luận lineage.
- Đối chiếu Silver bổ sung các giá trị chuẩn hóa `brand`, `category_id`, `category_code`, `price`; MerRec còn so sánh toàn bộ Bronze row với Silver `source_attributes`.
- Session ID dùng chung giữa users, timestamp ties, interaction lặp, session thiếu và metadata thiếu đã tồn tại trong Bronze. Báo cáo không suy diễn tính hợp lệ của hành vi lặp hoặc thứ tự trong timestamp ties.
- Versioning bucket được quan sát là `null`; các object có `version_id=null`. `MinioLoader` ghi bằng `fput_object` vào deterministic key, nên chạy lại có thể thay object hiện hành mà không lưu lịch sử version.

## Tệp báo cáo

| Tệp | Nội dung |
|---|---|
| [summary.md](summary.md) | Kết luận tổng hợp theo 12 issue và hai dataset, severity, số liệu cùng hướng xử lý. |
| [report.json](report.json) | Findings có cấu trúc, lineage, event counts, object metadata và trạng thái versioning. |
| [audit.py](audit.py) | Audit độc lập, đọc MinIO và ghi profiling/evidence; không thực thi pipeline. |
| [rees46_electronics/profiling.json](rees46_electronics/profiling.json) | Profile toàn snapshot, schema, lineage, metadata và findings REES46. |
| [rees46_electronics/evidence.json](rees46_electronics/evidence.json) | Mẫu Bronze/Silver/Gold, object keys và phương pháp matching REES46. |
| [rees46_electronics/root_cause_analysis.md](rees46_electronics/root_cause_analysis.md) | Root cause, phân loại, độ tin cậy, ảnh hưởng và đề xuất cho REES46. |
| [merrec/profiling.json](merrec/profiling.json) | Profile toàn snapshot, schema, lineage, metadata và findings MerRec. |
| [merrec/evidence.json](merrec/evidence.json) | Mẫu Bronze/Silver/Gold, event bị loại, object keys và matching MerRec. |
| [merrec/root_cause_analysis.md](merrec/root_cause_analysis.md) | Root cause, phân loại, độ tin cậy, ảnh hưởng và đề xuất cho MerRec. |

## Tái lập audit

Từ repository root, dùng môi trường Python/dependencies của project và `.env` hiện có với quyền đọc MinIO:

```powershell
python reports\root_cause_analysis\audit.py
```

Audit yêu cầu MinIO đang chạy, bucket và các object Bronze/Silver/Gold đã có. Không in credentials vào console/report. Lệnh tạo lại `report.json`, hai `profiling.json` và hai `evidence.json`; không tạo hoặc cập nhật objects trên MinIO.

## Đọc kết quả và giới hạn

- Confidence `HIGH` áp dụng cho thống kê snapshot và match multiset; không đồng nghĩa mọi bản ghi lặp là lỗi.
- `UNVERIFIED` được dùng khi dữ liệu không xác định được ý định lặp hoặc thứ tự thực.
- MerRec Bronze hiện diện ở phạm vi một shard Parquet; không đại diện cho toàn bộ dataset MerRec.
- Gold event contract không chứa product metadata; dùng Silver/raw metadata để làm semantic features. Không suy sinh giá trị thiếu.
- Chưa chạy K-core, train/test split hoặc benchmark model.
