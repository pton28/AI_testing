# Root Cause Analysis — REES46 Electronics

## Snapshot và phương pháp

- Bronze: `bronze/rees46_electronics/cbcbedc28c39a6b2add493bbbd9f71c061ad9d84087b85e64c442e4d47f418e7/electronics-events.csv.gz`
- Silver: `silver/rees46_electronics/rees46_events/cbcbedc28c39a6b2add493bbbd9f71c061ad9d84087b85e64c442e4d47f418e7/events.parquet`
- Gold: `gold/rees46_electronics/cbcbedc28c39a6b2add493bbbd9f71c061ad9d84087b85e64c442e4d47f418e7/events.parquet`
- Bronze SHA-256 khớp source version trong key. Audit đọc toàn bộ 885.129 rows; hiện có một object cho mỗi tầng.
- Lineage sử dụng multiset exact signature `(event_time UTC, event_type, user_id, product_id, session_id)`. Kết quả: 885.129 Bronze rows được map; Bronze→Silver và Silver→Gold đều match 885.129, không có unmatched row. Signature bị lặp được xác minh theo multiplicity, không theo row number.
- Evidence records theo issue nằm trong [evidence.json](evidence.json); full profile/schema/counts trong [profiling.json](profiling.json). Một evidence group có tối đa năm đại diện, không phải toàn bộ offending rows.

## Khả năng huấn luyện trên snapshot

| Metric | Gold snapshot |
|---|---:|
| Events | 885.129 |
| Users / products | 407.283 / 53.453 |
| Raw session IDs / composite `(user_id, session_id)` sessions | 490.398 / 490.633 |
| User-item pairs | 556.690 |
| User-item matrix sparsity | 99,997443% |
| Products có đúng một event | 14.738 (27,5719%) |
| Sessions ≥2 events (raw IDs / composite) | 140.731 / 140.688 |
| Users ≥2 distinct products | 67.259 |
| Session length median / p90 / max (raw IDs) | 1 / 3 / 572 |
| Session length median / p90 / max (composite) | 1 / 3 / 572 |
| Event types | product_view 793.748; add_to_cart 54.035; purchase 37.346 |

ALS bị ảnh hưởng bởi sparsity cao. Sequential/session-based có tập sessions nhiều event, nhưng shared session IDs và tied timestamps cần được xử lý rõ ràng. Semantic features có brand/category ở Silver, nhưng Gold contract không giữ metadata.

## Findings

| Issue | Snapshot evidence | Root cause, classification, confidence | Impact / proposed fix |
|---|---|---|---|
| **01 — session_shared_by_users (P0)** | Bronze/Silver/Gold đều có 214 IDs được dùng bởi 437 users, ảnh hưởng 1.527 events. Mẫu nằm trong [evidence.json](evidence.json), group `session_shared_*`. | Đã tồn tại ở Bronze; [Rees46Events.map_event, dòng 15](../../../transformer/datasets/rees46_electronics/silver.py#L15) copy `user_session` thành `session_id` (dòng 40), Gold copy ID ([NormalizedGoldEvents.map_event, dòng 37](../../../transformer/common/gold.py#L37)). **SOURCE_DATA_ISSUE, DESIGN_DECISION — HIGH.** | Nếu group theo session ID đơn, trộn hành vi users. Dùng analytical key `(user_id, source_session_id)` và giữ nguyên source ID phục vụ lineage. |
| **02 — same_session_timestamp (P1)** | 3.327 `(session,timestamp)` groups, 8.007 events; 3.294 same-user groups. Trong các tied groups: 34 có nhiều users, 2.557 có nhiều products, 148 có nhiều event types (các nhóm có thể overlap). Cả ba tầng giống nhau. Bronze CSV không có fractional digits (885.129/885.129). Mẫu trong [evidence.json](evidence.json), group `same_session_timestamp_*`. | Collision vốn có ở source; [parse_timestamp, dòng 62](../../../transformer/common/silver.py#L62) dùng `datetime.fromisoformat`; Silver schema là timestamp microsecond UTC (dòng 10), không tạo event order. **SOURCE_DATA_ISSUE, DATASET_LIMITATION — HIGH cho ties; UNVERIFIED về thứ tự mong muốn.** | Không sắp thứ tự tùy tiện theo row number. Giữ ties; chỉ bổ sung ordering nếu có trường nguồn với semantics được tài liệu hóa. |
| **03 — duplicate_interaction_content (P1)** | 629 mapped interaction signature groups, 655 rows sau bản đầu tiên. Toàn Bronze có cùng mức lặp; các duplicate signatures giữ nguyên qua Silver/Gold. Mẫu trong [evidence.json](evidence.json), groups `duplicate_interaction_*` và `duplicate_full_source_content`. | Repeated content ở source; [stable_event_id, dòng 51](../../../transformer/common/gold.py#L51) tạo event ID theo source version và row index, không dùng nội dung để deduplicate. **SOURCE_DATA_ISSUE / DESIGN_DECISION; ý định lặp UNVERIFIED.** | Không deduplicate tự động. Theo dõi exact full-source duplicates riêng với repeated interactions; cần phân tích domain trước khi định nghĩa policy. |
| **04 — missing_position_in_session (P1)** | Không có explicit position ở 885.129 Bronze/Silver rows; 885.129 Gold `position_in_session` null. 5 mẫu Bronze→Gold trong [evidence.json](evidence.json), groups `missing_position_*`. | Source schema không có event index; Silver không tạo position. [NormalizedGoldEvents.map_event, dòng 45](../../../transformer/common/gold.py#L45) dùng `row.get("position_in_session")` nên để null. **DATASET_LIMITATION, DESIGN_DECISION — HIGH.** | Không tái tạo thứ tự từ file order. Null là trạng thái trung thực; thêm position chỉ khi nguồn đáng tin cậy. |
| **05 — missing_session (P1)** | Bronze có 165 empty `user_session`; Silver/Gold có 165 null session, không row bị loại. Mẫu trong [evidence.json](evidence.json), group `missing_session`. | [Rees46Events.map_event, dòng 40](../../../transformer/datasets/rees46_electronics/silver.py#L40) đổi empty string thành `None`; đây là chuẩn hóa biểu diễn, không phải mất thông tin event. **SOURCE_DATA_ISSUE, DESIGN_DECISION — HIGH.** | Giữ null; không đưa vào session aggregates, không tự sinh session ID khi chưa có sessionization rule. |
| **06 — excluded_bronze_events (P1)** | Bronze event counts: `view` 793.748, `cart` 54.035, `purchase` 37.346. `remove_from_cart` không xuất hiện; excluded rows = 0. | [Rees46Events.map_event, dòng 15](../../../transformer/datasets/rees46_electronics/silver.py#L15) ánh xạ ba loại và có nhánh loại `remove_from_cart` (dòng 17). **DESIGN_DECISION — HIGH.** | Snapshot không chịu row loss. Nếu event xuất hiện ở nguồn sau, giữ Bronze và ghi exclusion count; chỉ thêm normalized type khi thống nhất semantics. |
| **07 — purchase_imbalance (P2)** | 37.346/885.129 Gold events là purchase (4,2193%). Bronze và Silver cũng có 37.346. | Là event prevalence trong snapshot; không có bằng chứng purchase bị mất tại Silver/Gold. **SOURCE_DATA_ISSUE, DATASET_LIMITATION — HIGH.** | Không gọi tỷ lệ này là conversion rate. Conversion cần denominator eligible users/sessions, exposure và attribution window. |
| **08 — products_with_one_event (P2)** | 14.738 sản phẩm (27,5719%) có một event; 556.690 user-item pairs; sparsity 99,997443%. 140.731 sessions ≥2 events; 67.259 users ≥2 products. | Long-tail/sparse observed matrix, metrics đồng nhất ở mapped pipeline. **DATASET_LIMITATION — HIGH cho snapshot.** | Báo cáo scope; quyết định K-core/train-test trong protocol benchmark, không làm trong RCA. |
| **09 — missing_brand (P2)** | Bronze/Silver thiếu 212.364 (23,9924%); 0 row thiếu có thể recover từ một row khác cùng product ID. Gold không có field. Mẫu trong [evidence.json](evidence.json), group `missing_brand`. | Giá trị trống ở Bronze (`brand=""`) trở thành null ở Silver; multiset so metadata không có thay đổi. [Rees46Events.map_event, dòng 35](../../../transformer/datasets/rees46_electronics/silver.py#L35); Gold schema 11 fields ([GOLD_SCHEMA, dòng 16](../../../transformer/common/gold.py#L16)) cố ý bỏ metadata. **SOURCE_DATA_ISSUE, DESIGN_DECISION — HIGH.** | Dùng Silver/catalog có nguồn tin cậy; không suy đoán brand. |
| **10 — missing_category_id (P2)** | Bronze/Silver: 0/885.129 thiếu, 718 distinct IDs. Gold không có field. | Không phải missing-data issue trong snapshot. ID ánh xạ một-một tới category code đang quan sát được. **SOURCE_DATA_ISSUE không áp dụng; Gold omission là DESIGN_DECISION — HIGH.** | Không cần sửa dữ liệu; nếu Gold consumer cần metadata, dùng product dimension có version. |
| **11 — missing_category_code (P2)** | Bronze/Silver thiếu 236.219 (26,6875%); 0 row thiếu recover được cùng product. Không có category ID nào map nhiều code; 58 codes dùng bởi nhiều IDs. Gold không có field. Mẫu trong [evidence.json](evidence.json), group `missing_category_code`. | [Rees46Events.map_event, dòng 41](../../../transformer/datasets/rees46_electronics/silver.py#L41) giữ category ID/code; thiếu ở source, Silver giữ nguyên. Một category name/code có thể không uniquely identify ID. **SOURCE_DATA_ISSUE, DESIGN_DECISION — HIGH.** | Không suy ra ID chỉ từ code; enrich từ canonical catalog nếu có. |
| **12 — MinIO versioning (P2)** | Bucket versioning status `null`; current Bronze/Silver/Gold keys có `version_id=null`; không thấy versions lịch sử trong prefix tại thời điểm audit. | [MinioLoader.load, dòng 22](../../../loader/common/minio_store.py#L22) gọi `fput_object` vào deterministic key. **STORAGE_CONFIGURATION, DESIGN_DECISION — HIGH cho trạng thái quan sát; không thử overwrite.** | Cân nhắc versioning hoặc immutable run prefixes và lưu source/config/code manifest sau khi được duyệt. |

## Metadata và phân bố category

- Brand: 212.364/885.129 missing (23,9924%).
- Category ID: 0 missing; 718 distinct values.
- Category code: 236.219/885.129 missing (26,6875%); 107 distinct values.
- Không có category ID nào nối với nhiều category codes; 58 category codes nối với nhiều IDs. Không thể kết luận đây là lỗi taxonomy nếu thiếu catalog chuẩn bên ngoài.
- Silver multiset so Bronze khớp toàn bộ 885.129 mapped events theo signature cùng `brand`, `category_id`, `category_code`, `price`. Gold schema không chứa `brand`, `category_id`, `category_code` hoặc `price`.

## Ảnh hưởng mô hình và giới hạn

- **ALS:** 99,997443% sparsity và long tail làm giảm lượng quan sát trên nhiều item; chưa chạy filtering/training.
- **Sequential/session-based:** 140.731 sessions có ≥2 events; 214 shared session IDs cần user scoping. Source timestamp chỉ ở độ chính xác giây và không có explicit position, do đó thứ tự trong tie không xác minh được.
- **Semantic:** Silver có brand/category nhưng metadata thiếu ở source; Gold bỏ các cột này. Category code không phải khóa thay thế cho category ID.
- Duplicate interactions không được kết luận là ingestion bug: nội dung giống nhau không cho biết đó là hành vi lặp hợp lệ hay duplicate bất thường.
- Không có unmatched mapped rows trong signature multiset. Khi signature trùng, exact record identity giữa tầng không thể phân biệt; confidence của row-level match khi đó là MEDIUM.
