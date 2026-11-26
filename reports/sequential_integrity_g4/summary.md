# GĐ4 – Sequential Integrity

**Status: TESTED — chờ người dùng nghiệm thu.** Không publish Gold chính thức, commit hoặc push.

## Thay đổi

- [transformer/common/gold.py](../../transformer/common/gold.py): thêm Gold V3 (13 trường) và logic sắp xếp chung theo `session_key`, `event_time`, `event_id`; gán `position_in_session` từ 1; đánh dấu mọi event trong timestamp tie bằng `order_ambiguous=true`.
- [pipeline.py](../../pipeline.py): chọn Gold V3 cho REES46 Electronics và MerRec; giữ mặc định helper V2 để tương thích.
- [test/common/test_gold_sequential.py](../../test/common/test_gold_sequential.py): kiểm tra thứ tự, tie, null session và tính bất biến khi đổi thứ tự input.
- [test/common/verify_gold_v3_minio.py](../../test/common/verify_gold_v3_minio.py): verifier kiểm thử MinIO độc lập, kiểm tra multiset, event IDs, ordering và inventory.
- [test/rees46_electronics/test_gold.py](../../test/rees46_electronics/test_gold.py), [test/merrec/verify_local.py](../../test/merrec/verify_local.py) và [README.md](../../README.md): cập nhật test/verifier và tài liệu cho Gold V3.

Gold V3 giữ nguyên 12 trường V2, thêm `order_ambiguous`. Với `session_key` null, position và ambiguity đều null. Không xóa duplicate interactions. `event_id` làm tie-breaker tạo deterministic ordering, không biểu thị ground truth khi timestamp trùng.

## Acceptance Criteria

| Tiêu chí | REES46 expected / actual | MerRec expected / actual | Kết quả |
|---|---:|---:|---|
| Gold events | 885,129 / 885,129 | 515,601 / 515,601 | PASS |
| Unique session keys | 490,633 / 490,633 | 112,859 / 112,859 | PASS |
| Events có position | 884,964 / 884,964 | 515,601 / 515,601 | PASS |
| Events thiếu position | 165 / 165 | 0 / 0 | PASS |
| Timestamp tie groups | 3,294 / 3,294 | 178 / 178 | PASS |
| Events đánh dấu ambiguous | 7,939 / 7,939 | 393 / 393 | PASS |
| Shared session keys | 0 / 0 | 0 / 0 | PASS |
| Events mất/thêm | 0 / 0 | 0 / 0 | PASS |
| Event IDs thay đổi | 0 / 0 | 0 / 0 | PASS |

MerRec Silver có đủ 605,269 events. Event distribution Gold không đổi:

| Dataset | product_view | add_to_cart | purchase |
|---|---:|---:|---:|
| REES46 Electronics | 793,748 | 54,035 | 37,346 |
| MerRec | 504,756 | 10,168 | 677 |

Multiset các trường Gold V2 ngoài `position_in_session` khớp hoàn toàn; event ID multiset cũng khớp. Đảo thứ tự input rows tạo cùng Gold V3 content hash cho cả hai dataset. Các kiểm tra position liên tục, timestamp không giảm, tie-break order và ambiguity đều PASS.

## Kiểm thử

- `python -m unittest test.common.test_gold_sequential test.common.test_gold_contract test.rees46_electronics.test_gold test.merrec.test_pipeline` — **PASS**, 16 tests.
- `python -m unittest discover -s test` — **PASS**, 31 tests.
- `python -m py_compile transformer/common/gold.py pipeline.py test/common/test_gold_sequential.py test/common/verify_gold_v3_minio.py test/merrec/verify_local.py test/rees46_electronics/test_gold.py` — **PASS**.
- `git diff --check` — **PASS**; Git chỉ phát cảnh báo line ending LF/CRLF.
- `python -B -m test.common.verify_gold_v3_minio --test-bucket nutee-sequential-integrity-g4-test-20261126` — **PASS** trên MinIO thật.

Gold V3 test objects được giữ lại:

- `gold/v3/rees46_electronics/cbcbedc28c39a6b2add493bbbd9f71c061ad9d84087b85e64c442e4d47f418e7/events.parquet`
- `gold/v3/merrec/3f5857b8763f54f6970c4c0c3c1b7ea4ec3c0d5643cc2b68e903b309df0a1bca/events.parquet`

## Bảo toàn dữ liệu và hạn chế

- Bucket chính thức `nutee-recsys`: inventory 16 objects; SHA-256 inventory trước/sau giống nhau (`1b766338a0e8ef8b1ea6ee4559423f7a533e3553544e201f16562ed450ce3b42`). Bucket versioning không đổi (`null`).
- Baseline GĐ2/GĐ3 cũng không đổi: GĐ2 inventory 4 objects (`c07751a52c5366038f5a0b76f1f987261c545eb95d0630ed5b19908d535838b4`); GĐ3 inventory 2 objects (`2bce9129a9ac82e860a4f064076fefe1fa9c1818fede0d474ec29c6bbbe17bcf`).
- Timestamp ties chỉ có deterministic ordering theo `event_id`; thứ tự này không khẳng định thứ tự hành vi thực tế. REES46 có 165 event thiếu session nên không có position hoặc ambiguity.
- Gold chính thức chưa được publish. GĐ4 chưa được ACCEPTED; chờ người dùng nghiệm thu.

Bằng chứng chi tiết, gồm metrics, inventory và hashes: [integration.json](./integration.json).
