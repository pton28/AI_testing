# GĐ6 – Model-ready Data

**Trạng thái: TESTED — chờ người dùng nghiệm thu.** Không training, temporal split, benchmark, publish chính thức, commit hoặc push.

## Files và logic

- [transformer/common/model_ready.py](../../transformer/common/model_ready.py): tạo ALS interactions và session sequences từ Gold V3; đọc Product Dimension GĐ5 để kiểm tra product coverage. Dùng SQLite tạm để aggregate/sắp xếp streaming, giữ duplicate interactions và xuất Parquet Snappy deterministic.
- [pipeline.py](../../pipeline.py): thêm `build_model_ready` và `--model-ready-only --dataset ... --source-version ...`.
- [test/common/test_model_ready.py](../../test/common/test_model_ready.py): kiểm tra counts, repeated interactions, null session, session ordering, ambiguous flags, coverage, deterministic rerun và CLI.
- [test/common/verify_model_ready_g6_minio.py](../../test/common/verify_model_ready_g6_minio.py): kiểm tra MinIO isolation, acceptance metrics, coverage, sequence fidelity với Gold V3, deterministic rerun và baseline inventory.
- [README.md](../../README.md): thêm lệnh chạy và hướng dẫn tránh data leakage cho nhóm AI.

## Output schemas

- **ALS:** `user_id` string, `product_id` string, ba event counts và `interaction_count` int64, `last_interaction_at` nullable UTC timestamp.
- **Session sequences:** `session_key`, `user_id`, `item_sequence: list[string]`, `event_sequence: list[string]`, `timestamp_sequence: list[UTC timestamp]`, `sequence_length` int32, `has_ambiguous_order` boolean.

## Acceptance results

| Tiêu chí | REES46 expected / actual | MerRec expected / actual | Status |
|---|---:|---:|---|
| Unique user-product pairs | 556,690 / 556,690 | 429,812 / 429,812 | PASS |
| Tổng interactions | 885,129 / 885,129 | 515,601 / 515,601 | PASS |
| Duplicate pairs; null user/product | 0; 0 / 0 | 0; 0 / 0 | PASS |
| View / cart / purchase counts khớp Gold | 793,748 / 54,035 / 37,346 — khớp | 504,756 / 10,168 / 677 — khớp | PASS |
| Unique sequences | 490,633 / 490,633 | 112,859 / 112,859 | PASS |
| Tổng sequence length | 884,964 / 884,964 | 515,601 / 515,601 | PASS |
| Events bỏ khỏi sequence do null `session_key` | 165 / 165 | 0 / 0 | PASS |
| Duplicate sequences; shared session users | 0; 0 / 0 | 0; 0 / 0 | PASS |
| Sequence content/order khớp Gold V3 | Có / Có | Có / Có | PASS |
| Product coverage (ALS; sequences) | 53,453/53,453; 53,452/53,452 | 416,784/416,784; 416,784/416,784 | PASS |
| Deterministic rerun | Có / Có | Có / Có | PASS |

Session có ambiguous events: **REES46 2,424 sessions / 7,939 events; MerRec 161 sessions / 393 events.** REES46 có một product chỉ xuất hiện trong event thiếu session; vì vậy sequence chứa 53,452 products còn ALS chứa 53,453. Cả hai tập đều có 100% coverage trong Product Dimension.

## MinIO outputs và kiểm thử

Gold V3 được đọc từ `nutee-sequential-integrity-g4-test-20261126`, Product Dimension được đọc từ `nutee-product-dimension-g5-test-20261126-1642`. Outputs chỉ ghi vào bucket kiểm thử PASS `nutee-model-ready-g6-test-20261126-1703b`:

- `gold/v3/rees46_electronics/cbcbedc28c39a6b2add493bbbd9f71c061ad9d84087b85e64c442e4d47f418e7/model_ready/als_interactions.parquet`
- `gold/v3/rees46_electronics/cbcbedc28c39a6b2add493bbbd9f71c061ad9d84087b85e64c442e4d47f418e7/model_ready/session_sequences.parquet`
- `gold/v3/merrec/3f5857b8763f54f6970c4c0c3c1b7ea4ec3c0d5643cc2b68e903b309df0a1bca/model_ready/als_interactions.parquet`
- `gold/v3/merrec/3f5857b8763f54f6970c4c0c3c1b7ea4ec3c0d5643cc2b68e903b309df0a1bca/model_ready/session_sequences.parquet`

- `python -B -m unittest test.common.test_model_ready -v` — **PASS**, 4 tests.
- `python -B -m unittest discover -s test -v` — **PASS**, 39 tests.
- `python -B -m py_compile transformer/common/model_ready.py pipeline.py test/common/test_model_ready.py test/common/verify_model_ready_g6_minio.py` — **PASS**.
- `python -B .\run_pipeline.py --help` — **PASS**; CLI hiển thị `--model-ready-only`.
- `python -B -m test.common.verify_model_ready_g6_minio --test-bucket nutee-model-ready-g6-test-20261126-1703b` — **PASS**, MinIO thật.

Integration report: [integration.json](./integration.json). Inventory các bucket chính thức và baseline GĐ2–GĐ5 không đổi; bucket `nutee-recsys` giữ 16 objects, versioning không đổi (`null`).

Lần verifier đầu tiên dừng vì comparator kiểm thử chưa so sánh `sequence_length`, không phải do dữ liệu. Theo nguyên tắc không tự xóa dữ liệu kiểm thử, bucket `nutee-model-ready-g6-test-20261126-1703` được giữ nguyên; chỉ bucket `...-1703b` là kết quả PASS.

## Hạn chế

- Outputs là aggregate của toàn snapshot, không phải train/test datasets. Không dùng ALS toàn snapshot để train rồi đánh giá trên cùng dữ liệu.
- Chưa có temporal split, training cutoff cho Product Dimension, target generation hay evaluation protocol; nhóm AI cần thống nhất protocol trước benchmark.
- REES46 không có `product_name`; không tạo embeddings hoặc gọi LLM API.
