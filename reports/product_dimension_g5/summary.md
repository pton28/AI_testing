# GĐ5 – Product Dimension

**Status: TESTED — chờ người dùng nghiệm thu.** Outputs chỉ được ghi vào bucket kiểm thử; không publish chính thức, commit hoặc push.

## Files và logic

- [transformer/common/product_dimension.py](../../transformer/common/product_dimension.py): logic dùng chung, giữ một hàng cho mỗi `product_id`, chọn giá trị hợp lệ mới nhất riêng cho từng thuộc tính, thống kê missing/conflicts, kiểm tra traceability, ghi Parquet Snappy và xác thực readback.
- [transformer/registry.py](../../transformer/registry.py): khai báo mapping tên sản phẩm; MerRec dùng Silver `name`, REES46 không có cột tên.
- [pipeline.py](../../pipeline.py): thêm `build_product_dimension` và CLI `--product-dimension-only`; không dựng lại hay ghi `events.parquet`.
- [test/common/test_product_dimension.py](../../test/common/test_product_dimension.py): unit tests cho latest non-null, null/blank, conflict, timestamp tie, provenance, rerun và CLI.
- [test/common/verify_product_dimension_minio.py](../../test/common/verify_product_dimension_minio.py): integration verifier MinIO cho count, set IDs, coverage, traceability, deterministic rerun và baseline inventory.
- [README.md](../../README.md): hướng dẫn schema, object key và cách chạy.

Tie-breaker khi timestamp trùng: chọn giá trị metadata theo thứ tự từ điển tăng dần. Đây là quy tắc deterministic, không phải suy luận về tính đúng đắn ngữ nghĩa. Mỗi metadata value có trường `*_observed_at` riêng để truy vết về Silver. Price giữ nguyên decimal (18,2); không quy đổi tiền tệ.

## Schema

Product Dimension gồm `source_dataset` và `product_id` (string, non-null); `product_name`, `brand`, `category_id`, `category_code` (nullable string); `price` (nullable decimal(18,2)); và timestamp nullable `product_name_observed_at`, `brand_observed_at`, `category_id_observed_at`, `category_code_observed_at`, `price_observed_at`.

## Data quality

Missing rate được tính trên số product trong dimension sau khi chọn metadata mới nhất hợp lệ.

| Dataset | Products | Product name missing | Brand missing | Category ID missing | Category code missing | Price missing | Products có metadata conflict |
|---|---:|---:|---:|---:|---:|---:|---:|
| REES46 Electronics | 53,453 | 53,453 (100.00%) | 16,486 (30.84%) | 0 (0.00%) | 18,856 (35.28%) | 0 (0.00%) | 0 |
| MerRec | 431,734 | 0 (0.00%) | 85,771 (19.87%) | 28,130 (6.52%) | 28,130 (6.52%) | 0 (0.00%) | 0 |

Conflict counts theo từng thuộc tính (`product_name`, `brand`, `category_id`, `category_code`, `price`) đều là **0** trên snapshot Silver đã kiểm thử.

## Acceptance Criteria

| Tiêu chí | REES46 expected / actual | MerRec expected / actual | Kết quả |
|---|---:|---:|---|
| Silver rows | 885,129 / 885,129 | 605,269 / 605,269 | PASS |
| Dimension rows = distinct Silver `product_id` | 53,453 / 53,453 | 431,734 / 431,734 | PASS |
| Null hoặc duplicate `product_id` | 0 / 0 | 0 / 0 | PASS |
| Gold V3 distinct products (baseline) | 53,453 / 53,453 | 416,784 / 416,784 | PASS |
| Gold V3 product coverage | 100% / 100% | 100% / 100% | PASS |
| Metadata không truy vết được về Silver | 0 / 0 | 0 / 0 | PASS |
| Output deterministic khi chạy lại | Có / Có | Có / Có | PASS |
| Gold Event Log V3 và baseline buckets không đổi | Không đổi / Không đổi | Không đổi / Không đổi | PASS |

MerRec Product Dimension lớn hơn Gold V3 **14,950** products (431,734 so với 416,784), do dimension lấy toàn bộ distinct IDs từ Silver; không ép dimension bằng Gold.

## Kiểm thử và MinIO

- `python -m unittest test.common.test_product_dimension` — **PASS**, 4 tests.
- `python -m unittest discover -s test` — **PASS**, 35 tests.
- `python -m py_compile transformer/common/product_dimension.py pipeline.py transformer/registry.py test/common/test_product_dimension.py test/common/verify_product_dimension_minio.py` — **PASS**.
- `git diff --check` — **PASS**; chỉ có cảnh báo line ending LF/CRLF trên một số file làm việc sẵn có.
- `python -B -m test.common.verify_product_dimension_minio --test-bucket nutee-product-dimension-g5-test-20261126-1642` — **PASS**, MinIO thật.

Object keys được giữ trong bucket `nutee-product-dimension-g5-test-20261126-1642`:

- `gold/v3/rees46_electronics/cbcbedc28c39a6b2add493bbbd9f71c061ad9d84087b85e64c442e4d47f418e7/products.parquet`
- `gold/v3/merrec/3f5857b8763f54f6970c4c0c3c1b7ea4ec3c0d5643cc2b68e903b309df0a1bca/products.parquet`

Inventory trước/sau giống nhau: bucket chính thức `nutee-recsys` có 16 objects; G2, G3 và G4 baseline inventories cũng không đổi. Versioning của các bucket được kiểm tra vẫn `null`. G5 report chi tiết, missing rates, conflict counts, coverage và content hashes: [integration.json](./integration.json).

## Hạn chế

- REES46 Silver không có tên sản phẩm nên `product_name` null cho toàn bộ products; pipeline không tự tạo tên.
- Snapshot dùng metadata của toàn bộ Silver; chưa áp dụng training cutoff và không mặc định an toàn cho benchmark theo thời gian. Việc giới hạn theo cutoff thuộc GĐ6.
- Chưa thực hiện training hoặc train/test split. GĐ5 chưa ACCEPTED; chờ người dùng nghiệm thu.
