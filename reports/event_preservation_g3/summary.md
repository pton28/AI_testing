# GĐ3 – Event Preservation

## Status

**TESTED — chờ nghiệm thu.** MerRec Silver bảo toàn toàn bộ event của Bronze;
Gold tiếp tục chỉ xuất ba event types thuộc contract. Gold V2 chỉ được ghi vào
bucket kiểm thử, không publish lên bucket chính thức.

## Thay đổi

- `transformer/datasets/merrec/silver.py`: giữ cả `item_like`, `offer_make` và
  `buy_start` trong Silver; lưu source action trong `source_event_type` và đầy
  đủ dòng Bronze trong `source_attributes`.
- `transformer/datasets/merrec/gold.py`: loại ba source actions ngoài contract
  tại Gold, không ánh xạ chúng sang event type khác.
- `transformer/common/gold.py`: bộ đếm event ID chỉ tăng khi Gold xuất event.
  Các event Gold hợp lệ giữ nguyên ID deterministic đã có; schema V2 và
  `session_key` không đổi.

## Event distribution

| Source action | Bronze | Silver sau GĐ3 | Gold V2 sau GĐ3 |
|---|---:|---:|---:|
| `item_view` → `product_view` | 504,756 | 504,756 | 504,756 |
| `item_add_to_cart_tap` → `add_to_cart` | 10,168 | 10,168 | 10,168 |
| `buy_comp` → `purchase` | 677 | 677 | 677 |
| `item_like` | 83,571 | 83,571 | 0 |
| `offer_make` | 4,260 | 4,260 | 0 |
| `buy_start` | 1,837 | 1,837 | 0 |
| **Tổng** | **605,269** | **605,269** | **515,601** |

## Acceptance criteria

| Tiêu chí | Expected | Actual | Status |
|---|---:|---:|---|
| MerRec Bronze rows | 605,269 | 605,269 | PASS |
| MerRec Silver rows | 605,269 | 605,269 | PASS |
| Events phục hồi tại Silver | 89,668 | 89,668 | PASS |
| MerRec Gold V2 rows | 515,601 | 515,601 | PASS |
| Gold `product_view` / `add_to_cart` / `purchase` | 504,756 / 10,168 / 677 | 504,756 / 10,168 / 677 | PASS |
| Unique Gold session keys | 112,859 | 112,859 | PASS |
| Shared / null session keys | 0 / 0 | 0 / 0 | PASS |
| Gold events mất/thêm | 0 / 0 | 0 / 0 | PASS |
| Gold event IDs thay đổi theo thứ tự | 0 | 0 | PASS |
| Multiset đủ 12 trường Gold V2 | Khớp baseline GĐ2 | Khớp | PASS |
| Bronze→Silver source-attributes multiset | Khớp | Khớp | PASS |
| REES46 Gold V2 rows | 885,129 | 885,129 | PASS |

Gold V2 content SHA-256 trước và sau đều là
`15c69bd6987ef9c49bf0cb9d44354fb310ee5a890dd05717209805c48520bb20`.
Bằng chứng chi tiết nằm trong [integration.json](./integration.json).

## Kiểm thử và an toàn dữ liệu

- `python -m unittest discover -s test`: **PASS**, 30 tests.
- MerRec/Gold targeted tests: **PASS**, 12 tests.
- `py_compile`, `git diff --check`: **PASS**.
- MinIO integration: **PASS**, dùng bucket
  `nutee-event-preservation-g3-test-20261126`.
- Bucket chính thức `nutee-recsys`: inventory 16 objects và SHA-256
  `1b766338a0e8ef8b1ea6ee4559423f7a533e3553544e201f16562ed450ce3b42`
  không đổi trước/sau; versioning vẫn `null`.
- Bucket baseline GĐ2 giữ nguyên 4 objects và inventory SHA-256
  `c07751a52c5366038f5a0b76f1f987261c545eb95d0630ed5b19908d535838b4`.
- Các object Silver và Gold V2 của bucket GĐ3 được giữ lại; không xóa dữ liệu.

## Hạn chế

Kết quả áp dụng cho shard MerRec `20230501/000000000030.parquet` hiện có trong
MinIO. Việc chuẩn hóa ID MerRec đã tồn tại tại Silver từ GĐ2 và không thay đổi
trong GĐ3. Chưa thực hiện các nội dung GĐ4 trở đi.
