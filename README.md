# NUTEE Benchmark Data Pipeline

Pipeline Python xử lý benchmark Recommendation System theo Medallion:
**Bronze** lưu file nguồn, **Silver** chuẩn hóa theo adapter, **Gold** tạo event
log theo hợp đồng chung. Dữ liệu lớn không lưu trong Git. Python chạy trên host;
Docker Compose hiện chỉ khởi động MinIO.

## Cấu trúc repository

```text
data/
  datasets.json                  # Cấu hình dataset được theo dõi trong Git
  amazon_m2/                     # CSV local, bị Git ignore
  sigir/                         # CSV Coveo local, bị Git ignore
  bronze/<dataset>/<sha256>/     # File nguồn khi chạy local
  silver/<dataset>/<adapter>/<sha256>/
  gold/<dataset>/<sha256>/       # Gold V1, retained for existing snapshots
  gold/v2/<dataset>/<sha256>/    # Gold V2, retained
  gold/v3/<dataset>/<sha256>/    # Gold V3, current REES46/MerRec output
extractor/common/                # Đọc URL/local CSV, gzip, Parquet; tính SHA-256
loader/common/                   # Ghi MinIO hoặc lưu local
transformer/common/              # Writer Silver, Gold và kiểm tra hợp đồng
transformer/datasets/            # Mapping riêng cho từng dataset
transformer/registry.py          # Đăng ký adapter
test/                            # Unit test, test adapter và script nghiệm thu
outputs/                         # Báo cáo nghiệm thu; Git ignore
pipeline.py                      # Điều phối pipeline và cấu hình storage
run_pipeline.py                  # Entrypoint CLI
requirements.txt                 # Dependencies Python
docker-compose.yml               # MinIO API/Console và persistent volume
Dockerfile                       # Hiện rỗng, chưa dùng để chạy pipeline
```

## Yêu cầu

- Python **3.11 trở lên** (đã kiểm tra với Python 3.13.2).
- Git, Docker Desktop và Docker Compose v2.
- Internet để tải REES46/MerRec theo cấu hình.
- Dung lượng đĩa cho file nguồn, bản tạm và Parquet; dataset Coveo/Amazon-M2
  lớn. Pipeline tạo file tạm khi đọc và chuẩn hóa.

Dependencies đang khai báo trong `requirements.txt`: `minio`, `pyarrow`,
`python-dotenv`.

## Khởi tạo môi trường

```powershell
git clone https://github.com/pton28/AI_testing.git
Set-Location .\AI_testing

py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

Copy-Item .env.example .env
notepad .env
```

Trong `.env`, thay giá trị mẫu bằng thông tin đăng nhập riêng:

```dotenv
MINIO_ENDPOINT_URL=localhost:9000
MINIO_ROOT_USER=<your-local-minio-user>
MINIO_ROOT_PASSWORD=<your-local-minio-password>
```

`MINIO_BUCKET` mặc định là `nutee-recsys`; `MINIO_SECURE` mặc định là `false`.
`MINIO_ENDPOINT_URL` dùng dạng `host:port`, không gồm `http://`. Có thể thêm
`MINIO_BUCKET` hoặc `MINIO_SECURE` vào `.env` nếu cần thay đổi. Không commit `.env`.

## Chuẩn bị dataset

**Google Drive:** [Link Dataset](<https://drive.google.com/drive/folders/1ttJrOOTqCZO_ur3E02iD4d_5A0wUkzo3?usp=sharing>)

Link Drive ở trên là nguồn tải file local; quyền truy cập chưa được xác minh.
Các đường dẫn pipeline hiện khai báo trong `data/datasets.json`: REES46 và
MerRec dùng URL trực tiếp; Amazon-M2 và Coveo đọc file local. Đặt file tải về
đúng tên và vị trí bên dưới. Không đổi tên cột hoặc file nếu chưa cập nhật
adapter/cấu hình.

| Dataset | File/nguồn theo cấu hình | Pipeline hiện có |
|---|---|---|
| `rees46_electronics` | URL CSV Gzip của REES46 | Bronze → Silver → Gold |
| `merrec` | Một shard Parquet `20230501/000000000030.parquet` từ Hugging Face | Bronze → Silver → Gold; chỉ shard này |
| `amazon_m2` | `data/amazon_m2/sessions_train.csv`, `data/amazon_m2/products_train.csv` | Bronze → Silver |
| `coveo_sigir_ecom_2021` | `data/sigir/browsing_train.csv`, `data/sigir/search_train.csv`, `data/sigir/sku_to_content.csv` | Bronze → Silver |

Coveo yêu cầu đăng ký và chấp nhận điều khoản sử dụng chính thức. `LOCAL_READY`
trong cấu hình chỉ mô tả bộ file local đã được chuẩn bị; pipeline không tải
hoặc kiểm tra quyền truy cập Google Drive.

Cấu trúc `data/` đầu vào:

```text
data/
  datasets.json
  amazon_m2/
    sessions_train.csv
    products_train.csv
  sigir/
    browsing_train.csv
    search_train.csv
    sku_to_content.csv
```

Kiểm tra các file local trước khi chạy:

```powershell
$required = @(
  ".\data\amazon_m2\sessions_train.csv",
  ".\data\amazon_m2\products_train.csv",
  ".\data\sigir\browsing_train.csv",
  ".\data\sigir\search_train.csv",
  ".\data\sigir\sku_to_content.csv"
)
$missing = $required | Where-Object { -not (Test-Path $_ -PathType Leaf) }
if ($missing) { throw "Missing dataset files: $($missing -join ', ')" }
```

`data/` bị Git ignore, ngoại trừ `data/datasets.json`; file dataset phải được
tải/copy riêng trên máy mới.

### Thêm dataset vào cấu hình

- Dataset một nguồn cho luồng Bronze → Silver → Gold khai báo trong `datasets`
  với `name`, `source`, `adapter`. `source` có thể là URL hoặc đường dẫn tương
  đối với thư mục `data/`.
- Dataset nhiều asset Silver-only khai báo trong `planned_datasets` với mảng
  `files`; mỗi asset cần `source` và adapter Silver đã đăng ký.
- Dataset có schema mới cần adapter trong
  `transformer/datasets/<dataset_name>/` và đăng ký trong
  `transformer/registry.py`. Không thêm adapter mới chỉ bằng cách sửa JSON.
- `experiments`, `status`, `scope` là metadata; pipeline hiện không dùng chúng
  để quyết định bước xử lý.

Ví dụ đường dẫn local trong JSON:

```json
{
  "name": "new_dataset",
  "source": "new_dataset/events.csv",
  "adapter": "registered_adapter"
}
```

Chỉ dùng ví dụ này khi `registered_adapter` thực sự tồn tại trong registry và
schema file tương thích.

## Khởi động MinIO

```powershell
docker compose config --quiet
docker compose up -d
docker compose ps
Invoke-WebRequest http://localhost:9000/minio/health/live
```

- API: `http://localhost:9000`
- Console: `http://localhost:9001`
- Đăng nhập bằng `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` trong `.env`.
- Bucket mặc định: `nutee-recsys`; pipeline tự tạo bucket khi bắt đầu chạy.
- Compose lưu dữ liệu trong named volume `minio_data`.

Compose **không build hoặc chạy ứng dụng Python**; chạy pipeline từ terminal đã
kích hoạt virtual environment. `Dockerfile` hiện rỗng, không dùng lệnh
`docker build` với file này.

## Chạy pipeline

Tất cả dataset trong `datasets` (hiện gồm REES46 và một shard MerRec):

```powershell
python -B .\run_pipeline.py
```

Chạy riêng từng dataset:

```powershell
python -B .\run_pipeline.py --dataset rees46_electronics --storage minio
python -B .\run_pipeline.py --dataset merrec --storage minio
```

Amazon-M2 và Coveo nằm trong `planned_datasets`, nên cần chạy riêng bằng
`--silver-only`:

```powershell
python -B .\run_pipeline.py --dataset amazon_m2 --storage minio --silver-only
python -B .\run_pipeline.py --dataset coveo_sigir_ecom_2021 --storage minio --silver-only
```

`--silver-only` ghi Bronze và Silver, không tạo Gold. Amazon-M2 giữ lịch sử
session/nhãn/catalog; Coveo giữ browsing, search vectors và SKU content.
Hai nguồn này hiện chưa có Gold adapter. Không có command Bronze-only.

Có thể lưu local để kiểm tra/thử nghiệm thay cho MinIO:

```powershell
python -B .\run_pipeline.py --dataset rees46_electronics --storage local
```

Để chỉ dựng lại Gold từ Silver đã có, lấy SHA-256 từ Bronze key/log lần chạy
trước:

```powershell
$sourceVersion = Read-Host "SHA-256 source version"
python -B .\run_pipeline.py --dataset rees46_electronics --storage minio `
  --gold-only --source-version $sourceVersion
```

`--gold-only` chỉ hỗ trợ dataset có Gold adapter (hiện REES46 và MerRec).
Xem thêm tùy chọn CLI:

```powershell
python -B .\run_pipeline.py --help
```

Tạo Product Dimension từ Silver đã có mà không dựng lại Event Log:

```powershell
$sourceVersion = Read-Host "SHA-256 source version"
python -B .\run_pipeline.py --dataset rees46_electronics --storage minio `
  --product-dimension-only --source-version $sourceVersion
```

Thay dataset bằng `merrec` để chạy cho MerRec. Kết quả nằm tại
`gold/v3/<dataset>/<sha256>/products.parquet`; `events.parquet` không bị thay
đổi. Dimension chọn giá trị không rỗng mới nhất theo từng thuộc tính; nếu
timestamp trùng, dùng giá trị theo thứ tự từ điển để phá tie deterministically.
Các trường `*_observed_at` ghi thời điểm nguồn của từng giá trị. MerRec lấy
tên từ Silver `name`; REES46 không có tên sản phẩm trong Silver nên
`product_name` là null. Đây là snapshot metadata toàn dataset, không có training
cutoff và không được mặc định dùng trực tiếp để benchmark theo thời gian.

Tạo ALS interactions và session sequences từ Gold V3 cùng Product Dimension đã
có, không dựng lại các tầng trước:

```powershell
$sourceVersion = Read-Host "SHA-256 source version"
python -B .\run_pipeline.py --dataset rees46_electronics --storage minio `
  --model-ready-only --source-version $sourceVersion
```

Thay dataset bằng `merrec` để chạy MerRec. Lệnh cần Gold V3 và
`products.parquet` cùng `source_version` trong bucket đã cấu hình; chỉ ghi hai
Parquet model-ready và kiểm tra lại nội dung sau khi upload:

```text
gold/v3/<dataset>/<sha256>/model_ready/als_interactions.parquet
gold/v3/<dataset>/<sha256>/model_ready/session_sequences.parquet
```

ALS aggregation giữ repeated interactions, không gán weights hoặc lọc K-core.
Session sequences bỏ events thiếu `session_key` khỏi output sequence, giữ
repeated products, sắp xếp theo `position_in_session` và đánh dấu session có
event ambiguous. Không tạo target item hoặc training samples.

### Data leakage và benchmark

Các output model-ready là aggregate của toàn snapshot, chưa phải train/test
dataset. Nhóm AI không được dùng ALS interactions toàn snapshot để train rồi
đánh giá trên cùng dữ liệu. Trước khi benchmark:

- Chia Gold Event Log theo thời gian trước, rồi mới aggregate ALS interactions
  từ phần training.
- Với session-based protocol, chia session và tạo input/target theo protocol
  benchmark đã thống nhất; không dùng future interactions trong training.
- Product metadata phải phù hợp với training cutoff và những thông tin thực sự
  sẵn có tại thời điểm dự đoán. REES46 không có `product_name` trong Silver.
- Không áp dụng K-core filtering trên toàn bộ train/test snapshot.

GĐ6 không thực hiện temporal split, benchmark protocol, training hoặc target
generation; các quyết định evaluation vẫn thuộc nhóm AI.

## Kiểm tra kết quả

MinIO object key theo dạng:

```text
bronze/<dataset>/<sha256>/<source-file>
silver/<dataset>/<adapter>/<sha256>/events.parquet   # REES46/MerRec
silver/<dataset>/<adapter>/<sha256>/data.parquet     # Amazon-M2/Coveo
gold/<dataset>/<sha256>/events.parquet               # Gold V1; retained
gold/v2/<dataset>/<sha256>/events.parquet            # Gold V2; retained, 12 columns
gold/v3/<dataset>/<sha256>/events.parquet            # Gold V3; 13 columns
gold/v3/<dataset>/<sha256>/products.parquet          # Product Dimension snapshot
gold/v3/<dataset>/<sha256>/model_ready/              # ALS interactions and session sequences
manifests/<dataset>/<sha256>/manifest.json           # Processing provenance and checksums
```

Log pipeline in vị trí Bronze/Silver/Gold và số dòng Silver. Trong Console, mở
bucket `nutee-recsys` và kiểm tra prefix tương ứng. Gold V1 giữ schema 11 cột;
Gold V2 giữ 12 cột, gồm `session_key`; Gold V3 thêm `order_ambiguous` (13 cột)
và sắp xếp REES46/MerRec theo session, timestamp, rồi `event_id`. Ties được đánh
dấu ambiguous; đây là deterministic ordering, không phải ground truth. Các
phiên bản Gold dùng prefix riêng, Parquet Snappy và được kiểm tra khi đọc lại.
Silver được ghi thành Parquet Snappy; pipeline dừng nếu thiếu cột, dữ liệu sai
kiểu theo adapter hoặc không có dòng hợp lệ. Bronze là file nguồn được upload;
không có bước validation nội dung Bronze riêng.

### Manifest và nghiệm thu GĐ7

Manifest ghi `source_version`, SHA-256 của source và từng output, schema,
configuration fingerprint, commit/worktree, row counts và trạng thái nghiệm
thu. Với Parquet, SHA-256 byte-level có thể khác dù schema và logical rows
giống nhau; verifier so sánh logical content trước khi xem đó là conflict.

Verifier GĐ7 đọc Bronze và các baseline từ bucket chính thức/accepted, nhưng
chỉ ghi vào bucket test riêng. Dùng tên bucket mới, chưa tồn tại:

```powershell
$testBucket = "nutee-final-acceptance-g7-test-$((Get-Date).ToUniversalTime().ToString('yyyyMMdd-HHmmss'))"
python -B -m test.common.verify_final_acceptance_g7_minio --test-bucket $testBucket
```

Không xóa bucket test sau khi chạy. Manifest nằm tại
`manifests/<dataset>/<sha256>/manifest.json`; báo cáo local nằm dưới
`reports/final_acceptance_g7/`. `--help` và verifier đã được chạy trong lượt
bàn giao hiện tại, nhưng nghiệm thu GĐ7 chỉ được đánh dấu PASS khi cả hai
dataset hoàn tất hai lượt pipeline, logical comparisons và inventory checks.
Nếu chỉ chạy một lượt, giữ outputs nhưng ghi reproducibility là NOT TESTED.

Có thể kiểm tra bucket và liệt kê object mà không in credentials:

```powershell
@'
import os
from dotenv import load_dotenv
from minio import Minio

load_dotenv(".env")
client = Minio(
    os.getenv("MINIO_ENDPOINT_URL", "localhost:9000"),
    access_key=os.environ["MINIO_ROOT_USER"],
    secret_key=os.environ["MINIO_ROOT_PASSWORD"],
    secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
)
bucket = os.getenv("MINIO_BUCKET", "nutee-recsys")
if not client.bucket_exists(bucket):
    raise SystemExit(f"Bucket not found: {bucket}")
print(f"Bucket OK: {bucket}")
for dataset in ("rees46_electronics", "merrec", "amazon_m2", "coveo_sigir_ecom_2021"):
    for layer in ("bronze", "silver", "gold"):
        prefix = f"{layer}/{dataset}/"
        for obj in client.list_objects(bucket, prefix=prefix, recursive=True):
            print(obj.object_name, obj.size)
'@ | python -
```

Chạy unit test không cần tải dataset hoặc ghi vào MinIO:

```powershell
python -B -m unittest discover -s test -v
```

## Dừng và khởi động lại

```powershell
docker compose stop
docker compose start
```

Hoặc gỡ container nhưng giữ dữ liệu MinIO:

```powershell
docker compose down
docker compose up -d
```

Không dùng `docker compose down --volumes` nếu cần giữ dữ liệu trong
`minio_data`.

## Xử lý sự cố

| Triệu chứng | Cách kiểm tra/xử lý |
|---|---|
| Compose báo thiếu `MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD` | Tạo `.env` từ `.env.example`, thay giá trị mẫu rồi chạy lại `docker compose config --quiet`. |
| Pipeline không kết nối được MinIO | Kiểm tra `docker compose ps`, health URL, API endpoint `localhost:9000` và `.env`. |
| `No matching configured dataset` với Amazon-M2/Coveo | Chạy đúng tên dataset cùng `--silver-only`; chúng chưa nằm trong `datasets`. |
| Thiếu file hoặc `Missing source columns` | Kiểm tra đường dẫn tương đối từ `data/`, tên file/header và phiên bản dataset. |
| Lỗi tải REES46/MerRec | Kiểm tra Internet và URL hiện hành trong `data/datasets.json`. |
| Hết dung lượng hoặc chạy lâu | Kiểm tra dung lượng đĩa tạm và MinIO; file đầu vào được copy/hash trước khi transform. |
| Không thấy dataset sau khi clone | Đây là file bị Git ignore; tải từ nguồn/Drive được nhóm cấp rồi đặt đúng cấu trúc ở trên. |

## Tình trạng bàn giao

- Có thể chạy REES46/MerRec từ URL đã cấu hình khi máy có Python dependencies,
  Internet và MinIO.
- Amazon-M2/Coveo cần file local; các file này không được clone cùng repository.
- Cần xác nhận quyền truy cập Google Drive với thành viên mới trước khi bàn giao.
- `.env.example` được giữ trong Git ignore exceptions; `.env`, dữ liệu dưới
  `data/` và `outputs/` bị loại khỏi Git theo `.gitignore`.
- Đã kiểm tra trạng thái Git: `.env.example`, `data/datasets.json` và README
  được theo dõi; `.env`, dataset local và `outputs/` bị ignore.
- `docker-compose.yml` dùng MinIO image tag `latest`, chưa pin phiên bản; nên
  pin tag trước khi cần deployment tái lập lâu dài.
