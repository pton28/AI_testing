# Benchmark Dataset Pipeline

Hệ thống thu thập, lưu trữ và xử lý các benchmark dataset phục vụ huấn luyện và đánh giá mô hình Recommendation System của NUTEE theo kiến trúc Medallion (Bronze – Silver – Gold).

## 1. Prerequisites

- Python
- Docker & Docker Compose
- Git

## 2. Project Structure

```text
AI_testing/
├── data/                # Original datasets
├── extractor/           # Extract data from source
├── loader/              # Upload data to MinIO
├── transformer/         # Clean and transform data
├── datasets.json        # Dataset configurations
├── docker-compose.yml   # MinIO infrastructure
├── requirements.txt     # Python dependencies
├── .env.example         # Environment template
├── .gitignore
├── main.py              # Pipeline entry point
└── README.md
```

## 3. Environment Setup

Clone repository và di chuyển vào thư mục project.

Cài đặt dependencies:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Khởi tạo biến môi trường:

```powershell
Copy-Item .env.example .env
```

## 4. Dataset Setup

Do dung lượng lớn, các dataset được lưu trữ trên Google Drive thay vì GitHub.

**Google Drive:** [Insert Google Drive Link]

### Download and organize

1. Tải các dataset từ Google Drive.
2. Giải nén vào thư mục `data/`.
3. Giữ nguyên tên thư mục và tên file.
4. Đảm bảo đường dẫn khớp với cấu hình trong `datasets.json`.

Ví dụ:

```text
data/
├── amazon-m2/
│   ├── products_train.csv
│   ├── sessions_train.csv
│   ├── sessions_test_task1.csv
│   ├── sessions_test_task2.csv
│   └── sessions_test_task3.csv
└── ...
```

**Lưu ý:**

- Không commit dataset lên GitHub.
- Không chỉnh sửa trực tiếp dữ liệu gốc.
- Chỉ cần tải dataset một lần trên mỗi máy.
- Dataset mới cần được khai báo trong `datasets.json`.

## 5. Start MinIO

Khởi động MinIO bằng Docker Compose:

```powershell
docker compose up -d
docker compose ps
```

Truy cập MinIO:

| Service | Endpoint |
|---|---|
| MinIO API | http://localhost:9000 |
| MinIO Console | http://localhost:9001 |

Thông tin đăng nhập được cấu hình trong `.env`.

## 6. Run Data Pipeline

Sau khi chuẩn bị dataset và khởi động MinIO, chạy:

```powershell
python main.py
```

Pipeline thực hiện theo các tầng:

| Layer | Description |
|---|---|
| Bronze | Lưu trữ dataset gốc |
| Silver | Làm sạch và chuẩn hóa dữ liệu |
| Gold | Chuẩn bị dữ liệu phục vụ AI Training |

## 7. Verify Results

Truy cập MinIO Console và kiểm tra:

- Dataset đã được upload vào Bronze.
- Dữ liệu Silver đã được xử lý.
- Dữ liệu Gold sẵn sàng cho training.

Các thành viên phụ trách AI sử dụng dữ liệu tại Gold để huấn luyện và đánh giá mô hình.

## 8. Stop System

```powershell
docker compose down
```

Không sử dụng `docker compose down -v` nếu muốn giữ lại dữ liệu đã lưu trong MinIO volume.