# Quick Start Guide

Hướng dẫn nhanh để crawl dữ liệu sản phẩm từ Tiki và export ra CSV.

## 📋 Yêu cầu

- Python 3.7+
- Các thư viện trong `requirements.txt`

## 🚀 Cài đặt

### 1. Cài đặt dependencies

```bash
pip install -r requirements.txt
```

### 2. Kiểm tra cấu hình

File cấu hình chính: `config/config.json`

Các thông số quan trọng:
- `max_workers`: Số worker song song (mặc định: 10)
- `rate_limit_per_second`: Giới hạn request/giây (mặc định: 3)
- `max_products_per_category`: Số sản phẩm tối đa mỗi danh mục (mặc định: 250)
- `categories_file`: File danh sách danh mục (mặc định: `config/categories.json`)

## 🕷️ Crawl dữ liệu

### Chạy với cấu hình mặc định

```bash
python scripts/crawl.py
```

### Tùy chỉnh số workers và rate limit

```bash
# Sử dụng 20 workers và rate limit 5 req/s
python scripts/crawl.py --workers 20 --rate-limit 5

# Chỉ thay đổi số workers
python scripts/crawl.py --workers 15

# Chỉ thay đổi rate limit
python scripts/crawl.py --rate-limit 10
```

### Sử dụng file config khác

```bash
python scripts/crawl.py --config config/my_config.json
```

### Kết quả sau khi crawl

- **Raw JSON**: `data/raw/parallel_crawl_results_YYYYMMDD_HHMMSS.json`
- **Logs**: `logs/crawler/parallel_crawl_YYYYMMDD_HHMMSS.log`

**Lưu ý**: Script `crawl.py` chỉ crawl và lưu JSON, không lưu vào database. Để import vào database, sử dụng `build_db.py` hoặc `update_db.py`.

## 🗄️ Xây dựng/Cập nhật Database

### Build database từ JSON (lần đầu tiên)

```bash
# Sử dụng file JSON mới nhất
python scripts/build_db.py

# Chỉ định file JSON cụ thể
python scripts/build_db.py --json data/raw/parallel_crawl_results_20251121_022421.json

# Chỉ định database path
python scripts/build_db.py --db data/database/my_database.db
```

### Update database từ JSON

```bash
# Cập nhật từ TẤT CẢ file JSON (mặc định, xử lý theo thứ tự thời gian)
python scripts/update_db.py

# Chỉ cập nhật file JSON mới nhất
python scripts/update_db.py --latest

# Cập nhật từ file JSON cụ thể
python scripts/update_db.py --json data/raw/parallel_crawl_results_20251121_022421.json
```

### Kết quả

- **Database**: `data/database/tiki_products_multi.db` (SQLite)

## 📊 Export to CSV

### Export tất cả dữ liệu

```bash
python scripts/export_to_csv.py
```

### Kết quả export

Tất cả file CSV được lưu trong thư mục `data/exports/`:

- `products.csv` - Thông tin sản phẩm
- `sellers.csv` - Thông tin người bán
- `price_history.csv` - Lịch sử giá
- `sales_history.csv` - Lịch sử bán hàng
- `rating_history.csv` - Lịch sử đánh giá
- `product_details.csv` - Chi tiết sản phẩm
- `crawl_logs.csv` - Log các lần crawl

## 📝 Workflow đầy đủ

### Bước 1: Crawl dữ liệu

```bash
python scripts/crawl.py --workers 10 --rate-limit 3
```

Chờ quá trình crawl hoàn tất. Bạn sẽ thấy:
- Số lượng categories đã crawl
- Số lượng products đã crawl
- Tốc độ crawl (products/second)
- Thời gian thực hiện
- File JSON được lưu tại `data/raw/`

### Bước 2: Build/Update Database

**Lần đầu tiên (build database mới):**
```bash
python scripts/build_db.py
```

**Các lần sau (cập nhật database):**
```bash
# Cập nhật từ tất cả file JSON (mặc định)
python scripts/update_db.py

# Hoặc chỉ cập nhật file mới nhất
python scripts/update_db.py --latest
```

### Bước 3: Export ra CSV

```bash
python scripts/export_to_csv.py
```

Sau khi export xong, kiểm tra thư mục `data/exports/` để lấy các file CSV.

## 🔄 Workflow với Google Drive

Nếu bạn thường xuyên xóa database local và upload JSON lên Drive:

1. **Crawl dữ liệu:**
   ```bash
   python scripts/crawl.py
   ```

2. **Upload JSON lên Google Drive** (file trong `data/raw/`)

3. **Download JSON từ Drive về local** (vào `data/raw/`)

4. **Build/Update database:**
   ```bash
   # Lần đầu tiên
   python scripts/build_db.py
   
   # Các lần sau
   python scripts/update_db.py --all
   ```

5. **Export CSV:**
   ```bash
   python scripts/export_to_csv.py
   ```

## ⚙️ Tùy chỉnh danh mục crawl

Chỉnh sửa file `config/categories.json` để thêm/bớt danh mục:

```json
{
  "categories": [
    {
      "id": 8322,
      "name": "Nhà sách Tiki",
      "url": "https://tiki.vn/nha-sach-tiki/c8322"
    },
    {
      "id": 1883,
      "name": "Nhà cửa đời sống",
      "url": "https://tiki.vn/nha-cua-doi-song/c1883"
    }
  ]
}
```

## 🔍 Kiểm tra kết quả

### Xem log crawl

```bash
# Windows
type logs\parallel_crawl_*.log | more

# Linux/Mac
tail -f logs/parallel_crawl_*.log
```

### Kiểm tra database

Sử dụng SQLite browser hoặc command line:

```bash
sqlite3 data/tiki_products_multi.db

# Xem số lượng sản phẩm
SELECT COUNT(*) FROM products;

# Xem các bảng
.tables

# Thoát
.exit
```

## 💡 Tips

1. **Tốc độ crawl**: Tăng `max_workers` và `rate_limit_per_second` để crawl nhanh hơn, nhưng cẩn thận không làm quá tải server Tiki.

2. **Giới hạn sản phẩm**: Điều chỉnh `max_products_per_category` trong `config.json` để giới hạn số sản phẩm crawl mỗi danh mục.

3. **Export định kỳ**: Chạy `export_to_csv.py` sau mỗi lần crawl để có dữ liệu CSV mới nhất.

4. **Backup database**: Sao lưu file `data/tiki_products_multi.db` định kỳ để tránh mất dữ liệu.

## ❓ Troubleshooting

### Lỗi: Database not found

**Nguyên nhân**: Chưa build database từ JSON hoặc đường dẫn database sai.

**Giải pháp**: 
- Chạy `python scripts/build_db.py` để tạo database từ JSON
- Hoặc `python scripts/update_db.py` nếu đã có database
- Kiểm tra đường dẫn trong `scripts/export_to_csv.py` (mặc định: `data/database/tiki_products_multi.db`)

### Crawl bị gián đoạn

**Giải pháp**: 
- Chạy lại `python scripts/crawl.py`, script sẽ crawl lại từ đầu
- Kiểm tra log để xem lỗi cụ thể
- File JSON đã crawl sẽ được lưu, có thể dùng `update_db.py` để import vào database

### Rate limit quá cao

**Triệu chứng**: Nhiều request bị từ chối hoặc timeout.

**Giải pháp**: Giảm `rate_limit_per_second` xuống 2-3 req/s.

## 📚 Thông tin thêm

- Database được lưu tại: `data/database/tiki_products_multi.db`
- Logs được lưu tại: `logs/crawler/`
- CSV exports được lưu tại: `data/exports/`
- Raw JSON results được lưu tại: `data/raw/`
- Xem thêm cấu trúc dự án tại: `PROJECT_STRUCTURE.md`

