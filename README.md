# Phân tích và dự đoán doanh số sản phẩm trên sàn thương mại điện tử

Dự án phân tích và dự đoán doanh số sản phẩm trên Tiki dựa trên dữ liệu lịch sử.

## 📋 Tổng quan

Dự án này bao gồm:
1. **Thu thập dữ liệu**: Crawl dữ liệu sản phẩm từ Tiki
2. **Phân tích dữ liệu**: Khám phá và phân tích dữ liệu thu thập được
3. **Dự đoán**: Xây dựng models để dự đoán doanh số sản phẩm

## 🚀 Quick Start

Xem hướng dẫn chi tiết tại [QUICK_START.md](QUICK_START.md)

### 1. Crawl dữ liệu
```bash
python scripts/crawl.py
```
→ Lưu JSON vào `data/raw/`

### 2. Build/Update Database
```bash
# Lần đầu tiên
python scripts/build_db.py

# Các lần sau
python scripts/update_db.py
```

### 3. Export to CSV
```bash
python scripts/export_to_csv.py
```
→ Lưu CSV vào `data/exports/`

## 📁 Cấu trúc dự án

Xem chi tiết tại [PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md)

```
tiki_crawl/
├── data/              # Dữ liệu (raw, processed, exports, database)
├── notebooks/         # Jupyter notebooks cho phân tích
├── src/              # Source code modules
│   ├── crawler/      # Module crawl dữ liệu
│   ├── analysis/     # Module phân tích
│   ├── models/       # Module ML models
│   ├── utils/        # Utilities
│   └── visualization/ # Visualization
├── scripts/          # Standalone scripts
│   ├── crawl.py     # Crawl dữ liệu (chỉ lưu JSON)
│   ├── build_db.py  # Build database từ JSON
│   ├── update_db.py # Update database từ JSON
│   └── export_to_csv.py # Export CSV từ database
├── models/           # Saved models
├── reports/          # Báo cáo và visualizations
└── config/           # Configuration files
```

## 📦 Cài đặt

```bash
pip install -r requirements.txt
```

## 🔧 Cấu hình

Chỉnh sửa `config/config.json` để tùy chỉnh:
- Số workers song song
- Rate limit
- Số sản phẩm mỗi danh mục
- Danh sách danh mục crawl

## 📊 Workflow

1. **Thu thập dữ liệu**: `scripts/crawl.py` → Lưu JSON vào `data/raw/`
2. **Xây dựng database**: `scripts/build_db.py` hoặc `scripts/update_db.py` → Tạo/cập nhật database
3. **Export CSV**: `scripts/export_to_csv.py` → Export CSV từ database
4. **Khám phá dữ liệu**: `notebooks/01_data_exploration.ipynb`
5. **Làm sạch dữ liệu**: `notebooks/02_data_cleaning.ipynb`
6. **Feature Engineering**: `notebooks/03_feature_engineering.ipynb`
7. **Phân tích khám phá**: `notebooks/04_eda.ipynb`
8. **Huấn luyện model**: `notebooks/05_model_training.ipynb`
9. **Dự đoán**: `notebooks/06_prediction.ipynb`

## 📝 Tài liệu

- [QUICK_START.md](QUICK_START.md) - Hướng dẫn nhanh
- [PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md) - Cấu trúc dự án chi tiết

## 📄 License

MIT License

