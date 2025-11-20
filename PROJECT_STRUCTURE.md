# Cấu trúc dự án

## Tổng quan

Dự án: **Phân tích và dự đoán doanh số sản phẩm trên sàn thương mại điện tử dựa trên dữ liệu lịch sử**

## Cấu trúc folder

```
tiki_crawl/
├── data/                      # Dữ liệu
│   ├── raw/                  # Dữ liệu crawl gốc (JSON)
│   ├── processed/            # Dữ liệu đã xử lý, làm sạch
│   ├── exports/              # CSV exports từ database
│   └── database/             # SQLite database files
│       └── tiki_products_multi.db
│
├── notebooks/                # Jupyter notebooks cho phân tích
│   ├── 01_data_exploration.ipynb
│   ├── 02_data_cleaning.ipynb
│   ├── 03_feature_engineering.ipynb
│   ├── 04_eda.ipynb          # Exploratory Data Analysis
│   ├── 05_model_training.ipynb
│   └── 06_prediction.ipynb
│
├── src/                      # Source code
│   ├── crawler/              # Module crawl dữ liệu
│   │   ├── __init__.py
│   │   ├── crawler_parallel.py
│   │   └── database_v2.py
│   │
│   ├── analysis/             # Module phân tích dữ liệu
│   │   ├── __init__.py
│   │   ├── data_loader.py    # Load data từ database
│   │   ├── data_cleaner.py   # Làm sạch dữ liệu
│   │   ├── feature_engineering.py
│   │   └── statistics.py     # Thống kê mô tả
│   │
│   ├── models/               # Module ML models
│   │   ├── __init__.py
│   │   ├── base_model.py
│   │   ├── time_series.py    # Time series models
│   │   ├── regression.py     # Regression models
│   │   └── ensemble.py       # Ensemble methods
│   │
│   ├── utils/                # Utilities
│   │   ├── __init__.py
│   │   ├── export_csv_v2.py
│   │   └── helpers.py
│   │
│   └── visualization/       # Module visualization
│       ├── __init__.py
│       └── plots.py
│
├── models/                   # Saved trained models
│   ├── checkpoints/
│   └── best_models/
│
├── reports/                  # Báo cáo và visualizations
│   ├── figures/             # Charts, graphs
│   └── reports/             # PDF reports
│
├── config/                   # Configuration files
│   ├── config.json          # Crawler config
│   ├── categories.json       # Categories to crawl
│   └── model_config.json    # Model hyperparameters
│
├── logs/                     # Logs
│   ├── crawler/             # Crawler logs
│   └── training/            # Training logs
│
├── scripts/                  # Standalone scripts
│   ├── crawl.py            # Main crawl script
│   └── export_to_csv.py    # Export script
│
├── tests/                    # Unit tests
│   ├── test_crawler.py
│   └── test_analysis.py
│
├── .ignore/                  # Files to ignore
├── .gitignore
├── requirements.txt
├── README.md
├── QUICK_START.md
└── PROJECT_STRUCTURE.md      # File này
```

## Mô tả các folder

### 📁 `data/`
- **raw/**: Dữ liệu JSON thô từ crawl
- **processed/**: Dữ liệu đã được làm sạch và xử lý
- **exports/**: File CSV export từ database
- **database/**: SQLite database files

### 📁 `notebooks/`
Jupyter notebooks cho các bước phân tích:
1. Data exploration - Khám phá dữ liệu
2. Data cleaning - Làm sạch dữ liệu
3. Feature engineering - Tạo features
4. EDA - Phân tích khám phá
5. Model training - Huấn luyện model
6. Prediction - Dự đoán

### 📁 `src/`
Source code được tổ chức theo modules:
- **crawler/**: Code crawl dữ liệu (phần hiện tại)
- **analysis/**: Code phân tích và xử lý dữ liệu
- **models/**: Code ML models cho prediction
- **utils/**: Utilities và helper functions
- **visualization/**: Code tạo visualizations

### 📁 `models/`
Lưu các trained models:
- **checkpoints/**: Model checkpoints trong quá trình training
- **best_models/**: Best models đã được chọn

### 📁 `reports/`
Báo cáo và visualizations:
- **figures/**: Charts, graphs, plots
- **reports/**: PDF reports, presentations

### 📁 `scripts/`
Standalone scripts để chạy:
- **crawl.py**: Script crawl dữ liệu
- **export_to_csv.py**: Script export CSV

### 📁 `tests/`
Unit tests cho các modules

## Workflow

1. **Thu thập dữ liệu** (`scripts/crawl.py`)
   - Crawl dữ liệu từ Tiki
   - Lưu vào database và `data/raw/`

2. **Khám phá dữ liệu** (`notebooks/01_data_exploration.ipynb`)
   - Load dữ liệu từ database
   - Hiểu cấu trúc dữ liệu

3. **Làm sạch dữ liệu** (`notebooks/02_data_cleaning.ipynb`, `src/analysis/data_cleaner.py`)
   - Xử lý missing values
   - Xử lý outliers
   - Lưu vào `data/processed/`

4. **Feature Engineering** (`notebooks/03_feature_engineering.ipynb`, `src/analysis/feature_engineering.py`)
   - Tạo features mới
   - Feature selection

5. **Phân tích khám phá** (`notebooks/04_eda.ipynb`, `src/visualization/plots.py`)
   - Phân tích xu hướng
   - Tạo visualizations
   - Lưu vào `reports/figures/`

6. **Huấn luyện model** (`notebooks/05_model_training.ipynb`, `src/models/`)
   - Train các models
   - Evaluate và chọn best model
   - Lưu vào `models/best_models/`

7. **Dự đoán** (`notebooks/06_prediction.ipynb`)
   - Sử dụng best model để dự đoán
   - Tạo báo cáo

## Best Practices

1. **Version Control**: Commit thường xuyên, sử dụng branches cho các features
2. **Documentation**: Viết docstrings cho functions, comments cho code phức tạp
3. **Testing**: Viết tests cho các functions quan trọng
4. **Config**: Sử dụng config files thay vì hardcode
5. **Logging**: Sử dụng logging thay vì print statements
6. **Data**: Không commit dữ liệu lớn vào git, sử dụng .gitignore

