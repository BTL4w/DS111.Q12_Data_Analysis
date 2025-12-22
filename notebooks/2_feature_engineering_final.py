import pandas as pd
import numpy as np
import os

DATA_DIR_PATH = '../DATASET' 
OUTPUT_DIR = '../data/processed'

# Hàm hỗ trợ xử lý thời gian cho file bị tách cột (Sales, Price)
def process_crawl_time(df):
    """
    Quy đổi crawl_section (10h-13h...) thành giờ cụ thể để máy tính sắp xếp được.
    """
    # 1. Từ điển quy ước giờ (Mapping)
    section_map = {
        '10h-13h': '11:00:00',       # Giờ trưa
        '15h30-17h30': '16:00:00',   # Giờ chiều
        '20h30-1h': '22:00:00'       # Giờ tối (Chốt ngày)
    }
    
    # 2. Tạo cột giờ tạm (nếu giá trị lạ thì gán 00:00:00)
    df['temp_time'] = df['crawl_section'].str.strip().map(section_map).fillna('00:00:00')
    
    # 3. Ghép Date + Giờ quy ước -> Timestamp chuẩn
    df['crawl_timestamp'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['temp_time'])
    
    # Xóa cột tạm cho gọn
    df.drop(columns=['temp_time'], inplace=True)
    return df

# 1. Giai đoạn EXTRACT 
def extract_data_from_csv(data_dir):
    print("--- BẮT ĐẦU LOAD DỮ LIỆU TỪ CSV ---")
    
    try:
        # A. Load SALES (Cần ghép thời gian)
        sales_path = os.path.join(data_dir, 'sales_history_processed.csv')
        df_sales = pd.read_csv(sales_path, usecols=['product_id', 'quantity_sold', 'date', 'crawl_section'])
        # Xử lý thời gian
        df_sales = process_crawl_time(df_sales)
        # Đổi tên cho khớp logic cũ
        df_sales.rename(columns={'quantity_sold': 'all_time_quantity_sold'}, inplace=True)
        print(f"1. Đã load Sales: {len(df_sales):,} dòng")

        # B. Load PRICE (Cần ghép thời gian)
        price_path = os.path.join(data_dir, 'price_history_processed.csv')
        df_price = pd.read_csv(price_path, usecols=['product_id', 'price', 'date', 'crawl_section'])
        df_price = process_crawl_time(df_price)
        print(f"2. Đã load Price: {len(df_price):,} dòng")

        # C. Load RATING (Đã có sẵn crawl_timestamp, file cleaned)
        rating_path = os.path.join(data_dir, 'ratings_processed.csv')
        df_rating = pd.read_csv(rating_path, usecols=['product_id', 'rating_average', 'review_count', 'crawl_timestamp'])
        df_rating['crawl_timestamp'] = pd.to_datetime(df_rating['crawl_timestamp']) # Convert sang datetime
        print(f"3. Đã load Rating: {len(df_rating):,} dòng")

        # D. Load PRODUCTS INFO (Thông tin tĩnh)
        info_path = os.path.join(data_dir, 'products_final.csv')
        df_info = pd.read_csv(info_path, usecols=['id', 'name', 'created_at'])
        df_info.rename(columns={'id': 'product_id'}, inplace=True)
        print(f"4. Đã load Info: {len(df_info):,} sản phẩm")
        
        return df_sales, df_price, df_rating, df_info
    
    except Exception as e:
        print(f"!!! LỖI KHI ĐỌC FILE CSV: {e}")
        return None, None, None, None

# 2. Giai đoạn TRANSFORM 
def transform_data(df_sales, df_price, df_rating, df_info):
    print("\n--- ĐANG TÍNH TOÁN CHỈ SỐ (FEATURE ENGINEERING) ---")
    
    # Lấy mốc thời gian đầu/cuối của dữ liệu Sales
    min_date = df_sales['crawl_timestamp'].min()
    max_date = df_sales['crawl_timestamp'].max()
    print(f" -> Dữ liệu trải dài từ: {min_date} đến {max_date}")
    first_day_date = min_date.date()

    # Xử lý Rating (Lấy trung bình sao cuối cùng) 
    # Sắp xếp theo thời gian để lấy dòng mới nhất
    df_rating_avg = df_rating.sort_values('crawl_timestamp').drop_duplicates('product_id', keep='last')[['product_id', 'rating_average']]

    # Aggregate Last Per Day (Chốt sổ cuối ngày) 
    def aggregate_daily_last(df, val_col):
        # Tạo cột ngày (bỏ giờ)
        df['day_only'] = df['crawl_timestamp'].dt.date
        # Sort để dòng có giờ muộn nhất nằm dưới cùng -> lấy last()
        df_daily = df.sort_values('crawl_timestamp').groupby(['product_id', 'day_only'])[val_col].last().reset_index()
        df_daily['date'] = pd.to_datetime(df_daily['day_only']) # Convert lại thành datetime để tính toán
        return df_daily
    
    # Áp dụng cho Sales, Price, Rating
    df_sales_agg = aggregate_daily_last(df_sales, 'all_time_quantity_sold')
    df_price_agg = aggregate_daily_last(df_price, 'price')
    df_rating_agg = aggregate_daily_last(df_rating, 'review_count') # Rating aggregate theo ngày 

    # Tính Daily Sales 
    print(" -> Đang tính số bán theo ngày (Daily Sales)...")
    df_sales_daily = df_sales_agg.copy().sort_values(['product_id', 'date'])
    df_sales_daily['daily_quantity_sold'] = df_sales_daily.groupby('product_id')['all_time_quantity_sold'].diff().fillna(0).clip(lower=0)
    
    # Tính Weekly Sales
    df_sales_daily['week_start'] = df_sales_daily['date'] - pd.to_timedelta(df_sales_daily['date'].dt.weekday, unit='D')
    df_sales_weekly = df_sales_daily.groupby(['product_id', 'week_start'])['daily_quantity_sold'].sum().reset_index()
    df_sales_weekly.rename(columns={'daily_quantity_sold': 'weekly_quantity_sold'}, inplace=True)

    # --- Tính toán Biến Mục tiêu & Features (Đầu kỳ vs Cuối kỳ) ---
    # Hàm lấy giá trị đầu/cuối
    def get_period_values(df_agg, val_col, rename_col_new, rename_col_old):
        df_sorted = df_agg.sort_values(['product_id', 'date'])
        
        # New: Lấy giá trị mới nhất (dòng cuối cùng của mỗi SP)
        df_new = df_sorted.drop_duplicates('product_id', keep='last')[['product_id', val_col]]
        df_new.rename(columns={val_col: rename_col_new}, inplace=True)
        
        # Old: Lấy giá trị cũ nhất (dòng đầu tiên của mỗi SP) 
        df_old = df_sorted.drop_duplicates('product_id', keep='first')[['product_id', val_col]]
        df_old.rename(columns={val_col: rename_col_old}, inplace=True)
        
        return df_new, df_old

    df_sales_new, df_sales_old = get_period_values(df_sales_agg, 'all_time_quantity_sold', 'qty_new', 'qty_old')
    df_price_new, df_price_old = get_period_values(df_price_agg, 'price', 'price_new', 'price_old')
    df_review_new, df_review_old = get_period_values(df_rating_agg, 'review_count', 'review_new', 'review_old')

    # --- MERGE TẤT CẢ VÀO DF_FINAL ---
    df_final = df_info.drop_duplicates(subset=['product_id'], keep='last').copy()
    
    dfs_to_merge = [df_sales_new, df_sales_old, df_price_new, df_price_old, 
                    df_review_new, df_review_old, df_rating_avg]
    
    for d in dfs_to_merge:
        df_final = pd.merge(df_final, d, on='product_id', how='left')

    # Fill NA 
    df_final['qty_old'] = df_final['qty_old'].fillna(0)
    df_final['qty_new'] = df_final['qty_new'].fillna(df_final['qty_old'])
    df_final['price_old'] = df_final['price_old'].fillna(df_final['price_new'])
    df_final['review_old'] = df_final['review_old'].fillna(0) # Review cũ chưa có coi như 0
    df_final['review_new'] = df_final['review_new'].fillna(df_final['review_old'])

    # --- TÍNH TOÁN CÔNG THỨC ---
    # 1. quantity_sold_in_period (Mục tiêu)
    df_final['quantity_sold_in_period'] = (df_final['qty_new'] - df_final['qty_old']).clip(lower=0)

    # 2. price_change_rate
    df_final['price_change_raw'] = np.where(
        df_final['price_old'] > 0,
        (df_final['price_new'] - df_final['price_old']) / df_final['price_old'],
        0
    )
    df_final['price_change_rate'] = (df_final['price_change_raw'] * 100).round(2)

    # 3. days_on_shelf (Tuổi đời SP trong kỳ)
    # Tính từ min_date của sales
    stats = df_sales_agg.groupby('product_id')['date'].agg(['min', 'max'])
    stats['days_on_shelf'] = (stats['max'] - stats['min']).dt.days + 1
    df_final = pd.merge(df_final, stats[['days_on_shelf']], on='product_id', how='left')
    df_final['days_on_shelf'] = df_final['days_on_shelf'].fillna(1) # Ít nhất 1 ngày

    # 4. review_growth_rate
    df_final['review_growth'] = df_final['review_new'] - df_final['review_old']
    df_final['review_growth_rate'] = (df_final['review_growth'] / df_final['days_on_shelf']).clip(lower=0).round(2)

    # 5. sales_per_day
    df_final['sales_per_day'] = (df_final['quantity_sold_in_period'] / df_final['days_on_shelf']).round(2)

    # 6. price_elasticity
    qty_change_rate = np.where(df_final['qty_old'] > 0,
                               df_final['quantity_sold_in_period'] / df_final['qty_old'], 0)
    
    df_final['price_elasticity'] = np.where(
        (df_final['price_change_raw'] != 0) & (df_final['qty_old'] > 0),
        qty_change_rate / df_final['price_change_raw'], 0
    )
    df_final['price_elasticity'] = df_final['price_elasticity'].replace([np.inf, -np.inf], 0).round(2)

    return df_final, df_sales_daily, df_sales_weekly

# 3. Giai đoạn LOAD (Lưu file)
def save_data_to_csv(df_final, df_daily, df_weekly, output_dir):
    print("\n--- ĐANG LƯU 5 FILE KẾT QUẢ ---")
    os.makedirs(output_dir, exist_ok=True)

    # FILE 1: Target Variable 
    target_cols = ['product_id', 'name', 'quantity_sold_in_period']
    df_final[target_cols].to_csv(f'{output_dir}/1_target_variable.csv', index=False)
    print(f"-> [1/5] Đã lưu: 1_target_variable.csv (Dùng làm nhãn y)")

    # FILE 2: Feature Engineering (Các chỉ số tĩnh dùng để train model)
    df_final.to_csv(f'{output_dir}/2_feature_engineering_static.csv', index=False)
    print(f"-> [2/5] Đã lưu: 2_feature_engineering_static.csv (Dùng làm Input X)")

    # FILE 3: Daily Sales (Chi tiết theo ngày)
    # Chỉ giữ các cột cần thiết cho Time Series
    daily_cols = ['product_id', 'date', 'daily_quantity_sold', 'week_start']
    df_daily_export = df_daily[daily_cols].merge(df_final[['product_id', 'name']], on='product_id', how='left')
    # Sắp xếp lại cột cho đẹp: ID -> Name -> Date -> ...
    df_daily_export = df_daily_export[['product_id', 'name', 'date', 'daily_quantity_sold', 'week_start']]
    
    df_daily_export.to_csv(f'{output_dir}/3_daily_sales.csv', index=False)
    print(f"-> [3/5] Đã lưu: 3_daily_sales.csv (Dùng vẽ biểu đồ ngày)")

    # FILE 4: Weekly Sales (Tổng hợp theo tuần)
    weekly_cols = ['product_id', 'week_start', 'weekly_quantity_sold']
    df_weekly_export = df_weekly[weekly_cols].merge(df_final[['product_id', 'name']], on='product_id', how='left')
    df_weekly_export = df_weekly_export[['product_id', 'name', 'week_start', 'weekly_quantity_sold']]
    
    df_weekly_export.to_csv(f'{output_dir}/4_weekly_sales.csv', index=False)
    print(f"-> [4/5] Đã lưu: 4_weekly_sales.csv (Dùng vẽ biểu đồ tuần)")

    # FILE 5: Full Combined (Daily + Weekly + Features)
    # B1: Lấy khung sườn là Daily
    df_full = df_daily[['product_id', 'date', 'daily_quantity_sold', 'week_start']].copy()
    
    # B2: Ghép số liệu Weekly vào từng ngày (Mỗi ngày sẽ biết tuần đó bán tổng bao nhiêu)
    df_full = df_full.merge(df_weekly[['product_id', 'week_start', 'weekly_quantity_sold']], 
                            on=['product_id', 'week_start'], 
                            how='left')
    
    # B3: Ghép Feature Tĩnh (Giá, Rating, Elasticity...)
    # Chọn các cột feature quan trọng từ df_final
    feature_cols = ['product_id', 'name', 'days_on_shelf', 'sales_per_day', 
                    'price_new', 'price_change_rate', 'price_elasticity', 
                    'review_growth_rate', 'rating_average', 'quantity_sold_in_period']
    
    df_full = df_full.merge(df_final[feature_cols], on='product_id', how='left')
    
    # Lưu file
    df_full.to_csv(f'{output_dir}/5_full_analysis_combined.csv', index=False)
    print(f"-> [5/5] Đã lưu: 5_full_analysis_combined.csv (Dùng cho Deep Learning/Sequence)")

    print(f"\n=> HOÀN TẤT! Dữ liệu nằm tại: {output_dir}")

# --- HÀM MAIN ---
def main():
    # 1. Extract
    sales, price, rating, info = extract_data_from_csv(DATA_DIR_PATH)
    if sales is not None:
        # 2. Transform
        final_df, daily_df, weekly_df = transform_data(sales, price, rating, info)
        # 3. Load
        save_data_to_csv(final_df, daily_df, weekly_df, OUTPUT_DIR)
if __name__ == "__main__":
    main()
