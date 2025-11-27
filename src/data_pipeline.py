import pandas as pd
import re
import numpy as np
from sqlalchemy import create_engine
import os
# Cấu hình Extract

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FILE_PATH = os.path.join(BASE_DIR, '..', 'data', 'data.csv') # <<< ĐÃ SỬA LỖI ĐƯỜNG DẪN

# Cấu hình
DB_USER = "root"
DB_PASSWORD = "123456"
DB_HOST = "localhost"
DB_PORT = "3306"
DB_NAME = "data_pipeline_db"
TABLE_NAME = 'job_listings_clean'



# 1. TRANSFORM


def process_salary(salary):
    """
    Chuẩn hóa cột salary.
    """
    avg_s, min_s, max_s, unit = np.nan, np.nan, np.nan, np.nan

    if pd.isna(salary) or not isinstance(salary, str):
        return avg_s, min_s, max_s, avg_s

    salary_lower = salary.lower().replace('.', '').replace(',', '').strip()

    def convert_value_and_set_unit(num, raw_unit):
        """ Chuyển đổi giá trị và thiết lập đơn vị chuẩn """
        if raw_unit in ['triệu', 'tr']:
            return num, 'VND'
        elif raw_unit in ['k', 'nghìn']:
            return num / 1000, 'VND'  # Chuyển từ Nghìn -> Triệu VND
        elif raw_unit in ['usd', '$']:
            return num, 'USD'
        return num, np.nan

    # 1. Xử lý "Thoả thuận" / "Negotiable"
    if 'thoả thuận' in salary_lower or 'thỏa thuận' in salary_lower or 'negotiable' in salary_lower:
        return 0.0, 0.0, 0.0, 'Thoả thuận'

    # 2. Xử lý Range (X - Y)
    match_range = re.search(r'(\d+)\s*-\s*(\d+)\s*(triệu|tr|k|nghìn|usd|\$)', salary_lower)
    if match_range:
        num1 = float(match_range.group(1));
        num2 = float(match_range.group(2));
        raw_unit = match_range.group(3)
        min_s, unit_min = convert_value_and_set_unit(num1, raw_unit)
        max_s, unit_max = convert_value_and_set_unit(num2, raw_unit)
        unit = unit_min
        if not pd.isna(min_s) and not pd.isna(max_s): avg_s = (min_s + max_s) / 2
        return avg_s, min_s, max_s, unit

    # 3. Xử lý Single/Threshold (Trên X, Tới X)
    match_threshold = re.search(r'(trên|above|từ|tới|up\s*to)\s*(\d+)\s*(triệu|tr|k|nghìn|usd|\$)', salary_lower)
    if match_threshold:
        keyword = match_threshold.group(1);
        num = float(match_threshold.group(2));
        raw_unit = match_threshold.group(3)
        val, unit = convert_value_and_set_unit(num, raw_unit)
        if 'trên' in keyword or 'từ' in keyword or 'above' in keyword:
            min_s = val; avg_s = val
        elif 'tới' in keyword or 'up to' in keyword:
            max_s = val; avg_s = val
        return avg_s, min_s, max_s, unit

    # 4. Fallback cho giá trị đơn
    match_simple = re.search(r'(\d+)\s*(triệu|tr|k|nghìn|usd|\$)', salary_lower)
    if match_simple:
        num = float(match_simple.group(1));
        raw_unit = match_simple.group(2)
        val, unit = convert_value_and_set_unit(num, raw_unit)
        min_s, max_s, avg_s = val, val, val
        return avg_s, min_s, max_s, unit

    return avg_s, min_s, max_s, avg_s


def standardize_title(job_title):
    """ Gom nhóm tiêu đề công việc. """
    if pd.isna(job_title): return 'Unknown'
    title = job_title.lower()
    if 'data analyst' in title or 'business analyst' in title or 'phân tích dữ liệu' in title or 'ba' == title.strip():
        return 'Data/Business Analyst'
    elif 'software engineer' in title or 'developer' in title or 'lập trình viên' in title or 'dev' in title or 'programmer' in title or 'dotnet' in title:
        return 'Software Developer'
    elif 'data engineer' in title or 'kỹ sư dữ liệu' in title or 'etl' in title or 'system engineer' in title:
        return 'Data/System Engineer'
    elif 'tester' in title or 'qa' in title or 'qc' in title:
        return 'QA/Tester'
    elif 'manager' in title or 'pm' in title or 'project lead' in title:
        return 'Management/Lead'
    else:
        return 'Other IT Role'


def extract_location_pairs(address):
    """ Trích xuất TẤT CẢ các cặp (City, District). """
    if pd.isna(address) or not isinstance(address, str): return [('Unknown', None)]

    address_lower = address.lower()

    # Xử lý các trường hợp đặc biệt
    if any(keyword in address_lower for keyword in ['toàn quốc', 'vietnam', 'viet nam']):
        return [('Toàn Quốc', None)]
    if any(keyword in address_lower for keyword in ['nước ngoài', 'oversea', 'global']):
        return [('Nước Ngoài', None)]
    if any(keyword in address_lower for keyword in ['nhiều địa điểm', 'multi-location']):
        return [('Multi-location', None)]

    # Tách chuỗi theo dấu ':'
    parts = [p.strip() for p in address.split(':') if p.strip()]
    location_pairs = []

    # Duyệt qua các phần tử, giả định cứ 2 phần tử là 1 cặp City:District
    i = 0
    while i < len(parts):
        city = parts[i].title()
        district = parts[i + 1] if (i + 1 < len(parts)) else None
        location_pairs.append((city, district))
        i += 2

    return location_pairs if location_pairs else [('Unknown', None)]



# 2. ETL


def run_etl_pipeline():
    """ Thực hiện toàn bộ quá trình Extract, Transform, Load. """

    df = None


# EXTRACT (E)

    print(f"[{'=' * 5} EXTRACT {'=' * 5}] Bắt đầu đọc dữ liệu...")
    try:
        df = pd.read_csv(CSV_FILE_PATH, encoding='utf-8')
        print(f"[E] Đã đọc thành công {len(df)} dòng dữ liệu từ {CSV_FILE_PATH}")
    except FileNotFoundError:
        print(f"[ERROR - E] KHÔNG TÌM THẤY file CSV: {CSV_FILE_PATH}. Dừng pipeline.")
        return
    except Exception as e:
        print(f"[ERROR - E] Lỗi đọc CSV: {e}. Dừng pipeline.")
        return


# TRANSFORM (T)

    print(f"\n[{'=' * 5} TRANSFORM {'=' * 5}] Bắt đầu làm sạch và chuẩn hóa...")
    df_clean = pd.DataFrame()
    try:
        # T1: Xử lý cột salary
        salary_processed = df['salary'].apply(lambda x: process_salary(x)).tolist()
        df = df.drop(columns=['salary'])

        df['salary_unit'] = [res[3] for res in salary_processed]
        df['salary'] = [res[0] for res in salary_processed]
        df['min_salary'] = [res[1] for res in salary_processed]
        df['max_salary'] = [res[2] for res in salary_processed]

        df['salary_unit'] = df['salary_unit'].fillna('Unknown').astype(str)

        # T2: Xử lý cột job_title
        df['standardized_job_title'] = df['job_title'].apply(standardize_title)

        # T3: Trích xuất các cặp (city, district)
        df['location_pairs'] = df['address'].apply(extract_location_pairs)

        # T4: Tách dòng dữ liệu (explode)
        df_exploded = df.explode('location_pairs').reset_index(drop=True)
        df_exploded[['city', 'district']] = pd.DataFrame(df_exploded['location_pairs'].tolist(),
                                                         index=df_exploded.index)

        # T5: Chọn các cột cuối cùng (ĐÃ THÊM CỘT job_title GỐC)
        df_clean = df_exploded[[
            'job_title',  # <<< ĐÃ THÊM CỘT NÀY CHO PHÂN TÍCH XU HƯỚNG CÔNG NGHỆ
            'company', 'salary', 'min_salary', 'max_salary', 'salary_unit', 'city',
            'district', 'standardized_job_title', 'link_description'
        ]].copy()

        df_clean['city'] = df_clean['city'].astype(str)
        df_clean['district'] = df_clean['district'].fillna('Unknown').astype(str)

        print(f"[T] Hoàn tất Transform. Dữ liệu sạch có {len(df_clean)} dòng.")

    except Exception as e:
        print(f"[ERROR - T] Lỗi trong quá trình Transform dữ liệu: {e}. Dừng pipeline.")
        return


# LOAD (L)

    print(f"\n[{'=' * 5} LOAD {'=' * 5}] Bắt đầu tải dữ liệu vào MySQL...")

    DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

    try:
        engine = create_engine(DATABASE_URL)

        # Sử dụng 'replace' để tạo lại bảng, đảm bảo cột job_title được thêm vào schema
        df_clean.to_sql(
            name=TABLE_NAME,
            con=engine,
            if_exists='replace',
            index=False
        )
        print(f"[L] Đã Tải thành công {len(df_clean)} dòng dữ liệu vào MySQL Bảng: {TABLE_NAME}")

    except Exception as e:
        print(f"Lỗi kết nối hoặc tải dữ liệu vào MySQL: {e}")
        print("Load thất bại. Kiểm tra lại thông tin DB và thông tin đăng nhập.")
        return

    print("\n PIPELINE ETL (E, T, L) ĐÃ HOÀN THIỆN!")


if __name__ == "__main__":
    run_etl_pipeline()