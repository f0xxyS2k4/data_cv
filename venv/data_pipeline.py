import pandas as pd
import re
import numpy as np
from sqlalchemy import create_engine

# =========================================================================
# 0. THÔNG TIN CẤU HÌNH CẦN THIẾT
# =========================================================================

# --- Cấu hình Extract ---
CSV_FILE_PATH = '~/Downloads/data.csv'  # VUI LÒNG ĐIỀN ĐƯỜNG DẪN TUYỆT ĐỐI CỦA BẠN VÀO ĐÂY

# --- Cấu hình Load (MySQL) ---
DB_USER = "root"  # !!! THAY THẾ TÊN USER MYSQL CỦA BẠN !!!
DB_PASSWORD = "123456"  # !!! THAY THẾ MẬT KHẨU MYSQL CỦA BẠN !!!
DB_HOST = "localhost"
DB_PORT = "3306"
DB_NAME = "data_pipeline_db"
TABLE_NAME = 'job_listings_clean'


# =========================================================================
# 1. KHÂU TRANSFORM (T) - CÁC HÀM XỬ LÝ DỮ LIỆU ĐÃ CẬP NHẬT
# =========================================================================

def process_salary(salary):
    """
    Chuẩn hóa cột salary. Nếu là "Thoả thuận", đặt giá trị số là 0 và unit là np.nan.
    """
    avg_s, min_s, max_s, unit = np.nan, np.nan, np.nan, np.nan

    if pd.isna(salary) or not isinstance(salary, str):
        return avg_s, min_s, max_s, avg_s

    salary_lower = salary.lower().replace('.', '').replace(',', '').strip()

    # 1. Xử lý "Thoả thuận" / "Negotiable" (ĐÃ CẬP NHẬT THEO YÊU CẦU MỚI)
    if 'thỏa thuận' in salary_lower or 'negotiable' in salary_lower:
        # Đặt giá trị số là 0.0 và unit là np.nan (sẽ được fill thành 'Unknown' sau)
        return 0.0, 0.0, 0.0, np.nan

        # --- Định nghĩa các đơn vị và hàm chuyển đổi ---
    SALARY_PATTERN = r'(\d+)\s*(triệu|tr|k|nghìn|usd|\$)'

    def convert_value_and_set_unit(num, raw_unit):
        """ Chuyển đổi giá trị và thiết lập đơn vị chuẩn """
        if raw_unit in ['triệu', 'tr']:
            return num, 'VND'
        elif raw_unit in ['k', 'nghìn']:
            return num / 1000, 'VND'  # Chuyển từ Nghìn -> Triệu VND
        elif raw_unit in ['usd', '$']:
            return num, 'USD'
        return num, np.nan

    # 2. Xử lý Range (X - Y)
    match_range = re.search(r'(\d+)\s*-\s*(\d+)\s*(' + SALARY_PATTERN.split('(')[-1], salary_lower)
    if match_range:
        num1 = float(match_range.group(1))
        num2 = float(match_range.group(2))
        raw_unit = match_range.group(3)

        min_s, unit_min = convert_value_and_set_unit(num1, raw_unit)
        max_s, unit_max = convert_value_and_set_unit(num2, raw_unit)
        unit = unit_min

        if not pd.isna(min_s) and not pd.isna(max_s):
            avg_s = (min_s + max_s) / 2
        return avg_s, min_s, max_s, unit

    # 3. Xử lý Single/Threshold (Trên X, Tới X)
    match_threshold = re.search(r'(trên|above|từ|tới|up\s*to)\s*(\d+)\s*(triệu|tr|k|nghìn|usd|\$)', salary_lower)
    if match_threshold:
        keyword = match_threshold.group(1)
        num = float(match_threshold.group(2))
        raw_unit = match_threshold.group(3)

        val, unit = convert_value_and_set_unit(num, raw_unit)

        if 'trên' in keyword or 'từ' in keyword or 'above' in keyword:
            min_s = val
            avg_s = val
        elif 'tới' in keyword or 'up to' in keyword:
            max_s = val
            avg_s = val

        return avg_s, min_s, max_s, unit

    # 4. Fallback cho giá trị đơn
    match_simple = re.search(r'(\d+)\s*(triệu|tr|k|nghìn|usd|\$)', salary_lower)
    if match_simple:
        num = float(match_simple.group(1))
        raw_unit = match_simple.group(2)

        val, unit = convert_value_and_set_unit(num, raw_unit)

        min_s, max_s, avg_s = val, val, val

        return avg_s, min_s, max_s, unit

        # Trường hợp không thể phân tích được
    return avg_s, min_s, max_s, avg_s


def standardize_title(job_title):
    """ Gom nhóm tiêu đề công việc. """
    if pd.isna(job_title):
        return 'Unknown'

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
    """ Trích xuất TẤT CẢ các cặp (City, District), dựa vào cấu trúc City:District. """
    if pd.isna(address) or not isinstance(address, str):
        return [('Unknown', None)]

    address_lower = address.lower()

    # 1. Xử lý các trường hợp đặc biệt
    if any(keyword in address_lower for keyword in ['toàn quốc', 'vietnam', 'viet nam']):
        return [('Toàn Quốc', None)]
    if any(keyword in address_lower for keyword in ['nước ngoài', 'oversea', 'global']):
        return [('Nước Ngoài', None)]
    if any(keyword in address_lower for keyword in ['nhiều địa điểm', 'multi-location']):
        return [('Multi-location', None)]

    # 2. Tách chuỗi theo dấu ':' và làm sạch
    parts = [p.strip() for p in address.split(':') if p.strip()]
    location_pairs = []

    # 3. Duyệt qua các phần tử, giả định cứ 2 phần tử là 1 cặp City:District
    i = 0
    while i < len(parts):
        raw_city = parts[i]
        city = raw_city.title()
        district = parts[i + 1] if (i + 1 < len(parts)) else None

        location_pairs.append((city, district))
        i += 2

    # 4. Nếu không trích xuất được cặp nào
    return location_pairs if location_pairs else [('Unknown', None)]


# =========================================================================
# 2. HÀM CHÍNH - THỰC HIỆN ETL
# =========================================================================

def run_etl_pipeline():
    """ Thực hiện toàn bộ quá trình Extract, Transform, Load. """

    # --------------------------
    # 2.1. KHÂU EXTRACT (E)
    # --------------------------
    print(f"[{'=' * 5} EXTRACT {'=' * 5}] Bắt đầu đọc dữ liệu...")
    try:
        df = pd.read_csv(CSV_FILE_PATH, encoding='utf-8')
        print(f"[E] Đã đọc thành công {len(df)} dòng dữ liệu từ {CSV_FILE_PATH}")
    except FileNotFoundError:
        print(f"[ERROR] KHÔNG TÌM THẤY file CSV: {CSV_FILE_PATH}. Dừng pipeline.")
        return
    except Exception as e:
        print(f"[ERROR] Lỗi đọc CSV: {e}. Dừng pipeline.")
        return

    # --------------------------
    # 2.2. KHÂU TRANSFORM (T)
    # --------------------------
    print(f"\n[{'=' * 5} TRANSFORM {'=' * 5}] Bắt đầu làm sạch và chuẩn hóa...")

    # T1: Xử lý cột salary (Tạo 4 cột mới: salary, min, max, unit)
    print("[T] Xử lý và chuẩn hóa Lương...")

    # 1. Áp dụng hàm xử lý lên cột salary thô và chuyển thành list
    salary_processed = df['salary'].apply(lambda x: process_salary(x)).tolist()

    # 2. TÁCH KẾT QUẢ VÀO CÁC DANH SÁCH RIÊNG BIỆT
    salaries = [res[0] for res in salary_processed]
    min_salaries = [res[1] for res in salary_processed]
    max_salaries = [res[2] for res in salary_processed]
    salary_units = [res[3] for res in salary_processed]  # Bao gồm np.nan cho cả 'Thoả thuận' và unparseable

    # 3. GÁN CÁC CỘT MỚI

    # Loại bỏ cột salary thô
    df = df.drop(columns=['salary'])

    # Gán cột đơn vị
    df['salary_unit'] = salary_units

    # Gán các cột số
    df['salary'] = salaries
    df['min_salary'] = min_salaries
    df['max_salary'] = max_salaries

    # 4. Chuẩn hóa các cột

    # Chuẩn hóa cột đơn vị (salary_unit) - Giờ chỉ còn np.nan -> Unknown
    is_missing = df['salary_unit'].isnull()

    # Thay thế tất cả các giá trị thiếu (bao gồm cả 'Thoả thuận' theo rule mới) thành 'Unknown'
    df.loc[is_missing, 'salary_unit'] = 'Unknown'

    # Ép kiểu sang str cho MySQL
    df['salary_unit'] = df['salary_unit'].astype(str)

    # Chuẩn hóa các cột số (salary, min_salary, max_salary)
    numeric_cols = ['salary', 'min_salary', 'max_salary']
    for col in numeric_cols:
        # astype(float) sẽ xử lý np.nan thành NULL khi tải lên MySQL
        df[col] = df[col].astype(float)

    # T2: Xử lý cột job_title
    df['standardized_job_title'] = df['job_title'].apply(standardize_title)

    # T3: Trích xuất các cặp (city, district)
    print("[T] Trích xuất tất cả các địa điểm (City, District) vào List...")
    df['location_pairs'] = df['address'].apply(extract_location_pairs)

    # T4 & T5: Tách dòng dữ liệu (explode) và tách cặp (city, district)
    print("[T] Tách dòng dữ liệu cho các công việc đa địa điểm (Explode)...")
    df_exploded = df.explode('location_pairs').reset_index(drop=True)

    df_exploded[['city', 'district']] = pd.DataFrame(
        df_exploded['location_pairs'].tolist(),
        index=df_exploded.index
    )

    # T6: Chọn các cột cuối cùng theo yêu cầu
    df_clean = df_exploded[[
        'created_date',
        'company',
        'salary',
        'min_salary',
        'max_salary',
        'salary_unit',
        'city',
        'district',
        'standardized_job_title',
        'link_description'
    ]].copy()

    # Chuẩn hóa các cột string cuối cùng
    df_clean['city'] = df_clean['city'].astype(str)
    df_clean['district'] = df_clean['district'].fillna('Unknown').astype(str)

    print(f"[T] Hoàn tất Transform. Dữ liệu sạch có {len(df_clean)} dòng (Đã tăng số dòng do Explode).")

    # --------------------------
    # 2.3. KHÂU LOAD (L)
    # --------------------------
    print(f"\n[{'=' * 5} LOAD {'=' * 5}] Bắt đầu tải dữ liệu vào MySQL...")

    # Tạo Engine Kết nối
    DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

    try:
        engine = create_engine(DATABASE_URL)

        # Tải dữ liệu vào bảng
        df_clean.to_sql(
            name=TABLE_NAME,
            con=engine,
            if_exists='replace',
            index=False
        )

        print(f"[L] Đã Tải thành công {len(df_clean)} dòng dữ liệu vào MySQL Bảng: {TABLE_NAME}")

    except Exception as e:
        print(f"[ERROR] Lỗi kết nối hoặc tải dữ liệu vào MySQL: {e}")
        print("[L] Load thất bại. Kiểm tra lại thông tin DB (tên DB phải được tạo sẵn) và thông tin đăng nhập.")


if __name__ == "__main__":
    run_etl_pipeline()