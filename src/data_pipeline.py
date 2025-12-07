import pandas as pd
import re
import numpy as np
from typing import Tuple, List, Optional

# --- CẤU HÌNH DATABASE DÙNG CHUNG ---
DB_CONFIG = {
    'user': 'root',
    'password': '123456',
    'host': 'localhost',
    'database': 'data_pipeline_db'
}


# 1. TRANSFORM


def process_salary(salary):
    """
    Chuẩn hóa cột salary (Mức lương tính bằng Triệu VND hoặc USD).
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
            min_s = val;
            avg_s = val
        elif 'tới' in keyword or 'up to' in keyword:
            max_s = val;
            avg_s = val
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
    if pd.isna(job_title) or job_title is None: return 'Unknown'
    title = job_title.lower()
    if 'data analyst' in title or 'business analyst' in title or 'phân tích dữ liệu' in title or 'ba' == title.strip():
        return 'Data/Business Analyst'
    elif 'data engineer' in title or 'kỹ sư dữ liệu' in title or 'etl' in title or 'system engineer' in title:
        return 'Data/System Engineer'  # <<< CẦN THIẾT CHO BÁO CÁO DISCORD
    elif 'software engineer' in title or 'developer' in title or 'lập trình viên' in title or 'dev' in title or 'programmer' in title or 'dotnet' in title:
        return 'Software Developer'
    elif 'tester' in title or 'qa' in title or 'qc' in title:
        return 'QA/Tester'
    elif 'manager' in title or 'pm' in title or 'project lead' in title:
        return 'Management/Lead'
    elif 'kinh doanh' in title or 'sales' in title or 'bán hàng' in title:
        return 'Business/Sales'
    elif 'kế toán' in title or 'accounting' in title or 'kiểm toán' in title:
        return 'Accounting/Auditing'
    else:
        return 'Other IT Role'


def extract_location_pairs(address) -> List[Tuple[Optional[str], Optional[str]]]:
    """ Trích xuất TẤT CẢ các cặp (City, District). """
    # ... (Logic giữ nguyên)
    if pd.isna(address) or not isinstance(address, str) or address.lower() == 'n/a':
        return [('Unknown', None)]

    address_lower = address.lower()

    if any(keyword in address_lower for keyword in ['toàn quốc', 'vietnam', 'viet nam']):
        return [('Toàn Quốc', None)]
    if any(keyword in address_lower for keyword in ['nước ngoài', 'oversea', 'global']):
        return [('Nước Ngoài', None)]
    if any(keyword in address_lower for keyword in ['nhiều địa điểm', 'multi-location']):
        return [('Multi-location', None)]

    parts = [p.strip() for p in re.split(r'[,;-]', address) if p.strip()]
    location_pairs = []

    for part in parts:
        if ':' in part:
            city, district = [p.strip().title() for p in part.split(':', 1)]
            location_pairs.append((city, district if district else None))
        else:
            if 'hà nội' in part.lower():
                location_pairs.append(('Hà Nội', part.replace('Hà Nội', '').strip().title() if len(part) > 6 else None))
            elif 'tp.hcm' in part.lower() or 'hồ chí minh' in part.lower():
                location_pairs.append(('TP.HCM',
                                       part.replace('TP.HCM', '').replace('Hồ Chí Minh', '').strip().title() if len(
                                           part) > 7 else None))
            else:
                location_pairs.append((part.title(), None))

    valid_pairs = [p for p in location_pairs if p[0] != 'Unknown']

    return valid_pairs if valid_pairs else [('Unknown', None)]

# Không cần hàm run_etl_pipeline và __main__ ở đây nữa