import requests
from bs4 import BeautifulSoup
import json
import mysql.connector
import numpy as np
import time
import random
from typing import List, Dict, Union
import pandas as pd

# --- Import các hàm xử lý dữ liệu (TRANSFORM) ---
# Chúng ta vẫn cần các hàm xử lý Lương và Địa điểm, chỉ bỏ qua standardize_title
try:
    from data_pipeline import process_salary, extract_location_pairs


    # Tự định nghĩa hàm giả standardize_title để tránh lỗi nếu nó được gọi.
    def standardize_title(t):
        return None  # Trả về None/NoneType để không chèn vào DB
except ImportError:
    # Hàm giả định nếu không tìm thấy file
    def process_salary(s):
        return np.nan, np.nan, np.nan, 'Unknown_Unit'


    def extract_location_pairs(a):
        return [('Unknown', None)]


    def standardize_title(t):
        return None

# Cấu hình Database MySQL (Giữ nguyên)
DB_CONFIG = {
    'user': 'root',
    'password': '123456',
    'host': 'localhost',
    'database': 'data_pipeline_db'
}

TOPCV_BASE_URL = "https://www.topcv.vn"
TARGET_URL = "https://www.topcv.vn/viec-lam-tot-nhat"
MAX_PAGES = 10


# --- 1. CRAWLING LOGIC SỬ DỤNG REQUESTS/BEAUTIFULSOUP (EXTRACT) ---

def crawl_topcv_job_listings_html(max_pages: int) -> List[dict]:
    """Crawl dữ liệu từ TopCV sử dụng Requests/GET và phân tích HTML bằng BeautifulSoup."""
    all_jobs = []

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
    }

    for page in range(1, max_pages + 1):
        current_url = f"{TARGET_URL}?page={page}"
        print(f"\n--- Đang gửi GET Request cho Trang {page} tại URL: {current_url} ---")

        try:
            response = requests.get(current_url, headers=headers, timeout=15)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"❌ Lỗi Requests khi truy cập trang {page}: {e}")
            break

        soup = BeautifulSoup(response.content, 'html.parser')
        job_items = soup.select('.job-ta')

        if not job_items:
            if page > 1:
                print(f"Hoàn thành trích xuất trang {page - 1}. Không còn tin tuyển dụng mới. Dừng crawl.")
            else:
                print(f"⚠️ Cảnh báo: Không tìm thấy tin tuyển dụng nào trên trang {page}. Dừng crawl.")
            break

        for item in job_items:
            try:

                # --- TRÍCH XUẤT TIÊU ĐỀ (JOB TITLE) & LINK ---
                job_title = 'N/A'
                link_description = 'N/A'

                # PHƯƠNG ÁN 1: Lấy từ thuộc tính title của thẻ <img>
                img_tag = item.select_one('div.avatar a img')
                if img_tag and 'title' in img_tag.attrs:
                    job_title = img_tag['title']

                # PHƯƠNG ÁN 2 (Dự phòng): Lấy từ thuộc tính data-original-title
                if job_title == 'N/A':
                    title_span_tag = item.select_one('h3.title span[data-original-title]')
                    if title_span_tag and 'data-original-title' in title_span_tag.attrs:
                        job_title = title_span_tag['data-original-title']

                # Link mô tả đầy đủ
                link_tag = item.select_one('h3.title a')
                if link_tag and 'href' in link_tag.attrs:
                    link_description_raw = link_tag['href']
                    link_description = TOPCV_BASE_URL + link_description_raw if link_description_raw.startswith(
                        '/') else link_description_raw

                # --- TRÍCH XUẤT TÊN CÔNG TY ---
                company_tag = item.select_one('span.company-name')
                company = company_tag.get_text(strip=True) if company_tag else 'N/A'

                # --- TRÍCH XUẤT MỨC LƯƠNG (SALARY) ---
                salary_tag = item.select_one('label.title-salary')
                salary_raw = salary_tag.get_text(strip=True) if salary_tag else 'Thoả thuận'

                # --- TRÍCH XUẤT ĐỊA ĐIỂM (ADDRESS/CITY) ---
                address_tag = item.select_one('label.address')
                address = address_tag.get_text(strip=True) if address_tag else 'N/A'

                # 3. ÁP DỤNG CÁC HÀM TỪ data_pipeline (TRANSFORM)
                avg, min_s, max_s, unit = process_salary(salary_raw)
                # BỎ QUA standardize_title theo yêu cầu
                # std_title = standardize_title(job_title)
                locations = extract_location_pairs(address)

                # 4. Lưu dữ liệu thô và đã xử lý
                for city, district in locations:
                    all_jobs.append({
                        'job_title': job_title,
                        'company': company,
                        'salary': avg,
                        'min_salary': min_s,
                        'max_salary': max_s,
                        'salary_unit': unit,
                        'city': city,
                        'district': district,
                        # LOẠI BỎ 'standardized_job_title': std_title,
                        'link_description': link_description
                    })
            except Exception as e:
                print(
                    f"Cảnh báo: Lỗi khi xử lý một mục công việc trên trang {page}: {e}. (HTML item might be corrupted)")
                continue

        sleep_time = random.uniform(2, 4)
        print(
            f"Hoàn thành trích xuất trang {page}. Tổng số công việc tạm thời: {len(all_jobs)}. Tạm dừng {sleep_time:.2f} giây.")
        time.sleep(sleep_time)

    return all_jobs


# --- 2. DATABASE LOGIC (LOAD) ---

def save_to_mysql(job_data: List[dict]):
    if not job_data:
        print("Không có dữ liệu để lưu.")
        return

    conn = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # ✅ LOẠI BỎ 'standardized_job_title' KHỎI CÂU LỆNH INSERT
        insert_query = """
        INSERT INTO job_listings_clean (
            job_title, company, salary, min_salary, max_salary, 
            salary_unit, city, district, link_description
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        records_to_insert = []
        for job in job_data:
            # Xử lý giá trị NaN/None cho database
            salary_val = None if isinstance(job['salary'], float) and np.isnan(job['salary']) else job['salary']
            min_val = None if isinstance(job['min_salary'], float) and np.isnan(job['min_salary']) else job[
                'min_salary']
            max_val = None if isinstance(job['max_salary'], float) and np.isnan(job['max_salary']) else job[
                'max_salary']
            unit_val = None if (isinstance(job['salary_unit'], float) and np.isnan(job['salary_unit'])) else job[
                'salary_unit']

            job_title_val = job['job_title'] if job['job_title'] is not None else 'Unknown Title'
            company_val = job['company'] if job['company'] is not None else 'Unknown Company'

            # ✅ LOẠI BỎ 'standardized_job_title' KHỎI DỮ LIỆU CHÈN
            records_to_insert.append((
                job_title_val,
                company_val,
                salary_val,
                min_val,
                max_val,
                unit_val,
                job['city'],
                job['district'],
                job['link_description']
            ))

        cursor.executemany(insert_query, records_to_insert)
        conn.commit()
        print(f"\n✅ Đã lưu thành công {cursor.rowcount} bản ghi mới vào MySQL.")

    except mysql.connector.Error as err:
        print(f"\n❌ Lỗi MySQL: {err}")
    except Exception as e:
        print(f"\n❌ Lỗi chung khi kết nối database: {e}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
            print("Đã đóng kết nối MySQL.")


# --- 3. MAIN EXECUTION ---

if __name__ == "__main__":
    print(
        f"Bắt đầu quy trình ETL: Extract dữ liệu mới từ TopCV ({TARGET_URL}) và Load vào MySQL (CHỈ SỬ DỤNG JOB TITLE GỐC).")

    new_jobs = crawl_topcv_job_listings_html(MAX_PAGES)

    if new_jobs:
        print(f"Tổng số lượng bản ghi đã crawl và xử lý: {len(new_jobs)}")
        save_to_mysql(new_jobs)
    else:
        print("Không có dữ liệu mới nào được thu thập.")