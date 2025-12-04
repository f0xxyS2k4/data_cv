import requests
from bs4 import BeautifulSoup
import json
import mysql.connector
import numpy as np
import time
import random
from typing import List, Dict, Union
import pandas as pd
import re

# --- Import các hàm xử lý dữ liệu (TRANSFORM) ---
# Đảm bảo các hàm Transform được import đầy đủ
try:
    from data_pipeline import process_salary, extract_location_pairs, standardize_title
except ImportError:
    # Fallback nếu data_pipeline.py không tìm thấy hoặc bị lỗi
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
# URL mục tiêu
TARGET_URL = "https://www.topcv.vn/tim-viec-lam-moi-nhat"
# Thiết lập giới hạn crawl tối đa
MAX_PAGES = 100


# --- HÀM KIỂM TRA LỌC (GIỮ NGUYÊN) ---
def is_recently_posted(time_raw: str) -> bool:
    """Kiểm tra xem tin tuyển dụng có phải là 'Đăng hôm nay' hoặc 'Đăng n ngày trước' không."""

    if not time_raw:
        return False

    cleaned_raw = time_raw.lower().replace(" ", "")

    if "đănghômnay" == cleaned_raw:
        return True

    if re.match(r'đăng\d+ngàytrước', cleaned_raw):
        return True

    return False


# --- 1. CRAWLING LOGIC SỬ DỤNG REQUESTS/BEAUTIFULSOUP (EXTRACT & PARTIAL TRANSFORM) ---

def crawl_topcv_job_listings_html() -> List[dict]:
    """
    Crawl dữ liệu từ TopCV, chỉ lấy các tin có nhãn 'Đăng hôm nay' hoặc 'Đăng n ngày trước'.
    Bắt đầu từ trang 1 cho quy trình tự động hóa hàng ngày.
    """
    all_jobs = []
    # !!! ĐIỀU CHỈNH: Bắt đầu lại từ trang 1 cho quy trình tự động hóa
    page = 1

    query_params = "sort=new&type_keyword=1&sba=1"

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
    }

    while True:
        if page > MAX_PAGES:
            print(f"⚠️ Giới hạn crawl đã đạt {MAX_PAGES} trang. Dừng vòng lặp.")
            break

        current_url = f"{TARGET_URL}?{query_params}&page={page}"
        print(f"\n--- Đang gửi GET Request cho Trang {page} tại URL: {current_url} ---")

        MAX_RETRIES = 3
        retry_count = 0
        response = None

        # --- LOGIC TÁI THỬ NGHIỆM (RETRY/BACKOFF) ---
        while retry_count < MAX_RETRIES:
            try:
                response = requests.get(current_url, headers=headers, timeout=20)
                response.raise_for_status()
                break

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    retry_count += 1
                    wait_time = 30 * retry_count
                    print(
                        f"⚠️ Lỗi 429 (Too Many Requests). Đang thử lại lần {retry_count}/{MAX_RETRIES} sau {wait_time} giây.")
                    time.sleep(wait_time)
                else:
                    print(f"❌ Lỗi Requests khi truy cập trang {page}: {e}. Dừng crawl.")
                    return all_jobs

            except requests.exceptions.RequestException as e:
                print(f"❌ Lỗi kết nối khi truy cập trang {page}: {e}. Dừng crawl.")
                return all_jobs

        if response is None or response.status_code != 200:
            print(f"❌ Thất bại sau {MAX_RETRIES} lần thử lại. Dừng crawl.")
            break

        # --- BẮT ĐẦU XỬ LÝ DỮ LIỆU ---
        soup = BeautifulSoup(response.content, 'html.parser')
        job_items = soup.select('.job-ta')

        if not job_items:
            print(f"✅ Hoàn thành crawl. Không còn tin tuyển dụng nào trên Trang {page}. Dừng vòng lặp.")
            break

        jobs_added_on_page = 0

        for item in job_items:
            try:
                time_raw = ''
                time_label = item.select_one('label.label-update')

                if time_label:
                    if 'data-original-title' in time_label.attrs:
                        time_raw = time_label['data-original-title'].strip()
                    elif not time_raw:
                        time_raw = time_label.get_text(strip=True)

                if is_recently_posted(time_raw):
                    job_title = 'N/A'
                    link_description = 'N/A'

                    img_tag = item.select_one('div.avatar a img')
                    if img_tag and 'title' in img_tag.attrs:
                        job_title = img_tag['title']

                    if job_title == 'N/A':
                        title_span_tag = item.select_one('h3.title span[data-original-title]')
                        if title_span_tag and 'data-original-title' in title_span_tag.attrs:
                            job_title = title_span_tag['data-original-title']

                    link_tag = item.select_one('h3.title a')
                    if link_tag and 'href' in link_tag.attrs:
                        link_description_raw = link_tag['href']
                        link_description = TOPCV_BASE_URL + link_description_raw if link_description_raw.startswith(
                            '/') else link_description_raw

                    company_tag = item.select_one('span.company-name')
                    company = company_tag.get_text(strip=True) if company_tag else 'N/A'

                    salary_tag = item.select_one('label.title-salary')
                    salary_raw = salary_tag.get_text(strip=True) if salary_tag else 'Thoả thuận'

                    address_tags = item.select('label.address')
                    address = 'N/A'
                    for tag in address_tags:
                        if 'label-update' not in tag.get('class', []):
                            address = tag.get_text(strip=True)
                            break

                    # 3. ÁP DỤNG CÁC HÀM TỪ data_pipeline (TRANSFORM)
                    avg, min_s, max_s, unit = process_salary(salary_raw)
                    locations = extract_location_pairs(address)

                    # !!! THAY ĐỔI: GỌI standardize_title
                    standardized_title_val = standardize_title(job_title)

                    # 4. Lưu dữ liệu
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
                            'standardized_job_title': standardized_title_val,  # <<< CỘT MỚI ĐÃ CHUẨN HÓA
                            'link_description': link_description
                        })
                    jobs_added_on_page += 1

                else:
                    display_time = time_raw if time_raw else "Không trích xuất được"
                    print(f"Bỏ qua tin tuyển dụng KHÔNG phải tin mới (Thời gian: {display_time}). Tiếp tục tìm kiếm.")
                    continue

            except Exception as e:
                print(f"Cảnh báo: Lỗi khi xử lý một mục công việc trên trang {page}: {e}. Tiếp tục.")
                continue

        print(
            f"Hoàn thành trích xuất Trang {page}. Đã thêm {jobs_added_on_page} công việc. Tổng số BẢN GHI (đã nhân bản): {len(all_jobs)}.")

        page += 1

        sleep_time = random.uniform(5, 12)
        print(f"Tạm dừng {sleep_time:.2f} giây trước khi chuyển sang trang tiếp theo.")
        time.sleep(sleep_time)

    return all_jobs


# --- 2. DATABASE LOGIC (LOAD) ---

def save_to_mysql(job_data: List[dict]):
    """Lưu dữ liệu công việc đã crawl vào MySQL. Sử dụng INSERT để tận dụng UNIQUE key."""
    if not job_data:
        print("Không có dữ liệu để lưu.")
        return

    conn = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # !!! THAY ĐỔI: Thêm standardized_job_title vào query
        insert_query = """
        INSERT INTO job_listings_clean (
            job_title, standardized_job_title, company, salary, min_salary, max_salary, 
            salary_unit, city, district, link_description
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        records_to_insert = []
        for job in job_data:
            salary_val = None if isinstance(job['salary'], float) and np.isnan(job['salary']) else job['salary']
            min_val = None if isinstance(job['min_salary'], float) and np.isnan(job['min_salary']) else job[
                'min_salary']
            max_val = None if isinstance(job['max_salary'], float) and np.isnan(job['max_salary']) else job[
                'max_salary']
            unit_val = None if (isinstance(job['salary_unit'], float) and np.isnan(job['salary_unit'])) else job[
                'salary_unit']

            job_title_val = job['job_title'] if job['job_title'] is not None else 'Unknown Title'
            company_val = job['company'] if job['company'] is not None else 'Unknown Company'
            standardized_title_val = job['standardized_job_title'] if job[
                                                                          'standardized_job_title'] is not None else 'Unknown'

            records_to_insert.append((
                job_title_val,
                standardized_title_val,  # <<< CỘT MỚI
                company_val,
                salary_val,
                min_val,
                max_val,
                unit_val,
                job['city'],
                job['district'],
                job['link_description']
            ))

        # SỬ DỤNG INSERT IGNORE ĐỂ BỎ QUA CÁC BẢN GHI TRÙNG LẶP (Dựa trên UNIQUE(link_description(255)))
        insert_ignore_query = insert_query.replace("INSERT INTO", "INSERT IGNORE INTO")

        cursor.executemany(insert_ignore_query, records_to_insert)
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
        f"Bắt đầu quy trình ETL TỰ ĐỘNG: Extract (Crawl) -> Transform -> Load vào MySQL.")

    new_jobs = crawl_topcv_job_listings_html()

    if new_jobs:
        try:
            df = pd.DataFrame(new_jobs)
            unique_links_count = df['link_description'].nunique()

            print("--- BÁO CÁO SỐ LƯỢNG DATA ---")
            print(f"Tổng số TIN TUYỂN DỤNG DUY NHẤT (trước khi Load): {unique_links_count}")
            print(f"Tổng số BẢN GHI (đã nhân bản theo địa điểm): {len(new_jobs)}")
            print("-----------------------------")
        except Exception as e:
            print(f"Cảnh báo: Không thể thống kê số lượng duy nhất bằng Pandas: {e}")

        save_to_mysql(new_jobs)
    else:
        print("Không có dữ liệu mới nào được thu thập.")