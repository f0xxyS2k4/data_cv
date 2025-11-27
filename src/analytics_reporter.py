# analytics_reporter.py

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
import re
import os

# =========================================================================
# 0. THÔNG TIN CẤU HÌNH CẦN THIẾT
# =========================================================================

# --- Cấu hình Load (MySQL) ---
# PHẢI KHỚP VỚI data_pipeline.py
DB_USER = "root"
DB_PASSWORD = "123456"
DB_HOST = "localhost"
DB_PORT = "3306"
DB_NAME = "data_pipeline_db"
TABLE_NAME = 'job_listings_clean'

# --- CẤU HÌNH ĐƯỜNG DẪN REPORT (ĐÃ TỔ CHỨC LẠI) ---
# REPORTS_DIR trỏ tới thư mục cha (..) và thư mục reports
REPORTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'reports')


# =========================================================================
# 1. HÀM CHÍNH - TRUY VẤN VÀ PHÂN TÍCH
# =========================================================================

def run_analytics():
    """ Kết nối DB, truy vấn dữ liệu sạch, và tạo báo cáo trực quan hóa. """
    print("\n=============================================")
    print("BẮT ĐẦU CHẠY BÁO CÁO PHÂN TÍCH")
    print("=============================================")

    # 1. KẾT NỐI VÀ TRUY VẤN DỮ LIỆU SẠCH TỪ DB
    DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    try:
        engine = create_engine(DATABASE_URL)

        # Truy vấn TẤT CẢ dữ liệu đã được làm sạch
        query = f"SELECT * FROM {TABLE_NAME}"
        df = pd.read_sql(query, con=engine)

        print(f"[ANALYTICS] Đã truy vấn thành công {len(df)} dòng dữ liệu từ MySQL.")

    except Exception as e:
        print(f"[ERROR] Lỗi truy vấn Database: {e}. Đảm bảo pipeline ETL đã chạy thành công trước đó.")
        return

    # --- ĐẢM BẢO THƯ MỤC REPORTS TỒN TẠI TRƯỚC KHI LƯU FILE ---
    os.makedirs(REPORTS_DIR, exist_ok=True)

    # 2. VẼ BIỂU ĐỒ

    # ----------------------------------------------------
    # 2.1. BIỂU ĐỒ CỘT SO SÁNH MỨC LƯƠNG TRUNG BÌNH THEO VỊ TRÍ
    # ----------------------------------------------------
    print("1. Tạo biểu đồ CỘT so sánh mức lương TRUNG BÌNH theo vị trí...")

    df_salary = df[
        (df['standardized_job_title'].isin(
            ['Data/Business Analyst', 'Software Developer', 'Data/System Engineer', 'QA/Tester', 'Management/Lead'])) &
        (df['salary_unit'] == 'VND') &
        (df['salary'] > 0)  # Loại bỏ các công việc 'Thoả thuận'
        ]

    if df_salary.empty:
        print("[CẢNH BÁO] Biểu đồ Lương: Dữ liệu sau khi lọc rỗng. Bỏ qua Bar Plot.")
    else:
        # TÍNH TOÁN LƯƠNG TRUNG BÌNH THEO TỪNG VỊ TRÍ
        avg_salary_df = df_salary.groupby('standardized_job_title')['salary'].mean().reset_index(name='avg_salary')
        avg_salary_df = avg_salary_df.sort_values(by='avg_salary', ascending=False)

        plt.figure(figsize=(10, 6))

        # SỬ DỤNG SEABORN BARPLOT (ĐÃ TỐI ƯU CẢNH BÁO HUE/LEGEND)
        sns.barplot(
            x='standardized_job_title',
            y='avg_salary',
            hue='standardized_job_title',
            data=avg_salary_df,
            palette='viridis',
            legend=False
        )

        plt.title('Mức Lương TRUNG BÌNH (VND - Triệu/tháng) theo Vị Trí')
        plt.xlabel('Vị Trí Công Việc')
        plt.ylabel('Mức Lương Trung Bình (Triệu VND)')
        plt.xticks(rotation=45, ha='right')
        plt.grid(axis='y', alpha=0.5)
        plt.tight_layout()

        # --- LƯU FILE VÀO REPORTS/ ---
        file_path = os.path.join(REPORTS_DIR, '1_average_salary_bar_plot.png')
        plt.savefig(file_path)
        plt.close()

    # ----------------------------------------------------
    # 2.2. BẢN ĐỒ NHIỆT (HEATMAP) PHÂN BỐ VIỆC LÀM THEO KHU VỰC
    # ----------------------------------------------------
    print("2. Tạo Bản đồ nhiệt phân bố việc làm theo THÀNH PHỐ...")

    city_counts = df[
        ~df['city'].isin(['Unknown', 'Toàn Quốc', 'Multi-location', 'Nước Ngoài'])
    ].groupby('city').size().reset_index(name='count')

    city_counts = city_counts.sort_values(by='count', ascending=False)

    pivot_table_city = city_counts.set_index('city')[['count']]

    plt.figure(figsize=(6, len(city_counts) * 0.8))

    # SỬ DỤNG SEABORN HEATMAP
    sns.heatmap(
        pivot_table_city,
        annot=True,
        fmt="d",
        cmap="YlGnBu",
        linewidths=.5,
        linecolor='black',
        cbar_kws={'label': 'Số lượng tin tuyển dụng'},
        annot_kws={"size": 12, "weight": "bold"}
    )

    plt.title('Bản Đồ Nhiệt Phân Bố Việc Làm theo Thành Phố')
    plt.xlabel('Tổng Số Lượng Tin Tuyển Dụng')
    plt.ylabel('Thành Phố')
    plt.xticks([])
    plt.tight_layout()

    # --- LƯU FILE VÀO REPORTS/ ---
    file_path = os.path.join(REPORTS_DIR, '2_job_location_heatmap_city_level.png')
    plt.savefig(file_path)
    plt.close()

    # ----------------------------------------------------
    # 2.3. BIỂU ĐỒ XU HƯỚNG CÔNG NGHỆ HOT
    # ----------------------------------------------------
    print("3. Tạo Biểu đồ xu hướng công nghệ hot")

    tech_keywords = [
        'Python', 'SQL', 'Java', 'Javascript', 'C++', 'C#',
        'AWS', 'Azure', 'GCP', 'Docker', 'Kubernetes',
        'React', 'Angular', 'Vue', 'ML', 'AI', 'Big Data', '.Net'
    ]

    tech_counts = {}
    for keyword in tech_keywords:
        regex_pattern = r'\b' + re.escape(keyword) + r'\b'

        count = df['job_title'].astype(str).str.contains(
            regex_pattern,
            case=False,
            na=False,
            regex=True
        ).sum()
        tech_counts[keyword] = count

    tech_df = pd.DataFrame(list(tech_counts.items()), columns=['Công Nghệ', 'Số Lượng Tin'])

    tech_df = tech_df.sort_values(by='Số Lượng Tin', ascending=False)

    plt.figure(figsize=(12, 6))

    sns.barplot(
        x='Công Nghệ',
        y='Số Lượng Tin',
        hue='Công Nghệ',  # <<< TỐI ƯU CẢNH BÁO
        data=tech_df,
        palette='Spectral',
        legend=False  # <<< TỐI ƯU CẢNH BÁO
    )

    plt.title('Xu Hướng Công Nghệ Hot (Hiển thị tất cả các từ khóa được tìm thấy)')
    plt.xlabel('Công Nghệ')
    plt.ylabel('Số Lượng Tin Tuyển Dụng')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()

    # --- LƯU FILE VÀO REPORTS/ ---
    file_path = os.path.join(REPORTS_DIR, '3_tech_trend_barplot_all.png')
    plt.savefig(file_path)
    plt.close()

    print("\n[CHẠY XONG] BÁO CÁO PHÂN TÍCH ĐÃ HOÀN THÀNH. Đã lưu 3 file ảnh vào reports/.")


if __name__ == "__main__":
    run_analytics()