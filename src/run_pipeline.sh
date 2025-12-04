#!/bin/bash

# --- CẤU HÌNH ĐƯỜNG DẪN TUYỆT ĐỐI ---
# Thư mục gốc của dự án (Giữ nguyên)
PROJECT_DIR="/home/cahara/Desktop/data_cv"

# Đường dẫn đến môi trường ảo (venv)
VENV_ACTIVATE="${PROJECT_DIR}/venv/bin/activate"

# !!! THAY ĐỔI QUAN TRỌNG: Script ETL là topcv_crawler.py
ETL_SCRIPT="${PROJECT_DIR}/src/topcv_crawler.py"

# Đường dẫn đến script Analytics (Giữ nguyên)
ANALYTICS_SCRIPT="${PROJECT_DIR}/src/analytics_reporter.py"

# Đường dẫn đến python interpreter trong venv (Giữ nguyên)
PYTHON_EXEC="${PROJECT_DIR}/venv/bin/python"

# --- BẮT ĐẦU CHẠY PIPELINE ---

echo "=========================================="
echo "BẮT ĐẦU CHẠY PIPELINE ETL & ANALYTICS: $(date)"
echo "=========================================="

# 1. Kích hoạt môi trường ảo
source $VENV_ACTIVATE
echo "[INFO] Đã kích hoạt môi trường ảo."

# 2. Chạy ETL (Extract, Transform, Load)
echo "--- BƯỚC 1: CHẠY ETL (Crawl dữ liệu mới và Tải vào MySQL) ---"
# Chạy topcv_crawler.py
$PYTHON_EXEC $ETL_SCRIPT
ETL_STATUS=$?

if [ $ETL_STATUS -ne 0 ]; then
    echo "[LỖI CRON] ETL thất bại (Mã: $ETL_STATUS). Dừng Analytics."
    # Để tránh việc Cron gửi email lỗi quá dài, có thể thêm một lệnh ghi log lỗi đơn giản hơn ở đây.
    exit 1
fi

# 3. Chạy Analytics (Vẽ biểu đồ)
echo "--- BƯỚC 2: CHẠY ANALYTICS (Vẽ biểu đồ và lưu reports) ---"
$PYTHON_EXEC $ANALYTICS_SCRIPT
ANALYTICS_STATUS=$?

if [ $ANALYTICS_STATUS -ne 0 ]; then
    echo "[LỖI CRON] Analytics thất bại (Mã: $ANALYTICS_STATUS)."
    exit 1
fi

# 4. Hủy kích hoạt môi trường ảo
deactivate
echo "[INFO] Đã hủy kích hoạt môi trường ảo."

echo "=========================================="
echo "PIPELINE HOÀN TẤT THÀNH CÔNG: $(date)"
echo "=========================================="
exit 0