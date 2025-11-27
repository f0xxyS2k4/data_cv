#!/bin/bash

# --- CẤU HÌNH ĐƯỜNG DẪN TUYỆT ĐỐI ---
# Thư mục gốc của dự án
PROJECT_DIR="/home/cahara/Desktop/data_cv"

# Đường dẫn đến môi trường ảo (venv)
VENV_ACTIVATE="${PROJECT_DIR}/venv/bin/activate"

# Đường dẫn đến các file Python (ĐÃ CẬP NHẬT ĐỂ TRỎ VÀO THƯ MỤC src/)
ETL_SCRIPT="${PROJECT_DIR}/src/data_pipeline.py"
ANALYTICS_SCRIPT="${PROJECT_DIR}/src/analytics_reporter.py"

# Đường dẫn đến python interpreter trong venv (dùng cho Cron Job)
PYTHON_EXEC="${PROJECT_DIR}/venv/bin/python"

# --- BẮT ĐẦU CHẠY PIPELINE ---

echo "=========================================="
echo "BẮT ĐẦU CHẠY PIPELINE ETL & ANALYTICS: $(date)"
echo "=========================================="

# 1. Kích hoạt môi trường ảo (Tùy chọn, nhưng tốt để đảm bảo môi trường)
source $VENV_ACTIVATE
echo "[INFO] Đã kích hoạt môi trường ảo."

# 2. Chạy ETL (Extract, Transform, Load)
echo "--- BƯỚC 1: CHẠY ETL (Tải dữ liệu sạch vào MySQL) ---"
# Sử dụng đường dẫn tuyệt đối đến Python Interpreter và Script mới
$PYTHON_EXEC $ETL_SCRIPT
ETL_STATUS=$?

if [ $ETL_STATUS -ne 0 ]; then
    echo "[LỖI CRON] ETL thất bại (Mã: $ETL_STATUS). Dừng Analytics."
    exit 1
fi

# 3. Chạy Analytics (Vẽ biểu đồ)
echo "--- BƯỚC 2: CHẠY ANALYTICS (Vẽ biểu đồ và lưu reports) ---"
# Sử dụng đường dẫn tuyệt đối đến Python Interpreter và Script mới
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
