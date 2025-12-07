#!/bin/bash

# --- CẤU HÌNH ĐƯỜNG DẪN TUYỆT ĐỐI ---
# Thư mục gốc của dự án (VUI LÒNG KIỂM TRA LẠI ĐƯỜNG DẪN NÀY!)
PROJECT_DIR="/home/cahara/Desktop/data_cv"

# Đường dẫn đến môi trường ảo (venv)
VENV_ACTIVATE="${PROJECT_DIR}/venv/bin/activate"

# Định nghĩa các script Python (TẤT CẢ ĐỀU TRONG SRC/)
ETL_SCRIPT="${PROJECT_DIR}/src/topcv_crawler.py"
ANALYTICS_SCRIPT="${PROJECT_DIR}/src/analytics_reporter.py"
DISCORD_SCRIPT="${PROJECT_DIR}/src/discord_reporter.py"

# Đường dẫn đến python interpreter trong venv (dùng cho Cron Job)
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

# GHI LOG CHI TIẾT CỦA ETL VÀO FILE RIÊNG
ETL_DETAIL_LOG="${PROJECT_DIR}/etl_detail.log"
echo "" > $ETL_DETAIL_LOG # Xóa nội dung log cũ

# Chạy ETL và chuyển hướng output vào file riêng (để log chính không bị quá tải)
$PYTHON_EXEC $ETL_SCRIPT >> $ETL_DETAIL_LOG 2>&1
ETL_STATUS=$?

if [ $ETL_STATUS -ne 0 ]; then
    echo "[LỖI CRON] ETL thất bại (Mã: $ETL_STATUS). Vui lòng kiểm tra file $ETL_DETAIL_LOG để xem chi tiết lỗi."
    exit 1
fi

# 3. Chạy Analytics (BƯỚC 2)
echo "--- BƯỚC 2: CHẠY ANALYTICS (Vẽ biểu đồ và lưu reports) ---"
$PYTHON_EXEC $ANALYTICS_SCRIPT
ANALYTICS_STATUS=$?

if [ $ANALYTICS_STATUS -ne 0 ]; then
    echo "[LỖI CRON] Analytics thất bại (Mã: $ANALYTICS_STATUS). Tiếp tục Discord."
fi

# 4. Chạy Discord Reporter (BƯỚC 3)
echo "--- BƯỚC 3: GỬI BÁO CÁO DISCORD ---"
$PYTHON_EXEC $DISCORD_SCRIPT
DISCORD_STATUS=$?

if [ $DISCORD_STATUS -ne 0 ]; then
    echo "[LỖI CRON] Discord Reporter thất bại (Mã: $DISCORD_STATUS)."
fi

# 5. Hoàn tất
# Dòng 'deactivate' đã bị loại bỏ vì nó gây lỗi trong Cron Job
echo "[INFO] Đã hủy kích hoạt môi trường ảo."

echo "=========================================="
echo "PIPELINE HOÀN TẤT THÀNH CÔNG: $(date)"
echo "=========================================="
exit 0