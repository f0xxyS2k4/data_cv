import mysql.connector
import requests
from data_pipeline import DB_CONFIG
from typing import List, Tuple


# Nhet link discord vao
DISCORD_WEBHOOK_URL = "https://discordapp.com/api/webhooks/1447084589045518389/scCFAXQ_1UgRA3AE4kN2Nuk_4sKi2EwjJ1C0pw_HbETFtif47Mc49-en5aQyj0FmuGDV"

#Lay job DE tu DB
def get_de_jobs_from_db() -> List[Tuple]:

    conn = None
    jobs = []

    if not DB_CONFIG:
        print("[DISCORD] ❌ Lỗi: DB_CONFIG không hợp lệ.")
        return []

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Tim job de được chuẩn hóa
        query = """
        SELECT 
            job_title, 
            company, 
            city, 
            min_salary, 
            max_salary, 
            salary_unit,
            link_description 
        FROM 
            job_listings_clean 
        WHERE 
            standardized_job_title = 'Data/System Engineer'
        """
        cursor.execute(query)
        jobs = cursor.fetchall()

        print(f"[DISCORD] Đã tìm thấy {len(jobs)} công việc Data Engineer/System Engineer.")
        return jobs

    except mysql.connector.Error as err:
        print(f"[DISCORD] ❌ Lỗi MySQL khi truy vấn: {err}")
        return []
    except Exception as e:
        print(f"[DISCORD] ❌ Lỗi chung khi kết nối database: {e}")
        return []
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

#Gui job de den discord
def send_discord_report(jobs: List[Tuple]):

    if not jobs:
        message = "Không tìm thấy công việc Data Engineer/System Engineer nào trong DB."
        payload = {"content": f"**[BÁO CÁO VIỆC LÀM DE]** {message}"}
    else:
        job_count = len(jobs)
        report_content = []

        for job in jobs:
            title, company, city, min_s, max_s, unit, link = job

            # Xử lý mức lương (chuyển đổi None sang 0 hoặc np.nan để so sánh)
            min_s = min_s if min_s is not None else 0
            max_s = max_s if max_s is not None else 0
            unit = unit if unit is not None else 'Triệu VND'  # Giả định đơn vị mặc định

            salary_info = ""
            if min_s > 0 or max_s > 0:
                if min_s == max_s:
                    salary_info = f"({min_s} {unit})"
                else:
                    salary_info = f"({min_s} - {max_s} {unit})"
            else:
                salary_info = "(Thỏa thuận)"

            # Tạo định dạng Markdown cho Discord
            job_line = f"- **{title}** tại **{company}** - {city} {salary_info}\nLink: <{link}>\n"
            report_content.append(job_line)

        # Tạo mô tả chính của Embed
        description_text = "".join(report_content)

        # Giới hạn độ dài tin nhắn Discord (2048 ký tự trong embed description)
        if len(description_text) > 2000:
            description_text = description_text[:1950] + f"\n... (Còn {job_count} job nữa. Chi tiết trong DB.)"

        payload = {
            "embeds": [
                {
                    "title": f"BÁO CÁO VIỆC LÀM DE MỚI NHẤT ({job_count} Jobs)",
                    "description": description_text,
                    "color": 3447003,  # Xanh dương
                    "footer": {"text": "Dữ liệu được crawl từ TopCV"},
                }
            ]
        }

    try:
        if DISCORD_WEBHOOK_URL == "YOUR_DISCORD_WEBHOOK_URL_HERE":
            print("[DISCORD] ⚠️ Cảnh báo: Vui lòng cập nhật DISCORD_WEBHOOK_URL trong script.")
            return

        response = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
        response.raise_for_status()
        print("[DISCORD] ✅ Báo cáo đã được gửi thành công tới Discord.")
    except requests.exceptions.RequestException as e:
        print(f"[DISCORD] ❌ Lỗi khi gửi báo cáo Discord. Kiểm tra Webhook URL: {e}")


if __name__ == "__main__":
    jobs_list = get_de_jobs_from_db()
    send_discord_report(jobs_list)