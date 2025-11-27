import pytest
import numpy as np
import pandas as pd
import re # Cần thiết nếu các hàm test sử dụng regex

# =========================================================================
# >>> FIX: BỎ COMMENT DÒNG NÀY <<<
# Dòng này phải được giữ nguyên để import các hàm từ transform.py
from transform import process_salary, standardize_title, extract_location_pairs
# =========================================================================


# =========================================================================
# 1. UNIT TESTS CHO HÀM process_salary
# =========================================================================

# Định nghĩa các trường hợp kiểm thử cho hàm process_salary
# Định dạng: (input_salary, expected_avg, expected_min, expected_max, expected_unit)
salary_test_cases = [
    # --- Yêu cầu đặc biệt (Negotiable/Thoả thuận) ---
    ('Thoả thuận', 0.0, 0.0, 0.0, None),
    ('NEGOTIABLE', 0.0, 0.0, 0.0, None),

    # --- Case Range VND (Triệu) ---
    ('10 - 20 triệu', 15.0, 10.0, 20.0, 'VND'),
    ('15-30 tr', 22.5, 15.0, 30.0, 'VND'),
    ('5,000,000 - 10,000,000 VND', np.nan, np.nan, np.nan, 'Unknown_Unit'),
    # Lỗi không parse được vì chưa chuẩn hóa đơn vị VND thường

    # --- Case Threshold VND (Trên/Từ/Tới) ---
    ('Trên 10 triệu', 10.0, 10.0, np.nan, 'VND'),
    ('Tới 15 tr', 15.0, np.nan, 15.0, 'VND'),
    ('Up to 25 triệu', 25.0, np.nan, 25.0, 'VND'),

    # --- Case Single VND (k/Nghìn) ---
    ('8000 k', 8.0, 8.0, 8.0, 'VND'),  # 8000k -> 8 triệu VND
    ('5000000 nghìn', 5000.0, 5000.0, 5000.0, 'VND'),
    # 5000000 nghìn -> 5000 triệu VND (Lỗi logic, nhưng theo luật của hàm)

    # --- Case Range USD ---
    ('500 - 1000 USD', 750.0, 500.0, 1000.0, 'USD'),
    ('2,000-3,000$', 2500.0, 2000.0, 3000.0, 'USD'),

    # --- Case Missing/Invalid ---
    (None, np.nan, np.nan, np.nan, 'Unknown_Unit'),
    ('', np.nan, np.nan, np.nan, 'Unknown_Unit'),
    ('Đẹp trai, nhiều tiền', np.nan, np.nan, np.nan, 'Unknown_Unit'),
]


@pytest.mark.parametrize("input_salary, expected_avg, expected_min, expected_max, expected_unit", salary_test_cases)
def test_process_salary(input_salary, expected_avg, expected_min, expected_max, expected_unit):
    """
    Kiểm tra hàm process_salary với nhiều định dạng đầu vào.
    Sử dụng numpy.testing.assert_allclose để so sánh giá trị float.
    Sử dụng numpy.isnan để so sánh giá trị NaN.
    """
    avg, min_s, max_s, unit = process_salary(input_salary) # Dòng này cần import mới chạy được

    # Kiểm tra giá trị trung bình (AVG)
    if np.isnan(expected_avg):
        assert np.isnan(avg)
    else:
        # Sử dụng allclose để so sánh float, chấp nhận sai số nhỏ
        assert np.allclose(avg, expected_avg, equal_nan=True)

    # Kiểm tra giá trị tối thiểu (MIN)
    if np.isnan(expected_min):
        assert np.isnan(min_s)
    else:
        assert np.allclose(min_s, expected_min, equal_nan=True)

    # Kiểm tra giá trị tối đa (MAX)
    if np.isnan(expected_max):
        assert np.isnan(max_s)
    else:
        assert np.allclose(max_s, expected_max, equal_nan=True)

    # Kiểm tra đơn vị (UNIT)
    assert unit == expected_unit


# =========================================================================
# 2. UNIT TESTS CHO HÀM standardize_title
# =========================================================================

title_test_cases = [
    ('Business Analyst', 'Data/Business Analyst'),
    ('Senior Data Analyst (SDA)', 'Data/Business Analyst'),
    ('Lập trình viên PHP', 'Software Developer'),
    ('Python Dev', 'Software Developer'),
    ('Data Engineer AWS', 'Data/System Engineer'),
    ('ETL System Engineer', 'Data/System Engineer'),
    ('QA/QC Tester', 'QA/Tester'),
    ('Project Manager', 'Management/Lead'),
    ('PM', 'Management/Lead'),
    ('Content Writer', 'Other IT Role'),
    (np.nan, 'Unknown'),
    (None, 'Unknown'),
]


@pytest.mark.parametrize("input_title, expected_output", title_test_cases)
def test_standardize_title(input_title, expected_output):
    """ Kiểm tra hàm standardize_title. """
    assert standardize_title(input_title) == expected_output


# =========================================================================
# 3. UNIT TESTS CHO HÀM extract_location_pairs
# =========================================================================

location_test_cases = [
    ('Hà Nội: Đống Đa: Hồ Chí Minh: Quận 1', [('Hà Nội', 'Đống Đa'), ('Hồ Chí Minh', 'Quận 1')]),
    ('Toàn Quốc', [('Toàn Quốc', None)]),
    ('OverSea: Global', [('Nước Ngoài', None)]),
    ('Hà Nội: Cầu Giấy', [('Hà Nội', 'Cầu Giấy')]),
    ('Hồ Chí Minh', [('Unknown', None)]),  # Thiếu district nên không parse được cặp nào (trừ trường hợp đặc biệt)
    (None, [('Unknown', None)]),
    ('Nhiều địa điểm', [('Multi-location', None)]),
]


@pytest.mark.parametrize("input_address, expected_output", location_test_cases)
def test_extract_location_pairs(input_address, expected_output):
    """ Kiểm tra hàm extract_location_pairs. """
    assert extract_location_pairs(input_address) == expected_output