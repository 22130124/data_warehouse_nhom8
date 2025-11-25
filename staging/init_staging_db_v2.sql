-- 1. CẬP NHẬT DB_CONTROL
USE db_control;

-- Bảng lưu lịch sử Load File (Giữ nguyên)
CREATE TABLE IF NOT EXISTS load_log (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    load_date DATE,
    file_name VARCHAR(255),
    file_path VARCHAR(500),
    start_time DATETIME,
    end_time DATETIME,
    rows_loaded INT DEFAULT 0,
    rows_error INT DEFAULT 0,
    status VARCHAR(50), 
    message TEXT,
    INDEX (load_date)
);

ALTER TABLE process_config ADD COLUMN description TEXT;

-- 2. KHỞI TẠO DB_STAGING
CREATE DATABASE IF NOT EXISTS db_staging;
USE db_staging;

-- Bảng Date Dimension (Giữ nguyên)
CREATE TABLE IF NOT EXISTS date_dim (
    date_sk INT PRIMARY KEY,
    full_date DATE,
    day_since_month_start INT,
    day_of_week_calendar VARCHAR(20),
    calendar_month_name VARCHAR(20),
    day_of_month INT,
    day_of_year INT,
    week_of_year VARCHAR(20),
    is_holiday VARCHAR(20),
    day_type VARCHAR(20)
);

-- Bảng Tạm (Temp) - Giữ nguyên để load CSV
DROP TABLE IF EXISTS staging_topcv_jobs_temp;
CREATE TABLE staging_topcv_jobs_temp (
    job_id TEXT,
    job_title TEXT,
    company_name TEXT,
    salary TEXT,
    location TEXT,
    experience_required TEXT,
    posted_time TEXT,
    job_url TEXT,
    extracted_date TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bảng Chính (Job) - CẬP NHẬT THEO CẤU TRÚC CỦA TEAMMATE
DROP TABLE IF EXISTS staging_topcv_jobs;
CREATE TABLE staging_topcv_jobs (
    job_id VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
    job_title VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
    company_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
    salary VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
    location VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
    experience_required VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
    posted_time VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
    job_url VARCHAR(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
    extracted_date DATE NULL DEFAULT NULL,
    date_id BIGINT NULL DEFAULT NULL,
    
    -- Index hỗ trợ tìm kiếm (Optional - nên thêm)
    UNIQUE KEY idx_job_id (job_id),
    INDEX idx_date (date_id)
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = Dynamic;