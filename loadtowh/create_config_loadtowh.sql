/* --------------------------
   DATABASE: db_control
--------------------------- */
USE db_control;

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

/* process_config */
CREATE TABLE IF NOT EXISTS process_config (
    process_id INT AUTO_INCREMENT PRIMARY KEY,
    process_name VARCHAR(100) NOT NULL,
    process_type ENUM('load','transform','consolidate') NOT NULL,
    source_table VARCHAR(100) DEFAULT NULL,
    target_table VARCHAR(100) DEFAULT NULL,
    enabled TINYINT DEFAULT 1,
    schedule_time TIME DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY idx_process_name (process_name)
);

/* Thêm record vào process_config */
INSERT INTO process_config (
    process_name, process_type, source_table, target_table, enabled, schedule_time
) VALUES (
    '1', 'load', '1', '1', 1, '00:00:01'
);

/* process_log */
CREATE TABLE IF NOT EXISTS process_log (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    process_id INT NOT NULL,
    execution_date DATE NOT NULL,
    status ENUM('Success','Failed','Running') DEFAULT 'Running',
    rows_processed INT DEFAULT 0,
    start_time TIMESTAMP NULL,
    end_time TIMESTAMP NULL,
    duration_seconds INT AS (TIMESTAMPDIFF(SECOND,start_time,end_time)) STORED,
    error_message TEXT DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_status (status),
    INDEX idx_execution_date (execution_date),
    FOREIGN KEY (process_id) REFERENCES process_config(process_id)
);

/* Thêm record vào process_log */
INSERT INTO process_log (
    process_id, execution_date, status, rows_processed, start_time, end_time, error_message
) VALUES (
    1, '2025-11-13', 'Success', 1500, '2025-11-13 00:03:00', '2025-11-13 00:05:30', NULL
);

/* Procedure: is_process_done_procedure */
DELIMITER $$
CREATE PROCEDURE IF NOT EXISTS is_process_done_procedure(IN p_day DATE)
BEGIN
    SELECT EXISTS (
        SELECT 1
        FROM process_log pl
        WHERE LOWER(pl.status) = 'success'
          AND pl.execution_date = p_day
    ) AS is_done;
END$$
DELIMITER ;

/* load_to_wh_config */
CREATE TABLE IF NOT EXISTS load_to_wh_config (
    id INT AUTO_INCREMENT PRIMARY KEY,
    enabled TINYINT(1) DEFAULT 1,
    is_process_done_procedure VARCHAR(100) DEFAULT NULL,
    sshuser VARCHAR(100) DEFAULT NULL,
    target_path VARCHAR(255) DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

/* Thêm 1 record vào load_to_wh_config */
INSERT INTO load_to_wh_config (
    is_process_done_procedure, sshuser, target_path
) VALUES (
    'is_process_done_procedure', 'ubuntu', '/opt/dw/dw/ready_to_wh'
);

/* load_to_wh_log */
CREATE TABLE IF NOT EXISTS load_to_wh_log (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    execution_date DATE NOT NULL,
    data_date DATE DEFAULT NULL,
    status ENUM('Success','Failed','Running') DEFAULT 'Running',
    rows_processed INT DEFAULT 0,
    start_time TIMESTAMP NULL,
    end_time TIMESTAMP NULL,
    duration_seconds INT AS (TIMESTAMPDIFF(SECOND,start_time,end_time)) STORED,
    message TEXT,
    INDEX idx_status (status),
    INDEX idx_execution_date (execution_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

SET FOREIGN_KEY_CHECKS = 1;

/* --------------------------
   DATABASE: db_staging
--------------------------- */
USE db_staging;

/* Thêm 1 record vào staging_topcv_jobs */
INSERT INTO staging_topcv_jobs (
    job_id, job_title, company_name, salary, location,
    experience_required, posted_time, job_url, extracted_date, date_id
) VALUES (
    'TOPCV_002', 'Backend Developer', 'XYZ Corp', '1200-1800 USD', 'Ho Chi Minh',
    '2 years', '2025-11-12', 'https://topcv.vn/job/backend-developer/xyz',
    '2025-11-13', 1
);
