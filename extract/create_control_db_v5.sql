-- =========================================================
-- Data Warehouse Control Database V5.0
-- Purpose: Control tables for Extract, Load, Transform, Consolidate
-- Date: November 12, 2025
-- Author: Khanh Huynh
-- =========================================================

-- Drop database if exists (CAUTION: For fresh install only!)
DROP DATABASE IF EXISTS db_control;

-- Create database
CREATE DATABASE db_control CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE db_control;

-- =========================================================
-- EXTRACT LAYER TABLES
-- =========================================================

-- Table: extract_config (replaces old 'config' table)
CREATE TABLE extract_config (
    config_id INT AUTO_INCREMENT PRIMARY KEY,
    src_id VARCHAR(50) NOT NULL UNIQUE,
    src_name VARCHAR(100) NOT NULL,
    src_url TEXT,
    enabled BOOLEAN DEFAULT TRUE,
    extraction_frequency VARCHAR(20) DEFAULT 'daily',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_src_id (src_id),
    INDEX idx_enabled (enabled)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Table: extract_log (replaces old 'log_config' table)
CREATE TABLE extract_log (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    src_id VARCHAR(50) NOT NULL,
    date DATE NOT NULL,
    status ENUM('Success', 'Failed', 'Running') DEFAULT 'Running',
    rows_extracted INT DEFAULT 0,
    file_path TEXT,
    file_size BIGINT DEFAULT 0,
    file_md5 VARCHAR(32),
    watermark_before VARCHAR(50),
    watermark_after VARCHAR(50),
    start_time TIMESTAMP NULL,
    end_time TIMESTAMP NULL,
    duration_seconds INT AS (TIMESTAMPDIFF(SECOND, start_time, end_time)) STORED,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (src_id) REFERENCES extract_config(src_id) ON DELETE CASCADE,
    INDEX idx_src_date (src_id, date),
    INDEX idx_status (status),
    INDEX idx_date (date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =========================================================
-- PROCESS LAYER TABLES (for Load/Transform/Consolidate)
-- =========================================================

-- Table: process_config
CREATE TABLE process_config (
    process_id INT AUTO_INCREMENT PRIMARY KEY,
    process_name VARCHAR(100) NOT NULL UNIQUE,
    process_type ENUM('load', 'transform', 'consolidate') NOT NULL,
    source_table VARCHAR(100),
    target_table VARCHAR(100),
    enabled BOOLEAN DEFAULT TRUE,
    schedule_time TIME,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_process_type (process_type),
    INDEX idx_enabled (enabled)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Table: process_log
CREATE TABLE process_log (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    process_id INT NOT NULL,
    execution_date DATE NOT NULL,
    status ENUM('Success', 'Failed', 'Running') DEFAULT 'Running',
    rows_processed INT DEFAULT 0,
    start_time TIMESTAMP NULL,
    end_time TIMESTAMP NULL,
    duration_seconds INT AS (TIMESTAMPDIFF(SECOND, start_time, end_time)) STORED,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (process_id) REFERENCES process_config(process_id) ON DELETE CASCADE,
    INDEX idx_process_date (process_id, execution_date),
    INDEX idx_status (status),
    INDEX idx_execution_date (execution_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =========================================================
-- VIEWS FOR MONITORING
-- =========================================================

-- View: v_extract_summary (daily extraction summary)
CREATE VIEW v_extract_summary AS
SELECT 
    src_id,
    date,
    status,
    rows_extracted,
    ROUND(file_size/1024/1024, 2) AS file_size_mb,
    duration_seconds,
    DATE_FORMAT(start_time, '%Y-%m-%d %H:%i:%s') AS start_time,
    DATE_FORMAT(end_time, '%Y-%m-%d %H:%i:%s') AS end_time
FROM extract_log
ORDER BY date DESC, log_id DESC;

-- View: v_extract_failures (recent failures)
CREATE VIEW v_extract_failures AS
SELECT 
    log_id,
    src_id,
    date,
    error_message,
    DATE_FORMAT(start_time, '%Y-%m-%d %H:%i:%s') AS failed_at
FROM extract_log
WHERE status = 'Failed'
ORDER BY date DESC, log_id DESC;

-- View: v_extract_stats (statistics by source)
CREATE VIEW v_extract_stats AS
SELECT 
    src_id,
    COUNT(*) AS total_runs,
    SUM(CASE WHEN status = 'Success' THEN 1 ELSE 0 END) AS success_count,
    SUM(CASE WHEN status = 'Failed' THEN 1 ELSE 0 END) AS failure_count,
    ROUND(AVG(rows_extracted), 0) AS avg_rows,
    MAX(date) AS last_run_date
FROM extract_log
GROUP BY src_id;

-- View: v_process_summary (process execution summary)
CREATE VIEW v_process_summary AS
SELECT 
    pl.log_id,
    pc.process_name,
    pc.process_type,
    pl.execution_date,
    pl.status,
    pl.rows_processed,
    pl.duration_seconds,
    DATE_FORMAT(pl.start_time, '%Y-%m-%d %H:%i:%s') AS start_time
FROM process_log pl
JOIN process_config pc ON pl.process_id = pc.process_id
ORDER BY pl.execution_date DESC, pl.log_id DESC;

-- View: v_daily_pipeline_status (complete daily pipeline status)
CREATE VIEW v_daily_pipeline_status AS
SELECT 
    el.date AS pipeline_date,
    COUNT(DISTINCT el.src_id) AS sources_extracted,
    SUM(CASE WHEN el.status = 'Success' THEN 1 ELSE 0 END) AS extract_success,
    SUM(CASE WHEN el.status = 'Failed' THEN 1 ELSE 0 END) AS extract_failed,
    (SELECT COUNT(*) FROM process_log WHERE DATE(execution_date) = el.date AND status = 'Success') AS process_success,
    (SELECT COUNT(*) FROM process_log WHERE DATE(execution_date) = el.date AND status = 'Failed') AS process_failed
FROM extract_log el
GROUP BY el.date
ORDER BY el.date DESC;

-- =========================================================
-- SEED DATA (Initial Configuration)
-- =========================================================

-- Insert TopCV source configuration
INSERT INTO extract_config (src_id, src_name, src_url, enabled, extraction_frequency)
VALUES (
    'topcv_jobs',
    'TopCV Job Listings',
    'https://www.topcv.vn/viec-lam-it',
    TRUE,
    'daily'
);

-- =========================================================
-- GRANTS FOR APPLICATION USER
-- =========================================================

-- Grant privileges to 'khanh' user
GRANT SELECT, INSERT, UPDATE, DELETE ON db_control.* TO 'khanh'@'localhost';
FLUSH PRIVILEGES;

-- =========================================================
-- VERIFICATION QUERIES
-- =========================================================

-- Show all tables
SHOW TABLES;

-- Verify seed data
SELECT * FROM extract_config;

SELECT 'Database db_control V5.0 created successfully!' AS Status;
