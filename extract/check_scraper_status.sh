#!/bin/bash

################################################################################
# TopCV Scraper - Status Dashboard V2.0 (Phase 2)
################################################################################

echo "=========================================="
echo "TopCV Scraper - Status Dashboard V2.0"
echo "Generated: $(date '+%Y-%m-%d %H:%M:%S')"
echo "=========================================="
echo ""

RUN_DATE=$(date +%Y-%m-%d)
DB_USER="khanh"
DB_PASS="khanh123!"
DB_NAME="db_control"
SOURCE_ID="topcv_jobs"
LOCK_DIR="/opt/dw/staging/extract/locks"
RAW_DIR="/opt/dw/staging/extract/raw"

# Current retry status
RETRY_FILE="${LOCK_DIR}/retry_count_${RUN_DATE}.txt"
if [ -f "${RETRY_FILE}" ]; then
    RETRY_COUNT=$(cat "${RETRY_FILE}")
    echo "=== TODAY'S STATUS ==="
    echo "Date: ${RUN_DATE}"
    echo "Retry count: ${RETRY_COUNT}/3"
    echo "Status: IN PROGRESS (retrying)"
else
    echo "=== TODAY'S STATUS ==="
    echo "Date: ${RUN_DATE}"
    echo "Retry count: 0/3"
    echo "Status: Completed or first attempt"
fi

# Check if running
LOCK_FILE="${LOCK_DIR}/scraper_${RUN_DATE}.lock"
if [ -f "${LOCK_FILE}" ]; then
    PID=$(cat "${LOCK_FILE}")
    if ps -p ${PID} > /dev/null 2>&1; then
        echo "Currently running: YES (PID: ${PID})"
    else
        echo "Currently running: NO (stale lock)"
    fi
else
    echo "Currently running: NO"
fi

echo ""

# Latest 5 runs from database (UPDATED table name)
echo "=== LATEST 5 RUNS ==="
mysql -u ${DB_USER} -p${DB_PASS} ${DB_NAME} -t -e "
SELECT 
    log_id,
    date,
    status,
    rows_extracted,
    ROUND(file_size/1024, 2) AS 'size_kb',
    TIME(start_time) AS 'start',
    duration_seconds AS 'duration_sec'
FROM extract_log
WHERE src_id = '${SOURCE_ID}'
ORDER BY date DESC, log_id DESC
LIMIT 5;
" 2>/dev/null

echo ""

# Statistics (UPDATED table name)
echo "=== STATISTICS ==="
mysql -u ${DB_USER} -p${DB_PASS} ${DB_NAME} -t -e "
SELECT 
    COUNT(*) AS total_runs,
    SUM(CASE WHEN status = 'Success' THEN 1 ELSE 0 END) AS success,
    SUM(CASE WHEN status = 'Failed' THEN 1 ELSE 0 END) AS failed,
    ROUND(AVG(rows_extracted), 0) AS avg_jobs,
    MAX(date) AS last_run_date
FROM extract_log
WHERE src_id = '${SOURCE_ID}';
" 2>/dev/null

echo ""

# Today's detailed log
echo "=== TODAY'S DETAILED LOG ==="
mysql -u ${DB_USER} -p${DB_PASS} ${DB_NAME} -t -e "
SELECT 
    log_id,
    TIME(start_time) AS time,
    status,
    rows_extracted,
    error_message
FROM extract_log
WHERE src_id = '${SOURCE_ID}' AND date = '${RUN_DATE}'
ORDER BY log_id DESC;
" 2>/dev/null

echo ""

# Failed runs (if any)
echo "=== RECENT FAILURES (IF ANY) ==="
mysql -u ${DB_USER} -p${DB_PASS} ${DB_NAME} -t -e "
SELECT 
    date,
    TIME(start_time) AS time,
    SUBSTRING(error_message, 1, 80) AS error_summary
FROM extract_log
WHERE src_id = '${SOURCE_ID}'
AND status = 'Failed'
ORDER BY date DESC, log_id DESC
LIMIT 5;
" 2>/dev/null

echo ""

# Disk usage (UPDATED path)
echo "=== DISK USAGE ==="
du -sh ${RAW_DIR}/source=${SOURCE_ID} 2>/dev/null || echo "No data yet"

echo ""
echo "=========================================="
echo "✓ Status check complete"
echo "=========================================="
