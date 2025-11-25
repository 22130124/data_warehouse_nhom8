#!/bin/bash

################################################################################
# TopCV Scraper - Auto Retry Wrapper V2.0 (Phase 2)
# Schedule: 7:00 AM daily
# Retry: Max 3 times, every 15 minutes on failure
# Author: KhanhHuynh
# Date: November 12, 2025
################################################################################

# Configuration (UPDATED for Phase 2)
BASE_DIR="/opt/dw/staging/extract"
SCRIPT_PATH="${BASE_DIR}/scripts/topcv_scraper_v5.py"
CONFIG_PATH="/opt/dw/staging/config.xml"
SOURCE_ID="topcv_jobs"
LOG_DIR="${BASE_DIR}/logs"
LOCK_DIR="${BASE_DIR}/locks"
RUN_DATE=$(date +%Y-%m-%d)
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Retry configuration
MAX_RETRIES=3
RETRY_INTERVAL=15  # minutes
LOCK_FILE="${LOCK_DIR}/scraper_${RUN_DATE}.lock"
RETRY_COUNT_FILE="${LOCK_DIR}/retry_count_${RUN_DATE}.txt"

# Database credentials
DB_USER="khanh"
DB_PASS="khanh123!"
DB_NAME="db_control"

# Create directories
mkdir -p "${LOG_DIR}"
mkdir -p "${LOCK_DIR}"

# Log file
LOG_FILE="${LOG_DIR}/retry_${TIMESTAMP}.log"

################################################################################
# Function: Log message
################################################################################
log_message() {
    local level=$1
    shift
    local message="$@"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [${level}] ${message}" | tee -a "${LOG_FILE}"
}

################################################################################
# Function: Check if succeeded today
################################################################################
check_success_today() {
    SUCCESS_COUNT=$(mysql -u ${DB_USER} -p${DB_PASS} ${DB_NAME} -se \
        "SELECT COUNT(*) FROM extract_log
         WHERE src_id='${SOURCE_ID}'
         AND date='${RUN_DATE}'
         AND status='Success';" 2>/dev/null)
    [ "${SUCCESS_COUNT}" -gt 0 ]
}

################################################################################
# Function: Get retry count
################################################################################
get_retry_count() {
    if [ -f "${RETRY_COUNT_FILE}" ]; then
        cat "${RETRY_COUNT_FILE}"
    else
        echo "0"
    fi
}

################################################################################
# Function: Increment retry count
################################################################################
increment_retry_count() {
    local current=$(($(get_retry_count)))
    local new_count=$((current + 1))
    echo "${new_count}" > "${RETRY_COUNT_FILE}"
    echo "${new_count}"
}

################################################################################
# Function: Reset retry count
################################################################################
reset_retry_count() {
    rm -f "${RETRY_COUNT_FILE}"
    log_message "INFO" "Retry count reset"
}

################################################################################
# Function: Clean up cron retry jobs
################################################################################
cleanup_retry_cron() {
    # Remove temporary retry jobs from crontab
    crontab -l 2>/dev/null | grep -v "# RETRY_TEMP_JOB" | crontab -
    log_message "INFO" "Cleaned up temporary retry cron jobs"
}

################################################################################
# Function: Schedule retry
################################################################################
schedule_retry() {
    local retry_num=$1
    local next_time=$(date -d "+${RETRY_INTERVAL} minutes" '+%H:%M')
    local next_hour=${next_time:0:2}
    local next_min=${next_time:3:2}
    
    log_message "INFO" "Scheduling retry #${retry_num} at ${next_time}"
    
    # Add temporary cron job for retry
    (crontab -l 2>/dev/null | grep -v "# RETRY_TEMP_JOB"; \
     echo "${next_min} ${next_hour} * * * ${BASE_DIR}/scripts/run_topcv_scraper_with_retry.sh >> ${LOG_DIR}/cron.log 2>&1 # RETRY_TEMP_JOB") | crontab -
}

################################################################################
# Main Execution
################################################################################
log_message "INFO" "=========================================="
log_message "INFO" "TopCV Scraper - Retry Wrapper V2.0"
log_message "INFO" "Date: ${RUN_DATE}"
log_message "INFO" "=========================================="

# Check if already succeeded
if check_success_today; then
    log_message "INFO" "✓ Data already extracted successfully today"
    log_message "INFO" "Cleaning up and exiting..."
    reset_retry_count
    cleanup_retry_cron
    exit 0
fi

# Check lock file
if [ -f "${LOCK_FILE}" ]; then
    PID=$(cat "${LOCK_FILE}")
    if ps -p ${PID} > /dev/null 2>&1; then
        log_message "WARN" "Another instance is running (PID: ${PID}). Exiting."
        exit 1
    else
        log_message "INFO" "Removing stale lock file"
        rm -f "${LOCK_FILE}"
    fi
fi

# Create lock file
echo $$ > "${LOCK_FILE}"

# Get current retry count
CURRENT_RETRY=$(get_retry_count)
log_message "INFO" "Retry attempt: ${CURRENT_RETRY}/${MAX_RETRIES}"

# Check if max retries exceeded
if [ ${CURRENT_RETRY} -ge ${MAX_RETRIES} ]; then
    log_message "ERROR" "=========================================="
    log_message "ERROR" "✗✗✗ MAX RETRIES EXCEEDED ✗✗✗"
    log_message "ERROR" "Failed after ${MAX_RETRIES} attempts"
    log_message "ERROR" "Date: ${RUN_DATE}"
    log_message "ERROR" "Manual intervention required!"
    log_message "ERROR" "=========================================="
    
    # Clean up for tomorrow
    reset_retry_count
    cleanup_retry_cron
    rm -f "${LOCK_FILE}"
    exit 1
fi

# Run scraper
log_message "INFO" "Navigating to base directory..."
cd "${BASE_DIR}"

log_message "INFO" "Activating virtual environment..."
source venv/bin/activate

log_message "INFO" "Executing scraper script..."
python3 "${SCRIPT_PATH}" \
    --config "${CONFIG_PATH}" \
    --source_id "${SOURCE_ID}" \
    --date "${RUN_DATE}" 2>&1 | tee -a "${LOG_FILE}"

EXIT_CODE=$?

# Remove lock file
rm -f "${LOCK_FILE}"

# Check database for actual status (more reliable than exit code)
FINAL_STATUS=$(mysql -u ${DB_USER} -p${DB_PASS} ${DB_NAME} -se \
    "SELECT status FROM extract_log
     WHERE src_id='${SOURCE_ID}'
     AND date='${RUN_DATE}'
     ORDER BY log_id DESC LIMIT 1;" 2>/dev/null)

log_message "INFO" "Script exit code: ${EXIT_CODE}"
log_message "INFO" "Database status: ${FINAL_STATUS}"

# Handle result based on database status
if [ "${FINAL_STATUS}" = "Success" ]; then
    log_message "INFO" "=========================================="
    log_message "INFO" "✓✓✓ SCRAPER COMPLETED SUCCESSFULLY ✓✓✓"
    log_message "INFO" "=========================================="
    reset_retry_count
    cleanup_retry_cron
    exit 0

elif [ "${FINAL_STATUS}" = "Failed" ]; then
    NEW_RETRY=$(increment_retry_count)
    log_message "ERROR" "=========================================="
    log_message "ERROR" "✗ SCRAPER FAILED"
    log_message "ERROR" "Retry count: ${NEW_RETRY}/${MAX_RETRIES}"
    log_message "ERROR" "=========================================="
    
    if [ ${NEW_RETRY} -lt ${MAX_RETRIES} ]; then
        schedule_retry ${NEW_RETRY}
        log_message "INFO" "Next retry in ${RETRY_INTERVAL} minutes"
    else
        log_message "ERROR" "This was the final retry attempt"
    fi
    exit 1

else
    log_message "WARN" "Unknown status: ${FINAL_STATUS}"
    log_message "WARN" "Treating as failure"
    NEW_RETRY=$(increment_retry_count)
    if [ ${NEW_RETRY} -lt ${MAX_RETRIES} ]; then
        schedule_retry ${NEW_RETRY}
    fi
    exit 1
fi
