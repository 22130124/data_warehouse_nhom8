#!/bin/bash

################################################################################
# JobsGO Scraper Wrapper - Auto Retry & Logic
# Author: Khanh Huynh
################################################################################

# 1. CAU HINH
BASE_DIR="/opt/dw/staging/extract"
SCRIPT_DIR="${BASE_DIR}/scripts"
VENV_DIR="${BASE_DIR}/venv"
# Luu y: Kiem tra duong dan config.xml co dung khong
CONFIG_FILE="/opt/dw/staging/config.xml" 
LOG_DIR="${BASE_DIR}/logs"
LOCK_DIR="${BASE_DIR}/locks"

# Tham so rieng cho JobsGO
SOURCE_ID="jobsgo_jobs"
SCRIPT_NAME="jobsgo_scraper_v1.py"

CURRENT_DATE=$(date +%Y-%m-%d)
MAX_RETRIES=50 # Tang len de test thoai mai

mkdir -p "${LOG_DIR}"
mkdir -p "${LOCK_DIR}"

RETRY_COUNT_FILE="${LOCK_DIR}/retry_jobsgo_${CURRENT_DATE}.txt"
LOCK_FILE="${LOCK_DIR}/scraper_jobsgo_${CURRENT_DATE}.lock"

# 2. KIEM TRA DANG CHAY
if [ -f "${LOCK_FILE}" ]; then
    PID=$(cat "${LOCK_FILE}")
    if ps -p $PID > /dev/null; then
        echo "[$(date)] JobsGO Scraper is already running (PID: $PID). Exiting." >> "${LOG_DIR}/cron.log"
        exit 1
    else
        rm "${LOCK_FILE}"
    fi
fi

echo $$ > "${LOCK_FILE}"

# 3. LOGIC RETRY
if [ -f "${RETRY_COUNT_FILE}" ]; then
    CURRENT_ATTEMPT=$(cat "${RETRY_COUNT_FILE}")
else
    CURRENT_ATTEMPT=0
fi

if [ "$CURRENT_ATTEMPT" -ge "$MAX_RETRIES" ]; then
    echo "[$(date)] [JobsGO] ERROR: Max retries reached. Manual check required." >> "${LOG_DIR}/cron.log"
    rm "${LOCK_FILE}"
    exit 1
fi

NEXT_ATTEMPT=$((CURRENT_ATTEMPT + 1))
echo $NEXT_ATTEMPT > "${RETRY_COUNT_FILE}"

echo "[$(date)] [JobsGO] STARTING ATTEMPT $NEXT_ATTEMPT..." >> "${LOG_DIR}/cron.log"

# 4. THUC THI PYTHON (Dung tee de vua ghi log vua hien ra man hinh)
source "${VENV_DIR}/bin/activate"

python3 "${SCRIPT_DIR}/${SCRIPT_NAME}" \
    --config "${CONFIG_FILE}" \
    --source_id "${SOURCE_ID}" \
    --date "${CURRENT_DATE}" 2>&1 | tee -a "${LOG_DIR}/cron.log"

EXIT_CODE=${PIPESTATUS[0]} 
deactivate

# 5. XU LY KET QUA
if [ $EXIT_CODE -eq 0 ]; then
    echo "[$(date)] [JobsGO] SUCCESS." >> "${LOG_DIR}/cron.log"
    rm "${LOCK_FILE}"
    exit 0
else
    echo "[$(date)] [JobsGO] FAILED. Code $EXIT_CODE." >> "${LOG_DIR}/cron.log"
    rm "${LOCK_FILE}"
    exit 1
fi
