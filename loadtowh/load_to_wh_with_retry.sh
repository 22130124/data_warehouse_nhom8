#!/bin/bash

################################################################################
# Load to WH - Advanced Retry Wrapper V1.0
# Mục đích: Wrapper chạy load_to_wh.sh với cơ chế retry tự động dựa trên db_control
# Retry: tối đa 3 lần, mỗi lần cách nhau 5 phút nếu thất bại, sử dụng cron tạm
# Author: Hoang Phuc
# Date: 2025-11-22
################################################################################

# ---------------- Cấu hình thư mục cơ bản ----------------
BASE_DIR="/opt/dw/staging/loadtowh"
SCRIPT_PATH="${BASE_DIR}/scripts/load_to_wh.sh"  # Script gốc
LOG_DIR="${BASE_DIR}/logs"
LOCK_DIR="${BASE_DIR}/locks"
mkdir -p "$LOG_DIR" "$LOCK_DIR"

# ---------------- Nhận 17 tham số ----------------
if [ $# -ne 17 ]; then
    echo "Usage: $0 <config.xml> <date_param> <wh_db> <wh_user> <wh_pass> <wh_ip> <wh_port> <wh_ssh_user> <remote_path> <dump_folder> <control_db> <control_user> <control_pass> <control_ip> <control_port> <dump_file> <start_time>"
    exit 1
fi

CONFIG_XML="$1"
DATE_PARAM="$2"
WH_DB="$3"
WH_USER="$4"
WH_PASS="$5"
WH_IP="$6"
WH_PORT="$7"
WH_SSH_USER="$8"
REMOTE_PATH="$9"
DUMP_FOLDER="${10}"
CONTROL_DB="${11}"
CONTROL_USER="${12}"
CONTROL_PASS="${13}"
CONTROL_IP="${14}"
CONTROL_PORT="${15}"
DUMP_FILE="${16}"
START_TIME="${17}"

# ---------------- Biến runtime ----------------
RUN_DATE=$(date +%F)  # Ngày hiện tại
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="${LOG_DIR}/load_to_wh_${TIMESTAMP}.log"
LOCK_FILE="${LOCK_DIR}/load_to_wh_${RUN_DATE}.lock"
RETRY_FILE="${LOCK_DIR}/retry_count_${RUN_DATE}.txt"
MAX_RETRIES=3          # Số lần retry tối đa
RETRY_INTERVAL=5       # Khoảng cách retry (phút)

# ---------------- Hàm log message ----------------
log_message() {
    local level=$1
    shift
    local message="$@"
    # Ghi log cả stdout và file
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [${level}] ${message}" | tee -a "$LOG_FILE"
}

# ---------------- Hàm lấy số lần retry hiện tại ----------------
get_retry_count() {
    if [ -f "$RETRY_FILE" ]; then
        cat "$RETRY_FILE"
    else
        echo "0"
    fi
}

# ---------------- Hàm tăng số lần retry ----------------
increment_retry_count() {
    local current=$(get_retry_count)
    local new_count=$((current + 1))
    echo "$new_count" > "$RETRY_FILE"
    echo "$new_count"
}

# ---------------- Hàm reset retry ----------------
reset_retry_count() {
    rm -f "$RETRY_FILE"
    log_message "INFO" "Đã reset số lần retry"
}

# ---------------- Hàm clean up cron tạm ----------------
cleanup_retry_cron() {
    crontab -l 2>/dev/null | grep -v "# RETRY_TEMP_JOB" | crontab -
    log_message "INFO" "Đã xóa cron tạm của retry"
}

# ---------------- Hàm kiểm tra trạng thái load thành công trong db_control ----------------
check_success_in_db() {
    local status
    status=$(mysql -h"$CONTROL_IP" -P"$CONTROL_PORT" -u"$CONTROL_USER" -p"$CONTROL_PASS" -D"$CONTROL_DB" -se \
        "SELECT status FROM load_to_wh_log
         WHERE data_date='$DATE_PARAM'
         ORDER BY log_id DESC
         LIMIT 1;" 2>/dev/null)
    [ "$status" = "Success" ]
}

# ---------------- Kiểm tra lock file có instance khác đang chạy hay không? ----------------
if [ -f "$LOCK_FILE" ]; then
    PID=$(cat "$LOCK_FILE")
    if ps -p $PID > /dev/null 2>&1; then
        log_message "WARN" "Một instance khác đang chạy (PID: $PID). Thoát."
        exit 1
    else
        log_message "INFO" "Xóa lock file cũ"
        rm -f "$LOCK_FILE"
    fi
fi
echo $$ > "$LOCK_FILE"

# ---------------- Main Execution ----------------
log_message "INFO" "=========================================="
log_message "INFO" "Load to WH - Advanced Retry Wrapper"
log_message "INFO" "Run date: $RUN_DATE"
log_message "INFO" "=========================================="

# ---------------- Kiểm tra xem dữ liệu đã load thành công chưa trong db_control? ----------------
if check_success_in_db; then
    log_message "INFO" "✓ Dữ liệu đã load thành công hôm nay"
    reset_retry_count
    cleanup_retry_cron
    rm -f "$LOCK_FILE"
    exit 0
fi

# ---------------- Bước 7: Lấy số lần retry hiện tại ----------------
CURRENT_RETRY=$(get_retry_count)
log_message "INFO" "Retry hiện tại: $CURRENT_RETRY/$MAX_RETRIES"

# ---------------- Hàm tạo cron tạm để retry ----------------
schedule_retry() {
    local retry_num=$1
    local next_time=$(date -d "+${RETRY_INTERVAL} minutes" '+%H:%M')
    local next_hour=${next_time:0:2}
    local next_min=${next_time:3:2}

    log_message "INFO" "Lên lịch retry #${retry_num} lúc ${next_time}"

    # Thêm cron tạm
    (crontab -l 2>/dev/null | grep -v "# RETRY_TEMP_JOB"; \
     echo "${next_min} ${next_hour} * * * ${SCRIPT_PATH} \"$CONFIG_XML\" \"$DATE_PARAM\" \"$WH_DB\" \"$WH_USER\" \"$WH_PASS\" \"$WH_IP\" \"$WH_PORT\" \"$WH_SSH_USER\" \"$REMOTE_PATH\" \"$DUMP_FOLDER\" \"$CONTROL_DB\" \"$CONTROL_USER\" \"$CONTROL_PASS\" \"$CONTROL_IP\" \"$CONTROL_PORT\" \"$DUMP_FILE\" \"$START_TIME\" >> ${LOG_FILE} 2>&1 # RETRY_TEMP_JOB") | crontab -
}

# ---------------- Bước 8: Chạy script load_to_wh.sh ----------------
log_message "INFO" "Chạy script load_to_wh.sh"
bash "$SCRIPT_PATH" "$CONFIG_XML" "$DATE_PARAM" "$WH_DB" "$WH_USER" "$WH_PASS" "$WH_IP" "$WH_PORT" "$WH_SSH_USER" "$REMOTE_PATH" "$DUMP_FOLDER" "$CONTROL_DB" "$CONTROL_USER" "$CONTROL_PASS" "$CONTROL_IP" "$CONTROL_PORT" "$DUMP_FILE" "$START_TIME" 2>&1 | tee -a "$LOG_FILE"
EXIT_CODE=${PIPESTATUS[0]}

# ---------------- Kiểm tra xem trong bảng load_to_wh_log đã có status = ' Success hay chưa?" sau khi chạy script load_to_wh.sh  ----------------
if check_success_in_db && [ $EXIT_CODE -eq 0 ]; then
    log_message "INFO" "✓ Load dữ liệu thành công"
    reset_retry_count
    cleanup_retry_cron
    rm -f "$LOCK_FILE"
    exit 0
else
    NEW_RETRY=$(increment_retry_count)
    log_message "ERROR" "✗ Load thất bại. Retry #$NEW_RETRY/$MAX_RETRIES sẽ được lên lịch"

    if [ "$NEW_RETRY" -lt "$MAX_RETRIES" ]; then
        schedule_retry "$NEW_RETRY"
        log_message "INFO" "Retry tiếp theo sau $RETRY_INTERVAL phút"
    else
        log_message "ERROR" "✗✗✗ Đã vượt quá số lần retry tối đa ✗✗✗"
        log_message "ERROR" "Cần can thiệp thủ công!"
        cleanup_retry_cron
    fi
    rm -f "$LOCK_FILE"
    exit 1
fi

