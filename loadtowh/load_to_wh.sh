#!/bin/bash

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

# Hàm log vào db_control
log_to_control() {
    local status="$1" message="$2" rows="$3"
    mysql -h"$CONTROL_IP" -P"$CONTROL_PORT" -u"$CONTROL_USER" -p"$CONTROL_PASS" -D"$CONTROL_DB" \
        -e "INSERT INTO load_to_wh_log(execution_date,data_date,status,rows_processed,start_time,end_time,message) VALUES(CURDATE(),'$DATE_PARAM','$status',$rows,FROM_UNIXTIME($START_TIME/1000),NOW(),'$message');" 2>/dev/null || true
}

# Kiểm tra dump file
if [ ! -f "$DUMP_FILE" ]; then
    echo "khong the dump du lieu tu table staging_topcv_jobs"
    log_to_control "Failed" "khong the dump du lieu tu table staging_topcv_jobs" 0
    exit 1
fi

# Bước 9. SCP file staging_<date>.sql từ server staging sang server warehouse
echo "Copying $DUMP_FILE → ${WH_SSH_USER}@${WH_IP}:${REMOTE_PATH}/staging_${DATE_PARAM}.sql"
scp "$DUMP_FILE" "${WH_SSH_USER}@${WH_IP}:${REMOTE_PATH}/staging_${DATE_PARAM}.sql" || { log_to_control "Failed" "khong the copy file staging_<date>.sql tu staging sang warehouse" 0; exit 1; }

# Bước 10. SSH vào warehouse và load dữ liệu từ file staging_<date>.sql vào table job_temp trên db_warehouse
RESULT=$(ssh -o "SetEnv HISTIGNORE=*" "${WH_SSH_USER}@${WH_IP}" "
mysql -u\"${WH_USER}\" -p\"${WH_PASS}\" -D\"${WH_DB}\" -sse \"
DROP TABLE IF EXISTS job_temp;
CREATE TABLE job_temp (
    job_id varchar(50) NULL,
    job_title varchar(255) NOT NULL,
    company_name varchar(255) NOT NULL,
    salary varchar(100) NULL,
    location varchar(255) NULL,
    experience_required varchar(100) NULL,
    posted_time varchar(50) NULL,
    job_url varchar(500) NULL,
    extracted_date date NULL,
    date_id bigint NULL
);
SOURCE ${REMOTE_PATH}/staging_${DATE_PARAM}.sql;

# Bước 11. update và insert dữ liệu từ bảng job_temp sang bảng job
# Update
UPDATE job w
JOIN job_temp t
  ON w.job_title = t.job_title
 AND w.company_name = t.company_name
SET w.expired = CURDATE()
WHERE w.expired = '9999-12-31'
  AND (w.salary <> t.salary
       OR w.location <> t.location
       OR w.experience_required <> t.experience_required
       OR w.posted_time <> t.posted_time
       OR w.job_url <> t.job_url);
SELECT ROW_COUNT();

# Insert
INSERT INTO job (job_title, company_name, salary, location, experience_required, posted_time, job_url, extracted_date, date_id, expired, is_deleted)
SELECT t.job_title, t.company_name, t.salary, t.location, t.experience_required, t.posted_time, t.job_url, t.extracted_date, t.date_id, '9999-12-31', FALSE
FROM job_temp t
WHERE NOT EXISTS (
    SELECT 1 FROM job w
    WHERE w.job_title = t.job_title
      AND w.company_name = t.company_name
      AND w.expired = '9999-12-31'
);
SELECT ROW_COUNT();
\" 
")

if [ $? -ne 0 ]; then
    echo "khong the insert hay update bang job_temp vao bang job"
    log_to_control "Failed" "khong the insert hay update bang job_temp vao bang job" 0
    exit 1
fi

# Lấy số dòng UPDATE và INSERT
UPDATED=$(echo "$RESULT" | sed -n '1p')
INSERTED=$(echo "$RESULT" | sed -n '2p')
TOTAL=$((UPDATED + INSERTED))

# Bước 12: Ghi log vào bảng load_to_wh_log
log_to_control "Success" "load du lieu vao warehouse thanh cong" $TOTAL
echo "load du lieu vao warehouse thanh cong"
echo "Updated rows: $UPDATED, Inserted rows: $INSERTED, Total processed: $TOTAL"

