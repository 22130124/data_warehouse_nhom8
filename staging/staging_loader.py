#!/usr/bin/env python3
import os, sys, csv, glob, argparse, mysql.connector
from datetime import datetime, date
import xml.etree.ElementTree as ET
import logging

class StagingLoader:
    def __init__(self, config_path):
        self.config = self._parse_config(config_path)
        self.conn = None
        self._setup_logging()

    def _parse_config(self, path):
        tree = ET.parse(path)
        root = tree.getroot()
        db = root.find('.//database/control')
        return {
            'host': db.find('host').text, 'port': int(db.find('port').text),
            'user': db.find('user').text, 'password': db.find('password').text,
            'raw_path': root.find('.//extract/raw_data_path').text,
            'log_path': root.find('.//load/base_path').text + '/logs'
        }

    def _setup_logging(self):
        logging.basicConfig(filename=f"{self.config['log_path']}/loader.log", level=logging.INFO, 
                            format='%(asctime)s %(message)s')

    def connect(self):
        self.conn = mysql.connector.connect(
            host=self.config['host'], port=self.config['port'],
            user=self.config['user'], password=self.config['password'],
            autocommit=True
        )

    def log_to_db(self, status, filename, filepath, rows=0, msg=""):
        cursor = self.conn.cursor()
        try:
            if status == 'RUNNING':
                sql = "INSERT INTO db_control.load_log (load_date, file_name, file_path, start_time, status) VALUES (CURDATE(), %s, %s, NOW(), 'RUNNING')"
                cursor.execute(sql, (filename, filepath))
                return cursor.lastrowid
            else:
                sql = "UPDATE db_control.load_log SET status=%s, end_time=NOW(), rows_loaded=%s, message=%s WHERE log_id=%s"
                cursor.execute(sql, (status, rows, msg, self.log_id))
        finally:
            cursor.close()

    def run(self, source_id, run_date):
        self.connect()
        date_str = run_date.strftime('%Y-%m-%d')
        
        # --- CẬP NHẬT PATTERN TẠI ĐÂY ---
        # Cũ: .../date=YYYY-MM-DD/*.csv
        # Mới: .../date=YYYY-MM-DD/{source_id}_*.csv (Để bắt được dạng {source_id}_{HHMMSS}.csv)
        path = f"{self.config['raw_path']}/source={source_id}/date={date_str}/{source_id}_*.csv"
        
        files = glob.glob(path)
        
        print(f"Searching in: {path}")
        print(f"Found {len(files)} files for {date_str}")
        
        cursor = self.conn.cursor()
        
        # Clear Temp Table
        cursor.execute("TRUNCATE TABLE db_staging.staging_topcv_jobs_temp")

        for f in files:
            fname = os.path.basename(f)
            self.log_id = self.log_to_db('RUNNING', fname, f)
            try:
                with open(f, 'r', encoding='utf-8-sig') as csvfile:
                    reader = csv.DictReader(csvfile)
                    data = []
                    for row in reader:
                        data.append((
                            row.get('job_id'), row.get('job_title'), row.get('company_name'),
                            row.get('salary'), row.get('location'), row.get('experience_required'),
                            row.get('posted_time'), row.get('job_url'), row.get('extracted_date')
                        ))
                    
                    sql = """INSERT INTO db_staging.staging_topcv_jobs_temp 
                             (job_id, job_title, company_name, salary, location, experience_required, posted_time, job_url, extracted_date) 
                             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                    if data:
                        cursor.executemany(sql, data)
                
                self.log_to_db('SUCCESS', fname, f, len(data), "Loaded to temp")
                print(f"Loaded {len(data)} rows from {fname}")
            except Exception as e:
                self.log_to_db('FAILED', fname, f, 0, str(e))
                print(f"Error loading {fname}: {e}")

if __name__ == "__main__":    
    # KHỞI TẠO BỘ ĐỌC THAM SỐ
    parser = argparse.ArgumentParser(description='Staging Loader Script')
    
    # ĐỊNH NGHĨA CÁC THAM SỐ CẦN THIẾT
    parser.add_argument('--config', required=True, help='Path to config.xml')
    parser.add_argument('--source_id', required=True, help='Source ID (e.g., topcv_jobs)')
    parser.add_argument('--date', required=False, help='Date YYYY-MM-DD')

    # LẤY GIÁ TRỊ
    args = parser.parse_args()

    # XỬ LÝ NGÀY THÁNG
    if args.date:
        try:
            run_date = datetime.strptime(args.date, '%Y-%m-%d').date()
        except ValueError:
            print(f"Error: Invalid date format {args.date}. Use YYYY-MM-DD.")
            sys.exit(1)
    else:
        run_date = date.today()

    # CHẠY LOADER
    # args.config sẽ chứa đường dẫn file thật (/opt/dm/.../config.xml)
    loader = StagingLoader(args.config)
    loader.run(args.source_id, run_date)