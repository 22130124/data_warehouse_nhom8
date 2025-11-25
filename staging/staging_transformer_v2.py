#!/usr/bin/env python3
import sys, re, mysql.connector, argparse
from datetime import datetime, timedelta, date
import xml.etree.ElementTree as ET

class StagingTransformer:
    def __init__(self, config_path):
        tree = ET.parse(config_path)
        root = tree.getroot()
        db = root.find('.//database/control')
        self.config = {
            'host': db.find('host').text, 
            'user': db.find('user').text,
            'password': db.find('password').text, 
            'port': int(db.find('port').text)
        }
        self.conn = None
        self.PROCESS_NAME = 'Staging_Transform_TopCV'
        self.date_lookup = {}

    def connect(self):
        """Thiết lập kết nối Database"""
        try:
            if self.conn is None or not self.conn.is_connected():
                self.conn = mysql.connector.connect(**self.config, autocommit=True)
        except Exception as e:
            print(f"Connection Failed: {e}")
            sys.exit(1)

    def get_process_id(self):
        """Lấy ID process an toàn (Tự động connect nếu chưa có)"""
        # --- FIX LỖI NoneType Ở ĐÂY ---
        if self.conn is None:
            self.connect()
        # ------------------------------

        c = self.conn.cursor()
        try:
            sql = "SELECT process_id FROM db_control.process_config WHERE process_name = %s"
            c.execute(sql, (self.PROCESS_NAME,))
            result = c.fetchone()
            
            if result:
                return result[0]
            else:
                print(f"Process '{self.PROCESS_NAME}' not found. Creating...")
                sql_insert = "INSERT INTO db_control.process_config (process_name, process_type, description) VALUES (%s, 'TRANSFORM', 'Auto-created')"
                c.execute(sql_insert, (self.PROCESS_NAME,))
                return c.lastrowid
        finally:
            c.close()

    def clean_salary(self, text):
        if not text: return 0, 0
        text = text.lower().replace(',', '').replace('.', '')
        if 'thỏa thuận' in text: return 0, 0
        nums = re.findall(r'\d+', text)
        multi = 1000000 
        if 'tới' in text and nums: return 0, int(nums[0]) * multi
        if 'trên' in text and nums: return int(nums[0]) * multi, 0
        if len(nums) >= 2: return int(nums[0]) * multi, int(nums[1]) * multi
        return 0, 0

    def calc_posted_date(self, text, extract_date_str):
        delta = 0
        s = text.lower()
        if 'hôm qua' in s: delta = 1
        elif 'ngày trước' in s: delta = int(re.findall(r'\d+', s)[0])
        elif 'tuần trước' in s: delta = int(re.findall(r'\d+', s)[0]) * 7
        
        try:
            base = datetime.strptime(extract_date_str, '%Y-%m-%d').date()
            return base - timedelta(days=delta)
        except:
            return None
            
    def load_date_lookup(self):
        """Load date_sk từ DB vào RAM để tra cứu"""
        print("Loading Date Dimension...")
        self.connect()
        c = self.conn.cursor()
        c.execute("SELECT full_date, date_sk FROM db_staging.date_dim")
        for row in c.fetchall():
            # Key là chuỗi ngày '2025-11-24', Value là ID (ví dụ: 325)
            self.date_lookup[str(row[0])] = row[1]
        c.close()

    def run(self):
        # Đảm bảo kết nối trước khi làm bất cứ điều gì
        self.connect()
        self.load_date_lookup()
        
        # 1. Lấy ID Process
        process_id = self.get_process_id()
        
        c = self.conn.cursor()
        log_id = None
        
        # Ghi Log Start
        try:
            c.execute("INSERT INTO db_control.process_log (process_id, execution_date, status) VALUES (%s, NOW(), 'Running')", (process_id,))
            log_id = c.lastrowid
        except Exception as e:
            print(f"Cannot write log start: {e}")
        
        try:
            # 2. Đọc dữ liệu từ bảng Tạm
            c.execute("SELECT * FROM db_staging.staging_topcv_jobs_temp")
            rows = c.fetchall()
            
            if not rows:
                print("No data in temp table.")
                return

            cols = [i[0] for i in c.description]
            
            count = 0
            for r in rows:
                row = dict(zip(cols, r))
                
                # Transform
                p_date_obj = self.calc_posted_date(row['posted_time'], row['extracted_date'])
                posted_time_clean = p_date_obj.strftime('%Y-%m-%d') if p_date_obj else None
                
                date_str = row['extracted_date']
                if date_str in self.date_lookup:
                    date_id = self.date_lookup[date_str] # Lấy ID chính xác từ DB (ví dụ: 325)
                else:
                    date_id = None # Hoặc ID mặc định
                    print(f"Warning: Date {date_str} not found in date_dim")

                # Load vào bảng job (của teammate)
                # Chú ý: Sửa tên bảng 'db_staging.job' nếu thực tế khác
                sql = """
                    INSERT INTO db_staging.staging_topcv_jobs
                    (job_id, job_title, company_name, salary, location, 
                     experience_required, posted_time, job_url, extracted_date, date_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE 
                        job_title = VALUES(job_title), 
                        salary = VALUES(salary),
                        posted_time = VALUES(posted_time),
                        date_id = VALUES(date_id)
                """
                
                c.execute(sql, (
                    row['job_id'], row['job_title'], row['company_name'], 
                    row['salary'], row['location'], row['experience_required'],
                    posted_time_clean, 
                    row['job_url'], row['extracted_date'], date_id
                ))
                count += 1
            
            # Update Log Success
            if log_id:
                c.execute("UPDATE db_control.process_log SET status='Success', error_message=%s WHERE log_id=%s", 
                          (f"Loaded {count} rows into 'job'", log_id))
            print(f"Success. Loaded {count} rows.")
            
        except Exception as e:
            # Update Log Failed
            if log_id:
                c.execute("UPDATE db_control.process_log SET status='Failed', error_message=%s WHERE log_id=%s", 
                          (str(e), log_id))
            print(f"Error: {e}")
            sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True)
    args = parser.parse_args()
    
    transformer = StagingTransformer(args.config)
    transformer.run()