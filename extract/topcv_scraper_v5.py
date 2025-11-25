#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TopCV Scraper V7.1 - Full Category Pagination
Mô tả: Cào toàn bộ dữ liệu việc làm IT bằng cách tự động lật trang.
Quy trình: Tuân thủ chặt chẽ Flowchart ELT (Extract - Load to Staging - Log).
"""

import pandas as pd
from datetime import datetime
import time
import os
import hashlib
import mysql.connector
import logging
import argparse
import xml.etree.ElementTree as ET
import tempfile
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ==============================================================================
# KHU VỰC HÀM HỖ TRỢ (HELPER FUNCTIONS)
# ==============================================================================

def parse_config_xml(config_path):
    """Đọc file cấu hình XML"""
    if not os.path.exists(config_path): raise FileNotFoundError(f"Not found: {config_path}")
    tree = ET.parse(config_path); root = tree.getroot()
    db_node = root.find('.//database/control')
    db_config = {'host': db_node.find('host').text, 'port': int(db_node.find('port').text), 'database': db_node.find('database').text, 'user': db_node.find('user').text, 'password': db_node.find('password').text}
    ext_node = root.find('.//extract')
    ext_config = {'driver_path': ext_node.find('driver_path').text, 'log_path': ext_node.find('log_path').text, 'raw_data_path': ext_node.find('raw_data_path').text, 'headless': ext_node.find('.//selenium/headless').text.lower() == 'true'}
    return db_config, ext_config

def setup_logger(log_path, source_id):
    """Thiết lập ghi log"""
    os.makedirs(log_path, exist_ok=True)
    log_file = os.path.join(log_path, f"{source_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    logger = logging.getLogger(__name__); logger.setLevel(logging.INFO); logger.handlers = []
    fh = logging.FileHandler(log_file); fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')); logger.addHandler(fh)
    ch = logging.StreamHandler(); ch.setFormatter(logging.Formatter('%(message)s')); logger.addHandler(ch)
    return logger

def setup_driver(path, headless):
    """Khởi tạo trình duyệt Edge"""
    opts = EdgeOptions()
    if headless: opts.add_argument('--headless')
    opts.add_argument('--no-sandbox'); opts.add_argument('--disable-gpu'); opts.add_argument('--window-size=1920,1080')
    opts.add_argument(f'--user-data-dir={tempfile.mkdtemp()}')
    opts.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    return webdriver.Edge(service=Service(path), options=opts)

# ==============================================================================
# CORE LOGIC: HÀM TRÍCH XUẤT DỮ LIỆU (PAGINATION) - [BƯỚC 6]
# ==============================================================================
def scrape_with_pagination(driver, logger, base_url, source_id, extract_date):
    all_jobs = []
    current_page = 1
    # Đặt giới hạn số trang (TopCV IT thường có khoảng 40-50 trang)
    MAX_PAGES = 3 
    
    logger.info(f">>> [BƯỚC 6] BẮT ĐẦU CÀO DỮ LIỆU ĐA TRANG (Max: {MAX_PAGES})")
    
    while current_page <= MAX_PAGES:
        # Xây dựng URL phân trang: Thêm &page=X vào cuối link gốc
        separator = '&' if '?' in base_url else '?'
        page_url = f"{base_url}{separator}page={current_page}"
        
        logger.info(f"--- Đang quét Trang {current_page}/{MAX_PAGES} ---")
        
        try:
            # 6.1: Truy cập URL trang hiện tại
            driver.get(page_url)
            time.sleep(4) # Chờ load trang
            
            # 6.2: Tìm các thẻ Job Card
            try:
                WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, '.job-item-search-result')))
                cards = driver.find_elements(By.CSS_SELECTOR, '.job-item-search-result')
            except:
                cards = []
            
            # Điều kiện dừng: Nếu trang không có job nào -> Đã hết dữ liệu
            if not cards:
                logger.info(f"-> Trang {current_page} không có dữ liệu. Dừng quét.")
                break 
                
            logger.info(f"-> Tìm thấy {len(cards)} tin trên trang {current_page}")
            
            # 6.3: Bóc tách từng thẻ (Parsing)
            count_ok = 0
            for card in cards:
                try:
                    j_id = card.get_attribute("data-job-id")
                    try: j_title = card.find_element(By.CSS_SELECTOR, '.title a span').text.strip()
                    except: j_title = ""
                    try: j_comp = card.find_element(By.CSS_SELECTOR, '.company .company-name').text.strip()
                    except: j_comp = ""
                    try: j_sal = card.find_element(By.CSS_SELECTOR, '.title-salary').text.strip()
                    except: j_sal = "Thỏa thuận"
                    try: j_loc = card.find_element(By.CSS_SELECTOR, '.address .city-text').text.strip()
                    except: j_loc = ""
                    try: j_exp = card.find_element(By.CSS_SELECTOR, 'label.exp').text.strip()
                    except: j_exp = "Không yêu cầu"
                    try: 
                        raw_time = card.find_element(By.CSS_SELECTOR, '.label-update').text.strip()
                        j_time = raw_time.replace("Đăng", "").strip()
                    except: j_time = ""
                    try: j_url = card.find_element(By.CSS_SELECTOR, '.title a').get_attribute("href")
                    except: j_url = ""
                    try: 
                        tags = [t.text.strip() for t in card.find_elements(By.CSS_SELECTOR, '.tag .item-tag, .tag a') if t.text.strip()]
                        j_tags = ", ".join(tags)
                    except: j_tags = ""
                    try: j_logo = card.find_element(By.CSS_SELECTOR, '.avatar img').get_attribute("src")
                    except: j_logo = ""

                    if j_id and j_title:
                        all_jobs.append({
                            'source_id': source_id, 'job_id': j_id, 'job_title': j_title, 
                            'company_name': j_comp, 'salary': j_sal, 'location': j_loc, 
                            'experience_required': j_exp, 'posted_time': j_time, 
                            'tags': j_tags, 'job_url': j_url, 'company_logo': j_logo,
                            'extracted_date': extract_date, 
                            'extracted_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        })
                        count_ok += 1
                except: continue
            
            logger.info(f"-> Đã lấy được {count_ok} jobs. Tổng cộng: {len(all_jobs)}")
            current_page += 1 # Chuyển sang trang tiếp theo
            
        except Exception as e:
            logger.error(f"Lỗi tại trang {current_page}: {e}")
            break
            
    return all_jobs

# ==============================================================================
# CHƯƠNG TRÌNH CHÍNH (MAIN FLOW)
# ==============================================================================
def main():
    # [INPUT] Nhận thông tin đầu vào
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True); 
    parser.add_argument('--source_id', required=True); 
    parser.add_argument('--date', help='YYYY-MM-DD')
    args = parser.parse_args()
    ext_date = args.date or datetime.now().strftime('%Y-%m-%d')
    
    conn = None; driver = None; log_id = None
    
    try:
        # [BƯỚC 1] Load file config
        print(">>> [BƯỚC 1] Load Config...")
        db_cfg, ext_cfg = parse_config_xml(args.config)
        logger = setup_logger(ext_cfg['log_path'], args.source_id)
        
        # [BƯỚC 2] Kết nối DB Control
        logger.info(">>> [BƯỚC 2] Kết nối DB Control...")
        conn = mysql.connector.connect(**db_cfg)

        if not conn or not conn.is_connected():
            logger.error("Lỗi: Không thể kết nối đến DB Control")
            return
        
        # [BƯỚC 3] Lấy thông tin nguồn cào (Check config table)
        logger.info(">>> [BƯỚC 3] Lấy thông tin bảng extract_config...")
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT * FROM extract_config WHERE src_id=%s", (args.source_id,))
        src_conf = cur.fetchone(); cur.close()
        
        if not src_conf or not src_conf['enabled']: 
            logger.error("Lỗi: Nguồn không tồn tại hoặc bị tắt (Enabled=False)"); return
        
        # [BƯỚC 4] Kiểm tra Status & Tạo Log Running
        logger.info(">>> [BƯỚC 4] Tạo Log Running...")
        cur = conn.cursor()
        cur.execute("INSERT INTO extract_log (src_id, date, status, start_time) VALUES (%s, %s, 'Running', NOW())",
                    (args.source_id, ext_date))
        conn.commit(); log_id = cur.lastrowid; cur.close()
        
        # [BƯỚC 5] Mở trình duyệt (Driver)
        logger.info(">>> [BƯỚC 5] Khởi tạo trình duyệt Edge...")
        driver = setup_driver(ext_cfg['driver_path'], ext_cfg['headless'])
        
        # [BƯỚC 6] Tiến hành trích xuất (Gọi hàm có vòng lặp trang)
        data = scrape_with_pagination(driver, logger, src_conf['src_url'], args.source_id, ext_date)
        
        # [BƯỚC 7] Lưu thông tin vào file CSV (Staging)
        if data:
            logger.info(f">>> [BƯỚC 7] Lưu {len(data)} dòng dữ liệu vào CSV...")
            out_dir = os.path.join(ext_cfg['raw_data_path'], f"source={args.source_id}", f"date={ext_date}")
            os.makedirs(out_dir, exist_ok=True)
            
            f_name = f"{args.source_id}_{datetime.now().strftime('%H%M%S')}.csv"
            f_path = os.path.join(out_dir, f_name)
            
            # Lưu file
            df = pd.DataFrame(data)
            cols = ['source_id', 'job_id', 'job_title', 'company_name', 'salary', 'location', 'experience_required', 'posted_time', 'tags', 'job_url', 'company_logo', 'extracted_date', 'extracted_timestamp']
            df = df[[c for c in cols if c in df.columns]]
            df.to_csv(f_path, index=False, encoding='utf-8-sig')
            logger.info(f"✓ File saved: {f_path}")
            
            # [BƯỚC 8] Xuất kết quả & Update DB Success
            logger.info(">>> [BƯỚC 8] Cập nhật DB Success & Kết thúc.")
            cur = conn.cursor()
            cur.execute("UPDATE extract_log SET status='Success', rows_extracted=%s, file_path=%s, file_size=%s, end_time=NOW() WHERE log_id=%s", (len(data), f_path, os.path.getsize(f_path), log_id))
            conn.commit()
            logger.info(f"✅ HOÀN THÀNH QUY TRÌNH. Tổng số job: {len(data)}")
            
        else:
            # Trường hợp chạy hết các trang mà không có dữ liệu
            logger.warning(">>> [BƯỚC 7] Không tìm thấy dữ liệu nào.")
            cur = conn.cursor()
            cur.execute("UPDATE extract_log SET status='Failed', error_message='No data found', end_time=NOW() WHERE log_id=%s", (log_id,))
            conn.commit()
            
    except Exception as e:
        # Xử lý lỗi toàn cục
        logger.error(f"⛔ LỖI TOÀN CỤC: {e}")
        if conn and log_id:
            cur = conn.cursor()
            cur.execute("UPDATE extract_log SET status='Failed', error_message=%s WHERE log_id=%s", (str(e), log_id))
            conn.commit()
    finally:
        # Dọn dẹp
        if driver: driver.quit()
        if conn: conn.close()

if __name__ == '__main__':
    main()
