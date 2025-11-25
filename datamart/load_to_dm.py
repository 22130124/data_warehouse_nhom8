#!/usr/bin/env python3

import mysql.connector
import xml.etree.ElementTree as ET
from datetime import datetime
import logging
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_FILE = "/opt/dw/staging/datamart/logs/datamart.log"

logger = logging.getLogger()
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler(LOG_FILE)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter("%(levelname)s - %(message)s"))

logger.addHandler(file_handler)
logger.addHandler(console_handler)

logger.info("=== START LOAD TO DATAMART JOB ===")

# =========================
# 1. Load config XML
# =========================
CONFIG_FILE = "/opt/dw/staging/config.xml"

try:
    tree = ET.parse(CONFIG_FILE)
    root = tree.getroot()
except Exception as e:
    # 2.1. Log error & exit
    logging.error("Cannot parse config file: %s", str(e))
    sys.exit(1)

# =========================
# 2. Extract database configs
# =========================

def load_db(root, path):
    node = root.find(path)
    return {
        'host': node.find('host').text,
        'port': int(node.find('port').text),
        'user': node.find('user').text,
        'password': node.find('password').text,
        'database': node.find('database').text
    }

# Load each DB config
WAREHOUSE_DB_CONFIG = load_db(root, './database/warehouse')
DATAMART_DB_CONFIG = load_db(root, './database/datamart')
CONTROL_DB_CONFIG = load_db(root, './database/control')

# Read truncate flag
truncate_before_insert = (root.find('./settings/truncateBeforeInsert').text.lower() == 'true')

# Read tables to process
tables = root.findall('.//aggregates/table')
logging.info("Tables found in config: %d", len(tables))

def log_config(cursor, config_file, status):
    cursor.execute(
        """
        INSERT INTO load_to_dm_config (config_file, status, created_at)
        VALUES (%s, %s, %s)
        """,
        (config_file, status, datetime.now())
    )

def log_table(cursor, table_name, status):
    cursor.execute(
        """
        INSERT INTO load_to_dm_log (table_name, status, created_at)
        VALUES (%s, %s, %s)
        """,
        (table_name, status, datetime.now())
    )

# 4. Connect to control DB
control_conn = mysql.connector.connect(**CONTROL_DB_CONFIG)
control_cursor = control_conn.cursor()

try:
    # 5. Log Start status into control DB
    log_config(control_cursor, CONFIG_FILE, "Start")
    control_conn.commit()
    logging.info("Config load logged to DB")

    # 6. Connect to warehouse and datamart DB
    warehouse_conn = mysql.connector.connect(**WAREHOUSE_DB_CONFIG)
    warehouse_cursor = warehouse_conn.cursor(dictionary=True)

    datamart_conn = mysql.connector.connect(**DATAMART_DB_CONFIG)
    datamart_cursor = datamart_conn.cursor()

    # 7. For each table in config
    for table in tables:
        table_name = table.find('name').text
        source = table.find('source').text
        group_by = table.find('groupBy').text
        metrics = [m.text for m in table.find('metrics').findall('metric')]
        metrics_sql = ", ".join(metrics)

        # 8. Log Start status into log table in control DB
        logging.info("Start processing table: %s", table_name)
        log_table(control_cursor, table_name, "Start")
        control_conn.commit()

        try:
            # 9. Query data from warehouse
            query_sql = f"""
                SELECT {group_by}, {metrics_sql}
                FROM {source}
                GROUP BY {group_by}
            """
            warehouse_cursor.execute(query_sql)
            rows = warehouse_cursor.fetchall()

            # 10. Drop and recreate datamart table
            datamart_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

            metric_columns = "\n".join(
                [f"    {m.split(' AS ')[-1]} INT," for m in metrics]
            ).rstrip(',')

            create_sql = f"""
                CREATE TABLE {table_name} (
                    {group_by} VARCHAR(255),
                    {metric_columns}
                )
            """
            datamart_cursor.execute(create_sql)

            # 11. Insert data into datamart
            metric_names = [m.split(" AS ")[-1] for m in metrics]
            insert_cols = ", ".join([group_by] + metric_names)
            placeholder = ", ".join(["%s"] * (1 + len(metric_names)))

            insert_sql = f"""
                INSERT INTO {table_name} ({insert_cols})
                VALUES ({placeholder})
            """

            insert_data = []
            for r in rows:
                row_values = [r[group_by]] + [r[name] for name in metric_names]
                insert_data.append(tuple(row_values))

            datamart_cursor.executemany(insert_sql, insert_data)
            datamart_conn.commit()

            # 12. Log Success status
            log_table(control_cursor, table_name, "Success")
            control_conn.commit()
            logging.info("Success table %s (%d rows)", table_name, len(rows))

        except Exception as e:
            # 12.1. Log Fail status
            logging.error("Fail table %s: %s", table_name, str(e))
            log_table(control_cursor, table_name, "Fail")
            control_conn.commit()

    # 13. Log Success status into config log
    log_config(control_cursor, CONFIG_FILE, "Success")
    control_conn.commit()
    logging.info("Config finished successfully")

except Exception as e:
    # 13.1. Log Fail status into config log
    log_config(control_cursor, CONFIG_FILE, "Fail")
    control_conn.commit()
    # 13.1. Log error into datamart log file
    logging.error("Job FAILED: %s", str(e))

finally:
    # 14. Close all DB connections
    try:
        warehouse_cursor.close()
        warehouse_conn.close()
        datamart_cursor.close()
        datamart_conn.close()
    except:
        pass

    control_cursor.close()
    control_conn.close()

# 15 End log
logging.info("=== END LOAD TO DATAMART JOB ===")