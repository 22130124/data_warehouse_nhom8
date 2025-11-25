import pandas as pd
import mysql.connector

# Config hardcode cho nhanh vì chỉ chạy 1 lần
DB_CONFIG = {'host':'localhost','user':'ntt','password':'khanh123!','database':'db_staging'}
CSV_PATH = '/opt/dw/staging/load/data/date_dim_without_quarter.csv'

try:
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    print("Reading CSV...")
    df = pd.read_csv(CSV_PATH).where(pd.notnull, None)

    print("Truncating table...")
    cursor.execute("TRUNCATE TABLE date_dim")

    print("Inserting data...")
    sql = """INSERT INTO date_dim (date_sk, full_date, day_since_month_start, day_of_week_calendar, 
             calendar_month_name, day_of_month, day_of_year, week_of_year, is_holiday, day_type)
             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    data = []
    for _, row in df.iterrows():
        data.append((
            row['date_sk'], row['full_date'], row['day_since_2005'], row['day_of_week'],
            row['calendar_month'], row['day_of_month'], row['day_of_year'],
            row['year_week_sunday'], row['holiday'], row['day_type']
        ))

    cursor.executemany(sql, data)
    conn.commit()
    print(f"Success! Imported {len(data)} rows.")
except Exception as e:
    print(f"Error: {e}")
finally:
    if 'conn' in locals(): conn.close()