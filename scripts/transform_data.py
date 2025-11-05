# load_data.py
import pandas as pd
import mysql.connector
from mysql.connector import errorcode
from pathlib import Path
import config

BASE_DIR = Path(r"C:\Users\adity\Downloads\retail_sales_pipeline")
PROCESSED_FILE = BASE_DIR / "data" / "processed_sales.csv"

def to_mysql_datetime(val):
    """
    Accepts a pandas Timestamp or a string and returns a string in
    MySQL DATETIME format 'YYYY-MM-DD HH:MM:SS' or None if invalid.
    """
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        # If it's already a pandas Timestamp
        if hasattr(val, "to_pydatetime"):
            return val.to_pydatetime().strftime("%Y-%m-%d %H:%M:%S")
        # If it's a string, try to parse then format
        ts = pd.to_datetime(val, errors="coerce", infer_datetime_format=True)
        if pd.isna(ts):
            return None
        return ts.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

def safe_int(val):
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)) or val == "":
            return None
        return int(val)
    except Exception:
        try:
            return int(float(val))
        except Exception:
            return None

def safe_float(val):
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)) or val == "":
            return None
        return float(val)
    except Exception:
        return None

def load_data():
    if not PROCESSED_FILE.exists():
        print(f"❗ Processed file not found: {PROCESSED_FILE}")
        return

    # Read processed CSV
    df = pd.read_csv(PROCESSED_FILE)
    print(f"✅ Read processed CSV: {len(df)} rows")

    # Connect to MySQL server (without DB) to ensure DB exists
    try:
        tmp_conn = mysql.connector.connect(
            host=config.MYSQL_HOST,
            user=config.MYSQL_USER,
            password=config.MYSQL_PASSWORD,
            port=config.MYSQL_PORT
        )
    except mysql.connector.Error as err:
        print("❌ Unable to connect to MySQL server:", err)
        return

    tmp_cursor = tmp_conn.cursor()
    try:
        tmp_cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{config.MYSQL_DB}`")
        print(f"✅ Ensured database exists: {config.MYSQL_DB}")
    except mysql.connector.Error as err:
        print("❌ Error creating database:", err)
        tmp_conn.close()
        return
    tmp_cursor.close()
    tmp_conn.close()

    # Connect to the target database
    try:
        conn = mysql.connector.connect(
            host=config.MYSQL_HOST,
            user=config.MYSQL_USER,
            password=config.MYSQL_PASSWORD,
            database=config.MYSQL_DB,
            port=config.MYSQL_PORT
        )
    except mysql.connector.Error as err:
        print("❌ Failed to connect to MySQL DB:", err)
        return

    cursor = conn.cursor()

    # Create table with DATETIME type for orderdate
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS retail_sales (
        ordernumber INT,
        orderdate DATETIME,
        productline VARCHAR(255),
        country VARCHAR(255),
        quantityordered INT,
        priceeach DOUBLE,
        sales DOUBLE,
        total_sales DOUBLE,
        dealsize VARCHAR(50)
    )
    """
    try:
        cursor.execute(create_table_sql)
        print("✅ Verified/created table: retail_sales")
    except mysql.connector.Error as err:
        print("❌ Error creating table:", err)
        cursor.close()
        conn.close()
        return

    insert_sql = """
    INSERT INTO retail_sales
    (ordernumber, orderdate, productline, country, quantityordered, priceeach, sales, total_sales, dealsize)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    rows_inserted = 0
    for idx, row in df.iterrows():
        # Map columns robustly (handle missing columns)
        ordernumber = safe_int(row.get("ordernumber") or row.get("ordernumber".lower()))
        # orderdate might already be formatted string or pandas Timestamp
        orderdate_val = row.get("orderdate")
        orderdate_val = to_mysql_datetime(orderdate_val)

        productline = row.get("productline") if "productline" in row.index else None
        country = row.get("country") if "country" in row.index else None
        quantityordered = safe_int(row.get("quantityordered") if "quantityordered" in row.index else row.get("quantityordered".lower()))
        priceeach = safe_float(row.get("priceeach") if "priceeach" in row.index else row.get("priceeach".lower()))
        sales = safe_float(row.get("sales") if "sales" in row.index else None)
        total_sales = safe_float(row.get("total_sales") if "total_sales" in row.index else None)
        dealsize = row.get("dealsize") if "dealsize" in row.index else None

        try:
            cursor.execute(insert_sql, (
                ordernumber,
                orderdate_val,
                productline,
                country,
                quantityordered,
                priceeach,
                sales,
                total_sales,
                dealsize
            ))
            rows_inserted += 1
        except mysql.connector.Error as err:
            print(f"⚠️ Row {idx} insert failed: {err} — skipping row.")
            continue

    conn.commit()
    print(f"✅ Inserted {rows_inserted} rows into `retail_sales`")

    cursor.close()
    conn.close()
    print("✅ MySQL connection closed.")

if __name__ == "__main__":
    load_data()
