import pandas as pd
import mysql.connector
from mysql.connector import errorcode
from pathlib import Path
import config  # import your MySQL credentials from config.py

BASE_DIR = Path(r"C:\Users\adity\Downloads\retail_sales_pipeline")
PROCESSED_FILE = BASE_DIR / "data" / "processed_sales.csv"

def load_data():
    # Step 1: Read processed CSV
    df = pd.read_csv(PROCESSED_FILE)
    print(f"✅ Loaded processed CSV with {len(df)} rows.")

    # Step 2: Connect to MySQL (create DB if missing)
    try:
        conn = mysql.connector.connect(
            host=config.MYSQL_HOST,
            user=config.MYSQL_USER,
            password=config.MYSQL_PASSWORD,
            port=config.MYSQL_PORT
        )
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {config.MYSQL_DB}")
        print(f"✅ Verified database: {config.MYSQL_DB}")
    except mysql.connector.Error as err:
        print(f"❌ Database connection error: {err}")
        return

    conn.database = config.MYSQL_DB

    # Step 3: Create table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS retail_sales (
        ordernumber INT,
        orderdate DATE,
        productline VARCHAR(100),
        country VARCHAR(100),
        quantityordered INT,
        priceeach FLOAT,
        sales FLOAT,
        total_sales FLOAT,
        dealsize VARCHAR(20)
    )
    """)
    print("✅ Verified table: retail_sales")

    # Step 4: Insert rows
    insert_query = """
        INSERT INTO retail_sales
        (ordernumber, orderdate, productline, country,
         quantityordered, priceeach, sales, total_sales, dealsize)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    for _, row in df.iterrows():
        cursor.execute(insert_query, tuple(row))

    conn.commit()
    print(f"✅ Inserted {len(df)} rows into retail_sales table")

    # Step 5: Close connection
    cursor.close()
    conn.close()
    print("✅ Data load complete and MySQL connection closed.")

if __name__ == "__main__":
    load_data()
