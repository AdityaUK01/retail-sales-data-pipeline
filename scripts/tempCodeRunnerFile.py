import pandas as pd
from pathlib import Path

BASE_DIR = Path(r"C:\Users\adity\Downloads\retail_sales_pipeline")
DATA_FILE = BASE_DIR / "data" / "sample_sales_data.csv"

def extract_data():
    # Use latin1 encoding for Kaggle sample sales data
    df = pd.read_csv(DATA_FILE, encoding="latin1")
    print(f"✅ Extracted {len(df)} rows from {DATA_FILE}")
    print(df.head())
    return df

if __name__ == "__main__":
    extract_data()
