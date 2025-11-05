# extract_data.py
import pandas as pd
from pathlib import Path

BASE_DIR = Path(r"C:\Users\adity\Downloads\retail_sales_pipeline")
DATA_FILE = BASE_DIR / "data" / "raw_sales.csv"

def extract_data():
    if not DATA_FILE.exists():
        raise FileNotFoundError(f"❌ File not found: {DATA_FILE}")
    df = pd.read_csv(DATA_FILE)
    print(f"✅ Extracted data successfully ({len(df)} rows).")
    return df

if __name__ == "__main__":
    extract_data()
