# extract_data.py (robust version)
from pathlib import Path
import pandas as pd
import chardet

BASE_DIR = Path(r"C:\Users\adity\Downloads\retail_sales_pipeline")
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

def find_csv_file():
    # prefer a known name if present
    preferred = DATA_DIR / "sample_sales_data.csv"
    preferred2 = DATA_DIR / "raw_sales.csv"
    if preferred.exists():
        return preferred
    if preferred2.exists():
        return preferred2
    # otherwise pick first .csv
    all_csv = sorted(DATA_DIR.glob("*.csv"))
    if all_csv:
        return all_csv[0]
    return None

def detect_encoding(path, n_bytes=4000):
    # returns a guessed encoding using chardet if available
    try:
        with open(path, "rb") as f:
            raw = f.read(n_bytes)
        guess = chardet.detect(raw)
        return guess.get("encoding") or "utf-8"
    except Exception:
        return "utf-8"

def extract_data():
    csv_path = find_csv_file()
    if csv_path is None:
        # create a small sample CSV so pipeline can run
        sample_path = DATA_DIR / "sample_sales_data.csv"
        sample = """ORDERNUMBER,QUANTITYORDERED,PRICEEACH,ORDERLINENUMBER,SALES,ORDERDATE,COUNTRY,PRODUCTLINE,DEALSIZE
1,2,85000,1,170000,2025-01-01,India,Electronics,Large
2,10,500,1,5000,2025-01-02,India,Accessories,Small
3,5,1200,1,6000,2025-01-02,Nepal,Accessories,Small
"""
        sample_path.write_text(sample, encoding="utf-8")
        print("No CSV found → sample created at:", sample_path)
        csv_path = sample_path

    print("Using CSV file:", csv_path)

    # try reading with common encodings
    encodings_to_try = ["utf-8", "latin1", "cp1252"]
    # add chardet suggestion first (if installed)
    try:
        guessed = detect_encoding(csv_path)
        if guessed and guessed not in encodings_to_try:
            encodings_to_try.insert(0, guessed)
    except Exception:
        pass

    for enc in encodings_to_try:
        try:
            df = pd.read_csv(csv_path, encoding=enc)
            print(f"✅ Successfully read CSV with encoding: {enc} (rows={len(df)})")
            return df
        except Exception as e:
            print(f"Failed to read with encoding {enc}: {e}")

    # final fallback: read with latin1 and ignore errors
    df = pd.read_csv(csv_path, encoding="latin1", error_bad_lines=False, warn_bad_lines=True)
    print("⚠️ Fallback read with latin1 (some rows may be skipped).")
    return df

if __name__ == "__main__":
    df = extract_data()
    print(df.head())
