import boto3
from pathlib import Path
import config

BASE_DIR = Path(r"C:\Users\adity\Downloads\retail_sales_pipeline")
PROCESSED_FILE = BASE_DIR / "data" / "processed_sales.csv"

def upload_to_s3():
    s3 = boto3.client("s3", region_name=config.AWS_REGION)
    try:
        s3.upload_file(str(PROCESSED_FILE), config.AWS_BUCKET, "processed/processed_sales.csv")
        print(f"✅ Uploaded successfully to s3://{config.AWS_BUCKET}/processed/processed_sales.csv")
    except Exception as e:
        print("❌ Upload failed:", e)

if __name__ == "__main__":
    upload_to_s3()
