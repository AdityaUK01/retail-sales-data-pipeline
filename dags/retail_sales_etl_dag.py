from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import boto3
import pandas as pd
import io
import sqlalchemy
import logging
import os

# Config: change S3_BUCKET and keys if you will use a custom prefix
S3_BUCKET = os.environ.get("PROJECT_S3_BUCKET", "aditya-mwaa-dags-2025")
RAW_KEY = "data/raw_sales.csv"           # put small sample CSV in this path in your S3 bucket
PROCESSED_KEY = "processed/processed_sales.csv"

default_args = {
    "owner": "aditya",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def extract_from_s3(**context):
    """Download raw CSV from S3 into a pandas DataFrame"""
    logging.info("Starting extract_from_s3")
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=S3_BUCKET, Key=RAW_KEY)
    raw_bytes = obj["Body"].read()
    df = pd.read_csv(io.BytesIO(raw_bytes))
    # store to XCom for small datasets (fine for demo)
    context['ti'].xcom_push(key='raw_df_csv', value=df.to_csv(index=False))
    logging.info("Extracted %d rows", len(df))
    return len(df)

def transform_df(**context):
    """Read CSV from XCom, clean, standardise types, and push processed CSV to XCom"""
    logging.info("Starting transform_df")
    raw_csv = context['ti'].xcom_pull(key='raw_df_csv')
    df = pd.read_csv(io.StringIO(raw_csv))
    # Example transforms — adapt to your real columns
    # Standardize column names
    df.columns = [c.strip() for c in df.columns]
    # Convert ORDERDATE to datetime (try multiple formats)
    df['ORDERDATE'] = pd.to_datetime(df['ORDERDATE'], errors='coerce')
    # Drop rows with bad dates
    df = df.dropna(subset=['ORDERDATE'])
    # Example derived column: SALES numeric
    df['SALES'] = pd.to_numeric(df['SALES'], errors='coerce').fillna(0)
    # Save processed CSV to XCom (small dataset ok). For bigger datasets, upload to S3 directly.
    processed_csv = df.to_csv(index=False)
    context['ti'].xcom_push(key='processed_df_csv', value=processed_csv)
    logging.info("Transformed dataframe rows: %d", len(df))
    return len(df)

def upload_processed_to_s3(**context):
    """Take processed CSV from XCom and upload to S3"""
    logging.info("Starting upload_processed_to_s3")
    processed_csv = context['ti'].xcom_pull(key='processed_df_csv')
    s3 = boto3.client("s3")
    s3.put_object(Bucket=S3_BUCKET, Key=PROCESSED_KEY, Body=processed_csv.encode('utf-8'))
    logging.info("Uploaded processed CSV to s3://%s/%s", S3_BUCKET, PROCESSED_KEY)
    return PROCESSED_KEY

def load_into_mysql(**context):
    """Load processed CSV into MySQL using Airflow connection 'retail_mysql'"""
    logging.info("Starting load_into_mysql")
    processed_csv = context['ti'].xcom_pull(key='processed_df_csv')
    df = pd.read_csv(io.StringIO(processed_csv))

    # Get DB connection from Airflow connections (create a connection named 'retail_mysql' in MWAA UI)
    conn = BaseHook.get_connection("retail_mysql")
    # Build SQLAlchemy engine URL
    # conn.host, conn.login (user), conn.password, conn.port, conn.schema (database)
    user = conn.login
    pwd = conn.password
    host = conn.host
    port = conn.port or 3306
    db = conn.schema
    engine_url = f"mysql+mysqlconnector://{user}:{pwd}@{host}:{port}/{db}"
    engine = sqlalchemy.create_engine(engine_url)

    # Write to table retail_sales (creates if not exists). Adjust dtype mapping as needed.
    df.to_sql(name="retail_sales", con=engine, if_exists="append", index=False, method="multi", chunksize=500)
    logging.info("Inserted %d rows into retail_sales table", len(df))
    return len(df)

with DAG(
    dag_id="retail_sales_etl_dag",
    default_args=default_args,
    description="Retail Sales ETL for MWAA (S3 -> transform -> MySQL)",
    schedule_interval="@daily",
    start_date=datetime(2025, 11, 1),
    catchup=False,
    tags=["retail", "etl", "s3", "mysql"],
) as dag:

    extract = PythonOperator(
        task_id="extract_from_s3",
        python_callable=extract_from_s3,
        provide_context=True
    )

    transform = PythonOperator(
        task_id="transform_df",
        python_callable=transform_df,
        provide_context=True
    )

    upload = PythonOperator(
        task_id="upload_processed_to_s3",
        python_callable=upload_processed_to_s3,
        provide_context=True
    )

    load = PythonOperator(
        task_id="load_into_mysql",
        python_callable=load_into_mysql,
        provide_context=True
    )

    extract >> transform >> upload >> load
