from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import subprocess

default_args = {
    'owner': 'aditya',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'retail_sales_etl_dag',
    default_args=default_args,
    description='ETL pipeline for Retail Sales Data (Extract->Transform->Load->Upload)',
    schedule_interval='@daily',
    start_date=datetime(2025, 11, 1),
    catchup=False,
    tags=['retail', 'etl', 'mysql', 's3'],
) as dag:

    def extract():
        subprocess.run(["python", "/opt/airflow/scripts/extract_data.py"], check=True)

    def transform():
        subprocess.run(["python", "/opt/airflow/scripts/transform_data.py"], check=True)

    def load():
        subprocess.run(["python", "/opt/airflow/scripts/load_data.py"], check=True)

    def upload():
        subprocess.run(["python", "/opt/airflow/scripts/upload_to_s3.py"], check=True)

    extract_task = PythonOperator(task_id='extract_task', python_callable=extract)
    transform_task = PythonOperator(task_id='transform_task', python_callable=transform)
    load_task = PythonOperator(task_id='load_task', python_callable=load)
    upload_task = PythonOperator(task_id='upload_task', python_callable=upload)

    extract_task >> transform_task >> load_task >> upload_task
