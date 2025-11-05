# 🧩 Retail Sales Data Warehouse / Cloud Pipeline

### 🚀 Project Overview
This project automates the **ETL (Extract, Transform, Load)** workflow for a retail sales dataset using **Python, MySQL, Apache Airflow, and AWS S3**.  
It demonstrates a real-world **data engineering pipeline** that ingests raw data, transforms it, and loads it into a data warehouse — ready for analytics or dashboards.

---

### 🏗️ Project Architecture

1. **Extract:** Reads raw retail sales data (CSV) using Python `pandas`.
2. **Transform:** Cleans, normalizes, and converts date formats for compatibility with SQL.
3. **Load:** Inserts transformed data into a MySQL data warehouse.
4. **Airflow DAG:** Automates and schedules the ETL pipeline with daily runs.
5. **AWS S3 Integration:** Stores raw and processed data in S3 for cloud-based access.

---

### 🛠️ Tech Stack

| Layer | Tools & Technologies |
|-------|----------------------|
| Language | Python 3 |
| Orchestration | Apache Airflow |
| Database | MySQL |
| Cloud Storage | AWS S3 |
| Libraries | Pandas, SQLAlchemy, MySQL Connector |
| Platform | AWS MWAA (Managed Airflow) |

---

### 📂 Folder Structure
retail_sales_pipeline/
│
├── dags/
│ └── retail_sales_etl_dag.py # Airflow DAG for the ETL workflow
├── scripts/
│ ├── extract_data.py # Extracts raw sales data
│ ├── transform_data.py # Cleans & prepares data
│ └── load_data.py # Loads into MySQL DW
├── docker-compose.yaml # Local Airflow setup (optional)
├── requirements.txt # Python dependencies
└── README.md # Project documentation