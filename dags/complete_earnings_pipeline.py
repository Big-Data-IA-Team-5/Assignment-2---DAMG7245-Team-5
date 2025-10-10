#!/usr/bin/env python3
"""
Complete Earnings Pipeline DAG - Simplified for Airflow

This DAG orchestrates the complete end-to-end pipeline:
1. Download earnings reports for Dow 30 companies
2. Parse PDFs with Docling
3. Upload to Google Cloud Storage
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# DAG Configuration
DEFAULT_ARGS = {
    'owner': 'lantern-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'complete_earnings_pipeline',
    default_args=DEFAULT_ARGS,
    description='Complete earnings pipeline: Download -> Parse -> Upload',
    schedule_interval='0 0 * * 1',
    catchup=False,
    max_active_runs=1,
    tags=['earnings', 'dow30', 'docling', 'gcs'],
)

# Task 1: Download earnings reports with Selenium (8 WORKERS - ALL 30 DOW COMPANIES!)
download_reports = BashOperator(
    task_id='download_earnings_reports',
    bash_command='''
    set -e
    export CHROME_BIN=/usr/bin/chromium
    export CHROMEDRIVER_PATH=/usr/bin/chromedriver
    cd /opt/airflow/dags/lantern-dow30 && \
    python3 main.py --mode multi --workers 8 && \
    echo "✅ Download complete - ALL 30 DOW companies with 8 workers!" && \
    ls -la dow30_pipeline_reports_2025/
    ''',
    dag=dag,
)

# Task 2: Parse PDFs with Docling
parse_with_docling = BashOperator(
    task_id='parse_pdfs_with_docling',
    bash_command='''
    cd /opt/airflow/dags && \
    python3 docling_parser.py --input-dir /opt/airflow/dags/lantern-dow30/dow30_pipeline_reports_2025 --output-dir /opt/airflow/parsed_local
    ''',
    dag=dag,
)

# Task 3: Upload Raw PDFs to GCS
upload_raw = BashOperator(
    task_id='upload_raw_pdfs',
    bash_command='''
    cd /opt/airflow/dags/lantern-dow30 && \
    python3 gcs_upload_helper.py --type raw --input-dir /opt/airflow/dags/lantern-dow30/dow30_pipeline_reports_2025 --bucket damgteam5
    ''',
    dag=dag,
)

# Task 4: Upload Parsed
upload_parsed = BashOperator(
    task_id='upload_parsed_content',
    bash_command='''
    cd /opt/airflow/dags/lantern-dow30 && \
    python3 gcs_upload_helper.py --type parsed --input-dir /opt/airflow/parsed_local --bucket damgteam5
    ''',
    dag=dag,
)

# Task dependencies
download_reports >> parse_with_docling >> [upload_raw, upload_parsed]
