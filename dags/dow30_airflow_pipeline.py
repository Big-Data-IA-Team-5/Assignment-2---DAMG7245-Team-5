"""
DOW 30 Complete Pipeline - Airflow DAG
Download → Parse with Docling → Upload to GCS

Orchestrates the complete earnings pipeline for all 30 DOW companies.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dow30_complete_pipeline',
    default_args=default_args,
    description='Complete DOW 30 Pipeline: Download → Parse → Upload',
    schedule_interval=None,
    catchup=False,
    tags=['dow30', 'earnings', 'complete'],
)

# Task 1: Download reports
download_task = BashOperator(
    task_id='download_reports',
    bash_command='''
    cd /opt/airflow/dags/lantern-dow30 && \
    python3 main.py --workers 8
    ''',
    dag=dag,
)

# Task 2: Parse with Docling
parse_task = BashOperator(
    task_id='parse_with_docling',
    bash_command='''
    python3 /opt/airflow/dags/docling_parser_wrapper.py
    ''',
    dag=dag,
)

# Task 3: Upload raw PDFs
upload_raw_task = BashOperator(
    task_id='upload_raw_pdfs',
    bash_command='''
    cd /opt/airflow/dags/lantern-dow30 && \
    python3 -c "
from gcs_utils import upload_directory_to_gcs
import os
bucket = os.environ.get('GCS_BUCKET', 'damgteam5')
upload_directory_to_gcs(
    bucket_name=bucket,
    source_directory='/opt/airflow/data/dow30_pipeline_reports_2025',
    destination_blob_prefix='raw_pdfs/'
)
print('✅ Raw PDFs uploaded to gs://' + bucket + '/raw_pdfs/')
"
    ''',
    dag=dag,
)

# Task 4: Upload parsed data
upload_parsed_task = BashOperator(
    task_id='upload_parsed_data',
    bash_command='''
    cd /opt/airflow/dags/lantern-dow30 && \
    python3 -c "
from gcs_utils import upload_directory_to_gcs
import os
bucket = os.environ.get('GCS_BUCKET', 'damgteam5')
upload_directory_to_gcs(
    bucket_name=bucket,
    source_directory='/opt/airflow/data/parsed_local',
    destination_blob_prefix='parsed_earnings/'
)
print('✅ Parsed data uploaded to gs://' + bucket + '/parsed_earnings/')
"
    ''',
    dag=dag,
)

# Define task flow
download_task >> parse_task >> [upload_raw_task, upload_parsed_task]
