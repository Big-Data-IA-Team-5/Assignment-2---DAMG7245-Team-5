#!/usr/bin/env python3
"""
AIRFLOW DAG - DOW 30 Earnings Pipeline Orchestration
Orchestrates: main.py → Docling → Google Cloud Storage

This DAG orchestrates the EXACT same workflow as running main.py manually:
1. Download Reports: Uses MultithreadedDow30Pipeline from main.py
2. Parse with Docling: Uses DoclingPDFParser 
3. Upload to GCS: Uploads to Google Cloud Storage bucket

Configuration via Airflow Variables (set in UI):
- GCS_BUCKET: damgteam5
- PROJECT_ID: damg-big-data
- RAW_PDF_DIR: /opt/airflow/data/dow30_pipeline_reports_2025
- DOCLING_OUTPUT_DIR: /opt/airflow/data/parsed_local
- MAX_WORKERS: 5
- ENABLED_TICKERS: (optional, comma-separated tickers or empty for all)
"""

from datetime import datetime, timedelta
from pathlib import Path
import json
import logging
import sys

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

# Setup paths
DAGS_DIR = Path(__file__).parent
sys.path.insert(0, str(DAGS_DIR / "lantern-dow30"))
sys.path.insert(0, str(DAGS_DIR))

logger = logging.getLogger(__name__)

# Get configuration from Airflow Variables
GCS_BUCKET = Variable.get("GCS_BUCKET", default_var="damgteam5")
PROJECT_ID = Variable.get("PROJECT_ID", default_var="damg-big-data")
RAW_PDF_DIR = Variable.get("RAW_PDF_DIR", default_var="/opt/airflow/data/dow30_pipeline_reports_2025")
DOCLING_OUTPUT_DIR = Variable.get("DOCLING_OUTPUT_DIR", default_var="/opt/airflow/data/parsed_local")
MAX_WORKERS = int(Variable.get("MAX_WORKERS", default_var="5"))
ENABLED_TICKERS = Variable.get("ENABLED_TICKERS", default_var="")

# DAG configuration
default_args = {
    'owner': 'lantern-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'simple_earnings_pipeline',
    default_args=default_args,
    description='Orchestrate: main.py → Docling → GCS',
    schedule_interval='@weekly',
    catchup=False,
    tags=['earnings', 'dow30', 'docling', 'gcs'],
)

# ============================================================================
# TASK 1: DOWNLOAD REPORTS (using main.py MultithreadedDow30Pipeline)
# ============================================================================

@task(task_id='download_reports', dag=dag)
def download_reports():
    """Download earnings reports using MultithreadedDow30Pipeline from main.py"""
    from main import MultithreadedDow30Pipeline
    
    logger.info("="*70)
    logger.info("TASK 1: DOWNLOADING REPORTS (main.py)")
    logger.info("="*70)
    logger.info(f"Output directory: {RAW_PDF_DIR}")
    logger.info(f"Max workers: {MAX_WORKERS}")
    
    # Parse enabled tickers
    target_companies = None
    if ENABLED_TICKERS:
        target_companies = [t.strip() for t in ENABLED_TICKERS.split(',')]
        logger.info(f"Target companies: {target_companies}")
    else:
        logger.info("Target: All DOW 30 companies")
    
    # Create output directory
    Path(RAW_PDF_DIR).mkdir(parents=True, exist_ok=True)
    
    # Run the pipeline (same as main.py)
    pipeline = MultithreadedDow30Pipeline(
        output_dir=RAW_PDF_DIR,
        max_workers=MAX_WORKERS
    )
    
    results = pipeline.run_multithreaded_pipeline(target_companies=target_companies)
    
    summary = {
        'total_companies': len(results),
        'successful_downloads': sum(1 for r in results if r.get('pdfs_downloaded', 0) > 0),
        'total_pdfs': sum(r.get('pdfs_downloaded', 0) for r in results),
        'output_dir': RAW_PDF_DIR
    }
    
    logger.info(f"Download complete: {summary}")
    return summary

# ============================================================================
# TASK 2: PARSE WITH DOCLING
# ============================================================================

@task(task_id='parse_with_docling', dag=dag)
def parse_with_docling(download_summary: dict):
    """Parse PDFs using DoclingPDFParser"""
    from docling_parser import DoclingPDFParser
    
    logger.info("="*70)
    logger.info("TASK 2: PARSING WITH DOCLING")
    logger.info("="*70)
    logger.info(f"Input directory: {RAW_PDF_DIR}")
    logger.info(f"Output directory: {DOCLING_OUTPUT_DIR}")
    
    # Create output directory
    Path(DOCLING_OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    
    # Find all PDFs
    pdf_files = list(Path(RAW_PDF_DIR).rglob("*.pdf"))
    logger.info(f"Found {len(pdf_files)} PDF files to parse")
    
    results = []
    parser = DoclingPDFParser()
    
    for pdf_path in pdf_files:
        # Extract ticker and period from filename (e.g., AAPL_2024-Q4_10-Q.pdf)
        parts = pdf_path.stem.split('_')
        ticker = parts[0] if len(parts) > 0 else 'UNKNOWN'
        period = parts[1] if len(parts) > 1 else 'UNKNOWN'
        
        output_dir = Path(DOCLING_OUTPUT_DIR) / ticker / period
        output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Parsing {ticker} {period}: {pdf_path.name}")
        
        try:
            result = parser.process_pdf_with_docling({
                'filepath': str(pdf_path),
                'ticker': ticker,
                'period': period,
                'output_dir': str(output_dir)
            })
            results.append(result)
        except Exception as e:
            logger.error(f"Failed to parse {pdf_path.name}: {e}")
            results.append({'success': False, 'error': str(e)})
    
    summary = {
        'total_parsed': len(results),
        'successful': sum(1 for r in results if r.get('success')),
        'failed': sum(1 for r in results if not r.get('success')),
        'output_dir': DOCLING_OUTPUT_DIR
    }
    
    logger.info(f"Docling parsing complete: {summary}")
    return summary

# ============================================================================
# TASK 3: UPLOAD TO GOOGLE CLOUD STORAGE
# ============================================================================

@task(task_id='upload_to_gcs', dag=dag)
def upload_to_gcs(parse_summary: dict):
    """Upload parsed outputs to Google Cloud Storage"""
    from google.cloud import storage
    
    logger.info("="*70)
    logger.info("TASK 3: UPLOADING TO GOOGLE CLOUD STORAGE")
    logger.info("="*70)
    logger.info(f"GCS Bucket: gs://{GCS_BUCKET}")
    logger.info(f"Project ID: {PROJECT_ID}")
    
    # Initialize GCS client
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(GCS_BUCKET)
    
    upload_count = 0
    parsed_path = Path(DOCLING_OUTPUT_DIR)
    
    # Upload all parsed files
    for ticker_dir in parsed_path.iterdir():
        if not ticker_dir.is_dir():
            continue
        
        ticker = ticker_dir.name
        
        for period_dir in ticker_dir.iterdir():
            if not period_dir.is_dir():
                continue
            
            period = period_dir.name
            
            # Upload all files in the period directory
            for file_path in period_dir.rglob('*'):
                if file_path.is_file():
                    # Create GCS path
                    relative_path = file_path.relative_to(parsed_path)
                    blob_path = f"lantern/parsed/{relative_path}"
                    
                    # Upload
                    blob = bucket.blob(blob_path)
                    blob.upload_from_filename(str(file_path))
                    upload_count += 1
                    logger.info(f"Uploaded: {blob_path}")
    
    # Also upload raw PDFs
    raw_path = Path(RAW_PDF_DIR)
    for pdf_file in raw_path.rglob('*.pdf'):
        # Extract ticker from filename
        ticker = pdf_file.stem.split('_')[0] if '_' in pdf_file.stem else 'UNKNOWN'
        blob_path = f"lantern/raw/{ticker}/{pdf_file.name}"
        
        blob = bucket.blob(blob_path)
        blob.upload_from_filename(str(pdf_file))
        upload_count += 1
        logger.info(f"Uploaded raw PDF: {blob_path}")
    
    summary = {
        'total_uploaded': upload_count,
        'gcs_bucket': f"gs://{GCS_BUCKET}/lantern/",
        'timestamp': datetime.now().isoformat()
    }
    
    logger.info(f"GCS upload complete: {summary}")
    return summary

# ============================================================================
# TASK 4: GENERATE PIPELINE SUMMARY
# ============================================================================

@task(task_id='generate_summary', dag=dag)
def generate_summary(download_summary: dict, parse_summary: dict, upload_summary: dict):
    """Generate final pipeline execution summary"""
    
    final_summary = {
        'pipeline': 'simple_earnings_pipeline',
        'execution_date': datetime.now().isoformat(),
        'workflow': 'main.py → Docling → Google Cloud Storage',
        'stages': {
            '1_download': {
                'total_companies': download_summary.get('total_companies', 0),
                'successful_downloads': download_summary.get('successful_downloads', 0),
                'total_pdfs': download_summary.get('total_pdfs', 0),
                'output_dir': download_summary.get('output_dir', '')
            },
            '2_parsing': {
                'total_parsed': parse_summary.get('total_parsed', 0),
                'successful': parse_summary.get('successful', 0),
                'failed': parse_summary.get('failed', 0),
                'output_dir': parse_summary.get('output_dir', '')
            },
            '3_upload': {
                'total_uploaded': upload_summary.get('total_uploaded', 0),
                'gcs_bucket': upload_summary.get('gcs_bucket', ''),
                'timestamp': upload_summary.get('timestamp', '')
            }
        },
        'status': 'SUCCESS'
    }
    
    logger.info("="*70)
    logger.info("PIPELINE EXECUTION COMPLETE")
    logger.info("="*70)
    logger.info(f"✅ Downloaded: {download_summary.get('total_pdfs', 0)} PDFs")
    logger.info(f"✅ Parsed: {parse_summary.get('successful', 0)} reports")
    logger.info(f"✅ Uploaded: {upload_summary.get('total_uploaded', 0)} files")
    logger.info(f"✅ GCS Location: {upload_summary.get('gcs_bucket', '')}")
    logger.info("="*70)
    
    return final_summary

# ============================================================================
# TASK FLOW: main.py → Docling → GCS → Summary
# ============================================================================

download_summary = download_reports()
parse_summary = parse_with_docling(download_summary)
upload_summary = upload_to_gcs(parse_summary)
final_summary = generate_summary(download_summary, parse_summary, upload_summary)

# Set task dependencies
download_summary >> parse_summary >> upload_summary >> final_summary
