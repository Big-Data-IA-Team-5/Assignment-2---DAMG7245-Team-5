#!/usr/bin/env python3
"""
Complete Earnings Pipeline DAG - Airflow Orchestration
Download → Docling Parse → Cloud Upload

This DAG orchestrates the complete end-to-end pipeline:
1. Download earnings reports for Dow 30 companies
2. Parse PDFs with Docling to extract text, tables, images
3. Upload everything to Google Cloud Storage

DAG ID: complete_earnings_pipeline
Schedule: Weekly on Mondays
"""

from datetime import datetime, timedelta
from typing import Dict, Any, List
import logging
import json
import os
import sys
from pathlib import Path

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator

logger = logging.getLogger(__name__)

# DAG Configuration
DEFAULT_ARGS = {
    'owner': 'lantern-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Get configuration from Airflow Variables (with defaults)
PROJECT_ID = Variable.get("PROJECT_ID", default_var="your-project-id")
GCS_BUCKET = Variable.get("GCS_BUCKET", default_var="lantern-earnings-pipeline")
GOOGLE_AI_API_KEY = Variable.get("GOOGLE_AI_API_KEY", default_var="")
BASE_DIR = Variable.get("BASE_DIR", default_var="/opt/airflow/data")

# Directory paths
DOWNLOAD_DIR = f"{BASE_DIR}/dow30_pipeline_reports_2025"
PARSED_DIR = f"{BASE_DIR}/parsed_local"
PROCESSED_DIR = f"{BASE_DIR}/processed"

# Pipeline modules path
PIPELINE_DIR = "/opt/airflow/dags/lantern-dow30"
sys.path.insert(0, PIPELINE_DIR)

# Create DAG
dag = DAG(
    'complete_earnings_pipeline',
    default_args=DEFAULT_ARGS,
    description='Complete earnings pipeline: Download → Parse → Upload to GCS',
    schedule_interval='0 0 * * 1',  # Weekly on Mondays at midnight
    catchup=False,
    max_active_runs=1,
    tags=['earnings', 'dow30', 'docling', 'gcs', 'complete-pipeline'],
)


# ============================================================================
# TASK GROUP 1: DOWNLOAD EARNINGS REPORTS
# ============================================================================

@task_group(group_id='download_reports', dag=dag)
def download_reports_group():
    """Download earnings reports for Dow 30 companies"""
    
    @task
    def get_dow30_tickers() -> List[str]:
        """Get list of Dow 30 tickers to process"""
        from tickers import get_companies
        
        companies = get_companies()
        tickers = [ticker for ticker, _, _ in companies]
        
        logger.info(f"Retrieved {len(tickers)} Dow 30 tickers")
        return tickers
    
    @task
    def download_company_reports(ticker: str) -> Dict[str, Any]:
        """Download earnings report for a single company"""
        from main import MultithreadedDow30Pipeline
        from tickers import get_companies
        
        # Get company info
        companies = get_companies()
        company_info = next((c for c in companies if c[0] == ticker), None)
        
        if not company_info:
            logger.error(f"Company {ticker} not found")
            return {'ticker': ticker, 'status': 'failed', 'error': 'Company not found'}
        
        ticker_code, name, website = company_info
        
        # Download using pipeline
        pipeline = MultithreadedDow30Pipeline(
            output_dir=DOWNLOAD_DIR,
            headless=True,
            max_workers=1
        )
        
        result = pipeline.process_company_threaded((ticker_code, name, website))
        
        logger.info(f"Download result for {ticker}: {result.get('status')}")
        return result
    
    @task
    def consolidate_download_results(results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Consolidate all download results"""
        successful = [r['ticker'] for r in results if r.get('status') == 'success']
        failed = [r['ticker'] for r in results if r.get('status') != 'success']
        
        summary = {
            'total': len(results),
            'successful': successful,
            'failed': failed,
            'success_count': len(successful),
            'fail_count': len(failed),
            'timestamp': datetime.now().isoformat()
        }
        
        # Save summary
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        summary_path = f"{DOWNLOAD_DIR}/download_summary.json"
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"Download complete: {len(successful)}/{len(results)} successful")
        return summary
    
    # Task flow
    tickers = get_dow30_tickers()
    download_results = download_company_reports.expand(ticker=tickers)
    summary = consolidate_download_results(download_results)
    
    return summary


# ============================================================================
# TASK GROUP 2: PARSE WITH DOCLING
# ============================================================================

@task_group(group_id='parse_with_docling', dag=dag)
def parse_with_docling_group():
    """Parse downloaded PDFs with Docling"""
    
    @task
    def find_downloaded_pdfs() -> List[Dict[str, str]]:
        """Find all downloaded PDFs to parse"""
        from pathlib import Path
        
        pdf_files = []
        download_path = Path(DOWNLOAD_DIR)
        
        if not download_path.exists():
            logger.warning(f"Download directory not found: {DOWNLOAD_DIR}")
            return []
        
        for pdf_file in download_path.glob("*.pdf"):
            # Extract ticker from filename (e.g., AAPL_2025_financial_report.pdf)
            ticker = pdf_file.stem.split('_')[0]
            
            pdf_files.append({
                'ticker': ticker,
                'pdf_path': str(pdf_file),
                'filename': pdf_file.name
            })
        
        logger.info(f"Found {len(pdf_files)} PDFs to parse")
        return pdf_files
    
    @task
    def parse_single_pdf(pdf_info: Dict[str, str]) -> Dict[str, Any]:
        """Parse a single PDF with Docling"""
        from docling_parser import DoclingPDFParser
        
        ticker = pdf_info['ticker']
        pdf_path = pdf_info['pdf_path']
        
        logger.info(f"Parsing {ticker}: {pdf_path}")
        
        try:
            parser = DoclingPDFParser(
                input_dir=DOWNLOAD_DIR,
                output_dir=PARSED_DIR
            )
            
            result = parser.parse_single_pdf(pdf_path)
            
            if result:
                logger.info(f"Successfully parsed {ticker}")
                return {
                    'ticker': ticker,
                    'status': 'success',
                    'output': result
                }
            else:
                logger.error(f"Failed to parse {ticker}")
                return {
                    'ticker': ticker,
                    'status': 'failed',
                    'error': 'Parse returned None'
                }
                
        except Exception as e:
            logger.error(f"Error parsing {ticker}: {e}")
            return {
                'ticker': ticker,
                'status': 'failed',
                'error': str(e)
            }
    
    @task
    def consolidate_parse_results(results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Consolidate parsing results"""
        successful = [r['ticker'] for r in results if r.get('status') == 'success']
        failed = [r['ticker'] for r in results if r.get('status') != 'success']
        
        summary = {
            'total': len(results),
            'successful': successful,
            'failed': failed,
            'success_count': len(successful),
            'fail_count': len(failed),
            'timestamp': datetime.now().isoformat()
        }
        
        # Save summary
        os.makedirs(PARSED_DIR, exist_ok=True)
        summary_path = f"{PARSED_DIR}/parsing_summary.json"
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"Parsing complete: {len(successful)}/{len(results)} successful")
        return summary
    
    # Task flow
    pdfs = find_downloaded_pdfs()
    parse_results = parse_single_pdf.expand(pdf_info=pdfs)
    summary = consolidate_parse_results(parse_results)
    
    return summary


# ============================================================================
# TASK GROUP 3: UPLOAD TO GOOGLE CLOUD STORAGE
# ============================================================================

@task_group(group_id='upload_to_gcs', dag=dag)
def upload_to_gcs_group():
    """Upload parsed outputs to Google Cloud Storage"""
    
    @task
    def prepare_gcs_bucket() -> str:
        """Ensure GCS bucket exists"""
        from google.cloud import storage
        
        try:
            client = storage.Client(project=PROJECT_ID)
            
            # Check if bucket exists
            bucket = client.lookup_bucket(GCS_BUCKET)
            
            if bucket is None:
                # Create bucket
                bucket = client.create_bucket(GCS_BUCKET, location="US")
                logger.info(f"Created GCS bucket: {GCS_BUCKET}")
            else:
                logger.info(f"GCS bucket exists: {GCS_BUCKET}")
            
            return GCS_BUCKET
            
        except Exception as e:
            logger.error(f"Error with GCS bucket: {e}")
            raise
    
    @task
    def find_parsed_outputs() -> List[Dict[str, Any]]:
        """Find all parsed outputs to upload"""
        from pathlib import Path
        
        uploads = []
        parsed_path = Path(PARSED_DIR)
        
        if not parsed_path.exists():
            logger.warning(f"Parsed directory not found: {PARSED_DIR}")
            return []
        
        # Iterate through ticker directories
        for ticker_dir in parsed_path.iterdir():
            if not ticker_dir.is_dir():
                continue
            
            ticker = ticker_dir.name
            
            # Iterate through period directories
            for period_dir in ticker_dir.iterdir():
                if not period_dir.is_dir():
                    continue
                
                period = period_dir.name
                
                # Collect all files to upload
                for file_type in ['text', 'tables', 'images']:
                    type_dir = period_dir / file_type
                    if type_dir.exists():
                        for file_path in type_dir.iterdir():
                            if file_path.is_file():
                                uploads.append({
                                    'ticker': ticker,
                                    'period': period,
                                    'file_type': file_type,
                                    'local_path': str(file_path),
                                    'gcs_path': f"lantern/parsed/{ticker}/{period}/{file_type}/{file_path.name}"
                                })
                
                # Upload metadata
                metadata_file = period_dir / 'metadata.json'
                if metadata_file.exists():
                    uploads.append({
                        'ticker': ticker,
                        'period': period,
                        'file_type': 'metadata',
                        'local_path': str(metadata_file),
                        'gcs_path': f"lantern/parsed/{ticker}/{period}/metadata.json"
                    })
        
        logger.info(f"Found {len(uploads)} files to upload")
        return uploads
    
    @task
    def upload_single_file(file_info: Dict[str, Any]) -> Dict[str, Any]:
        """Upload a single file to GCS"""
        from google.cloud import storage
        
        try:
            client = storage.Client(project=PROJECT_ID)
            bucket = client.bucket(GCS_BUCKET)
            blob = bucket.blob(file_info['gcs_path'])
            
            blob.upload_from_filename(file_info['local_path'])
            
            logger.info(f"Uploaded: {file_info['gcs_path']}")
            
            return {
                'ticker': file_info['ticker'],
                'status': 'success',
                'gcs_path': f"gs://{GCS_BUCKET}/{file_info['gcs_path']}"
            }
            
        except Exception as e:
            logger.error(f"Upload failed for {file_info['gcs_path']}: {e}")
            return {
                'ticker': file_info['ticker'],
                'status': 'failed',
                'error': str(e)
            }
    
    @task
    def upload_raw_pdfs() -> List[Dict[str, Any]]:
        """Upload original PDFs to GCS raw/ directory"""
        from pathlib import Path
        from google.cloud import storage
        
        results = []
        download_path = Path(DOWNLOAD_DIR)
        
        if not download_path.exists():
            return results
        
        client = storage.Client(project=PROJECT_ID)
        bucket = client.bucket(GCS_BUCKET)
        
        for pdf_file in download_path.glob("*.pdf"):
            ticker = pdf_file.stem.split('_')[0]
            # Extract period from filename if available
            parts = pdf_file.stem.split('_')
            period = parts[1] if len(parts) > 1 else "latest"
            
            gcs_path = f"lantern/raw/{ticker}/{period}/{pdf_file.name}"
            
            try:
                blob = bucket.blob(gcs_path)
                blob.upload_from_filename(str(pdf_file))
                
                results.append({
                    'ticker': ticker,
                    'status': 'success',
                    'gcs_path': f"gs://{GCS_BUCKET}/{gcs_path}"
                })
                logger.info(f"Uploaded raw PDF: {gcs_path}")
                
            except Exception as e:
                logger.error(f"Failed to upload {pdf_file.name}: {e}")
                results.append({
                    'ticker': ticker,
                    'status': 'failed',
                    'error': str(e)
                })
        
        return results
    
    @task
    def consolidate_upload_results(
        parsed_results: List[Dict[str, Any]], 
        raw_results: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Consolidate all upload results"""
        all_results = parsed_results + raw_results
        
        successful = [r for r in all_results if r.get('status') == 'success']
        failed = [r for r in all_results if r.get('status') != 'success']
        
        summary = {
            'total_files': len(all_results),
            'successful_uploads': len(successful),
            'failed_uploads': len(failed),
            'parsed_files': len(parsed_results),
            'raw_files': len(raw_results),
            'gcs_bucket': f"gs://{GCS_BUCKET}/lantern/",
            'timestamp': datetime.now().isoformat()
        }
        
        # Save summary
        os.makedirs(PROCESSED_DIR, exist_ok=True)
        summary_path = f"{PROCESSED_DIR}/upload_summary.json"
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"Upload complete: {len(successful)}/{len(all_results)} files uploaded")
        return summary
    
    # Task flow
    bucket = prepare_gcs_bucket()
    files_to_upload = find_parsed_outputs()
    parsed_upload_results = upload_single_file.expand(file_info=files_to_upload)
    raw_upload_results = upload_raw_pdfs()
    summary = consolidate_upload_results(parsed_upload_results, raw_upload_results)
    
    # Set dependencies
    bucket >> files_to_upload
    
    return summary


# ============================================================================
# FINAL TASK: PIPELINE SUMMARY
# ============================================================================

@task(dag=dag)
def generate_pipeline_summary(
    download_summary: Dict[str, Any],
    parse_summary: Dict[str, Any],
    upload_summary: Dict[str, Any]
) -> Dict[str, Any]:
    """Generate final pipeline execution summary"""
    
    final_summary = {
        'pipeline': 'complete_earnings_pipeline',
        'execution_time': datetime.now().isoformat(),
        'stages': {
            'download': {
                'total_companies': download_summary.get('total', 0),
                'successful': download_summary.get('success_count', 0),
                'failed': download_summary.get('fail_count', 0),
                'success_rate': f"{(download_summary.get('success_count', 0) / max(download_summary.get('total', 1), 1) * 100):.1f}%"
            },
            'parsing': {
                'total_pdfs': parse_summary.get('total', 0),
                'successful': parse_summary.get('success_count', 0),
                'failed': parse_summary.get('fail_count', 0),
                'success_rate': f"{(parse_summary.get('success_count', 0) / max(parse_summary.get('total', 1), 1) * 100):.1f}%"
            },
            'upload': {
                'total_files': upload_summary.get('total_files', 0),
                'successful': upload_summary.get('successful_uploads', 0),
                'failed': upload_summary.get('failed_uploads', 0),
                'gcs_bucket': upload_summary.get('gcs_bucket', '')
            }
        },
        'status': 'completed',
        'directories': {
            'downloads': DOWNLOAD_DIR,
            'parsed': PARSED_DIR,
            'processed': PROCESSED_DIR
        }
    }
    
    # Save final summary
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    summary_path = f"{PROCESSED_DIR}/pipeline_summary.json"
    with open(summary_path, 'w') as f:
        json.dump(final_summary, f, indent=2)
    
    logger.info("="*70)
    logger.info("COMPLETE EARNINGS PIPELINE - EXECUTION SUMMARY")
    logger.info("="*70)
    logger.info(f"Download: {final_summary['stages']['download']['successful']} companies")
    logger.info(f"Parsing: {final_summary['stages']['parsing']['successful']} PDFs")
    logger.info(f"Upload: {final_summary['stages']['upload']['successful']} files")
    logger.info(f"GCS Location: {final_summary['stages']['upload']['gcs_bucket']}")
    logger.info("="*70)
    
    return final_summary


# ============================================================================
# DAG TASK FLOW
# ============================================================================

with dag:
    # Stage 1: Download reports
    download_summary = download_reports_group()
    
    # Stage 2: Parse with Docling
    parse_summary = parse_with_docling_group()
    
    # Stage 3: Upload to GCS
    upload_summary = upload_to_gcs_group()
    
    # Final summary
    final_summary = generate_pipeline_summary(
        download_summary,
        parse_summary,
        upload_summary
    )
    
    # Set dependencies
    download_summary >> parse_summary >> upload_summary >> final_summary
