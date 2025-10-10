#!/usr/bin/env python3
"""
Airflow DAG: Lantern Docling to Cloud
Post-Docling earnings pipeline on GCS with Airflow and Guidance+Vertex

DAG ID: lantern_docling_to_cloud
Schedule: Daily (can be configured via Airflow Variables)
"""

from datetime import datetime, timedelta
from typing import Dict, Any, List
import logging
import json

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.utils.task_group import TaskGroup

# Import our custom modules
import sys
from pathlib import Path
sys.path.append('/opt/airflow/dags/lantern-dow30')

from gcs_utils import GCSManager, build_gcs_paths
from guidance_extractor import create_extractor
from manifest import create_manifest_manager
from state import create_state_manager
from qc import create_quality_checker
from periods import infer_period

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
    'retry_exponential_backoff': True,
}

# Get configuration from Airflow Variables
PROJECT_ID = Variable.get("PROJECT_ID", default_var="your-project-id")
REGION = Variable.get("REGION", default_var="us-central1")
GCS_BUCKET = Variable.get("GCS_BUCKET", default_var="lantern-earnings-pipeline")
PARALLEL_TICKERS = int(Variable.get("PARALLEL_TICKERS", default_var="5"))
VERTEX_LOCATION = Variable.get("VERTEX_LOCATION", default_var="us-central1")
VERTEX_MODEL = Variable.get("VERTEX_MODEL", default_var="gemini-1.5-pro")
GUIDANCE_TEMPLATE_VERSION = Variable.get("GUIDANCE_TEMPLATE_VERSION", default_var="1.0")
BQ_DATASET = Variable.get("BQ_DATASET", default_var="")
BQ_TABLE = Variable.get("BQ_TABLE", default_var="")

# Docling outputs base path (should be mounted or accessible)
DOCLING_BASE_PATH = Variable.get("DOCLING_BASE_PATH", default_var="/opt/airflow/data/docling_outputs")

dag = DAG(
    'lantern_docling_to_cloud',
    default_args=DEFAULT_ARGS,
    description='Post-Docling earnings pipeline with GCS, Guidance+Vertex, and manifest generation',
    schedule_interval='@daily',  # Can be changed to '@weekly' or cron expression
    catchup=False,
    max_active_runs=1,
    tags=['lantern', 'earnings', 'docling', 'gcs', 'guidance', 'vertex-ai'],
)

# Task: Load Universe (Dow 30 tickers)
@task(dag=dag, task_id="load_universe")
def load_universe(**context) -> List[str]:
    """Load the universe of tickers to process (Dow 30)."""
    dow_30_tickers = [
        'AAPL', 'MSFT', 'UNH', 'JNJ', 'WMT', 'V', 'PG', 'JPM', 'HD', 'CVX',
        'MRK', 'PFE', 'KO', 'ABBV', 'BAC', 'PEP', 'COST', 'TMO', 'AVGO', 'XOM',
        'ABT', 'LLY', 'ACN', 'DHR', 'VZ', 'ADBE', 'NFLX', 'NKE', 'CRM', 'MCD'
    ]
    
    # Can be filtered or modified based on Variable configuration
    enabled_tickers = Variable.get("ENABLED_TICKERS", default_var="").split(',')
    if enabled_tickers and enabled_tickers[0]:
        tickers = [t.strip().upper() for t in enabled_tickers if t.strip()]
        logger.info(f"Using configured tickers: {tickers}")
    else:
        tickers = dow_30_tickers
        logger.info(f"Using default Dow 30 tickers: {len(tickers)} tickers")
    
    return tickers

# Task: Collect Docling Outputs
@task(dag=dag, task_id="collect_docling_outputs")
def collect_docling_outputs(tickers: List[str], **context) -> List[Dict[str, Any]]:
    """Collect and validate Docling outputs for processing."""
    import sys
    from pathlib import Path
    
    # Add scripts to path
    scripts_path = Path('/opt/airflow/dags/scripts')
    sys.path.append(str(scripts_path))
    
    try:
        from collect_docling_outputs import DoclingOutputCollector
        
        collector = DoclingOutputCollector(DOCLING_BASE_PATH)
        
        # Collect outputs for specified tickers
        all_outputs = []
        for output_set in collector.find_docling_outputs():
            if output_set['ticker'] in tickers:
                all_outputs.append(output_set)
        
        logger.info(f"Found {len(all_outputs)} Docling output sets for {len(tickers)} tickers")
        
        # Store summary stats
        stats = collector.get_summary_stats()
        context['task_instance'].xcom_push(key='collection_stats', value=stats)
        
        return all_outputs
        
    except Exception as e:
        logger.error(f"Failed to collect Docling outputs: {e}")
        raise

# Task: Period Normalize  
@task(dag=dag, task_id="period_normalize")
def period_normalize(docling_outputs: List[Dict[str, Any]], **context) -> List[Dict[str, Any]]:
    """Normalize periods to YYYY-Q# format and validate."""
    normalized_outputs = []
    
    for output_set in docling_outputs:
        try:
            # Try to normalize the period
            if output_set.get('period'):
                from periods import normalize_period, is_valid_period
                
                period = output_set['period']
                if not is_valid_period(period):
                    # Try to infer from title and date
                    period = infer_period(
                        title=output_set.get('title', ''),
                        published_iso=output_set.get('published_iso', ''),
                        report_type=output_set.get('report_type', 'press')
                    )
                
                output_set['period'] = normalize_period(period)
                normalized_outputs.append(output_set)
                
        except Exception as e:
            logger.warning(f"Failed to normalize period for {output_set.get('ticker')}: {e}")
            # Skip this output set
            continue
    
    logger.info(f"Successfully normalized {len(normalized_outputs)} output sets")
    return normalized_outputs

# Task Group: Process Individual Reports
@task_group(group_id="process_reports")
def process_reports_group(normalized_outputs: List[Dict[str, Any]]):
    """Process each report with parallel execution."""
    
    @task(dag=dag, task_id="guidance_extract_structured", pool='guidance_pool')
    def guidance_extract_structured(output_set: Dict[str, Any], **context) -> Dict[str, Any]:
        """Extract structured JSON using Guidance + Google AI."""
        try:
            # Initialize extractor
            extractor = create_extractor(
                api_key=Variable.get("GOOGLE_AI_API_KEY", default_var=None),
                model_name=VERTEX_MODEL
            )
            
            # Read text and table files
            text_file = Path(output_set['docling_text_path'])
            table_files = [Path(f) for f in output_set.get('table_files', [])]
            
            # Extract structured data
            structured_data = extractor.extract_from_files(
                text_file=text_file,
                ticker=output_set['ticker'],
                period=output_set['period'],
                published=output_set['published_iso'],
                report_type=output_set['report_type'],
                table_files=table_files
            )
            
            # Add extraction metadata
            output_set['structured_data'] = structured_data
            output_set['extraction_completed'] = True
            
            logger.info(f"Extracted structured data for {output_set['ticker']} {output_set['period']} "
                       f"(confidence: {structured_data.get('confidence', 0)})")
            
            return output_set
            
        except Exception as e:
            logger.error(f"Failed to extract structured data for {output_set.get('ticker')}: {e}")
            # Add error info but don't fail the task
            output_set['structured_data'] = {'error': str(e), 'confidence': 0.0}
            output_set['extraction_completed'] = False
            return output_set
    
    @task(dag=dag, task_id="upload_to_gcs", pool='gcs_upload_pool')
    def upload_to_gcs(output_set: Dict[str, Any], **context) -> Dict[str, Any]:
        """Upload raw PDFs and parsed assets to GCS."""
        try:
            # Initialize GCS manager
            gcs_manager = GCSManager(GCS_BUCKET, PROJECT_ID)
            
            ticker = output_set['ticker']
            period = output_set['period']
            filename = Path(output_set['filename']).stem
            
            # Build GCS paths
            paths = build_gcs_paths(ticker, period, filename)
            
            uploaded_files = []
            
            # Upload raw PDF
            pdf_path = output_set['raw_pdf_path']
            if Path(pdf_path).exists():
                gcs_uri = gcs_manager.upload_file(
                    local_path=pdf_path,
                    gcs_path=paths['raw_pdf'],
                    content_type='application/pdf',
                    metadata={
                        'ticker': ticker,
                        'period': period,
                        'sha256': output_set.get('sha256', ''),
                        'report_type': output_set.get('report_type', 'press')
                    }
                )
                uploaded_files.append(gcs_uri)
            
            # Upload text file
            text_path = output_set['docling_text_path']
            if Path(text_path).exists():
                gcs_uri = gcs_manager.upload_file(
                    local_path=text_path,
                    gcs_path=f"{paths['text_dir']}/{filename}.txt",
                    content_type='text/plain'
                )
                uploaded_files.append(gcs_uri)
            
            # Upload table files
            for table_file in output_set.get('table_files', []):
                if Path(table_file).exists():
                    table_name = Path(table_file).name
                    gcs_uri = gcs_manager.upload_file(
                        local_path=table_file,
                        gcs_path=f"{paths['tables_dir']}/{table_name}",
                        content_type='text/csv'
                    )
                    uploaded_files.append(gcs_uri)
            
            # Upload image files
            for image_file in output_set.get('image_files', []):
                if Path(image_file).exists():
                    image_name = Path(image_file).name
                    gcs_uri = gcs_manager.upload_file(
                        local_path=image_file,
                        gcs_path=f"{paths['images_dir']}/{image_name}",
                        content_type='image/png'
                    )
                    uploaded_files.append(gcs_uri)
            
            # Upload structured JSON if extraction succeeded
            if output_set.get('extraction_completed') and output_set.get('structured_data'):
                gcs_uri = gcs_manager.upload_json(
                    data=output_set['structured_data'],
                    gcs_path=paths['structured_json'],
                    metadata={'ticker': ticker, 'period': period}
                )
                uploaded_files.append(gcs_uri)
            
            output_set['uploaded_files'] = uploaded_files
            output_set['gcs_paths'] = paths
            output_set['upload_completed'] = True
            
            logger.info(f"Uploaded {len(uploaded_files)} files to GCS for {ticker} {period}")
            return output_set
            
        except Exception as e:
            logger.error(f"Failed to upload to GCS for {output_set.get('ticker')}: {e}")
            output_set['upload_completed'] = False
            output_set['upload_error'] = str(e)
            return output_set
    
    @task(dag=dag, task_id="write_manifest")
    def write_manifest(output_set: Dict[str, Any], **context) -> Dict[str, Any]:
        """Write manifest entry for the processed report."""
        try:
            # Initialize managers
            gcs_manager = GCSManager(GCS_BUCKET, PROJECT_ID)
            manifest_manager = create_manifest_manager(gcs_manager, GCS_BUCKET)
            
            ticker = output_set['ticker']
            period = output_set['period']
            
            # Create manifest entry
            entry = manifest_manager.create_entry(
                ticker=ticker,
                period=period,
                published=output_set.get('published_iso', ''),
                report_type=output_set.get('report_type', 'press'),
                source_url=output_set.get('source_url', ''),
                sha256=output_set.get('sha256', ''),
                filename=output_set.get('filename', '')
            )
            
            # Set Docling info
            entry.set_docling_info(
                pages=output_set.get('pages', 0),
                tables=output_set.get('tables', 0),
                version="1.0.0"
            )
            
            # Set structured extraction info
            structured_data = output_set.get('structured_data', {})
            confidence = structured_data.get('confidence', 0.0)
            gcs_structured_path = output_set.get('gcs_paths', {}).get('structured_json', '')
            
            entry.set_structured_info(
                model=VERTEX_MODEL,
                confidence=confidence,
                gcs_path=gcs_structured_path
            )
            
            # Add notes for any issues
            if not output_set.get('extraction_completed', False):
                entry.add_note("Structured extraction failed")
            if not output_set.get('upload_completed', False):
                entry.add_note("GCS upload failed")
            
            # Write to manifest
            manifest_path = f"lantern/parsed/{ticker}/{period}/manifest.jsonl"
            manifest_uri = manifest_manager.write_manifest_entry(entry, manifest_path)
            
            output_set['manifest_written'] = True
            output_set['manifest_uri'] = manifest_uri
            
            logger.info(f"Wrote manifest entry for {ticker} {period}")
            return output_set
            
        except Exception as e:
            logger.error(f"Failed to write manifest for {output_set.get('ticker')}: {e}")
            output_set['manifest_written'] = False
            output_set['manifest_error'] = str(e)
            return output_set
    
    @task(dag=dag, task_id="quality_checks")
    def quality_checks(output_set: Dict[str, Any], **context) -> Dict[str, Any]:
        """Run quality checks on processed report."""
        try:
            # Initialize quality checker
            gcs_manager = GCSManager(GCS_BUCKET, PROJECT_ID)
            qc = create_quality_checker(gcs_manager)
            
            # Prepare inputs for quality check
            docling_outputs = {
                'text_file': output_set.get('docling_text_path', ''),
                'table_files': output_set.get('table_files', []),
                'image_files': output_set.get('image_files', [])
            }
            
            structured_data = output_set.get('structured_data', {})
            
            # Create a mock manifest entry for validation
            manifest_entry = {
                'ticker': output_set['ticker'],
                'period': output_set['period'],
                'published': output_set.get('published_iso', ''),
                'report_type': output_set.get('report_type', 'press'),
                'source_url': output_set.get('source_url', ''),
                'sha256': output_set.get('sha256', ''),
                'filename': output_set.get('filename', ''),
                'gcs_pdf': output_set.get('gcs_paths', {}).get('raw_pdf', ''),
                'structured': {
                    'confidence': structured_data.get('confidence', 0)
                }
            }
            
            # Run full quality check
            qc_results = qc.run_full_quality_check(
                ticker=output_set['ticker'],
                period=output_set['period'],
                filename=output_set.get('filename', ''),
                docling_outputs=docling_outputs,
                structured_data=structured_data,
                manifest_entry=manifest_entry
            )
            
            output_set['qc_results'] = qc_results
            output_set['qc_passed'] = qc_results['overall_passed']
            
            if qc_results['overall_passed']:
                logger.info(f"Quality checks PASSED for {output_set['ticker']} {output_set['period']}")
            else:
                logger.warning(f"Quality checks FAILED for {output_set['ticker']} {output_set['period']} "
                              f"({qc_results['error_count']} errors)")
            
            return output_set
            
        except Exception as e:
            logger.error(f"Quality checks failed for {output_set.get('ticker')}: {e}")
            output_set['qc_passed'] = False
            output_set['qc_error'] = str(e)
            return output_set
    
    @task(dag=dag, task_id="update_state")
    def update_state(output_set: Dict[str, Any], **context) -> Dict[str, Any]:
        """Update processing state for idempotency."""
        try:
            # Initialize state manager
            gcs_manager = GCSManager(GCS_BUCKET, PROJECT_ID)
            state_manager = create_state_manager(gcs_manager)
            
            ticker = output_set['ticker']
            period = output_set['period']
            sha256 = output_set.get('sha256', '')
            published = output_set.get('published_iso', '')
            
            # Build state path
            state_path = f"lantern/state/{ticker}.json"
            
            # Additional state data
            additional_data = {
                'period': period,
                'extraction_completed': output_set.get('extraction_completed', False),
                'upload_completed': output_set.get('upload_completed', False),
                'manifest_written': output_set.get('manifest_written', False),
                'qc_passed': output_set.get('qc_passed', False),
                'confidence': output_set.get('structured_data', {}).get('confidence', 0.0)
            }
            
            # Update state
            state_uri = state_manager.update_ticker_state(
                ticker=ticker,
                sha256=sha256,
                published=published,
                period=period,
                state_path=state_path,
                additional_data=additional_data
            )
            
            output_set['state_updated'] = True
            output_set['state_uri'] = state_uri
            
            logger.info(f"Updated state for {ticker}: {sha256[:8]}...")
            return output_set
            
        except Exception as e:
            logger.error(f"Failed to update state for {output_set.get('ticker')}: {e}")
            output_set['state_updated'] = False
            output_set['state_error'] = str(e)
            return output_set
    
    # Chain the tasks for each report
    processed_outputs = []
    for output_set in normalized_outputs[:PARALLEL_TICKERS]:  # Limit parallelism
        
        structured_output = guidance_extract_structured(output_set)
        uploaded_output = upload_to_gcs(structured_output)  
        manifest_output = write_manifest(uploaded_output)
        qc_output = quality_checks(manifest_output)
        final_output = update_state(qc_output)
        
        processed_outputs.append(final_output)
    
    return processed_outputs

# Task: Final Summary
@task(dag=dag, task_id="generate_summary")
def generate_summary(processed_outputs: List[Dict[str, Any]], **context) -> Dict[str, Any]:
    """Generate final processing summary."""
    
    summary = {
        'total_reports': len(processed_outputs),
        'successful_extractions': 0,
        'successful_uploads': 0, 
        'successful_manifests': 0,
        'passed_qc': 0,
        'updated_states': 0,
        'avg_confidence': 0.0,
        'errors': [],
        'processing_time': context['task_instance'].duration
    }
    
    confidences = []
    
    for output in processed_outputs:
        if output.get('extraction_completed'):
            summary['successful_extractions'] += 1
        if output.get('upload_completed'):
            summary['successful_uploads'] += 1
        if output.get('manifest_written'):
            summary['successful_manifests'] += 1
        if output.get('qc_passed'):
            summary['passed_qc'] += 1
        if output.get('state_updated'):
            summary['updated_states'] += 1
            
        # Collect confidence scores
        confidence = output.get('structured_data', {}).get('confidence', 0.0)
        if confidence > 0:
            confidences.append(confidence)
        
        # Collect errors
        for error_key in ['extraction_error', 'upload_error', 'manifest_error', 'qc_error', 'state_error']:
            if error_key in output:
                summary['errors'].append({
                    'ticker': output.get('ticker'),
                    'period': output.get('period'),
                    'error_type': error_key,
                    'error': output[error_key]
                })
    
    if confidences:
        summary['avg_confidence'] = round(sum(confidences) / len(confidences), 3)
    
    logger.info(f"Processing summary: {summary['successful_extractions']}/{summary['total_reports']} "
               f"successful extractions, avg confidence: {summary['avg_confidence']}")
    
    return summary

# Build the DAG
tickers = load_universe()
docling_outputs = collect_docling_outputs(tickers)
normalized_outputs = period_normalize(docling_outputs)
processed_outputs = process_reports_group(normalized_outputs)
final_summary = generate_summary(processed_outputs)

# Set dependencies
tickers >> docling_outputs >> normalized_outputs >> processed_outputs >> final_summary
