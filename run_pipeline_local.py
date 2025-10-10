#!/usr/bin/env python3
"""
Complete DOW 30 Pipeline - Local Execution
Download → Parse → Upload

Runs the complete pipeline locally without Airflow.
"""

import os
import sys
import time
import json
import logging
from datetime import datetime
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)

# Directories
BASE_DIR = Path(__file__).parent
LANTERN_DIR = BASE_DIR / 'lantern-dow30'
DOWNLOAD_DIR = BASE_DIR / 'dow30_pipeline_reports_2025'
PARSED_DIR = BASE_DIR / 'parsed_local'

# Add to path
sys.path.insert(0, str(LANTERN_DIR))

def step1_download_reports(workers=8):
    """Step 1: Download earnings reports"""
    log.info("="*70)
    log.info("STEP 1: DOWNLOADING EARNINGS REPORTS")
    log.info("="*70)
    
    try:
        os.chdir(LANTERN_DIR)
        
        # Import after changing directory
        from main import MultithreadedDow30Pipeline
        
        pipeline = MultithreadedDow30Pipeline(
            output_dir=str(DOWNLOAD_DIR),
            headless=True,
            max_workers=workers
        )
        
        log.info(f"Starting download with {workers} workers...")
        results = pipeline.run_multithreaded_pipeline()
        
        success = results.get('successful_companies', [])
        failed = results.get('failed_companies', [])
        
        log.info(f"✅ Downloaded: {len(success)} companies")
        if failed:
            log.warning(f"❌ Failed: {len(failed)} companies - {', '.join(failed)}")
        
        return results
        
    except Exception as e:
        log.error(f"❌ Download failed: {e}")
        import traceback
        traceback.print_exc()
        return {'successful_companies': [], 'failed_companies': []}
    finally:
        os.chdir(BASE_DIR)

def step2_parse_with_docling():
    """Step 2: Parse PDFs with Docling"""
    log.info("\n" + "="*70)
    log.info("STEP 2: PARSING PDFs WITH DOCLING")
    log.info("="*70)
    
    try:
        # Check if download directory has PDFs
        pdf_files = list(DOWNLOAD_DIR.glob('**/*.pdf'))
        if not pdf_files:
            log.warning("No PDFs found to parse")
            return {}
        
        log.info(f"Found {len(pdf_files)} PDFs to parse")
        
        # Import docling parser
        from docling_parser import DoclingPDFParser
        
        parser = DoclingPDFParser(
            input_dir=str(DOWNLOAD_DIR),
            output_dir=str(PARSED_DIR)
        )
        
        # Discover PDFs
        pdfs = parser.discover_pdfs()
        
        log.info(f"Processing {len(pdfs)} PDFs...")
        results = {}
        
        for pdf in pdfs:
            ticker = pdf['ticker']
            result = parser.process_pdf_with_docling(pdf)
            results[ticker] = result
        
        success_count = sum(1 for r in results.values() if r.get('success'))
        log.info(f"✅ Parsed: {success_count}/{len(results)} PDFs")
        
        return results
        
    except Exception as e:
        log.error(f"❌ Parsing failed: {e}")
        import traceback
        traceback.print_exc()
        return {}

def step3_upload_to_gcs():
    """Step 3: Upload to Google Cloud Storage"""
    log.info("\n" + "="*70)
    log.info("STEP 3: UPLOADING TO GCS")
    log.info("="*70)
    
    try:
        os.chdir(LANTERN_DIR)
        from gcs_utils import upload_directory_to_gcs
        
        bucket = os.environ.get('GCS_BUCKET', 'damgteam5')
        
        # Upload raw PDFs
        log.info(f"📤 Uploading raw PDFs to gs://{bucket}/raw_pdfs/...")
        upload_directory_to_gcs(
            bucket_name=bucket,
            source_directory=str(DOWNLOAD_DIR),
            destination_blob_prefix='raw_pdfs/'
        )
        
        # Upload parsed data
        log.info(f"📤 Uploading parsed data to gs://{bucket}/parsed_earnings/...")
        upload_directory_to_gcs(
            bucket_name=bucket,
            source_directory=str(PARSED_DIR),
            destination_blob_prefix='parsed_earnings/'
        )
        
        log.info(f"✅ All data uploaded to gs://{bucket}/")
        return True
        
    except Exception as e:
        log.error(f"❌ Upload failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        os.chdir(BASE_DIR)

def save_summary(download_results, parse_results, upload_success):
    """Save execution summary"""
    summary = {
        'timestamp': datetime.now().isoformat(),
        'download': {
            'successful': len(download_results.get('successful_companies', [])),
            'failed': len(download_results.get('failed_companies', [])),
        },
        'parsing': {
            'successful': sum(1 for r in parse_results.values() if r.get('success')),
            'total': len(parse_results),
        },
        'upload': upload_success
    }
    
    with open('pipeline_summary.json', 'w') as f:
        json.dump(summary, f, indent=2)
    
    log.info(f"\n📊 Summary saved to pipeline_summary.json")
    return summary

def main():
    """Run complete pipeline"""
    log.info("\n" + "="*70)
    log.info("🚀 DOW 30 COMPLETE PIPELINE - LOCAL EXECUTION")
    log.info("="*70 + "\n")
    
    start = time.time()
    
    # Step 1: Download
    download_results = step1_download_reports(workers=8)
    
    # Step 2: Parse
    parse_results = step2_parse_with_docling()
    
    # Step 3: Upload
    upload_success = step3_upload_to_gcs()
    
    # Summary
    duration = time.time() - start
    summary = save_summary(download_results, parse_results, upload_success)
    
    # Final report
    log.info("\n" + "="*70)
    log.info("🏁 PIPELINE COMPLETE")
    log.info("="*70)
    log.info(f"⏱️  Duration: {duration/60:.1f} minutes")
    log.info(f"📥 Downloaded: {summary['download']['successful']} companies")
    log.info(f"📄 Parsed: {summary['parsing']['successful']} PDFs")
    log.info(f"☁️  Uploaded: {'Yes' if upload_success else 'No'}")
    log.info("="*70)
    
    return upload_success

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        log.info("\n⚠️  Interrupted by user")
        sys.exit(1)
    except Exception as e:
        log.error(f"\n❌ Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
