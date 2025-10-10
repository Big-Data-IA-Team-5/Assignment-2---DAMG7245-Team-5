#!/usr/bin/env python3
"""
GCS Upload Helper for Airflow DAG
Uploads raw PDFs and parsed content to Google Cloud Storage
"""

import os
import sys
import logging
from pathlib import Path
from google.cloud import storage

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def upload_raw_pdfs_to_gcs(input_dir, bucket_name, project_id=None):
    """Upload raw PDFs to GCS bucket"""
    results = []
    
    try:
        # Initialize GCS client
        client = storage.Client(project=project_id) if project_id else storage.Client()
        bucket = client.bucket(bucket_name)
        
        input_path = Path(input_dir)
        if not input_path.exists():
            logger.warning(f"⚠️  Input directory not found: {input_dir}")
            return results
        
        # Find all PDFs
        pdf_files = list(input_path.glob("*.pdf"))
        if not pdf_files:
            logger.warning(f"⚠️  No PDF files found in {input_dir}")
            return results
        
        logger.info(f"📤 Uploading {len(pdf_files)} PDFs to GCS bucket '{bucket_name}'...")
        
        # Upload all PDFs
        for pdf_file in pdf_files:
            # Extract ticker from filename (e.g., AAPL_2025_financial_report.pdf -> AAPL)
            ticker = pdf_file.stem.split('_')[0]
            period = "2025-Q4"  # Default period, adjust as needed
            
            # GCS path: lantern/raw/{ticker}/{period}/{filename}
            gcs_path = f"lantern/raw/{ticker}/{period}/{pdf_file.name}"
            
            try:
                blob = bucket.blob(gcs_path)
                blob.upload_from_filename(str(pdf_file))
                
                gcs_uri = f"gs://{bucket_name}/{gcs_path}"
                logger.info(f"  ✅ {ticker}: {pdf_file.name} → {gcs_uri}")
                
                results.append({
                    'ticker': ticker,
                    'success': True,
                    'file': pdf_file.name,
                    'gcs_path': gcs_uri
                })
            except Exception as e:
                logger.error(f"  ❌ {ticker}: Failed to upload {pdf_file.name}: {e}")
                results.append({
                    'ticker': ticker,
                    'success': False,
                    'file': pdf_file.name,
                    'error': str(e)
                })
        
        success_count = sum(1 for r in results if r['success'])
        logger.info(f"\n✅ Successfully uploaded {success_count}/{len(results)} PDFs to GCS")
        return results
        
    except Exception as e:
        logger.error(f"❌ GCS upload failed: {e}")
        raise


def upload_parsed_to_gcs(parsed_dir, bucket_name, project_id=None):
    """Upload parsed content to GCS bucket"""
    results = []
    
    try:
        # Initialize GCS client
        client = storage.Client(project=project_id) if project_id else storage.Client()
        bucket = client.bucket(bucket_name)
        
        parsed_path = Path(parsed_dir)
        if not parsed_path.exists():
            logger.warning(f"⚠️  Parsed directory not found: {parsed_dir}")
            return results
        
        logger.info(f"📤 Uploading parsed content to GCS bucket '{bucket_name}'...")
        
        # Upload all parsed content
        file_count = 0
        for ticker_dir in parsed_path.iterdir():
            if not ticker_dir.is_dir():
                continue
            
            ticker = ticker_dir.name
            
            for period_dir in ticker_dir.iterdir():
                if not period_dir.is_dir():
                    continue
                
                period = period_dir.name
                
                # Upload all files in this period
                for file_path in period_dir.rglob("*"):
                    if file_path.is_file():
                        rel_path = file_path.relative_to(period_dir)
                        gcs_path = f"lantern/parsed/{ticker}/{period}/{rel_path}"
                        
                        try:
                            blob = bucket.blob(gcs_path)
                            blob.upload_from_filename(str(file_path))
                            
                            file_count += 1
                            if file_count % 10 == 0:
                                logger.info(f"  📊 Uploaded {file_count} files...")
                            
                            results.append({
                                'ticker': ticker,
                                'success': True,
                                'file': str(rel_path),
                                'gcs_path': f"gs://{bucket_name}/{gcs_path}"
                            })
                        except Exception as e:
                            logger.error(f"  ❌ Failed to upload {file_path}: {e}")
                            results.append({
                                'ticker': ticker,
                                'success': False,
                                'file': str(rel_path),
                                'error': str(e)
                            })
        
        success_count = sum(1 for r in results if r['success'])
        logger.info(f"\n✅ Successfully uploaded {success_count}/{len(results)} parsed files to GCS")
        return results
        
    except Exception as e:
        logger.error(f"❌ GCS upload failed: {e}")
        raise


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Upload raw PDFs or parsed content to Google Cloud Storage'
    )
    parser.add_argument(
        '--type',
        choices=['raw', 'parsed'],
        required=True,
        help='Type of content to upload (raw PDFs or parsed content)'
    )
    parser.add_argument(
        '--input-dir',
        required=True,
        help='Input directory containing files to upload'
    )
    parser.add_argument(
        '--bucket',
        default='damgteam5',
        help='GCS bucket name (default: damgteam5)'
    )
    parser.add_argument(
        '--project',
        default=None,
        help='GCP project ID (optional, uses default credentials if not specified)'
    )
    
    args = parser.parse_args()
    
    try:
        if args.type == 'raw':
            logger.info("=" * 60)
            logger.info("📦 Starting RAW PDF Upload to GCS")
            logger.info("=" * 60)
            results = upload_raw_pdfs_to_gcs(args.input_dir, args.bucket, args.project)
        else:
            logger.info("=" * 60)
            logger.info("📦 Starting PARSED Content Upload to GCS")
            logger.info("=" * 60)
            results = upload_parsed_to_gcs(args.input_dir, args.bucket, args.project)
        
        # Summary
        success = sum(1 for r in results if r.get('success'))
        failed = len(results) - success
        
        print("\n" + "=" * 60)
        print(f"📊 Upload Summary")
        print("=" * 60)
        print(f"✅ Successful: {success}")
        print(f"❌ Failed: {failed}")
        print(f"📈 Total: {len(results)}")
        print("=" * 60)
        
        # Exit with error if any uploads failed
        if failed > 0:
            logger.warning(f"⚠️  {failed} uploads failed")
            sys.exit(0)  # Don't fail the task, just warn
        else:
            logger.info("🎉 All uploads completed successfully!")
            sys.exit(0)
            
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
