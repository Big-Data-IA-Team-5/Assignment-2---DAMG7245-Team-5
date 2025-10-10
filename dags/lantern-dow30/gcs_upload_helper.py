#!/usr/bin/env python3
"""
Simple GCS upload helpers for Airflow DAG
"""

import os
import sys
import logging
from pathlib import Path
from google.cloud import storage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def upload_raw_pdfs_to_gcs(input_dir, bucket_name, project_id=None):
    """Upload raw PDFs to GCS bucket"""
    results = []
    
    try:
        client = storage.Client(project=project_id)
        bucket = client.bucket(bucket_name)
        
        input_path = Path(input_dir)
        if not input_path.exists():
            logger.warning(f"Input directory not found: {input_dir}")
            return results
        
        # Upload all PDFs
        for pdf_file in input_path.glob("*.pdf"):
            ticker = pdf_file.stem.split('_')[0]
            period = "2025-Q4"  # Default period
            
            gcs_path = f"lantern/raw/{ticker}/{period}/{pdf_file.name}"
            
            try:
                blob = bucket.blob(gcs_path)
                blob.upload_from_filename(str(pdf_file))
                
                gcs_uri = f"gs://{bucket_name}/{gcs_path}"
                logger.info(f"✅ Uploaded: {gcs_uri}")
                
                results.append({
                    'ticker': ticker,
                    'success': True,
                    'gcs_path': gcs_uri
                })
            except Exception as e:
                logger.error(f"❌ Failed to upload {pdf_file.name}: {e}")
                results.append({
                    'ticker': ticker,
                    'success': False,
                    'error': str(e)
                })
        
        logger.info(f"Uploaded {len([r for r in results if r['success']])} PDFs to GCS")
        return results
        
    except Exception as e:
        logger.error(f"GCS upload failed: {e}")
        return results


def upload_parsed_to_gcs(parsed_dir, bucket_name, project_id=None):
    """Upload parsed content to GCS bucket"""
    results = []
    
    try:
        client = storage.Client(project=project_id)
        bucket = client.bucket(bucket_name)
        
        parsed_path = Path(parsed_dir)
        if not parsed_path.exists():
            logger.warning(f"Parsed directory not found: {parsed_dir}")
            return results
        
        # Upload all parsed content
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
                            
                            results.append({
                                'ticker': ticker,
                                'success': True,
                                'file': str(rel_path),
                                'gcs_path': f"gs://{bucket_name}/{gcs_path}"
                            })
                        except Exception as e:
                            logger.error(f"Failed to upload {file_path}: {e}")
                            results.append({
                                'ticker': ticker,
                                'success': False,
                                'error': str(e)
                            })
        
        logger.info(f"Uploaded {len([r for r in results if r['success']])} parsed files to GCS")
        return results
        
    except Exception as e:
        logger.error(f"GCS upload failed: {e}")
        return results


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
        
        # Exit successfully even if some uploads failed (don't break the pipeline)
        if failed > 0:
            logger.warning(f"⚠️  {failed} uploads failed, but continuing...")
        else:
            logger.info("🎉 All uploads completed successfully!")
        
        sys.exit(0)
            
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
