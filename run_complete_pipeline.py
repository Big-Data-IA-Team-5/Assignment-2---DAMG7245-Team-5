#!/usr/bin/env python3
"""
Complete Pipeline Runner
Orchestrates the full flow: Docling → Guidance + GCS
"""

import sys
import logging
from pathlib import Path
from typing import List, Dict, Any
import json
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add modules to path
sys.path.insert(0, str(Path(__file__).parent / 'lantern-dow30'))
sys.path.insert(0, str(Path(__file__).parent / 'scripts'))

from config.pipeline_config import get_config, print_config_status
from collect_docling_outputs import DoclingOutputCollector
from gcs_utils import GCSManager, build_gcs_paths
from guidance_extractor import create_extractor
from manifest import create_manifest_manager
from state import create_state_manager
from qc import create_quality_checker
from periods import normalize_period


def print_banner(message: str):
    """Print a formatted banner."""
    print("\n" + "="*80)
    print(f"  {message}")
    print("="*80 + "\n")


def stage1_collect_docling_outputs(base_path: str) -> List[Dict[str, Any]]:
    """Stage 1: Collect Docling outputs from local filesystem."""
    print_banner("📂 STAGE 1: Collecting Docling Outputs")
    
    collector = DoclingOutputCollector(base_path)
    outputs = list(collector.find_docling_outputs())
    
    logger.info(f"Found {len(outputs)} Docling output sets")
    
    # Print summary
    stats = collector.get_summary_stats()
    print(f"\n📊 Collection Summary:")
    print(f"   Total reports: {stats['total_reports']}")
    print(f"   Unique tickers: {stats['unique_tickers']}")
    print(f"   Unique periods: {stats['unique_periods']}")
    print(f"   Total tables: {stats['total_tables']}")
    print(f"   Total images: {stats['total_images']}")
    
    return outputs


def stage2_normalize_periods(outputs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Stage 2: Normalize periods to YYYY-Q# format."""
    print_banner("📅 STAGE 2: Normalizing Periods")
    
    normalized = []
    for output in outputs:
        try:
            period = output.get('period', '')
            normalized_period = normalize_period(period)
            output['period'] = normalized_period
            normalized.append(output)
            logger.info(f"Normalized {output['ticker']}: {period} → {normalized_period}")
        except Exception as e:
            logger.warning(f"Failed to normalize period for {output.get('ticker')}: {e}")
    
    print(f"\n✅ Successfully normalized {len(normalized)}/{len(outputs)} periods")
    return normalized


def stage3_extract_structured(
    outputs: List[Dict[str, Any]], 
    config
) -> List[Dict[str, Any]]:
    """Stage 3: Extract structured data using Guidance + Gemini."""
    print_banner("🧠 STAGE 3: Extracting Structured Data (Guidance + Gemini)")
    
    extractor = create_extractor(
        api_key=config.gemini.api_key,
        model_name=config.gemini.model
    )
    
    processed = []
    for i, output in enumerate(outputs, 1):
        try:
            logger.info(f"Processing {i}/{len(outputs)}: {output['ticker']} {output['period']}")
            
            text_file = Path(output['docling_text_path'])
            table_files = [Path(f) for f in output.get('table_files', [])]
            
            structured = extractor.extract_from_files(
                text_file=text_file,
                ticker=output['ticker'],
                period=output['period'],
                published=output.get('published_iso', ''),
                report_type=output.get('report_type', 'press'),
                table_files=table_files
            )
            
            output['structured_data'] = structured
            output['extraction_success'] = True
            processed.append(output)
            
            print(f"   ✓ {output['ticker']} {output['period']} - "
                  f"Confidence: {structured.get('confidence', 0):.2f}")
            
        except Exception as e:
            logger.error(f"Extraction failed for {output.get('ticker')}: {e}")
            output['structured_data'] = {'error': str(e)}
            output['extraction_success'] = False
            processed.append(output)
    
    success_count = sum(1 for o in processed if o.get('extraction_success'))
    print(f"\n✅ Successfully extracted {success_count}/{len(processed)} reports")
    
    return processed


def stage4_upload_to_gcs(
    outputs: List[Dict[str, Any]], 
    config
) -> List[Dict[str, Any]]:
    """Stage 4: Upload raw PDFs and parsed assets to GCS."""
    print_banner("☁️  STAGE 4: Uploading to Google Cloud Storage")
    
    gcs = GCSManager(config.gcs.bucket_name, config.gcs.project_id)
    
    for i, output in enumerate(outputs, 1):
        try:
            logger.info(f"Uploading {i}/{len(outputs)}: {output['ticker']} {output['period']}")
            
            ticker = output['ticker']
            period = output['period']
            filename = Path(output['filename']).stem
            
            paths = build_gcs_paths(ticker, period, filename)
            uploaded = []
            
            # Upload raw PDF
            pdf_path = output['raw_pdf_path']
            if Path(pdf_path).exists():
                uri = gcs.upload_file(
                    pdf_path,
                    paths['raw_pdf'],
                    content_type='application/pdf',
                    metadata={
                        'ticker': ticker,
                        'period': period,
                        'sha256': output.get('sha256', '')
                    }
                )
                uploaded.append(uri)
            
            # Upload text file
            text_path = output['docling_text_path']
            if Path(text_path).exists():
                uri = gcs.upload_file(
                    text_path,
                    f"{paths['text_dir']}/{filename}.txt"
                )
                uploaded.append(uri)
            
            # Upload structured JSON
            if output.get('extraction_success') and output.get('structured_data'):
                uri = gcs.upload_json(
                    output['structured_data'],
                    paths['structured_json'],
                    metadata={'ticker': ticker, 'period': period}
                )
                uploaded.append(uri)
            
            output['uploaded_files'] = uploaded
            output['gcs_paths'] = paths
            output['upload_success'] = True
            
            print(f"   ✓ {ticker} {period} - Uploaded {len(uploaded)} files")
            
        except Exception as e:
            logger.error(f"Upload failed for {output.get('ticker')}: {e}")
            output['upload_success'] = False
    
    success_count = sum(1 for o in outputs if o.get('upload_success'))
    print(f"\n✅ Successfully uploaded {success_count}/{len(outputs)} reports")
    
    return outputs


def stage5_write_manifests(outputs: List[Dict[str, Any]], config) -> List[Dict[str, Any]]:
    """Stage 5: Write manifest entries."""
    print_banner("📋 STAGE 5: Writing Manifest Entries")
    
    gcs = GCSManager(config.gcs.bucket_name, config.gcs.project_id)
    manifest_mgr = create_manifest_manager(gcs, config.gcs.bucket_name)
    
    for i, output in enumerate(outputs, 1):
        try:
            logger.info(f"Writing manifest {i}/{len(outputs)}: {output['ticker']} {output['period']}")
            
            entry = manifest_mgr.create_entry(
                ticker=output['ticker'],
                period=output['period'],
                published=output.get('published_iso', ''),
                report_type=output.get('report_type', 'press'),
                source_url=output.get('source_url', ''),
                sha256=output.get('sha256', ''),
                filename=output.get('filename', '')
            )
            
            entry.set_docling_info(
                pages=output.get('pages', 0),
                tables=output.get('tables', 0),
                version="1.0.0"
            )
            
            if output.get('extraction_success'):
                structured = output.get('structured_data', {})
                entry.set_structured_info(
                    model=config.gemini.model,
                    confidence=structured.get('confidence', 0),
                    gcs_path=output.get('gcs_paths', {}).get('structured_json', '')
                )
            
            manifest_path = f"lantern/parsed/{output['ticker']}/{output['period']}/manifest.jsonl"
            uri = manifest_mgr.write_manifest_entry(entry, manifest_path)
            
            output['manifest_uri'] = uri
            output['manifest_success'] = True
            
            print(f"   ✓ {output['ticker']} {output['period']}")
            
        except Exception as e:
            logger.error(f"Manifest write failed for {output.get('ticker')}: {e}")
            output['manifest_success'] = False
    
    success_count = sum(1 for o in outputs if o.get('manifest_success'))
    print(f"\n✅ Successfully wrote {success_count}/{len(outputs)} manifests")
    
    return outputs


def stage6_run_quality_checks(outputs: List[Dict[str, Any]], config) -> List[Dict[str, Any]]:
    """Stage 6: Run quality checks."""
    print_banner("✅ STAGE 6: Running Quality Checks")
    
    gcs = GCSManager(config.gcs.bucket_name, config.gcs.project_id)
    qc = create_quality_checker(gcs)
    
    for i, output in enumerate(outputs, 1):
        try:
            logger.info(f"QC check {i}/{len(outputs)}: {output['ticker']} {output['period']}")
            
            docling_outputs = {
                'text_file': output.get('docling_text_path', ''),
                'table_files': output.get('table_files', []),
                'image_files': output.get('image_files', [])
            }
            
            results = qc.run_full_quality_check(
                ticker=output['ticker'],
                period=output['period'],
                filename=output.get('filename', ''),
                docling_outputs=docling_outputs,
                structured_data=output.get('structured_data', {}),
                manifest_entry={}
            )
            
            output['qc_results'] = results
            output['qc_passed'] = results['overall_passed']
            
            status = "✓ PASSED" if results['overall_passed'] else "✗ FAILED"
            print(f"   {status} {output['ticker']} {output['period']} - "
                  f"{results['error_count']} errors, {results['warning_count']} warnings")
            
        except Exception as e:
            logger.error(f"QC failed for {output.get('ticker')}: {e}")
            output['qc_passed'] = False
    
    passed_count = sum(1 for o in outputs if o.get('qc_passed'))
    print(f"\n✅ Quality checks: {passed_count}/{len(outputs)} passed")
    
    return outputs


def generate_final_summary(outputs: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Generate final processing summary."""
    print_banner("📊 FINAL SUMMARY")
    
    summary = {
        'total_reports': len(outputs),
        'extraction_success': sum(1 for o in outputs if o.get('extraction_success')),
        'upload_success': sum(1 for o in outputs if o.get('upload_success')),
        'manifest_success': sum(1 for o in outputs if o.get('manifest_success')),
        'qc_passed': sum(1 for o in outputs if o.get('qc_passed')),
        'avg_confidence': 0.0,
        'timestamp': datetime.now().isoformat()
    }
    
    confidences = [
        o.get('structured_data', {}).get('confidence', 0)
        for o in outputs
        if o.get('extraction_success')
    ]
    
    if confidences:
        summary['avg_confidence'] = round(sum(confidences) / len(confidences), 3)
    
    print(f"Total Reports Processed: {summary['total_reports']}")
    print(f"Successful Extractions:  {summary['extraction_success']}")
    print(f"Successful Uploads:      {summary['upload_success']}")
    print(f"Manifests Written:       {summary['manifest_success']}")
    print(f"QC Passed:               {summary['qc_passed']}")
    print(f"Average Confidence:      {summary['avg_confidence']:.3f}")
    
    # Save summary
    summary_file = Path('pipeline_summary.json')
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"\n💾 Summary saved to: {summary_file}")
    
    return summary


def main():
    """Run the complete pipeline."""
    print_banner("🚀 LANTERN POST-DOCLING PIPELINE")
    print("Complete flow: Docling → Guidance + GCS + Manifest\n")
    
    # Load and validate configuration
    config = get_config()
    status = print_config_status()
    
    if not status['valid']:
        print("\n❌ Configuration invalid. Please fix errors and try again.")
        return 1
    
    # Run pipeline stages
    try:
        # Stage 1: Collect Docling outputs
        outputs = stage1_collect_docling_outputs(config.input_dir)
        
        if not outputs:
            print("\n⚠️  No Docling outputs found. Run Docling parser first.")
            return 1
        
        # Stage 2: Normalize periods
        outputs = stage2_normalize_periods(outputs)
        
        # Stage 3: Extract structured data
        outputs = stage3_extract_structured(outputs, config)
        
        # Stage 4: Upload to GCS
        outputs = stage4_upload_to_gcs(outputs, config)
        
        # Stage 5: Write manifests
        outputs = stage5_write_manifests(outputs, config)
        
        # Stage 6: Quality checks
        outputs = stage6_run_quality_checks(outputs, config)
        
        # Generate summary
        summary = generate_final_summary(outputs)
        
        print_banner("✨ PIPELINE COMPLETE")
        print("All stages executed successfully!")
        
        return 0
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        print(f"\n❌ Pipeline failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
