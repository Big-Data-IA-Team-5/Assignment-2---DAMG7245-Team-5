#!/usr/bin/env python3
"""
Test pipeline with AAPL - Should have tables and images
"""

import os
import sys
import json
import hashlib
from pathlib import Path
from datetime import datetime

# Add lantern-dow30 to path
sys.path.insert(0, str(Path(__file__).parent / "lantern-dow30"))

print("=" * 70)
print("🧪 PIPELINE TEST - AAPL (Apple)")
print("=" * 70)

REPORTS_DIR = Path("dow30_pipeline_reports_2025")
PARSED_DIR = Path("parsed_local")
REPORTS_DIR.mkdir(exist_ok=True)

# ============================================================================
# STEP 1: Download AAPL report
# ============================================================================

print("\n📥 STEP 1: Downloading AAPL earnings report...")

# Check if already downloaded
aapl_files = list(REPORTS_DIR.glob("AAPL*.pdf"))
if aapl_files:
    pdf_path = aapl_files[0]
    pdf_file = pdf_path.name
    print(f"✅ AAPL PDF already downloaded - {pdf_file}")
    print(f"   Size: {pdf_path.stat().st_size / 1024:.1f} KB")
else:
    print("   Downloading from Apple investor relations...")
    try:
        from main import MultithreadedDow30Pipeline
        
        pipeline = MultithreadedDow30Pipeline(
            output_dir='dow30_pipeline_reports_2025',
            max_workers=1
        )
        
        result = pipeline.run_multithreaded_pipeline(target_companies=['AAPL'])
        
        # Find the downloaded file
        aapl_files = list(REPORTS_DIR.glob("AAPL*.pdf"))
        if aapl_files:
            pdf_path = aapl_files[0]
            pdf_file = pdf_path.name
            print(f"   ✅ Downloaded: {pdf_file}")
        else:
            print("   ❌ Download failed")
            sys.exit(1)
            
    except Exception as e:
        print(f"   ❌ Download error: {e}")
        sys.exit(1)

# ============================================================================
# STEP 2: Parse with Docling
# ============================================================================

print("\n📄 STEP 2: Parsing PDF with Docling...")
print("   Extracting text, tables, and images...")

from docling_parser import DoclingPDFParser

# Initialize Docling parser
parser = DoclingPDFParser(
    input_dir=str(REPORTS_DIR),
    output_dir=str(PARSED_DIR)
)

# Create PDF info dict
pdf_info = {
    "filename": pdf_path.name,
    "filepath": str(pdf_path),
    "ticker": "AAPL",
    "period": "2025-Q4",
    "file_size": pdf_path.stat().st_size,
    "sha256": hashlib.sha256(pdf_path.read_bytes()).hexdigest()
}

# Parse
result = parser.process_pdf_with_docling(pdf_info)

if result and result.get('success'):
    print(f"   ✅ Parsing complete!")
    
    # Build output path
    output_path = Path('parsed_local') / 'AAPL' / '2025-Q4'
    
    # Check what was extracted
    text_files = list(output_path.glob('text/*.txt'))
    table_files = list(output_path.glob('tables/*.csv'))
    image_files = list(output_path.glob('images/*'))
    
    print(f"\n📊 EXTRACTION RESULTS:")
    print(f"   📝 Text files: {len(text_files)}")
    for f in text_files:
        size = f.stat().st_size
        print(f"      - {f.name} ({size:,} bytes)")
    
    print(f"   📊 Tables: {len(table_files)}")
    for f in table_files:
        size = f.stat().st_size
        print(f"      - {f.name} ({size:,} bytes)")
    
    print(f"   🖼️  Images: {len(image_files)}")
    for f in sorted(image_files)[:5]:  # Show first 5
        size = f.stat().st_size
        print(f"      - {f.name} ({size / 1024:.1f} KB)")
    if len(image_files) > 5:
        print(f"      ... and {len(image_files) - 5} more images")
    
    # Show sample table content if available
    if table_files:
        print(f"\n📋 SAMPLE TABLE (first 5 rows of {table_files[0].name}):")
        try:
            import pandas as pd
            df = pd.read_csv(table_files[0])
            print(df.head().to_string(index=False))
        except:
            with open(table_files[0]) as f:
                lines = f.readlines()[:5]
                for line in lines:
                    print(f"   {line.strip()}")
    
    # Show metadata
    metadata_file = output_path / "metadata.json"
    if metadata_file.exists():
        with open(metadata_file) as f:
            metadata = json.load(f)
        
        extracted = metadata.get('extracted_content', {})
        print(f"\n📋 SUMMARY:")
        print(f"   Pages: {extracted.get('pages', 0)}")
        print(f"   Text extracted: {len(text_files) > 0}")
        print(f"   Tables extracted: {extracted.get('tables', 0)}")
        print(f"   Images extracted: {extracted.get('images', 0)}")
    
    print(f"\n✅ TEST COMPLETE!")
    print(f"📁 Output: {output_path}")
    
    # Summary
    if table_files or image_files:
        print(f"\n🎉 SUCCESS! Found tables and/or images in AAPL report")
    else:
        print(f"\nℹ️  Note: AAPL file may be text-only (transcript vs 10-Q)")
    
else:
    print(f"\n❌ Parsing failed: {result.get('error', 'Unknown error')}")
    sys.exit(1)
