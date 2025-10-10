"""
Docling Parser Wrapper for Airflow DAGs
Imports the actual parser from the root directory
"""

import sys
from pathlib import Path

# Add root directory to path to import the actual docling_parser
sys.path.insert(0, str(Path(__file__).parent.parent))

from docling_parser import DoclingParser

# Create alias for compatibility
DoclingPDFParser = DoclingParser

# For direct execution
if __name__ == "__main__":
    import os
    
    DOWNLOAD_DIR = os.environ.get('RAW_PDF_DIR', '/opt/airflow/data/dow30_pipeline_reports_2025')
    PARSED_DIR = os.environ.get('DOCLING_OUTPUT_DIR', '/opt/airflow/data/parsed_local')
    
    print(f"Processing PDFs from {DOWNLOAD_DIR} to {PARSED_DIR}")
    
    parser = DoclingParser(
        input_dir=DOWNLOAD_DIR,
        output_dir=PARSED_DIR
    )
    
    results = parser.process_all_pdfs()
    
    print(f"\n✅ Processed {len(results)} PDFs")
    for ticker, result in results.items():
        if result.get('success'):
            print(f"  ✅ {ticker}: Success")
        else:
            print(f"  ❌ {ticker}: {result.get('error', 'Failed')}")
