"""
Real Docling PDF Parser - Extract text, tables, and images from financial reports

This implementation uses the actual Docling library to parse PDFs and extract:
- Text content → parsed_local/{ticker}/{period}/text/report.txt
- Tables → parsed_local/{ticker}/{period}/tables/table_1.csv, table_2.csv, etc.
- Images → parsed_local/{ticker}/{period}/images/page_1.png, page_2.png, etc.
- Metadata → parsed_local/{ticker}/{period}/metadata.json
"""

import os
import sys
import json
import hashlib
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime
import re
import traceback

# Import Docling
try:
    from docling.document_converter import DocumentConverter, PdfFormatOption
    from docling.datamodel.base_models import InputFormat
    from docling.datamodel.pipeline_options import PdfPipelineOptions, TableFormerMode
    from docling.backend.pypdfium2_backend import PyPdfiumDocumentBackend
    print("✅ Docling imported successfully")
except ImportError as e:
    print(f"❌ Failed to import Docling: {e}")
    print("Install with: pip install docling")
    sys.exit(1)

# Try importing additional dependencies
try:
    import pandas as pd
    from PIL import Image
    import fitz  # PyMuPDF for better image extraction
    print("✅ Additional dependencies available (pandas, PIL, PyMuPDF)")
except ImportError as e:
    print(f"⚠️ Some dependencies missing: {e}")
    print("For better results, install: pip install pandas pillow PyMuPDF")

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DoclingPDFParser:
    """Real PDF parser using Docling for financial reports."""
    
    def __init__(self, input_dir="dow30_pipeline_reports_2025", output_dir="parsed_local"):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Configure pipeline options for better extraction
        pipeline_options = PdfPipelineOptions()
        pipeline_options.do_ocr = False  # Disable OCR for faster processing
        pipeline_options.do_table_structure = True  # Enable table extraction
        pipeline_options.table_structure_options.do_cell_matching = True
        pipeline_options.table_structure_options.mode = TableFormerMode.ACCURATE
        pipeline_options.images_scale = 2.0  # Higher resolution images
        pipeline_options.generate_page_images = True  # Extract page images
        pipeline_options.generate_picture_images = True  # Extract embedded pictures
        
        # Create format option with backend
        pdf_format_option = PdfFormatOption(
            pipeline_options=pipeline_options,
            backend=PyPdfiumDocumentBackend
        )
        
        # Initialize Docling converter with configured options
        try:
            self.converter = DocumentConverter(
                format_options={
                    InputFormat.PDF: pdf_format_option,
                }
            )
            logger.info("✅ Docling converter initialized with table & image extraction")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Docling converter: {e}")
            raise
        
        logger.info(f"📁 Input directory: {self.input_dir}")
        logger.info(f"📁 Output directory: {self.output_dir}")
    
    def discover_pdfs(self):
        """Discover PDF files in input directory."""
        pdfs = []
        
        if not self.input_dir.exists():
            logger.error(f"❌ Input directory not found: {self.input_dir}")
            return pdfs
            
        pdf_files = list(self.input_dir.glob("*.pdf"))
        logger.info(f"🔍 Found {len(pdf_files)} PDF files")
        
        for pdf_path in pdf_files:
            try:
                ticker, period = self._parse_filename(pdf_path.name)
                file_hash = self._calculate_hash(pdf_path)
                
                pdf_info = {
                    "filename": pdf_path.name,
                    "filepath": str(pdf_path),
                    "ticker": ticker,
                    "period": period,
                    "file_size": pdf_path.stat().st_size,
                    "sha256": file_hash
                }
                pdfs.append(pdf_info)
                logger.info(f"📄 {ticker} {period} - {pdf_path.name} ({pdf_info['file_size']:,} bytes)")
                
            except Exception as e:
                logger.error(f"❌ Failed to analyze {pdf_path.name}: {e}")
        
        return pdfs
    
    def _parse_filename(self, filename):
        """Parse ticker and period from filename."""
        name = filename.replace('.pdf', '').upper()
        
        # Extract ticker (2-5 uppercase letters at start)
        ticker_match = re.match(r'^([A-Z]{2,5})', name)
        ticker = ticker_match.group(1) if ticker_match else "UNKNOWN"
        
        # Extract period (YYYY-Q# format)
        period_match = re.search(r'(\d{4})[_\-]?Q?([1-4])', name)
        if period_match:
            year, quarter = period_match.groups()
            period = f"{year}-Q{quarter}"
        else:
            period = "2025-Q4"  # Default fallback
            
        return ticker, period
    
    def _calculate_hash(self, file_path):
        """Calculate SHA256 hash of file."""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()
    
    def _extract_images_pymupdf(self, pdf_path: Path, images_dir: Path) -> int:
        """
        Extract images using PyMuPDF (more reliable than Docling for images)
        Based on extract_images_direct.py implementation
        """
        try:
            import fitz
            import hashlib
            
            doc = fitz.open(str(pdf_path))
            image_count = 0
            seen_hashes = set()
            
            logger.info(f"  🖼️ Extracting embedded images with PyMuPDF...")
            
            # Extract only embedded images (charts, logos, photos)
            for page_num in range(len(doc)):
                page = doc[page_num]
                image_list = page.get_images(full=True)
                
                for img_index, img in enumerate(image_list):
                    try:
                        xref = img[0]
                        base_image = doc.extract_image(xref)
                        image_bytes = base_image["image"]
                        image_ext = base_image["ext"]
                        
                        # Calculate hash to detect duplicates
                        img_hash = hashlib.md5(image_bytes).hexdigest()
                        
                        # Skip if we've already saved this exact image
                        if img_hash in seen_hashes:
                            logger.debug(f"    ⊘ Duplicate skipped: page {page_num+1}")
                            continue
                        
                        # Mark this hash as seen
                        seen_hashes.add(img_hash)
                        
                        # Save the unique image
                        image_file = images_dir / f"page{page_num+1}_img{img_index+1}.{image_ext}"
                        with open(image_file, 'wb') as f:
                            f.write(image_bytes)
                        
                        image_count += 1
                        logger.info(f"    ✅ Saved unique image from page {page_num+1}")
                        
                    except Exception as e:
                        logger.debug(f"    ⚠️ Failed to extract image from page {page_num+1}: {e}")
            
            doc.close()
            
            if image_count > 0:
                logger.info(f"  ✓ Extracted {image_count} unique images (duplicates removed)")
            
            return image_count
            
        except ImportError:
            logger.warning("  ⚠️ PyMuPDF not installed - skipping image extraction")
            logger.info("     Install with: pip install PyMuPDF")
            return 0
        except Exception as e:
            logger.error(f"  ❌ PyMuPDF extraction failed: {e}")
            return 0
    
    def process_pdf_with_docling(self, pdf_info):
        """Process a PDF file using actual Docling."""
        ticker = pdf_info["ticker"]
        period = pdf_info["period"]
        pdf_path = Path(pdf_info["filepath"])
        
        logger.info(f"🔄 Processing {ticker} {period} with Docling...")
        
        # Create output directories
        base_dir = self.output_dir / ticker / period
        text_dir = base_dir / "text"
        tables_dir = base_dir / "tables"
        images_dir = base_dir / "images"
        
        for dir_path in [text_dir, tables_dir, images_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
        
        extracted_content = {
            "text_files": [],
            "table_files": [],
            "image_files": [],
            "pages": 0,
            "tables": 0,
            "images": 0
        }
        
        try:
            # Convert PDF with Docling
            logger.info(f"  📖 Converting PDF: {pdf_path.name}")
            result = self.converter.convert(str(pdf_path))
            document = result.document
            
            logger.info(f"  ✅ PDF converted successfully")
            
            # Extract text content
            try:
                text_content = document.export_to_text()
                if text_content and text_content.strip():
                    text_file = text_dir / "report.txt"
                    with open(text_file, 'w', encoding='utf-8') as f:
                        f.write(text_content)
                    extracted_content["text_files"].append(str(text_file))
                    logger.info(f"  📝 Extracted text: {len(text_content):,} characters")
                else:
                    logger.warning(f"  ⚠️ No text content extracted")
            except Exception as e:
                logger.error(f"  ❌ Text extraction failed: {e}")
            
            # Extract tables
            table_count = 0
            try:
                if hasattr(document, 'tables') and document.tables:
                    logger.info(f"  📊 Found {len(document.tables)} tables")
                    
                    for i, table in enumerate(document.tables):
                        try:
                            table_file = tables_dir / f"table_{i+1}.csv"
                            
                            # Try different table export methods
                            table_saved = False
                            
                            # Method 1: export_to_dataframe (best quality)
                            if hasattr(table, 'export_to_dataframe'):
                                try:
                                    df = table.export_to_dataframe()
                                    if df is not None and not df.empty:
                                        df.to_csv(table_file, index=False)
                                        table_saved = True
                                        logger.info(f"    ✅ Saved table {i+1} ({len(df)} rows)")
                                except Exception as e:
                                    logger.debug(f"      export_to_dataframe failed: {e}")
                            
                            # Method 2: Direct CSV export
                            if not table_saved and hasattr(table, 'export_to_csv'):
                                try:
                                    table.export_to_csv(str(table_file))
                                    table_saved = True
                                    logger.info(f"    ✅ Saved table {i+1} via CSV export")
                                except Exception as e:
                                    logger.debug(f"      export_to_csv failed: {e}")
                            
                            # Method 3: to_dataframe
                            if not table_saved and hasattr(table, 'to_dataframe'):
                                try:
                                    df = table.to_dataframe()
                                    if df is not None and not df.empty:
                                        df.to_csv(table_file, index=False)
                                        table_saved = True
                                        logger.info(f"    ✅ Saved table {i+1} via to_dataframe")
                                except Exception as e:
                                    logger.debug(f"      to_dataframe failed: {e}")
                            
                            # Method 4: Export to markdown and convert to CSV
                            if not table_saved and hasattr(table, 'export_to_markdown'):
                                try:
                                    md_text = table.export_to_markdown()
                                    if md_text:
                                        # Convert markdown table to CSV
                                        lines = [l.strip() for l in md_text.split('\n') if l.strip() and not l.strip().startswith('|--')]
                                        if lines:
                                            csv_lines = []
                                            for line in lines:
                                                cells = [c.strip() for c in line.split('|') if c.strip()]
                                                if cells:
                                                    csv_lines.append(','.join(f'"{c}"' for c in cells))
                                            
                                            if csv_lines:
                                                with open(table_file, 'w') as f:
                                                    f.write('\n'.join(csv_lines))
                                                table_saved = True
                                                logger.info(f"    ✅ Saved table {i+1} from markdown")
                                except Exception as e:
                                    logger.debug(f"      markdown export failed: {e}")
                            
                            if table_saved:
                                extracted_content["table_files"].append(str(table_file))
                                table_count += 1
                            else:
                                logger.warning(f"    ⚠️ Could not extract table {i+1}")
                                
                        except Exception as e:
                            logger.error(f"    ❌ Table {i+1} processing failed: {e}")
                
                extracted_content["tables"] = table_count
                if table_count > 0:
                    logger.info(f"  📊 Extracted {table_count} tables successfully")
                else:
                    logger.info(f"  ℹ️ No tables found or extracted from document")
                    
            except Exception as e:
                logger.error(f"  ❌ Table extraction failed: {e}")
            
            # Extract images - Use PyMuPDF for better results
            image_count = 0
            
            # Method 1: PyMuPDF (most reliable for embedded images)
            try:
                pymupdf_count = self._extract_images_pymupdf(Path(pdf_path), images_dir)
                if pymupdf_count > 0:
                    image_count = pymupdf_count
                    logger.info(f"  🖼️ Extracted {image_count} images with PyMuPDF")
                    extracted_content["images"] = image_count
                    extracted_content["image_files"] = [str(f) for f in images_dir.glob('*')]
            except Exception as e:
                logger.warning(f"  ⚠️ PyMuPDF extraction failed: {e}")
            
            # Method 2: Fallback to Docling if PyMuPDF found nothing
            if image_count == 0:
                try:
                    # Extract pictures/figures from document
                    if hasattr(document, 'pictures') and document.pictures:
                        logger.info(f"  🖼️ Found {len(document.pictures)} pictures in document")
                        
                        for i, picture in enumerate(document.pictures):
                            try:
                                image_file = images_dir / f"figure_{i+1}.png"
                                
                                # Try different image save methods
                                image_saved = False
                                
                                # Try get_image() method
                                if hasattr(picture, 'get_image'):
                                    try:
                                        img = picture.get_image()
                                        if img:
                                            img.save(str(image_file))
                                            image_saved = True
                                    except Exception as e:
                                        logger.debug(f"      get_image() failed: {e}")
                                
                                # Try pil_image attribute
                                if not image_saved and hasattr(picture, 'pil_image'):
                                    try:
                                        if picture.pil_image:
                                            picture.pil_image.save(str(image_file))
                                            image_saved = True
                                    except Exception as e:
                                        logger.debug(f"      pil_image failed: {e}")
                                
                                # Try image attribute
                                if not image_saved and hasattr(picture, 'image'):
                                    try:
                                        if picture.image:
                                            picture.image.save(str(image_file))
                                            image_saved = True
                                    except Exception as e:
                                        logger.debug(f"      image failed: {e}")
                                
                                if image_saved:
                                    extracted_content["image_files"].append(str(image_file))
                                    image_count += 1
                                    logger.info(f"    ✅ Saved figure {i+1}")
                                else:
                                    logger.debug(f"    ⚠️ Could not save figure {i+1}")
                                
                            except Exception as e:
                                logger.warning(f"    ⚠️ Figure {i+1} extraction failed: {e}")
                    
                    # Method 2: Export page images using Docling's export
                    if hasattr(result, 'render_as_image'):
                        logger.info(f"  📄 Rendering pages as images...")
                        try:
                            # Render each page as PNG
                            for page_num in range(len(document.pages)):
                                page_image_file = images_dir / f"page_{page_num + 1}.png"
                                result.render_as_image(page_num).save(str(page_image_file))
                                extracted_content["image_files"].append(str(page_image_file))
                                image_count += 1
                                logger.info(f"    ✅ Rendered page {page_num + 1}")
                        except Exception as e:
                            logger.warning(f"    ⚠️ Page rendering failed: {e}")
                    
                    # Method 3: Extract from pages using export_to_image
                    elif hasattr(document, 'export_to_image'):
                        logger.info(f"  📄 Exporting pages to images...")
                        try:
                            images = document.export_to_image()
                            for page_num, img in enumerate(images, 1):
                                page_image_file = images_dir / f"page_{page_num}.png"
                                img.save(str(page_image_file))
                                extracted_content["image_files"].append(str(page_image_file))
                                image_count += 1
                                logger.info(f"    ✅ Exported page {page_num}")
                        except Exception as e:
                            logger.warning(f"    ⚠️ Page export failed: {e}")
                    
                    # Method 4: Manual page image extraction from pages
                    elif hasattr(document, 'pages') and document.pages:
                        logger.info(f"  📄 Extracting images from {len(document.pages)} pages")
                        extracted_content["pages"] = len(document.pages)
                        
                        for page_num, page in enumerate(document.pages, 1):
                            try:
                                # Try different page image attributes
                                page_img = None
                                
                                if hasattr(page, 'image') and page.image:
                                    page_img = page.image
                                elif hasattr(page, 'get_image'):
                                    page_img = page.get_image()
                                elif hasattr(page, 'pil_image'):
                                    page_img = page.pil_image
                                
                                if page_img:
                                    page_image_file = images_dir / f"page_{page_num}.png"
                                    page_img.save(str(page_image_file))
                                    extracted_content["image_files"].append(str(page_image_file))
                                    image_count += 1
                                    logger.info(f"    ✅ Saved page {page_num} image")
                            except Exception as e:
                                logger.debug(f"    ⚠️ Page {page_num} image extraction failed: {e}")
                    
                    extracted_content["images"] = image_count
                    if image_count > 0:
                        logger.info(f"  🖼️ Extracted {image_count} images total")
                    else:
                        logger.info(f"  ℹ️ No images found in document")
                        
                except Exception as e:
                    logger.error(f"  ❌ Image extraction failed: {e}")
                    logger.error(f"  Traceback: {traceback.format_exc()}")
            
            # Create metadata
            metadata = {
                "source": pdf_info,
                "processing": {
                    "timestamp": datetime.now().isoformat(),
                    "engine": "docling",
                    "success": True,
                    "docling_version": "1.0.0"
                },
                "extracted_content": extracted_content,
                "output_paths": {
                    "base": str(base_dir),
                    "text": str(text_dir),
                    "tables": str(tables_dir),
                    "images": str(images_dir)
                }
            }
            
            # Save metadata
            metadata_file = base_dir / "metadata.json"
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2, default=str)
            
            logger.info(f"✅ {ticker} {period} processed successfully")
            
            return {
                "success": True,
                "ticker": ticker,
                "period": period,
                "extracted": extracted_content,
                "metadata": metadata
            }
            
        except Exception as e:
            error_msg = f"Failed to process {ticker} {period}: {str(e)}"
            logger.error(f"❌ {error_msg}")
            logger.error(f"Full traceback: {traceback.format_exc()}")
            
            return {
                "success": False,
                "ticker": ticker,
                "period": period,
                "error": error_msg
            }

def main():
    """Main function - run real Docling PDF processing."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Parse PDFs with real Docling")
    parser.add_argument("--input-dir", default="dow30_pipeline_reports_2025", help="PDF input directory")
    parser.add_argument("--output-dir", default="parsed_local", help="Output directory")
    parser.add_argument("--ticker", help="Process only specific ticker")
    parser.add_argument("--dry-run", action="store_true", help="Just discover files, don't process")
    
    args = parser.parse_args()
    
    print("🚀 Starting Real Docling PDF Parser")
    print("==================================")
    
    # Initialize parser
    try:
        docling_parser = DoclingPDFParser(args.input_dir, args.output_dir)
    except Exception as e:
        print(f"❌ Failed to initialize parser: {e}")
        return
    
    if args.dry_run:
        # Just show discovered files
        pdfs = docling_parser.discover_pdfs()
        print(f"\n📋 Discovered {len(pdfs)} PDF files:")
        for pdf in pdfs:
            print(f"  📄 {pdf['ticker']} {pdf['period']} - {pdf['filename']}")
        return
    
    # Process PDFs
    pdfs = docling_parser.discover_pdfs()
    
    if not pdfs:
        print("❌ No PDF files found to process")
        return
    
    if args.ticker:
        pdfs = [p for p in pdfs if p['ticker'].upper() == args.ticker.upper()]
        if not pdfs:
            print(f"❌ No PDFs found for ticker: {args.ticker}")
            return
    
    print(f"\n🎯 Processing {len(pdfs)} PDF files...")
    
    results = []
    successful = 0
    
    for i, pdf in enumerate(pdfs, 1):
        print(f"\n📄 [{i}/{len(pdfs)}] Processing {pdf['ticker']} {pdf['period']}")
        result = docling_parser.process_pdf_with_docling(pdf)
        results.append(result)
        
        if result["success"]:
            successful += 1
            extracted = result["extracted"]
            print(f"  ✅ Success! Text: {len(extracted['text_files'])}, Tables: {extracted['tables']}, Images: {extracted['images']}")
        else:
            print(f"  ❌ Failed: {result['error']}")
    
    print(f"\n🎉 Processing Complete!")
    print(f"�� Total PDFs: {len(pdfs)}")
    print(f"✅ Successful: {successful}")
    print(f"❌ Failed: {len(pdfs) - successful}")
    print(f"📈 Success Rate: {(successful/len(pdfs)*100):.1f}%")
    print(f"📁 Output: {args.output_dir}/")

if __name__ == "__main__":
    main()
