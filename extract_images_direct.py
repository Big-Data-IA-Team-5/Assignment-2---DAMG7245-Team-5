#!/usr/bin/env python3
"""
Direct Image Extraction using PyMuPDF
--------------------------------------
Lightweight image extraction that doesn't require Docling's full parsing.
Perfect for extracting images without memory-intensive text processing.

Usage:
    python extract_images_direct.py
"""

import sys
from pathlib import Path
import logging
import hashlib

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
log = logging.getLogger("image_extractor")


def get_image_hash(image_bytes: bytes) -> str:
    """Calculate MD5 hash of image bytes for duplicate detection"""
    return hashlib.md5(image_bytes).hexdigest()


def extract_images_from_pdf(pdf_path: Path, output_dir: Path) -> dict:
    """
    Extract only embedded images and charts from PDF (no full page renders)
    Automatically removes duplicates by keeping only one copy of each unique image.
    
    Args:
        pdf_path: Path to PDF file
        output_dir: Directory to save extracted images
        
    Returns:
        Dictionary with extraction statistics
    """
    try:
        import fitz  # PyMuPDF
        from PIL import Image
        import io
        
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Extract ticker from filename
        ticker = pdf_path.stem.split('_')[0] if '_' in pdf_path.stem else pdf_path.stem
        
        log.info(f"Processing: {pdf_path.name}")
        
        doc = fitz.open(str(pdf_path))
        
        # Track unique images by hash to avoid duplicates
        seen_hashes = set()
        images_extracted = 0
        duplicates_skipped = 0
        
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
                    img_hash = get_image_hash(image_bytes)
                    
                    # Skip if we've already saved this exact image
                    if img_hash in seen_hashes:
                        duplicates_skipped += 1
                        log.debug(f"  ⊘ Duplicate skipped: page {page_num+1}, img {img_index+1}")
                        continue
                    
                    # Mark this hash as seen
                    seen_hashes.add(img_hash)
                    
                    # Save the unique image
                    image_file = output_dir / f"{ticker}_page{page_num+1}_img{img_index+1}.{image_ext}"
                    
                    with open(image_file, 'wb') as img_file:
                        img_file.write(image_bytes)
                    
                    images_extracted += 1
                    log.info(f"  ✓ Extracted: {image_file.name}")
                    
                except Exception as e:
                    log.warning(f"  Failed to extract image from page {page_num+1}, img {img_index+1}: {e}")
        
        doc.close()
        
        log.info(f"✓ Unique images extracted: {images_extracted}")
        if duplicates_skipped > 0:
            log.info(f"  ⊘ Duplicates skipped: {duplicates_skipped}")
        
        return {
            'extracted': images_extracted,
            'duplicates': duplicates_skipped,
            'total': images_extracted + duplicates_skipped
        }
        
    except ImportError:
        log.error("PyMuPDF not installed. Install with: pip install PyMuPDF")
        return 0
    except Exception as e:
        log.error(f"Error processing {pdf_path.name}: {e}")
        return 0


def extract_all_images():
    """Extract images from all PDFs in the directory"""
    
    print("="*70)
    print("Direct Image Extraction (PyMuPDF)")
    print("="*70)
    print()
    
    # Setup directories
    raw_dir = Path("dow30_pipeline_reports_2025")
    images_dir = Path("parsed_reports/images")
    
    if not raw_dir.exists():
        print(f"❌ Directory not found: {raw_dir}")
        print("   Please run the main pipeline first to download PDFs.")
        return 1
    
    pdf_files = list(raw_dir.glob("*.pdf"))
    if not pdf_files:
        print(f"❌ No PDF files found in: {raw_dir}")
        return 1
    
    print(f"✓ Found {len(pdf_files)} PDF files")
    print(f"✓ Output directory: {images_dir}")
    print()
    
    # Process each PDF
    total_images = 0
    total_duplicates = 0
    successful_pdfs = 0
    
    for pdf_path in pdf_files:
        try:
            result = extract_images_from_pdf(pdf_path, images_dir)
            if result['extracted'] > 0:
                total_images += result['extracted']
                total_duplicates += result['duplicates']
                successful_pdfs += 1
            print()
        except Exception as e:
            log.error(f"Error processing {pdf_path.name}: {e}")
            print()
    
    # Summary
    print("="*70)
    print("Extraction Summary")
    print("="*70)
    print(f"PDFs processed: {len(pdf_files)}")
    print(f"PDFs with images: {successful_pdfs}")
    print(f"Unique images extracted: {total_images}")
    print(f"Duplicate images skipped: {total_duplicates}")
    print(f"Total images found: {total_images + total_duplicates}")
    print()
    
    if total_images > 0:
        print("✅ Image extraction successful!")
        print(f"📁 Images saved to: {images_dir}")
        print()
        print("Image types extracted:")
        print("  • Charts and graphs")
        print("  • Company logos")
        print("  • Photos and illustrations")
        print("  • Diagrams")
        print()
        if total_duplicates > 0:
            print(f"💡 Space saved by removing {total_duplicates} duplicate images")
        print()
        
        # Show some examples
        image_files = list(images_dir.glob("*.png")) + list(images_dir.glob("*.jpg"))
        if image_files:
            print(f"Sample files ({min(5, len(image_files))} of {len(image_files)}):")
            for img_file in sorted(image_files)[:5]:
                file_size = img_file.stat().st_size / 1024  # KB
                print(f"  • {img_file.name} ({file_size:.1f} KB)")
        
        return 0
    else:
        print("⚠️  No images were extracted")
        print("   PDFs might be text-only or contain only vector graphics.")
        return 0


def extract_single_pdf(pdf_name: str):
    """Extract images from a single PDF"""
    
    raw_dir = Path("dow30_pipeline_reports_2025")
    images_dir = Path("parsed_reports/images")
    
    pdf_path = raw_dir / pdf_name
    
    if not pdf_path.exists():
        print(f"❌ File not found: {pdf_path}")
        return 1
    
    print(f"Extracting images from: {pdf_name}")
    print("-" * 70)
    
    result = extract_images_from_pdf(pdf_path, images_dir)
    
    print()
    if result['extracted'] > 0:
        print(f"✅ Extracted {result['extracted']} unique images")
        if result['duplicates'] > 0:
            print(f"⊘ Skipped {result['duplicates']} duplicates")
        print(f"📁 Saved to: {images_dir}")
    else:
        print("⚠️  No images found in this PDF")
    
    return 0


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Extract images from PDFs using PyMuPDF')
    parser.add_argument('--file', '-f', help='Extract from specific PDF file')
    parser.add_argument('--all', '-a', action='store_true', 
                       help='Extract from all PDFs (default)')
    
    args = parser.parse_args()
    
    if args.file:
        sys.exit(extract_single_pdf(args.file))
    else:
        sys.exit(extract_all_images())
