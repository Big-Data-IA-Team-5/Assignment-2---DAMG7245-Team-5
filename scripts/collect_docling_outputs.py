#!/usr/bin/env python3
"""
Collect Docling outputs script for the Lantern post-Docling pipeline.
Discovers local Docling artifacts and yields structured data for processing.
"""

import os
import sys
import json
import logging
from pathlib import Path
from typing import Iterator, Dict, Any, List, Optional
import hashlib
import argparse

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from lantern-dow30.periods import infer_period

logger = logging.getLogger(__name__)

class DoclingOutputCollector:
    """Collects and structures Docling output data for pipeline processing."""
    
    def __init__(self, base_directory: Path):
        """
        Initialize the collector.
        
        Args:
            base_directory: Base directory containing Docling outputs
        """
        self.base_directory = Path(base_directory)
        self.logger = logging.getLogger(__name__)
        
    def find_docling_outputs(self) -> Iterator[Dict[str, Any]]:
        """
        Find and yield Docling output sets.
        
        Expected structure:
        base_directory/
          {TICKER}/
            {PERIOD}/
              raw_pdf/
                {filename}.pdf
              docling_output/
                text/
                  {filename}.txt
                tables/
                  {filename}-t{n}.csv
                images/
                  {filename}-p{page}-{k}.png
                  
        Yields:
            Dictionary with structured Docling output information
        """
        if not self.base_directory.exists():
            self.logger.error(f"Base directory does not exist: {self.base_directory}")
            return
            
        self.logger.info(f"Scanning for Docling outputs in {self.base_directory}")
        
        # Look for ticker directories
        for ticker_dir in self.base_directory.iterdir():
            if not ticker_dir.is_dir() or ticker_dir.name.startswith('.'):
                continue
                
            ticker = ticker_dir.name.upper()
            self.logger.debug(f"Processing ticker: {ticker}")
            
            # Look for period directories
            for period_dir in ticker_dir.iterdir():
                if not period_dir.is_dir() or period_dir.name.startswith('.'):
                    continue
                    
                # Find PDF and Docling outputs
                for output_set in self._find_output_sets_in_period(ticker, period_dir):
                    yield output_set
    
    def _find_output_sets_in_period(self, ticker: str, period_dir: Path) -> Iterator[Dict[str, Any]]:
        """Find complete Docling output sets within a period directory."""
        period = period_dir.name
        self.logger.debug(f"Processing period: {ticker}/{period}")
        
        # Look for raw PDFs
        raw_pdf_dir = period_dir / "raw_pdf"
        docling_output_dir = period_dir / "docling_output"
        
        if not raw_pdf_dir.exists():
            self.logger.warning(f"No raw_pdf directory found in {period_dir}")
            return
            
        if not docling_output_dir.exists():
            self.logger.warning(f"No docling_output directory found in {period_dir}")
            return
        
        # Find PDF files
        pdf_files = list(raw_pdf_dir.glob("*.pdf"))
        if not pdf_files:
            self.logger.warning(f"No PDF files found in {raw_pdf_dir}")
            return
            
        for pdf_file in pdf_files:
            output_set = self._build_output_set(ticker, period, pdf_file, docling_output_dir)
            if output_set:
                yield output_set
    
    def _build_output_set(self, ticker: str, period: str, pdf_file: Path, docling_dir: Path) -> Optional[Dict[str, Any]]:
        """Build a complete output set for a PDF file."""
        filename_base = pdf_file.stem  # Remove .pdf extension
        
        # Find corresponding Docling outputs
        text_dir = docling_dir / "text"
        tables_dir = docling_dir / "tables"
        images_dir = docling_dir / "images"
        
        # Look for text file
        text_file = text_dir / f"{filename_base}.txt"
        if not text_file.exists():
            self.logger.warning(f"Text file not found: {text_file}")
            return None
        
        # Find table files
        table_files = []
        if tables_dir.exists():
            table_pattern = f"{filename_base}-t*.csv"
            table_files = list(tables_dir.glob(table_pattern))
            table_files.sort()  # Sort by table number
        
        # Find image files
        image_files = []
        if images_dir.exists():
            image_pattern = f"{filename_base}-p*-*.png"
            image_files = list(images_dir.glob(image_pattern))
            image_files.sort()  # Sort by page and index
        
        # Calculate SHA256 of PDF
        sha256 = self._calculate_sha256(pdf_file)
        
        # Try to extract metadata from text file or filename
        metadata = self._extract_metadata(text_file, pdf_file.name)
        
        # Infer period if not already in correct format
        try:
            normalized_period = infer_period(
                title=metadata.get('title', ''),
                published_iso=metadata.get('published', ''),
                report_type=metadata.get('report_type', 'press')
            )
        except Exception as e:
            self.logger.warning(f"Could not infer period for {pdf_file.name}: {e}")
            normalized_period = period
        
        output_set = {
            'ticker': ticker,
            'period': normalized_period,
            'source_url': metadata.get('source_url', ''),
            'published_iso': metadata.get('published', ''),
            'report_type': metadata.get('report_type', 'press'),
            'sha256': sha256,
            'raw_pdf_path': str(pdf_file),
            'docling_text_path': str(text_file),
            'docling_tables_dir': str(tables_dir) if tables_dir.exists() else '',
            'docling_images_dir': str(images_dir) if images_dir.exists() else '',
            'table_files': [str(f) for f in table_files],
            'image_files': [str(f) for f in image_files],
            'filename': pdf_file.name,
            'pages': len(image_files) if image_files else 0,
            'tables': len(table_files)
        }
        
        self.logger.info(f"Found complete output set: {ticker}/{normalized_period}/{pdf_file.name} "
                        f"({len(table_files)} tables, {len(image_files)} images)")
        
        return output_set
    
    def _calculate_sha256(self, file_path: Path) -> str:
        """Calculate SHA256 hash of a file."""
        sha256_hash = hashlib.sha256()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(chunk)
            return sha256_hash.hexdigest()
        except Exception as e:
            self.logger.error(f"Failed to calculate SHA256 for {file_path}: {e}")
            return ""
    
    def _extract_metadata(self, text_file: Path, filename: str) -> Dict[str, str]:
        """Extract metadata from text file or filename."""
        metadata = {
            'title': '',
            'published': '',
            'source_url': '',
            'report_type': 'press'
        }
        
        # Try to read first few lines of text file for metadata
        try:
            with open(text_file, 'r', encoding='utf-8') as f:
                first_lines = []
                for i, line in enumerate(f):
                    if i >= 20:  # Only read first 20 lines
                        break
                    first_lines.append(line.strip())
                
                text_content = '\n'.join(first_lines).lower()
                
                # Infer report type from filename or content
                if any(word in filename.lower() for word in ['presentation', 'slides', 'deck']):
                    metadata['report_type'] = 'presentation'
                elif any(word in filename.lower() for word in ['supplemental', 'supplement']):
                    metadata['report_type'] = 'supplemental'
                elif any(word in text_content for word in ['earnings call', 'conference call']):
                    metadata['report_type'] = 'presentation'
                
                # Use filename as title if no better title found
                metadata['title'] = filename.replace('.pdf', '').replace('_', ' ')
                
        except Exception as e:
            self.logger.warning(f"Could not read metadata from {text_file}: {e}")
        
        return metadata
    
    def get_summary_stats(self) -> Dict[str, Any]:
        """Get summary statistics of found Docling outputs."""
        stats = {
            'total_reports': 0,
            'tickers': set(),
            'periods': set(),
            'report_types': {},
            'total_tables': 0,
            'total_images': 0,
            'total_size_mb': 0
        }
        
        for output_set in self.find_docling_outputs():
            stats['total_reports'] += 1
            stats['tickers'].add(output_set['ticker'])
            stats['periods'].add(output_set['period'])
            
            report_type = output_set['report_type']
            stats['report_types'][report_type] = stats['report_types'].get(report_type, 0) + 1
            
            stats['total_tables'] += output_set['tables']
            stats['total_images'] += output_set['pages']
            
            # Calculate file size
            try:
                pdf_path = Path(output_set['raw_pdf_path'])
                if pdf_path.exists():
                    size_mb = pdf_path.stat().st_size / (1024 * 1024)
                    stats['total_size_mb'] += size_mb
            except Exception:
                pass
        
        # Convert sets to counts
        stats['unique_tickers'] = len(stats['tickers'])
        stats['unique_periods'] = len(stats['periods'])
        stats['tickers'] = sorted(list(stats['tickers']))
        stats['periods'] = sorted(list(stats['periods']))
        stats['total_size_mb'] = round(stats['total_size_mb'], 2)
        
        return stats


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description="Collect Docling outputs for processing")
    parser.add_argument("base_directory", help="Base directory containing Docling outputs")
    parser.add_argument("--output", "-o", help="Output JSON file (default: stdout)")
    parser.add_argument("--summary", "-s", action="store_true", help="Show summary statistics only")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    collector = DoclingOutputCollector(args.base_directory)
    
    if args.summary:
        # Show summary statistics only
        stats = collector.get_summary_stats()
        print(json.dumps(stats, indent=2))
        return
    
    # Collect all output sets
    output_sets = list(collector.find_docling_outputs())
    
    if args.output:
        # Write to file
        with open(args.output, 'w') as f:
            json.dump(output_sets, f, indent=2)
        print(f"Wrote {len(output_sets)} output sets to {args.output}")
    else:
        # Write to stdout
        print(json.dumps(output_sets, indent=2))
    
    logger.info(f"Found {len(output_sets)} complete Docling output sets")


if __name__ == "__main__":
    main()
