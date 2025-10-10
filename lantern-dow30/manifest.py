#!/usr/bin/env python3
"""
Manifest management for the Lantern post-Docling pipeline.
Handles creation and validation of JSONL manifest entries.
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List
from pathlib import Path

logger = logging.getLogger(__name__)


class ManifestEntry:
    """Represents a single manifest entry for an earnings report."""
    
    def __init__(self,
                 ticker: str,
                 period: str,
                 published: str,
                 report_type: str,
                 source_url: str,
                 sha256: str,
                 filename: str):
        """
        Initialize manifest entry.
        
        Args:
            ticker: Stock ticker symbol
            period: Period in YYYY-Q# format
            published: Publication date (YYYY-MM-DD)
            report_type: Type of report (press|presentation|supplemental)
            source_url: Original source URL
            sha256: SHA256 hash of PDF
            filename: PDF filename
        """
        self.ticker = ticker
        self.period = period
        self.published = published
        self.report_type = report_type
        self.source_url = source_url
        self.sha256 = sha256
        self.filename = filename
        self.pages = 0
        self.tables = 0
        self.gcs_paths = {}
        self.structured_data = {}
        self.parser_info = {}
        self.notes = []
        
    def set_gcs_paths(self, gcs_bucket: str, **paths):
        """Set GCS paths for various assets."""
        base_raw = f"gs://{gcs_bucket}/lantern/raw/{self.ticker}/{self.period}"
        base_parsed = f"gs://{gcs_bucket}/lantern/parsed/{self.ticker}/{self.period}"
        
        self.gcs_paths.update({
            'gcs_pdf': f"{base_raw}/{self.filename}",
            'gcs_text_dir': f"{base_parsed}/text",
            'gcs_tables_dir': f"{base_parsed}/tables",
            'gcs_images_dir': f"{base_parsed}/images",
            'gcs_structured': f"{base_parsed}/structured.json",
            **paths
        })
        
    def set_docling_info(self, pages: int = 0, tables: int = 0, version: str = "1.0.0"):
        """Set Docling parser information."""
        self.pages = pages
        self.tables = tables
        self.parser_info = {
            'engine': 'docling',
            'version': version
        }
        
    def set_structured_info(self, 
                          model: str = "gemini-1.5-pro",
                          confidence: float = 0.0,
                          gcs_path: str = ""):
        """Set structured extraction information."""
        self.structured_data = {
            'engine': 'guidance+vertex',
            'model': model,
            'path': gcs_path,
            'confidence': confidence
        }
        
    def add_note(self, note: str):
        """Add a note to the manifest entry."""
        self.notes.append(note)
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert manifest entry to dictionary."""
        return {
            'ticker': self.ticker,
            'period': self.period,
            'published': self.published,
            'report_type': self.report_type,
            'source_url': self.source_url,
            'gcs_pdf': self.gcs_paths.get('gcs_pdf', ''),
            'sha256': self.sha256,
            'filename': self.filename,
            'pages': self.pages,
            'tables': self.tables,
            'parser': self.parser_info,
            'structured': self.structured_data,
            'notes': self.notes
        }
        
    def to_jsonl(self) -> str:
        """Convert manifest entry to JSONL format."""
        return json.dumps(self.to_dict(), ensure_ascii=False)


class ManifestManager:
    """Manages manifest creation and validation."""
    
    def __init__(self, gcs_manager, bucket_name: str):
        """
        Initialize manifest manager.
        
        Args:
            gcs_manager: GCSManager instance
            bucket_name: GCS bucket name
        """
        self.gcs_manager = gcs_manager
        self.bucket_name = bucket_name
        
    def create_entry(self,
                    ticker: str,
                    period: str,
                    published: str,
                    report_type: str,
                    source_url: str,
                    sha256: str,
                    filename: str) -> ManifestEntry:
        """Create a new manifest entry."""
        entry = ManifestEntry(
            ticker=ticker,
            period=period,
            published=published,
            report_type=report_type,
            source_url=source_url,
            sha256=sha256,
            filename=filename
        )
        
        # Set GCS paths
        entry.set_gcs_paths(self.bucket_name)
        
        return entry
        
    def write_manifest_entry(self,
                           entry: ManifestEntry,
                           manifest_path: str) -> str:
        """
        Write manifest entry to GCS JSONL file.
        
        Args:
            entry: ManifestEntry to write
            manifest_path: GCS path to manifest.jsonl
            
        Returns:
            GCS URI of manifest file
        """
        try:
            record = entry.to_dict()
            
            # Add timestamp
            record['_written_at'] = datetime.utcnow().isoformat()
            
            # Validate required fields
            self._validate_manifest_entry(record)
            
            # Append to manifest
            gcs_uri = self.gcs_manager.append_to_manifest(manifest_path, record)
            
            logger.info(f"Wrote manifest entry for {entry.ticker} {entry.period}")
            return gcs_uri
            
        except Exception as e:
            logger.error(f"Failed to write manifest entry: {e}")
            raise
            
    def _validate_manifest_entry(self, record: Dict[str, Any]):
        """Validate manifest entry has required fields."""
        required_fields = [
            'ticker', 'period', 'published', 'report_type',
            'source_url', 'sha256', 'filename'
        ]
        
        for field in required_fields:
            if not record.get(field):
                raise ValueError(f"Missing required manifest field: {field}")
                
        # Validate format constraints
        if not record['ticker'].isupper():
            raise ValueError(f"Ticker must be uppercase: {record['ticker']}")
            
        if len(record['sha256']) != 64:
            raise ValueError(f"Invalid SHA256 length: {len(record['sha256'])}")
    
    def load_manifest_entries(self, manifest_path: str) -> List[Dict[str, Any]]:
        """
        Load all entries from a JSONL manifest file.
        
        Args:
            manifest_path: GCS path to manifest.jsonl
            
        Returns:
            List of manifest entry dictionaries
        """
        try:
            # Download manifest content
            blob = self.gcs_manager.bucket.blob(manifest_path)
            content = blob.download_as_text()
            
            entries = []
            for line_num, line in enumerate(content.strip().split('\n'), 1):
                if line.strip():
                    try:
                        entry = json.loads(line)
                        entries.append(entry)
                    except json.JSONDecodeError as e:
                        logger.warning(f"Invalid JSON on line {line_num}: {e}")
                        
            logger.info(f"Loaded {len(entries)} entries from manifest")
            return entries
            
        except Exception as e:
            logger.error(f"Failed to load manifest entries: {e}")
            return []
    
    def get_latest_entry(self, ticker: str, manifest_path: str) -> Optional[Dict[str, Any]]:
        """
        Get the latest manifest entry for a ticker.
        
        Args:
            ticker: Stock ticker symbol
            manifest_path: GCS path to manifest.jsonl
            
        Returns:
            Latest entry dictionary or None
        """
        entries = self.load_manifest_entries(manifest_path)
        
        # Filter by ticker and sort by published date
        ticker_entries = [e for e in entries if e.get('ticker') == ticker]
        if not ticker_entries:
            return None
            
        # Sort by published date (newest first)
        ticker_entries.sort(key=lambda x: x.get('published', ''), reverse=True)
        return ticker_entries[0]
    
    def check_duplicate(self, 
                       ticker: str,
                       sha256: str,
                       manifest_path: str) -> bool:
        """
        Check if a report with this ticker/sha256 already exists in manifest.
        
        Args:
            ticker: Stock ticker symbol
            sha256: Report SHA256 hash
            manifest_path: GCS path to manifest.jsonl
            
        Returns:
            True if duplicate exists, False otherwise
        """
        entries = self.load_manifest_entries(manifest_path)
        
        for entry in entries:
            if (entry.get('ticker') == ticker and 
                entry.get('sha256') == sha256):
                logger.info(f"Duplicate found for {ticker} with SHA256 {sha256[:8]}...")
                return True
                
        return False
    
    def generate_summary_stats(self, manifest_path: str) -> Dict[str, Any]:
        """
        Generate summary statistics from manifest.
        
        Args:
            manifest_path: GCS path to manifest.jsonl
            
        Returns:
            Dictionary with summary statistics
        """
        entries = self.load_manifest_entries(manifest_path)
        
        if not entries:
            return {'total_reports': 0}
        
        # Basic counts
        total_reports = len(entries)
        unique_tickers = len(set(e.get('ticker') for e in entries))
        
        # By report type
        report_types = {}
        for entry in entries:
            rt = entry.get('report_type', 'unknown')
            report_types[rt] = report_types.get(rt, 0) + 1
        
        # By period
        periods = {}
        for entry in entries:
            period = entry.get('period', 'unknown')
            periods[period] = periods.get(period, 0) + 1
        
        # Confidence distribution (from structured data)
        confidences = []
        for entry in entries:
            structured = entry.get('structured', {})
            if 'confidence' in structured:
                confidences.append(structured['confidence'])
        
        avg_confidence = sum(confidences) / len(confidences) if confidences else 0
        
        return {
            'total_reports': total_reports,
            'unique_tickers': unique_tickers,
            'report_types': report_types,
            'periods': periods,
            'avg_confidence': round(avg_confidence, 3),
            'confidence_samples': len(confidences)
        }


def create_manifest_manager(gcs_manager, bucket_name: str) -> ManifestManager:
    """
    Factory function to create a ManifestManager instance.
    
    Args:
        gcs_manager: GCSManager instance
        bucket_name: GCS bucket name
        
    Returns:
        Configured ManifestManager instance
    """
    return ManifestManager(gcs_manager, bucket_name)
