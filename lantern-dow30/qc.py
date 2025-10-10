#!/usr/bin/env python3
"""
Quality checks for the Lantern post-Docling pipeline.
Validates processing results and GCS artifacts.
"""

import logging
from typing import Dict, List, Any, Tuple
from pathlib import Path
import json

logger = logging.getLogger(__name__)


class QualityChecker:
    """Handles quality checks for processed reports."""
    
    def __init__(self, gcs_manager):
        """
        Initialize quality checker.
        
        Args:
            gcs_manager: GCSManager instance
        """
        self.gcs_manager = gcs_manager
        
    def check_docling_outputs(self,
                            text_file: Path,
                            table_files: List[Path],
                            image_files: List[Path]) -> Tuple[bool, List[str]]:
        """
        Check that Docling outputs meet minimum requirements.
        
        Args:
            text_file: Path to text output file
            table_files: List of table CSV files
            image_files: List of image files
            
        Returns:
            Tuple of (passed, error_messages)
        """
        errors = []
        
        # Check text file exists and has content
        if not text_file.exists():
            errors.append(f"Text file not found: {text_file}")
        else:
            try:
                with open(text_file, 'r', encoding='utf-8') as f:
                    content = f.read().strip()
                    if len(content) < 100:
                        errors.append(f"Text file too short: {len(content)} chars")
                    elif not any(keyword in content.lower() for keyword in 
                               ['revenue', 'earnings', 'quarter', 'financial']):
                        errors.append("Text file does not appear to contain earnings content")
            except Exception as e:
                errors.append(f"Failed to read text file: {e}")
        
        # Check that we have either meaningful text OR tables
        has_good_text = text_file.exists() and not any("text file" in err for err in errors)
        has_tables = len(table_files) > 0
        
        if not has_good_text and not has_tables:
            errors.append("Neither meaningful text nor tables found - extraction may have failed")
        
        # Validate table files if present
        for table_file in table_files:
            if not table_file.exists():
                errors.append(f"Table file not found: {table_file}")
            else:
                try:
                    with open(table_file, 'r', encoding='utf-8') as f:
                        content = f.read().strip()
                        if len(content) < 10:
                            errors.append(f"Table file too small: {table_file}")
                except Exception as e:
                    errors.append(f"Failed to read table file {table_file}: {e}")
        
        # Check images (optional but should exist if present)
        missing_images = [img for img in image_files if not img.exists()]
        if missing_images:
            # This is just a warning, not a failure
            logger.warning(f"Some image files missing: {len(missing_images)}")
        
        passed = len(errors) == 0
        return passed, errors
    
    def check_gcs_artifacts(self,
                          ticker: str,
                          period: str,
                          filename: str,
                          required_objects: List[str]) -> Tuple[bool, List[str]]:
        """
        Check that required GCS objects exist.
        
        Args:
            ticker: Stock ticker
            period: Period in YYYY-Q# format
            filename: Base filename
            required_objects: List of required object types
            
        Returns:
            Tuple of (passed, error_messages)
        """
        from .gcs_utils import build_gcs_paths
        
        paths = build_gcs_paths(ticker, period, filename.replace('.pdf', ''))
        errors = []
        
        for obj_type in required_objects:
            if obj_type not in paths:
                errors.append(f"Unknown object type: {obj_type}")
                continue
                
            gcs_path = paths[obj_type]
            if not self.gcs_manager.object_exists(gcs_path):
                errors.append(f"Missing GCS object: {gcs_path}")
        
        passed = len(errors) == 0
        return passed, errors
    
    def check_structured_json(self,
                            structured_data: Dict[str, Any],
                            ticker: str,
                            period: str) -> Tuple[bool, List[str]]:
        """
        Validate structured JSON extraction results.
        
        Args:
            structured_data: Extracted structured data
            ticker: Expected ticker
            period: Expected period
            
        Returns:
            Tuple of (passed, error_messages)
        """
        errors = []
        
        # Check required fields
        required_fields = ['ticker', 'period', 'published', 'report_type', 'confidence']
        for field in required_fields:
            if field not in structured_data:
                errors.append(f"Missing required field: {field}")
        
        # Check ticker matches
        if structured_data.get('ticker') != ticker:
            errors.append(f"Ticker mismatch: expected {ticker}, got {structured_data.get('ticker')}")
        
        # Check period matches
        if structured_data.get('period') != period:
            errors.append(f"Period mismatch: expected {period}, got {structured_data.get('period')}")
        
        # Check confidence is reasonable
        confidence = structured_data.get('confidence', 0)
        if not isinstance(confidence, (int, float)) or not 0 <= confidence <= 1:
            errors.append(f"Invalid confidence value: {confidence}")
        elif confidence < 0.1:
            errors.append(f"Very low confidence: {confidence}")
        
        # Check revenue structure if present
        revenue = structured_data.get('revenue')
        if revenue:
            if not isinstance(revenue, dict):
                errors.append("Revenue should be a dictionary")
            else:
                if 'value' in revenue and revenue['value'] is not None:
                    if not isinstance(revenue['value'], (int, float)):
                        errors.append("Revenue value should be numeric")
                    elif revenue['value'] < 0:
                        errors.append("Revenue value is negative")
                
                if revenue.get('unit') != 'USD':
                    errors.append(f"Unexpected revenue unit: {revenue.get('unit')}")
                
                scale = revenue.get('scale')
                if scale and scale not in ['B', 'M', 'K', 'null']:
                    errors.append(f"Invalid revenue scale: {scale}")
        
        # Check EPS if present
        eps = structured_data.get('eps_diluted')
        if eps is not None and not isinstance(eps, (int, float)):
            errors.append("EPS should be numeric or null")
        
        # Check key highlights format
        highlights = structured_data.get('key_highlights', [])
        if not isinstance(highlights, list):
            errors.append("Key highlights should be a list")
        elif len(highlights) > 5:
            errors.append(f"Too many key highlights: {len(highlights)} (max 5)")
        
        passed = len(errors) == 0
        return passed, errors
    
    def check_manifest_entry(self,
                           manifest_entry: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate a manifest entry.
        
        Args:
            manifest_entry: Manifest entry dictionary
            
        Returns:
            Tuple of (passed, error_messages)
        """
        errors = []
        
        # Check required fields
        required_fields = [
            'ticker', 'period', 'published', 'report_type',
            'source_url', 'sha256', 'filename'
        ]
        
        for field in required_fields:
            if field not in manifest_entry or not manifest_entry[field]:
                errors.append(f"Missing or empty required field: {field}")
        
        # Validate SHA256 format
        sha256 = manifest_entry.get('sha256', '')
        if len(sha256) != 64:
            errors.append(f"Invalid SHA256 length: {len(sha256)}")
        
        # Check ticker format
        ticker = manifest_entry.get('ticker', '')
        if not ticker.isupper() or len(ticker) > 5:
            errors.append(f"Invalid ticker format: {ticker}")
        
        # Check GCS paths exist
        gcs_pdf = manifest_entry.get('gcs_pdf', '')
        if not gcs_pdf.startswith('gs://'):
            errors.append(f"Invalid GCS PDF path: {gcs_pdf}")
        
        # Check structured data info
        structured = manifest_entry.get('structured', {})
        if structured:
            confidence = structured.get('confidence')
            if confidence is not None and not 0 <= confidence <= 1:
                errors.append(f"Invalid structured confidence: {confidence}")
        
        passed = len(errors) == 0
        return passed, errors
    
    def run_full_quality_check(self,
                             ticker: str,
                             period: str,
                             filename: str,
                             docling_outputs: Dict[str, Any],
                             structured_data: Dict[str, Any],
                             manifest_entry: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run comprehensive quality check on all artifacts.
        
        Args:
            ticker: Stock ticker
            period: Period in YYYY-Q# format
            filename: Report filename
            docling_outputs: Dictionary with paths to Docling outputs
            structured_data: Extracted structured data
            manifest_entry: Manifest entry dictionary
            
        Returns:
            Quality check results dictionary
        """
        results = {
            'ticker': ticker,
            'period': period,
            'filename': filename,
            'checks': {},
            'overall_passed': True,
            'error_count': 0,
            'warning_count': 0
        }
        
        # Check Docling outputs
        try:
            text_file = Path(docling_outputs.get('text_file', ''))
            table_files = [Path(f) for f in docling_outputs.get('table_files', [])]
            image_files = [Path(f) for f in docling_outputs.get('image_files', [])]
            
            passed, errors = self.check_docling_outputs(text_file, table_files, image_files)
            results['checks']['docling_outputs'] = {
                'passed': passed,
                'errors': errors
            }
            
            if not passed:
                results['overall_passed'] = False
                results['error_count'] += len(errors)
                
        except Exception as e:
            results['checks']['docling_outputs'] = {
                'passed': False,
                'errors': [f"Failed to check Docling outputs: {e}"]
            }
            results['overall_passed'] = False
            results['error_count'] += 1
        
        # Check GCS artifacts
        try:
            required_objects = ['raw_pdf', 'structured_json']
            passed, errors = self.check_gcs_artifacts(ticker, period, filename, required_objects)
            results['checks']['gcs_artifacts'] = {
                'passed': passed,
                'errors': errors
            }
            
            if not passed:
                results['overall_passed'] = False
                results['error_count'] += len(errors)
                
        except Exception as e:
            results['checks']['gcs_artifacts'] = {
                'passed': False,
                'errors': [f"Failed to check GCS artifacts: {e}"]
            }
            results['overall_passed'] = False
            results['error_count'] += 1
        
        # Check structured JSON
        try:
            passed, errors = self.check_structured_json(structured_data, ticker, period)
            results['checks']['structured_json'] = {
                'passed': passed,
                'errors': errors
            }
            
            if not passed:
                results['overall_passed'] = False
                results['error_count'] += len(errors)
                
        except Exception as e:
            results['checks']['structured_json'] = {
                'passed': False,
                'errors': [f"Failed to check structured JSON: {e}"]
            }
            results['overall_passed'] = False
            results['error_count'] += 1
        
        # Check manifest entry
        try:
            passed, errors = self.check_manifest_entry(manifest_entry)
            results['checks']['manifest_entry'] = {
                'passed': passed,
                'errors': errors
            }
            
            if not passed:
                results['overall_passed'] = False
                results['error_count'] += len(errors)
                
        except Exception as e:
            results['checks']['manifest_entry'] = {
                'passed': False,
                'errors': [f"Failed to check manifest entry: {e}"]
            }
            results['overall_passed'] = False
            results['error_count'] += 1
        
        logger.info(f"Quality check for {ticker} {period}: "
                   f"{'PASSED' if results['overall_passed'] else 'FAILED'} "
                   f"({results['error_count']} errors)")
        
        return results


def create_quality_checker(gcs_manager) -> QualityChecker:
    """
    Factory function to create a QualityChecker instance.
    
    Args:
        gcs_manager: GCSManager instance
        
    Returns:
        Configured QualityChecker instance
    """
    return QualityChecker(gcs_manager)
