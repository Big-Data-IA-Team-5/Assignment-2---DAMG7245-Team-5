#!/usr/bin/env python3
"""
State management for the Lantern post-Docling pipeline.
Handles idempotency by tracking processed reports.
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


class StateManager:
    """Manages processing state for idempotency."""
    
    def __init__(self, gcs_manager):
        """
        Initialize state manager.
        
        Args:
            gcs_manager: GCSManager instance
        """
        self.gcs_manager = gcs_manager
        
    def get_ticker_state(self, ticker: str, state_path: str) -> Optional[Dict[str, Any]]:
        """
        Get current state for a ticker.
        
        Args:
            ticker: Stock ticker symbol
            state_path: GCS path to state file
            
        Returns:
            State dictionary or None if not found
        """
        try:
            state_data = self.gcs_manager.download_json(state_path)
            if state_data and state_data.get('ticker') == ticker:
                logger.debug(f"Found state for {ticker}: {state_data.get('sha256', '')[:8]}...")
                return state_data
            return None
        except Exception as e:
            logger.debug(f"No state found for {ticker}: {e}")
            return None
    
    def update_ticker_state(self,
                          ticker: str,
                          sha256: str,
                          published: str,
                          period: str,
                          state_path: str,
                          additional_data: Optional[Dict[str, Any]] = None) -> str:
        """
        Update state for a ticker.
        
        Args:
            ticker: Stock ticker symbol
            sha256: Report SHA256 hash
            published: Publication date
            period: Period in YYYY-Q# format
            state_path: GCS path to state file
            additional_data: Extra data to store
            
        Returns:
            GCS URI of state file
        """
        state_data = {
            'ticker': ticker,
            'sha256': sha256,
            'published': published,
            'period': period,
            'last_updated': datetime.utcnow().isoformat(),
            'processing_count': 1,
            **(additional_data or {})
        }
        
        # Try to get existing state to increment processing count
        existing_state = self.get_ticker_state(ticker, state_path)
        if existing_state:
            state_data['processing_count'] = existing_state.get('processing_count', 0) + 1
        
        gcs_uri = self.gcs_manager.upload_json(state_data, state_path)
        logger.info(f"Updated state for {ticker}: {sha256[:8]}...")
        return gcs_uri
    
    def check_already_processed(self,
                              ticker: str,
                              sha256: str,
                              state_path: str) -> bool:
        """
        Check if a report has already been processed.
        
        Args:
            ticker: Stock ticker symbol
            sha256: Report SHA256 hash
            state_path: GCS path to state file
            
        Returns:
            True if already processed, False otherwise
        """
        state = self.get_ticker_state(ticker, state_path)
        if not state:
            return False
            
        return state.get('sha256') == sha256
    
    def should_skip_processing(self,
                             ticker: str,
                             sha256: str,
                             published: str,
                             state_path: str) -> tuple[bool, str]:
        """
        Determine if processing should be skipped based on state.
        
        Args:
            ticker: Stock ticker symbol
            sha256: Report SHA256 hash
            published: Publication date
            state_path: GCS path to state file
            
        Returns:
            Tuple of (should_skip, reason)
        """
        state = self.get_ticker_state(ticker, state_path)
        
        if not state:
            return False, "No previous state found"
        
        existing_sha256 = state.get('sha256')
        existing_published = state.get('published')
        
        # Same SHA256 = definitely skip
        if existing_sha256 == sha256:
            return True, f"Same SHA256 already processed: {sha256[:8]}..."
        
        # Different SHA256 but older publication date = skip
        if existing_published and published < existing_published:
            return True, f"Older report (published {published} vs {existing_published})"
        
        return False, f"New report to process (SHA256: {sha256[:8]}...)"
    
    def get_processing_summary(self, state_paths: list[str]) -> Dict[str, Any]:
        """
        Get summary of processing state across multiple tickers.
        
        Args:
            state_paths: List of GCS paths to state files
            
        Returns:
            Summary statistics dictionary
        """
        summary = {
            'total_tickers': len(state_paths),
            'processed_tickers': 0,
            'latest_updates': [],
            'processing_counts': {}
        }
        
        for state_path in state_paths:
            try:
                # Extract ticker from path (assumes lantern/state/{TICKER}.json)
                ticker = Path(state_path).stem
                
                state = self.get_ticker_state(ticker, state_path)
                if state:
                    summary['processed_tickers'] += 1
                    
                    # Track latest updates
                    if 'last_updated' in state:
                        summary['latest_updates'].append({
                            'ticker': ticker,
                            'last_updated': state['last_updated'],
                            'period': state.get('period', 'unknown')
                        })
                    
                    # Track processing counts
                    count = state.get('processing_count', 0)
                    if count in summary['processing_counts']:
                        summary['processing_counts'][count] += 1
                    else:
                        summary['processing_counts'][count] = 1
                        
            except Exception as e:
                logger.warning(f"Failed to process state for {state_path}: {e}")
        
        # Sort latest updates by timestamp
        summary['latest_updates'].sort(
            key=lambda x: x.get('last_updated', ''), 
            reverse=True
        )
        
        return summary
    
    def cleanup_old_state(self, 
                         ticker: str,
                         state_path: str,
                         keep_versions: int = 1) -> bool:
        """
        Cleanup old state versions (placeholder for future implementation).
        
        Args:
            ticker: Stock ticker symbol
            state_path: GCS path to state file
            keep_versions: Number of versions to keep
            
        Returns:
            True if cleanup successful, False otherwise
        """
        # For now, just validate the current state exists
        state = self.get_ticker_state(ticker, state_path)
        if state:
            logger.debug(f"State exists for {ticker}, cleanup not needed")
            return True
        return False
    
    def backup_state(self,
                    ticker: str,
                    state_path: str,
                    backup_suffix: str = None) -> Optional[str]:
        """
        Create a backup of current state.
        
        Args:
            ticker: Stock ticker symbol
            state_path: GCS path to state file
            backup_suffix: Optional suffix for backup file
            
        Returns:
            GCS URI of backup file or None if failed
        """
        try:
            state = self.get_ticker_state(ticker, state_path)
            if not state:
                logger.warning(f"No state to backup for {ticker}")
                return None
            
            # Create backup path
            suffix = backup_suffix or datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            backup_path = state_path.replace('.json', f'_backup_{suffix}.json')
            
            # Upload backup
            gcs_uri = self.gcs_manager.upload_json(state, backup_path)
            logger.info(f"Backed up state for {ticker} to {gcs_uri}")
            return gcs_uri
            
        except Exception as e:
            logger.error(f"Failed to backup state for {ticker}: {e}")
            return None


def create_state_manager(gcs_manager) -> StateManager:
    """
    Factory function to create a StateManager instance.
    
    Args:
        gcs_manager: GCSManager instance
        
    Returns:
        Configured StateManager instance
    """
    return StateManager(gcs_manager)
