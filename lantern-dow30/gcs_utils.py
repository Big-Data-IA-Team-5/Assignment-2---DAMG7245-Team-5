#!/usr/bin/env python3
"""
GCS utilities for the Lantern post-Docling pipeline.
Handles atomic uploads, manifest appending, and bucket operations.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from datetime import datetime

from google.cloud import storage
from google.cloud.exceptions import NotFound, GoogleCloudError


logger = logging.getLogger(__name__)


class GCSManager:
    """Manages GCS operations for the Lantern pipeline."""
    
    def __init__(self, bucket_name: str, project_id: Optional[str] = None):
        """Initialize GCS client and bucket."""
        self.client = storage.Client(project=project_id)
        self.bucket_name = bucket_name
        self.bucket = self.client.bucket(bucket_name)
        
    def upload_file(self, 
                   local_path: Union[str, Path], 
                   gcs_path: str,
                   content_type: Optional[str] = None,
                   metadata: Optional[Dict[str, str]] = None) -> str:
        """
        Upload a file to GCS with atomic operation.
        
        Args:
            local_path: Local file path
            gcs_path: GCS object path (without gs://bucket/)
            content_type: MIME type for the object
            metadata: Custom metadata to attach
            
        Returns:
            Full GCS URI (gs://bucket/path)
        """
        try:
            blob = self.bucket.blob(gcs_path)
            
            # Set metadata if provided
            if metadata:
                blob.metadata = metadata
                
            # Set content type if provided
            if content_type:
                blob.content_type = content_type
            
            # Upload with atomic operation
            with open(local_path, 'rb') as f:
                blob.upload_from_file(f)
                
            gcs_uri = f"gs://{self.bucket_name}/{gcs_path}"
            logger.info(f"Uploaded {local_path} to {gcs_uri}")
            return gcs_uri
            
        except Exception as e:
            logger.error(f"Failed to upload {local_path} to {gcs_path}: {e}")
            raise
    
    def upload_text(self, 
                   content: str, 
                   gcs_path: str,
                   content_type: str = "text/plain",
                   metadata: Optional[Dict[str, str]] = None) -> str:
        """
        Upload text content directly to GCS.
        
        Args:
            content: Text content to upload
            gcs_path: GCS object path
            content_type: MIME type
            metadata: Custom metadata
            
        Returns:
            Full GCS URI
        """
        try:
            blob = self.bucket.blob(gcs_path)
            
            if metadata:
                blob.metadata = metadata
                
            blob.content_type = content_type
            blob.upload_from_string(content)
            
            gcs_uri = f"gs://{self.bucket_name}/{gcs_path}"
            logger.info(f"Uploaded text content to {gcs_uri}")
            return gcs_uri
            
        except Exception as e:
            logger.error(f"Failed to upload text to {gcs_path}: {e}")
            raise
    
    def upload_json(self, 
                   data: Dict[str, Any], 
                   gcs_path: str,
                   metadata: Optional[Dict[str, str]] = None) -> str:
        """
        Upload JSON data to GCS.
        
        Args:
            data: Dictionary to serialize as JSON
            gcs_path: GCS object path
            metadata: Custom metadata
            
        Returns:
            Full GCS URI
        """
        json_content = json.dumps(data, indent=2, ensure_ascii=False)
        return self.upload_text(
            content=json_content, 
            gcs_path=gcs_path,
            content_type="application/json",
            metadata=metadata
        )
    
    def append_to_manifest(self, 
                          manifest_path: str, 
                          record: Dict[str, Any]) -> str:
        """
        Append a JSON record to a JSONL manifest file.
        Creates the file if it doesn't exist.
        
        Args:
            manifest_path: GCS path to manifest.jsonl file
            record: Dictionary to append as JSON line
            
        Returns:
            Full GCS URI of the manifest
        """
        try:
            # Try to download existing manifest
            existing_content = ""
            blob = self.bucket.blob(manifest_path)
            
            try:
                existing_content = blob.download_as_text()
            except NotFound:
                logger.info(f"Creating new manifest at {manifest_path}")
            
            # Append new record
            new_line = json.dumps(record, ensure_ascii=False) + "\n"
            updated_content = existing_content + new_line
            
            # Upload updated manifest
            blob.content_type = "application/x-ndjson"
            blob.upload_from_string(updated_content)
            
            gcs_uri = f"gs://{self.bucket_name}/{manifest_path}"
            logger.info(f"Appended record to manifest {gcs_uri}")
            return gcs_uri
            
        except Exception as e:
            logger.error(f"Failed to append to manifest {manifest_path}: {e}")
            raise
    
    def object_exists(self, gcs_path: str) -> bool:
        """Check if an object exists in GCS."""
        try:
            blob = self.bucket.blob(gcs_path)
            return blob.exists()
        except Exception as e:
            logger.warning(f"Error checking if {gcs_path} exists: {e}")
            return False
    
    def download_json(self, gcs_path: str) -> Optional[Dict[str, Any]]:
        """
        Download and parse JSON from GCS.
        
        Args:
            gcs_path: GCS object path
            
        Returns:
            Parsed JSON data or None if not found
        """
        try:
            blob = self.bucket.blob(gcs_path)
            content = blob.download_as_text()
            return json.loads(content)
        except NotFound:
            logger.debug(f"Object not found: {gcs_path}")
            return None
        except Exception as e:
            logger.error(f"Failed to download JSON from {gcs_path}: {e}")
            raise
    
    def list_objects(self, prefix: str) -> List[str]:
        """
        List objects with a given prefix.
        
        Args:
            prefix: GCS path prefix
            
        Returns:
            List of object names
        """
        try:
            blobs = self.client.list_blobs(self.bucket, prefix=prefix)
            return [blob.name for blob in blobs]
        except Exception as e:
            logger.error(f"Failed to list objects with prefix {prefix}: {e}")
            raise
    
    def get_object_metadata(self, gcs_path: str) -> Optional[Dict[str, Any]]:
        """
        Get metadata for a GCS object.
        
        Args:
            gcs_path: GCS object path
            
        Returns:
            Object metadata or None if not found
        """
        try:
            blob = self.bucket.blob(gcs_path)
            blob.reload()  # Fetch metadata
            return {
                'name': blob.name,
                'size': blob.size,
                'content_type': blob.content_type,
                'created': blob.time_created,
                'updated': blob.updated,
                'etag': blob.etag,
                'md5_hash': blob.md5_hash,
                'metadata': blob.metadata or {}
            }
        except NotFound:
            return None
        except Exception as e:
            logger.error(f"Failed to get metadata for {gcs_path}: {e}")
            raise


def build_gcs_paths(ticker: str, period: str, filename: str) -> Dict[str, str]:
    """
    Build standardized GCS paths for a report.
    
    Args:
        ticker: Stock ticker symbol
        period: Period in YYYY-Q# format
        filename: Base filename (without extension)
        
    Returns:
        Dictionary with all relevant GCS paths
    """
    base_raw = f"lantern/raw/{ticker}/{period}"
    base_parsed = f"lantern/parsed/{ticker}/{period}"
    
    return {
        'raw_pdf': f"{base_raw}/{filename}.pdf",
        'text_dir': f"{base_parsed}/text",
        'tables_dir': f"{base_parsed}/tables", 
        'images_dir': f"{base_parsed}/images",
        'structured_json': f"{base_parsed}/structured.json",
        'docling_manifest': f"{base_parsed}/docling/manifest.json",
        'manifest_jsonl': f"{base_parsed}/manifest.jsonl",
        'state_json': f"lantern/state/{ticker}.json"
    }


def validate_gcs_layout(gcs_manager: GCSManager, 
                       paths: Dict[str, str], 
                       required_objects: List[str]) -> Dict[str, bool]:
    """
    Validate that required objects exist in GCS.
    
    Args:
        gcs_manager: GCS manager instance
        paths: Dictionary of GCS paths
        required_objects: List of required object keys from paths
        
    Returns:
        Dictionary mapping object keys to existence status
    """
    results = {}
    for obj_key in required_objects:
        if obj_key in paths:
            results[obj_key] = gcs_manager.object_exists(paths[obj_key])
        else:
            results[obj_key] = False
            
    return results


def atomic_state_update(gcs_manager: GCSManager,
                       state_path: str,
                       ticker: str,
                       sha256: str,
                       published: str,
                       additional_data: Optional[Dict[str, Any]] = None) -> str:
    """
    Atomically update the state file for a ticker.
    
    Args:
        gcs_manager: GCS manager instance
        state_path: GCS path to state file
        ticker: Stock ticker
        sha256: Report hash
        published: Publication date
        additional_data: Extra data to include in state
        
    Returns:
        GCS URI of updated state file
    """
    state_data = {
        'ticker': ticker,
        'sha256': sha256,
        'published': published,
        'last_updated': datetime.utcnow().isoformat(),
        **(additional_data or {})
    }
    
    return gcs_manager.upload_json(state_data, state_path)
