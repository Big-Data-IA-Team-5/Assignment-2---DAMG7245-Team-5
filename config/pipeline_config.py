"""
Post-Docling Pipeline Configuration
Centralized settings for Guidance + GCS + Airflow integration
"""

import os
from typing import Dict, Any
from dataclasses import dataclass
from pathlib import Path


@dataclass
class GeminiConfig:
    """Google AI Gemini configuration."""
    api_key: str
    model: str = "gemini-1.5-pro-latest"
    temperature: float = 0.1
    max_tokens: int = 8192
    timeout: int = 30


@dataclass
class GCSConfig:
    """Google Cloud Storage configuration."""
    project_id: str
    bucket_name: str
    service_account_path: str = ""
    location: str = "US"
    
    
@dataclass
class AirflowConfig:
    """Airflow/Cloud Composer configuration."""
    dag_id: str = "dow30_financial_pipeline"
    schedule_interval: str = "@daily"
    catchup: bool = False
    max_active_runs: int = 1
    default_retries: int = 2
    retry_delay_minutes: int = 5


@dataclass
class PipelineConfig:
    """Main pipeline configuration."""
    
    # Directories
    input_dir: str = "dow30_pipeline_reports_2025"
    parsed_dir: str = "parsed_local" 
    structured_dir: str = "structured_output"
    state_dir: str = "state"
    
    # Processing settings
    batch_size: int = 5
    max_workers: int = 3
    enable_validation: bool = True
    
    # Manifest settings
    manifest_file: str = "manifest.jsonl"
    
    # Cloud settings
    gemini: GeminiConfig = None
    gcs: GCSConfig = None
    airflow: AirflowConfig = None
    
    def __post_init__(self):
        """Initialize cloud configs with environment variables."""
        if self.gemini is None:
            self.gemini = GeminiConfig(
                api_key=os.getenv("GEMINI_API_KEY", ""),
                model=os.getenv("GEMINI_MODEL", "gemini-1.5-pro-latest"),
                temperature=float(os.getenv("GEMINI_TEMPERATURE", "0.1")),
                max_tokens=int(os.getenv("GEMINI_MAX_TOKENS", "8192"))
            )
            
        if self.gcs is None:
            self.gcs = GCSConfig(
                project_id=os.getenv("GCP_PROJECT_ID", ""),
                bucket_name=os.getenv("GCS_BUCKET_NAME", "dow30-financial-pipeline"),
                service_account_path=os.getenv("GOOGLE_APPLICATION_CREDENTIALS", ""),
                location=os.getenv("GCS_LOCATION", "US")
            )
            
        if self.airflow is None:
            self.airflow = AirflowConfig(
                dag_id=os.getenv("AIRFLOW_DAG_ID", "dow30_financial_pipeline"),
                schedule_interval=os.getenv("AIRFLOW_SCHEDULE", "@daily"),
                catchup=os.getenv("AIRFLOW_CATCHUP", "False").lower() == "true",
                max_active_runs=int(os.getenv("AIRFLOW_MAX_RUNS", "1")),
                default_retries=int(os.getenv("AIRFLOW_RETRIES", "2"))
            )
    
    @classmethod
    def from_env(cls) -> "PipelineConfig":
        """Create configuration from environment variables."""
        return cls()
    
    def validate(self) -> Dict[str, Any]:
        """Validate configuration and return status."""
        errors = []
        warnings = []
        
        # Required API keys
        if not self.gemini.api_key:
            errors.append("GEMINI_API_KEY not set")
            
        if not self.gcs.project_id:
            errors.append("GCP_PROJECT_ID not set")
            
        # Directory checks
        for dir_name in [self.input_dir, self.parsed_dir]:
            if not Path(dir_name).exists():
                warnings.append(f"Directory {dir_name} does not exist")
        
        # Service account check
        if self.gcs.service_account_path and not Path(self.gcs.service_account_path).exists():
            warnings.append(f"Service account file not found: {self.gcs.service_account_path}")
        
        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings
        }


# Global configuration instance
config = PipelineConfig.from_env()


def get_config() -> PipelineConfig:
    """Get global configuration instance."""
    return config


def print_config_status():
    """Print configuration validation status."""
    status = config.validate()
    
    print("📋 Pipeline Configuration Status:")
    print(f"   Valid: {'✅' if status['valid'] else '❌'}")
    
    if status['errors']:
        print("   🚨 Errors:")
        for error in status['errors']:
            print(f"      - {error}")
    
    if status['warnings']:
        print("   ⚠️ Warnings:")
        for warning in status['warnings']:
            print(f"      - {warning}")
    
    print(f"\n📁 Directories:")
    print(f"   Input: {config.input_dir}")
    print(f"   Parsed: {config.parsed_dir}")
    print(f"   Structured: {config.structured_dir}")
    print(f"   State: {config.state_dir}")
    
    print(f"\n☁️ Cloud Settings:")
    print(f"   GCP Project: {config.gcs.project_id or 'Not set'}")
    print(f"   GCS Bucket: {config.gcs.bucket_name}")
    print(f"   Gemini Model: {config.gemini.model}")
    
    return status


if __name__ == "__main__":
    print_config_status()
