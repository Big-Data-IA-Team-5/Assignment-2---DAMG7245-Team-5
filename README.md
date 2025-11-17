CODELAB: https://codelabs-preview.appspot.com/?file_id=1IlN56Pnl8Jaa8ayBx8qgIxRNDoqFGvKQkDHCYyBCtxE#0
# DOW 30 Earnings Pipeline - DAMG7245 Assignment 2

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8.0+-green.svg)](https://airflow.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue.svg)](https://docs.docker.com/compose/)
[![GCP](https://img.shields.io/badge/Google%20Cloud-Storage-orange.svg)](https://cloud.google.com/storage)

An automated end-to-end pipeline for downloading, parsing, and analyzing quarterly earnings reports from all 30 Dow Jones Industrial Average companies. Built with Apache Airflow, Docling, and Google Cloud Platform.

## � Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Pipeline Components](#pipeline-components)
- [Configuration](#configuration)
- [Airflow DAGs](#airflow-dags)
- [Troubleshooting](#troubleshooting)
- [Team](#team)

## 🎯 Overview

This project implements a production-ready data pipeline that:

1. **Discovers & Downloads** - Automatically finds and downloads the latest quarterly earnings reports for all DOW 30 companies using Selenium-based web scraping
2. **Parses Content** - Extracts text, tables, and images from PDF reports using Docling
3. **Stores Results** - Uploads raw PDFs and parsed content to Google Cloud Storage
4. **Orchestrates Workflow** - Manages the entire process with Apache Airflow

## ✨ Features

### � Core Capabilities

- **Multi-threaded Processing**: Download reports for all 30 companies with 8 parallel workers
- **Dynamic IR Discovery**: Automatically discovers investor relations pages (no hard-coded URLs)
- **Smart Report Detection**: Date-aware prioritization to fetch the most recent quarterly reports
- **Comprehensive Parsing**: Extracts text, tables, images, and metadata from PDFs
- **Cloud Integration**: Seamless upload to Google Cloud Storage
- **Airflow Orchestration**: Production-ready DAGs with retry logic and error handling

### 🔧 Technical Features

- Dockerized deployment with Docker Compose
- PostgreSQL backend for Airflow metadata
- LocalExecutor for efficient task execution
- Comprehensive logging and monitoring
- Failure analysis and retry mechanisms
- State management and progress tracking

## 🏗 Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Apache Airflow (Orchestrator)           │
└────────────────┬──────────────┬─────────────┬───────────────┘
                 │              │             │
                 ▼              ▼             ▼
        ┌────────────┐  ┌──────────────┐  ┌──────────┐
        │  Download  │  │   Docling    │  │   GCS    │
        │  (Selenium)│→ │   Parser     │→ │  Upload  │
        └────────────┘  └──────────────┘  └──────────┘
             │                  │                │
             ▼                  ▼                ▼
        [Raw PDFs]        [Parsed Data]    [Cloud Storage]
```

## 📦 Prerequisites

Before you begin, ensure you have the following installed:

- **Docker** (20.10+) and **Docker Compose** (2.0+)
- **Python** 3.10 or higher (for local development)
- **Google Cloud Platform** account with:
  - Project created
  - Service account with Storage Admin role
  - Service account JSON key downloaded

### System Requirements

- **Memory**: 4GB RAM minimum (8GB recommended)
- **Disk Space**: 10GB free space
- **OS**: Linux, macOS, or Windows with WSL2

## 🔧 Installation

### 1. Clone the Repository

```bash
git clone https://github.com/Big-Data-IA-Team-5/Assignment-2---DAMG7245-Team-5.git
cd Assignment-2---DAMG7245-Team-5
```

### 2. Set Up Google Cloud Credentials

Place your GCP service account JSON key in the project root:

```bash
cp /path/to/your/service-account-key.json ./damg-big-data-1571d9725efd.json
```

Update the file path in `docker-compose.yaml` if your filename differs.

### 3. Configure Environment Variables

Create a `.env` file (optional):

```bash
# Google Cloud
export GOOGLE_CLOUD_PROJECT=damg-big-data
export GCS_BUCKET=damgteam5

# Airflow
export AIRFLOW_UID=50000
```

### 4. Build and Start Services

```bash
# Build the Docker images
docker compose build

# Start all services
docker compose up -d

# Check service status
docker compose ps
```

### 5. Initialize Airflow

The `airflow-init` service automatically:
- Creates the database schema
- Sets up admin user (username: `airflow`, password: `airflow`)
- Configures Airflow variables

## 🚀 Quick Start

### Access Airflow Web UI

1. Open your browser and navigate to: `http://localhost:8080`
2. Login with:
   - **Username**: `airflow`
   - **Password**: `airflow`

### Run the Complete Pipeline

**Option 1: Via Airflow UI**
1. Navigate to DAGs page
2. Find `complete_earnings_pipeline`
3. Toggle the DAG to "On"
4. Click the "Play" button → "Trigger DAG"

**Option 2: Via Command Line**

```bash
# Trigger the complete pipeline
docker compose exec airflow-scheduler airflow dags trigger complete_earnings_pipeline

# Monitor DAG execution
docker compose exec airflow-scheduler airflow dags list-runs -d complete_earnings_pipeline

# Check task status
docker compose exec airflow-scheduler airflow tasks list complete_earnings_pipeline
```

### Run Individual Components

**Download Reports Only:**
```bash
docker compose exec airflow-scheduler bash -c '
  cd /opt/airflow/dags/lantern-dow30 && 
  python3 main.py --mode multi --workers 8
'
```

**Parse PDFs Only:**
```bash
docker compose exec airflow-scheduler bash -c '
  cd /opt/airflow/dags && 
  python3 docling_parser.py \
    --input-dir /opt/airflow/dags/lantern-dow30/dow30_pipeline_reports_2025 \
    --output-dir /opt/airflow/parsed_local
'
```

## 📖 Usage

### Download Modes

The downloader supports multiple execution modes:

```bash
# Multi-threaded (default: 8 workers, all 30 companies)
python main.py --mode multi --workers 8

# Process specific companies
python main.py --companies AAPL MSFT NVDA AMZN

# Single-threaded for debugging
python main.py --mode single --debug

# Quick test with subset
python main.py --mode test

# With failure analysis
python main.py --analyze-failures
```

### Parser Options

```bash
# Parse all PDFs in a directory
python docling_parser.py \
  --input-dir dow30_pipeline_reports_2025 \
  --output-dir parsed_local

# Parse with debug logging
python docling_parser.py \
  --input-dir dow30_pipeline_reports_2025 \
  --output-dir parsed_local \
  --debug
```

### GCS Upload

```bash
# Upload raw PDFs
python gcs_upload_helper.py \
  --type raw \
  --input-dir dow30_pipeline_reports_2025 \
  --bucket damgteam5

# Upload parsed content
python gcs_upload_helper.py \
  --type parsed \
  --input-dir parsed_local \
  --bucket damgteam5
```

## 📁 Project Structure

```
Assignment-2---DAMG7245-Team-5/
├── dags/                           # Airflow DAG definitions
│   ├── complete_earnings_pipeline.py
│   ├── dow30_airflow_pipeline.py
│   └── simple_earnings_pipeline.py
├── lantern-dow30/                  # Core pipeline modules
│   ├── main.py                     # Main entry point
│   ├── downloader.py               # Report downloader
│   ├── ir_discovery.py             # IR page discovery
│   ├── report_finder.py            # Report detection
│   ├── gcs_utils.py                # GCS utilities
│   ├── tickers.py                  # DOW 30 tickers
│   └── gcs_upload_helper.py        # Upload helper
├── config/                         # Configuration files
│   └── pipeline_config.py          # Pipeline configuration
├── credentials/                    # GCP credentials
│   └── damg-big-data-*.json        # Service account key
├── docling_parser.py               # PDF parsing with Docling
├── docker-compose.yaml             # Docker services definition
├── Dockerfile                      # Custom Airflow image
├── requirements.txt                # Python dependencies
├── requirements-airflow.txt        # Airflow-specific deps
├── dow30_pipeline_reports_2025/    # Downloaded PDFs
├── parsed_local/                   # Parsed content
├── airflow-logs/                   # Airflow execution logs
└── README.md                       # This file
```

## 🔄 Pipeline Components

### 1. Downloader (`lantern-dow30/`)

**Purpose**: Automated discovery and download of quarterly earnings reports

**Key Features**:
- Multi-threaded Selenium-based scraping
- Dynamic IR page discovery
- Smart report detection with date prioritization
- Comprehensive error handling

**Main Modules**:
- `ir_discovery.py` - Finds investor relations pages
- `report_finder.py` - Locates quarterly earnings reports
- `downloader.py` - Downloads PDF files
- `tickers.py` - DOW 30 company data

### 2. Parser (`docling_parser.py`)

**Purpose**: Extract structured data from PDF reports

**Extracts**:
- **Text**: Full document text content
- **Tables**: CSV format with preserved structure
- **Images**: Page snapshots and embedded pictures
- **Metadata**: File info, parsing stats, checksums

**Output Structure**:
```
parsed_local/{ticker}/{period}/
├── text/
│   └── report.txt
├── tables/
│   ├── table_1.csv
│   └── table_2.csv
├── images/
│   ├── page_1.png
│   └── page_2.png
└── metadata.json
```

### 3. GCS Uploader (`gcs_upload_helper.py`)

**Purpose**: Upload content to Google Cloud Storage

**Features**:
- Parallel uploads for better performance
- Automatic retry on failures
- Progress tracking
- Metadata preservation

### 4. Airflow DAGs (`dags/`)

**complete_earnings_pipeline.py**: End-to-end orchestration
- Downloads all 30 DOW companies (8 workers)
- Parses PDFs with Docling
- Uploads to GCS (parallel: raw + parsed)

**Schedule**: Weekly on Mondays (`0 0 * * 1`)

## ⚙️ Configuration

### Airflow Variables

Set via UI or CLI:

```bash
docker compose exec airflow-scheduler airflow variables set GCS_BUCKET "damgteam5"
docker compose exec airflow-scheduler airflow variables set PROJECT_ID "damg-big-data"
docker compose exec airflow-scheduler airflow variables set MAX_WORKERS "8"
```

### Pipeline Configuration

Edit `config/pipeline_config.py`:

```python
@dataclass
class PipelineConfig:
    input_dir: str = "dow30_pipeline_reports_2025"
    parsed_dir: str = "parsed_local"
    batch_size: int = 5
    max_workers: int = 3
```

### Environment Variables

Key environment variables in `docker-compose.yaml`:

- `GOOGLE_APPLICATION_CREDENTIALS`: Path to GCP service account
- `GOOGLE_CLOUD_PROJECT`: GCP project ID
- `GCS_BUCKET`: GCS bucket name
- `PYTHONPATH`: Python module path

## 📊 Airflow DAGs

### complete_earnings_pipeline

**Tasks**:
1. `download_earnings_reports` - Download PDFs (8 workers, all 30 companies)
2. `parse_pdfs_with_docling` - Extract content from PDFs
3. `upload_raw_pdfs` - Upload raw PDFs to GCS
4. `upload_parsed_content` - Upload parsed data to GCS

**Dependencies**:
```
download_earnings_reports → parse_pdfs_with_docling → [upload_raw_pdfs, upload_parsed_content]
```

**Schedule**: Weekly on Mondays at midnight UTC

## 🔍 Monitoring & Logs

### View Airflow Logs

```bash
# Follow scheduler logs
docker compose logs -f airflow-scheduler

# View webserver logs
docker compose logs -f airflow-webserver

# Check DAG execution logs
docker compose exec airflow-scheduler airflow dags list-jobs -d complete_earnings_pipeline
```

### Monitor DAG Runs

```bash
# List all DAG runs
docker compose exec airflow-scheduler airflow dags list-runs -d complete_earnings_pipeline

# Get task instance states
docker compose exec airflow-scheduler airflow tasks states-for-dag-run complete_earnings_pipeline manual__2025-10-10T17:04:17+00:00

# View task instance details
docker compose exec airflow-scheduler airflow tasks state complete_earnings_pipeline download_earnings_reports 2025-10-10
```

### Check Airflow Event Logs

View event logs in the Airflow UI:
1. Navigate to `http://localhost:8080`
2. Go to **Browse** → **Audit Logs**
3. Filter by DAG ID: `complete_earnings_pipeline`

**Understanding Event Types:**
- `running` - Task execution started
- `success` - Task completed successfully
- `failed` - Task failed (will retry if configured)
- `clear` - Task cleared for re-execution
- `confirm` - Task state confirmed after clearing

**Common Event Patterns:**
```
Task Retry Pattern:
1. running → failed (first attempt)
2. clear → confirm (manual or automatic retry)
3. running → success (successful retry)
```

### Access Log Files

Logs are persisted in `./airflow-logs/`:

```bash
# View recent task logs
ls -ltr airflow-logs/dag_id=complete_earnings_pipeline/

# Check specific task log
cat airflow-logs/dag_id=complete_earnings_pipeline/run_id=*/task_id=download_earnings_reports/attempt=1.log

# View all attempts for a task
ls airflow-logs/dag_id=complete_earnings_pipeline/run_id=manual__*/task_id=upload_raw_pdfs/
```

### Pipeline Outputs

```bash
# Check downloaded PDFs
ls -lh dow30_pipeline_reports_2025/

# View parsing summary
cat parsed_local/parsing_summary.json

# Check download summary
cat dow30_pipeline_reports_2025/download_summary.json

# Count successful downloads
ls dow30_pipeline_reports_2025/*.pdf | wc -l
```

### Real-time Monitoring

```bash
# Watch DAG status in real-time
watch -n 5 'docker compose exec airflow-scheduler airflow dags list-runs -d complete_earnings_pipeline --state running'

# Monitor task progress
docker compose exec airflow-scheduler bash -c 'tail -f /opt/airflow/logs/dag_id=complete_earnings_pipeline/run_id=manual__*/task_id=download_earnings_reports/attempt=*.log'
```

## 🐛 Troubleshooting

### Common Issues

**1. GCS Upload Tasks Failing: "No such file or directory"**

**Problem**: Tasks `upload_raw_pdfs` and `upload_parsed_content` fail with:
```
python3: can't open file '/opt/airflow/dags/lantern-dow30/gcs_upload_helper.py': [Errno 2] No such file or directory
```

**Solution**:
```bash
# The gcs_upload_helper.py file is missing. It has been created in the repository.
# Restart Airflow services to pick up the new file:
docker compose restart airflow-scheduler airflow-webserver

# Or rebuild if the file still isn't found:
docker compose down
docker compose up -d

# Verify the file exists in the container:
docker compose exec airflow-scheduler ls -la /opt/airflow/dags/lantern-dow30/gcs_upload_helper.py
```

**2. Airflow services won't start**
```bash
# Check Docker resources
docker stats

# Restart services
docker compose down
docker compose up -d

# Rebuild if needed
docker compose build --no-cache
```

**3. Permission errors**
```bash
# Fix permissions
sudo chown -R $USER:$USER airflow-logs
sudo chown -R $USER:$USER dow30_pipeline_reports_2025
sudo chown -R $USER:$USER parsed_local
```

**3. Chrome/Selenium issues**
```bash
# Verify Chrome installation in container
docker compose exec airflow-scheduler which chromium
docker compose exec airflow-scheduler which chromedriver

# Test Selenium
docker compose exec airflow-scheduler bash -c '
  export CHROME_BIN=/usr/bin/chromium && 
  export CHROMEDRIVER_PATH=/usr/bin/chromedriver && 
  cd /opt/airflow/dags/lantern-dow30 && 
  python3 main.py --mode single --companies AAPL
'
```

**4. GCS upload failures**
```bash
# Verify credentials
docker compose exec airflow-scheduler bash -c '
  cat $GOOGLE_APPLICATION_CREDENTIALS
'

# Test GCS access
docker compose exec airflow-scheduler bash -c '
  python3 -c "from google.cloud import storage; client = storage.Client(); print(list(client.list_buckets()))"
'
```

**5. Database connection issues**
```bash
# Reset database
docker compose down -v
docker compose up -d

# Check PostgreSQL health
docker compose exec postgres pg_isready -U airflow
```

### Debug Mode

Enable debug logging:

```bash
# In docker-compose.yaml
environment:
  AIRFLOW__LOGGING__LOGGING_LEVEL: DEBUG
  
# Or in specific tasks
docker compose exec airflow-scheduler bash -c '
  cd /opt/airflow/dags/lantern-dow30 && 
  python3 main.py --mode single --debug --companies AAPL
'
```

## 🧪 Testing

### Run Unit Tests

```bash
# Install test dependencies
pip install pytest pytest-asyncio

# Run tests
pytest tests/
```

### Validate Pipeline

```bash
# Check pipeline configuration
docker compose exec airflow-scheduler python3 /opt/airflow/dags/config/pipeline_config.py

# Verify DAG integrity
docker compose exec airflow-scheduler airflow dags list
docker compose exec airflow-scheduler airflow dags test complete_earnings_pipeline 2025-01-01
```

## � Development

### Adding New Companies

Edit `lantern-dow30/tickers.py`:

```python
DOW30_COMPANIES = {
    "AAPL": {
        "name": "Apple Inc.",
        "domain": "apple.com"
    },
    # Add new company here
}
```

### Creating Custom DAGs

Create a new file in `dags/`:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG(
    'my_custom_dag',
    default_args={'owner': 'team'},
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
)

task = BashOperator(
    task_id='my_task',
    bash_command='echo "Hello World"',
    dag=dag,
)
```

## 📚 Documentation

- [Airflow Documentation](https://airflow.apache.org/docs/)
- [Docling Documentation](https://github.com/DS4SD/docling)
- [Google Cloud Storage Python Client](https://cloud.google.com/python/docs/reference/storage/latest)
- [Selenium Documentation](https://selenium-python.readthedocs.io/)

## 👥 Team

**Big Data IA Team 5**
- Course: DAMG7245 - Big Data Systems and Intelligence Analytics
- Institution: Northeastern University
- Semester: Fall 2024

## 📄 License

This project is created for educational purposes as part of DAMG7245 coursework.

## 🙏 Acknowledgments

- Apache Airflow community
- Docling development team
- Google Cloud Platform
- DOW Jones & Company

---

**Made with ❤️ by DAMG7245 Team 5**
