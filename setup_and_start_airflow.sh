#!/bin/bash
#
# 🚀 Airflow + Docling Quick Setup & Start Script
# 
# This script installs and configures Airflow with Docling integration
#

set -e  # Exit on error

echo "========================================================================"
echo "🚀 AIRFLOW + DOCLING SETUP"
echo "========================================================================"

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# ============================================================================
# 1. ENVIRONMENT SETUP
# ============================================================================

echo -e "\n${BLUE}📋 Step 1: Environment Setup${NC}"

# Set Airflow home
export AIRFLOW_HOME=~/airflow
echo "✓ AIRFLOW_HOME set to: $AIRFLOW_HOME"

# Get project directory
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "✓ Project directory: $PROJECT_DIR"

# Load GCS credentials
if [ -f "$PROJECT_DIR/damg-big-data-1571d9725efd.json" ]; then
    export GOOGLE_APPLICATION_CREDENTIALS="$PROJECT_DIR/damg-big-data-1571d9725efd.json"
    echo "✓ GCS credentials loaded"
else
    echo -e "${YELLOW}⚠️  Warning: GCS credentials not found${NC}"
fi

# ============================================================================
# 2. INSTALL AIRFLOW (if not installed)
# ============================================================================

echo -e "\n${BLUE}📦 Step 2: Checking Airflow Installation${NC}"

if ! command -v airflow &> /dev/null; then
    echo "Installing Apache Airflow..."
    
    AIRFLOW_VERSION=2.8.0
    PYTHON_VERSION="$(python3 --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
    CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
    
    pip3 install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
    pip3 install apache-airflow-providers-google
    
    echo "✓ Airflow installed"
else
    echo "✓ Airflow already installed ($(airflow version))"
fi

# ============================================================================
# 3. INSTALL DEPENDENCIES
# ============================================================================

echo -e "\n${BLUE}📦 Step 3: Installing Dependencies${NC}"

pip3 install -q docling PyMuPDF pandas google-cloud-storage selenium beautifulsoup4

echo "✓ Dependencies installed"

# ============================================================================
# 4. INITIALIZE AIRFLOW
# ============================================================================

echo -e "\n${BLUE}🗄️  Step 4: Initializing Airflow Database${NC}"

# Create directories
mkdir -p $AIRFLOW_HOME/dags
mkdir -p $AIRFLOW_HOME/logs
mkdir -p $AIRFLOW_HOME/plugins
mkdir -p $AIRFLOW_HOME/data/docling_outputs
mkdir -p $AIRFLOW_HOME/data/raw_pdfs

# Initialize DB (if not exists)
if [ ! -f "$AIRFLOW_HOME/airflow.db" ]; then
    airflow db init
    echo "✓ Database initialized"
    
    # Create admin user
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin123 2>/dev/null || echo "✓ User already exists"
else
    echo "✓ Database already initialized"
fi

# ============================================================================
# 5. CONFIGURE AIRFLOW
# ============================================================================

echo -e "\n${BLUE}⚙️  Step 5: Configuring Airflow${NC}"

# Update airflow.cfg
sed -i.bak 's/load_examples = True/load_examples = False/' $AIRFLOW_HOME/airflow.cfg
sed -i.bak 's/executor = SequentialExecutor/executor = LocalExecutor/' $AIRFLOW_HOME/airflow.cfg

echo "✓ Airflow configured"

# ============================================================================
# 6. SETUP DAGS
# ============================================================================

echo -e "\n${BLUE}📂 Step 6: Setting up DAGs${NC}"

# Link DAG files
if [ -f "$PROJECT_DIR/dags/simple_earnings_pipeline.py" ]; then
    ln -sf "$PROJECT_DIR/dags/simple_earnings_pipeline.py" $AIRFLOW_HOME/dags/
    echo "✓ Linked simple_earnings_pipeline.py"
fi

# Link project modules
ln -sf "$PROJECT_DIR/lantern-dow30" $AIRFLOW_HOME/dags/
ln -sf "$PROJECT_DIR/docling_parser.py" $AIRFLOW_HOME/dags/
ln -sf "$PROJECT_DIR/config" $AIRFLOW_HOME/dags/

echo "✓ Project modules linked"

# ============================================================================
# 7. SET AIRFLOW VARIABLES
# ============================================================================

echo -e "\n${BLUE}🔧 Step 7: Setting Airflow Variables${NC}"

# Set variables
airflow variables set GOOGLE_APPLICATION_CREDENTIALS "$GOOGLE_APPLICATION_CREDENTIALS" 2>/dev/null || true
airflow variables set GCS_BUCKET "damgteam5" 2>/dev/null || true
airflow variables set PROJECT_ID "damg-big-data" 2>/dev/null || true
airflow variables set RAW_PDF_DIR "$PROJECT_DIR/dow30_pipeline_reports_2025" 2>/dev/null || true
airflow variables set DOCLING_OUTPUT_DIR "$PROJECT_DIR/parsed_local" 2>/dev/null || true
airflow variables set MAX_WORKERS "5" 2>/dev/null || true
airflow variables set ENABLED_TICKERS "" 2>/dev/null || true

echo "✓ Variables configured"

# ============================================================================
# 8. START AIRFLOW
# ============================================================================

echo -e "\n${BLUE}🚀 Step 8: Starting Airflow Services${NC}"

# Kill existing processes
pkill -f "airflow webserver" 2>/dev/null || true
pkill -f "airflow scheduler" 2>/dev/null || true
sleep 2

# Start webserver in background
echo "Starting webserver..."
airflow webserver -p 8080 -D

# Start scheduler in background  
echo "Starting scheduler..."
airflow scheduler -D

# Wait for services to start
echo "Waiting for services to start..."
sleep 5

# ============================================================================
# 9. VERIFY SETUP
# ============================================================================

echo -e "\n${BLUE}✅ Step 9: Verifying Setup${NC}"

# Check if services are running
if pgrep -f "airflow webserver" > /dev/null; then
    echo "✓ Webserver running"
else
    echo "✗ Webserver not running"
fi

if pgrep -f "airflow scheduler" > /dev/null; then
    echo "✓ Scheduler running"
else
    echo "✗ Scheduler not running"
fi

# List DAGs
echo -e "\n📋 Available DAGs:"
airflow dags list 2>/dev/null | grep simple_earnings || echo "  (DAGs loading...)"

# ============================================================================
# 10. COMPLETE
# ============================================================================

echo -e "\n========================================================================"
echo -e "${GREEN}✅ AIRFLOW + DOCLING SETUP COMPLETE!${NC}"
echo "========================================================================"
echo ""
echo "🌐 Web UI:     http://localhost:8080"
echo "👤 Username:   admin"
echo "🔑 Password:   admin123"
echo ""
echo "📋 Next Steps:"
echo "   1. Open http://localhost:8080 in your browser"
echo "   2. Login with admin/admin123"
echo "   3. Find 'simple_earnings_pipeline' DAG"
echo "   4. Toggle it ON and click 'Trigger DAG'"
echo ""
echo "📊 Monitor execution:"
echo "   - Graph View: Visual task flow"
echo "   - Grid View:  Execution history"
echo "   - Logs:       Real-time logs"
echo ""
echo "🛑 To stop Airflow:"
echo "   pkill -f 'airflow webserver'"
echo "   pkill -f 'airflow scheduler'"
echo ""
echo "📝 Logs location:"
echo "   ~/airflow/logs/"
echo ""
echo "========================================================================"
