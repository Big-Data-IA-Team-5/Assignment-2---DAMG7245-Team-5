#!/bin/bash
# Demo Setup Script for DOW 30 Earnings Pipeline with Airflow

set -e

echo "🎬 Setting up DOW 30 Earnings Pipeline Demo"
echo "============================================"
echo ""

# Step 1: Clean up unnecessary files
echo "🧹 Step 1: Cleaning workspace..."
rm -rf __pycache__ */__pycache__ */*/__pycache__ 2>/dev/null || true
rm -rf airflow-logs/* 2>/dev/null || true
rm -rf logs/* 2>/dev/null || true
rm -rf parsed_local/* 2>/dev/null || true
rm -rf state/* 2>/dev/null || true
rm -rf test_reports/* 2>/dev/null || true
rm -f test_summary.json 2>/dev/null || true
find . -name "*.pyc" -delete 2>/dev/null || true
find . -name ".DS_Store" -delete 2>/dev/null || true
echo "✅ Workspace cleaned"
echo ""

# Step 2: Create necessary directories
echo "📁 Step 2: Creating directories..."
mkdir -p dow30_pipeline_reports_2025
mkdir -p parsed_local
mkdir -p credentials
mkdir -p airflow-logs
mkdir -p airflow-plugins
echo "✅ Directories created"
echo ""

# Step 3: Fix docling_parser.py in root (should be empty or symlink)
echo "🔧 Step 3: Fixing file conflicts..."
# Remove the empty docling_parser.py from root since we have it in dags/
if [ -f "docling_parser.py" ] && [ ! -s "docling_parser.py" ]; then
    rm -f docling_parser.py
    echo "✅ Removed empty docling_parser.py from root"
fi
echo ""

# Step 4: Setup credentials
echo "🔐 Step 4: Setting up GCS credentials..."
if [ -f "damg-big-data-1571d9725efd.json" ]; then
    cp damg-big-data-1571d9725efd.json credentials/
    echo "✅ Credentials copied to credentials/"
elif [ -f "config/damg-big-data-1571d9725efd.json" ]; then
    cp config/damg-big-data-1571d9725efd.json credentials/
    echo "✅ Credentials copied from config/"
elif [ -f "credentials/damg-big-data-1571d9725efd.json" ]; then
    echo "✅ Credentials already in place"
else
    echo "⚠️  Warning: GCS credentials file not found"
    echo "   Please place damg-big-data-1571d9725efd.json in the project root"
fi
echo ""

# Step 5: Stop and remove old containers
echo "🛑 Step 5: Stopping existing Airflow containers..."
/usr/local/bin/docker compose down -v
echo "✅ Containers stopped and volumes removed"
echo ""

# Step 6: Rebuild and start services
echo "🔨 Step 6: Building and starting Airflow services..."
/usr/local/bin/docker compose build
/usr/local/bin/docker compose up -d
echo "✅ Services started"
echo ""

# Step 7: Wait for services to be healthy
echo "⏳ Step 7: Waiting for services to be healthy (60 seconds)..."
sleep 60
echo ""

# Step 8: Check service health
echo "🏥 Step 8: Checking service health..."
/usr/local/bin/docker compose ps
echo ""

# Step 9: Set Airflow Variables
echo "⚙️  Step 9: Configuring Airflow Variables..."
/usr/local/bin/docker compose exec -T airflow-scheduler airflow variables set GCS_BUCKET "damgteam5" 2>/dev/null || true
/usr/local/bin/docker compose exec -T airflow-scheduler airflow variables set PROJECT_ID "damg-big-data" 2>/dev/null || true
/usr/local/bin/docker compose exec -T airflow-scheduler airflow variables set GOOGLE_APPLICATION_CREDENTIALS "/opt/airflow/credentials/damg-big-data-1571d9725efd.json" 2>/dev/null || true
/usr/local/bin/docker compose exec -T airflow-scheduler airflow variables set RAW_PDF_DIR "/opt/airflow/data/dow30_pipeline_reports_2025" 2>/dev/null || true
/usr/local/bin/docker compose exec -T airflow-scheduler airflow variables set DOCLING_OUTPUT_DIR "/opt/airflow/data/parsed_local" 2>/dev/null || true
/usr/local/bin/docker compose exec -T airflow-scheduler airflow variables set MAX_WORKERS "5" 2>/dev/null || true
/usr/local/bin/docker compose exec -T airflow-scheduler airflow variables set ENABLED_TICKERS "" 2>/dev/null || true
echo "✅ Variables configured"
echo ""

# Step 10: Check available DAGs
echo "📋 Step 10: Checking available DAGs..."
echo ""
/usr/local/bin/docker compose exec -T airflow-scheduler airflow dags list 2>/dev/null | grep -E "dag_id|simple_earnings|complete_earnings|lantern" | head -20
echo ""

# Step 11: Check DAG files in container
echo "📂 Step 11: Verifying DAG files in container..."
/usr/local/bin/docker compose exec -T airflow-scheduler ls -la /opt/airflow/dags/ | grep -E "\.py$"
echo ""

echo "✅ =================================================="
echo "✅ DEMO SETUP COMPLETE!"
echo "✅ =================================================="
echo ""
echo "🌐 Airflow UI: http://localhost:8080"
echo "   Username: airflow"
echo "   Password: airflow"
echo ""
echo "📝 Next steps for your demo:"
echo "   1. Open http://localhost:8080 in your browser"
echo "   2. Log in with credentials above"
echo "   3. Find 'simple_earnings_pipeline' in the DAGs list"
echo "   4. Unpause the DAG (toggle switch)"
echo "   5. Trigger the DAG manually (play button)"
echo "   6. Watch the execution in the Graph or Grid view"
echo ""
echo "🎥 For a clean demo, you may want to:"
echo "   - Clear browser cache/cookies for localhost:8080"
echo "   - Use incognito/private browsing"
echo "   - Have the Graph view ready to show"
echo ""
