#!/bin/bash

# Add Docker to PATH
export PATH="/usr/local/bin:/Applications/Docker.app/Contents/Resources/bin:$PATH"

echo "=========================================="
echo "  AIRFLOW STATUS CHECKER"
echo "=========================================="
echo ""

# Check if Docker is running
if ! docker info &> /dev/null; then
    echo "❌ Docker is not running!"
    echo "   Please start Docker Desktop and try again."
    exit 1
fi

echo "✅ Docker is running"
echo ""

# Check containers
echo "📦 CONTAINER STATUS:"
echo "===================="
docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "(NAMES|airflow|postgres)"
echo ""

# Check if webserver is healthy
echo "🌐 WEBSERVER STATUS:"
echo "===================="
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/health 2>/dev/null)

if [ "$HTTP_CODE" == "200" ]; then
    echo "✅ Airflow webserver is READY!"
    echo "   🌐 URL: http://localhost:8080"
    echo "   👤 Username: airflow"
    echo "   🔑 Password: airflow"
    echo ""
    echo "🎯 NEXT STEPS:"
    echo "1. Open http://localhost:8080 in your browser"
    echo "2. Login with airflow/airflow"
    echo "3. Go to Admin → Variables and add:"
    echo "   - GCS_BUCKET = damgteam5"
    echo "   - PROJECT_ID = damg-big-data"
    echo "   - RAW_PDF_DIR = /opt/airflow/data/dow30_pipeline_reports_2025"
    echo "   - DOCLING_OUTPUT_DIR = /opt/airflow/data/parsed_local"
    echo "   - MAX_WORKERS = 5"
    echo "4. Find 'simple_earnings_pipeline' DAG"
    echo "5. Toggle it ON"
    echo "6. Click Trigger DAG"
elif [ "$HTTP_CODE" == "000" ]; then
    echo "⏳ Airflow is still starting up..."
    echo "   This can take 2-3 minutes for package installation."
    echo ""
    echo "📋 Recent logs:"
    docker compose logs --tail=10 airflow-webserver 2>/dev/null | tail -5
    echo ""
    echo "💡 Run this script again in 1-2 minutes to check status."
else
    echo "⚠️  Webserver returned HTTP $HTTP_CODE"
    echo "   Check logs with: docker compose logs airflow-webserver"
fi

echo ""
echo "=========================================="
echo "  USEFUL COMMANDS:"
echo "=========================================="
echo "docker ps                        # Check containers"
echo "docker compose logs -f           # View all logs"
echo "docker compose logs webserver    # Webserver logs"
echo "docker compose restart           # Restart all services"
echo "docker compose down              # Stop all services"
echo "docker compose up -d             # Start all services"
echo "=========================================="
