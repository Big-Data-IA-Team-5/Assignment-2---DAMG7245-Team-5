#!/bin/bash
# QUICK START - Launch Airflow Pipeline in 1 Minute!

set -e

echo "🚀 QUICK START - DOW 30 EARNINGS PIPELINE"
echo "========================================"
echo ""

# Step 1: Stop any running containers
echo "📦 Step 1: Cleaning up old containers..."
docker-compose down 2>/dev/null || true
echo "✅ Cleanup complete"
echo ""

# Step 2: Start Airflow
echo "🐳 Step 2: Starting Airflow (this may take 30-60 seconds)..."
docker-compose up -d
echo "✅ Airflow started!"
echo ""

# Step 3: Wait for Airflow to be ready
echo "⏳ Step 3: Waiting for Airflow to initialize..."
sleep 15

# Check if webserver is ready
max_attempts=12
attempt=0
while [ $attempt -lt $max_attempts ]; do
    if curl -s http://localhost:8080/health > /dev/null 2>&1; then
        echo "✅ Airflow is ready!"
        break
    fi
    echo "   Waiting... ($((attempt + 1))/$max_attempts)"
    sleep 5
    attempt=$((attempt + 1))
done

if [ $attempt -eq $max_attempts ]; then
    echo "⚠️  Airflow might still be starting. Check http://localhost:8080"
fi

echo ""
echo "🎉 AIRFLOW IS RUNNING!"
echo "========================================"
echo ""
echo "📊 Airflow UI: http://localhost:8080"
echo "   Username: admin"
echo "   Password: admin"
echo ""
echo "🔧 Available DAGs:"
echo "   1. complete_earnings_pipeline (MAIN - Use this for demo!)"
echo "   2. dow30_airflow_pipeline"
echo "   3. simple_earnings_pipeline"
echo ""
echo "🎬 TO RUN THE DEMO:"
echo "   1. Open http://localhost:8080 in your browser"
echo "   2. Login with admin/admin"
echo "   3. Find 'complete_earnings_pipeline' in the DAG list"
echo "   4. Click the toggle to ENABLE it (if not enabled)"
echo "   5. Click the ▶️ PLAY button to trigger the DAG"
echo "   6. Click the DAG name to see the Graph View"
echo "   7. Watch the pipeline execute!"
echo ""
echo "📁 Pipeline will:"
echo "   ✅ Download earnings reports from Dow 30 companies"
echo "   ✅ Parse PDFs with Docling (extract text, tables, images)"
echo "   ✅ Upload to GCS bucket: gs://damgteam5/lantern/"
echo ""
echo "🔍 Monitor logs:"
echo "   docker logs -f airflow-scheduler"
echo ""
echo "🛑 To stop Airflow:"
echo "   docker-compose down"
echo ""
