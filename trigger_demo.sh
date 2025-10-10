#!/bin/bash
# Quick script to trigger the pipeline for demo

echo "🎯 Triggering DOW 30 Earnings Pipeline Demo"
echo "==========================================="
echo ""

# Unpause the DAG
echo "▶️  Unpausing the pipeline..."
/usr/local/bin/docker compose exec -T airflow-scheduler airflow dags unpause simple_earnings_pipeline 2>/dev/null || true

# Trigger the DAG
echo "🚀 Triggering the pipeline..."
/usr/local/bin/docker compose exec -T airflow-scheduler airflow dags trigger simple_earnings_pipeline 2>/dev/null

echo ""
echo "✅ Pipeline triggered successfully!"
echo ""
echo "🌐 View in Airflow UI: http://localhost:8080/dags/simple_earnings_pipeline/grid"
echo ""
echo "📊 Monitor execution:"
echo "   - Graph View: http://localhost:8080/dags/simple_earnings_pipeline/graph"
echo "   - Logs: /usr/local/bin/docker compose logs -f airflow-scheduler airflow-worker"
echo ""
