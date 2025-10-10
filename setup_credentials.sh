#!/bin/bash
# 🚀 Quick Setup Script - Use YOUR Credentials
# This script sets up everything you need to run the pipeline

# ============================================================================
# YOUR ACTUAL CREDENTIALS (from damg-big-data-1571d9725efd.json)
# ============================================================================

export PROJECT_ID="damg-big-data"
export SERVICE_ACCOUNT="pranav-patel@damg-big-data.iam.gserviceaccount.com"

# Set the path to your service account JSON file
export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/damg-big-data-1571d9725efd.json"

# ============================================================================
# GCS BUCKET CONFIGURATION
# ============================================================================

# Option 1: Use existing bucket (if you have one)
# export GCS_BUCKET="your-existing-bucket-name"

# Option 2: Create a new bucket (recommended)
export GCS_BUCKET="lantern-earnings-pipeline-${USER}"

echo "📦 Creating GCS bucket: $GCS_BUCKET"
gsutil mb -p $PROJECT_ID -l us-central1 gs://$GCS_BUCKET 2>/dev/null || echo "✅ Bucket already exists"

# Verify bucket access
echo "🔍 Verifying GCS access..."
gsutil ls gs://$GCS_BUCKET/ && echo "✅ GCS access verified!" || echo "❌ GCS access failed - check credentials"

# ============================================================================
# OPTIONAL: GOOGLE AI API KEY (for advanced features)
# ============================================================================

# Get your Gemini API key from: https://aistudio.google.com/app/apikey
# Then uncomment and set it:
# export GOOGLE_AI_API_KEY="AIza..."

# ============================================================================
# AIRFLOW CONFIGURATION
# ============================================================================

# Set Airflow home directory
export AIRFLOW_HOME="${HOME}/airflow"

echo "🔧 Setting up Airflow variables..."

# Set required Airflow variables
airflow variables set GCS_BUCKET "$GCS_BUCKET" 2>/dev/null || echo "⚠️  Airflow not installed or not running"
airflow variables set PROJECT_ID "$PROJECT_ID" 2>/dev/null || echo "⚠️  Set this manually in Airflow UI"

# Optional: Set Google AI API key
if [ ! -z "$GOOGLE_AI_API_KEY" ]; then
    airflow variables set GOOGLE_AI_API_KEY "$GOOGLE_AI_API_KEY" 2>/dev/null
fi

# ============================================================================
# VERIFY SETUP
# ============================================================================

echo ""
echo "✅ CONFIGURATION COMPLETE!"
echo "=================================="
echo "Project ID:      $PROJECT_ID"
echo "Service Account: $SERVICE_ACCOUNT"
echo "GCS Bucket:      gs://$GCS_BUCKET"
echo "Credentials:     $GOOGLE_APPLICATION_CREDENTIALS"
echo "Airflow Home:    $AIRFLOW_HOME"
echo "=================================="

# ============================================================================
# NEXT STEPS
# ============================================================================

echo ""
echo "🚀 NEXT STEPS:"
echo "1. Deploy to Airflow:"
echo "   ./deploy_to_airflow.sh"
echo ""
echo "2. Or test locally first:"
echo "   cd lantern-dow30"
echo "   python3 main.py --mode test --companies AAPL"
echo ""
echo "3. Check your bucket:"
echo "   gsutil ls gs://$GCS_BUCKET/"
echo ""

# Save configuration for later use
cat > .env << EOF
# Auto-generated configuration
export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/damg-big-data-1571d9725efd.json"
export PROJECT_ID="$PROJECT_ID"
export GCS_BUCKET="$GCS_BUCKET"
export AIRFLOW_HOME="$AIRFLOW_HOME"
# export GOOGLE_AI_API_KEY="your-key-here"
EOF

echo "💾 Configuration saved to .env"
echo "   Run 'source .env' to load these settings"
