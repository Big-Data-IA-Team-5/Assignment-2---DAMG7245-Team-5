#!/bin/bash
# Test Complete Earnings Pipeline Manually

echo "🧪 Testing Complete Earnings Pipeline"
echo "====================================="
echo ""

# Test 1: Download (Single Company Test)
echo "📥 Test 1: Download Test (AAPL only)"
echo "-----------------------------------"
cd dags/lantern-dow30
python3 main.py --companies AAPL --mode multi --workers 1
echo ""

# Test 2: Parse Test  
echo "📝 Test 2: Parse Test"
echo "-------------------"
cd ../..
python3 docling_parser.py --input-dir dow30_pipeline_reports_2025 --output-dir parsed_local
echo ""

# Test 3: GCS Upload Test
echo "☁️  Test 3: GCS Upload Test"
echo "-------------------------"
cd dags/lantern-dow30
python3 gcs_upload_helper.py --type raw --input-dir ../../dow30_pipeline_reports_2025 --bucket damgteam5
python3 gcs_upload_helper.py --type parsed --input-dir ../../parsed_local --bucket damgteam5
echo ""

echo "✅ Manual test complete!"
echo ""
echo "Check GCS bucket: https://console.cloud.google.com/storage/browser/damgteam5/lantern"
