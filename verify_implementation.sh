#!/bin/bash
# Verification script for post-Docling pipeline

echo "🔍 Verifying Post-Docling Pipeline Implementation"
echo "=================================================="
echo

# Check core modules
echo "�� Core Modules (lantern-dow30/):"
for file in gcs_utils.py periods.py guidance_extractor.py manifest.py state.py qc.py; do
    if [ -f "lantern-dow30/$file" ]; then
        size=$(ls -lh "lantern-dow30/$file" | awk '{print $5}')
        echo "  ✅ $file ($size)"
    else
        echo "  ❌ $file - MISSING"
    fi
done
echo

# Check orchestration
echo "🔄 Orchestration:"
if [ -f "dags/lantern_docling_to_cloud.py" ]; then
    size=$(ls -lh "dags/lantern_docling_to_cloud.py" | awk '{print $5}')
    echo "  ✅ lantern_docling_to_cloud.py ($size)"
else
    echo "  ❌ lantern_docling_to_cloud.py - MISSING"
fi

if [ -f "scripts/collect_docling_outputs.py" ]; then
    size=$(ls -lh "scripts/collect_docling_outputs.py" | awk '{print $5}')
    echo "  ✅ collect_docling_outputs.py ($size)"
else
    echo "  ❌ collect_docling_outputs.py - MISSING"
fi
echo

# Check integration
echo "🔗 Integration:"
if [ -f "run_complete_pipeline.py" ]; then
    size=$(ls -lh "run_complete_pipeline.py" | awk '{print $5}')
    echo "  ✅ run_complete_pipeline.py ($size)"
else
    echo "  ❌ run_complete_pipeline.py - MISSING"
fi
echo

# Check documentation
echo "📚 Documentation:"
for file in README_PIPELINE.md QUICK_START.md IMPLEMENTATION_COMPLETE.md; do
    if [ -f "$file" ]; then
        size=$(ls -lh "$file" | awk '{print $5}')
        echo "  ✅ $file ($size)"
    else
        echo "  ❌ $file - MISSING"
    fi
done
echo

# Check examples
echo "📋 Examples:"
if [ -d "examples/parsed_samples/AAPL/2024-Q4" ]; then
    echo "  ✅ examples/parsed_samples/AAPL/2024-Q4/"
    if [ -f "examples/parsed_samples/AAPL/2024-Q4/structured.json" ]; then
        echo "    ✅ structured.json"
    fi
    if [ -f "examples/parsed_samples/AAPL/2024-Q4/manifest.jsonl" ]; then
        echo "    ✅ manifest.jsonl"
    fi
else
    echo "  ❌ examples/parsed_samples/ - MISSING"
fi
echo

# Summary
echo "📊 Summary:"
total_files=12
present_files=$(ls lantern-dow30/{gcs_utils,periods,guidance_extractor,manifest,state,qc}.py \
                   dags/lantern_docling_to_cloud.py \
                   scripts/collect_docling_outputs.py \
                   run_complete_pipeline.py \
                   README_PIPELINE.md QUICK_START.md IMPLEMENTATION_COMPLETE.md \
                   2>/dev/null | wc -l)
echo "  Files present: $present_files/$total_files"

if [ "$present_files" -eq "$total_files" ]; then
    echo "  Status: ✅ ALL COMPLETE"
else
    echo "  Status: ⚠️  Some files missing"
fi
echo

echo "🎯 Next Steps:"
echo "  1. Review README_PIPELINE.md for usage instructions"
echo "  2. Set environment variables (GOOGLE_AI_API_KEY, GCS_BUCKET, PROJECT_ID)"
echo "  3. Run: ./run_complete_pipeline.py"
echo
echo "✨ Implementation verification complete!"
