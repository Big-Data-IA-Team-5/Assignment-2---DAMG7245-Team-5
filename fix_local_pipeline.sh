#!/bin/bash

# Fix Local Pipeline - Install missing dependencies and test

echo "🔧 Fixing Local Pipeline Environment"
echo "===================================="

# Activate virtual environment if it exists
if [ -d ".venv" ]; then
    echo "📦 Activating virtual environment..."
    source .venv/bin/activate
else
    echo "⚠️  No .venv found, using system Python"
fi

echo ""
echo "📥 Installing missing dependencies..."
echo ""

# Install Docling and dependencies
echo "1️⃣  Installing Docling..."
pip install -q docling

echo "2️⃣  Installing PyMuPDF (for better image extraction)..."
pip install -q PyMuPDF

echo "3️⃣  Installing Pillow (for image processing)..."
pip install -q Pillow

echo "4️⃣  Installing google-cloud-storage..."
pip install -q google-cloud-storage

echo ""
echo "✅ Dependencies installed successfully!"
echo ""

# Test imports
echo "🧪 Testing imports..."
python3 << 'EOF'
import sys

print("  Testing docling imports...")
try:
    from docling.document_converter import DocumentConverter
    from docling_parser import DoclingPDFParser
    print("    ✅ Docling imports OK")
except ImportError as e:
    print(f"    ❌ Docling import failed: {e}")
    sys.exit(1)

print("  Testing PyMuPDF...")
try:
    import fitz
    print("    ✅ PyMuPDF OK")
except ImportError:
    print("    ⚠️  PyMuPDF not available (optional)")

print("  Testing google-cloud-storage...")
try:
    from google.cloud import storage
    print("    ✅ Google Cloud Storage OK")
except ImportError as e:
    print(f"    ❌ Google Cloud Storage import failed: {e}")
    sys.exit(1)

print("\n✅ All critical imports successful!")
EOF

if [ $? -ne 0 ]; then
    echo ""
    echo "❌ Import test failed. Please check the errors above."
    exit 1
fi

echo ""
echo "🎉 Environment fixed successfully!"
echo ""
echo "📋 Next steps:"
echo "  1. Test docling parser: python3 docling_parser.py --dry-run"
echo "  2. Run local pipeline: python3 run_pipeline_local.py"
echo ""
