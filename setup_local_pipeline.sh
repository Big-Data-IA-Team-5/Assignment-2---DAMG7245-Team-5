#!/bin/bash
# Setup script for local pipeline execution
# This installs all required dependencies

set -e  # Exit on error

echo "=================================="
echo "🔧 Setting up Local Pipeline"
echo "=================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check Python version
echo -e "\n${YELLOW}Checking Python version...${NC}"
python_version=$(python3 --version 2>&1 | awk '{print $2}')
echo "Python version: $python_version"

# Create virtual environment if it doesn't exist
if [ ! -d ".venv" ]; then
    echo -e "\n${YELLOW}Creating virtual environment...${NC}"
    python3 -m venv .venv
    echo -e "${GREEN}✅ Virtual environment created${NC}"
else
    echo -e "\n${GREEN}✅ Virtual environment already exists${NC}"
fi

# Activate virtual environment
echo -e "\n${YELLOW}Activating virtual environment...${NC}"
source .venv/bin/activate

# Upgrade pip
echo -e "\n${YELLOW}Upgrading pip...${NC}"
pip install --upgrade pip

# Install dependencies
echo -e "\n${YELLOW}Installing dependencies...${NC}"

# Core dependencies
echo "Installing web scraping dependencies..."
pip install selenium==4.15.2 webdriver-manager==4.0.1 requests==2.32.3 beautifulsoup4==4.12.3 lxml==5.2.2

# Docling and PDF processing
echo "Installing Docling and PDF dependencies..."
pip install docling docling-core docling-ibm-models docling-parse PyMuPDF pypdf pillow

# Google Cloud
echo "Installing Google Cloud dependencies..."
pip install google-cloud-storage google-auth

# Data processing
echo "Installing data processing dependencies..."
pip install pandas numpy python-dotenv click tqdm

echo -e "\n${GREEN}✅ All dependencies installed${NC}"

# Verify critical imports
echo -e "\n${YELLOW}Verifying installations...${NC}"

python3 << EOF
import sys
errors = []

try:
    import selenium
    print("✅ Selenium installed")
except ImportError:
    errors.append("selenium")
    print("❌ Selenium not found")

try:
    import docling
    print("✅ Docling installed")
except ImportError:
    errors.append("docling")
    print("❌ Docling not found")

try:
    from google.cloud import storage
    print("✅ Google Cloud Storage installed")
except ImportError:
    errors.append("google-cloud-storage")
    print("❌ Google Cloud Storage not found")

try:
    import pandas
    print("✅ Pandas installed")
except ImportError:
    errors.append("pandas")
    print("❌ Pandas not found")

if errors:
    print(f"\n❌ Missing packages: {', '.join(errors)}")
    sys.exit(1)
else:
    print("\n✅ All critical packages verified!")
EOF

if [ $? -eq 0 ]; then
    echo -e "\n${GREEN}=================================="
    echo "✅ Setup Complete!"
    echo "==================================${NC}"
    echo ""
    echo "To run the pipeline:"
    echo "  1. Activate the environment: source .venv/bin/activate"
    echo "  2. Run: python3 run_pipeline_fixed.py --companies AAPL"
    echo ""
    echo "Or run the complete pipeline demo:"
    echo "  python3 run_complete_local_pipeline.py AAPL"
    echo ""
else
    echo -e "\n${RED}=================================="
    echo "❌ Setup Failed - Missing packages"
    echo "==================================${NC}"
    exit 1
fi
