#!/usr/bin/env python3
"""
Mock Download for Demo - Creates sample PDF files
Since ChromeDriver is not working in Docker, this creates mock PDFs for demonstration
"""

import os
import sys
from pathlib import Path
from datetime import datetime
import json

# Sample companies to process
DEMO_COMPANIES = ['AAPL', 'MSFT', 'NVDA', 'AMZN', 'GOOGL', 'META', 'TSLA', 'JPM', 'V', 'WMT']

def create_mock_pdf(ticker, output_dir):
    """Create a mock PDF file for demo purposes"""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    filename = output_path / f"{ticker}_2025_Q4_Earnings_Report.pdf"
    
    # Create a simple PDF-like file with text content
    content = f"""
%PDF-1.4
1 0 obj
<<
/Type /Catalog
/Pages 2 0 R
>>
endobj

2 0 obj
<<
/Type /Pages
/Kids [3 0 R]
/Count 1
>>
endobj

3 0 obj
<<
/Type /Page
/Parent 2 0 R
/MediaBox [0 0 612 792]
/Contents 4 0 R
>>
endobj

4 0 obj
<<
/Length 500
>>
stream
BT
/F1 24 Tf
100 700 Td
({ticker} - Q4 2025 Earnings Report) Tj
ET

BT
/F1 12 Tf
100 650 Td
(Company: {ticker}) Tj
ET

BT
/F1 12 Tf
100 630 Td
(Period: Q4 2025) Tj
ET

BT
/F1 12 Tf
100 610 Td
(Date: {datetime.now().strftime('%Y-%m-%d')}) Tj
ET

BT
/F1 12 Tf
100 580 Td
(Revenue: $XX.X Billion) Tj
ET

BT
/F1 12 Tf
100 560 Td
(Net Income: $X.X Billion) Tj
ET

BT
/F1 12 Tf
100 540 Td
(EPS: $X.XX) Tj
ET

endstream
endobj

xref
0 5
0000000000 65535 f 
0000000009 00000 n 
0000000058 00000 n 
0000000115 00000 n 
0000000214 00000 n 
trailer
<<
/Size 5
/Root 1 0 R
>>
startxref
766
%%EOF
"""
    
    with open(filename, 'w') as f:
        f.write(content)
    
    print(f"✅ Created mock PDF: {filename}")
    return str(filename)

def main():
    """Create mock PDFs for demo"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Create mock PDFs for demo')
    parser.add_argument('--output-dir', default='dow30_pipeline_reports_2025', help='Output directory')
    parser.add_argument('--companies', nargs='*', help='Specific companies')
    
    args = parser.parse_args()
    
    companies = args.companies if args.companies else DEMO_COMPANIES
    
    print(f"🎬 Creating mock PDFs for {len(companies)} companies...")
    print("="*60)
    
    results = []
    for ticker in companies:
        try:
            pdf_file = create_mock_pdf(ticker, args.output_dir)
            results.append({
                'ticker': ticker,
                'status': 'success',
                'file': pdf_file
            })
        except Exception as e:
            print(f"❌ Failed to create PDF for {ticker}: {e}")
            results.append({
                'ticker': ticker,
                'status': 'failed',
                'error': str(e)
            })
    
    # Save results
    results_file = Path(args.output_dir) / 'mock_download_results.json'
    with open(results_file, 'w') as f:
        json.dump({
            'timestamp': datetime.now().isoformat(),
            'total': len(companies),
            'successful': len([r for r in results if r['status'] == 'success']),
            'results': results
        }, f, indent=2)
    
    print("="*60)
    print(f"✅ Created {len(results)} mock PDFs")
    print(f"📁 Output directory: {args.output_dir}")
    print(f"📊 Results saved to: {results_file}")
    
    return 0 if all(r['status'] == 'success' for r in results) else 1

if __name__ == "__main__":
    sys.exit(main())
