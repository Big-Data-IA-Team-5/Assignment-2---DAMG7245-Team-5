"""
Financial Report Downloader Module
Automatically downloads discovered financial reports
"""

import os
import requests
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("downloader")

class ReportDownloader:
    def __init__(self, output_dir='dow30_reports_2025'):
        """Initialize downloader with output directory"""
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        log.info(f"Initialized downloader with output directory: {output_dir}")
    
    def get_file_extension(self, url):
        """Determine file extension from URL"""
        url_lower = url.lower()
        if '.xlsx' in url_lower:
            return '.xlsx'
        elif '.xls' in url_lower:
            return '.xls'
        else:
            return '.pdf'
    
    def generate_filename(self, ticker, report_info):
        """Generate standardized filename for report"""
        year = report_info.get('year', 'unknown')
        ext = self.get_file_extension(report_info['url'])
        
        # Clean year for filename
        year_clean = year.replace('_', '-')
        
        filename = f"{ticker}_{year_clean}_financial_report{ext}"
        return os.path.join(self.output_dir, filename)
    
    def file_already_exists(self, filepath):
        """Check if file already exists and is valid (not corrupted)"""
        if os.path.exists(filepath):
            # Use enhanced validation
            ticker = os.path.basename(filepath).split('_')[0]  # Extract ticker from filename
            is_corrupted = self._validate_downloaded_file(filepath, ticker)
            
            if is_corrupted:
                log.warning(f"File exists but appears corrupted: {os.path.basename(filepath)}")
                log.info(f"Removing corrupted file for re-download...")
                os.remove(filepath)
                return False
            else:
                file_size = os.path.getsize(filepath)
                log.info(f"Valid file already exists: {os.path.basename(filepath)} ({file_size/1024:.1f} KB)")
                return True
        return False
    
    def is_direct_download_url(self, url):
        """Check if URL is a direct download (file) or a page URL"""
        if not url:
            return False
        
        url_lower = url.lower()
        
        # Direct download if it has a file extension
        file_extensions = ['.pdf', '.xlsx', '.xls', '.doc', '.docx']
        return any(url_lower.endswith(ext) for ext in file_extensions)
    
    def find_pdf_links_on_page(self, page_url):
        """Scan an event/detail page to find PDF download links"""
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            from selenium.webdriver.common.by import By
            from webdriver_manager.chrome import ChromeDriverManager
            
            log.info(f"Scanning page for PDF links: {page_url}")
            
            # Setup headless browser
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            
            driver = None
            try:
                driver = webdriver.Chrome(options=chrome_options)
                driver.set_page_load_timeout(30)
                driver.get(page_url)
                
                # Look for PDF links
                pdf_links = []
                elements = driver.find_elements(By.TAG_NAME, 'a')
                
                for element in elements:
                    try:
                        href = element.get_attribute('href')
                        text = element.text.strip()
                        
                        if href and '.pdf' in href.lower():
                            pdf_links.append({
                                'url': href,
                                'text': text,
                                'element_type': 'link'
                            })
                    except:
                        continue
                
                log.info(f"Found {len(pdf_links)} PDF links on page")
                return pdf_links
                
            finally:
                if driver:
                    driver.quit()
                    
        except Exception as e:
            log.error(f"Error scanning page for PDFs: {str(e)}")
            return []

    def download_report(self, report_info, ticker):
        """Download a single financial report - enhanced to handle event pages"""
        try:
            url = report_info['url']
            log.info(f"Downloading report for {ticker}")
            log.info(f"URL: {url}")
            
            # PRE-FILTER: Only reject ACTUAL calendar files, not transcripts or PDFs
            url_lower = url.lower()
            
            # Only reject actual calendar files, NOT legitimate reports
            rejected_patterns = [
                'webcast.ics', '.ics', 'add-to-calendar', 'calendar.ics'
            ]
            
            # Be more lenient - allow earnings calls if they might contain transcripts/PDFs
            should_reject = False
            for pattern in rejected_patterns:
                if pattern in url_lower:
                    should_reject = True
                    break
            
            # Allow if it's a PDF even if it contains "earnings-call"  
            if '.pdf' in url_lower or 'doc_financials' in url_lower or 'transcript' in url_lower:
                should_reject = False
                
            if should_reject:
                log.warning(f"Skipping actual calendar file URL: {url}")
                return False
            
            # Generate filename
            filepath = self.generate_filename(ticker, report_info)
            
            # Check if already downloaded
            if self.file_already_exists(filepath):
                return True
            
            # ENHANCED: Try multiple download methods for better success rate
            download_methods = [
                ("direct_file", self._download_direct_file),
                ("enhanced_headers", self._download_with_enhanced_headers),
                ("html_extraction", self._download_with_html_extraction),
                ("selenium_download", self._download_with_selenium)
            ]
            
            for method_name, method_func in download_methods:
                try:
                    log.info(f"Trying download method: {method_name}")
                    success = method_func(url, filepath, ticker)
                    if success:
                        log.info(f"✅ {ticker}: Download successful with {method_name}")
                        return True
                except Exception as method_error:
                    log.warning(f"Method {method_name} failed for {ticker}: {method_error}")
                    continue
            
            log.error(f"❌ All download methods failed for {ticker}")
            return False
            
        except Exception as e:
            log.error(f"Download failed for {ticker}: {str(e)}")
            return False
            
    def _download_with_enhanced_headers(self, url, filepath, ticker):
        """Enhanced headers download method with more aggressive browser simulation"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Microsoft Edge";v="120"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"'
            }
            
            max_retries = 3
            for attempt in range(max_retries):
                log.info(f"Enhanced headers download attempt {attempt + 1}/{max_retries} for {ticker}")
                
                response = requests.get(url, headers=headers, timeout=90, stream=True, allow_redirects=True)
                
                # Check content type - strictly reject HTML
                content_type = response.headers.get('content-type', '').lower()
                if 'text/html' in content_type or 'application/html' in content_type:
                    log.warning(f"HTML content detected (Content-Type: {content_type}) - skipping attempt {attempt + 1}")
                    if attempt < max_retries - 1:
                        continue
                    else:
                        log.warning(f"All attempts returned HTML content for {ticker}")
                        return False
                
                response.raise_for_status()
                
                # Save the file
                with open(filepath, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                # Validate file - if corrupted, try next attempt
                if self._validate_downloaded_file(filepath, ticker):
                    log.warning(f"Downloaded file validation failed for {ticker} - attempt {attempt + 1}")
                    if os.path.exists(filepath):
                        os.remove(filepath)  # Remove corrupted file
                    if attempt < max_retries - 1:
                        continue
                    else:
                        return False  # All validation attempts failed
                
                file_size = os.path.getsize(filepath)
                log.info(f"Successfully downloaded: {os.path.basename(filepath)} ({file_size / 1024:.1f} KB)")
                return True
                
        except Exception as e:
            log.error(f"Enhanced headers download failed for {ticker}: {e}")
            return False
    
    def _download_with_selenium(self, url, filepath, ticker):
        """Selenium-based download for complex JavaScript-heavy pages"""
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            import time
            
            # Setup headless Chrome
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1920,1080')
            
            # Set download preferences
            prefs = {
                "download.default_directory": os.path.dirname(filepath),
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True
            }
            chrome_options.add_experimental_option("prefs", prefs)
            
            log.info(f"Selenium download for {ticker}")
            
            driver = webdriver.Chrome(options=chrome_options)
            
            try:
                driver.get(url)
                
                # Wait for page to load
                WebDriverWait(driver, 30).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )
                
                # Look for direct download links or try to get page content
                download_links = driver.find_elements(By.XPATH, "//a[contains(@href, '.pdf')]")
                
                if download_links:
                    # Click the first PDF download link
                    download_links[0].click()
                    time.sleep(5)  # Wait for download
                else:
                    # If it's a PDF viewer page, get the content differently
                    page_source = driver.page_source
                    
                    # Try to extract PDF content or save as HTML converted to PDF
                    if '%PDF' in page_source:
                        # Direct PDF content in page source
                        pdf_start = page_source.find('%PDF')
                        pdf_content = page_source[pdf_start:].encode('latin1')
                        with open(filepath, 'wb') as f:
                            f.write(pdf_content)
                    else:
                        # Save current content and validate
                        with open(filepath, 'wb') as f:
                            # Get binary content via requests using current session cookies
                            cookies = driver.get_cookies()
                            session = requests.Session()
                            for cookie in cookies:
                                session.cookies.set(cookie['name'], cookie['value'])
                            
                            response = session.get(url, stream=True)
                            for chunk in response.iter_content(chunk_size=8192):
                                if chunk:
                                    f.write(chunk)
                
                # Validate the downloaded file
                if os.path.exists(filepath):
                    file_size = os.path.getsize(filepath)
                    if self._validate_downloaded_file(filepath, ticker):
                        # File is corrupted, delete it
                        log.warning(f"Deleting corrupted file for {ticker}")
                        os.remove(filepath)
                        return False
                    else:
                        log.info(f"✅ Selenium download successful for {ticker}")
                        return True
                
                return False
                
            finally:
                driver.quit()
                
        except Exception as e:
            log.error(f"Selenium download failed for {ticker}: {e}")
            return False

    def _download_with_html_extraction(self, url, filepath, ticker):
        """Extract PDF links from HTML earnings pages - fixes MSFT, NKE, AMGN, IBM, INTC, MRK"""
        try:
            log.info(f"HTML extraction download for {ticker}")
            
            # Enhanced headers for better success
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
            
            # Get the HTML page
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code != 200:
                log.warning(f"Failed to fetch HTML page: {response.status_code}")
                return False
                
            html_content = response.text
            log.info(f"Fetched HTML page ({len(html_content)} chars), extracting PDF links...")
            
            # Extract PDF links using multiple patterns
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Look for PDF links
            pdf_links = []
            
            # Pattern 1: Direct PDF links in href attributes
            for link in soup.find_all('a', href=True):
                href = link.get('href', '')
                text = link.get_text(strip=True)
                
                if '.pdf' in href.lower():
                    # Make absolute URL if needed
                    if href.startswith('/'):
                        from urllib.parse import urljoin
                        href = urljoin(url, href)
                    elif not href.startswith('http'):
                        continue
                        
                    pdf_links.append({
                        'url': href,
                        'text': text,
                        'score': self._score_pdf_link(href, text, ticker)
                    })
            
            # Pattern 2: Look for download buttons or earnings-specific links
            for element in soup.find_all(['button', 'div', 'span'], class_=True):
                class_str = ' '.join(element.get('class', []))
                text = element.get_text(strip=True)
                
                if any(keyword in class_str.lower() or keyword in text.lower() 
                       for keyword in ['download', 'pdf', 'earnings', 'financial-report']):
                    # Look for nearby links
                    for link in element.find_all('a', href=True) + element.parent.find_all('a', href=True):
                        href = link.get('href', '')
                        if '.pdf' in href.lower():
                            if href.startswith('/'):
                                href = urljoin(url, href)
                            pdf_links.append({
                                'url': href,
                                'text': text or link.get_text(strip=True),
                                'score': self._score_pdf_link(href, text, ticker)
                            })
            
            if not pdf_links:
                log.warning(f"No PDF links found in HTML page for {ticker}")
                return False
            
            # Sort by score and try to download the best PDF
            pdf_links.sort(key=lambda x: x['score'], reverse=True)
            
            for pdf_link in pdf_links[:3]:  # Try top 3 candidates
                log.info(f"Trying PDF: {pdf_link['text'][:50]} -> {pdf_link['url']}")
                
                try:
                    # Download the PDF
                    pdf_response = requests.get(pdf_link['url'], headers=headers, timeout=60, stream=True)
                    if pdf_response.status_code == 200:
                        with open(filepath, 'wb') as f:
                            for chunk in pdf_response.iter_content(chunk_size=8192):
                                if chunk:
                                    f.write(chunk)
                        
                        # Validate the downloaded file
                        if not self._validate_downloaded_file(filepath, ticker):
                            log.info(f"✅ Successfully extracted and downloaded PDF for {ticker}")
                            return True
                        else:
                            log.warning(f"Extracted PDF failed validation for {ticker}")
                            if os.path.exists(filepath):
                                os.remove(filepath)
                
                except Exception as pdf_error:
                    log.warning(f"Failed to download extracted PDF: {pdf_error}")
                    continue
            
            return False
            
        except ImportError:
            log.error("BeautifulSoup not available - install with: pip install beautifulsoup4")
            return False
        except Exception as e:
            log.error(f"HTML extraction failed for {ticker}: {e}")
            return False

    def _score_pdf_link(self, href, text, ticker):
        """Score PDF link relevance for selection"""
        score = 0
        href_lower = href.lower()
        text_lower = text.lower()
        ticker_lower = ticker.lower()
        
        # High priority: Company-specific patterns
        if ticker_lower in href_lower:
            score += 10
        
        # High priority: Earnings/financial terms
        earnings_terms = ['earnings', 'financial', 'quarterly', 'q1', 'q2', 'q3', 'q4', '10-q', 'results']
        for term in earnings_terms:
            if term in href_lower or term in text_lower:
                score += 5
        
        # Medium priority: Year relevance  
        if '2025' in href_lower or '2025' in text_lower:
            score += 3
        elif '2024' in href_lower or '2024' in text_lower:
            score += 1
        
        # Bonus for trusted domains
        trusted_domains = ['q4cdn', 'sec.gov', 'investor', 'ir.']
        for domain in trusted_domains:
            if domain in href_lower:
                score += 2
        
        # Penalty for generic terms
        generic_terms = ['download', 'click', 'here', 'view']
        for term in generic_terms:
            if term in text_lower:
                score -= 1
                
        return score
    
    def _select_best_pdf_link(self, pdf_links, ticker):
        """Select the best PDF link from multiple options"""
        if not pdf_links:
            return None
        
        # Filter out non-company PDFs first
        company_pdfs = []
        ticker_lower = ticker.lower()
        
        for link in pdf_links:
            url = link['url'].lower()
            text = link['text'].lower()
            
            # EXCLUDE third-party PDFs (S&P Global, Moody's, etc.)
            exclude_domains = [
                'spglobal.com', 'moodys.com', 'fitchratings.com', 
                'bloomberg.com', 'reuters.com', 'morningstar.com',
                'sec.gov'  # Exclude direct SEC filings for now
            ]
            
            # Skip if it's from an excluded domain
            if any(domain in url for domain in exclude_domains):
                log.debug(f"Skipping third-party PDF: {url}")
                continue
            
            # PREFER company's own domain or investor relations domains
            company_indicators = [
                ticker_lower, 'investor', 'earnings', 'q4cdn', 'ir.',
                'financial', 'quarterly', 'amgen' if ticker == 'AMGN' else ticker_lower
            ]
            
            # Must contain company or earnings indicators
            if any(indicator in url or indicator in text for indicator in company_indicators):
                company_pdfs.append(link)
        
        if not company_pdfs:
            log.warning("No company-specific PDFs found, using all PDFs")
            company_pdfs = pdf_links
        
        if len(company_pdfs) == 1:
            return company_pdfs[0]
        
        # Scoring criteria for company PDFs
        scored_links = []
        for link in company_pdfs:
            score = 0
            text = link['text'].lower()
            url = link['url'].lower()
            
            # HIGHEST PRIORITY: Company's own earnings documents
            if any(word in url for word in [ticker_lower, 'investor', 'earnings']):
                score += 3
            
            # HIGH PRIORITY: Earnings/financial content
            if any(word in text or word in url for word in ['earnings', 'financial', 'report', '10-q']):
                score += 2
            
            # MEDIUM PRIORITY: Presentation/press release
            if any(word in text or word in url for word in ['presentation', 'press release']):
                score += 1
            
            # Prefer recent years
            if '2025' in text or '2025' in url:
                score += 2
            elif '2024' in text or '2024' in url:
                score += 1
            
            # Prefer quarterly indicators
            if any(q in text or q in url for q in ['q1', 'q2', 'q3', 'q4', 'quarterly']):
                score += 1
            
            # Bonus for company-specific domains (like q4cdn which hosts investor docs)
            if any(domain in url for domain in ['q4cdn', 'ir.', 'investors.']):
                score += 1
            
            # PENALTY for overly generic terms
            if any(generic in text for generic in ['download', 'view', 'click here']):
                score -= 1
            
            scored_links.append((score, link))
            log.info(f"PDF candidate: {text[:50]} (score: {score}) -> {url}")
        
        if not scored_links:
            log.warning("No scored PDFs found")
            return None
        
        # Sort by score and return the best
        scored_links.sort(key=lambda x: x[0], reverse=True)
        best_link = scored_links[0][1]
        log.info(f"Selected best PDF (score: {scored_links[0][0]}): {best_link['text']} -> {best_link['url']}")
        return best_link
    
    def _download_direct_file(self, url, filepath, ticker):
        """Download a file directly from URL with enhanced validation"""
        try:
            # Download the file with proper headers
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'application/pdf,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/vnd.ms-excel,*/*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            }
            
            response = requests.get(url, timeout=60, headers=headers, stream=True)
            response.raise_for_status()
            
            # More lenient content-type checking - only reject obvious HTML
            content_type = response.headers.get('content-type', '').lower()
            
            # Check first few bytes to detect content type
            first_chunk = next(response.iter_content(chunk_size=2048), b'')
            
            # Only reject if it's clearly HTML and not a PDF wrapper
            if (first_chunk.startswith(b'<!DOCTYPE') or 
                first_chunk.startswith(b'<html') or
                (b'<html' in first_chunk[:500] and b'%PDF' not in first_chunk[:100])):
                
                # BUT allow if it might redirect to PDF or be a PDF viewer page
                if any(term in url.lower() for term in ['.pdf', 'download', 'filing', 'doc_financials']):
                    log.info(f"Potential PDF URL despite HTML content, proceeding: {ticker}")
                else:
                    log.warning(f"Detected HTML content (not PDF) for {ticker}")
                    return False
            
            # Save file
            with open(filepath, 'wb') as f:
                f.write(first_chunk)  # Write the first chunk we already read
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            # Verify file was downloaded and is valid
            if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                file_size = os.path.getsize(filepath)
                file_size_mb = file_size / 1024 / 1024  # MB
                
                # Enhanced corruption detection
                is_corrupted = self._validate_downloaded_file(filepath, ticker)
                if is_corrupted:
                    log.error(f"Downloaded file appears corrupted: {os.path.basename(filepath)} ({file_size} bytes)")
                    os.remove(filepath)  # Remove corrupted file
                    return False
                
                log.info(f"Successfully downloaded: {os.path.basename(filepath)} ({file_size_mb:.2f} MB)")
                return True
            else:
                log.error(f"Downloaded file is empty or missing: {os.path.basename(filepath)}")
                return False
            
        except requests.exceptions.RequestException as e:
            log.error(f"Download failed for {ticker}: {str(e)}")
            return False
        except Exception as e:
            log.error(f"Unexpected error downloading {ticker}: {str(e)}")
            return False
    
    def _validate_downloaded_file(self, filepath, ticker):
        """Strict validation to reject HTML pages and corrupted files"""
        try:
            file_size = os.path.getsize(filepath)
            
            # Rule 1: Size validation - reject files that are too small or too large
            if file_size < 10000:  # Less than 10KB is likely corrupted 
                log.warning(f"File too small ({file_size} bytes) for financial report: {ticker}")
                return True  # True means corrupted
            
            # Rule 2: Strict PDF validation - reject HTML pages disguised as PDFs
            if filepath.lower().endswith('.pdf'):
                with open(filepath, 'rb') as f:
                    header = f.read(10)
                    # Must start with PDF magic number
                    if not header.startswith(b'%PDF-'):
                        log.warning(f"Invalid PDF header for {ticker}: {header}")
                        return True  # Corrupted
                    
                    # Check for HTML content disguised as PDF
                    f.seek(0)
                    content = f.read(min(file_size, 2048)).decode('utf-8', errors='ignore')
                    
                    # Reject HTML pages, error pages, and non-PDF content
                    html_indicators = ['<!doctype', '<html>', '<body>', '<head>', '</html>', '<div', '<script']
                    error_indicators = ['404', 'not found', 'access denied', 'forbidden', 'error', 'page not found']
                    
                    content_lower = content.lower()
                    
                    # Check for HTML content
                    for html_tag in html_indicators:
                        if html_tag in content_lower:
                            log.warning(f"HTML content detected in PDF for {ticker}: found '{html_tag}'")
                            return True  # Corrupted
                    
                    # Check for error pages
                    for error in error_indicators:
                        if error in content_lower:
                            log.warning(f"Error page detected for {ticker}: found '{error}'")
                            return True  # Corrupted
            
            # Rule 3: Excel file validation
            elif filepath.lower().endswith(('.xlsx', '.xls')):
                with open(filepath, 'rb') as f:
                    header = f.read(8)
                    # Check for ZIP signature (XLSX) or OLE signature (XLS)
                    if not (header.startswith(b'PK\x03\x04') or header.startswith(b'\xd0\xcf\x11\xe0')):
                        log.warning(f"Invalid Excel header for {ticker}: {header}")
                        return True  # Corrupted
            
            # Rule 4: Minimum file size requirements for actual financial reports
            min_sizes = {
                '.pdf': 50000,    # 50KB minimum for real financial PDFs
                '.xlsx': 10000,   # 10KB minimum for Excel files
                '.xls': 10000     # 10KB minimum for Excel files
            }
            
            file_ext = next((ext for ext in min_sizes.keys() if filepath.lower().endswith(ext)), '.pdf')
            min_size = min_sizes[file_ext]
            
            if file_size < min_size:
                log.warning(f"File smaller than minimum for {file_ext} ({file_size} < {min_size} bytes): {ticker}")
                return True  # Corrupted
                
            # If file is reasonably sized, accept it
            log.info(f"File validation passed for {ticker}: {file_size} bytes")
                
            return False  # Not corrupted
            
        except Exception as e:
            log.error(f"Error validating file for {ticker}: {e}")
            return True  # Assume corrupted if we can't validate
    
    def download_reports(self, reports_dict):
        """Download reports for multiple companies"""
        successful_downloads = []
        failed_downloads = []
        
        log.info(f"Starting download of reports for {len(reports_dict)} companies")
        
        for ticker, reports in reports_dict.items():
            if not reports:
                log.warning(f"No reports found for {ticker}")
                failed_downloads.append(ticker)
                continue
            
            # Download the first (best) report for each company
            report = reports[0] if isinstance(reports, list) else reports
            
            if self.download_report(report, ticker):
                successful_downloads.append(ticker)
            else:
                failed_downloads.append(ticker)
        
        # Print summary
        self._print_download_summary(successful_downloads, failed_downloads)
        
        return {
            'successful': successful_downloads,
            'failed': failed_downloads,
            'total_success': len(successful_downloads),
            'total_failed': len(failed_downloads)
        }
    
    def _print_download_summary(self, successful, failed):
        """Print download summary"""
        log.info("="*60)
        log.info("DOWNLOAD SUMMARY")
        log.info("="*60)
        log.info(f"Successful downloads: {len(successful)}")
        log.info(f"Failed downloads: {len(failed)}")
        
        if successful:
            log.info("Successfully downloaded:")
            for ticker in successful:
                log.info(f"  ✅ {ticker}")
        
        if failed:
            log.info("Failed to download:")
            for ticker in failed:
                log.info(f"  ❌ {ticker}")
        
        # Check output directory
        if os.path.exists(self.output_dir):
            files = [f for f in os.listdir(self.output_dir) if f.endswith(('.pdf', '.xlsx', '.xls'))]
            total_size = sum(os.path.getsize(os.path.join(self.output_dir, f)) for f in files) / 1024 / 1024
            log.info(f"Total files in output directory: {len(files)} ({total_size:.2f} MB)")
    
    def save_download_results(self, results):
        """Save download results to JSON file"""
        import json
        
        results['timestamp'] = datetime.now().isoformat()
        results['output_directory'] = self.output_dir
        
        results_file = os.path.join(self.output_dir, 'download_results.json')
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        log.info(f"Download results saved to: {results_file}")
        return results_file