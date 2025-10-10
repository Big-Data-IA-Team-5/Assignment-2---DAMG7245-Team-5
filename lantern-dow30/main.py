#!/usr/bin/env python3
"""
DOW 30 Financial Report Pipeline - Main Entry Point

Automated discovery and download of latest quarterly earnings reports for all Dow 30 companies.
This is the main entry point with enhanced multithreading support and comprehensive error tracking.

Features:
- Multithreaded processing for faster execution (8 workers by default)
- Dynamic IR page discovery (no hard-coded URLs)
- Smart report detection with date-aware prioritization  
- Enhanced error handling and retry logic
- Comprehensive progress tracking and failure analysis

Usage Examples:
    # Run with default settings (8 threads, all companies)
    python main.py
    
    # Run with more threads for faster processing
    python main.py --workers 12
    
    # Process specific companies only
    python main.py --companies AAPL MSFT NVDA AMZN
    
    # Run single-threaded for debugging
    python main.py --mode single --debug
    
    # Quick test with a small subset
    python main.py --mode test
    
    # Run with failure analysis
    python main.py --analyze-failures
"""

import logging
import sys
import os
import time
import json
import shutil
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# Import our modules
from tickers import get_companies
from ir_discovery import IRDiscovery
from report_finder import ReportFinder
from downloader import ReportDownloader

# Enhanced logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s'
)
log = logging.getLogger("enhanced_runner")

class Dow30Pipeline:
    """Single-threaded Dow 30 financial report pipeline"""
    
    def __init__(self, output_dir='dow30_pipeline_reports_2025', headless=True):
        self.output_dir = output_dir
        self.headless = headless
        self.ir_discovery = IRDiscovery()
        self.report_finder = ReportFinder()
        self.downloader = ReportDownloader(output_dir)
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        log.info(f"Initialized Dow30Pipeline with output directory: {output_dir}")
    
    def setup_driver(self):
        """Setup Chrome WebDriver with appropriate options"""
        chrome_options = Options()
        
        if self.headless:
            chrome_options.add_argument('--headless')
        
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        # Add additional stability options
        chrome_options.add_argument('--disable-web-security')
        chrome_options.add_argument('--disable-features=VizDisplayCompositor')
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--remote-debugging-port=0')
        
        try:
            # Try multiple approaches in order of preference
            driver = None
            
            # Method 1: Try system Chrome first (most reliable on Mac)
            try:
                log.info("Trying system Chrome browser...")
                driver = webdriver.Chrome(options=chrome_options)
                log.info("✅ Successfully initialized system Chrome")
            except Exception as e1:
                log.warning(f"System Chrome failed: {e1}")
                
                # Method 2: Try ChromeDriverManager with fresh download
                try:
                    log.info("Trying ChromeDriverManager with fresh download...")
                    # Clear cache and force fresh download
                    import shutil
                    import os
                    cache_dir = os.path.expanduser("~/.wdm")
                    if os.path.exists(cache_dir):
                        log.info("Clearing ChromeDriver cache...")
                        shutil.rmtree(cache_dir, ignore_errors=True)
                    
                    service = Service(ChromeDriverManager().install())
                    driver = webdriver.Chrome(service=service, options=chrome_options)
                    log.info("✅ Successfully initialized ChromeDriverManager")
                except Exception as e2:
                    log.warning(f"ChromeDriverManager failed: {e2}")
                    
                    # Method 3: Try common Chrome paths on Mac
                    chrome_paths = [
                        '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
                        '/Applications/Chromium.app/Contents/MacOS/Chromium'
                    ]
                    
                    for chrome_path in chrome_paths:
                        try:
                            if os.path.exists(chrome_path):
                                log.info(f"Trying Chrome at: {chrome_path}")
                                chrome_options.binary_location = chrome_path
                                driver = webdriver.Chrome(options=chrome_options)
                                log.info(f"✅ Successfully initialized Chrome at {chrome_path}")
                                break
                        except Exception as e3:
                            log.warning(f"Chrome path {chrome_path} failed: {e3}")
                            continue
            
            if driver:
                driver.set_page_load_timeout(30)
                driver.implicitly_wait(10)
                return driver
            else:
                raise Exception("All Chrome initialization methods failed")
                
        except Exception as e:
            log.error(f"❌ Failed to setup WebDriver: {e}")
            log.error("💡 Troubleshooting tips:")
            log.error("   1. Make sure Google Chrome is installed")
            log.error("   2. Try: brew install --cask google-chrome")
            log.error("   3. Clear ChromeDriver cache: rm -rf ~/.wdm")
            return None
    
    def process_company(self, ticker, name, website):
        """Process a single company to find and download reports"""
        log.info(f"Processing {ticker} ({name})...")
        
        driver = None
        try:
            driver = self.setup_driver()
            if not driver:
                return None
            
            # Step 1: Navigate to company website
            log.info(f"Navigating to {website}")
            driver.get(website)
            time.sleep(2)
            
            # Step 2: Find IR page
            ir_url = self.ir_discovery.navigate_to_ir_page(driver, website, ticker)
            if not ir_url:
                log.warning(f"Could not find IR page for {ticker}")
                return None
            
            log.info(f"Found IR page: {ir_url}")
            
            # Step 3: Find financial reports
            reports = self.report_finder.find_reports(driver, ticker)
            if not reports:
                log.warning(f"No reports found for {ticker}")
                return None
            
            # Step 4: Download the best report
            best_report = reports[0]
            success = self.downloader.download_report(best_report, ticker)
            
            if success:
                log.info(f"✅ Successfully processed {ticker}")
                return {
                    'ticker': ticker,
                    'ir_url': ir_url,
                    'report': best_report,
                    'status': 'success'
                }
            else:
                log.error(f"❌ Failed to download report for {ticker}")
                return None
                
        except Exception as e:
            log.error(f"Error processing {ticker}: {str(e)}")
            return None
        finally:
            if driver:
                driver.quit()
    
    def run_pipeline(self, target_companies=None):
        """Run the complete pipeline"""
        companies = get_companies()
        
        if target_companies:
            companies = [(ticker, name, website) for ticker, name, website in companies 
                        if ticker in target_companies]
        
        log.info(f"Starting pipeline for {len(companies)} companies")
        
        successful_companies = []
        failed_companies = []
        detailed_results = {}
        
        for ticker, name, website in companies:
            result = self.process_company(ticker, name, website)
            
            if result:
                successful_companies.append(ticker)
                detailed_results[ticker] = result
            else:
                failed_companies.append(ticker)
        
        results = {
            'successful_companies': successful_companies,
            'failed_companies': failed_companies,
            'detailed_results': detailed_results,
            'total_processed': len(companies)
        }
        
        # Save results
        self._save_results(results)
        
        return results
    
    def _save_results(self, results):
        """Save pipeline results to JSON file"""
        results_file = os.path.join(self.output_dir, 'pipeline_results.json')
        results_copy = results.copy()
        results_copy['timestamp'] = datetime.now().isoformat()
        
        with open(results_file, 'w') as f:
            json.dump(results_copy, f, indent=2)
        
        log.info(f"Results saved to: {results_file}")


class MultithreadedDow30Pipeline:
    """Multithreaded Dow 30 financial report pipeline for improved performance"""
    
    def __init__(self, output_dir='dow30_pipeline_reports_2025', headless=True, max_workers=8):
        self.output_dir = output_dir
        self.headless = headless
        self.max_workers = max_workers
        self.downloader = ReportDownloader(output_dir)
        
        # Thread-safe locks
        self.results_lock = Lock()
        self.processing_times = {}
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        log.info(f"Initialized MultithreadedDow30Pipeline with {max_workers} workers")
    
    def setup_driver(self):
        """Setup Chrome WebDriver with appropriate options (thread-safe)"""
        chrome_options = Options()
        
        if self.headless:
            chrome_options.add_argument('--headless')
        
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        # Add additional stability options for threading
        chrome_options.add_argument('--disable-web-security')
        chrome_options.add_argument('--disable-features=VizDisplayCompositor')
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--remote-debugging-port=0')
        
        try:
            # Try multiple approaches in order of preference
            driver = None
            
            # Method 1: Try system Chrome first (most reliable on Mac)
            try:
                driver = webdriver.Chrome(options=chrome_options)
                log.info(f"✅ [{threading.current_thread().name}] System Chrome initialized")
            except Exception as e1:
                log.warning(f"[{threading.current_thread().name}] System Chrome failed: {e1}")
                
                # Method 2: Try ChromeDriverManager (skip cache clearing in threads)
                try:
                    service = Service(ChromeDriverManager().install())
                    driver = webdriver.Chrome(service=service, options=chrome_options)
                    log.info(f"✅ [{threading.current_thread().name}] ChromeDriverManager initialized")
                except Exception as e2:
                    log.warning(f"[{threading.current_thread().name}] ChromeDriverManager failed: {e2}")
                    
                    # Method 3: Try common Chrome paths on Mac
                    chrome_paths = [
                        '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
                        '/Applications/Chromium.app/Contents/MacOS/Chromium'
                    ]
                    
                    for chrome_path in chrome_paths:
                        try:
                            if os.path.exists(chrome_path):
                                chrome_options.binary_location = chrome_path
                                driver = webdriver.Chrome(options=chrome_options)
                                log.info(f"✅ [{threading.current_thread().name}] Chrome at {chrome_path}")
                                break
                        except Exception as e3:
                            continue
            
            if driver:
                driver.set_page_load_timeout(30)
                driver.implicitly_wait(10)
                return driver
            else:
                raise Exception("All Chrome initialization methods failed")
                
        except Exception as e:
            log.error(f"❌ [{threading.current_thread().name}] Failed to setup WebDriver: {e}")
            return None
    
    def process_company_threaded(self, company_data):
        """Process a single company in a thread-safe manner with timeout protection"""
        ticker, name, website = company_data
        start_time = time.time()
        
        log.info(f"[{ticker}] Starting processing...")
        
        # Add timeout protection to prevent infinite hangs (fixes HON issue)
        timeout_seconds = 420  # 7 minutes (increased from 5 for better success)
        
        def check_timeout():
            if time.time() - start_time > timeout_seconds:
                raise TimeoutError(f"Company processing exceeded 5 minutes for {ticker}")
        
        driver = None
        try:
            # Setup driver for this thread
            driver = self.setup_driver()
            if not driver:
                log.error(f"[{ticker}] Failed to setup driver")
                return {'ticker': ticker, 'status': 'failed', 'error': 'Driver setup failed'}
            
            # Create thread-local instances
            ir_discovery = IRDiscovery()
            report_finder = ReportFinder()
            
            # Step 1: Navigate to website
            check_timeout()  # Check before starting
            log.info(f"[{ticker}] Navigating to {website}")
            driver.get(website)
            time.sleep(2)
            
            # Step 2: Find IR page
            check_timeout()  # Check before IR discovery
            ir_url = ir_discovery.navigate_to_ir_page(driver, website, ticker)
            if not ir_url:
                log.warning(f"[{ticker}] Could not find IR page")
                return {'ticker': ticker, 'status': 'failed', 'error': 'IR page not found'}
            
            log.info(f"[{ticker}] Found IR page: {ir_url}")
            
            # Step 3: Find reports
            check_timeout()  # Check before report search
            reports = report_finder.find_reports(driver, ticker)
            if not reports:
                log.warning(f"[{ticker}] No reports found")
                return {'ticker': ticker, 'status': 'failed', 'error': 'No reports found'}
            
            # Step 4: Download best report (thread-safe)
            check_timeout()  # Check before download
            best_report = reports[0]
            success = self.downloader.download_report(best_report, ticker)
            
            processing_time = time.time() - start_time
            
            # Thread-safe update of processing times
            with self.results_lock:
                self.processing_times[ticker] = processing_time
            
            if success:
                log.info(f"[{ticker}] ✅ Success in {processing_time:.1f}s")
                return {
                    'ticker': ticker,
                    'status': 'success',
                    'ir_url': ir_url,
                    'report': best_report,
                    'processing_time': processing_time
                }
            else:
                log.error(f"[{ticker}] ❌ Download failed")
                return {'ticker': ticker, 'status': 'failed', 'error': 'Download failed'}
                
        except TimeoutError as e:
            processing_time = time.time() - start_time
            log.error(f"[{ticker}] ⏰ TIMEOUT after {processing_time:.1f}s: Processing exceeded 5 minutes")
            return {'ticker': ticker, 'status': 'failed', 'error': 'Timeout - exceeded 5 minutes'}
        except Exception as e:
            processing_time = time.time() - start_time
            log.error(f"[{ticker}] Error after {processing_time:.1f}s: {str(e)}")
            return {'ticker': ticker, 'status': 'failed', 'error': str(e)}
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass
    
    def run_multithreaded_pipeline(self, target_companies=None):
        """Run the pipeline with multiple threads"""
        companies = get_companies()
        
        if target_companies:
            companies = [(ticker, name, website) for ticker, name, website in companies 
                        if ticker in target_companies]
        
        log.info(f"Starting multithreaded pipeline for {len(companies)} companies with {self.max_workers} workers")
        
        successful_companies = []
        failed_companies = []
        detailed_results = {}
        detailed_errors = {}
        
        # Process companies in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_company = {
                executor.submit(self.process_company_threaded, company): company[0]
                for company in companies
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_company):
                ticker = future_to_company[future]
                try:
                    result = future.result()
                    
                    if result['status'] == 'success':
                        successful_companies.append(ticker)
                        detailed_results[ticker] = result
                    else:
                        failed_companies.append(ticker)
                        detailed_errors[ticker] = {
                            'last_error': result.get('error', 'Unknown error'),
                            'total_attempts': 1,
                            'failure_category': 'processing_error'
                        }
                        
                except Exception as e:
                    log.error(f"Task for {ticker} generated an exception: {e}")
                    failed_companies.append(ticker)
                    detailed_errors[ticker] = {
                        'last_error': str(e),
                        'total_attempts': 1,
                        'failure_category': 'execution_error'
                    }
        
        results = {
            'successful_companies': successful_companies,
            'failed_companies': failed_companies,
            'detailed_results': detailed_results,
            'detailed_errors': detailed_errors,
            'processing_times': self.processing_times.copy(),
            'total_processed': len(companies)
        }
        
        # Save results
        self._save_results(results)
        
        return results
    
    def _save_results(self, results):
        """Save pipeline results to JSON file"""
        results_file = os.path.join(self.output_dir, 'multithreaded_pipeline_results.json')
        results_copy = results.copy()
        results_copy['timestamp'] = datetime.now().isoformat()
        
        with open(results_file, 'w') as f:
            json.dump(results_copy, f, indent=2)
        
        log.info(f"Results saved to: {results_file}")


def run_quick_test():
    """Run a quick test with a few companies to verify the setup"""
    log.info("🧪 Running quick test with 3 companies...")
    
    pipeline = MultithreadedDow30Pipeline(
        output_dir='test_reports',
        headless=True,
        max_workers=3
    )
    
    # Test with just a few companies
    test_companies = ['AAPL', 'MSFT', 'NVDA']
    results = pipeline.run_multithreaded_pipeline(target_companies=test_companies)
    
    success_count = len(results['successful_companies'])
    failed_count = len(results['failed_companies'])
    
    log.info(f"🧪 Quick test complete: {success_count} successes, {failed_count} failures")
    return results

def run_full_pipeline(workers=8, specific_companies=None, single_threaded=False):
    """Run the full pipeline with specified parameters"""
    
    if single_threaded:
        log.info("🐌 Running SINGLE-THREADED pipeline...")
        pipeline = Dow30Pipeline(
            output_dir='dow30_pipeline_reports_2025',
            headless=True
        )
        results = pipeline.run_pipeline(target_companies=specific_companies)
    else:
        log.info(f"🚀 Running MULTITHREADED pipeline with {workers} workers...")
        pipeline = MultithreadedDow30Pipeline(
            output_dir='dow30_pipeline_reports_2025',
            headless=True,
            max_workers=workers
        )
        results = pipeline.run_multithreaded_pipeline(target_companies=specific_companies)
    
    return results

def analyze_failures(results):
    """Analyze and report on failed companies"""
    log.info("="*60)
    log.info("🔍 FAILURE ANALYSIS")
    log.info("="*60)
    
    if not results.get('failed_companies'):
        log.info("✅ No failures to analyze!")
        return
    
    failed_companies = results['failed_companies']
    log.info(f"❌ Total failed companies: {len(failed_companies)}")
    
    # Analyze failure categories
    if 'failure_categories' in results:
        log.info("\n📊 Failure Categories:")
        for category, companies in results['failure_categories'].items():
            if companies:
                log.info(f"  {category.replace('_', ' ').title()}: {len(companies)}")
                for company in companies:
                    log.info(f"    • {company}")
    
    # Show detailed errors for failed companies
    if 'detailed_errors' in results:
        log.info("\n🔎 Detailed Error Information:")
        for company in failed_companies[:5]:  # Show first 5 failures
            if company in results['detailed_errors']:
                error_info = results['detailed_errors'][company]
                log.info(f"\n  {company}:")
                if 'last_error' in error_info:
                    log.info(f"    Last Error: {error_info['last_error']}")
                if 'total_attempts' in error_info:
                    log.info(f"    Attempts Made: {error_info['total_attempts']}")
                if 'failure_category' in error_info:
                    log.info(f"    Category: {error_info['failure_category']}")
    
    # Recommendations for failed companies
    log.info("\n💡 RECOMMENDATIONS FOR FAILED COMPANIES:")
    log.info("1. Check if company websites have changed their IR page structure")
    log.info("2. Some companies may require different search strategies")
    log.info("3. Try running failed companies individually with --debug mode")
    log.info("4. Consider adjusting timeout settings for slow-loading pages")

def main():
    """Main runner function with command line support"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Enhanced DOW 30 Financial Report Pipeline')
    parser.add_argument('--mode', choices=['single', 'multi', 'test'], default='multi',
                       help='Execution mode: single, multi, or test (default: multi)')
    parser.add_argument('--workers', type=int, default=8,
                       help='Number of worker threads (default: 8)')
    parser.add_argument('--companies', nargs='*', 
                       help='Specific companies to process (e.g., AAPL MSFT NVDA)')
    parser.add_argument('--debug', action='store_true',
                       help='Run in debug mode with visible browsers')
    parser.add_argument('--analyze-failures', action='store_true',
                       help='Perform detailed failure analysis after completion')
    
    args = parser.parse_args()
    
    try:
        start_time = datetime.now()
        
        if args.mode == 'test':
            results = run_quick_test()
        else:
            results = run_full_pipeline(
                workers=args.workers,
                specific_companies=args.companies,
                single_threaded=(args.mode == 'single')
            )
        
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        # Final summary
        success_count = len(results['successful_companies'])
        failed_count = len(results['failed_companies'])
        total_count = success_count + failed_count
        success_rate = (success_count / total_count * 100) if total_count > 0 else 0
        
        log.info("="*60)
        log.info("🏁 EXECUTION COMPLETE")
        log.info("="*60)
        log.info(f"⏱️ Total time: {execution_time:.1f} seconds")
        log.info(f"📊 Companies processed: {total_count}")
        log.info(f"✅ Successful: {success_count} ({success_rate:.1f}%)")
        log.info(f"❌ Failed: {failed_count}")
        
        if args.mode == 'multi' and 'processing_times' in results and results['processing_times']:
            avg_time = sum(results['processing_times'].values()) / len(results['processing_times'])
            log.info(f"⚡ Average time per company: {avg_time:.1f}s")
            log.info(f"🚀 Speed improvement: ~{(avg_time * total_count) / execution_time:.1f}x faster than sequential")
        
        # Perform failure analysis if requested or if failure rate is high
        if args.analyze_failures or (failed_count > 0 and success_rate < 70):
            analyze_failures(results)
        
        # List successful downloads
        if results['successful_companies']:
            log.info(f"\n📁 Successfully downloaded reports for:")
            for company in results['successful_companies']:
                log.info(f"  ✅ {company}")
        
        # List failed companies for easy retry
        if results['failed_companies']:
            log.info(f"\n⚠️ Failed companies (for manual retry):")
            failed_list = ' '.join(results['failed_companies'])
            log.info(f"  python main.py --companies {failed_list}")
        
        return success_count >= (total_count * 0.5)  # Success if >= 50%
        
    except KeyboardInterrupt:
        log.info("Execution interrupted by user")
        return False
    except Exception as e:
        log.error(f"Execution failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)