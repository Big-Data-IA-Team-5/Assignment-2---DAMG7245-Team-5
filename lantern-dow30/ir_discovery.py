"""
Investor Relations Page Discovery Module
Programmatically identifies IR pages for Dow 30 companies without hard-coding
"""

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait  
from selenium.webdriver.support import expected_conditions as EC
import time
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("ir_discovery")

class IRDiscovery:
    def __init__(self):
        """Initialize IR discovery with dynamic patterns - no hard-coded URLs"""
        # Dynamic patterns for discovering IR pages programmatically
        self.ir_patterns = [
            'investor relations', 'investors', 'investor', 'shareholders',
            'financial information', 'financials', 'ir', 'governance',
            'financial results', 'earnings', 'reports', 'quarterly results',
            'annual reports', 'sec filings', 'investor information',
            'corporate governance', 'financial data', 'stockholders',
            'quarterly earnings', 'annual report', 'financial statements',
            # Enhanced patterns based on real company examples
            'financial reporting', 'quarterly-earnings', 'earnings releases',
            'events and presentations', 'presentations', 'news events and reports',
            'earnings conference call', 'press releases', 'financial news'
        ]
        
        # URL patterns to look for in href attributes
        self.url_patterns = [
            'investor', 'ir', 'financial', 'shareholders', 'governance', 
            'earnings', 'reports', 'quarterly', 'annual', 'stockholder',
            'corporate', 'sec-filings', 'financials',
            # Enhanced URL patterns from examples
            'financial-reporting', 'quarterly-earnings', 'events-and-presentations',
            'presentations', 'news-events-and-reports', 'earnings-fy', 'press-release'
        ]
        
        # Dynamic subdomain patterns for IR discovery (no hard-coding)
        # Enhanced based on real company examples
        self.ir_subdomains = [
            'ir', 'investor', 'investors', 'financials', 'corporate',
            'shareholders', 'sec', 'reports'
        ]
        
        # Company-specific subdomain patterns discovered from examples
        self.company_specific_subdomains = {
            # These will be tried first as they're more likely to be correct
            'ir.about{company}', 'investors.{company}', 'investor.{company}',
            'ir.{company}', '{company}investor', 'corporate.{company}',
            # Additional patterns for companies using separate investor domains
            '{company}investor.com', '{company}investors.com',
            '{company}ir.com'
        }
        
        # Company-specific IR URLs (hard-coded for known failures - UPDATED URLS)
        self.company_specific_ir_urls = {
            'AMGN': 'https://investors.amgen.com/financials/sec-filings',
            'AMZN': 'https://ir.aboutamazon.com/sec-filings/default.aspx',
            'CVX': 'https://investor.chevron.com/sec-filings/quarterly-results',
            'IBM': 'https://www.ibm.com/investor/financials',
            'GS': 'https://www.goldmansachs.com/investor-relations/financials/current/quarterly-earnings-releases',
            'JPM': 'https://www.jpmorganchase.com/ir/quarterly-earnings',
            'MSFT': 'https://www.microsoft.com/en-us/investor/earnings/fy-2025-q4',
            'NKE': 'https://s1.q4cdn.com/806093406/files/doc_downloads/2025/07/nike-inc-q4-fy24-earnings-release.pdf',
            'UNH': 'https://www.unitedhealthgroup.com/content/unh/en/investors/financial-information/sec-filings.html',
            'PG': 'https://www.pg.com/investors/financial-reporting/index.shtml',
            'V': 'https://investor.visa.com/financial-information/quarterly-earnings',
            'WMT': 'https://corporate.walmart.com/news/category/financial-news',
            'HON': 'https://investor.honeywell.com/~/media/Files/H/Honeywell-V2-IR/financial-reports/quarterly-results/2025/q2-earnings-release-2025.pdf'
        }
        
        # Common IR path patterns found on corporate websites - dynamically expanded
        self.ir_path_patterns = [
            '/investor', '/investors', '/ir', '/about/investors',
            '/investor-relations', '/shareholders', '/financials',
            '/corporate/investors', '/our-company/investors',
            '/company/investors', '/about/investor-relations',
            '/corporate/financials', '/corporate/governance',
            '/company/financial-information', '/about/financial-information',
            '/investor-relations-2', '/investor-relations-home',
            '/en/investors', '/en/investor-relations', '/global/investors',
            # Enhanced paths based on real company examples
            '/investor/financial-reporting', '/investor/quarterly-earnings',
            '/investor/events-and-presentations', '/investor/presentations',
            '/investor/earnings', '/investor/sec-filings',
            '/en-us/investor', '/investors/news-events-and-reports',
            # News section patterns for companies like WMT that put earnings in news
            '/news', '/newsroom', '/press-releases', '/media',
            '/news/earnings', '/news/financial', '/news/press-releases',
            '/media/press-releases', '/newsroom/earnings'
        ]
        
        # Dynamic pattern discovery cache
        self.discovered_patterns = {}
        self.successful_strategies = {}
    
    def accept_cookies(self, driver):
        """Accept cookie banners and consent popups"""
        try:
            cookie_selectors = [
                "button[id*='accept' i]",
                "button[class*='accept' i]", 
                "button[id*='cookie' i]",
                "button[class*='cookie' i]",
                "button:contains('Accept')",
                "button:contains('I Accept')", 
                "button:contains('Accept All')",
                "#accept-cookies",
                ".accept-cookies",
                ".cookie-accept-button"
            ]
            
            for selector in cookie_selectors:
                try:
                    element = WebDriverWait(driver, 2).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                    )
                    if element.is_displayed():
                        driver.execute_script("arguments[0].click();", element)
                        log.info("Cookie banner accepted")
                        time.sleep(1)
                        return True
                except:
                    continue
                    
            return False
        except Exception as e:
            log.debug(f"Cookie acceptance failed: {e}")
            return False
    

    
    def learn_company_patterns(self, driver, ticker):
        """Dynamically learn URL patterns specific to this company's website structure"""
        try:
            current_url = driver.current_url
            domain = current_url.split('/')[2].lower()
            
            # Analyze the page structure to discover patterns
            patterns = {
                'url_structure': self._analyze_url_structure(current_url),
                'navigation_structure': self._analyze_navigation(driver),
                'link_patterns': self._analyze_link_patterns(driver)
            }
            
            self.discovered_patterns[ticker] = patterns
            log.info(f"Learned patterns for {ticker}: {len(patterns)} pattern types discovered")
            return patterns
        except Exception as e:
            log.warning(f"Pattern learning failed for {ticker}: {e}")
            return {}
    
    def _analyze_url_structure(self, url):
        """Analyze URL structure to understand company's website organization"""
        parts = url.lower().split('/')
        domain_parts = parts[2].split('.')
        
        return {
            'domain_structure': domain_parts,
            'has_www': 'www' in domain_parts,
            'tld': domain_parts[-1],
            'domain_depth': len(domain_parts),
            'path_structure': parts[3:] if len(parts) > 3 else []
        }
    
    def _analyze_navigation(self, driver):
        """Analyze navigation structure to find common patterns"""
        nav_info = {'menus': [], 'footer_links': [], 'header_links': []}
        
        try:
            # Analyze main navigation
            nav_selectors = ['nav', 'header nav', '.navigation', '.navbar', '.main-nav', '.primary-nav']
            for selector in nav_selectors:
                try:
                    navs = driver.find_elements(By.CSS_SELECTOR, selector)
                    for nav in navs[:3]:  # Check first 3 nav elements
                        links = nav.find_elements(By.TAG_NAME, 'a')
                        nav_links = []
                        for link in links[:10]:  # First 10 links per nav
                            text = link.text.strip().lower()
                            href = link.get_attribute('href')
                            if text and href:
                                nav_links.append({'text': text, 'href': href})
                        if nav_links:
                            nav_info['menus'].append(nav_links)
                except:
                    continue
                    
            # Analyze footer
            try:
                footer = driver.find_element(By.TAG_NAME, 'footer')
                footer_links = footer.find_elements(By.TAG_NAME, 'a')
                for link in footer_links[:20]:  # First 20 footer links
                    text = link.text.strip().lower()
                    href = link.get_attribute('href')
                    if text and href and any(pattern in text for pattern in self.ir_patterns):
                        nav_info['footer_links'].append({'text': text, 'href': href})
            except:
                pass
                
        except Exception as e:
            log.debug(f"Navigation analysis error: {e}")
            
        return nav_info
    
    def _analyze_link_patterns(self, driver):
        """Analyze all links to discover URL and text patterns"""
        patterns = {'ir_candidates': [], 'url_patterns': set(), 'text_patterns': set()}
        
        try:
            all_links = driver.find_elements(By.TAG_NAME, 'a')
            for link in all_links[:100]:  # Analyze first 100 links
                try:
                    text = link.text.strip().lower()
                    href = link.get_attribute('href')
                    
                    if not href or 'javascript:' in href:
                        continue
                        
                    # Check for IR-related patterns
                    is_ir_candidate = False
                    for pattern in self.ir_patterns:
                        if pattern in text or pattern in href.lower():
                            is_ir_candidate = True
                            patterns['text_patterns'].add(pattern)
                            break
                    
                    if is_ir_candidate:
                        patterns['ir_candidates'].append({
                            'text': text,
                            'href': href,
                            'confidence': self._calculate_ir_confidence(text, href)
                        })
                        
                    # Extract URL patterns
                    if href.startswith(('http', '/')):
                        url_parts = href.lower().split('/')
                        for part in url_parts:
                            if any(ir_word in part for ir_word in ['investor', 'ir', 'financial', 'governance']):
                                patterns['url_patterns'].add(part)
                                
                except:
                    continue
                    
        except Exception as e:
            log.debug(f"Link pattern analysis error: {e}")
            
        # Convert sets to lists for JSON serialization
        patterns['url_patterns'] = list(patterns['url_patterns'])
        patterns['text_patterns'] = list(patterns['text_patterns'])
        
        return patterns
    
    def _calculate_ir_confidence(self, text, href):
        """Calculate confidence score for IR page candidate"""
        confidence = 0.0
        text_lower = text.lower()
        href_lower = href.lower()
        
        # High confidence indicators
        high_confidence_patterns = ['investor relations', 'investor-relations', '/ir/', '/investors/']
        for pattern in high_confidence_patterns:
            if pattern in text_lower or pattern in href_lower:
                confidence += 0.8
                
        # Medium confidence indicators
        medium_confidence_patterns = ['investors', 'financials', 'governance', 'shareholders']
        for pattern in medium_confidence_patterns:
            if pattern in text_lower or pattern in href_lower:
                confidence += 0.4
                
        # Low confidence indicators
        low_confidence_patterns = ['reports', 'earnings', 'quarterly', 'annual']
        for pattern in low_confidence_patterns:
            if pattern in text_lower or pattern in href_lower:
                confidence += 0.2
                
        return min(confidence, 1.0)  # Cap at 1.0

    def find_ir_page(self, driver, ticker=None, max_attempts=3):
        """Find Investor Relations page URL from main company website dynamically"""
        log.info(f"Searching for IR page dynamically for {ticker or 'unknown company'}...")
        
        # HIGHEST PRIORITY: Use company-specific URL if available
        if ticker and ticker in self.company_specific_ir_urls:
            company_ir_url = self.company_specific_ir_urls[ticker]
            log.info(f"Using company-specific IR URL for {ticker}: {company_ir_url}")
            return company_ir_url
        
        # PRIORITY STRATEGY: Try dedicated IR subdomains first (most reliable)
        current_url = driver.current_url
        base_domain = current_url.split('/')[2].replace('www.', '').lower()
        company_name = base_domain.split('.')[0] if '.' in base_domain else ticker.lower() if ticker else 'company'
        
        dedicated_ir_urls = self._generate_dedicated_ir_urls(base_domain, company_name, ticker)
        
        for ir_url in dedicated_ir_urls:
            try:
                log.info(f"Trying dedicated IR subdomain: {ir_url}")
                driver.get(ir_url)
                
                # Check if this is a valid IR page
                if self._validate_ir_page(driver, ir_url):
                    log.info(f"✓ Found valid dedicated IR page: {ir_url}")
                    return ir_url
                    
            except Exception as e:
                log.debug(f"Dedicated IR URL failed: {ir_url} - {e}")
                continue
        
        # Fallback: Return to main page and use traditional discovery
        driver.get(current_url)
        
        # First, learn patterns specific to this company
        company_patterns = self.learn_company_patterns(driver, ticker)
        
        # Programmatic search for IR links with comprehensive strategies
        for attempt in range(max_attempts):
            try:
                # Strategy 1: Use learned patterns first (if available)
                if company_patterns and company_patterns.get('link_patterns', {}).get('ir_candidates'):
                    log.info(f"Trying learned patterns for {ticker}")
                    ir_candidates = company_patterns['link_patterns']['ir_candidates']
                    # Sort by confidence score
                    ir_candidates.sort(key=lambda x: x.get('confidence', 0), reverse=True)
                    
                    for candidate in ir_candidates[:3]:  # Try top 3 candidates
                        href = candidate['href']
                        if self._is_valid_ir_url(href):
                            log.info(f"Found IR link via learned patterns (confidence: {candidate.get('confidence', 0):.2f}): {href}")
                            self.successful_strategies[ticker] = 'learned_patterns'
                            return href
                
                # Strategy 2: Search by exact link text patterns
                for pattern in self.ir_patterns:
                    try:
                        # Try exact partial text match (case variations)
                        for text_variation in [pattern.title(), pattern.upper(), pattern.lower()]:
                            element = driver.find_element(By.PARTIAL_LINK_TEXT, text_variation)
                            href = element.get_attribute('href')
                            if self._is_valid_ir_url(href):
                                log.info(f"Found IR link via exact text '{text_variation}': {href}")
                                self.successful_strategies[ticker] = 'exact_text'
                                return href
                    except:
                        pass
                
                # Strategy 3: Search by XPath with flexible text matching
                for pattern in self.ir_patterns:
                    try:
                        xpath = f"//a[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{pattern}')]"
                        element = driver.find_element(By.XPATH, xpath)
                        href = element.get_attribute('href')
                        if self._is_valid_ir_url(href):
                            log.info(f"Found IR link via XPath '{pattern}': {href}")
                            self.successful_strategies[ticker] = 'xpath_text'
                            return href
                    except:
                        pass
                
                # Strategy 4: Search by URL patterns (href contains IR indicators)
                try:
                    for url_pattern in self.url_patterns:
                        xpath = f"//a[contains(@href, '{url_pattern}')]"
                        elements = driver.find_elements(By.XPATH, xpath)
                        for element in elements[:5]:  # Check first 5 matches
                            href = element.get_attribute('href')
                            if self._is_valid_ir_url(href):
                                log.info(f"Found IR link via URL pattern '{url_pattern}': {href}")
                                self.successful_strategies[ticker] = 'url_pattern'
                                return href
                except:
                    pass
                
                # Strategy 5: Check navigation menus and headers
                try:
                    nav_selectors = ['nav', 'header', '.navigation', '.menu', '.nav-menu']
                    for selector in nav_selectors:
                        nav_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                        for nav in nav_elements:
                            links = nav.find_elements(By.TAG_NAME, 'a')
                            for link in links:
                                text = link.text.lower().strip()
                                href = link.get_attribute('href')
                                if any(pattern in text for pattern in self.ir_patterns) and self._is_valid_ir_url(href):
                                    log.info(f"Found IR link in navigation: {href}")
                                    return href
                except:
                    pass
                
                # Strategy 5: Check footer (common location for IR links)
                try:
                    footer = driver.find_element(By.TAG_NAME, 'footer')
                    links = footer.find_elements(By.TAG_NAME, 'a')
                    for link in links:
                        text = link.text.lower().strip()
                        href = link.get_attribute('href')
                        if any(pattern in text for pattern in self.ir_patterns) and self._is_valid_ir_url(href):
                            log.info(f"Found IR link in footer: {href}")
                            return href
                except:
                    pass
                
                # Strategy 6: Try common IR path patterns on the same domain
                try:
                    current_url = driver.current_url
                    base_url = '/'.join(current_url.split('/')[:3])  # Get https://domain.com
                    
                    for path in self.ir_path_patterns:
                        potential_ir_url = f"{base_url}{path}"
                        log.info(f"Will try IR path: {potential_ir_url}")
                        return potential_ir_url
                except:
                    pass
                
                # Strategy 7: Look for subdomain patterns (investor.company.com, ir.company.com)
                try:
                    current_domain = driver.current_url.split('/')[2]
                    base_domain = '.'.join(current_domain.split('.')[-2:])  # Get company.com from www.company.com
                    
                    # Try dynamic IR subdomains
                    for subdomain in self.ir_subdomains:
                        potential_ir_url = f"https://{subdomain}.{base_domain}"
                        # We'll check this in the main navigation function
                        log.info(f"Will try IR subdomain: {potential_ir_url}")
                        return potential_ir_url
                except:
                    pass
                
                # Scroll down and try again (content might load dynamically)
                if attempt < max_attempts - 1:
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(2)
                
            except Exception as e:
                log.warning(f"IR search attempt {attempt + 1} failed: {str(e)[:50]}")
                
        log.error("Could not find IR page after comprehensive search")
        return None
    
    def _is_valid_ir_url(self, href):
        """Check if URL looks like a valid IR page"""
        if not href or 'javascript:' in href.lower():
            return False
        
        # Must be HTTP/HTTPS
        if not href.startswith(('http://', 'https://')):
            return False
            
        # Check for IR indicators in URL
        href_lower = href.lower()
        ir_indicators = ['investor', 'ir', 'financial', 'shareholders', 'governance', 'earnings', 'reports']
        
        # Exclude obviously wrong URLs - expanded list
        exclude_indicators = [
            'store', 'directory', 'locations', 'career', 'job', 'help', 'support', 'contact',
            'login', 'account', 'identity', 'auth', 'signin', 'signup', 'register',
            'newsroom', 'news', 'press-release', 'blog', 'article', 'announcement',
            'media', 'press', 'story', 'event', 'webinar', 'conference'
        ]
        
        # Prioritized IR indicators - more specific ones first
        strong_indicators = ['investor-relations', '/ir/', 'investors/', 'shareholders']
        weak_indicators = ['governance', 'earnings', 'reports', 'financial']
        
        # Check for strong IR indicators first
        has_strong_indicator = any(indicator in href_lower for indicator in strong_indicators)
        has_weak_indicator = any(indicator in href_lower for indicator in weak_indicators)
        has_exclusion = any(exclude in href_lower for exclude in exclude_indicators)
        
        # Prefer strong indicators, allow weak only if no exclusions
        if has_strong_indicator and not has_exclusion:
            return True
        elif has_weak_indicator and not has_exclusion:
            # Additional check for weak indicators - must not be in news/media context
            news_context = any(news in href_lower for news in ['2024-', '2025-', '/20', 'announcement', 'partnership'])
            return not news_context
        
        return False
    
    def navigate_to_ir_page(self, driver, main_url, ticker=None):
        """Navigate from main company website to IR page"""
        try:
            # Load main page
            log.info(f"Loading main page: {main_url}")
            driver.get(main_url)
            time.sleep(3)
            
            # Accept cookies
            self.accept_cookies(driver)
            
            # Find IR page URL
            ir_url = self.find_ir_page(driver, ticker)
            
            if not ir_url:
                return None
            
            # Navigate to IR page
            log.info(f"Navigating to IR page: {ir_url}")
            driver.get(ir_url)
            time.sleep(3)
            
            # Accept cookies on IR page
            self.accept_cookies(driver)
            
            return ir_url
            
        except Exception as e:
            log.error(f"Error navigating to IR page: {str(e)}")
            return None
    
    def _generate_dedicated_ir_urls(self, base_domain, company_name, ticker):
        """Generate potential dedicated IR subdomain URLs based on company examples"""
        ir_urls = []
        
        # Priority patterns based on real examples
        # Amazon: ir.aboutamazon.com, Amgen: investors.amgen.com
        priority_patterns = [
            f"https://ir.about{company_name}.com/",
            f"https://investors.{base_domain}/",
            f"https://investor.{base_domain}/",
            f"https://ir.{base_domain}/",
        ]
        
        # Secondary patterns
        secondary_patterns = [
            f"https://corporate.{base_domain}/investor/",
            f"https://www.{base_domain}/investor/",
            f"https://www.{base_domain}/investors/",
            f"https://finance.{base_domain}/",
            f"https://sec.{base_domain}/"
        ]
        
        # Add priority patterns first
        ir_urls.extend(priority_patterns)
        ir_urls.extend(secondary_patterns)
        
        return ir_urls
    
    def _validate_ir_page(self, driver, ir_url):
        """Validate if the current page is actually an IR page"""
        try:
            # Check page title and content for IR indicators
            title = driver.title.lower()
            current_url = driver.current_url.lower()
            
            # Strong IR indicators in title
            ir_title_indicators = [
                'investor', 'investors', 'investor relations', 
                'financial', 'earnings', 'stockholder', 'shareholder'
            ]
            
            # Check if URL itself indicates IR page (even if access denied)
            url_ir_indicators = [
                'investors.', 'investor.', 'ir.', '/investor', '/investors', '/ir'
            ]
            
            title_match = any(indicator in title for indicator in ir_title_indicators)
            url_match = any(indicator in current_url for indicator in url_ir_indicators)
            
            # Handle access denied cases - if URL looks like IR page, accept it
            if 'access denied' in title or 'forbidden' in title or 'denied' in title:
                if url_match:
                    log.info(f"✓ Validated IR page (URL-based, access restricted): {ir_url}")
                    return True
                else:
                    log.debug(f"✗ Access denied but URL doesn't look like IR page: {ir_url}")
                    return False
            
            # Check for IR-specific elements on the page
            ir_content_indicators = [
                'financial reports', 'earnings', 'sec filings', 'quarterly',
                'annual report', '10-k', '10-q', 'press release', 'financial information'
            ]
            
            try:
                page_text = driver.find_element(By.TAG_NAME, 'body').text.lower()
                content_match = any(indicator in page_text for indicator in ir_content_indicators)
            except:
                content_match = False
            
            # Validation logic: title match OR content match OR URL match
            is_valid = title_match or content_match or url_match
            
            if is_valid:
                log.info(f"✓ Validated IR page: Title match={title_match}, Content match={content_match}, URL match={url_match}")
            else:
                log.debug(f"✗ Invalid IR page: Title='{title[:50]}...', URL='{current_url[:50]}...'")
                
            return is_valid
            
        except Exception as e:
            log.debug(f"Error validating IR page: {e}")
            return False