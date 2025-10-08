"""
Selenium Scraper - ROBUST VERSION
Better DOM selectors + regex fallback for factory job extraction
"""

import logging
import re
import time
from datetime import datetime
from typing import List, Dict, Optional

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from src.config import FACTORY_CAREER_URL

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SeleniumScraper:
    """Production-grade Selenium scraper with multiple strategies"""
    
    def __init__(self, headless: bool = True):
        self.headless = headless
        self.factory_keywords = [
            'plant', 'production', 'manufacturing', 'assembly', 'operator',
            'technician', 'mechanic', 'maintenance', 'quality', 'supervisor',
            'foreman', 'shift', 'machine', 'tool', 'welder', 'fitter',
            'warehouse', 'logistics', 'die', 'mold', 'press', 'injection',
            'stamping', 'painting', 'welding', 'material', 'inventory',
            'floor', 'line', 'process', 'supply chain'
        ]
        
        self.indian_cities = [
            'Chennai', 'Pune', 'Bangalore', 'Bengaluru', 'Hosur', 'Sanand',
            'Manesar', 'Ahmedabad', 'Mumbai', 'Hyderabad', 'Chakan', 'Navagam',
            'Gurgaon', 'Gurugram', 'Noida', 'Haridwar', 'Bawal', 'Dharuhera',
            'Greater Noida', 'Aurangabad', 'Coimbatore', 'India'
        ]
    
    def _init_driver(self):
        """Initialize ChromeDriver with robust settings"""
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        
        try:
            # FIXED: Remove os_type parameter
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
            return driver
        except Exception as e:
            logger.error(f"ChromeDriver initialization failed: {e}")
            # Fallback: try without service
            driver = webdriver.Chrome(options=chrome_options)
            return driver
    
    def scrape_jobs(self, url: str = FACTORY_CAREER_URL) -> List[Dict]:
        """Scrape jobs with multiple extraction strategies"""
        logger.info(f"🔍 Scraping jobs from: {url}")
        driver = self._init_driver()
        
        try:
            driver.get(url)
            time.sleep(3)  # Wait for JavaScript to load
            
            # Try multiple extraction strategies
            jobs = []
            
            # Strategy 1: Try structured DOM extraction (multiple selectors)
            jobs = self._extract_with_dom_strategy1(driver)
            if not jobs:
                jobs = self._extract_with_dom_strategy2(driver)
            if not jobs:
                jobs = self._extract_with_dom_strategy3(driver)
            
            # Strategy 2: Fallback to regex extraction
            if not jobs:
                logger.warning("⚠️ DOM extraction failed, using regex fallback")
                page_text = driver.find_element(By.TAG_NAME, "body").text
                jobs = self._extract_with_regex(page_text)
            
            # Strategy 3: Last resort - parse HTML source
            if not jobs:
                logger.warning("⚠️ Regex failed, parsing HTML source")
                html_source = driver.page_source
                jobs = self._extract_from_html(html_source)
            
            logger.info(f"✅ Extracted {len(jobs)} jobs")
            return jobs
        
        except Exception as e:
            logger.error(f"❌ Job scraping failed: {e}")
            import traceback
            traceback.print_exc()
            return []
        
        finally:
            driver.quit()
    
    def _extract_with_dom_strategy1(self, driver) -> List[Dict]:
        """Strategy 1: Common job board selectors"""
        try:
            wait = WebDriverWait(driver, 10)
            
            # Try common selectors
            selectors = [
                "div.job-item",
                "div.job-card",
                "div.position-item",
                "div.career-item",
                "li.job-listing",
                "tr.job-row",
                "div[class*='job']",
                "div[data-job-id]"
            ]
            
            jobs = []
            for selector in selectors:
                try:
                    elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        logger.info(f"✅ Found {len(elements)} jobs with selector: {selector}")
                        
                        for elem in elements:
                            try:
                                # Extract job data
                                title = self._extract_text(elem, ["a.job-title", "h3", "h4", ".title", "a[href*='job']"])
                                location = self._extract_text(elem, [".location", ".job-location", "span.city", ".place"])
                                department = self._extract_text(elem, [".department", ".category", ".division"])
                                link = self._extract_link(elem, ["a.job-title", "a.job-link", "a[href*='job']"])
                                
                                if title and self._is_factory_job(title):
                                    jobs.append({
                                        'title': title.strip(),
                                        'location': location or self._guess_location(elem.text),
                                        'department': department,
                                        'url': link or FACTORY_CAREER_URL,
                                        'source': 'motherson_careers'
                                    })
                            except:
                                continue
                        
                        if jobs:
                            return jobs
                except:
                    continue
            
            return jobs
        
        except Exception as e:
            logger.warning(f"Strategy 1 failed: {e}")
            return []
    
    def _extract_with_dom_strategy2(self, driver) -> List[Dict]:
        """Strategy 2: Table-based extraction"""
        try:
            tables = driver.find_elements(By.TAG_NAME, "table")
            
            jobs = []
            for table in tables:
                rows = table.find_elements(By.TAG_NAME, "tr")
                
                for row in rows[1:]:  # Skip header
                    cells = row.find_elements(By.TAG_NAME, "td")
                    
                    if len(cells) >= 2:
                        title = cells[0].text.strip()
                        location = cells[1].text.strip() if len(cells) > 1 else 'India'
                        
                        if title and self._is_factory_job(title):
                            jobs.append({
                                'title': title,
                                'location': location,
                                'department': None,
                                'url': FACTORY_CAREER_URL,
                                'source': 'motherson_careers_table'
                            })
            
            if jobs:
                logger.info(f"✅ Strategy 2 found {len(jobs)} jobs")
            return jobs
        
        except Exception as e:
            logger.warning(f"Strategy 2 failed: {e}")
            return []
    
    def _extract_with_dom_strategy3(self, driver) -> List[Dict]:
        """Strategy 3: List-based extraction"""
        try:
            lists = driver.find_elements(By.CSS_SELECTOR, "ul, ol")
            
            jobs = []
            for list_elem in lists:
                items = list_elem.find_elements(By.TAG_NAME, "li")
                
                for item in items:
                    text = item.text.strip()
                    
                    # Check if it looks like a job posting
                    if len(text) > 10 and len(text) < 200:
                        # Try to split title and location
                        parts = re.split(r'[-|–—]', text, maxsplit=1)
                        title = parts[0].strip()
                        location = parts[1].strip() if len(parts) > 1 else 'India'
                        
                        if self._is_factory_job(title):
                            jobs.append({
                                'title': title,
                                'location': location or self._guess_location(text),
                                'department': None,
                                'url': FACTORY_CAREER_URL,
                                'source': 'motherson_careers_list'
                            })
            
            if jobs:
                logger.info(f"✅ Strategy 3 found {len(jobs)} jobs")
            return jobs
        
        except Exception as e:
            logger.warning(f"Strategy 3 failed: {e}")
            return []
    
    def _extract_with_regex(self, text: str) -> List[Dict]:
        """Regex-based extraction as fallback"""
        jobs = []
        
        # Enhanced job patterns
        patterns = [
            r'(Production|Manufacturing|Assembly|Plant|Quality|Maintenance|Warehouse|Logistics|Tool|Die|Mold|Process|Line)\s+(Manager|Engineer|Supervisor|Operator|Technician|Coordinator|Specialist|Planner|Designer|Controller|Lead|Head|In-charge)',
            r'(Senior|Junior|Lead|Chief|Deputy|Assistant|Sr\.|Jr\.)\s+\w+\s+(Engineer|Manager|Supervisor|Coordinator|Technician|Specialist)',
            r'\w+\s+(Operator|Technician|Mechanic|Fitter|Welder|Assembler|Inspector|Machinist)',
            r'(Shift|Floor|Line|Production|Process|Material)\s+(Manager|Supervisor|Coordinator|In-charge|Lead|Engineer)',
            r'(Inventory|Supply Chain|Stamping|Welding|Painting|Injection|Molding)\s+(Engineer|Manager|Supervisor|Technician|Specialist)'
        ]
        
        for pattern in patterns:
            for match in re.finditer(pattern, text, re.IGNORECASE):
                title = match.group(0).strip()
                
                if self._is_factory_job(title):
                    # Try to find location nearby
                    context_start = max(0, match.start() - 200)
                    context_end = min(len(text), match.end() + 200)
                    context = text[context_start:context_end]
                    
                    location = self._guess_location(context)
                    
                    jobs.append({
                        'title': title,
                        'location': location,
                        'department': None,
                        'url': FACTORY_CAREER_URL,
                        'source': 'regex_extraction'
                    })
        
        # Deduplicate
        unique_jobs = []
        seen_titles = set()
        for job in jobs:
            title_lower = job['title'].lower()
            if title_lower not in seen_titles:
                seen_titles.add(title_lower)
                unique_jobs.append(job)
        
        if unique_jobs:
            logger.info(f"✅ Regex extraction found {len(unique_jobs)} jobs")
        return unique_jobs
    
    def _extract_from_html(self, html: str) -> List[Dict]:
        """Parse HTML source as last resort"""
        from bs4 import BeautifulSoup
        
        jobs = []
        soup = BeautifulSoup(html, 'html.parser')
        
        # Look for job-related elements
        job_elements = soup.find_all(['div', 'li', 'tr'], class_=re.compile(r'job|position|career|vacancy', re.I))
        
        for elem in job_elements:
            text = elem.get_text(strip=True)
            
            # Extract title
            title_elem = elem.find(['h3', 'h4', 'a', 'span'], class_=re.compile(r'title|name', re.I))
            title = title_elem.get_text(strip=True) if title_elem else text[:100]
            
            # Extract location
            loc_elem = elem.find(['span', 'div'], class_=re.compile(r'location|city|place', re.I))
            location = loc_elem.get_text(strip=True) if loc_elem else self._guess_location(text)
            
            if title and self._is_factory_job(title):
                jobs.append({
                    'title': title,
                    'location': location,
                    'department': None,
                    'url': FACTORY_CAREER_URL,
                    'source': 'html_parsing'
                })
        
        if jobs:
            logger.info(f"✅ HTML parsing found {len(jobs)} jobs")
        return jobs
    
    def _extract_text(self, element, selectors: List[str]) -> Optional[str]:
        """Try multiple selectors to extract text"""
        for selector in selectors:
            try:
                elem = element.find_element(By.CSS_SELECTOR, selector)
                text = elem.text.strip()
                if text:
                    return text
            except:
                continue
        
        # Fallback: return element text
        try:
            return element.text.strip()
        except:
            return None
    
    def _extract_link(self, element, selectors: List[str]) -> Optional[str]:
        """Try multiple selectors to extract link"""
        for selector in selectors:
            try:
                elem = element.find_element(By.CSS_SELECTOR, selector)
                href = elem.get_attribute('href')
                if href:
                    return href
            except:
                continue
        return None
    
    def _is_factory_job(self, title: str) -> bool:
        """Check if job title is factory-related"""
        if not title or len(title) < 5:
            return False
        
        title_lower = title.lower()
        
        # Exclude non-factory roles
        non_factory_keywords = [
            'software', 'developer', 'programmer', 'data scientist',
            'it ', 'digital', 'cyber', 'application', 'web', 'mobile',
            'cloud', 'devops', 'analyst', 'sap', 'erp', 'finance',
            'hr', 'marketing', 'sales', 'legal', 'admin'
        ]
        
        if any(kw in title_lower for kw in non_factory_keywords):
            return False
        
        # Check for factory keywords
        return any(kw in title_lower for kw in self.factory_keywords)
    
    def _guess_location(self, text: str) -> str:
        """Guess location from text"""
        if not text:
            return 'India'
        
        text_lower = text.lower()
        
        # Check for city names
        for city in self.indian_cities:
            if city.lower() in text_lower:
                return city
        
        return 'India'


# Regex fallback class (for backward compatibility)
class RegexFallbackScraper:
    """Standalone regex-based scraper"""
    
    def __init__(self):
        self.factory_keywords = [
            'plant', 'production', 'manufacturing', 'assembly', 'operator',
            'technician', 'mechanic', 'maintenance', 'quality', 'supervisor',
            'foreman', 'shift', 'machine', 'tool', 'welder', 'fitter',
            'warehouse', 'logistics', 'die', 'mold', 'press', 'injection',
            'stamping', 'painting', 'welding', 'material', 'inventory',
            'floor', 'line'
        ]
        self.indian_cities = [
            'Chennai', 'Pune', 'Bangalore', 'Bengaluru', 'Hosur', 'Sanand',
            'Manesar', 'Ahmedabad', 'Mumbai', 'Hyderabad', 'Chakan', 'Navagam',
            'Gurgaon', 'Gurugram', 'Noida', 'Haridwar', 'Bawal', 'Dharuhera',
            'Greater Noida', 'Aurangabad', 'Coimbatore'
        ]
    
    def extract_from_text(self, text: str) -> List[Dict]:
        """Use regex to identify likely factory job listings"""
        jobs = []
        job_patterns = [
            r'(Production|Manufacturing|Assembly|Plant|Quality|Maintenance|Warehouse|Logistics|Tool|Die|Mold)\s+(Manager|Engineer|Supervisor|Operator|Technician)',
            r'(Shift|Line|Floor|Material)\s+(Manager|Supervisor|Lead|Coordinator)',
            r'(Technician|Mechanic|Fitter|Welder|Assembler|Inspector|Machinist)'
        ]
        
        for pattern in job_patterns:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for m in matches:
                title = m.group(0).strip()
                if self._is_valid_job(title):
                    jobs.append({"title": title, "location": self._guess_location(text)})
        
        # Deduplicate
        unique = []
        seen = set()
        for job in jobs:
            title = job["title"].lower()
            if title not in seen:
                seen.add(title)
                unique.append(job)
        return unique
    
    def _is_valid_job(self, title: str) -> bool:
        title_lower = title.lower()
        if any(kw in title_lower for kw in ['software', 'developer', 'it', 'digital']):
            return False
        return any(kw in title_lower for kw in self.factory_keywords)
    
    def _guess_location(self, text: str) -> str:
        for city in self.indian_cities:
            if city.lower() in text.lower():
                return city
        return "India"