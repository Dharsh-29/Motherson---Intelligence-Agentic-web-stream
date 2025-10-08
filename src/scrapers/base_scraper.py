"""Base web scraper with rate limiting and caching"""
import os
import time
import hashlib
import json
import requests
import logging
from datetime import datetime
from typing import Dict, Optional, List
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
from pathlib import Path

from bs4 import BeautifulSoup
import trafilatura

from ..config import CACHE_DIR, SCRAPER_RATE_LIMIT, SCRAPER_TIMEOUT, SCRAPER_MAX_RETRIES

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BaseScraper:
    def __init__(self, cache_dir: Path = None, rate_limit: float = None):
        self.cache_dir = cache_dir or CACHE_DIR
        self.rate_limit = rate_limit or SCRAPER_RATE_LIMIT
        self.timeout = SCRAPER_TIMEOUT
        self.max_retries = SCRAPER_MAX_RETRIES
        
        self.last_request_time = {}
        self.robot_parsers = {}
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        }
    
    def _get_cache_path(self, url: str) -> Path:
        url_hash = hashlib.md5(url.encode()).hexdigest()
        return self.cache_dir / f"{url_hash}.json"
    
    def _load_from_cache(self, url: str) -> Optional[Dict]:
        cache_path = self._get_cache_path(url)
        if cache_path.exists():
            try:
                with open(cache_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                pass
        return None
    
    def _save_to_cache(self, url: str, data: Dict):
        try:
            with open(self._get_cache_path(url), 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False, default=str)
        except Exception as e:
            logger.warning(f"Cache write error: {e}")
    
    def _check_robots_txt(self, url: str) -> bool:
        parsed = urlparse(url)
        domain = f"{parsed.scheme}://{parsed.netloc}"
        
        if domain not in self.robot_parsers:
            robots_url = urljoin(domain, '/robots.txt')
            rp = RobotFileParser()
            rp.set_url(robots_url)
            try:
                rp.read()
                self.robot_parsers[domain] = rp
            except:
                self.robot_parsers[domain] = None
        
        rp = self.robot_parsers[domain]
        return rp.can_fetch("*", url) if rp else True
    
    def _apply_rate_limit(self, domain: str):
        if domain in self.last_request_time:
            elapsed = time.time() - self.last_request_time[domain]
            if elapsed < self.rate_limit:
                time.sleep(self.rate_limit - elapsed)
        self.last_request_time[domain] = time.time()
    
    def _fetch_url(self, url: str) -> Optional[bytes]:
        parsed = urlparse(url)
        domain = parsed.netloc
        
        if not self._check_robots_txt(url):
            return None
        
        self._apply_rate_limit(domain)
        
        for attempt in range(self.max_retries):
            try:
                response = requests.get(url, headers=self.headers, timeout=self.timeout)
                response.raise_for_status()
                return response.content
            except Exception as e:
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
        return None
    
    def _extract_text_from_html(self, html_content: bytes, url: str) -> Dict:
        try:
            text = trafilatura.extract(html_content, include_tables=True, no_fallback=False)
            soup = BeautifulSoup(html_content, 'html.parser')
            title = soup.title.string if soup.title else url
            
            publish_date = None
            for tag in soup.find_all(['time', 'meta']):
                if tag.name == 'time' and tag.get('datetime'):
                    publish_date = tag['datetime']
                    break
            
            if not text:
                for script in soup(['script', 'style', 'nav', 'footer']):
                    script.decompose()
                text = soup.get_text(separator='\n', strip=True)
            
            return {
                'url': url,
                'text': text,
                'title': title,
                'fetched_at': datetime.now().isoformat(),
                'mime': 'text/html',
                'publish_dt': publish_date
            }
        except Exception as e:
            logger.error(f"HTML extraction error: {e}")
            return None
    
    def scrape_url(self, url: str, use_cache: bool = True) -> Optional[Dict]:
        if use_cache:
            cached = self._load_from_cache(url)
            if cached:
                logger.info(f"Loaded from cache: {url}")
                return cached
        
        content = self._fetch_url(url)
        if not content:
            return None
        
        if url.lower().endswith('.pdf') or content[:4] == b'%PDF':
            from .pdf_extractor import PDFExtractor
            extractor = PDFExtractor()
            data = extractor.extract_from_bytes(content, url)
        else:
            data = self._extract_text_from_html(content, url)
        
        if data and use_cache:
            self._save_to_cache(url, data)
        
        return data
    
    def scrape_multiple(self, urls: List[str], use_cache: bool = True) -> List[Dict]:
        results = []
        for idx, url in enumerate(urls, 1):
            logger.info(f"Scraping {idx}/{len(urls)}: {url}")
            data = self.scrape_url(url, use_cache=use_cache)
            if data:
                results.append(data)
        return results