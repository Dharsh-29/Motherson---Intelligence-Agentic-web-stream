"""
Address Directory Scraper - FIXED VERSION
Scrapes https://www.motherson.com/contact/address-directory?country=India&page=X
Critical for Query 1 - List Facilities
"""

import logging
import json
import time
from pathlib import Path
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
import re

from src.config import CACHE_DIR

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AddressScraper:
    """Scraper for Motherson address directory with proper table parsing"""
    
    def __init__(self, rate_limit: float = 2.0):
        self.rate_limit = rate_limit
        self.cache_file = CACHE_DIR / "address_facilities.json"
        self.base_url = (
            "https://www.motherson.com/contact/address-directory?country=India&page="
        )
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        # Division mapping
        self.division_map = {
            "MSWIL": "Wiring Systems",
            "MSW": "Wiring Systems",
            "WIRING": "Wiring Systems",
            "SMP": "Polymers",
            "SMR": "Vision Systems",
            "VISION": "Vision Systems",
            "MSSL": "Wiring Systems",
            "PKC": "Wiring Systems",
            "SEATING": "Seating Systems",
            "LOGISTICS": "Logistics"
        }
        
        # City to state mapping
        self.city_to_state = {
            'Sanand': 'Gujarat', 'Ahmedabad': 'Gujarat', 'Navagam': 'Gujarat',
            'Pune': 'Maharashtra', 'Chakan': 'Maharashtra', 'Mumbai': 'Maharashtra',
            'Aurangabad': 'Maharashtra',
            'Chennai': 'Tamil Nadu', 'Hosur': 'Tamil Nadu', 'Coimbatore': 'Tamil Nadu',
            'Bangalore': 'Karnataka', 'Bengaluru': 'Karnataka',
            'Manesar': 'Haryana', 'Gurgaon': 'Haryana', 'Gurugram': 'Haryana',
            'Bawal': 'Haryana', 'Dharuhera': 'Haryana',
            'Noida': 'Uttar Pradesh', 'Greater Noida': 'Uttar Pradesh',
            'Haridwar': 'Uttarakhand',
            'Hyderabad': 'Telangana',
            'Kolkata': 'West Bengal'
        }
    
    def scrape_all(self, pages: int = 19, use_cache: bool = True) -> List[Dict]:
        """Scrape all address-directory pages"""
        if use_cache and self.cache_file.exists():
            logger.info("📦 Using cached address directory results")
            return json.loads(self.cache_file.read_text(encoding="utf-8"))
        
        all_facilities = []
        
        for page in range(1, pages + 1):
            url = f"{self.base_url}{page}"
            logger.info(f"🌐 Scraping page {page}/{pages}")
            
            try:
                resp = requests.get(url, headers=self.headers, timeout=30)
                resp.raise_for_status()
                facilities = self._parse_page(resp.text)
                
                if facilities:
                    all_facilities.extend(facilities)
                    logger.info(f"  ✅ Extracted {len(facilities)} facilities")
                else:
                    logger.warning(f"  ⚠️ No facilities found on page {page}")
                
            except Exception as e:
                logger.error(f"  ✗ Failed page {page}: {e}")
            
            time.sleep(self.rate_limit)
        
        # Save cache
        self.cache_file.write_text(
            json.dumps(all_facilities, indent=2, ensure_ascii=False),
            encoding="utf-8"
        )
        
        logger.info(f"✅ Total facilities scraped: {len(all_facilities)}")
        return all_facilities
    
    def _parse_page(self, html: str) -> List[Dict]:
        """
        Parse facilities from page - FIXED VERSION
        Handles multiple table structures and div-based layouts
        """
        soup = BeautifulSoup(html, "html.parser")
        results = []
        
        # Method 1: Try to find table with class or id
        table = soup.find("table", class_=re.compile(r'address|facility|location', re.I))
        if not table:
            table = soup.find("table")
        
        if table:
            logger.info("  📋 Found table structure")
            results = self._parse_table(table)
            if results:
                return results
        
        # Method 2: Try div-based card layout
        logger.info("  🔍 Trying card-based layout")
        cards = soup.find_all("div", class_=re.compile(r'card|facility|location|address', re.I))
        
        for card in cards:
            facility = self._parse_card(card)
            if facility:
                results.append(facility)
        
        if results:
            return results
        
        # Method 3: Try list-based layout
        logger.info("  🔍 Trying list-based layout")
        list_items = soup.find_all("li", class_=re.compile(r'facility|location|address', re.I))
        
        for item in list_items:
            facility = self._parse_list_item(item)
            if facility:
                results.append(facility)
        
        return results
    
    def _parse_table(self, table) -> List[Dict]:
        """Parse table structure"""
        results = []
        rows = table.find_all("tr")
        
        if len(rows) < 2:
            return results
        
        # Try to identify header
        header = rows[0]
        header_cells = [cell.get_text(strip=True).lower() for cell in header.find_all(["th", "td"])]
        
        # Find column indices
        name_col = self._find_col_index(header_cells, ['name', 'facility', 'company', 'location'])
        city_col = self._find_col_index(header_cells, ['city', 'location', 'place'])
        state_col = self._find_col_index(header_cells, ['state', 'region'])
        address_col = self._find_col_index(header_cells, ['address', 'street'])
        
        # Parse data rows
        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue
            
            cell_texts = [cell.get_text(strip=True) for cell in cells]
            
            # Extract name
            name = None
            if name_col is not None and name_col < len(cell_texts):
                name = cell_texts[name_col]
            elif len(cell_texts) > 0:
                name = cell_texts[0]
            
            if not name or len(name) < 3:
                continue
            
            # Extract location
            city = None
            state = None
            
            if city_col is not None and city_col < len(cell_texts):
                location_text = cell_texts[city_col]
                city, state = self._split_location(location_text)
            
            if state_col is not None and state_col < len(cell_texts):
                state = cell_texts[state_col]
            
            # Extract address
            address = None
            if address_col is not None and address_col < len(cell_texts):
                address = cell_texts[address_col]
            
            # Infer state from city
            if city and not state:
                state = self.city_to_state.get(city)
            
            # Map division
            division = self._map_division(name)
            
            results.append({
                "name": name,
                "division": division,
                "city": city,
                "state": state,
                "address": address,
                "country": "India",
                "status": "operational",
                "source": "address_directory",
                "date": None
            })
        
        return results
    
    def _parse_card(self, card) -> Optional[Dict]:
        """Parse card/div structure"""
        text = card.get_text(separator='|', strip=True)
        
        # Try to find name in heading
        name_elem = card.find(['h3', 'h4', 'h5', 'strong', 'b'])
        name = name_elem.get_text(strip=True) if name_elem else None
        
        if not name:
            # Use first line
            lines = text.split('|')
            name = lines[0] if lines else None
        
        if not name or len(name) < 3:
            return None
        
        # Extract location
        city = self._extract_city(text)
        state = self._extract_state(text)
        
        if city and not state:
            state = self.city_to_state.get(city)
        
        # Map division
        division = self._map_division(text)
        
        return {
            "name": name,
            "division": division,
            "city": city,
            "state": state,
            "address": None,
            "country": "India",
            "status": "operational",
            "source": "address_directory",
            "date": None
        }
    
    def _parse_list_item(self, item) -> Optional[Dict]:
        """Parse list item structure"""
        return self._parse_card(item)  # Same logic
    
    def _find_col_index(self, headers: List[str], keywords: List[str]) -> Optional[int]:
        """Find column index matching keywords"""
        for idx, header in enumerate(headers):
            if any(kw in header for kw in keywords):
                return idx
        return None
    
    def _split_location(self, text: str) -> tuple:
        """Split location text into city and state"""
        if not text:
            return None, None
        
        parts = [p.strip() for p in text.split(",")]
        
        if len(parts) == 2:
            return parts[0], parts[1]
        elif len(parts) > 2:
            return parts[0], parts[-1]
        else:
            # Try to identify if it's a city or state
            text_clean = text.strip()
            if text_clean in self.city_to_state:
                return text_clean, self.city_to_state[text_clean]
            elif text_clean in self.city_to_state.values():
                return None, text_clean
            else:
                return text_clean, None
    
    def _extract_city(self, text: str) -> Optional[str]:
        """Extract city from text"""
        for city in self.city_to_state.keys():
            if city.lower() in text.lower():
                return city
        return None
    
    def _extract_state(self, text: str) -> Optional[str]:
        """Extract state from text"""
        for state in self.city_to_state.values():
            if state.lower() in text.lower():
                return state
        return None
    
    def _map_division(self, text: str) -> str:
        """Map division from text"""
        text_upper = text.upper()
        
        for abbr, full_name in self.division_map.items():
            if abbr in text_upper:
                return full_name
        
        # Keyword matching
        if 'WIRING' in text_upper or 'HARNESS' in text_upper:
            return 'Wiring Systems'
        elif 'VISION' in text_upper or 'MIRROR' in text_upper:
            return 'Vision Systems'
        elif 'POLYMER' in text_upper:
            return 'Polymers'
        elif 'SEATING' in text_upper:
            return 'Seating Systems'
        elif 'LOGISTIC' in text_upper:
            return 'Logistics'
        
        return 'Unknown'