"""
Enhanced Entity Extractor - COMPLETE REWRITE
Significantly improved facility, division, and job extraction
"""

import spacy
import re
import logging
from typing import Dict, List
import datetime

try:
    from src.config import SPACY_MODEL
except ImportError:
    SPACY_MODEL = "en_core_web_sm"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EntityExtractor:
    def __init__(self):
        try:
            self.nlp = spacy.load(SPACY_MODEL)
            logger.info(f"Loaded spaCy model: {SPACY_MODEL}")
        except:
            logger.warning("spaCy model not found. Run: python -m spacy download en_core_web_sm")
            self.nlp = None
        
        # EXPANDED Division patterns
        self.division_patterns = [
            r'\b(Motherson Wiring Systems?|Wiring Systems?|Wiring Division|MSWIL|MSW)\b',
            r'\b(Motherson Vision Systems?|Vision Systems?|Vision Division|SMR|Samvardhana Motherson Reflectec)\b',
            r'\b(Motherson Seating Systems?|Seating Systems?|Seating Division)\b',
            r'\b(Motherson Polymers?|Polymers Division|SMP)\b',
            r'\b(Motherson Logistics|Logistics Division)\b',
            r'\b(PKC Group|PKC Wiring)\b',
        ]
        
        # Division mapping
        self.division_map = {
            'MSWIL': 'Wiring Systems',
            'MSW': 'Wiring Systems',
            'WIRING': 'Wiring Systems',
            'SMR': 'Vision Systems',
            'VISION': 'Vision Systems',
            'SMP': 'Polymers',
            'POLYMER': 'Polymers',
            'SEATING': 'Seating Systems',
            'LOGISTICS': 'Logistics',
            'PKC': 'Wiring Systems'
        }
        
        # Status patterns
        self.status_patterns = {
            'operational': r'\b(operational|operating|commissioned|inaugurated|started operations|in operation|existing plant|currently operating|active)\b',
            'under-construction': r'\b(under construction|being built|construction phase|currently building|construction underway|construction started|under development)\b',
            'planned': r'\b(planned|proposed|announced|upcoming|future|will establish|to be set up|proposed plant|plans to set up|announced plant)\b'
        }
        
        # EXPANDED Event patterns
        self.event_patterns = [
            r'\b(announced?|announcing|announcement of)\b',
            r'\b(groundbreaking|foundation stone|ground breaking)\b',
            r'\b(commissioned?|commissioning|inaugurated?|inauguration)\b',
            r'\b(expansion|expanding|expand|phase \d+|scale up|brownfield)\b',
            r'\b(greenfield|new plant|new facility|new manufacturing|new unit)\b',
            r'\b(set up|setting up|establish|establishing|established)\b',
            r'\b(started operations?|commenced operations?|begin operations?)\b',
        ]
        
        # EXPANDED Date patterns
        self.date_patterns = [
            r'\b(FY\s*\d{2,4}(?:-\d{2,4})?)\b',
            r'\b(Q[1-4]\s+\d{4})\b',
            r'\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})\b',
            r'\b(\d{1,2})[-/](\d{1,2})[-/](\d{4})\b',
            r'\b(202[0-9]|201[5-9])\b',
            r'\b(H[1-2]\s+(?:FY\s*)?\d{4})\b',  # Half-year format
        ]
        
        # EXPANDED Indian locations
        self.indian_states = [
            'Gujarat', 'Tamil Nadu', 'Maharashtra', 'Haryana', 'Karnataka',
            'Uttar Pradesh', 'Rajasthan', 'Punjab', 'Telangana', 'Andhra Pradesh',
            'West Bengal', 'Madhya Pradesh', 'Kerala', 'Odisha', 'Uttarakhand'
        ]
        
        self.indian_cities = [
            'Ahmedabad', 'Pune', 'Chennai', 'Bangalore', 'Bengaluru', 'Mumbai',
            'Gurgaon', 'Gurugram', 'Hyderabad', 'Kolkata', 'Sanand', 'Navagam',
            'Chakan', 'Manesar', 'Noida', 'Haridwar', 'Bawal', 'Hosur',
            'Dharuhera', 'Greater Noida', 'Aurangabad', 'Coimbatore'
        ]
        
        # City to state mapping
        self.city_to_state = {
            'Sanand': 'Gujarat', 'Ahmedabad': 'Gujarat', 'Navagam': 'Gujarat',
            'Pune': 'Maharashtra', 'Chakan': 'Maharashtra', 'Mumbai': 'Maharashtra', 'Aurangabad': 'Maharashtra',
            'Chennai': 'Tamil Nadu', 'Hosur': 'Tamil Nadu', 'Coimbatore': 'Tamil Nadu',
            'Bangalore': 'Karnataka', 'Bengaluru': 'Karnataka',
            'Manesar': 'Haryana', 'Gurgaon': 'Haryana', 'Gurugram': 'Haryana', 'Bawal': 'Haryana', 'Dharuhera': 'Haryana',
            'Noida': 'Uttar Pradesh', 'Greater Noida': 'Uttar Pradesh',
            'Haridwar': 'Uttarakhand',
            'Hyderabad': 'Telangana',
            'Kolkata': 'West Bengal'
        }
        
        # STRICTER facility blacklist
        self.facility_blacklist = [
            r'\b(board of directors|management team|audit committee)\b',
            r'\b(page \d+|section \d+|chapter \d+)\b',
            r'\b(registered office|corporate office|head office)\b',
            r'\b(financial statement|balance sheet|profit and loss)\b',
            r'\b(total assets|total revenue|net profit)\b',
            r'^(the|a|an|this|that|our|their)\b',
            r'\b(company|corporation|limited|group)$',
        ]
        
        # EXPANDED Factory job keywords
        self.factory_job_keywords = [
            'plant', 'production', 'manufacturing', 'assembly', 'operator',
            'technician', 'mechanic', 'maintenance', 'quality', 'supervisor',
            'foreman', 'machinist', 'welder', 'fitter', 'shift', 'floor',
            'warehouse', 'logistics', 'supply chain', 'material', 'inventory',
            'tool', 'die', 'mold', 'press', 'injection', 'stamping',
            'paint', 'welding', 'inspection', 'process', 'line'
        ]
        
        self.non_factory_keywords = [
            'software', 'developer', 'programmer', 'data scientist',
            'it ', 'digital', 'cyber', 'application', 'web', 'mobile',
            'cloud', 'devops', 'analyst', 'sap', 'erp'
        ]
    
    def extract_facilities(self, text: str) -> List[Dict]:
        """Extract facility mentions - COMPREHENSIVE PATTERNS"""
        facilities = []
        
        # Pattern 1: "City Plant/Facility/Unit"
        pattern1 = r'\b(Sanand|Hosur|Chakan|Manesar|Pune|Ahmedabad|Chennai|Bangalore|Bengaluru|Navagam|Bawal|Haridwar|Noida|Gurgaon|Gurugram|Hyderabad|Mumbai|Dharuhera|Coimbatore|Aurangabad|Greater Noida)\s+(Plant|Facility|Unit|Manufacturing|Operations|Factory|Site|Campus)\b'
        
        # Pattern 2: "facility in City" or "plant at City"
        pattern2 = r'\b(plant|facility|unit|manufacturing|operations)\s+(?:in|at|located in|located at)\s+(Sanand|Hosur|Chakan|Manesar|Pune|Ahmedabad|Chennai|Bangalore|Bengaluru|Navagam|Bawal|Haridwar|Noida|Gurgaon|Gurugram|Hyderabad|Mumbai|Dharuhera)\b'
        
        # Pattern 3: "Division City" (e.g., "Wiring Sanand")
        pattern3 = r'\b(Wiring|Vision|Seating|Polymers|Logistics)\s+(?:Systems?\s+)?(Sanand|Hosur|Chakan|Manesar|Pune|Ahmedabad|Chennai|Bangalore|Bengaluru|Navagam|Bawal|Haridwar|Noida)\b'
        
        # Pattern 4: Context-based
        pattern4 = r'(?:announced|established|set up|commissioned|operates|inaugurated?)(?:\s+\w+){0,8}\s+(?:plant|facility|unit|manufacturing)?\s*(?:in|at)\s+(Sanand|Hosur|Chakan|Manesar|Pune|Ahmedabad|Chennai|Bangalore|Bengaluru|Navagam|Bawal|Haridwar|Noida|Gurgaon|Gurugram|Hyderabad|Mumbai)\b'
        
        # Pattern 5: "MSWIL/SMR/SMP City"
        pattern5 = r'\b(MSWIL|SMR|SMP|PKC)\s+(?:plant|facility|unit)?\s*(?:at|in)?\s*(Sanand|Hosur|Chakan|Manesar|Pune|Ahmedabad|Chennai|Bangalore|Bengaluru|Navagam|Bawal|Haridwar|Noida)\b'
        
        # Pattern 6: "City, State" format
        pattern6 = r'\b(Sanand|Hosur|Chakan|Manesar|Pune|Ahmedabad|Chennai|Bangalore|Bengaluru|Navagam|Bawal|Haridwar|Noida|Gurgaon|Gurugram|Hyderabad|Mumbai),\s*(Gujarat|Tamil Nadu|Maharashtra|Haryana|Karnataka|Uttar Pradesh|Uttarakhand|Telangana)\b'
        
        # Pattern 7: Table-like patterns
        pattern7 = r'(?:plant|facility|unit|manufacturing|site)\s*[:\-]\s*(Sanand|Hosur|Chakan|Manesar|Pune|Ahmedabad|Chennai|Bangalore|Bengaluru|Navagam|Bawal|Haridwar|Noida)\b'
        
        all_patterns = [pattern1, pattern2, pattern3, pattern4, pattern5, pattern6, pattern7]
        
        for pattern in all_patterns:
            for match in re.finditer(pattern, text, re.IGNORECASE):
                matched_text = match.group(0)
                
                # Extract city
                city = None
                for city_name in self.indian_cities:
                    if city_name.lower() in matched_text.lower():
                        city = city_name
                        break
                
                if not city:
                    continue
                
                # Build facility name
                facility_name = None
                
                if pattern == pattern3:
                    division = match.group(1)
                    facility_name = f"{division} {city} Plant"
                elif pattern == pattern5:
                    prefix = match.group(1)
                    facility_name = f"{prefix} {city} Plant"
                elif 'MSWIL' in matched_text.upper():
                    facility_name = f"MSWIL {city} Plant"
                elif 'SMR' in matched_text.upper():
                    facility_name = f"SMR {city} Plant"
                elif 'SMP' in matched_text.upper():
                    facility_name = f"SMP {city} Plant"
                else:
                    facility_name = f"{city} Plant"
                
                if self._is_valid_facility_name(facility_name):
                    facilities.append({
                        'text': facility_name,
                        'label': 'FACILITY',
                        'start': match.start(),
                        'end': match.end()
                    })
        
        # Deduplicate
        unique_facilities = {}
        for fac in facilities:
            if fac['text'] not in unique_facilities:
                unique_facilities[fac['text']] = fac
        
        return list(unique_facilities.values())
    
    def _is_valid_facility_name(self, name: str) -> bool:
        """Check if facility name is valid"""
        name_lower = name.lower()
        
        if len(name) < 5 or len(name) > 100:
            return False
        
        if re.match(r'^\d+', name):
            return False
        
        for pattern in self.facility_blacklist:
            if re.search(pattern, name_lower):
                return False
        
        return True
    
    def extract_divisions(self, text: str) -> List[Dict]:
        """Extract divisions"""
        divisions = []
        
        for pattern in self.division_patterns:
            for match in re.finditer(pattern, text, re.IGNORECASE):
                div_name = match.group(0).strip()
                
                # Normalize
                div_upper = div_name.upper()
                normalized_name = None
                
                for abbr, full_name in self.division_map.items():
                    if abbr in div_upper:
                        normalized_name = full_name
                        break
                
                if not normalized_name:
                    if 'wiring' in div_name.lower():
                        normalized_name = 'Wiring Systems'
                    elif 'vision' in div_name.lower():
                        normalized_name = 'Vision Systems'
                    elif 'seating' in div_name.lower():
                        normalized_name = 'Seating Systems'
                    elif 'polymer' in div_name.lower():
                        normalized_name = 'Polymers'
                    elif 'logistic' in div_name.lower():
                        normalized_name = 'Logistics'
                    else:
                        normalized_name = div_name
                
                divisions.append({
                    'text': normalized_name,
                    'label': 'DIVISION',
                    'start': match.start(),
                    'end': match.end()
                })
        
        # Deduplicate
        unique_divisions = {}
        for div in divisions:
            if div['text'] not in unique_divisions:
                unique_divisions[div['text']] = div
        
        return list(unique_divisions.values())
    
    def extract_locations(self, text: str) -> List[Dict]:
        """Extract locations"""
        locations = []
        
        # Extract states
        for state in self.indian_states:
            for match in re.finditer(r'\b' + re.escape(state) + r'\b', text):
                locations.append({
                    'text': state,
                    'label': 'LOCATION',
                    'start': match.start(),
                    'end': match.end()
                })
        
        # Extract cities
        for city in self.indian_cities:
            for match in re.finditer(r'\b' + re.escape(city) + r'\b', text):
                locations.append({
                    'text': city,
                    'label': 'LOCATION',
                    'start': match.start(),
                    'end': match.end()
                })
        
        # City, State pattern
        city_state_pattern = r'\b([A-Z][a-z]{2,}),\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\b'
        for match in re.finditer(city_state_pattern, text):
            city = match.group(1).strip()
            state = match.group(2).strip()
            
            if city in self.indian_cities or state in self.indian_states:
                locations.append({
                    'text': match.group(0).strip(),
                    'label': 'LOCATION',
                    'start': match.start(),
                    'end': match.end()
                })
        
        # Deduplicate
        unique_locations = {}
        for loc in locations:
            if loc['text'] not in unique_locations:
                unique_locations[loc['text']] = loc
        
        return list(unique_locations.values())
    
    def extract_status(self, text: str) -> List[Dict]:
        """Extract status mentions"""
        statuses = []
        for status_type, pattern in self.status_patterns.items():
            for match in re.finditer(pattern, text, re.IGNORECASE):
                statuses.append({
                    'text': status_type,
                    'label': 'STATUS',
                    'start': match.start(),
                    'end': match.end()
                })
        return statuses
    
    def extract_events(self, text: str) -> List[Dict]:
        """Extract event mentions"""
        events = []
        for pattern in self.event_patterns:
            for match in re.finditer(pattern, text, re.IGNORECASE):
                events.append({
                    'text': match.group(0).strip(),
                    'label': 'EVENT',
                    'start': match.start(),
                    'end': match.end()
                })
        return events
    
    def extract_dates(self, text: str) -> List[Dict]:
        """Extract and normalize dates"""
        dates = []
        
        for pattern in self.date_patterns:
            for match in re.finditer(pattern, text, re.IGNORECASE):
                date_text = match.group(0).strip()
                normalized_date = self._normalize_date(date_text)
                
                if normalized_date:
                    dates.append({
                        'text': normalized_date,
                        'label': 'DATE',
                        'start': match.start(),
                        'end': match.end()
                    })
        
        # Deduplicate
        unique_dates = {}
        for date in dates:
            if date['text'] not in unique_dates:
                unique_dates[date['text']] = date
        
        return list(unique_dates.values())
    
    def _normalize_date(self, date_text: str) -> str:
        """Normalize date to ISO format"""
        date_text = date_text.strip()
        
        try:
            # FY format
            if date_text.upper().startswith('FY'):
                year_match = re.search(r'\d{4}', date_text)
                if year_match:
                    year = year_match.group(0)
                    return f"{year}-03-31"
                return None
        except Exception as e:
            logger.warning(f"Error processing FY format for '{date_text}': {e}")
            return None
            
            # Quarter format
            quarter_match = re.match(r'Q([1-4])\s+(\d{4})', date_text, re.IGNORECASE)
            if quarter_match:
                quarter = int(quarter_match.group(1))
                year = quarter_match.group(2)
                month = quarter * 3
                return f"{year}-{month:02d}-01"
            
            # Half-year format
            half_match = re.match(r'H([1-2])\s+(?:FY\s*)?(\d{4})', date_text, re.IGNORECASE)
            if half_match:
                half = int(half_match.group(1))
                year = half_match.group(2)
                month = 6 if half == 1 else 12
                return f"{year}-{month:02d}-01"
            
            # Month Year format
            month_year = re.match(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})', date_text, re.IGNORECASE)
            if month_year:
                months = ['January', 'February', 'March', 'April', 'May', 'June',
                         'July', 'August', 'September', 'October', 'November', 'December']
                month = months.index(month_year.group(1).capitalize()) + 1
                year = month_year.group(2)
                return f"{year}-{month:02d}-01"
            
            # Year only
            if re.match(r'^\d{4}', date_text):
                year = int(date_text)
                if 2010 <= year <= datetime.datetime.now().year + 10:
                    return f"{date_text}-01-01"
            
            return None
            
        except Exception as e:
            logger.warning(f"Date normalization failed for '{date_text}': {e}")
            return None
    
    def extract_job_titles(self, text: str) -> List[Dict]:
        """Extract job titles with locations - ENHANCED VERSION"""
        jobs = []
        
        # ENHANCED job patterns WITH location capture
        job_patterns = [
            # Pattern: "Title - Location" or "Title | Location"
            r'\b(Plant|Production|Manufacturing|Assembly|Quality|Maintenance|Warehouse|Logistics|Tool|Die|Mold|Process|Line)\s+(Manager|Engineer|Supervisor|Operator|Technician|Coordinator|Specialist|Planner|Designer|Controller|Lead|Head|In-charge)(?:\s*[-|]\s*|\s+at\s+|\s+in\s+|\s+-\s+)?(Chennai|Pune|Bangalore|Bengaluru|Hosur|Sanand|Manesar|Ahmedabad|Mumbai|Hyderabad|Tamil Nadu|Gujarat|Maharashtra|Haryana|Karnataka|India)?\b',
            
            # Pattern: "Senior/Junior Title" with location
            r'\b(Senior|Junior|Lead|Chief|Deputy|Assistant|Sr\.|Jr\.)\s+\w+\s+(Engineer|Manager|Supervisor|Coordinator|Technician|Specialist)(?:\s*[-|]\s*|\s+at\s+|\s+in\s+|\s+-\s+)?(Chennai|Pune|Bangalore|Bengaluru|Hosur|Sanand|Manesar|Ahmedabad|Mumbai|Hyderabad)?\b',
            
            # Pattern: "Role Technician/Operator"
            r'\b\w+\s+(Operator|Technician|Mechanic|Fitter|Welder|Assembler|Inspector|Machinist)(?:\s*[-|]\s*|\s+at\s+|\s+in\s+|\s+-\s+)?(Chennai|Pune|Bangalore|Bengaluru|Hosur|Sanand|Manesar|Ahmedabad)?\b',
            
            # Pattern: "Shift/Floor Manager"
            r'\b(Shift|Floor|Line|Production|Process|Material)\s+(Manager|Supervisor|Coordinator|In-charge|Lead|Engineer)(?:\s*[-|]\s*|\s+at\s+|\s+in\s+|\s+-\s+)?(Chennai|Pune|Bangalore|Bengaluru|Hosur|Sanand|Manesar)?\b',
            
            # Pattern: Additional factory roles
            r'\b(Inventory|Supply Chain|Stamping|Welding|Painting|Injection|Molding)\s+(Engineer|Manager|Supervisor|Technician|Specialist)(?:\s*[-|]\s*|\s+at\s+|\s+in\s+)?(Chennai|Pune|Bangalore|Bengaluru|Hosur|Sanand)?\b',
        ]
        
        for pattern in job_patterns:
            for match in re.finditer(pattern, text, re.IGNORECASE):
                # Extract title
                if match.lastindex and match.lastindex >= 2:
                    title_parts = [match.group(i) for i in range(1, min(match.lastindex, 3))]
                    title = ' '.join(p for p in title_parts if p).strip()
                else:
                    title = match.group(0).strip()
                
                # Extract location if captured
                location = None
                if match.lastindex and match.lastindex >= 3 and match.group(match.lastindex):
                    location = match.group(match.lastindex).strip()
                
                # Search for location in nearby context if not captured
                if not location:
                    context_end = min(match.end() + 200, len(text))
                    context = text[match.end():context_end]
                    
                    # Look for city names
                    for city in self.indian_cities:
                        if city in context:
                            location = city
                            break
                    
                    # Look for state names
                    if not location:
                        for state in self.indian_states:
                            if state in context:
                                location = state
                                break
                
                title_lower = title.lower()
                
                # Check if it's a factory role
                is_factory = any(kw in title_lower for kw in self.factory_job_keywords)
                is_non_factory = any(kw in title_lower for kw in self.non_factory_keywords)
                
                if is_factory and not is_non_factory:
                    jobs.append({
                        'text': title,
                        'location': location or 'India',
                        'label': 'JOB_TITLE',
                        'start': match.start(),
                        'end': match.end(),
                        'is_factory_role': True
                    })
        
        return jobs
    
    def extract_entities(self, text: str) -> Dict[str, List[Dict]]:
        """Extract all entities"""
        if not self.nlp:
            logger.error("NLP model not loaded")
            return {}
        
        logger.info(f"Extracting entities from {len(text)} chars")
        
        entities = {
            'facilities': self.extract_facilities(text),
            'divisions': self.extract_divisions(text),
            'locations': self.extract_locations(text),
            'status': self.extract_status(text),
            'events': self.extract_events(text),
            'dates': self.extract_dates(text),
            'job_titles': self.extract_job_titles(text)
        }
        
        total = sum(len(v) for v in entities.values())
        logger.info(f"Extracted {total} entities total")
        return entities  # Ensure all entities are returned properly