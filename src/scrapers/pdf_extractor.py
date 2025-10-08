"""
Enhanced PDF Extractor - CRITICAL FIX FOR QUERY 2
Now extracts expansion/greenfield data from MSWIL Annual Report
"""
import logging
import re
from datetime import datetime
from typing import Dict, Optional, List
from io import BytesIO
import pdfplumber

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PDFExtractor:
    def __init__(self):
        # Division mapping
        self.division_map = {
            'MSWIL': 'Wiring Systems',
            'MSW': 'Wiring Systems',
            'WIRING': 'Wiring Systems',
            'HARNESS': 'Wiring Systems',
            'SMR': 'Vision Systems',
            'SAMVARDHANA': 'Vision Systems',
            'VISION': 'Vision Systems',
            'MIRRORS': 'Vision Systems',
            'SMP': 'Polymers',
            'POLYMER': 'Polymers',
            'PKC': 'Wiring Systems',
            'SEATING': 'Seating Systems',
            'LOGISTICS': 'Logistics'
        }
        
        # Indian cities to states mapping
        self.city_to_state = {
            'Sanand': 'Gujarat', 'Ahmedabad': 'Gujarat', 'Navagam': 'Gujarat',
            'Pune': 'Maharashtra', 'Chakan': 'Maharashtra', 'Mumbai': 'Maharashtra',
            'Chennai': 'Tamil Nadu', 'Hosur': 'Tamil Nadu',
            'Bangalore': 'Karnataka', 'Bengaluru': 'Karnataka',
            'Manesar': 'Haryana', 'Gurgaon': 'Haryana', 'Gurugram': 'Haryana',
            'Noida': 'Uttar Pradesh', 'Haridwar': 'Uttarakhand',
            'Bawal': 'Haryana', 'Dharuhera': 'Haryana',
            'Hyderabad': 'Telangana', 'Kolkata': 'West Bengal'
        }
        
        # CRITICAL: Expansion keywords for Query 2
        self.expansion_keywords = [
            'new plant', 'new facility', 'greenfield', 'brownfield',
            'expansion', 'phase', 'establishing', 'set up', 'setting up',
            'commenced operations', 'expected to commence', 'operational',
            'inaugurated', 'announced', 'upcoming', 'proposed'
        ]
    
    def extract_from_bytes(self, pdf_content: bytes, url: str) -> Optional[Dict]:
        """Extract comprehensive data from PDF with EXPANSION FOCUS"""
        try:
            full_text = []
            facilities = []
            expansions = []  # NEW: Track expansion-specific data
            
            with pdfplumber.open(BytesIO(pdf_content)) as pdf:
                # Extract metadata
                metadata = pdf.metadata or {}
                title = metadata.get('Title', url)
                
                for page_num, page in enumerate(pdf.pages, 1):
                    # Extract text
                    page_text = page.extract_text()
                    if page_text:
                        full_text.append(f"[Page {page_num}]\n{page_text}")
                        
                        # CRITICAL: Extract expansion mentions from text
                        page_expansions = self._extract_expansions_from_text(page_text, page_num)
                        expansions.extend(page_expansions)
                    
                    # Extract tables
                    tables = page.extract_tables()
                    if tables:
                        for table_idx, table in enumerate(tables):
                            table_facilities = self._parse_facility_table(table, page_num)
                            facilities.extend(table_facilities)
                            
                            # Add table to text
                            table_text = self._table_to_text(table)
                            full_text.append(f"\n[Table {table_idx+1} on Page {page_num}]\n{table_text}")
                
                # Parse creation date
                creation_date = metadata.get('CreationDate')
                publish_date = None
                if creation_date:
                    try:
                        date_str = creation_date.replace('D:', '').split('+')[0][:8]
                        if len(date_str) == 8:
                            publish_date = f"{date_str[0:4]}-{date_str[4:6]}-{date_str[6:8]}"
                    except:
                        pass
                
                combined_text = '\n\n'.join(full_text)
                
                # Extract inline facilities
                inline_facilities = self._extract_inline_facilities(combined_text)
                facilities.extend(inline_facilities)
                
                # CRITICAL: Merge expansion data with facilities
                facilities = self._merge_expansion_data(facilities, expansions)
                
                # Add structured data to text
                if facilities:
                    combined_text += "\n\n=== EXTRACTED FACILITIES ===\n"
                    for fac in facilities:
                        combined_text += f"Facility: {fac['name']}\n"
                        combined_text += f"  Division: {fac.get('division', 'Unknown')}\n"
                        combined_text += f"  Location: {fac.get('city', 'N/A')}, {fac.get('state', 'N/A')}\n"
                        combined_text += f"  Status: {fac.get('status', 'operational')}\n"
                        
                        # CRITICAL: Add expansion info
                        if fac.get('expansion_type'):
                            combined_text += f"  Expansion Type: {fac['expansion_type']}\n"
                        if fac.get('date'):
                            combined_text += f"  Date: {fac['date']}\n"
                        if fac.get('timeline'):
                            combined_text += f"  Timeline: {fac['timeline']}\n"
                        
                        combined_text += "\n"
                
                return {
                    'url': url,
                    'text': combined_text,
                    'title': title,
                    'fetched_at': datetime.now().isoformat(),
                    'mime': 'application/pdf',
                    'publish_dt': publish_date,
                    'structured_facilities': facilities
                }
            
        except Exception as e:
            logger.error(f"PDF extraction error: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _extract_expansions_from_text(self, text: str, page_num: int) -> List[Dict]:
        """CRITICAL: Extract expansion/greenfield mentions from text"""
        expansions = []
        
        # Pattern 1: "establishing X new plants in Y"
        pattern1 = r'establishing\s+(\w+)\s+new\s+plants?\s+in\s+(\w+(?:\s+\([^)]+\))?(?:\s+and\s+\w+(?:\s+\([^)]+\))?)?)'
        
        # Pattern 2: "new plant in City"
        pattern2 = r'new\s+(?:plant|facility|unit)\s+in\s+(\w+)'
        
        # Pattern 3: "City plant... operations in FY"
        pattern3 = r'(\w+)\s+plant[^.]*(?:commence|start|begin)\s+operations\s+in\s+(FY\s*\d{4}[-–]\d{2,4}|Q\d\s+\d{4}|\d{4})'
        
        # Pattern 4: "greenfield/brownfield in City"
        pattern4 = r'(greenfield|brownfield)[^.]*in\s+(\w+)'
        
        # Pattern 5: Specific MSWIL expansion text
        pattern5 = r'two\s+new\s+plants\s+in\s+([^.]+)\s+equipped\s+to\s+manufacture'
        
        all_patterns = [pattern1, pattern2, pattern3, pattern4, pattern5]
        
        for pattern in all_patterns:
            for match in re.finditer(pattern, text, re.IGNORECASE):
                # Extract location and timeline
                matched_text = match.group(0)
                
                # Try to find cities
                cities = []
                for city in self.city_to_state.keys():
                    if city.lower() in matched_text.lower():
                        cities.append(city)
                
                # If pattern1, extract from groups
                if pattern == pattern1:
                    try:
                        count = match.group(1)
                        locations = match.group(2)
                        
                        # Parse "Navagam (Gujarat) and Pune (Maharashtra)"
                        location_parts = re.findall(r'(\w+)\s*\(([^)]+)\)', locations)
                        if location_parts:
                            for city, state in location_parts:
                                cities.append(city)
                    except:
                        pass
                
                # Extract timeline
                timeline = self._extract_timeline_from_context(text, match.start(), match.end())
                
                # Create expansion entries
                if cities:
                    for city in cities:
                        state = self.city_to_state.get(city)
                        expansions.append({
                            'city': city,
                            'state': state,
                            'expansion_type': 'greenfield' if 'greenfield' in matched_text.lower() or 'new plant' in matched_text.lower() else 'brownfield',
                            'status': self._infer_status_from_text(matched_text),
                            'timeline': timeline,
                            'page': page_num,
                            'context': matched_text
                        })
                        logger.info(f"  🔍 Found expansion: {city}, {state} - {timeline}")
        
        return expansions
    
    def _extract_timeline_from_context(self, text: str, start: int, end: int) -> Optional[str]:
        """Extract timeline from surrounding context"""
        # Get context window
        context_start = max(0, start - 500)
        context_end = min(len(text), end + 500)
        context = text[context_start:context_end]
        
        # Timeline patterns
        patterns = [
            r'FY\s*(\d{4})[-–](\d{2,4})',
            r'Q(\d)\s+(\d{4})',
            r'(expected to|will|planned to)\s+commence\s+operations\s+in\s+(FY\s*\d{4}[-–]\d{2,4})',
            r'operational\s+(?:by|in)\s+(FY\s*\d{4}[-–]\d{2,4}|Q\d\s+\d{4}|\d{4})',
            r'(\d{4})[-–](\d{2,4})'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, context, re.IGNORECASE)
            if match:
                return match.group(0)
        
        return None
    
    def _infer_status_from_text(self, text: str) -> str:
        """Infer facility status from text"""
        text_lower = text.lower()
        
        if any(kw in text_lower for kw in ['expected to commence', 'will commence', 'planned', 'upcoming', 'proposed']):
            return 'planned'
        elif any(kw in text_lower for kw in ['under construction', 'being established', 'setting up']):
            return 'under-construction'
        elif any(kw in text_lower for kw in ['operational', 'commenced', 'inaugurated']):
            return 'operational'
        
        return 'planned'
    
    def _merge_expansion_data(self, facilities: List[Dict], expansions: List[Dict]) -> List[Dict]:
        """Merge expansion data into facilities"""
        # Create map of cities to expansion data
        expansion_map = {}
        for exp in expansions:
            city = exp.get('city')
            if city:
                if city not in expansion_map or exp.get('timeline'):
                    expansion_map[city] = exp
        
        # Merge into facilities
        for fac in facilities:
            city = fac.get('city')
            if city and city in expansion_map:
                exp = expansion_map[city]
                
                # Add expansion info if not already present
                if not fac.get('expansion_type'):
                    fac['expansion_type'] = exp.get('expansion_type')
                if not fac.get('timeline'):
                    fac['timeline'] = exp.get('timeline')
                
                # Update status if expansion is more specific
                if exp.get('status') and exp['status'] != 'operational':
                    fac['status'] = exp['status']
        
        # Add standalone expansions that don't match existing facilities
        for exp in expansions:
            city = exp.get('city')
            
            # Check if already in facilities
            exists = any(f.get('city') == city for f in facilities)
            
            if not exists:
                # Create new facility entry from expansion
                facilities.append({
                    'name': f"{city} Plant",
                    'division': 'Wiring Systems',  # Default from MSWIL report
                    'city': city,
                    'state': exp.get('state'),
                    'status': exp.get('status', 'planned'),
                    'expansion_type': exp.get('expansion_type'),
                    'timeline': exp.get('timeline'),
                    'date': self._parse_date(exp.get('timeline', '')),
                    'source_type': 'expansion',
                    'page': exp.get('page')
                })
        
        return facilities
    
    def _parse_facility_table(self, table: List[List], page_num: int) -> List[Dict]:
        """Parse facility data from table"""
        facilities = []
        
        if not table or len(table) < 2:
            return facilities
        
        # Try to identify header row
        header = table[0]
        header_lower = [str(cell).lower() if cell else '' for cell in header]
        
        # Find column indices
        facility_col = self._find_column(header_lower, ['facility', 'plant', 'location', 'unit', 'site'])
        division_col = self._find_column(header_lower, ['division', 'business', 'segment'])
        city_col = self._find_column(header_lower, ['city', 'location', 'place'])
        state_col = self._find_column(header_lower, ['state', 'region'])
        status_col = self._find_column(header_lower, ['status', 'stage', 'phase'])
        date_col = self._find_column(header_lower, ['date', 'year', 'commissioned', 'operational'])
        
        # Parse data rows
        for row_idx, row in enumerate(table[1:], 1):
            if not row or len(row) < 2:
                continue
            
            facility_data = {}
            
            # Extract facility name
            facility_name = None
            if facility_col is not None and facility_col < len(row):
                facility_name = self._clean_cell(row[facility_col])
            
            if not facility_name or len(facility_name) < 3:
                continue
            
            facility_data['name'] = facility_name
            
            # Extract division
            if division_col is not None and division_col < len(row):
                division = self._clean_cell(row[division_col])
                facility_data['division'] = self._map_division(division)
            else:
                facility_data['division'] = self._infer_division(facility_name)
            
            # Extract location
            city = None
            state = None
            
            if city_col is not None and city_col < len(row):
                city = self._clean_cell(row[city_col])
            
            if state_col is not None and state_col < len(row):
                state = self._clean_cell(row[state_col])
            
            # Parse combined location
            if city and ',' in city:
                parts = [p.strip() for p in city.split(',')]
                city = parts[0]
                if len(parts) > 1 and not state:
                    state = parts[1]
            
            # Infer state from city
            if city and not state and city in self.city_to_state:
                state = self.city_to_state[city]
            
            facility_data['city'] = city
            facility_data['state'] = state
            
            # Extract status
            if status_col is not None and status_col < len(row):
                status = self._clean_cell(row[status_col])
                facility_data['status'] = self._normalize_status(status)
            else:
                facility_data['status'] = 'operational'
            
            # Extract date
            if date_col is not None and date_col < len(row):
                date_text = self._clean_cell(row[date_col])
                facility_data['date'] = self._parse_date(date_text)
            
            facility_data['page'] = page_num
            facility_data['source_type'] = 'table'
            
            facilities.append(facility_data)
        
        return facilities
    
    def _extract_inline_facilities(self, text: str) -> List[Dict]:
        """Extract facilities from inline text"""
        facilities = []
        
        # Enhanced patterns
        pattern1 = r'\b(Sanand|Hosur|Chakan|Manesar|Pune|Ahmedabad|Chennai|Bangalore|Bengaluru|Navagam|Bawal|Haridwar|Noida|Gurgaon|Gurugram|Hyderabad|Mumbai)\s+(Plant|Facility|Unit|Manufacturing|Operations|Factory)\b'
        pattern2 = r'\b(MSWIL|SMR|SMP|PKC)\s+(Sanand|Hosur|Chakan|Manesar|Pune|Ahmedabad|Chennai|Bangalore|Bengaluru|Navagam|Bawal|Haridwar|Noida)\b'
        pattern3 = r'\b(plant|facility|unit|manufacturing|operations)\s+(?:in|at|located in|located at)\s+(Sanand|Hosur|Chakan|Manesar|Pune|Ahmedabad|Chennai|Bangalore|Bengaluru|Navagam|Bawal|Haridwar|Noida|Gurgaon|Gurugram|Hyderabad|Mumbai)'
        
        all_patterns = [pattern1, pattern2, pattern3]
        
        for pattern in all_patterns:
            for match in re.finditer(pattern, text, re.IGNORECASE):
                matched_text = match.group(0)
                
                # Extract city
                city = None
                for city_name in self.city_to_state.keys():
                    if city_name.lower() in matched_text.lower():
                        city = city_name
                        break
                
                if not city:
                    continue
                
                # Build facility name
                if 'MSWIL' in matched_text.upper():
                    facility_name = f"MSWIL {city} Plant"
                elif 'SMR' in matched_text.upper():
                    facility_name = f"SMR {city} Plant"
                elif 'SMP' in matched_text.upper():
                    facility_name = f"SMP {city} Plant"
                else:
                    facility_name = f"{city} Plant"
                
                # Infer division
                division = self._infer_division(matched_text)
                
                # Get state
                state = self.city_to_state.get(city)
                
                # Find nearby date and status
                context_start = max(0, match.start() - 300)
                context_end = min(len(text), match.end() + 300)
                context = text[context_start:context_end]
                
                date = self._find_date_in_context(context)
                status = self._find_status_in_context(context)
                
                facilities.append({
                    'name': facility_name,
                    'division': division,
                    'city': city,
                    'state': state,
                    'status': status or 'operational',
                    'date': date,
                    'source_type': 'inline'
                })
        
        # Deduplicate
        unique_facilities = {}
        for fac in facilities:
            key = (fac['name'].lower(), fac.get('city', '').lower())
            if key not in unique_facilities:
                unique_facilities[key] = fac
        
        return list(unique_facilities.values())
    
    def _find_column(self, header: List[str], keywords: List[str]) -> Optional[int]:
        """Find column index matching keywords"""
        for idx, cell in enumerate(header):
            if any(kw in cell for kw in keywords):
                return idx
        return None
    
    def _clean_cell(self, cell) -> str:
        """Clean table cell"""
        if cell is None:
            return ''
        text = str(cell).strip()
        text = re.sub(r'\s+', ' ', text)
        return text
    
    def _map_division(self, division_text: str) -> str:
        """Map division abbreviations to full names"""
        if not division_text:
            return 'Unknown'
        
        division_upper = division_text.upper()
        
        for abbr, full_name in self.division_map.items():
            if abbr in division_upper:
                return full_name
        
        if 'WIRING' in division_upper or 'HARNESS' in division_upper:
            return 'Wiring Systems'
        elif 'VISION' in division_upper or 'MIRROR' in division_upper:
            return 'Vision Systems'
        elif 'POLYMER' in division_upper:
            return 'Polymers'
        elif 'SEATING' in division_upper:
            return 'Seating Systems'
        elif 'LOGISTIC' in division_upper:
            return 'Logistics'
        
        return division_text.title()
    
    def _infer_division(self, text: str) -> str:
        """Infer division from text"""
        text_upper = text.upper()
        
        for abbr, full_name in self.division_map.items():
            if abbr in text_upper:
                return full_name
        
        return 'Unknown'
    
    def _normalize_status(self, status_text: str) -> str:
        """Normalize status"""
        if not status_text:
            return 'operational'
        
        status_lower = status_text.lower()
        
        if any(kw in status_lower for kw in ['plan', 'propos', 'upcom', 'futur', 'announc']):
            return 'planned'
        elif any(kw in status_lower for kw in ['construction', 'building', 'develop']):
            return 'under-construction'
        else:
            return 'operational'
    
    def _parse_date(self, date_text: str) -> Optional[str]:
        """Parse date from text"""
        if not date_text:
            return None
        
        try:
            # FY format
            fy_match = re.search(r'FY\s*(\d{4})', date_text, re.IGNORECASE)
            if fy_match:
                year = fy_match.group(1)
                return f"{year}-03-31"
            
            # Quarter format
            q_match = re.search(r'Q([1-4])\s+(\d{4})', date_text, re.IGNORECASE)
            if q_match:
                quarter = int(q_match.group(1))
                year = q_match.group(2)
                month = quarter * 3
                return f"{year}-{month:02d}-01"
            
            # Year only
            year_match = re.search(r'\b(20\d{2})\b', date_text)
            if year_match:
                year = int(year_match.group(1))
                if 2010 <= year <= datetime.now().year + 5:
                    return f"{year}-01-01"
            
            return None
        
        except Exception as e:
            logger.warning(f"Date parsing failed for '{date_text}': {e}")
            return None
    
    def _find_date_in_context(self, context: str) -> Optional[str]:
        """Find date in surrounding context"""
        date_patterns = [
            r'FY\s*(\d{4})',
            r'Q([1-4])\s+(\d{4})',
            r'\b(20\d{2})\b'
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, context, re.IGNORECASE)
            if match:
                return self._parse_date(match.group(0))
        
        return None
    
    def _find_status_in_context(self, context: str) -> Optional[str]:
        """Find status in surrounding context"""
        context_lower = context.lower()
        
        if any(kw in context_lower for kw in ['planned', 'proposed', 'upcoming', 'future', 'announced', 'expected to commence']):
            return 'planned'
        elif any(kw in context_lower for kw in ['construction', 'building', 'under development', 'establishing']):
            return 'under-construction'
        elif any(kw in context_lower for kw in ['operational', 'operating', 'commissioned', 'inaugurated']):
            return 'operational'
        
        return None
    
    def _table_to_text(self, table: List[List]) -> str:
        """Convert table to readable text"""
        lines = []
        for row in table:
            line = ' | '.join([str(cell) if cell else '' for cell in row])
            lines.append(line)
        return '\n'.join(lines)
    
    def extract_from_file(self, filepath: str) -> Optional[Dict]:
        """Extract from file path"""
        try:
            with open(filepath, 'rb') as f:
                return self.extract_from_bytes(f.read(), filepath)
        except Exception as e:
            logger.error(f"File read error: {e}")
            return None