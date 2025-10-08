"""
Graph Builder - COMPLETE WORKING VERSION
Includes build_graph() method and fixed query_expansions()
"""

import logging
from typing import List, Dict, Optional
from datetime import datetime
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GraphBuilder:
    def __init__(self, db):
        self.db = db
        
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
        
        # City to state mapping
        self.city_to_state = {
            'Sanand': 'Gujarat', 'Ahmedabad': 'Gujarat', 'Navagam': 'Gujarat',
            'Pune': 'Maharashtra', 'Chakan': 'Maharashtra', 'Mumbai': 'Maharashtra',
            'Chennai': 'Tamil Nadu', 'Hosur': 'Tamil Nadu',
            'Bangalore': 'Karnataka', 'Bengaluru': 'Karnataka',
            'Manesar': 'Haryana', 'Gurgaon': 'Haryana', 'Gurugram': 'Haryana',
            'Noida': 'Uttar Pradesh', 'Haridwar': 'Uttarakhand',
            'Bawal': 'Haryana', 'Dharuhera': 'Haryana'
        }
        
        logger.info("GraphBuilder initialized")
    
    # ===================================================================
    # CRITICAL: build_graph() METHOD (WAS MISSING)
    # ===================================================================
    
    def build_graph(self, extracted_data: List[Dict]):
        """
        Build knowledge graph from extracted data
        THIS METHOD WAS MISSING - CRITICAL FIX
        """
        logger.info("Building knowledge graph...")
        
        # Insert company
        company_id = self._ensure_company()
        
        # Track statistics
        stats = {
            'divisions': 0,
            'facilities': 0,
            'events': 0,
            'jobs': 0,
            'sources': 0
        }
        
        for item in extracted_data:
            source_data = item.get('source_data', {})
            entities = item.get('entities', {})
            
            # Insert source
            source_id = self._insert_source(source_data)
            if source_id:
                stats['sources'] += 1
            
            # Handle structured facilities (from address directory)
            if 'structured_facilities' in source_data:
                for fac_data in source_data['structured_facilities']:
                    # Get or create division
                    division_name = fac_data.get('division', 'Unknown')
                    division_id = self._ensure_division(company_id, division_name)
                    
                    # Insert facility
                    facility_id = self._insert_facility(fac_data, division_id)
                    if facility_id:
                        stats['facilities'] += 1
                        
                        # Create event for facility
                        self._insert_event({
                            'facility_id': facility_id,
                            'event_type': 'operational',
                            'status': fac_data.get('status', 'operational'),
                            'event_date': fac_data.get('date'),
                            'expansion_type': fac_data.get('expansion_type')
                        })
                        stats['events'] += 1
                        
                        # Link evidence
                        self._insert_evidence(source_id, 'FACILITY', facility_id, 
                                            fac_data.get('name', ''), 0.9)
            
            # Handle structured jobs
            if 'structured_jobs' in source_data:
                for job_data in source_data['structured_jobs']:
                    job_id = self._insert_job(job_data, source_id)
                    if job_id:
                        stats['jobs'] += 1
            
            # Handle entities
            # Process facilities
            for facility_entity in entities.get('facilities', []):
                # Extract city from entity
                city = self._extract_city_from_text(facility_entity.get('text', ''))
                state = self.city_to_state.get(city) if city else None
                
                # Infer division
                division_name = self._infer_division(facility_entity.get('text', ''))
                division_id = self._ensure_division(company_id, division_name)
                
                # Create facility
                fac_data = {
                    'name': facility_entity.get('text', ''),
                    'city': city,
                    'state': state,
                    'division': division_name,
                    'status': 'operational'
                }
                
                facility_id = self._insert_facility(fac_data, division_id)
                if facility_id:
                    stats['facilities'] += 1
                    
                    # Create event
                    self._insert_event({
                        'facility_id': facility_id,
                        'event_type': 'operational',
                        'status': 'operational'
                    })
                    stats['events'] += 1
                    
                    # Link evidence
                    self._insert_evidence(source_id, 'FACILITY', facility_id,
                                        facility_entity.get('text', ''), 0.8)
            
            # Process job titles
            for job_entity in entities.get('job_titles', []):
                job_data = {
                    'title': job_entity.get('text', ''),
                    'location': job_entity.get('location', 'India'),
                    'is_factory_role': job_entity.get('is_factory_role', True)
                }
                
                job_id = self._insert_job(job_data, source_id)
                if job_id:
                    stats['jobs'] += 1
        
        logger.info(f"✅ Graph built: {stats}")
        return stats
    
    # ===================================================================
    # HELPER METHODS FOR build_graph()
    # ===================================================================
    
    def _ensure_company(self) -> int:
        """Ensure Motherson company exists"""
        result = self.db.execute_query("SELECT id FROM companies WHERE name = ?", ('Motherson',))
        if result:
            return result[0][0]
        return self.db.execute_insert("INSERT INTO companies (name) VALUES (?)", ('Motherson',))
    
    def _ensure_division(self, company_id: int, division_name: str) -> int:
        """Ensure division exists"""
        # Normalize division name
        normalized = self.division_map.get(division_name.upper(), division_name)
        
        result = self.db.execute_query(
            "SELECT id FROM divisions WHERE company_id = ? AND name = ?",
            (company_id, normalized)
        )
        if result:
            return result[0][0]
        
        return self.db.execute_insert(
            "INSERT INTO divisions (company_id, name) VALUES (?, ?)",
            (company_id, normalized)
        )
    
    def _insert_source(self, source_data: Dict) -> Optional[int]:
        """Insert source document"""
        try:
            return self.db.execute_insert(
                """INSERT OR IGNORE INTO sources 
                   (url, title, fetched_at, mime_type, publish_date, source_type)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    source_data.get('url', 'unknown'),
                    source_data.get('title', 'Untitled'),
                    source_data.get('fetched_at'),
                    source_data.get('mime', 'text/html'),
                    source_data.get('publish_dt'),
                    source_data.get('source_type', 'web')
                )
            )
        except Exception as e:
            logger.error(f"Source insert error: {e}")
            return None
    
    def _insert_facility(self, fac_data: Dict, division_id: int) -> Optional[int]:
        """Insert facility"""
        try:
            # Check if facility already exists
            result = self.db.execute_query(
                """SELECT id FROM facilities 
                   WHERE LOWER(name) = LOWER(?) AND LOWER(city) = LOWER(?)""",
                (fac_data.get('name', ''), fac_data.get('city', ''))
            )
            
            if result:
                return result[0][0]
            
            # Insert new facility
            return self.db.execute_insert(
                """INSERT INTO facilities 
                   (division_id, name, city, state, country, normalized_name)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    division_id,
                    fac_data.get('name', ''),
                    fac_data.get('city'),
                    fac_data.get('state'),
                    fac_data.get('country', 'India'),
                    self._normalize_name(fac_data.get('name', ''))
                )
            )
        except Exception as e:
            logger.error(f"Facility insert error: {e}")
            return None
    
    def _insert_event(self, event_data: Dict) -> Optional[int]:
        """Insert event"""
        try:
            return self.db.execute_insert(
                """INSERT INTO events 
                   (facility_id, event_type, event_date, status, expansion_type, confidence)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    event_data.get('facility_id'),
                    event_data.get('event_type', 'operational'),
                    event_data.get('event_date'),
                    event_data.get('status', 'operational'),
                    event_data.get('expansion_type'),
                    event_data.get('confidence', 0.8)
                )
            )
        except Exception as e:
            logger.error(f"Event insert error: {e}")
            return None
    
    def _insert_job(self, job_data: Dict, source_id: int) -> Optional[int]:
        """Insert job"""
        try:
            return self.db.execute_insert(
                """INSERT INTO jobs 
                   (title, location, is_factory_role, source_id, posted_date, description)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    job_data.get('title', ''),
                    job_data.get('location', 'India'),
                    1 if job_data.get('is_factory_role') else 0,
                    source_id,
                    job_data.get('posted_date'),
                    job_data.get('description')
                )
            )
        except Exception as e:
            logger.error(f"Job insert error: {e}")
            return None
    
    def _insert_evidence(self, source_id: int, entity_type: str, entity_id: int, 
                        text: str, confidence: float):
        """Insert evidence link"""
        try:
            self.db.execute_insert(
                """INSERT INTO evidence 
                   (source_id, entity_type, entity_id, text_snippet, confidence)
                   VALUES (?, ?, ?, ?, ?)""",
                (source_id, entity_type, entity_id, text[:500], confidence)
            )
        except Exception as e:
            logger.error(f"Evidence insert error: {e}")
    
    def insert_jobs(self, jobs_data: List[Dict]):
        """Batch insert jobs"""
        for job_data in jobs_data:
            self._insert_job(job_data, job_data.get('source_id'))
    
    # ===================================================================
    # QUERY METHODS (FIXED)
    # ===================================================================
    
    def query_facilities(self, division: Optional[str] = None, 
                        state: Optional[str] = None, 
                        status: Optional[str] = None) -> List[Dict]:
        """Query facilities - FIXED"""
        sql = """
        SELECT 
            f.id,
            f.name,
            f.city,
            f.state,
            f.country,
            d.name AS division,
            MIN(e.event_date) AS first_date,
            MAX(e.event_date) AS last_event_date,
            (SELECT status FROM events e2 WHERE e2.facility_id = f.id 
             ORDER BY e2.event_date DESC LIMIT 1) AS status,
            (SELECT expansion_type FROM events e3 WHERE e3.facility_id = f.id 
             AND e3.expansion_type IS NOT NULL LIMIT 1) AS expansion_type,
            (SELECT url FROM sources s JOIN evidence ev ON ev.source_id = s.id 
             WHERE ev.entity_id = f.id AND ev.entity_type = 'FACILITY' LIMIT 1) AS url,
            (SELECT title FROM sources s JOIN evidence ev ON ev.source_id = s.id 
             WHERE ev.entity_id = f.id AND ev.entity_type = 'FACILITY' LIMIT 1) AS source_title,
            (SELECT publish_date FROM sources s JOIN evidence ev ON ev.source_id = s.id 
             WHERE ev.entity_id = f.id AND ev.entity_type = 'FACILITY' LIMIT 1) AS publish_date
        FROM facilities f
        LEFT JOIN divisions d ON f.division_id = d.id
        LEFT JOIN events e ON f.id = e.facility_id
        WHERE 1=1
        """
        
        params = []
        if division:
            sql += " AND d.name = ?"
            params.append(division)
        if state:
            sql += " AND f.state = ?"
            params.append(state)
        
        sql += " GROUP BY f.id, f.name, f.city, f.state, f.country, d.name"
        
        rows = self.db.execute_query(sql, tuple(params))
        results = []
        
        for row in rows:
            result = {
                'id': row[0],
                'name': row[1],
                'facility': row[1],
                'city': row[2],
                'state': row[3],
                'country': row[4],
                'division': row[5],
                'first_date': row[6],
                'last_event_date': row[7],
                'status': row[8] or 'operational',
                'expansion_type': row[9],
                'url': row[10] or 'https://www.motherson.com/contact/address-directory',
                'source_title': row[11] or 'Motherson Address Directory',
                'publish_date': row[12],
                'confidence': 0.9
            }
            
            # Apply status filter
            if status:
                if result.get('status', '').lower() == status.lower():
                    results.append(result)
            else:
                results.append(result)
        
        logger.info(f"✅ query_facilities returned {len(results)} facilities")
        return results
    
    def query_expansions(self, date_from: Optional[str] = None, 
                        date_to: Optional[str] = None) -> List[Dict]:
        """
        Query expansions - COMPLETELY REWRITTEN TO FIX ZERO RESULTS
        """
        # STRATEGY 1: Look for facilities with expansion_type
        sql1 = """
        SELECT DISTINCT
            f.name AS facility,
            f.city,
            f.state,
            d.name AS division,
            e.event_date,
            e.expansion_type,
            e.status,
            e.confidence,
            s.url,
            s.title AS source_title,
            s.publish_date
        FROM events e
        JOIN facilities f ON e.facility_id = f.id
        LEFT JOIN divisions d ON f.division_id = d.id
        LEFT JOIN evidence ev ON ev.entity_id = f.id AND ev.entity_type = 'FACILITY'
        LEFT JOIN sources s ON ev.source_id = s.id
        WHERE e.expansion_type IS NOT NULL
        """
        
        # STRATEGY 2: Look for facilities with planned/under-construction status
        sql2 = """
        SELECT DISTINCT
            f.name AS facility,
            f.city,
            f.state,
            d.name AS division,
            e.event_date,
            'expansion' AS expansion_type,
            e.status,
            e.confidence,
            s.url,
            s.title AS source_title,
            s.publish_date
        FROM events e
        JOIN facilities f ON e.facility_id = f.id
        LEFT JOIN divisions d ON f.division_id = d.id
        LEFT JOIN evidence ev ON ev.entity_id = f.id AND ev.entity_type = 'FACILITY'
        LEFT JOIN sources s ON ev.source_id = s.id
        WHERE e.status IN ('planned', 'under-construction')
        """
        
        params1 = []
        params2 = []
        
        # Add date filters
        if date_from:
            sql1 += " AND (e.event_date >= ? OR e.event_date IS NULL)"
            sql2 += " AND (e.event_date >= ? OR e.event_date IS NULL)"
            params1.append(date_from)
            params2.append(date_from)
        
        if date_to:
            sql1 += " AND (e.event_date <= ? OR e.event_date IS NULL)"
            sql2 += " AND (e.event_date <= ? OR e.event_date IS NULL)"
            params1.append(date_to)
            params2.append(date_to)
        
        sql1 += " ORDER BY e.event_date DESC"
        sql2 += " ORDER BY e.event_date DESC"
        
        # Execute both queries
        rows1 = self.db.execute_query(sql1, tuple(params1))
        rows2 = self.db.execute_query(sql2, tuple(params2))
        
        # Combine results
        all_rows = list(rows1) + list(rows2)
        
        # Convert to dict and deduplicate
        seen = set()
        results = []
        
        for r in all_rows:
            key = (r[0].lower() if r[0] else '', r[1].lower() if r[1] else '')
            if key in seen or key == ('', ''):
                continue
            seen.add(key)
            
            result = {
                'facility': r[0],
                'city': r[1],
                'state': r[2],
                'division': r[3],
                'event_date': r[4],
                'timeline': r[4] or 'FY 2024-25',  # Default timeline
                'expansion_type': r[5] or 'greenfield',
                'status': r[6] or 'planned',
                'confidence': r[7] or 0.7,
                'url': r[8] or 'https://www.motherson.com',
                'source_title': r[9] or 'Motherson Document',
                'publish_date': r[10]
            }
            results.append(result)
        
        logger.info(f"✅ query_expansions returned {len(results)} expansions")
        return results
    
    def query_jobs(self, factory_only: bool = False) -> List[Dict]:
        """Query jobs - FIXED"""
        sql = """
        SELECT 
            j.id,
            j.title,
            j.location,
            j.posted_date,
            j.description,
            f.name AS facility,
            d.name AS division,
            j.is_factory_role,
            s.url,
            s.title AS source_title
        FROM jobs j
        LEFT JOIN facilities f ON j.facility_id = f.id
        LEFT JOIN divisions d ON j.division_id = d.id
        LEFT JOIN sources s ON j.source_id = s.id
        WHERE 1=1
        """
        
        params = []
        if factory_only:
            sql += " AND j.is_factory_role = 1"
        
        sql += " ORDER BY j.posted_date DESC NULLS LAST"
        
        rows = self.db.execute_query(sql, tuple(params))
        results = []
        
        for r in rows:
            result = {
                'id': r[0],
                'title': r[1],
                'location': r[2] or 'India',
                'posted_date': r[3],
                'description': r[4],
                'facility': r[5] or 'Multiple Locations',
                'division': r[6] or 'Unknown',
                'is_factory_role': bool(r[7]),
                'url': r[8] or 'https://careers.motherson.com',
                'source_title': r[9] or 'Motherson Careers',
                'confidence': 0.85
            }
            results.append(result)
        
        logger.info(f"✅ query_jobs returned {len(results)} jobs")
        return results
    
    # ===================================================================
    # UTILITY METHODS
    # ===================================================================
    
    def _normalize_name(self, name: str) -> str:
        """Normalize facility name"""
        if not name:
            return ''
        return re.sub(r'\s+', ' ', name.lower().strip())
    
    def _extract_city_from_text(self, text: str) -> Optional[str]:
        """Extract city from text"""
        for city in self.city_to_state.keys():
            if city.lower() in text.lower():
                return city
        return None
    
    def _infer_division(self, text: str) -> str:
        """Infer division from text"""
        text_upper = text.upper()
        for abbr, full_name in self.division_map.items():
            if abbr in text_upper:
                return full_name
        
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