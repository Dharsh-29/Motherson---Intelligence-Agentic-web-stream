"""
Entity Resolver - IMPROVED VERSION
Merge duplicate facilities via normalized name + location
"""

import re
import logging
from typing import List, Dict, Tuple, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EntityResolver:
    def __init__(self):
        self.common_suffixes = ['plant', 'facility', 'unit', 'manufacturing', 'factory', 'site']
    
    def normalize_name(self, name: str) -> str:
        """
        Normalize facility name for deduplication
        Example: "Sanand Plant" -> "sanand"
        """
        if not name:
            return ""
        
        name = name.lower().strip()
        name = re.sub(r'\s+', ' ', name)  # Normalize whitespace
        name = re.sub(r'[^\w\s]', '', name)  # Remove punctuation
        
        # Remove common suffixes
        for suffix in self.common_suffixes:
            if name.endswith(' ' + suffix):
                name = name[:-len(suffix)].strip()
        
        return name
    
    def normalize_location(self, city: Optional[str], state: Optional[str]) -> str:
        """
        Normalize location for matching
        Example: "Sanand, Gujarat" -> "sanand gujarat"
        """
        parts = []
        if city:
            parts.append(city.lower().strip())
        if state:
            parts.append(state.lower().strip())
        return ' '.join(parts)
    
    def calculate_similarity(self, name1: str, name2: str) -> float:
        """
        Simple token-based similarity
        Returns: 0.0 to 1.0
        """
        if not name1 or not name2:
            return 0.0
        
        tokens1 = set(name1.lower().split())
        tokens2 = set(name2.lower().split())
        
        if not tokens1 or not tokens2:
            return 0.0
        
        intersection = tokens1.intersection(tokens2)
        union = tokens1.union(tokens2)
        
        return len(intersection) / len(union) if union else 0.0
    
    def should_merge(self,
                     name1: str, loc1: str,
                     name2: str, loc2: str,
                     threshold: float = 0.8) -> bool:  # Increased threshold to 0.8 for stricter merging
        """
        Decide if two facilities should be merged - STRICTER VERSION
        
        Rules:
        1. Exact normalized name match + same location -> merge
        2. Very high name similarity (0.8+) + same state -> merge
        3. Otherwise -> don't merge
        """
        norm_name1 = self.normalize_name(name1)
        norm_name2 = self.normalize_name(name2)
        
        # Rule 1: Exact name match
        if norm_name1 == norm_name2 and norm_name1:  # Must not be empty
            # Check location overlap
            loc1_lower = loc1.lower() if loc1 else ""
            loc2_lower = loc2.lower() if loc2 else ""
            
            # If either location is empty, require additional check
            if not loc1_lower or not loc2_lower:
                # Only merge if names are longer than 5 chars
                if len(norm_name1) > 5:
                    return True
                return False
            
            # Check if locations overlap (any common tokens)
            loc1_tokens = set(loc1_lower.split())
            loc2_tokens = set(loc2_lower.split())
            
            if loc1_tokens.intersection(loc2_tokens):
                return True
        
        # Rule 2: Very high name similarity + location match
        name_sim = self.calculate_similarity(norm_name1, norm_name2)
        
        if name_sim >= threshold:
            loc1_lower = loc1.lower() if loc1 else ""
            loc2_lower = loc2.lower() if loc2 else ""
            
            if loc1_lower and loc2_lower:
                # Check for state-level match
                states = ['gujarat', 'tamil nadu', 'maharashtra', 'haryana', 'karnataka',
                         'uttar pradesh', 'rajasthan', 'punjab', 'telangana']
                
                for state in states:
                    if state in loc1_lower and state in loc2_lower:
                        return True
        
        return False
    
    def resolve_facilities(self, facilities: List[Dict]) -> List[Dict]:
        """
        Resolve duplicate facilities - IMPROVED VERSION
        Returns: Deduplicated list with merged entries
        """
        if not facilities:
            return []
        
        # Group by division first (only merge within same division)
        division_groups = {}
        for fac in facilities:
            div = fac.get('division', 'Unknown')
            if div not in division_groups:
                division_groups[div] = []
            division_groups[div].append(fac)
        
        # Resolve within each division
        resolved = []
        
        for division, fac_list in division_groups.items():
            merged_indices = set()
            
            for i, fac1 in enumerate(fac_list):
                if i in merged_indices:
                    continue
                
                # Find all facilities that should merge with fac1
                merge_group = [fac1]
                
                for j, fac2 in enumerate(fac_list):
                    if j <= i or j in merged_indices:
                        continue
                    
                    name1 = fac1.get('name', '')
                    name2 = fac2.get('name', '')
                    
                    loc1 = self.normalize_location(fac1.get('city'), fac1.get('state'))
                    loc2 = self.normalize_location(fac2.get('city'), fac2.get('state'))
                    
                    if self.should_merge(name1, loc1, name2, loc2):
                        merge_group.append(fac2)
                        merged_indices.add(j)
                
                # Create merged facility (use best data from group)
                merged_fac = self._merge_facility_group(merge_group)
                resolved.append(merged_fac)
        
        logger.info(f"Resolved {len(facilities)} facilities to {len(resolved)} unique facilities")
        return resolved
    
    def _merge_facility_group(self, facilities: List[Dict]) -> Dict:
        """
        Merge a group of duplicate facilities
        Strategy: Keep most complete data, prefer non-null values
        """
        merged = {}
        
        # Take first facility as base
        merged.update(facilities[0])
        
        # Override with better data from other facilities
        for fac in facilities[1:]:
            for key, value in fac.items():
                # If current value is None/empty and new value exists, use it
                if not merged.get(key) and value:
                    merged[key] = value
                
                # For dates, keep the earliest
                if key in ['event_date', 'announcement_date'] and value:
                    if not merged.get(key) or value < merged[key]:
                        merged[key] = value
                
                # For confidence, keep the highest
                if key == 'confidence' and value:
                    if not merged.get(key) or value > merged[key]:
                        merged[key] = value
        
        # Mark as merged
        merged['was_merged'] = True
        merged['merge_count'] = len(facilities)
        
        return merged
    
    def resolve_duplicate_names(self, name_list: List[str]) -> str:
        """
        Given multiple names, pick the best one
        Prefer: longer names, more specific names
        """
        if not name_list:
            return ""
        
        if len(name_list) == 1:
            return name_list[0]
        
        # Remove duplicates
        unique_names = list(set(name_list))
        
        # Sort by length (longer is usually more specific)
        unique_names.sort(key=len, reverse=True)
        
        return unique_names[0]