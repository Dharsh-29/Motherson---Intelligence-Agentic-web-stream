"""
Query Classifier - Classifies user queries into 3 core tasks
"""

import logging
from typing import Dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class QueryClassifier:
    def __init__(self):
        # Keywords for each query type
        self.query_keywords = {
            'list_facilities': [
                'list', 'show', 'display', 'all facilities', 'facilities by division',
                'what facilities', 'which facilities', 'facilities in', 'plants in'
            ],
            'new_expansions': [
                'new', 'expansion', 'greenfield', 'brownfield', 'expanded',
                'recent', 'latest', 'upcoming', 'announced', 'phase',
                'new plant', 'new facility', 'expansion plan', 'future plant'
            ],
            'hiring_positions': [
                'hiring', 'jobs', 'positions', 'careers', 'recruitment',
                'vacancies', 'openings', 'job postings', 'employment',
                'looking for', 'hiring for', 'factory roles'
            ]
        }
    
    def classify(self, query: str) -> str:
        """
        Classify query into one of 3 types
        Returns: 'list_facilities', 'new_expansions', or 'hiring_positions'
        """
        if not query:
            return 'list_facilities'
        
        query_lower = query.lower()
        
        # Calculate scores for each query type
        scores = {}
        
        for query_type, keywords in self.query_keywords.items():
            score = 0
            for keyword in keywords:
                if keyword in query_lower:
                    # Longer keyword matches get higher scores
                    score += len(keyword.split())
            scores[query_type] = score
        
        # Return type with highest score
        if max(scores.values()) == 0:
            # No clear match, default to list_facilities
            logger.info(f"No clear classification for '{query}', defaulting to list_facilities")
            return 'list_facilities'
        
        classified_type = max(scores, key=scores.get)
        logger.info(f"Classified query '{query}' as: {classified_type}")
        
        return classified_type
    
    def get_query_description(self, query_type: str) -> str:
        """Get human-readable description of query type"""
        descriptions = {
            'list_facilities': 'List all Motherson facilities by division',
            'new_expansions': 'Show new/expanded plants in the last 24 months',
            'hiring_positions': 'Surface hiring positions for factory roles'
        }
        return descriptions.get(query_type, 'Unknown query type')