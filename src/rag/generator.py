"""
Generator - WITH ALL POSSIBLE GEMINI MODELS
Tries multiple model names until one works
"""

import logging
from typing import List, Dict, Optional
import google.generativeai as genai

from src.config import GEMINI_API_KEY

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Generator:
    def __init__(self, api_key: str = None):
        api_key = api_key or GEMINI_API_KEY
        
        # List of all possible Gemini model names to try
        self.model_names = [
            'gemini-1.5-flash-latest',      # Most common
            'gemini-1.5-flash',              # Alternative
            'gemini-1.5-pro-latest',         # Pro version
            'gemini-1.5-pro',                # Pro alternative
            'gemini-pro',                    # Older stable
            'gemini-1.5-flash-002',          # Versioned
            'gemini-1.5-flash-001',          # Older version
            'models/gemini-1.5-flash',       # With prefix
            'models/gemini-1.5-flash-latest',# With prefix
            'models/gemini-pro'              # With prefix
        ]
        
        self.model = None
        
        if api_key:
            try:
                genai.configure(api_key=api_key)
                
                # Try each model name until one works
                for model_name in self.model_names:
                    try:
                        logger.info(f"🔄 Trying model: {model_name}")
                        test_model = genai.GenerativeModel(model_name)
                        
                        # Test with a simple prompt
                        response = test_model.generate_content("Hello")
                        
                        # If we get here, the model works!
                        self.model = test_model
                        logger.info(f"✅ Successfully initialized Gemini with model: {model_name}")
                        break
                        
                    except Exception as e:
                        logger.warning(f"   ❌ {model_name} failed: {str(e)[:100]}")
                        continue
                
                if not self.model:
                    logger.warning("⚠️ All Gemini models failed. Using fallback mode.")
                    
            except Exception as e:
                logger.warning(f"Gemini initialization failed: {e}. Using fallback mode.")
                self.model = None
        else:
            self.model = None
            logger.warning("No Gemini API key. LLM features disabled")
    
    def generate_answer(
        self,
        query: str,
        graph_results: List[Dict],
        vector_passages: List[Dict] = None
    ) -> str:
        """Generate evidence-backed answer with fallback"""
        if not graph_results:
            return "❌ No evidence found in the database."
        
        # Try LLM if available
        if self.model:
            try:
                context = self._build_context(graph_results, vector_passages)
                prompt = self._build_prompt(query, context, len(graph_results))
                response = self.model.generate_content(prompt)
                return response.text
            except Exception as e:
                logger.error(f"LLM generation error: {e}")
                logger.info("Falling back to simple answer generation")
        
        # Fallback to simple answer
        return self._generate_fallback_answer(graph_results)
    
    def _build_context(self, graph_results: List[Dict], vector_passages: List[Dict] = None) -> str:
        """Build evidence context for LLM"""
        context = "# Evidence from Database:\n\n"
        
        for idx, result in enumerate(graph_results[:20], 1):
            context += f"[{idx}] "
            
            if 'facility' in result or 'name' in result:
                facility_name = result.get('facility') or result.get('name', 'N/A')
                context += f"Facility: {facility_name}\n"
                context += f"  Division: {result.get('division', 'N/A')}\n"
                context += f"  Location: {result.get('city', 'N/A')}, {result.get('state', 'N/A')}\n"
                context += f"  Status: {result.get('status', 'N/A')}\n"
                
                if result.get('expansion_type'):
                    context += f"  Type: {result['expansion_type']}\n"
                
                if result.get('last_event_date') or result.get('event_date'):
                    date = result.get('last_event_date') or result.get('event_date')
                    context += f"  Date: {date}\n"
                
                if result.get('url'):
                    context += f"  Source: {result['url']}\n"
            
            elif 'title' in result:
                context += f"Job: {result.get('title', 'N/A')}\n"
                context += f"  Location: {result.get('location', 'N/A')}\n"
                context += f"  Facility: {result.get('facility', 'N/A')}\n"
                if result.get('url'):
                    context += f"  Source: {result['url']}\n"
            
            context += "\n"
        
        if vector_passages:
            context += "\n# Supporting Document Passages:\n\n"
            for idx, passage in enumerate(vector_passages[:3], 1):
                context += f"[P{idx}] {passage['text'][:300]}...\n"
                context += f"  Source: {passage['metadata'].get('url', 'N/A')}\n\n"
        
        return context
    
    def _build_prompt(self, query: str, context: str, evidence_count: int) -> str:
        """Build prompt with strict guardrails"""
        return f"""You are a precise analyst providing evidence-based answers about Motherson India facilities.

STRICT RULES:
1. Answer ONLY using the evidence provided below
2. Every claim MUST be supported by a citation [1], [2], etc.
3. If evidence is weak, explicitly state this
4. DO NOT invent information
5. Be concise and factual

Evidence Available: {evidence_count} records

{context}

User Query: {query}

Answer with bullet points for facilities/jobs, using citations [1], [2] for each claim:"""
    
    def _generate_fallback_answer(self, graph_results: List[Dict]) -> str:
        """Generate simple answer without LLM"""
        if not graph_results:
            return "No results found in the database."
        
        answer = f"Found {len(graph_results)} results:\n\n"
        
        for idx, result in enumerate(graph_results[:10], 1):
            if 'facility' in result or 'name' in result:
                facility_name = result.get('facility') or result.get('name', 'N/A')
                answer += f"{idx}. {facility_name} "
                answer += f"({result.get('division', 'N/A')}) - "
                answer += f"{result.get('city', 'N/A')}, {result.get('state', 'N/A')}\n"
            elif 'title' in result:
                answer += f"{idx}. {result.get('title', 'N/A')} - "
                answer += f"{result.get('location', 'N/A')}\n"
        
        if len(graph_results) > 10:
            answer += f"\n...and {len(graph_results) - 10} more results"
        
        return answer
    
    def apply_guardrails(self, results: List[Dict], query_type: str) -> Dict:
        """Apply guardrails and quality checks"""
        if not results:
            return {
                'data': [],
                'evidence': [],
                'warning': '⚠️ No evidence found. Results may be incomplete.',
                'confidence': 0.0,
                'answer': None
            }
        
        # Calculate confidence
        confidences = [r.get('confidence', 0.7) for r in results]
        avg_confidence = sum(confidences) / len(confidences) if confidences else 0.7
        
        # Determine warning
        warning = None
        if avg_confidence < 0.5:
            warning = "⚠️ Low confidence results. Evidence may be weak."
        elif avg_confidence < 0.7:
            warning = "⚠️ Moderate confidence. Some results may need verification."
        
        # Filter for hiring query
        if query_type == 'hiring_positions':
            non_factory_keywords = ['software', 'developer', 'programmer', 'data scientist', 'analyst', 'it ', 'digital']
            filtered_results = []
            
            for result in results:
                title = result.get('title', '').lower()
                is_non_factory = any(kw in title for kw in non_factory_keywords)
                
                if not is_non_factory or result.get('is_factory_role'):
                    filtered_results.append(result)
            
            removed_count = len(results) - len(filtered_results)
            if removed_count > 0:
                warning = f"ℹ️ Filtered out {removed_count} non-factory roles"
            
            results = filtered_results
        
        # Build evidence list
        evidence = self._build_evidence_list(results)
        
        return {
            'data': results,
            'evidence': evidence,
            'warning': warning,
            'confidence': avg_confidence,
            'answer': None
        }
    
    def _build_evidence_list(self, results: List[Dict]) -> List[Dict]:
        """Build evidence list with highlighting info"""
        evidence = []
        
        for idx, result in enumerate(results):
            if 'facility' in result or 'name' in result:
                facility_name = result.get('facility') or result.get('name', 'N/A')
                snippet = f"Facility: {facility_name}\n"
                snippet += f"Division: {result.get('division', 'N/A')}\n"
                snippet += f"Location: {result.get('city', 'N/A')}, {result.get('state', 'N/A')}\n"
                snippet += f"Status: {result.get('status', 'N/A')}"
                
                if result.get('expansion_type'):
                    snippet += f"\nType: {result['expansion_type']}"
            
            elif 'title' in result:
                snippet = f"Job Title: {result.get('title', 'N/A')}\n"
                snippet += f"Location: {result.get('location', 'N/A')}\n"
                snippet += f"Facility: {result.get('facility', 'N/A')}"
            
            else:
                snippet = result.get('text_snippet', 'N/A')
            
            evidence.append({
                'id': idx,
                'title': result.get('source_title', f"Evidence {idx + 1}"),
                'text': snippet,
                'url': result.get('url', 'N/A'),
                'date': result.get('publish_date') or result.get('last_event_date') or result.get('event_date') or result.get('posted_date') or 'N/A',
                'source_type': result.get('source_type', 'Document'),
                'char_start': result.get('char_start'),
                'char_end': result.get('char_end'),
                'confidence': result.get('confidence', 0.7)
            })
        
        return evidence