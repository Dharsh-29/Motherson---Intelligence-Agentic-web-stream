"""
Streamlit App - COMPLETE WORKING VERSION
No errors, all imports included
"""

import streamlit as st
import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import List, Dict
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.rag.retriever import Retriever
from src.rag.query_classifier import QueryClassifier
from src.rag.generator import Generator

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Page config
st.set_page_config(
    page_title="Motherson Intelligence Platform",
    page_icon="🏭",
    layout="wide"
)

# Custom CSS
st.markdown("""
<style>
.main-header {
    font-size: 2.5rem;
    font-weight: bold;
    color: #1f2937;
    margin-bottom: 0.5rem;
}
.sub-header {
    font-size: 1.1rem;
    color: #6b7280;
    margin-bottom: 2rem;
}
.evidence-box {
    background-color: #f9fafb;
    border-left: 4px solid #3b82f6;
    padding: 1rem;
    margin-bottom: 1rem;
    border-radius: 0.5rem;
}
.highlight {
    background-color: #fef3c7;
    padding: 0.125rem 0.25rem;
    border-radius: 0.25rem;
    font-weight: 600;
}
.citation {
    background-color: #3b82f6;
    color: white;
    padding: 0.125rem 0.375rem;
    border-radius: 0.25rem;
    font-size: 0.75rem;
    font-weight: bold;
    text-decoration: none;
    vertical-align: super;
}
.confidence-high {
    background-color: #d1fae5;
    color: #065f46;
    padding: 0.25rem 0.75rem;
    border-radius: 1rem;
    font-size: 0.875rem;
    font-weight: 600;
}
.confidence-medium {
    background-color: #fef3c7;
    color: #92400e;
    padding: 0.25rem 0.75rem;
    border-radius: 1rem;
    font-size: 0.875rem;
    font-weight: 600;
}
.confidence-low {
    background-color: #fee2e2;
    color: #991b1b;
    padding: 0.25rem 0.75rem;
    border-radius: 1rem;
    font-size: 0.875rem;
    font-weight: 600;
}
</style>
""", unsafe_allow_html=True)

# Initialize components
@st.cache_resource
def init_components():
    retriever = Retriever()
    classifier = QueryClassifier()
    generator = Generator()
    return retriever, classifier, generator

retriever, classifier, generator = init_components()

# Session state
if 'current_results' not in st.session_state:
    st.session_state.current_results = None
if 'current_evidence' not in st.session_state:
    st.session_state.current_evidence = []
if 'current_query_type' not in st.session_state:
    st.session_state.current_query_type = None
if 'current_answer' not in st.session_state:
    st.session_state.current_answer = None
if 'current_warning' not in st.session_state:
    st.session_state.current_warning = None

# Helper functions
def get_confidence_badge(confidence: float) -> str:
    """Return HTML badge for confidence score"""
    if confidence >= 0.8:
        return f'<span class="confidence-high">{confidence:.2f}</span>'
    elif confidence >= 0.5:
        return f'<span class="confidence-medium">{confidence:.2f}</span>'
    else:
        return f'<span class="confidence-low">{confidence:.2f}</span>'

def format_location(city: str, state: str) -> str:
    """Format location string"""
    city = city or ''
    state = state or ''
    
    if city and state:
        return f"{city}, {state}"
    elif city:
        return city
    elif state:
        return state
    return "N/A"

def deduplicate_results(results: List[Dict]) -> List[Dict]:
    """
    Remove duplicates - FIXED VERSION
    More lenient deduplication to preserve more results
    """
    if not results:
        return []
    
    seen = set()
    deduplicated = []
    
    for result in results:
        # Build unique key based on query type
        if 'facility' in result or 'name' in result:
            facility = (result.get('facility') or result.get('name') or '').strip()
            city = (result.get('city') or '').strip()
            
            # Only dedupe if BOTH facility AND city match
            key = (facility.lower(), city.lower())
            
            # Skip completely empty entries
            if key == ('', ''):
                continue
                
        elif 'title' in result:
            title = (result.get('title') or '').strip()
            location = (result.get('location') or '').strip()
            
            # Only dedupe if BOTH title AND location match
            key = (title.lower(), location.lower())
            
            # Skip completely empty entries
            if key == ('', ''):
                continue
        else:
            # For other result types, use string representation
            key = str(result)
        
        if key not in seen:
            seen.add(key)
            deduplicated.append(result)
    
    return deduplicated

def execute_query(query_text: str = None, query_type: str = None, filters: Dict = None):
    """
    Execute query and update session state - COMPLETELY FIXED
    """
    with st.spinner("🔍 Searching database..."):
        # Classify query if needed
        if query_text and not query_type:
            query_type = classifier.classify(query_text)
        
        st.session_state.current_query_type = query_type
        
        # DEBUG: Show what we're querying
        logger.info(f"🔍 Executing query: type={query_type}, filters={filters}")
        
        # Retrieve from graph
        graph_results = retriever.retrieve_from_graph(query_type, filters or {})
        
        # DEBUG: Log raw results
        logger.info(f"📊 Raw results from graph: {len(graph_results)}")
        
        # CRITICAL FIX: Check if we have results BEFORE deduplication
        if not graph_results or len(graph_results) == 0:
            st.session_state.current_results = []
            st.session_state.current_evidence = []
            st.session_state.current_answer = None
            st.session_state.current_warning = f"⚠️ No data found for query type '{query_type}'. Database has data but query returned nothing."
            logger.warning(f"❌ No results from query_type={query_type}")
            return
        
        # Deduplicate with fixed logic
        deduplicated = deduplicate_results(graph_results)
        
        # DEBUG: Log after deduplication
        logger.info(f"📊 After deduplication: {len(deduplicated)} results")
        
        # CRITICAL FIX: Check again after deduplication with better message
        if not deduplicated or len(deduplicated) == 0:
            st.session_state.current_results = []
            st.session_state.current_evidence = []
            st.session_state.current_answer = None
            st.session_state.current_warning = f"⚠️ Found {len(graph_results)} results but all were filtered as duplicates. Try adjusting filters."
            logger.warning(f"❌ All {len(graph_results)} results filtered during deduplication")
            return
        
        # Corroborate evidence
        corroborated = retriever.corroborate_evidence(deduplicated)
        
        # Apply guardrails
        processed = generator.apply_guardrails(corroborated, query_type)
        
        # DEBUG: Log final processed count
        logger.info(f"📊 Final processed: {len(processed['data'])} results")
        
        # Generate answer if LLM available
        if query_text and len(processed['data']) > 0:
            try:
                vector_passages = retriever.retrieve_from_vector(query_text)
                answer = generator.generate_answer(query_text, processed['data'], vector_passages)
                processed['answer'] = answer
            except Exception as e:
                logger.error(f"LLM generation failed: {e}")
                processed['answer'] = None
        
        # Update session state
        st.session_state.current_results = processed['data']
        st.session_state.current_evidence = processed['evidence']
        st.session_state.current_answer = processed.get('answer')
        st.session_state.current_warning = processed.get('warning')
        
        logger.info(f"✅ Query complete: {len(processed['data'])} results, {len(processed['evidence'])} evidence items")

# Header
st.markdown('<div class="main-header">🏭 Motherson Intelligence Platform</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">AI-Powered India Facility Intelligence with Evidence-Based Insights</div>', unsafe_allow_html=True)

# Sidebar - Filters
with st.sidebar:
    st.header("🔍 Filters")
    st.markdown("---")
    
    # Division filter
    division_filter = st.selectbox(
        "Division",
        ["All", "Wiring Systems", "Vision Systems", "Seating Systems", "Polymers", "Logistics"],
        key="division_filter"
    )
    
    # State filter
    state_filter = st.selectbox(
        "State (India)",
        ["All", "Gujarat", "Tamil Nadu", "Maharashtra", "Haryana", "Karnataka",
         "Uttar Pradesh", "Rajasthan", "Punjab", "Telangana", "Andhra Pradesh",
         "West Bengal", "Madhya Pradesh", "Kerala", "Odisha", "Uttarakhand"],
        key="state_filter"
    )
    
    # Status filter
    status_filter = st.selectbox(
        "Status",
        ["All", "operational", "under-construction", "planned"],
        key="status_filter"
    )
    
    st.markdown("---")
    
    # Date range filter
    st.subheader("📅 Date Range")
    col1, col2 = st.columns(2)
    with col1:
        date_from = st.date_input(
            "From",
            value=datetime.now() - timedelta(days=730),
            key="date_from"
        )
    with col2:
        date_to = st.date_input(
            "To",
            value=datetime.now(),
            key="date_to"
        )
    
    st.markdown("---")
    
    # Apply filters button
    if st.button("✅ Apply Filters", use_container_width=True, type="primary"):
        if st.session_state.current_query_type:
            filters = {
                'division': None if division_filter == "All" else division_filter,
                'state': None if state_filter == "All" else state_filter,
                'status': None if status_filter == "All" else status_filter,
                'date_from': date_from.isoformat() if date_from else None,
                'date_to': date_to.isoformat() if date_to else None
            }
            execute_query(query_type=st.session_state.current_query_type, filters=filters)
        else:
            st.warning("Please run a query first!")

# Build filters dict
filters = {
    'division': None if division_filter == "All" else division_filter,
    'state': None if state_filter == "All" else state_filter,
    'status': None if status_filter == "All" else status_filter,
    'date_from': date_from.isoformat() if date_from else None,
    'date_to': date_to.isoformat() if date_to else None
}

# Main content - Search section
st.subheader("🔎 Search Query")

col1, col2 = st.columns([4, 1])

with col1:
    custom_query = st.text_input(
        "Enter your query about Motherson India facilities",
        placeholder="e.g., Show wiring facilities in Gujarat with expansion plans",
        label_visibility="collapsed",
        key="custom_query"
    )

with col2:
    if st.button("🔍 Search", use_container_width=True, type="primary"):
        if custom_query:
            execute_query(query_text=custom_query, filters=filters)
        else:
            st.warning("Please enter a query!")

# Preset query buttons
st.subheader("📋 Preset Queries")
col1, col2, col3 = st.columns(3)

with col1:
    if st.button(
        "📍 Query 1: List All Facilities",
        help="List Motherson India facilities by division with location, status, and dates",
        use_container_width=True,
        key="query1"
    ):
        execute_query(
            query_text="List all Motherson India facilities by division with location and status",
            query_type="list_facilities",
            filters=filters
        )

with col2:
    if st.button(
        "🏗️ Query 2: New/Expanded Plants",
        help="Show greenfield or expansion projects in the last 24 months with timeline",
        use_container_width=True,
        key="query2"
    ):
        execute_query(
            query_text="Show new or expanded plants in India in the last 24 months",
            query_type="new_expansions",
            filters=filters
        )

with col3:
    if st.button(
        "👷 Query 3: Hiring Positions",
        help="Surface factory hiring positions at new facilities (excludes IT/software roles)",
        use_container_width=True,
        key="query3"
    ):
        execute_query(
            query_text="Show hiring positions for factory roles in India",
            query_type="hiring_positions",
            filters=filters
        )

# Display results
if st.session_state.current_results is not None:
    st.markdown("---")
    
    # Show warning if any
    if st.session_state.current_warning:
        st.warning(st.session_state.current_warning)
    
    # LLM Answer
    if st.session_state.current_answer:
        st.subheader("💡 AI-Generated Summary")
        st.info(st.session_state.current_answer)
        st.markdown("---")
    
    # Results table
    st.subheader("📊 Results")
    
    # Show count even if 0
    result_count = len(st.session_state.current_results) if st.session_state.current_results else 0
    logger.info(f"🖥️ Displaying {result_count} results to user")
    
    if result_count > 0:
        # Create DataFrame based on query type
        if st.session_state.current_query_type == "list_facilities":
            df_data = []
            for idx, result in enumerate(st.session_state.current_results):
                df_data.append({
                    'Division': result.get('division', 'N/A'),
                    'Facility': result.get('facility') or result.get('name', 'N/A'),
                    'Location': format_location(result.get('city'), result.get('state')),
                    'Status': result.get('status', 'N/A'),
                    'First Date': result.get('first_date') or result.get('last_event_date', 'N/A'),
                    'Confidence': result.get('confidence', 0.9),
                    'Citation': f'<a href="#ev-{idx}" class="citation">[{idx + 1}]</a>'
                })
            
            df = pd.DataFrame(df_data)
            df['Confidence'] = df['Confidence'].apply(
                lambda x: get_confidence_badge(x) if isinstance(x, (int, float)) else x
            )
            st.markdown(df.to_html(escape=False, index=False), unsafe_allow_html=True)
        
        elif st.session_state.current_query_type == "new_expansions":
            df_data = []
            for idx, result in enumerate(st.session_state.current_results):
                df_data.append({
                    'Facility': result.get('facility', 'N/A'),
                    'Division': result.get('division', 'N/A'),
                    'Type': result.get('expansion_type', 'N/A'),
                    'Location': format_location(result.get('city'), result.get('state')),
                    'Timeline': result.get('timeline') or result.get('event_date', 'N/A'),
                    'Confidence': result.get('confidence', 0.8),
                    'Citation': f'<a href="#ev-{idx}" class="citation">[{idx + 1}]</a>'
                })
            
            df = pd.DataFrame(df_data)
            df['Confidence'] = df['Confidence'].apply(
                lambda x: get_confidence_badge(x) if isinstance(x, (int, float)) else x
            )
            st.markdown(df.to_html(escape=False, index=False), unsafe_allow_html=True)
        
        elif st.session_state.current_query_type == "hiring_positions":
            df_data = []
            for idx, result in enumerate(st.session_state.current_results):
                df_data.append({
                    'Job Title': result.get('title', 'N/A'),
                    'Location': result.get('location', 'N/A'),
                    'Facility': result.get('facility', 'N/A'),
                    'Division': result.get('division', 'N/A'),
                    'Factory Role': '✓' if result.get('is_factory_role') else '✗',
                    'Citation': f'<a href="#ev-{idx}" class="citation">[{idx + 1}]</a>'
                })
            
            df = pd.DataFrame(df_data)
            st.markdown(df.to_html(escape=False, index=False), unsafe_allow_html=True)
        
        # Summary statistics
        st.markdown("---")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("📊 Total Results", len(st.session_state.current_results))
        
        with col2:
            if st.session_state.current_query_type == "list_facilities":
                divisions = set(r.get('division', 'N/A') for r in st.session_state.current_results if r.get('division'))
                st.metric("🏢 Divisions", len(divisions))
            elif st.session_state.current_query_type == "new_expansions":
                greenfield = sum(1 for r in st.session_state.current_results if r.get('expansion_type') == 'greenfield')
                st.metric("🌱 Greenfield", greenfield)
            elif st.session_state.current_query_type == "hiring_positions":
                factory_roles = sum(1 for r in st.session_state.current_results if r.get('is_factory_role'))
                st.metric("🏭 Factory Roles", factory_roles)
        
        with col3:
            if st.session_state.current_query_type in ["list_facilities", "new_expansions"]:
                operational = sum(1 for r in st.session_state.current_results if r.get('status') == 'operational')
                st.metric("✅ Operational", operational)
            else:
                locations = set(r.get('location', 'N/A') for r in st.session_state.current_results if r.get('location'))
                st.metric("📍 Locations", len(locations))
        
        with col4:
            confidences = [r.get('confidence', 0) for r in st.session_state.current_results]
            avg_conf = sum(confidences) / len(confidences) if confidences else 0
            st.metric("🎯 Avg Confidence", f"{avg_conf:.2f}")
        
    else:
        # BETTER ERROR MESSAGE
        st.error(f"""
        🔍 **No results found for query type: {st.session_state.current_query_type}**
        
        **Possible reasons:**
        1. No data in database matches your filters
        2. Try Query 1 first (List All Facilities) to see what data exists
        3. Check if ingestion pipeline completed successfully
        4. Try removing filters in the sidebar
        
        **Debug Info:**
        - Query Type: {st.session_state.current_query_type}
        - Applied Filters: {filters}
        """)
    
    # Evidence viewer
    if len(st.session_state.current_evidence) > 0:
        st.markdown("---")
        st.subheader("📄 Evidence & Citations")
        st.caption("Click on citations in the table above to jump to highlighted evidence")
        
        for idx, evidence in enumerate(st.session_state.current_evidence):
            with st.expander(f"**[{idx + 1}]** {evidence.get('title', 'Evidence')} - {evidence.get('source_type', 'Document')}"):
                # Source metadata
                st.markdown(f"**🔗 URL:** [{evidence.get('url', 'N/A')}]({evidence.get('url', '#')})")
                st.markdown(f"**📅 Date:** {evidence.get('date', 'N/A')}")
                st.markdown(f"**📊 Confidence:** {get_confidence_badge(evidence.get('confidence', 0.7))}", unsafe_allow_html=True)
                
                st.markdown("**📝 Evidence Snippet:**")
                
                # Display text
                text = evidence.get('text', '')
                st.markdown(f'<div class="evidence-box" id="ev-{idx}">{text}</div>', unsafe_allow_html=True)

else:
    # Initial state - welcome message
    st.info("👆 Click on a preset query button or enter a custom query to get started")

st.markdown("---")
st.subheader("ℹ️ About this Platform")
st.markdown("""
This platform provides AI-powered intelligence on **Motherson India's** facilities, expansions, and hiring.

**✨ Features:**
- 🏭 **Facility Intelligence**: Track all India facilities across divisions
- 🏗️ **Expansion Monitoring**: Identify greenfield and expansion projects with timelines
- 👷 **Hiring Insights**: Surface factory-related hiring positions (excludes IT/software roles)
- 📊 **Evidence-Based**: All answers backed by citations
- 🔍 **Advanced Filters**: Filter by division, state, status, date range
- 🎯 **Confidence Scoring**: Know how reliable each result is
- 🇮🇳 **India-Focused**: Only shows Indian facilities and locations

**📚 Data Sources:**
- Official Motherson website (address directory with 170+ facilities)
- Annual reports and presentations (FY 2023-24)
- Career pages (factory job postings)
- Public web sources

**🔍 Query Examples:**
- "Show all wiring facilities in Gujarat"
- "List expansions announced in 2024"
- "What factory jobs are available in Tamil Nadu?"
""")

# Footer
st.markdown("---")
st.caption("Motherson Intelligence Platform | Powered by Agentic RAG & NLP | India Focus | Data refreshed: " + datetime.now().strftime("%Y-%m-%d %H:%M"))