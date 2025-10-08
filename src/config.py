"""Configuration settings - FIXED VERSION"""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Base paths
BASE_DIR = Path(__file__).parent.parent.resolve()
DATA_DIR = BASE_DIR / "data"
CACHE_DIR = DATA_DIR / "cache"
VECTOR_STORE_DIR = DATA_DIR / "vector_store"
PDF_DIR = DATA_DIR / "pdfs"

# Ensure directories exist
for d in [DATA_DIR, CACHE_DIR, VECTOR_STORE_DIR, PDF_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# Database
DB_PATH = str(BASE_DIR / "motherson_graph.db")

# API Keys
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

# Scraping settings
SCRAPER_RATE_LIMIT = float(os.getenv("SCRAPER_RATE_LIMIT", 2.0))
SCRAPER_TIMEOUT = int(os.getenv("SCRAPER_TIMEOUT", 30))
SCRAPER_MAX_RETRIES = int(os.getenv("SCRAPER_MAX_RETRIES", 3))

# ✅ ADDED: Factory career URL (CRITICAL FIX)
FACTORY_CAREER_URL = "https://careers.motherson.com/en/jobs?area=EnvironmentHealthandSafety&area=LogisticsShippingScheduling&area=ManufacturingOperations&area=Purchasing&area=Quality"

# Default URLs - UPDATED with address directory
DEFAULT_URLS = [
    "https://motherson.com",
    "https://www.motherson.com/about-us",
    "https://www.motherson.com/our-businesses",
    "https://www.motherson.com/company/business-divisions",
    # ✅ Address directory pages (19 pages - CRITICAL for Query 1)
    *[f"https://www.motherson.com/contact/address-directory?country=India&page={i}" for i in range(1, 20)],
    # ✅ Pre-filtered career URL (CRITICAL for Query 3)
    FACTORY_CAREER_URL,
]

# NER / Vector settings
SPACY_MODEL = os.getenv("SPACY_MODEL", "en_core_web_sm")
CHROMA_COLLECTION = os.getenv("CHROMA_COLLECTION", "motherson_docs")
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 500))
CHUNK_OVERLAP = int(os.getenv("CHUNK_OVERLAP", 50))

# Query defaults
EXPANSION_MONTHS_DEFAULT = int(os.getenv("EXPANSION_MONTHS_DEFAULT", 24))