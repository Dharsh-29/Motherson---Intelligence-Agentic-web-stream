# ==================================================
# File: src/scrapers/__init__.py
# ==================================================
"""
Web scraping and PDF extraction module
"""

from .base_scraper import BaseScraper
from .pdf_extractor import PDFExtractor

__all__ = ['BaseScraper', 'PDFExtractor']