"""
Enhanced Pipeline with Address Directory and Fixed Selenium
"""
import argparse
import logging
import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from src.graph.database import Database
from src.config import DB_PATH, DEFAULT_URLS, CACHE_DIR, PDF_DIR, CHROMA_COLLECTION, FACTORY_CAREER_URL

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import components
from src.scrapers.base_scraper import BaseScraper
from src.scrapers.pdf_extractor import PDFExtractor
from src.scrapers.address_scraper import AddressScraper  # ✅ NEW
from src.scrapers.selenium_scraper import SeleniumScraper  # ✅ FIXED
from src.ner.entity_extractor import EntityExtractor
from src.graph.graph_builder import GraphBuilder
from src.rag.retriever import Retriever


class EnhancedPipeline:
    """Production pipeline with all fixes"""
    
    def __init__(self):
        self.db = Database(DB_PATH)
        self.scraper = BaseScraper()
        self.pdf_extractor = PDFExtractor()
        self.address_scraper = AddressScraper()  # ✅ NEW
        self.selenium_scraper = SeleniumScraper()  # ✅ FIXED
        self.entity_extractor = EntityExtractor()
        self.graph_builder = GraphBuilder(self.db)
        self.retriever = Retriever()

    def step1_scraping(self, urls: list = None, use_cache: bool = True) -> list:
        """Step 1: Scrape all sources"""
        logger.info("=" * 70)
        logger.info("STEP 1: SCRAPING (ENHANCED)")
        logger.info("=" * 70)

        scraped_data = []

        if not use_cache and os.path.exists(CACHE_DIR):
            shutil.rmtree(CACHE_DIR)
            os.makedirs(CACHE_DIR, exist_ok=True)
            logger.info("Cache cleared.")

        # ✅ STEP 1A: Scrape Address Directory (CRITICAL for Query 1)
        logger.info("🏢 Scraping Address Directory (19 pages)...")
        try:
            address_facilities = self.address_scraper.scrape_all(pages=19, use_cache=use_cache)
            
            if address_facilities:
                # Convert to standard format
                address_doc = {
                    'url': 'https://www.motherson.com/contact/address-directory',
                    'title': 'Motherson Address Directory',
                    'text': json.dumps(address_facilities, indent=2),  # Store as JSON text
                    'fetched_at': datetime.now().isoformat(),
                    'mime': 'application/json',
                    'publish_dt': None,
                    'structured_facilities': address_facilities  # ✅ Pass structured data
                }
                scraped_data.append(address_doc)
                logger.info(f"✅ Address directory: {len(address_facilities)} facilities")
            else:
                logger.warning("⚠️ No facilities from address directory")
        except Exception as e:
            logger.error(f"❌ Address directory scraping failed: {e}")

        # ✅ STEP 1B: Scrape Career Page (CRITICAL for Query 3)
        logger.info("👔 Scraping Factory Jobs...")
        try:
            jobs = self.selenium_scraper.scrape_jobs(url=FACTORY_CAREER_URL)
            
            if jobs:
                # Convert to standard format
                jobs_text = "\n\n".join([f"{j['title']} - {j['location']}" for j in jobs])
                jobs_doc = {
                    'url': FACTORY_CAREER_URL,
                    'title': 'Motherson Factory Jobs',
                    'text': jobs_text,
                    'fetched_at': datetime.now().isoformat(),
                    'mime': 'text/html',
                    'publish_dt': None,
                    'structured_jobs': jobs  # ✅ Pass structured data
                }
                scraped_data.append(jobs_doc)
                logger.info(f"✅ Career page: {len(jobs)} jobs")
            else:
                logger.warning("⚠️ No jobs from career page")
        except Exception as e:
            logger.error(f"❌ Career page scraping failed: {e}")

        # ✅ STEP 1C: Scrape PDFs (CRITICAL for Query 2)
        pdf_files = list(PDF_DIR.glob("*.pdf"))
        if pdf_files:
            logger.info(f"📄 Found {len(pdf_files)} PDF files")
            for pdf_file in pdf_files:
                logger.info(f"Extracting: {pdf_file.name}")
                pdf_data = self.pdf_extractor.extract_from_file(str(pdf_file))
                if pdf_data:
                    scraped_data.append(pdf_data)
        else:
            logger.warning("⚠️ No PDFs found in data/pdfs/")

        # ✅ STEP 1D: Scrape Other URLs (optional)
        other_urls = [
            "https://www.motherson.com/about-us",
            "https://www.motherson.com/company/business-divisions",
        ]
        
        for url in other_urls:
            try:
                data = self.scraper.scrape_url(url, use_cache=use_cache)
                if data:
                    scraped_data.append(data)
            except Exception as e:
                logger.error(f"Failed {url}: {e}")

        # Save cache
        cache_file = CACHE_DIR / "scraped_data.json"
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(scraped_data, f, indent=2, default=str)

        logger.info(f"✅ Total documents scraped: {len(scraped_data)}")
        return scraped_data

    def step2_extraction(self, scraped_data: list) -> list:
        """Step 2: Extract entities"""
        logger.info("=" * 70)
        logger.info("STEP 2: ENTITY EXTRACTION")
        logger.info("=" * 70)

        extracted_data = []

        for idx, doc in enumerate(scraped_data, 1):
            logger.info(f"Processing document {idx}/{len(scraped_data)}: {doc.get('title', 'Untitled')}")

            text = doc.get('text', '')
            if not text or len(text) < 50:
                logger.warning("  ⚠️ Document too short, skipping")
                continue

            try:
                entities = self.entity_extractor.extract_entities(text)

                extracted_item = {
                    'source_data': {
                        'url': doc.get('url', 'unknown'),
                        'title': doc.get('title', 'Untitled'),
                        'fetched_at': doc.get('fetched_at'),
                        'mime': doc.get('mime', 'text/html'),
                        'publish_dt': doc.get('publish_dt'),
                        'source_type': 'pdf' if doc.get('mime') == 'application/pdf' else 'web'
                    },
                    'entities': entities
                }

                # ✅ Pass structured data
                if 'structured_facilities' in doc:
                    extracted_item['source_data']['structured_facilities'] = doc['structured_facilities']
                    logger.info(f"  ✅ Found {len(doc['structured_facilities'])} structured facilities")

                if 'structured_jobs' in doc:
                    extracted_item['source_data']['structured_jobs'] = doc['structured_jobs']
                    logger.info(f"  ✅ Found {len(doc['structured_jobs'])} structured jobs")

                extracted_data.append(extracted_item)

                total_entities = sum(len(v) for v in entities.values())
                logger.info(f"  ✅ Extracted {total_entities} entities")

            except Exception as e:
                logger.error(f"  ✗ Error extracting from {doc.get('url')}: {e}")
                import traceback
                traceback.print_exc()

        cache_file = CACHE_DIR / "extracted_data.json"
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(extracted_data, f, indent=2, default=str)

        logger.info(f"✅ Extracted entities from {len(extracted_data)} documents")
        return extracted_data

    def step3_graph_building(self, extracted_data: list):
        """Step 3: Build knowledge graph"""
        logger.info("=" * 70)
        logger.info("STEP 3: GRAPH BUILDING")
        logger.info("=" * 70)

        try:
            self.graph_builder.build_graph(extracted_data)

            # ✅ Insert jobs
            logger.info("Extracting job postings...")
            jobs_data = []

            for item in extracted_data:
                source_data = item.get('source_data', {})
                
                # Get source_id
                source_id = None
                try:
                    result = self.graph_builder.db.execute_query(
                        "SELECT id FROM sources WHERE url = ?",
                        (source_data.get('url', 'unknown'),)
                    )
                    if result:
                        source_id = result[0][0]
                except:
                    pass

                # Use structured jobs if available
                if 'structured_jobs' in source_data:
                    for job in source_data['structured_jobs']:
                        jobs_data.append({
                            'title': job.get('title', ''),
                            'location': job.get('location', 'India'),
                            'division': None,
                            'is_factory_role': True,  # Already filtered
                            'source_id': source_id,
                            'posted_date': None,
                            'description': None
                        })

            if jobs_data:
                self.graph_builder.insert_jobs(jobs_data)
                logger.info(f"✅ Inserted {len(jobs_data)} job postings")

            logger.info("✅ Graph building complete!")

        except Exception as e:
            logger.error(f"✗ Graph building failed: {e}")
            import traceback
            traceback.print_exc()
            raise

    def step4_vector_indexing(self, scraped_data: list):
        """Step 4: Build vector index"""
        logger.info("=" * 70)
        logger.info("STEP 4: VECTOR INDEXING")
        logger.info("=" * 70)

        try:
            self.retriever.index_documents(scraped_data)
            count = self.retriever.collection.count()
            logger.info(f"✅ Indexed {count} document chunks")
        except Exception as e:
            logger.error(f"✗ Vector indexing failed: {e}")
            import traceback
            traceback.print_exc()
            raise

    def run_full_pipeline(self, urls: list = None, use_cache: bool = True):
        """Run complete pipeline"""
        start_time = datetime.now()

        logger.info("=" * 70)
        logger.info("🚀 MOTHERSON INTELLIGENCE PIPELINE (PRODUCTION)")
        logger.info("=" * 70)
        logger.info(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        try:
            scraped_data = self.step1_scraping(urls, use_cache)
            if not scraped_data:
                logger.error("❌ No data scraped. Exiting.")
                return False

            print()
            extracted_data = self.step2_extraction(scraped_data)
            if not extracted_data:
                logger.error("❌ No entities extracted. Exiting.")
                return False

            print()
            self.step3_graph_building(extracted_data)

            print()
            self.step4_vector_indexing(scraped_data)

            elapsed = (datetime.now() - start_time).total_seconds()

            logger.info("")
            logger.info("=" * 70)
            logger.info("✅ PIPELINE COMPLETE!")
            logger.info("=" * 70)
            logger.info(f"Total time: {elapsed:.2f} seconds ({elapsed/60:.1f} minutes)")
            logger.info("")
            logger.info("Next steps:")
            logger.info("  1. Run: streamlit run src/ui/app.py")
            logger.info("  2. Open browser at: http://localhost:8501")
            logger.info("  3. Try the 3 preset queries!")
            logger.info("")

            return True

        except Exception as e:
            logger.error(f"❌ Pipeline failed: {e}")
            import traceback
            traceback.print_exc()
            return False

    def show_statistics(self):
        """Show database statistics"""
        logger.info("=" * 70)
        logger.info("DATABASE STATISTICS")
        logger.info("=" * 70)

        stats = self.db.get_statistics()

        print("\n📊 Graph Database:")
        for key, value in stats.items():
            print(f"  {key}: {value}")

        print("\n🔍 Vector Store:")
        count = self.retriever.collection.count()
        print(f"  Document chunks: {count}")

        print("\n💾 Cache:")
        scraped_file = CACHE_DIR / "scraped_data.json"
        extracted_file = CACHE_DIR / "extracted_data.json"

        if scraped_file.exists():
            with open(scraped_file, 'r') as f:
                scraped_count = len(json.load(f))
            print(f"  Scraped documents: {scraped_count}")

        if extracted_file.exists():
            with open(extracted_file, 'r') as f:
                extracted_count = len(json.load(f))
            print(f"  Extracted documents: {extracted_count}")

        print("")


def main():
    parser = argparse.ArgumentParser(description="Motherson Intelligence Pipeline (Production)")
    parser.add_argument('--ingest', action='store_true', help='Run full ingestion pipeline')
    parser.add_argument('--urls', nargs='+', help='URLs to scrape (optional)')
    parser.add_argument('--no-cache', action='store_true', help='Disable cache, force fresh scraping')
    parser.add_argument('--stats', action='store_true', help='Show database statistics')

    args = parser.parse_args()

    pipeline = EnhancedPipeline()

    if args.stats:
        pipeline.show_statistics()
    elif args.ingest:
        use_cache = not args.no_cache
        success = pipeline.run_full_pipeline(urls=args.urls, use_cache=use_cache)

        if success:
            print("\n" + "=" * 70)
            print("🎉 SUCCESS! Ready to use the platform.")
            print("=" * 70)
        else:
            print("\n" + "=" * 70)
            print("❌ FAILED! Check logs above for errors.")
            print("=" * 70)
    else:
        parser.print_help()
        print("\n" + "=" * 70)
        print("Quick Start:")
        print("  python run.py --ingest --no-cache  # Run full pipeline with fresh data")
        print("  python run.py --stats  # Show statistics")
        print("  streamlit run src/ui/app.py  # Start web app")
        print("=" * 70)


if __name__ == "__main__":
    try:
        from src.config import BASE_DIR
        if os.getcwd() != str(BASE_DIR):
            os.chdir(BASE_DIR)
    except Exception:
        pass

    main()