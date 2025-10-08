# ==================================================
# File: src/rag/retriever.py
# ==================================================

import logging
from typing import List, Dict, Optional
import chromadb
from chromadb.config import Settings
from pathlib import Path
from datetime import datetime, timedelta

from ..config import VECTOR_STORE_DIR, CHROMA_COLLECTION, CHUNK_SIZE, CHUNK_OVERLAP
from src.graph.database import Database
from src.graph.graph_builder import GraphBuilder

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Retriever:
    def __init__(self):
        # Graph setup
        db = Database()
        self.graph_builder = GraphBuilder(db)

        # ChromaDB setup
        self.chroma_client = chromadb.PersistentClient(
            path=str(VECTOR_STORE_DIR),
            settings=Settings(anonymized_telemetry=False)
        )

        try:
            self.collection = self.chroma_client.get_collection(CHROMA_COLLECTION)
            logger.info(f"Loaded existing collection: {CHROMA_COLLECTION}")
        except:
            self.collection = self.chroma_client.create_collection(
                name=CHROMA_COLLECTION,
                metadata={"description": "Motherson documents"}
            )
            logger.info(f"Created new collection: {CHROMA_COLLECTION}")

    def _clean_metadata(self, metadata: Dict) -> Dict:
        cleaned = {}
        for key, value in metadata.items():
            if value is None:
                cleaned[key] = ''
            elif isinstance(value, (str, int, float, bool)):
                cleaned[key] = value
            else:
                cleaned[key] = str(value)
        return cleaned

    def index_documents(self, documents: List[Dict]):
        try:
            self.chroma_client.delete_collection(CHROMA_COLLECTION)
            self.collection = self.chroma_client.create_collection(CHROMA_COLLECTION)
            logger.info("Cleared existing collection")
        except Exception as e:
            logger.warning(f"Could not clear collection: {e}")

        doc_chunks = []
        metadatas = []
        ids = []

        for idx, doc in enumerate(documents):
            text = doc.get('text', '')
            if not text or len(text) < 100:
                continue

            chunks = self._chunk_text(text, CHUNK_SIZE, CHUNK_OVERLAP)

            for chunk_idx, chunk in enumerate(chunks):
                doc_chunks.append(chunk)
                metadata = {
                    'url': doc.get('url', 'unknown'),
                    'title': doc.get('title', 'Untitled'),
                    'publish_date': doc.get('publish_dt'),
                    'chunk_idx': chunk_idx,
                    'source_type': doc.get('mime', 'text/html')
                }
                metadatas.append(self._clean_metadata(metadata))
                ids.append(f"doc_{idx}_chunk_{chunk_idx}")

        if doc_chunks:
            try:
                self.collection.add(
                    documents=doc_chunks,
                    metadatas=metadatas,
                    ids=ids
                )
                logger.info(f"✅ Indexed {len(doc_chunks)} document chunks")
            except Exception as e:
                logger.error(f"Error adding to ChromaDB: {e}")
                raise
        else:
            logger.warning("No chunks to index")

    def _chunk_text(self, text: str, chunk_size: int, overlap: int) -> List[str]:
        chunks = []
        start = 0

        while start < len(text):
            end = start + chunk_size
            chunk = text[start:end]

            if end < len(text):
                last_period = chunk.rfind('.')
                last_newline = chunk.rfind('\n')
                break_point = max(last_period, last_newline)

                if break_point > chunk_size // 2:
                    chunk = chunk[:break_point + 1]
                    end = start + break_point + 1

            chunks.append(chunk.strip())
            start = end - overlap

        return [c for c in chunks if len(c) > 50]

    def retrieve_from_graph(self, query_type: str, filters: Dict) -> List[Dict]:
        logger.info(f"Retrieving from graph: {query_type}")
        results = []

        if query_type == 'list_facilities':
            results = self.graph_builder.query_facilities(
                division=filters.get('division'),
                state=filters.get('state'),
                status=filters.get('status')
            )

        elif query_type == 'new_expansions':
            date_from = filters.get('date_from')
            date_to = filters.get('date_to')

            if date_from and date_to:
                results = self.graph_builder.query_expansions(
                    date_from=date_from,
                    date_to=date_to
                )
            else:
                # FIXED: Removed months_back param to match graph_builder signature
                date_to = datetime.now().date().isoformat()
                date_from = (datetime.now() - timedelta(days=730)).date().isoformat()
                results = self.graph_builder.query_expansions(
                    date_from=date_from,
                    date_to=date_to
                )

        elif query_type == 'hiring_positions':
            results = self.graph_builder.query_jobs(factory_only=True)

        logger.info(f"Retrieved {len(results)} results from graph")
        return results

    def retrieve_from_vector(self, query: str, n_results: int = 5) -> List[Dict]:
        try:
            if self.collection.count() == 0:
                logger.warning("Vector store is empty")
                return []

            results = self.collection.query(
                query_texts=[query],
                n_results=min(n_results, self.collection.count())
            )

            passages = []
            if results['documents'] and results['documents'][0]:
                for doc, metadata, distance in zip(
                    results['documents'][0],
                    results['metadatas'][0],
                    results['distances'][0]
                ):
                    passages.append({
                        'text': doc,
                        'metadata': metadata,
                        'relevance': 1 - distance
                    })

            logger.info(f"Retrieved {len(passages)} passages from vector store")
            return passages

        except Exception as e:
            logger.error(f"Vector retrieval error: {e}")
            return []

    def corroborate_evidence(self, results: List[Dict]) -> List[Dict]:
        if not results:
            return results

        if results and 'title' in results[0]:
            return results  # Job postings don’t need corroboration

        facility_groups = {}
        for result in results:
            facility_name = (result.get('facility') or '').lower()
            if not facility_name:
                continue
            facility_groups.setdefault(facility_name, []).append(result)

        corroborated_results = []
        for facility_name, group in facility_groups.items():
            unique_sources = set(r.get('url', '') for r in group if r.get('url'))
            num_sources = len(unique_sources)

            for result in group:
                if num_sources >= 2:
                    result['confidence'] = min(0.95, result.get('confidence', 0.5) + 0.2)
                    result['corroboration'] = f"Confirmed by {num_sources} sources"
                else:
                    result['corroboration'] = "Single source"
                corroborated_results.append(result)

        return corroborated_results
