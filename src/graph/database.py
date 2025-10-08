"""
Database - SQLite schema and operations
Creates tables: companies, divisions, facilities, events, sources, evidence, jobs
"""

import sqlite3
import logging
from typing import List, Dict, Optional, Tuple
from pathlib import Path

from ..config import DB_PATH

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Database:
    def __init__(self, path: str = DB_PATH):
        self.db_path = path
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()

        # Ensure schema exists (idempotent)
        self._create_tables()
    
    def _create_tables(self):
        """Create minimal DB schema if missing (safe to call repeatedly)."""
        schema = """
        CREATE TABLE IF NOT EXISTS companies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL
        );

        CREATE TABLE IF NOT EXISTS divisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            FOREIGN KEY(company_id) REFERENCES companies(id)
        );

        CREATE TABLE IF NOT EXISTS sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE,
            title TEXT,
            fetched_at TEXT,
            mime_type TEXT,
            publish_date TEXT,
            source_type TEXT
        );

        CREATE TABLE IF NOT EXISTS facilities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            division_id INTEGER,
            name TEXT,
            city TEXT,
            state TEXT,
            country TEXT,
            normalized_name TEXT,
            FOREIGN KEY(division_id) REFERENCES divisions(id)
        );

        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            facility_id INTEGER,
            event_type TEXT,
            event_date TEXT,
            status TEXT,
            expansion_type TEXT,
            confidence REAL,
            FOREIGN KEY(facility_id) REFERENCES facilities(id)
        );

        CREATE TABLE IF NOT EXISTS evidence (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id INTEGER,
            entity_type TEXT,
            entity_id INTEGER,
            text_snippet TEXT,
            char_start INTEGER,
            char_end INTEGER,
            confidence REAL,
            FOREIGN KEY(source_id) REFERENCES sources(id)
        );

        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            facility_id INTEGER,
            title TEXT,
            location TEXT,
            division_id INTEGER,
            is_factory_role INTEGER DEFAULT 0,
            source_id INTEGER,
            posted_date TEXT,
            description TEXT,
            FOREIGN KEY(facility_id) REFERENCES facilities(id),
            FOREIGN KEY(division_id) REFERENCES divisions(id),
            FOREIGN KEY(source_id) REFERENCES sources(id)
        );
        """
        self.conn.executescript(schema)
        self.conn.commit()
        logger.info(f"Database initialized at {self.db_path}")
    
    def get_connection(self) -> sqlite3.Connection:
        """Get database connection"""
        return sqlite3.connect(self.db_path)
    
    def execute_query(self, query: str, params: Tuple = ()) -> List[Tuple]:
        """Execute SELECT query"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(query, params)
        results = cursor.fetchall()
        conn.close()
        return results
    
    def execute_insert(self, query: str, params: Tuple = ()) -> int:
        """Execute INSERT and return last row id"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(query, params)
        last_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return last_id
    
    def execute_many(self, query: str, params_list: List[Tuple]):
        """Execute multiple INSERT/UPDATE statements"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.executemany(query, params_list)
        conn.commit()
        conn.close()
    
    def get_statistics(self) -> Dict:
        """Get database statistics"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        stats = {}
        tables = ['companies', 'divisions', 'facilities', 'events', 'sources', 'evidence', 'jobs']
        
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            stats[f'total_{table}'] = cursor.fetchone()[0]
        
        conn.close()
        return stats
    
    def clear_all_data(self):
        """Clear all data (for testing)"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        tables = ['evidence', 'jobs', 'events', 'facilities', 'divisions', 'companies', 'sources']
        for table in tables:
            cursor.execute(f"DELETE FROM {table}")
        
        conn.commit()
        conn.close()
        logger.info("All data cleared")