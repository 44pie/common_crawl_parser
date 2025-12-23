#!/usr/bin/env python3
"""
Checkpoint Manager for Common Crawl Crawler
Auto-saves progress to SQLite, enables resume after crashes
"""

import sqlite3
import json
import time
import threading
from pathlib import Path
from datetime import datetime


class CheckpointManager:
    def __init__(self, db_path, save_interval_domains=1000, save_interval_seconds=60):
        self.db_path = Path(db_path)
        self.save_interval_domains = save_interval_domains
        self.save_interval_seconds = save_interval_seconds
        self.lock = threading.Lock()
        self.last_save_time = time.time()
        self.domains_since_save = 0
        self.conn = None
        self._init_db()
    
    def _init_db(self):
        self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self.conn.execute('PRAGMA journal_mode=WAL')
        self.conn.execute('PRAGMA synchronous=NORMAL')
        
        self.conn.executescript('''
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT
            );
            
            CREATE TABLE IF NOT EXISTS tld_progress (
                tld TEXT PRIMARY KEY,
                urls_scanned INTEGER DEFAULT 0,
                domains_found INTEGER DEFAULT 0,
                completed INTEGER DEFAULT 0,
                last_url TEXT,
                last_timestamp TEXT
            );
            
            CREATE TABLE IF NOT EXISTS domains (
                domain TEXT PRIMARY KEY,
                tld TEXT,
                country TEXT,
                is_ecommerce INTEGER,
                cms TEXT,
                timestamp TEXT,
                language TEXT,
                url_count INTEGER DEFAULT 1
            );
            
            CREATE TABLE IF NOT EXISTS stats (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                total_urls INTEGER DEFAULT 0,
                total_domains INTEGER DEFAULT 0,
                ecommerce_count INTEGER DEFAULT 0,
                skipped INTEGER DEFAULT 0,
                live_checked INTEGER DEFAULT 0,
                live_detected INTEGER DEFAULT 0,
                start_time REAL,
                domains_by_tld TEXT,
                cms_counts TEXT,
                live_platforms TEXT
            );
            
            CREATE INDEX IF NOT EXISTS idx_domains_tld ON domains(tld);
        ''')
        self.conn.commit()
    
    def has_checkpoint(self):
        cursor = self.conn.execute('SELECT COUNT(*) FROM domains')
        count = cursor.fetchone()[0]
        return count > 0
    
    def save_metadata(self, key, value):
        with self.lock:
            self.conn.execute(
                'INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)',
                (key, json.dumps(value))
            )
            self.conn.commit()
    
    def get_metadata(self, key, default=None):
        cursor = self.conn.execute('SELECT value FROM metadata WHERE key = ?', (key,))
        row = cursor.fetchone()
        return json.loads(row[0]) if row else default
    
    def save_tld_progress(self, tld, urls_scanned, domains_found, completed=False, last_url='', last_timestamp=''):
        with self.lock:
            self.conn.execute('''
                INSERT OR REPLACE INTO tld_progress 
                (tld, urls_scanned, domains_found, completed, last_url, last_timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (tld, urls_scanned, domains_found, 1 if completed else 0, last_url, last_timestamp))
            self.conn.commit()
    
    def get_tld_progress(self, tld):
        cursor = self.conn.execute(
            'SELECT urls_scanned, domains_found, completed, last_url, last_timestamp FROM tld_progress WHERE tld = ?',
            (tld,)
        )
        row = cursor.fetchone()
        if row:
            return {
                'urls_scanned': row[0],
                'domains_found': row[1],
                'completed': bool(row[2]),
                'last_url': row[3],
                'last_timestamp': row[4]
            }
        return None
    
    def get_completed_tlds(self):
        cursor = self.conn.execute('SELECT tld FROM tld_progress WHERE completed = 1')
        return {row[0] for row in cursor.fetchall()}
    
    def save_domain(self, domain, tld, country, is_ecommerce, cms, timestamp, language, url_count=1):
        with self.lock:
            try:
                self.conn.execute('''
                    INSERT INTO domains (domain, tld, country, is_ecommerce, cms, timestamp, language, url_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(domain) DO UPDATE SET
                        url_count = url_count + ?,
                        cms = COALESCE(NULLIF(excluded.cms, ''), cms),
                        is_ecommerce = MAX(is_ecommerce, excluded.is_ecommerce)
                ''', (domain, tld, country, 1 if is_ecommerce else 0, cms or '', timestamp, language, url_count, 1))
                self.domains_since_save += 1
            except sqlite3.IntegrityError:
                pass
    
    def domain_exists(self, domain):
        cursor = self.conn.execute('SELECT 1 FROM domains WHERE domain = ?', (domain,))
        return cursor.fetchone() is not None
    
    def get_domains_for_tld(self, tld):
        cursor = self.conn.execute('SELECT domain FROM domains WHERE tld = ?', (tld,))
        return {row[0] for row in cursor.fetchall()}
    
    def get_all_domains(self):
        cursor = self.conn.execute('SELECT domain FROM domains')
        return {row[0] for row in cursor.fetchall()}
    
    def save_stats(self, stats):
        with self.lock:
            self.conn.execute('''
                INSERT OR REPLACE INTO stats 
                (id, total_urls, total_domains, ecommerce_count, skipped, 
                 live_checked, live_detected, start_time, domains_by_tld, cms_counts, live_platforms)
                VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                stats.total_urls,
                stats.total_domains,
                stats.ecommerce_count,
                stats.skipped,
                stats.live_checked,
                stats.live_detected,
                stats.start_time,
                json.dumps(stats.domains_by_tld),
                json.dumps(stats.cms_counts),
                json.dumps(stats.live_platforms)
            ))
            self.conn.commit()
    
    def load_stats(self, stats):
        cursor = self.conn.execute('''
            SELECT total_urls, total_domains, ecommerce_count, skipped,
                   live_checked, live_detected, start_time, domains_by_tld, cms_counts, live_platforms
            FROM stats WHERE id = 1
        ''')
        row = cursor.fetchone()
        if row:
            stats.total_urls = row[0]
            stats.total_domains = row[1]
            stats.ecommerce_count = row[2]
            stats.skipped = row[3]
            stats.live_checked = row[4]
            stats.live_detected = row[5]
            stats.domains_by_tld = json.loads(row[7]) if row[7] else {}
            stats.cms_counts = json.loads(row[8]) if row[8] else {}
            stats.live_platforms = json.loads(row[9]) if row[9] else {}
            return True
        return False
    
    def should_save(self):
        now = time.time()
        return (
            self.domains_since_save >= self.save_interval_domains or
            now - self.last_save_time >= self.save_interval_seconds
        )
    
    def commit(self):
        with self.lock:
            self.conn.commit()
            self.last_save_time = time.time()
            self.domains_since_save = 0
    
    def get_domain_count(self):
        cursor = self.conn.execute('SELECT COUNT(*) FROM domains')
        return cursor.fetchone()[0]
    
    def get_resume_info(self):
        cursor = self.conn.execute('''
            SELECT 
                (SELECT COUNT(*) FROM domains) as domain_count,
                (SELECT COUNT(*) FROM tld_progress WHERE completed = 1) as completed_tlds,
                (SELECT GROUP_CONCAT(tld) FROM tld_progress WHERE completed = 0) as pending_tlds
        ''')
        row = cursor.fetchone()
        return {
            'domain_count': row[0],
            'completed_tlds': row[1],
            'pending_tlds': row[2].split(',') if row[2] else []
        }
    
    def export_to_csv(self, csv_path):
        with open(csv_path, 'w', encoding='utf-8') as f:
            f.write('domain,tld,country,is_ecommerce,cms,timestamp,language\n')
            cursor = self.conn.execute(
                'SELECT domain, tld, country, is_ecommerce, cms, timestamp, language FROM domains ORDER BY tld, domain'
            )
            for row in cursor:
                f.write(f'{row[0]},{row[1]},{row[2]},{bool(row[3])},{row[4]},{row[5]},{row[6]}\n')
    
    def close(self):
        if self.conn:
            self.conn.commit()
            self.conn.close()
            self.conn = None
