#!/usr/bin/env python3
"""
Multi-Source Domain Scraper v3.0 - MULTITHREADED
Fast parallel collection from: Reviews.io (US+UK), TrustedSite, Feedaty
"""

import requests
import re
import sys
import time
import argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

class DomainCollector:
    def __init__(self, output_dir='domains_collected', threads=20):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.threads = threads
        self.lock = Lock()
        self.domains = set()
        self.stats = {'pages': 0, 'errors': 0}
        
    def log(self, msg):
        print(f"[{time.strftime('%H:%M:%S')}] {msg}")
    
    def save(self, domains, source):
        path = self.output_dir / f"{source}.txt"
        with open(path, 'w') as f:
            for d in sorted(domains):
                f.write(d + '\n')
        self.log(f"Saved {len(domains):,} to {path}")
        
    def fetch(self, url, timeout=30):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=timeout)
            return resp.text
        except:
            with self.lock:
                self.stats['errors'] += 1
            return ""

    def _fetch_sitemap(self, url):
        """Fetch sitemap and extract domains"""
        content = self.fetch(url)
        found = set(re.findall(r'/company-reviews/store/([a-zA-Z0-9][a-zA-Z0-9\-\.]+\.[a-z]{2,})', content))
        with self.lock:
            self.domains.update(found)
            self.stats['pages'] += 1
        return len(found)

    def scrape_reviews_io(self):
        """Reviews.io - US + UK parallel"""
        self.log("=" * 60)
        self.log(f"REVIEWS.IO (US + UK) - {self.threads} threads")
        self.log("=" * 60)
        self.domains = set()
        self.stats = {'pages': 0, 'errors': 0}
        
        all_sitemaps = []
        
        # Get US sitemaps
        us_content = self.fetch("https://www.reviews.io/sitemap.xml")
        us_sitemaps = re.findall(r'<loc>([^<]+\.xml)</loc>', us_content)
        all_sitemaps.extend(us_sitemaps)
        self.log(f"US: {len(us_sitemaps)} sitemaps")
        
        # UK is direct - add as single "sitemap"
        all_sitemaps.append("https://www.reviews.co.uk/sitemap.xml")
        self.log(f"UK: 1 sitemap")
        
        # Parallel fetch
        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            futures = {executor.submit(self._fetch_sitemap, sm): sm for sm in all_sitemaps}
            for future in as_completed(futures):
                sm = futures[future]
                try:
                    count = future.result()
                    sys.stdout.write(f"\r  Processed: {self.stats['pages']}/{len(all_sitemaps)}, domains: {len(self.domains):,}   ")
                    sys.stdout.flush()
                except Exception as e:
                    pass
        
        print()
        self.save(self.domains, 'reviews_io')
        return self.domains.copy()

    def _fetch_trustedsite_page(self, args):
        """Fetch single TrustedSite page"""
        cat, page = args
        url = f"https://www.trustedsite.com/directory/{cat}/?page={page}"
        content = self.fetch(url)
        found = set(re.findall(r'/verify\?host=([a-zA-Z0-9][a-zA-Z0-9\-\.]+\.[a-z]{2,})', content))
        with self.lock:
            new = found - self.domains
            self.domains.update(new)
            self.stats['pages'] += 1
        return (cat, page, len(found))

    def scrape_trustedsite(self, max_pages=500):
        """TrustedSite - parallel categories and pages"""
        self.log("=" * 60)
        self.log(f"TRUSTEDSITE - {self.threads} threads, max {max_pages} pages/cat")
        self.log("=" * 60)
        self.domains = set()
        self.stats = {'pages': 0, 'errors': 0}
        
        # Get all directories
        sitemap = self.fetch("https://www.trustedsite.com/sitemap-xml")
        dirs = list(set(re.findall(r'/directory/([^/]+)/', sitemap)))
        self.log(f"Found {len(dirs)} directories")
        
        # Generate all page URLs
        tasks = []
        for d in dirs:
            for page in range(1, max_pages + 1):
                tasks.append((d, page))
        
        self.log(f"Total tasks: {len(tasks):,}")
        
        start = time.time()
        empty_cats = {}  # Track empty pages per category
        
        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            futures = {executor.submit(self._fetch_trustedsite_page, task): task for task in tasks}
            
            completed = 0
            for future in as_completed(futures):
                completed += 1
                try:
                    cat, page, count = future.result()
                    if count == 0:
                        empty_cats[cat] = empty_cats.get(cat, 0) + 1
                except:
                    pass
                
                if completed % 100 == 0:
                    elapsed = time.time() - start
                    rate = completed / elapsed if elapsed > 0 else 0
                    eta = (len(tasks) - completed) / rate if rate > 0 else 0
                    sys.stdout.write(f"\r  Pages: {completed:,}/{len(tasks):,} | Domains: {len(self.domains):,} | {rate:.0f}/s | ETA: {eta/60:.0f}m   ")
                    sys.stdout.flush()
        
        print()
        self.save(self.domains, 'trustedsite')
        return self.domains.copy()

    def scrape_feedaty(self):
        """Feedaty - Italian shops"""
        self.log("=" * 60)
        self.log("FEEDATY")
        self.log("=" * 60)
        
        content = self.fetch("https://www.feedaty.com/feedaty/smurls.xml")
        shops = set(re.findall(r'/recensioni/([a-zA-Z0-9][a-zA-Z0-9\-]+)', content))
        
        content2 = self.fetch("https://www.feedaty.com/sitemap.xml")
        shops2 = set(re.findall(r'/recensioni/([a-zA-Z0-9][a-zA-Z0-9\-]+)', content2))
        shops.update(shops2)
        
        self.log(f"Found {len(shops):,} shop names")
        
        path = self.output_dir / 'feedaty_shops.txt'
        with open(path, 'w') as f:
            for s in sorted(shops):
                f.write(s + '\n')
        return shops

    def run_all(self):
        self.log("=" * 60)
        self.log(f"MULTI-SOURCE SCRAPER v3.0 - {self.threads} THREADS")
        self.log("=" * 60)
        start = time.time()
        
        results = {}
        results['reviews_io'] = self.scrape_reviews_io()
        results['trustedsite'] = self.scrape_trustedsite()
        results['feedaty'] = self.scrape_feedaty()
        
        # Combine
        all_domains = set()
        all_domains.update(results['reviews_io'])
        all_domains.update(results['trustedsite'])
        
        combined_path = self.output_dir / 'all_domains.txt'
        with open(combined_path, 'w') as f:
            for d in sorted(all_domains):
                f.write(d + '\n')
        
        elapsed = time.time() - start
        self.log("\n" + "=" * 60)
        self.log("FINAL SUMMARY")
        self.log("=" * 60)
        self.log(f"  Reviews.io:  {len(results['reviews_io']):,}")
        self.log(f"  TrustedSite: {len(results['trustedsite']):,}")
        self.log(f"  Feedaty:     {len(results['feedaty']):,}")
        self.log(f"  ─────────────────────────────")
        self.log(f"  TOTAL:       {len(all_domains):,}")
        self.log(f"  Time:        {elapsed/60:.1f} min")
        self.log("=" * 60)


def main():
    parser = argparse.ArgumentParser(description='Multi-Source Domain Scraper v3.0')
    parser.add_argument('-o', '--output', default='domains_collected', help='Output directory')
    parser.add_argument('-t', '--threads', type=int, default=20, help='Number of threads (default: 20)')
    parser.add_argument('-s', '--source', choices=['reviews_io', 'trustedsite', 'feedaty', 'all'], 
                       default='all', help='Source to scrape')
    parser.add_argument('--max-pages', type=int, default=500, help='Max pages per TrustedSite category')
    args = parser.parse_args()
    
    collector = DomainCollector(args.output, args.threads)
    
    if args.source == 'all':
        collector.run_all()
    elif args.source == 'reviews_io':
        collector.scrape_reviews_io()
    elif args.source == 'trustedsite':
        collector.scrape_trustedsite(args.max_pages)
    elif args.source == 'feedaty':
        collector.scrape_feedaty()


if __name__ == '__main__':
    main()
