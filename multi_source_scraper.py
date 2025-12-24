#!/usr/bin/env python3
"""
Multi-Source Domain Scraper v4.0 - SMART PAGINATION
Auto-stops when category is exhausted
"""

import requests
import re
import sys
import time
import argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from collections import defaultdict

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

class DomainCollector:
    def __init__(self, output_dir='domains_collected', threads=50):
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
            return ""

    def scrape_reviews_io(self):
        """Reviews.io - US + UK parallel"""
        self.log("=" * 60)
        self.log(f"REVIEWS.IO (US + UK) - {self.threads} threads")
        self.log("=" * 60)
        domains = set()
        
        def fetch_sitemap(url):
            content = self.fetch(url)
            return set(re.findall(r'/company-reviews/store/([a-zA-Z0-9][a-zA-Z0-9\-\.]+\.[a-z]{2,})', content))
        
        # Get US sitemaps
        us_content = self.fetch("https://www.reviews.io/sitemap.xml")
        us_sitemaps = re.findall(r'<loc>([^<]+\.xml)</loc>', us_content)
        us_sitemaps.append("https://www.reviews.co.uk/sitemap.xml")
        self.log(f"Found {len(us_sitemaps)} sitemaps")
        
        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            futures = {executor.submit(fetch_sitemap, sm): sm for sm in us_sitemaps}
            for future in as_completed(futures):
                domains.update(future.result())
                sys.stdout.write(f"\r  Domains: {len(domains):,}   ")
                sys.stdout.flush()
        
        print()
        self.save(domains, 'reviews_io')
        return domains

    def scrape_trustedsite(self):
        """TrustedSite - SMART pagination per category"""
        self.log("=" * 60)
        self.log(f"TRUSTEDSITE - {self.threads} threads, SMART STOP")
        self.log("=" * 60)
        
        # Get directories
        sitemap = self.fetch("https://www.trustedsite.com/sitemap-xml")
        dirs = list(set(re.findall(r'/directory/([^/]+)/', sitemap)))
        self.log(f"Found {len(dirs)} directories")
        
        all_domains = set()
        
        def scrape_category(cat):
            """Scrape single category with smart stop"""
            cat_domains = set()
            page = 1
            empty_streak = 0
            
            while empty_streak < 3:  # Stop after 3 empty pages
                url = f"https://www.trustedsite.com/directory/{cat}/?page={page}"
                content = self.fetch(url)
                found = set(re.findall(r'/verify\?host=([a-zA-Z0-9][a-zA-Z0-9\-\.]+\.[a-z]{2,})', content))
                
                if found:
                    cat_domains.update(found)
                    empty_streak = 0
                else:
                    empty_streak += 1
                
                page += 1
                time.sleep(0.05)  # Small delay
            
            return cat, cat_domains, page - 1
        
        start = time.time()
        completed = 0
        
        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            futures = {executor.submit(scrape_category, d): d for d in dirs}
            
            for future in as_completed(futures):
                completed += 1
                cat, domains, pages = future.result()
                new = domains - all_domains
                all_domains.update(new)
                
                elapsed = time.time() - start
                rate = completed / elapsed if elapsed > 0 else 0
                eta = (len(dirs) - completed) / rate if rate > 0 else 0
                
                sys.stdout.write(f"\r  [{completed}/{len(dirs)}] {cat}: {len(domains)} | Total: {len(all_domains):,} | ETA: {eta:.0f}s   ")
                sys.stdout.flush()
        
        print()
        self.save(all_domains, 'trustedsite')
        return all_domains

    def scrape_feedaty(self):
        """Feedaty"""
        self.log("=" * 60)
        self.log("FEEDATY")
        self.log("=" * 60)
        
        content = self.fetch("https://www.feedaty.com/feedaty/smurls.xml")
        shops = set(re.findall(r'/recensioni/([a-zA-Z0-9][a-zA-Z0-9\-]+)', content))
        
        content2 = self.fetch("https://www.feedaty.com/sitemap.xml")
        shops.update(set(re.findall(r'/recensioni/([a-zA-Z0-9][a-zA-Z0-9\-]+)', content2)))
        
        self.log(f"Found {len(shops):,} shops")
        
        path = self.output_dir / 'feedaty_shops.txt'
        with open(path, 'w') as f:
            for s in sorted(shops):
                f.write(s + '\n')
        return shops

    def run_all(self):
        self.log("=" * 60)
        self.log(f"MULTI-SOURCE SCRAPER v4.0 - {self.threads} THREADS")
        self.log("=" * 60)
        start = time.time()
        
        results = {}
        results['reviews_io'] = self.scrape_reviews_io()
        results['trustedsite'] = self.scrape_trustedsite()
        results['feedaty'] = self.scrape_feedaty()
        
        all_domains = results['reviews_io'] | results['trustedsite']
        
        combined_path = self.output_dir / 'all_domains.txt'
        with open(combined_path, 'w') as f:
            for d in sorted(all_domains):
                f.write(d + '\n')
        
        elapsed = time.time() - start
        self.log("\n" + "=" * 60)
        self.log("SUMMARY")
        self.log("=" * 60)
        self.log(f"  Reviews.io:  {len(results['reviews_io']):,}")
        self.log(f"  TrustedSite: {len(results['trustedsite']):,}")
        self.log(f"  Feedaty:     {len(results['feedaty']):,}")
        self.log(f"  TOTAL:       {len(all_domains):,}")
        self.log(f"  Time:        {elapsed:.0f}s")
        self.log("=" * 60)


def main():
    parser = argparse.ArgumentParser(description='Multi-Source Domain Scraper v4.0')
    parser.add_argument('-o', '--output', default='domains_collected')
    parser.add_argument('-t', '--threads', type=int, default=50)
    parser.add_argument('-s', '--source', choices=['reviews_io', 'trustedsite', 'feedaty', 'all'], default='all')
    args = parser.parse_args()
    
    collector = DomainCollector(args.output, args.threads)
    
    if args.source == 'all':
        collector.run_all()
    elif args.source == 'reviews_io':
        collector.scrape_reviews_io()
    elif args.source == 'trustedsite':
        collector.scrape_trustedsite()
    elif args.source == 'feedaty':
        collector.scrape_feedaty()


if __name__ == '__main__':
    main()
