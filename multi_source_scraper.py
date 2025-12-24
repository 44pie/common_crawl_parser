#!/usr/bin/env python3
"""
Multi-Source Domain Scraper v2.0 - COMPLETE COLLECTION
Collects ALL domains from: Reviews.io (US+UK), TrustedSite (full pagination), Feedaty
"""

import requests
import re
import sys
import time
import argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

class DomainCollector:
    def __init__(self, output_dir='domains_collected'):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        
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
            resp = self.session.get(url, timeout=timeout)
            return resp.text
        except:
            return ""

    def scrape_reviews_io(self):
        """Reviews.io - US + UK sitemaps"""
        self.log("=" * 60)
        self.log("REVIEWS.IO (US + UK)")
        self.log("=" * 60)
        domains = set()
        
        sources = [
            ("https://www.reviews.io/sitemap.xml", "US"),
            ("https://www.reviews.co.uk/sitemap.xml", "UK"),
        ]
        
        for sitemap_url, region in sources:
            self.log(f"\n[{region}] Fetching {sitemap_url}")
            content = self.fetch(sitemap_url)
            
            # Check if it's a sitemap index or direct sitemap
            child_sitemaps = re.findall(r'<loc>([^<]+\.xml)</loc>', content)
            
            if child_sitemaps:
                # It's an index - fetch children
                self.log(f"  Found {len(child_sitemaps)} child sitemaps")
                for i, sm in enumerate(child_sitemaps, 1):
                    sm_content = self.fetch(sm)
                    found = set(re.findall(r'/company-reviews/store/([a-zA-Z0-9][a-zA-Z0-9\-\.]+\.[a-z]{2,})', sm_content))
                    domains.update(found)
                    self.log(f"  [{i}/{len(child_sitemaps)}] {sm.split('/')[-1]}: +{len(found):,} (total: {len(domains):,})")
                    time.sleep(0.2)
            else:
                # Direct sitemap with domains
                found = set(re.findall(r'/company-reviews/store/([a-zA-Z0-9][a-zA-Z0-9\-\.]+\.[a-z]{2,})', content))
                domains.update(found)
                self.log(f"  Direct sitemap: {len(found):,} domains")
        
        self.save(domains, 'reviews_io')
        return domains

    def scrape_trustedsite(self, max_pages=500):
        """TrustedSite - ALL directories with FULL pagination"""
        self.log("=" * 60)
        self.log("TRUSTEDSITE (FULL PAGINATION)")
        self.log("=" * 60)
        domains = set()
        
        # Get all directories
        sitemap = self.fetch("https://www.trustedsite.com/sitemap-xml")
        dirs = list(set(re.findall(r'/directory/([^/]+)/', sitemap)))
        self.log(f"Found {len(dirs)} directories")
        
        for di, d in enumerate(dirs, 1):
            cat_domains = set()
            page = 1
            empty_pages = 0
            
            while page <= max_pages:
                url = f"https://www.trustedsite.com/directory/{d}/?page={page}"
                content = self.fetch(url)
                found = set(re.findall(r'/verify\?host=([a-zA-Z0-9][a-zA-Z0-9\-\.]+\.[a-z]{2,})', content))
                
                if not found:
                    empty_pages += 1
                    if empty_pages >= 3:  # 3 empty pages = end
                        break
                else:
                    empty_pages = 0
                    new = found - domains
                    cat_domains.update(new)
                    domains.update(new)
                
                sys.stdout.write(f"\r  [{di}/{len(dirs)}] {d}: page {page}, +{len(cat_domains):,} new, total: {len(domains):,}   ")
                sys.stdout.flush()
                page += 1
                time.sleep(0.15)
            
            print()
        
        self.save(domains, 'trustedsite')
        return domains

    def scrape_feedaty(self):
        """Feedaty - Italian shops"""
        self.log("=" * 60)
        self.log("FEEDATY")
        self.log("=" * 60)
        
        # Main sitemap with shops
        content = self.fetch("https://www.feedaty.com/feedaty/smurls.xml")
        shops = set(re.findall(r'/recensioni/([a-zA-Z0-9][a-zA-Z0-9\-]+)', content))
        
        # Also check sitemap.xml
        content2 = self.fetch("https://www.feedaty.com/sitemap.xml")
        shops2 = set(re.findall(r'/recensioni/([a-zA-Z0-9][a-zA-Z0-9\-]+)', content2))
        shops.update(shops2)
        
        self.log(f"Found {len(shops):,} shop names")
        
        path = self.output_dir / 'feedaty_shops.txt'
        with open(path, 'w') as f:
            for s in sorted(shops):
                f.write(s + '\n')
        self.log(f"Saved to {path}")
        return shops

    def run_all(self):
        self.log("=" * 60)
        self.log("MULTI-SOURCE DOMAIN SCRAPER v2.0")
        self.log("COMPLETE COLLECTION MODE")
        self.log("=" * 60)
        start = time.time()
        
        results = {}
        results['reviews_io'] = self.scrape_reviews_io()
        results['trustedsite'] = self.scrape_trustedsite()
        results['feedaty'] = self.scrape_feedaty()
        
        # Combine domains (not feedaty - those are shop names)
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
        self.log(f"  Reviews.io (US+UK): {len(results['reviews_io']):,}")
        self.log(f"  TrustedSite:        {len(results['trustedsite']):,}")
        self.log(f"  Feedaty (shops):    {len(results['feedaty']):,}")
        self.log(f"  ─────────────────────────────")
        self.log(f"  TOTAL UNIQUE:       {len(all_domains):,}")
        self.log(f"  Time: {elapsed/60:.1f} min")
        self.log(f"  Output: {self.output_dir}/")
        self.log("=" * 60)


def main():
    parser = argparse.ArgumentParser(description='Multi-Source Domain Scraper v2.0')
    parser.add_argument('-o', '--output', default='domains_collected', help='Output directory')
    parser.add_argument('-s', '--source', choices=['reviews_io', 'trustedsite', 'feedaty', 'all'], 
                       default='all', help='Source to scrape')
    parser.add_argument('--max-pages', type=int, default=500, help='Max pages per TrustedSite category')
    args = parser.parse_args()
    
    collector = DomainCollector(args.output)
    
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
