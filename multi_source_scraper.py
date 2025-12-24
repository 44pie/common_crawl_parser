#!/usr/bin/env python3
"""
Multi-Source Domain Scraper v1.0
Collects domains from: Reviews.io, TrustedSite, Feedaty, Wappalyzer
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
        self.all_domains = set()
        
    def log(self, msg):
        print(f"[{time.strftime('%H:%M:%S')}] {msg}")
    
    def save(self, domains, source):
        path = self.output_dir / f"{source}.txt"
        with open(path, 'w') as f:
            for d in sorted(domains):
                f.write(d + '\n')
        self.log(f"  Saved {len(domains):,} to {path}")
        
    def fetch(self, url, timeout=30):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=timeout)
            return resp.text
        except Exception as e:
            self.log(f"  Error fetching {url}: {e}")
            return ""

    def scrape_reviews_io(self):
        """Reviews.io sitemaps"""
        self.log("=" * 50)
        self.log("REVIEWS.IO")
        domains = set()
        
        # Get all sitemaps
        main = self.fetch("https://www.reviews.io/sitemap.xml")
        sitemaps = re.findall(r'https://www\.reviews\.io/sitemap-[^<]+\.xml', main)
        self.log(f"Found {len(sitemaps)} sitemaps")
        
        for i, sm in enumerate(sitemaps, 1):
            content = self.fetch(sm)
            # Extract domains from /company-reviews/store/domain.com
            found = re.findall(r'/company-reviews/store/([a-zA-Z0-9][a-zA-Z0-9\-\.]+\.[a-z]{2,})', content)
            domains.update(found)
            self.log(f"  [{i}/{len(sitemaps)}] {sm.split('/')[-1]}: {len(found)} domains")
            time.sleep(0.3)
            
        self.save(domains, 'reviews_io')
        return domains

    def scrape_trustedsite(self):
        """TrustedSite directories"""
        self.log("=" * 50)
        self.log("TRUSTEDSITE")
        domains = set()
        
        # Get directories from sitemap
        sitemap = self.fetch("https://www.trustedsite.com/sitemap-xml")
        dirs = set(re.findall(r'/directory/([^/]+)/', sitemap))
        self.log(f"Found {len(dirs)} directories")
        
        for i, d in enumerate(dirs, 1):
            url = f"https://www.trustedsite.com/directory/{d}/"
            content = self.fetch(url)
            # Extract from /verify?host=domain.com
            found = re.findall(r'/verify\?host=([a-zA-Z0-9][a-zA-Z0-9\-\.]+\.[a-z]{2,})', content)
            domains.update(found)
            sys.stdout.write(f"\r  [{i}/{len(dirs)}] {d}: {len(domains):,} total")
            sys.stdout.flush()
            time.sleep(0.3)
        
        print()
        self.save(domains, 'trustedsite')
        return domains

    def scrape_feedaty(self):
        """Feedaty sitemap - Italian review platform"""
        self.log("=" * 50)
        self.log("FEEDATY")
        
        content = self.fetch("https://www.feedaty.com/feedaty/smurls.xml")
        # Extract shop names from /recensioni/shopname
        shops = set(re.findall(r'/recensioni/([a-zA-Z0-9\-]+)', content))
        self.log(f"Found {len(shops):,} shops (names, not full domains)")
        
        # Save shop names
        path = self.output_dir / 'feedaty_shops.txt'
        with open(path, 'w') as f:
            for s in sorted(shops):
                f.write(s + '\n')
        self.log(f"  Saved to {path}")
        return shops

    def scrape_wappalyzer(self):
        """Wappalyzer - technology detection"""
        self.log("=" * 50)
        self.log("WAPPALYZER")
        domains = set()
        
        sitemap = self.fetch("https://www.wappalyzer.com/sitemap.xml")
        # Find technology pages
        tech_urls = re.findall(r'https://www\.wappalyzer\.com/websites/[^<]+', sitemap)
        self.log(f"Found {len(tech_urls)} technology website pages")
        
        # Extract domains from website listing pages
        for i, url in enumerate(tech_urls[:50], 1):  # Limit to avoid overload
            content = self.fetch(url)
            found = re.findall(r'(?:https?://)?([a-zA-Z0-9][a-zA-Z0-9\-\.]+\.[a-z]{2,})', content)
            # Filter common non-domains
            found = [d for d in found if not any(x in d for x in ['wappalyzer', 'googleapis', 'cloudfront'])]
            domains.update(found)
            sys.stdout.write(f"\r  [{i}/50] {len(domains):,} domains")
            sys.stdout.flush()
            time.sleep(0.5)
        
        print()
        self.save(domains, 'wappalyzer')
        return domains

    def run_all(self):
        self.log("=" * 50)
        self.log("MULTI-SOURCE DOMAIN SCRAPER")
        self.log("=" * 50)
        start = time.time()
        
        results = {}
        
        # Run all scrapers
        results['reviews_io'] = self.scrape_reviews_io()
        results['trustedsite'] = self.scrape_trustedsite()
        results['feedaty'] = self.scrape_feedaty()
        # results['wappalyzer'] = self.scrape_wappalyzer()  # Slow, optional
        
        # Combine all
        all_domains = set()
        for source, domains in results.items():
            if source != 'feedaty':  # Feedaty has shop names, not domains
                all_domains.update(domains)
        
        # Save combined
        combined_path = self.output_dir / 'all_domains.txt'
        with open(combined_path, 'w') as f:
            for d in sorted(all_domains):
                f.write(d + '\n')
        
        elapsed = time.time() - start
        self.log("=" * 50)
        self.log("SUMMARY")
        self.log("=" * 50)
        for source, domains in results.items():
            self.log(f"  {source}: {len(domains):,}")
        self.log(f"  TOTAL UNIQUE: {len(all_domains):,}")
        self.log(f"  Time: {elapsed:.0f}s")
        self.log(f"  Output: {self.output_dir}/")


def main():
    parser = argparse.ArgumentParser(description='Multi-Source Domain Scraper')
    parser.add_argument('-o', '--output', default='domains_collected', help='Output directory')
    parser.add_argument('-s', '--source', choices=['reviews_io', 'trustedsite', 'feedaty', 'wappalyzer', 'all'], 
                       default='all', help='Source to scrape')
    args = parser.parse_args()
    
    collector = DomainCollector(args.output)
    
    if args.source == 'all':
        collector.run_all()
    elif args.source == 'reviews_io':
        collector.scrape_reviews_io()
    elif args.source == 'trustedsite':
        collector.scrape_trustedsite()
    elif args.source == 'feedaty':
        collector.scrape_feedaty()
    elif args.source == 'wappalyzer':
        collector.scrape_wappalyzer()


if __name__ == '__main__':
    main()
