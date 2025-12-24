#!/usr/bin/env python3
"""
SiteJabber Domain Scraper
Collects e-commerce domains from SiteJabber reviews
"""

import requests
import re
import sys
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

BASE_URL = "https://www.sitejabber.com"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

DOMAIN_PATTERN = re.compile(r'/reviews/([a-zA-Z0-9][a-zA-Z0-9\-\.]+\.[a-zA-Z]{2,})')
PAGE_PATTERN = re.compile(r'page=(\d+)')


def get_categories():
    """Get all category URLs"""
    try:
        resp = requests.get(f"{BASE_URL}/categories", headers=HEADERS, timeout=30)
        cats = set(re.findall(r'/categories/([a-z\-]+)', resp.text))
        # Filter out pagination
        return [c for c in cats if c != 'page-']
    except Exception as e:
        print(f"Error getting categories: {e}")
        return []


def get_max_page(category):
    """Get max page number for category"""
    try:
        resp = requests.get(f"{BASE_URL}/categories/{category}", headers=HEADERS, timeout=30)
        pages = PAGE_PATTERN.findall(resp.text)
        return max(int(p) for p in pages) if pages else 1
    except:
        return 1


def scrape_page(category, page):
    """Scrape domains from a category page"""
    try:
        url = f"{BASE_URL}/categories/{category}?page={page}"
        resp = requests.get(url, headers=HEADERS, timeout=30)
        domains = set(DOMAIN_PATTERN.findall(resp.text))
        return domains
    except:
        return set()


def progress_bar(current, total, width=30, prefix=''):
    pct = current / total if total > 0 else 0
    filled = int(width * pct)
    bar = '█' * filled + '░' * (width - filled)
    sys.stdout.write(f'\r{prefix}[{bar}] {current}/{total} ({pct*100:.0f}%)')
    sys.stdout.flush()


def main():
    import argparse
    parser = argparse.ArgumentParser(description='SiteJabber Domain Scraper')
    parser.add_argument('-o', '--out', default='sitejabber_domains.txt', help='Output file')
    parser.add_argument('-t', '--threads', type=int, default=5, help='Threads')
    args = parser.parse_args()
    
    print("=" * 60)
    print("SITEJABBER DOMAIN SCRAPER")
    print("=" * 60)
    
    # Get categories
    print("\nFetching categories...")
    categories = get_categories()
    print(f"Found {len(categories)} categories")
    
    all_domains = set()
    start = time.time()
    
    for i, cat in enumerate(categories, 1):
        max_page = get_max_page(cat)
        print(f"\n[{i}/{len(categories)}] {cat} ({max_page} pages)")
        
        cat_domains = set()
        for page in range(1, max_page + 1):
            progress_bar(page, max_page, prefix=f'  {cat}: ')
            domains = scrape_page(cat, page)
            new = domains - all_domains
            cat_domains.update(new)
            time.sleep(0.3)  # Rate limit
        
        all_domains.update(cat_domains)
        print(f"\r  {cat}: {len(cat_domains)} new, total: {len(all_domains):,}" + " " * 20)
    
    # Save
    with open(args.out, 'w') as f:
        for d in sorted(all_domains):
            f.write(d + '\n')
    
    elapsed = time.time() - start
    print("\n" + "=" * 60)
    print(f"DONE in {elapsed:.0f}s")
    print(f"Total domains: {len(all_domains):,}")
    print(f"Saved to: {args.out}")
    print("=" * 60)


if __name__ == '__main__':
    main()
