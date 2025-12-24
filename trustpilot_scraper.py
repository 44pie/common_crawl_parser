#!/usr/bin/env python3
"""
Trustpilot Sitemap Domain Scraper v3
"""

import argparse
import requests
import re
import sys
import time
from pathlib import Path

LOCALES = {
    'us': 'en-us', 'gb': 'en-gb', 'au': 'en-au', 'ca': 'en-ca', 
    'nz': 'en-nz', 'ie': 'en-ie', 'de': 'de-de', 'at': 'de-at', 
    'ch': 'de-ch', 'fr': 'fr-fr', 'be': 'fr-be', 'es': 'es-es', 
    'it': 'it-it', 'nl': 'nl-nl', 'dk': 'da-dk',
    'be-nl': 'nl-be',  # Belgium (Dutch)
}

BASE_URL = "https://sitemaps.trustpilot.com/domains{page}_{locale}.xml"
# Fixed regex - matches any subdomain (www, uk, de, fr, etc.)
DOMAIN_PATTERN = re.compile(r'<loc>https://[a-z]+\.trustpilot\.com/review/([^<]+)</loc>')


def load_existing(base_log: Path) -> set:
    if not base_log.exists():
        return set()
    with open(base_log, 'r', encoding='utf-8', errors='ignore') as f:
        return set(line.strip() for line in f if line.strip())


def save_to_base(base_log: Path, domains: set):
    with open(base_log, 'a', encoding='utf-8') as f:
        for d in sorted(domains):
            f.write(d + '\n')


def fetch_sitemap(url: str, timeout: int = 120) -> str:
    try:
        resp = requests.get(url, timeout=timeout)
        return resp.text if resp.status_code == 200 else ""
    except:
        return ""


def extract_domains(content: str) -> set:
    return set(DOMAIN_PATTERN.findall(content))


def get_max_page(locale: str) -> int:
    low, high = 1, 100
    max_found = 0
    while low <= high:
        mid = (low + high) // 2
        url = BASE_URL.format(page=mid, locale=locale)
        try:
            resp = requests.head(url, timeout=10)
            if resp.status_code == 200:
                max_found = mid
                low = mid + 1
            else:
                high = mid - 1
        except:
            high = mid - 1
    return max_found


def progress_bar(current, total, width=30, prefix=''):
    pct = current / total if total > 0 else 0
    filled = int(width * pct)
    bar = '█' * filled + '░' * (width - filled)
    sys.stdout.write(f'\r  {prefix}[{bar}] {current}/{total} ({pct*100:.0f}%)')
    sys.stdout.flush()


def scrape_locale(locale: str, country: str, existing: set) -> set:
    new_domains = set()
    
    sys.stdout.write(f"  Finding pages...")
    sys.stdout.flush()
    
    max_page = get_max_page(locale)
    
    if max_page == 0:
        print(f"\r  No sitemaps found                    ")
        return new_domains
    
    print(f"\r  Found {max_page} pages                  ")
    
    total_found = 0
    
    for page in range(1, max_page + 1):
        url = BASE_URL.format(page=page, locale=locale)
        progress_bar(page, max_page, prefix=f'{country.upper()} ')
        
        content = fetch_sitemap(url)
        if not content:
            continue
            
        domains = extract_domains(content)
        total_found += len(domains)
        fresh = domains - existing - new_domains
        new_domains.update(fresh)
    
    print(f"\r  {country.upper()}: {total_found:,} found | {len(new_domains):,} new" + " " * 30)
    return new_domains


def main():
    parser = argparse.ArgumentParser(description='Trustpilot Sitemap Scraper v3')
    parser.add_argument('-c', '--country', help='Countries (comma-separated)')
    parser.add_argument('-a', '--all', action='store_true', help='All countries')
    parser.add_argument('-o', '--out', required=True, help='Output directory')
    args = parser.parse_args()
    
    if not args.country and not args.all:
        parser.print_help()
        sys.exit(1)
    
    countries = list(LOCALES.keys()) if args.all else [c.strip().lower() for c in args.country.split(',')]
    
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    base_log = out_dir / 'base.log'
    
    existing = load_existing(base_log)
    
    print("=" * 60)
    print("TRUSTPILOT SCRAPER v3")
    print("=" * 60)
    print(f"Countries: {len(countries)}")
    print(f"Existing:  {len(existing):,}")
    print("=" * 60)
    
    all_new = set()
    stats = {}
    start = time.time()
    
    for i, country in enumerate(countries, 1):
        if country not in LOCALES:
            print(f"\n[{i}/{len(countries)}] {country.upper()} - UNKNOWN, skipping")
            continue
            
        locale = LOCALES[country]
        print(f"\n[{i}/{len(countries)}] {country.upper()} ({locale})")
        
        new_domains = scrape_locale(locale, country, existing | all_new)
        
        if new_domains:
            stats[country] = len(new_domains)
            all_new.update(new_domains)
            
            country_file = out_dir / f"{country}.txt"
            with open(country_file, 'a', encoding='utf-8') as f:
                for d in sorted(new_domains):
                    f.write(d + '\n')
    
    if all_new:
        save_to_base(base_log, all_new)
    
    print("\n" + "=" * 60)
    print(f"DONE in {time.time()-start:.1f}s")
    print(f"New: {len(all_new):,} | Total: {len(existing)+len(all_new):,}")
    if stats:
        print("\nBy country:")
        for c, n in sorted(stats.items(), key=lambda x: -x[1]):
            print(f"  {c.upper()}: {n:,}")
    print("=" * 60)


if __name__ == '__main__':
    main()
