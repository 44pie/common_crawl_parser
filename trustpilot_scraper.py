#!/usr/bin/env python3
"""
Trustpilot Sitemap Domain Scraper v2
Collects domains from Trustpilot sitemaps by country
"""

import argparse
import requests
import re
import sys
import time
from pathlib import Path

# Verified working locales on Trustpilot (no CIS countries)
LOCALES = {
    'us': 'en-us',      # United States
    'gb': 'en-gb',      # United Kingdom
    'au': 'en-au',      # Australia
    'ca': 'en-ca',      # Canada
    'nz': 'en-nz',      # New Zealand
    'ie': 'en-ie',      # Ireland
    'de': 'de-de',      # Germany
    'at': 'de-at',      # Austria
    'ch': 'de-ch',      # Switzerland
    'fr': 'fr-fr',      # France
    'be': 'fr-be',      # Belgium
    'es': 'es-es',      # Spain
    'it': 'it-it',      # Italy
    'nl': 'nl-nl',      # Netherlands
    'dk': 'da-dk',      # Denmark
}

BASE_URL = "https://sitemaps.trustpilot.com/domains{page}_{locale}.xml"
DOMAIN_PATTERN = re.compile(r'<loc>https://www\.trustpilot\.com/review/([^<]+)</loc>')


def load_existing(base_log: Path) -> set:
    """Load already collected domains"""
    if not base_log.exists():
        return set()
    with open(base_log, 'r', encoding='utf-8', errors='ignore') as f:
        return set(line.strip() for line in f if line.strip())


def save_to_base(base_log: Path, domains: set):
    """Append new domains to base log"""
    with open(base_log, 'a', encoding='utf-8') as f:
        for d in sorted(domains):
            f.write(d + '\n')


def fetch_sitemap(url: str, timeout: int = 120) -> str:
    """Fetch sitemap content"""
    try:
        resp = requests.get(url, timeout=timeout)
        if resp.status_code == 200:
            return resp.text
        return ""
    except Exception as e:
        return ""


def extract_domains(content: str) -> set:
    """Extract domains from sitemap XML"""
    return set(DOMAIN_PATTERN.findall(content))


def get_max_page(locale: str) -> int:
    """Find maximum page number for locale using binary search"""
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
    """Print progress bar"""
    pct = current / total if total > 0 else 0
    filled = int(width * pct)
    bar = '█' * filled + '░' * (width - filled)
    sys.stdout.write(f'\r  {prefix}[{bar}] {current}/{total} ({pct*100:.0f}%)')
    sys.stdout.flush()


def scrape_locale(locale: str, country: str, existing: set, verbose: bool = True) -> set:
    """Scrape all pages for a locale"""
    new_domains = set()
    
    if verbose:
        sys.stdout.write(f"  Finding pages...")
        sys.stdout.flush()
    
    max_page = get_max_page(locale)
    
    if max_page == 0:
        if verbose:
            print(f"\r  No sitemaps found for {locale}        ")
        return new_domains
    
    if verbose:
        print(f"\r  Found {max_page} sitemap pages          ")
    
    page_domains = 0
    page_new = 0
    
    for page in range(1, max_page + 1):
        url = BASE_URL.format(page=page, locale=locale)
        
        if verbose:
            progress_bar(page, max_page, prefix=f'{country.upper()} ')
        
        content = fetch_sitemap(url)
        if not content:
            continue
            
        domains = extract_domains(content)
        page_domains += len(domains)
        
        # Filter already collected
        fresh = domains - existing - new_domains
        page_new += len(fresh)
        new_domains.update(fresh)
    
    if verbose:
        print(f"\r  {country.upper()}: {page_domains:,} total | {len(new_domains):,} new | {max_page} pages" + " " * 20)
    
    return new_domains


def main():
    parser = argparse.ArgumentParser(
        description='Trustpilot Sitemap Domain Scraper',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Examples:
  python3 trustpilot_scraper.py -a -o ./domains
      Scrape ALL countries

  python3 trustpilot_scraper.py -c us,gb,de -o ./domains  
      Scrape US, UK, Germany only

Available countries ({len(LOCALES)}):
  {', '.join(sorted(LOCALES.keys()))}
"""
    )
    
    parser.add_argument('-c', '--country', 
                        help='Countries (comma-separated: us,gb,de)')
    parser.add_argument('-a', '--all', action='store_true',
                        help='Scrape ALL countries')
    parser.add_argument('-o', '--out', required=True,
                        help='Output directory')
    parser.add_argument('-q', '--quiet', action='store_true',
                        help='Minimal output')
    
    args = parser.parse_args()
    
    if not args.country and not args.all:
        parser.print_help()
        print("\nError: Use -c COUNTRIES or -a for all")
        sys.exit(1)
    
    # Determine countries
    if args.all:
        countries = list(LOCALES.keys())
    else:
        countries = [c.strip().lower() for c in args.country.split(',')]
        invalid = [c for c in countries if c not in LOCALES]
        if invalid:
            print(f"Unknown: {', '.join(invalid)}")
            print(f"Available: {', '.join(sorted(LOCALES.keys()))}")
            sys.exit(1)
    
    # Setup output
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    base_log = out_dir / 'base.log'
    
    # Load existing
    existing = load_existing(base_log)
    
    print("=" * 60)
    print("TRUSTPILOT SITEMAP SCRAPER")
    print("=" * 60)
    print(f"Countries: {len(countries)}")
    print(f"Output:    {out_dir}/")
    print(f"Existing:  {len(existing):,} domains in base.log")
    print("=" * 60)
    
    all_new = set()
    country_stats = {}
    start = time.time()
    
    for i, country in enumerate(countries, 1):
        locale = LOCALES[country]
        print(f"\n[{i}/{len(countries)}] {country.upper()} ({locale})")
        
        new_domains = scrape_locale(locale, country, existing | all_new, not args.quiet)
        
        if new_domains:
            country_stats[country] = len(new_domains)
            all_new.update(new_domains)
            
            # Save to country file
            country_file = out_dir / f"{country}.txt"
            with open(country_file, 'a', encoding='utf-8') as f:
                for d in sorted(new_domains):
                    f.write(d + '\n')
    
    # Update base.log
    if all_new:
        save_to_base(base_log, all_new)
    
    elapsed = time.time() - start
    
    # Summary
    print("\n" + "=" * 60)
    print("COMPLETE")
    print("=" * 60)
    print(f"Time:         {elapsed:.1f}s")
    print(f"New domains:  {len(all_new):,}")
    print(f"Total in log: {len(existing) + len(all_new):,}")
    
    if country_stats:
        print(f"\nBy country:")
        for c, cnt in sorted(country_stats.items(), key=lambda x: -x[1]):
            print(f"  {c.upper()}: {cnt:,}")
    
    print(f"\nOutput: {out_dir}/")
    print("=" * 60)


if __name__ == '__main__':
    main()
