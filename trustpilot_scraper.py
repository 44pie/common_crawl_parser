#!/usr/bin/env python3
"""
Trustpilot Sitemap Domain Scraper
Collects domains from Trustpilot sitemaps by country
"""

import argparse
import requests
import re
import os
import sys
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# Available locales on Trustpilot
LOCALES = {
    'us': 'en-us', 'gb': 'en-gb', 'au': 'en-au', 'ca': 'en-ca', 'nz': 'en-nz',
    'ie': 'en-ie', 'de': 'de-de', 'at': 'de-at', 'ch': 'de-ch',
    'fr': 'fr-fr', 'be': 'fr-be', 'es': 'es-es', 'it': 'it-it',
    'nl': 'nl-nl', 'dk': 'da-dk', 'se': 'sv-se', 'no': 'nb-no',
    'fi': 'fi-fi', 'pl': 'pl-pl', 'pt': 'pt-pt', 'br': 'pt-br',
    'jp': 'ja-jp', 'cz': 'cs-cz', 'hu': 'hu-hu', 'ro': 'ro-ro',
    'bg': 'bg-bg', 'sk': 'sk-sk', 'ru': 'ru-ru', 'ua': 'uk-ua',
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
    """Save domains to base log"""
    with open(base_log, 'a', encoding='utf-8') as f:
        for d in domains:
            f.write(d + '\n')


def fetch_sitemap(url: str, timeout: int = 60) -> str:
    """Fetch sitemap content"""
    try:
        resp = requests.get(url, timeout=timeout)
        if resp.status_code == 200:
            return resp.text
        return ""
    except Exception:
        return ""


def extract_domains(content: str) -> set:
    """Extract domains from sitemap XML"""
    return set(DOMAIN_PATTERN.findall(content))


def get_max_page(locale: str) -> int:
    """Find maximum page number for locale"""
    for page in range(50, 0, -1):
        url = BASE_URL.format(page=page, locale=locale)
        try:
            resp = requests.head(url, timeout=10)
            if resp.status_code == 200:
                return page
        except:
            continue
    return 0


def scrape_locale(locale: str, existing: set, verbose: bool = True) -> set:
    """Scrape all pages for a locale"""
    new_domains = set()
    max_page = get_max_page(locale)
    
    if max_page == 0:
        if verbose:
            print(f"  [{locale}] No pages found")
        return new_domains
    
    if verbose:
        print(f"  [{locale}] Found {max_page} pages, scraping...")
    
    for page in range(1, max_page + 1):
        url = BASE_URL.format(page=page, locale=locale)
        content = fetch_sitemap(url)
        domains = extract_domains(content)
        
        # Filter out already collected
        fresh = domains - existing - new_domains
        new_domains.update(fresh)
        
        if verbose and page % 5 == 0:
            print(f"    Page {page}/{max_page}: +{len(fresh)} new (total: {len(new_domains)})")
    
    if verbose:
        print(f"  [{locale}] Done: {len(new_domains)} new domains")
    
    return new_domains


def main():
    parser = argparse.ArgumentParser(
        description='Trustpilot Sitemap Domain Scraper',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python trustpilot_scraper.py -a -o ./domains
      Scrape ALL countries, save to ./domains/

  python trustpilot_scraper.py -c us,gb,de -o ./domains
      Scrape only US, UK, Germany

  python trustpilot_scraper.py -c us
      Scrape US only, print to stdout

Available countries:
  """ + ', '.join(sorted(LOCALES.keys()))
    )
    
    parser.add_argument('-c', '--country', 
                        help='Countries to scrape (comma-separated: us,gb,de)')
    parser.add_argument('-a', '--all', action='store_true',
                        help='Scrape ALL countries')
    parser.add_argument('-o', '--out', default=None,
                        help='Output directory (creates country .txt files)')
    parser.add_argument('-t', '--threads', type=int, default=3,
                        help='Parallel threads (default: 3)')
    parser.add_argument('-q', '--quiet', action='store_true',
                        help='Quiet mode')
    
    args = parser.parse_args()
    
    if not args.country and not args.all:
        parser.print_help()
        print("\nError: Specify -c COUNTRIES or -a for all")
        sys.exit(1)
    
    # Determine countries to scrape
    if args.all:
        countries = list(LOCALES.keys())
    else:
        countries = [c.strip().lower() for c in args.country.split(',')]
        invalid = [c for c in countries if c not in LOCALES]
        if invalid:
            print(f"Unknown countries: {', '.join(invalid)}")
            print(f"Available: {', '.join(sorted(LOCALES.keys()))}")
            sys.exit(1)
    
    # Setup output directory
    out_dir = None
    base_log = Path('base.log')
    
    if args.out:
        out_dir = Path(args.out)
        out_dir.mkdir(parents=True, exist_ok=True)
        base_log = out_dir / 'base.log'
    
    # Load existing domains
    existing = load_existing(base_log)
    if existing and not args.quiet:
        print(f"Loaded {len(existing):,} existing domains from base.log")
    
    if not args.quiet:
        print(f"\nScraping {len(countries)} countries: {', '.join(countries)}")
        print("=" * 50)
    
    all_new = set()
    country_domains = {}
    
    for country in countries:
        locale = LOCALES[country]
        if not args.quiet:
            print(f"\n[{country.upper()}] ({locale})")
        
        new_domains = scrape_locale(locale, existing | all_new, not args.quiet)
        
        if new_domains:
            country_domains[country] = new_domains
            all_new.update(new_domains)
            
            # Save to country file
            if out_dir:
                country_file = out_dir / f"{country}.txt"
                # Append new domains
                with open(country_file, 'a', encoding='utf-8') as f:
                    for d in sorted(new_domains):
                        f.write(d + '\n')
    
    # Update base.log
    if all_new:
        save_to_base(base_log, all_new)
    
    # Summary
    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)
    print(f"Total new domains: {len(all_new):,}")
    print(f"Base.log total:    {len(existing) + len(all_new):,}")
    
    if country_domains:
        print("\nBy country:")
        for c, doms in sorted(country_domains.items(), key=lambda x: -len(x[1])):
            print(f"  {c.upper()}: {len(doms):,}")
    
    if out_dir:
        print(f"\nOutput: {out_dir}/")


if __name__ == '__main__':
    main()
