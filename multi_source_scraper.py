#!/usr/bin/env python3
"""
Multi-Source Domain Scraper v5.0 - FIXED & FAST
"""
import requests, re, sys, time, argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

UA = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

def fetch(url):
    try:
        return requests.get(url, headers=UA, timeout=20).text
    except:
        return ""

def scrape_reviews_io(threads=50):
    print(f"[REVIEWS.IO] Fetching...")
    domains = set()
    
    # US sitemaps
    us = fetch("https://www.reviews.io/sitemap.xml")
    sitemaps = re.findall(r'<loc>([^<]+\.xml)</loc>', us)
    sitemaps.append("https://www.reviews.co.uk/sitemap.xml")
    
    def get_domains(url):
        content = fetch(url)
        return set(re.findall(r'/company-reviews/store/([a-zA-Z0-9][a-zA-Z0-9\-\.]+\.[a-z]{2,})', content))
    
    with ThreadPoolExecutor(max_workers=threads) as ex:
        for result in ex.map(get_domains, sitemaps):
            domains.update(result)
    
    print(f"[REVIEWS.IO] {len(domains):,} domains")
    return domains

def scrape_trustedsite(threads=50):
    print(f"[TRUSTEDSITE] Fetching categories...")
    
    sitemap = fetch("https://www.trustedsite.com/sitemap-xml")
    cats = list(set(re.findall(r'/directory/([^/]+)/', sitemap)))
    print(f"[TRUSTEDSITE] {len(cats)} categories")
    
    all_domains = set()
    
    def scrape_cat(cat):
        domains = set()
        page = 1
        empty = 0
        while empty < 3:
            content = fetch(f"https://www.trustedsite.com/directory/{cat}/?page={page}")
            found = set(re.findall(r'host=([^"&]+)', content))
            if found:
                domains.update(found)
                empty = 0
            else:
                empty += 1
            page += 1
        return cat, domains
    
    done = 0
    with ThreadPoolExecutor(max_workers=threads) as ex:
        futures = {ex.submit(scrape_cat, c): c for c in cats}
        for f in as_completed(futures):
            cat, doms = f.result()
            all_domains.update(doms)
            done += 1
            sys.stdout.write(f"\r[TRUSTEDSITE] {done}/{len(cats)} | {len(all_domains):,} domains   ")
            sys.stdout.flush()
    
    print()
    return all_domains

def scrape_feedaty():
    print(f"[FEEDATY] Fetching...")
    content = fetch("https://www.feedaty.com/feedaty/smurls.xml")
    content += fetch("https://www.feedaty.com/sitemap.xml")
    shops = set(re.findall(r'/recensioni/([a-zA-Z0-9][a-zA-Z0-9\-]+)', content))
    print(f"[FEEDATY] {len(shops):,} shops")
    return shops

def main():
    p = argparse.ArgumentParser()
    p.add_argument('-o', '--output', default='domains_out')
    p.add_argument('-t', '--threads', type=int, default=50)
    p.add_argument('-s', '--source', choices=['reviews', 'trustedsite', 'feedaty', 'all'], default='all')
    args = p.parse_args()
    
    Path(args.output).mkdir(exist_ok=True)
    
    print("=" * 50)
    print(f"DOMAIN SCRAPER v5.0 | {args.threads} threads")
    print("=" * 50)
    
    all_domains = set()
    
    if args.source in ['all', 'reviews']:
        d = scrape_reviews_io(args.threads)
        with open(f"{args.output}/reviews_io.txt", 'w') as f:
            f.write('\n'.join(sorted(d)))
        all_domains.update(d)
    
    if args.source in ['all', 'trustedsite']:
        d = scrape_trustedsite(args.threads)
        with open(f"{args.output}/trustedsite.txt", 'w') as f:
            f.write('\n'.join(sorted(d)))
        all_domains.update(d)
    
    if args.source in ['all', 'feedaty']:
        s = scrape_feedaty()
        with open(f"{args.output}/feedaty.txt", 'w') as f:
            f.write('\n'.join(sorted(s)))
    
    if all_domains:
        with open(f"{args.output}/all_domains.txt", 'w') as f:
            f.write('\n'.join(sorted(all_domains)))
    
    print("=" * 50)
    print(f"TOTAL: {len(all_domains):,} unique domains")
    print(f"Saved to: {args.output}/")
    print("=" * 50)

if __name__ == '__main__':
    main()
