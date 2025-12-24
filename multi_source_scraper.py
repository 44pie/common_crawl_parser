#!/usr/bin/env python3
"""
Multi-Source Domain Scraper v7.0 - LINE BY LINE PROGRESS
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
    print(f"[REVIEWS.IO] Fetching sitemaps...", flush=True)
    domains = set()
    
    us = fetch("https://www.reviews.io/sitemap.xml")
    sitemaps = re.findall(r'<loc>([^<]+\.xml)</loc>', us)
    sitemaps.append("https://www.reviews.co.uk/sitemap.xml")
    print(f"[REVIEWS.IO] Found {len(sitemaps)} sitemaps", flush=True)
    
    def get_domains(url):
        content = fetch(url)
        return set(re.findall(r'/company-reviews/store/([a-zA-Z0-9][a-zA-Z0-9\-\.]+\.[a-z]{2,})', content))
    
    with ThreadPoolExecutor(max_workers=threads) as ex:
        futures = {ex.submit(get_domains, sm): sm for sm in sitemaps}
        for f in as_completed(futures):
            domains.update(f.result())
    
    print(f"[REVIEWS.IO] Done: {len(domains):,} domains", flush=True)
    return domains

def scrape_trustedsite(threads=50):
    print(f"[TRUSTEDSITE] Fetching categories...", flush=True)
    
    sitemap = fetch("https://www.trustedsite.com/sitemap-xml")
    cats = list(set(re.findall(r'/directory/([^/]+)/', sitemap)))
    print(f"[TRUSTEDSITE] Found {len(cats)} categories", flush=True)
    
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
        return cat, domains, page - 1
    
    done = 0
    total_pages = 0
    start = time.time()
    
    with ThreadPoolExecutor(max_workers=threads) as ex:
        futures = {ex.submit(scrape_cat, c): c for c in cats}
        for f in as_completed(futures):
            cat, doms, pages = f.result()
            all_domains.update(doms)
            done += 1
            total_pages += pages
            elapsed = time.time() - start
            # Print every 10 categories
            if done % 10 == 0 or done == len(cats):
                print(f"[TRUSTEDSITE] {done}/{len(cats)} | {total_pages} pages | {len(all_domains):,} domains | {elapsed:.0f}s", flush=True)
    
    return all_domains

def scrape_feedaty():
    print(f"[FEEDATY] Fetching...", flush=True)
    content = fetch("https://www.feedaty.com/feedaty/smurls.xml")
    content += fetch("https://www.feedaty.com/sitemap.xml")
    shops = set(re.findall(r'/recensioni/([a-zA-Z0-9][a-zA-Z0-9\-]+)', content))
    print(f"[FEEDATY] Done: {len(shops):,} shops", flush=True)
    return shops

def main():
    p = argparse.ArgumentParser()
    p.add_argument('-o', '--output', default='domains_out')
    p.add_argument('-t', '--threads', type=int, default=50)
    p.add_argument('-s', '--source', choices=['reviews', 'trustedsite', 'feedaty', 'all'], default='all')
    args = p.parse_args()
    
    Path(args.output).mkdir(exist_ok=True)
    
    print("=" * 60, flush=True)
    print(f"DOMAIN SCRAPER v7.0 | {args.threads} threads", flush=True)
    print("=" * 60, flush=True)
    
    start = time.time()
    all_domains = set()
    
    if args.source in ['all', 'reviews']:
        d = scrape_reviews_io(args.threads)
        with open(f"{args.output}/reviews_io.txt", 'w') as f:
            f.write('\n'.join(sorted(d)))
        print(f"[SAVED] {args.output}/reviews_io.txt ({len(d):,})", flush=True)
        all_domains.update(d)
    
    if args.source in ['all', 'trustedsite']:
        d = scrape_trustedsite(args.threads)
        with open(f"{args.output}/trustedsite.txt", 'w') as f:
            f.write('\n'.join(sorted(d)))
        print(f"[SAVED] {args.output}/trustedsite.txt ({len(d):,})", flush=True)
        all_domains.update(d)
    
    if args.source in ['all', 'feedaty']:
        s = scrape_feedaty()
        with open(f"{args.output}/feedaty.txt", 'w') as f:
            f.write('\n'.join(sorted(s)))
        print(f"[SAVED] {args.output}/feedaty.txt ({len(s):,})", flush=True)
    
    if all_domains:
        with open(f"{args.output}/all_domains.txt", 'w') as f:
            f.write('\n'.join(sorted(all_domains)))
    
    elapsed = time.time() - start
    print("=" * 60, flush=True)
    print(f"TOTAL: {len(all_domains):,} unique domains in {elapsed:.0f}s", flush=True)
    print("=" * 60, flush=True)

if __name__ == '__main__':
    main()
