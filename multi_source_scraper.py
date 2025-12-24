#!/usr/bin/env python3
import requests, re, sys, time, argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

sys.stdout.reconfigure(line_buffering=True)
UA = {'User-Agent': 'Mozilla/5.0'}
lock = Lock()
stats = {'cats': 0, 'pages': 0, 'domains': set()}

def fetch(url):
    try: return requests.get(url, headers=UA, timeout=20).text
    except: return ""

def scrape_reviews_io(threads):
    print("[REVIEWS.IO] Start")
    domains = set()
    us = fetch("https://www.reviews.io/sitemap.xml")
    sitemaps = re.findall(r'<loc>([^<]+\.xml)</loc>', us)
    sitemaps.append("https://www.reviews.co.uk/sitemap.xml")
    print(f"[REVIEWS.IO] {len(sitemaps)} sitemaps")
    def get_domains(url):
        return set(re.findall(r'/company-reviews/store/([a-zA-Z0-9][a-zA-Z0-9\-\.]+\.[a-z]{2,})', fetch(url)))
    with ThreadPoolExecutor(max_workers=threads) as ex:
        for r in ex.map(get_domains, sitemaps): domains.update(r)
    print(f"[REVIEWS.IO] DONE: {len(domains):,}")
    return domains

def scrape_page(args):
    cat, page = args
    content = fetch(f"https://www.trustedsite.com/directory/{cat}/?page={page}")
    found = set(re.findall(r'host=([^"&]+)', content))
    with lock:
        stats['pages'] += 1
        stats['domains'].update(found)
        if stats['pages'] % 50 == 0:
            print(f"[TRUSTEDSITE] {stats['pages']} pages | {len(stats['domains']):,} domains")
    return found

def scrape_trustedsite(threads):
    global stats
    stats = {'cats': 0, 'pages': 0, 'domains': set()}
    print("[TRUSTEDSITE] Start")
    sitemap = fetch("https://www.trustedsite.com/sitemap-xml")
    cats = list(set(re.findall(r'/directory/([^/]+)/', sitemap)))
    print(f"[TRUSTEDSITE] {len(cats)} categories, scanning pages...")
    
    # First pass: get page 1 of all categories
    tasks = [(c, 1) for c in cats]
    with ThreadPoolExecutor(max_workers=threads) as ex:
        list(ex.map(scrape_page, tasks))
    
    print(f"[TRUSTEDSITE] Page 1 done: {len(stats['domains']):,} domains, scanning more pages...")
    
    # Second pass: pages 2-50 for all categories
    tasks = [(c, p) for c in cats for p in range(2, 51)]
    with ThreadPoolExecutor(max_workers=threads) as ex:
        list(ex.map(scrape_page, tasks))
    
    print(f"[TRUSTEDSITE] DONE: {len(stats['domains']):,} domains")
    return stats['domains']

def scrape_feedaty():
    print("[FEEDATY] Start")
    content = fetch("https://www.feedaty.com/feedaty/smurls.xml") + fetch("https://www.feedaty.com/sitemap.xml")
    shops = set(re.findall(r'/recensioni/([a-zA-Z0-9][a-zA-Z0-9\-]+)', content))
    print(f"[FEEDATY] DONE: {len(shops):,}")
    return shops

def main():
    p = argparse.ArgumentParser()
    p.add_argument('-o', '--output', default='domains_out')
    p.add_argument('-t', '--threads', type=int, default=50)
    args = p.parse_args()
    Path(args.output).mkdir(exist_ok=True)
    print(f"{'='*50}\nSCRAPER v9 | {args.threads} threads\n{'='*50}")
    all_d = set()
    d = scrape_reviews_io(args.threads)
    open(f"{args.output}/reviews_io.txt",'w').write('\n'.join(sorted(d)))
    all_d.update(d)
    d = scrape_trustedsite(args.threads)
    open(f"{args.output}/trustedsite.txt",'w').write('\n'.join(sorted(d)))
    all_d.update(d)
    s = scrape_feedaty()
    open(f"{args.output}/feedaty.txt",'w').write('\n'.join(sorted(s)))
    open(f"{args.output}/all_domains.txt",'w').write('\n'.join(sorted(all_d)))
    print(f"{'='*50}\nTOTAL: {len(all_d):,} domains\n{'='*50}")

if __name__ == '__main__': main()
