#!/usr/bin/env python3
import requests, re, sys, time, argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

sys.stdout.reconfigure(line_buffering=True)
UA = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
lock = Lock()
stats = {'pages': 0, 'domains': set()}

def fetch(url):
    try: 
        resp = requests.get(url, headers=UA, timeout=30)
        return resp.text
    except Exception as e:
        print(f"[ERROR] {url}: {e}")
        return ""

def scrape_reviews_io(threads):
    print("[REVIEWS.IO] Start")
    domains = set()
    
    # Get sitemap index
    idx = fetch("https://www.reviews.io/sitemap.xml")
    sitemaps = re.findall(r'<loc>([^<]+)</loc>', idx)
    sitemaps = [s for s in sitemaps if 'sitemap' in s and s.endswith('.xml')]
    
    # Add UK
    sitemaps.append("https://www.reviews.co.uk/sitemap.xml")
    print(f"[REVIEWS.IO] {len(sitemaps)} sitemaps: {sitemaps}")
    
    def get_domains(url):
        content = fetch(url)
        # Extract domains from /company-reviews/store/DOMAIN
        found = re.findall(r'/company-reviews/store/([^\s<>"]+)', content)
        # Clean domains
        clean = set()
        for d in found:
            d = d.strip().lower()
            if '.' in d and len(d) > 3:
                clean.add(d)
        return clean
    
    for sm in sitemaps:
        print(f"[REVIEWS.IO] Fetching {sm}...")
        d = get_domains(sm)
        domains.update(d)
        print(f"[REVIEWS.IO] +{len(d):,} = {len(domains):,} total")
    
    print(f"[REVIEWS.IO] DONE: {len(domains):,}")
    return domains

def scrape_trustedsite(threads):
    global stats
    stats = {'pages': 0, 'domains': set()}
    print("[TRUSTEDSITE] Start")
    sitemap = fetch("https://www.trustedsite.com/sitemap-xml")
    cats = list(set(re.findall(r'/directory/([^/]+)/', sitemap)))
    print(f"[TRUSTEDSITE] {len(cats)} categories")
    
    def scrape_page(args):
        cat, offset = args
        content = fetch(f"https://www.trustedsite.com/directory/{cat}/?s={offset}")
        found = set(re.findall(r'host=([^"&]+)', content))
        with lock:
            stats['pages'] += 1
            stats['domains'].update(found)
            if stats['pages'] % 100 == 0:
                print(f"[TRUSTEDSITE] {stats['pages']} requests | {len(stats['domains']):,} domains")
        return len(found)
    
    tasks = [(c, s) for c in cats for s in range(0, 201, 10)]
    print(f"[TRUSTEDSITE] {len(tasks)} total requests")
    
    with ThreadPoolExecutor(max_workers=threads) as ex:
        list(ex.map(scrape_page, tasks))
    
    print(f"[TRUSTEDSITE] DONE: {len(stats['domains']):,}")
    return stats['domains']

def scrape_feedaty():
    print("[FEEDATY] Start")
    content = fetch("https://www.feedaty.com/feedaty/smurls.xml")
    content += fetch("https://www.feedaty.com/sitemap.xml")
    shops = set(re.findall(r'/recensioni/([a-zA-Z0-9][a-zA-Z0-9\-]+)', content))
    print(f"[FEEDATY] DONE: {len(shops):,}")
    return shops

def main():
    p = argparse.ArgumentParser()
    p.add_argument('-o', '--output', default='domains_out')
    p.add_argument('-t', '--threads', type=int, default=50)
    args = p.parse_args()
    Path(args.output).mkdir(exist_ok=True)
    print(f"{'='*50}\nSCRAPER v11 | {args.threads} threads\n{'='*50}")
    start = time.time()
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
    print(f"{'='*50}\nTOTAL: {len(all_d):,} | Time: {time.time()-start:.0f}s\n{'='*50}")

if __name__ == '__main__': main()
