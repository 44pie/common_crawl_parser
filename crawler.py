#!/usr/bin/env python3
"""
Global E-Commerce Domain Crawler
Collect domains from Common Crawl by TLD with CMS detection

Usage:
  python crawler.py -t us,ch,de -l 5000 -o domains.csv
  python crawler.py -t co.uk --from 2024 --to 2024 --min 3
  python crawler.py -t us -l 1000 --live-check -w 50
"""

import sys
import argparse
import csv
import re
import threading
import time
import queue
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.stdout.reconfigure(line_buffering=True)

try:
    import cdx_toolkit
except ImportError:
    print("Install: pip install cdx_toolkit")
    sys.exit(1)

try:
    from detector import check_domain
    DETECTOR_AVAILABLE = True
except ImportError:
    DETECTOR_AVAILABLE = False


TLD_COUNTRIES = {
    'us': 'United States',
    'ch': 'Switzerland',
    'co.uk': 'United Kingdom',
    'de': 'Germany',
    'fr': 'France',
    'es': 'Spain',
    'it': 'Italy',
    'nl': 'Netherlands',
    'be': 'Belgium',
    'at': 'Austria',
    'au': 'Australia',
    'ca': 'Canada',
    'jp': 'Japan',
    'br': 'Brazil',
    'mx': 'Mexico',
    'pl': 'Poland',
    'ru': 'Russia',
    'cn': 'China',
    'store': 'Generic',
    'shop': 'Generic',
    'online': 'Generic',
    'com': 'Global',
}

CMS_PATTERNS = {
    'PrestaShop': [
        'prestashop', '/modules/ps_', 'id_product=', 'blockcart',
        '/themes/classic/', 'prestashop-ui-kit'
    ],
    'Magento': [
        'magento', '/checkout/cart/', 'mage.', '/static/frontend/',
        '/skin/frontend/', 'mage-'
    ],
    'WooCommerce': [
        '/wp-content/plugins/woocommerce', 'wc-ajax', 'add-to-cart=',
        'woocommerce', 'wc-block'
    ],
    'Shopify': [
        '.myshopify.com', 'cdn.shopify', '/cart.js', 'shopify'
    ],
    'OpenCart': [
        'route=product', 'route=checkout', 'opencart', 'catalog/view/theme'
    ],
    'VTEX': [
        'vtex', '.vteximg.com', '/api/checkout', 'vtexcommercestable'
    ],
    'BigCommerce': [
        'bigcommerce', 'cdn.bigcommerce.com'
    ],
    'Wix': [
        'wix.com', 'wixsite.com', '_wix_'
    ],
    'Squarespace': [
        'squarespace', 'static.squarespace.com'
    ],
}

ECOMMERCE_KEYWORDS = [
    'cart', 'checkout', 'buy', 'shop', 'store', 'product', 
    'catalog', 'price', 'order', 'basket', 'purchase', 'payment'
]

BAD_PATTERNS = ['example.', 'test.', 'localhost']


class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.domains_by_tld = {}
        self.cms_counts = {}
        self.ecommerce_count = 0
        self.total_domains = 0
        self.total_urls = 0
        self.skipped = 0
        self.start_time = time.time()
        
        self.live_checked = 0
        self.live_detected = 0
        self.live_platforms = {}
        self.live_queue_size = 0
        self.live_active = False
    
    def add_domain(self, tld, cms=None, is_ecommerce=False):
        with self.lock:
            self.total_domains += 1
            self.domains_by_tld[tld] = self.domains_by_tld.get(tld, 0) + 1
            if cms:
                self.cms_counts[cms] = self.cms_counts.get(cms, 0) + 1
            if is_ecommerce:
                self.ecommerce_count += 1
    
    def add_url(self):
        with self.lock:
            self.total_urls += 1
    
    def add_skip(self):
        with self.lock:
            self.skipped += 1
    
    def add_live_check(self, platform=None):
        with self.lock:
            self.live_checked += 1
            if platform:
                self.live_detected += 1
                self.live_platforms[platform] = self.live_platforms.get(platform, 0) + 1


def format_time(seconds):
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}m"
    else:
        return f"{seconds/3600:.1f}h"


def print_progress(stats, active_tlds, config, all_tlds=None):
    elapsed = time.time() - stats.start_time
    url_rate = stats.total_urls / elapsed if elapsed > 0 else 0
    live_rate = stats.live_checked / elapsed if elapsed > 0 else 0
    
    sys.stdout.write('\033[H\033[J')
    
    W = 35
    G = '   '
    SEP = '-' * W
    EQ = '=' * (W * 2 + len(G))
    BLANK = ' ' * W
    
    def fmt(label, value, width=W):
        gap = width - len(label) - len(value)
        if gap < 1:
            gap = 1
        return label + ' ' * gap + value
    
    def line(left, right=''):
        left = left[:W].ljust(W)
        right = (right[:W].ljust(W)) if right else BLANK
        print(left + G + right if stats.live_active else left)
    
    print(EQ)
    line('COMMON CRAWL SCANNER', 'LIVE CMS CHECKER')
    print(EQ)
    
    line(fmt('URLs Scanned:', f'{stats.total_urls:,}'), fmt('Live Checked:', f'{stats.live_checked:,}'))
    line(fmt('Domains Found:', f'{stats.total_domains:,}'), fmt('CMS Detected:', f'{stats.live_detected:,}'))
    
    det_rate = 100 * stats.live_detected / stats.live_checked if stats.live_checked > 0 else 0
    line(fmt('E-commerce:', f'{stats.ecommerce_count:,}'), fmt('Detection:', f'{det_rate:.1f}%'))
    line(fmt('Speed:', f'{url_rate:.0f}/s'), fmt('Speed:', f'{live_rate:.0f}/s'))
    line(fmt('Queue:', f'{stats.live_queue_size:,}'), fmt('Elapsed:', format_time(elapsed)))
    
    line(SEP, SEP)
    line('DOMAINS BY TLD:', 'LIVE PLATFORMS:')
    line(SEP, SEP)
    
    if all_tlds:
        tld_list = [(t, stats.domains_by_tld.get(t, 0)) for t in all_tlds]
    else:
        tld_list = list(sorted(stats.domains_by_tld.items(), key=lambda x: -x[1]))[:8]
    
    plat_list = list(sorted(stats.live_platforms.items(), key=lambda x: -x[1])) if stats.live_active else []
    
    max_rows = max(len(tld_list), len(plat_list), 1)
    for i in range(max_rows):
        left = BLANK
        if i < len(tld_list):
            tld, count = tld_list[i]
            status = ' *' if tld in active_tlds else ''
            left = fmt(f'.{tld}', f'{count:,}{status}')
        
        right = BLANK
        if i < len(plat_list):
            p, c = plat_list[i]
            pct = 100 * c / stats.live_detected if stats.live_detected > 0 else 0
            right = fmt(p, f'{c:,} ({pct:5.1f}%)')
        
        line(left, right)
    
    print()
    sys.stdout.flush()


def extract_domain(url):
    m = re.search(r'https?://(?:www\.)?([^/]+)', url)
    return m.group(1).lower() if m else None


def is_ecommerce(url, keywords):
    u = url.lower()
    return any(k in u for k in keywords)


def detect_cms(url):
    u = url.lower()
    for cms, patterns in CMS_PATTERNS.items():
        if any(p in u for p in patterns):
            return cms
    return None


def matches_keywords(url, keywords):
    if not keywords:
        return True
    u = url.lower()
    return any(k.lower() in u for k in keywords)


def load_exclude_list(filepath):
    if not filepath or not Path(filepath).exists():
        return set()
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        return {line.strip().lower() for line in f if line.strip()}


def collect_tld(tld, args, csv_fh, lock, stats, active_tlds, exclude_domains, config):
    country = TLD_COUNTRIES.get(tld, 'Unknown')
    seen = {}
    cdx = cdx_toolkit.CDXFetcher(source='cc')
    
    active_tlds.add(tld)
    
    cdx_filter = []
    
    if args.status:
        statuses = args.status.split(',')
        cdx_filter.append(f"status:({'|'.join(statuses)})")
    else:
        cdx_filter.append('status:200')
    
    if args.mime:
        cdx_filter.append(f'mimetype:{args.mime}')
    
    try:
        iter_kwargs = {
            'limit': args.limit * 150,
            'filter': cdx_filter
        }
        
        if args.date_from:
            iter_kwargs['from_ts'] = args.date_from
        if args.date_to:
            iter_kwargs['to'] = args.date_to
        
        for obj in cdx.iter(f'*.{tld}/*', **iter_kwargs):
            stats.add_url()
            
            if obj.get('page', 0) > args.pages:
                break
            
            url = obj.get('url', '')
            domain = extract_domain(url)
            
            if not domain or not domain.endswith(f'.{tld}'):
                continue
            
            if any(b in domain for b in BAD_PATTERNS):
                stats.add_skip()
                continue
            
            if domain in exclude_domains:
                stats.add_skip()
                continue
            
            if args.keywords and not matches_keywords(url, args.keywords.split(',')):
                stats.add_skip()
                continue
            
            ecom = is_ecommerce(url, ECOMMERCE_KEYWORDS)
            cms = detect_cms(url)
            
            if domain in seen:
                seen[domain]['count'] += 1
                if cms and not seen[domain]['cms']:
                    seen[domain]['cms'] = cms
                if ecom:
                    seen[domain]['ecom'] = True
                continue
            
            seen[domain] = {
                'count': 1,
                'cms': cms,
                'ecom': ecom,
                'timestamp': obj.get('timestamp', ''),
                'lang': obj.get('languages', '')
            }
            
            if args.min_urls <= 1:
                if hasattr(stats, 'add_domain_with_queue'):
                    stats.add_domain_with_queue(tld, cms, ecom, domain)
                else:
                    stats.add_domain(tld, cms, ecom)
                
                with lock:
                    csv_fh.write(
                        f"{domain},{tld},{country},{ecom},{cms or ''},"
                        f"{obj.get('timestamp','')},{seen[domain]['lang']}\n"
                    )
                    csv_fh.flush()
            
            if len(seen) >= args.limit * 2:
                break
    
    except Exception as e:
        pass
    
    if args.min_urls > 1:
        for domain, data in seen.items():
            if data['count'] >= args.min_urls:
                if hasattr(stats, 'add_domain_with_queue'):
                    stats.add_domain_with_queue(tld, data['cms'], data['ecom'], domain)
                else:
                    stats.add_domain(tld, data['cms'], data['ecom'])
                
                with lock:
                    csv_fh.write(
                        f"{domain},{tld},{country},{data['ecom']},{data['cms'] or ''},"
                        f"{data['timestamp']},{data['lang']}\n"
                    )
                    csv_fh.flush()
                
                if stats.domains_by_tld.get(tld, 0) >= args.limit:
                    break
    
    active_tlds.discard(tld)
    return tld, stats.domains_by_tld.get(tld, 0)


def main():
    parser = argparse.ArgumentParser(
        description='Global E-Commerce Domain Crawler',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python crawler.py -t us,ch,de -l 5000 -o domains.csv
  python crawler.py -t co.uk --from 2024 --to 2024 --min 3
  python crawler.py -t store -k cart,checkout,buy
  python crawler.py -t us -x exclude.txt --status 200,301
  python crawler.py -t cl --live-check --live-threads 100

Output CSV columns:
  domain, tld, country, is_ecommerce, cms, timestamp, language
        """
    )
    
    parser.add_argument(
        '-t', '--tld',
        help='Comma-separated TLDs (e.g., us,ch,de,shop,store)'
    )
    
    parser.add_argument(
        '-l', '--limit',
        type=int,
        default=100000000,
        help='Max domains per TLD (default: 100,000,000)'
    )
    
    parser.add_argument(
        '-o', '--output',
        default='output',
        help='Output directory (default: output/)'
    )
    
    parser.add_argument(
        '-w', '--workers',
        type=int,
        default=1,
        help='Parallel workers for TLDs (default: 1)'
    )
    
    parser.add_argument(
        '-p', '--pages',
        type=int,
        default=80,
        help='Max Common Crawl pages per TLD (default: 80)'
    )
    
    parser.add_argument(
        '-k', '--keywords',
        help='Filter by keywords in URL (comma-separated: cart,checkout,buy)'
    )
    
    
    parser.add_argument(
        '-x', '--exclude',
        help='File with domains to exclude (one per line)'
    )
    
    parser.add_argument(
        '--min',
        dest='min_urls',
        type=int,
        default=1,
        help='Min URL occurrences for domain to be saved (default: 1)'
    )
    
    parser.add_argument(
        '--from',
        dest='date_from',
        help='Start date for CC records (YYYY or YYYYMMDD)'
    )
    
    parser.add_argument(
        '--to',
        dest='date_to',
        help='End date for CC records (YYYY or YYYYMMDD)'
    )
    
    parser.add_argument(
        '--mime',
        default='text/html',
        help='MIME type filter (default: text/html)'
    )
    
    parser.add_argument(
        '--status',
        help='HTTP status codes (comma-separated: 200,301,302)'
    )
    
    parser.add_argument(
        '--lang',
        help='Filter by language in Content-Language header'
    )
    
    parser.add_argument(
        '--live-check',
        action='store_true',
        help='After crawling, verify domains with live HTTP requests (uses detector.py)'
    )
    
    parser.add_argument(
        '--live-threads',
        type=int,
        default=50,
        help='Threads for live checking (default: 50)'
    )
    
    parser.add_argument(
        '--live-timeout',
        type=int,
        default=10,
        help='Timeout for live requests in seconds (default: 10)'
    )
    
    args = parser.parse_args()
    
    if not args.tld:
        parser.error('-t/--tld is required')
    
    tlds = [t.strip() for t in args.tld.split(',')]
    
    filters = []
    if args.keywords:
        filters.append(f"Keywords: {args.keywords}")
    if args.exclude:
        filters.append(f"Exclude: {args.exclude}")
    if args.min_urls > 1:
        filters.append(f"Min URLs: {args.min_urls}")
    if args.date_from or args.date_to:
        filters.append(f"Date: {args.date_from or '*'} - {args.date_to or '*'}")
    if args.status:
        filters.append(f"Status: {args.status}")
    if args.lang:
        filters.append(f"Language: {args.lang}")
    
    config = {'filters': filters}
    
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    crawl_csv_path = output_dir / 'crawl_domains.csv'
    live_csv_path = output_dir / 'live_detected.csv'
    
    print()
    print('=' * 60)
    print(' GLOBAL E-COMMERCE DOMAIN CRAWLER '.center(60))
    print('=' * 60)
    print()
    print(f'  TLDs:      {", ".join(tlds)}')
    print(f'  Limit:     {args.limit:,} per TLD')
    print(f'  Threads:   {args.workers}')
    print(f'  Pages:     {args.pages}')
    print(f'  Output:    {output_dir}/')
    print(f'             - crawl_domains.csv')
    if args.live_check:
        print(f'             - live_detected.csv')
    
    if filters:
        print()
        print('  Filters:')
        for f in filters:
            print(f'    - {f}')
    
    print()
    print('Starting in 3 seconds...')
    time.sleep(3)
    
    exclude_domains = load_exclude_list(args.exclude)
    if exclude_domains:
        print(f'Loaded {len(exclude_domains):,} domains to exclude')
    
    csv_fh = open(crawl_csv_path, 'w', encoding='utf-8')
    csv_fh.write('domain,tld,country,is_ecommerce,cms,timestamp,language\n')
    csv_fh.flush()
    
    lock = threading.Lock()
    stats = Stats()
    for t in tlds:
        stats.domains_by_tld[t] = 0
    active_tlds = set()
    
    live_queue = queue.Queue()
    live_csv_fh = None
    live_writer = None
    crawl_done = threading.Event()
    
    if args.live_check:
        if not DETECTOR_AVAILABLE:
            print('ERROR: detector.py not found. --live-check requires detector module.')
            sys.exit(1)
        stats.live_active = True
        live_csv_fh = open(live_csv_path, 'w', newline='', encoding='utf-8')
        live_writer = csv.DictWriter(live_csv_fh, fieldnames=['domain', 'platform', 'status_code', 'error'])
        live_writer.writeheader()
        live_csv_fh.flush()
    
    def live_worker():
        while True:
            try:
                domain = live_queue.get(timeout=1)
                if domain is None:
                    break
                
                try:
                    result = check_domain(domain, args.live_timeout)
                    platform = result.get('platform', '')
                    stats.add_live_check(platform if platform else None)
                    
                    if platform and live_writer:
                        with lock:
                            live_writer.writerow(result)
                            live_csv_fh.flush()
                except Exception:
                    stats.add_live_check(None)
                
                with stats.lock:
                    stats.live_queue_size = live_queue.qsize()
                
                live_queue.task_done()
            except queue.Empty:
                if crawl_done.is_set() and live_queue.empty():
                    break
    
    live_workers = []
    if args.live_check:
        for _ in range(args.live_threads):
            t = threading.Thread(target=live_worker, daemon=True)
            t.start()
            live_workers.append(t)
    
    original_add_domain = stats.add_domain
    def add_domain_with_queue(tld, cms=None, is_ecommerce=False, domain=None):
        original_add_domain(tld, cms, is_ecommerce)
        if args.live_check and domain:
            live_queue.put(domain)
            with stats.lock:
                stats.live_queue_size = live_queue.qsize()
    stats.add_domain_with_queue = add_domain_with_queue
    
    def progress_thread():
        while active_tlds or stats.total_domains == 0 or (args.live_check and stats.live_queue_size > 0):
            print_progress(stats, active_tlds, config, tlds)
            time.sleep(2)
        print_progress(stats, active_tlds, config, tlds)
    
    progress = threading.Thread(target=progress_thread, daemon=True)
    progress.start()
    
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = [
            ex.submit(
                collect_tld, tld, args, csv_fh, lock, stats, 
                active_tlds, exclude_domains, config
            )
            for tld in tlds
        ]
        for f in as_completed(futures):
            f.result()
    
    crawl_done.set()
    
    if args.live_check:
        live_queue.join()
        for _ in live_workers:
            live_queue.put(None)
        for t in live_workers:
            t.join(timeout=5)
        if live_csv_fh:
            live_csv_fh.close()
    
    time.sleep(1)
    csv_fh.close()
    
    print()
    print('=' * 60)
    print(' COMPLETED '.center(60))
    print('=' * 60)
    print()
    print(f'  URLs Scanned:   {stats.total_urls:,}')
    print(f'  Domains Found:  {stats.total_domains:,}')
    print(f'  E-commerce:     {stats.ecommerce_count:,}')
    print(f'  Skipped:        {stats.skipped:,}')
    print(f'  Time:           {format_time(time.time() - stats.start_time)}')
    print(f'  Output:         {output_dir}/')
    print()
    
    print('  DOMAINS BY TLD:')
    print('  ' + '-' * 40)
    for tld, count in sorted(stats.domains_by_tld.items(), key=lambda x: -x[1]):
        print(f'    .{tld:12} {count:>8,}')
    print()
    
    if stats.cms_counts:
        print('  CMS DETECTED (from URL patterns):')
        print('  ' + '-' * 40)
        for cms, count in sorted(stats.cms_counts.items(), key=lambda x: -x[1]):
            pct = 100 * count / stats.total_domains if stats.total_domains > 0 else 0
            print(f'    {cms:15} {count:>8,} ({pct:.1f}%)')
    print()
    
    if args.live_check and stats.live_checked > 0:
        print('  LIVE CHECK RESULTS:')
        print('  ' + '-' * 40)
        print(f'    Checked:       {stats.live_checked:>8,}')
        print(f'    Detected:      {stats.live_detected:>8,}')
        det_rate = 100 * stats.live_detected / stats.live_checked if stats.live_checked > 0 else 0
        print(f'    Detection:     {det_rate:>7.1f}%')
        print(f'    Output:        {live_csv_path}')
        print()
        
        if stats.live_platforms:
            print('  LIVE PLATFORMS:')
            print('  ' + '-' * 40)
            for p, c in sorted(stats.live_platforms.items(), key=lambda x: -x[1]):
                pp = 100 * c / stats.live_detected if stats.live_detected > 0 else 0
                bar_len = int(25 * c / max(stats.live_platforms.values())) if stats.live_platforms else 0
                bar = '▓' * bar_len
                print(f'    {p:18} {c:>6,} ({pp:4.1f}%) {bar}')
        print()


if __name__ == '__main__':
    main()
