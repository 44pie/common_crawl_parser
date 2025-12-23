#!/usr/bin/env python3
"""
Global E-Commerce Domain Crawler v2
With checkpoint/resume support - never lose progress again!

Usage:
  python crawler_v2.py -t us,ch,de -l 5000 -o domains.csv
  python crawler_v2.py --resume  # Resume from last checkpoint
"""

import sys
import argparse
import csv
import re
import threading
import time
import queue
import atexit
import signal
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

try:
    from checkpoint import CheckpointManager
    CHECKPOINT_AVAILABLE = True
except ImportError:
    CHECKPOINT_AVAILABLE = False
    print("Warning: checkpoint.py not found, running without resume support")


TLD_COUNTRIES = {
    'us': 'United States', 'ch': 'Switzerland', 'co.uk': 'United Kingdom',
    'de': 'Germany', 'fr': 'France', 'es': 'Spain', 'it': 'Italy',
    'nl': 'Netherlands', 'be': 'Belgium', 'at': 'Austria', 'au': 'Australia',
    'ca': 'Canada', 'jp': 'Japan', 'br': 'Brazil', 'mx': 'Mexico',
    'pl': 'Poland', 'ru': 'Russia', 'cn': 'China', 'cl': 'Chile',
    'pe': 'Peru', 'ar': 'Argentina', 'za': 'South Africa', 'uk': 'United Kingdom',
    'store': 'Generic', 'shop': 'Generic', 'online': 'Generic', 'com': 'Global',
}

CMS_PATTERNS = {
    'PrestaShop': ['prestashop', '/modules/ps_', 'id_product=', 'blockcart', '/themes/classic/', 'prestashop-ui-kit'],
    'Magento': ['magento', '/checkout/cart/', 'mage.', '/static/frontend/', '/skin/frontend/', 'mage-'],
    'WooCommerce': ['/wp-content/plugins/woocommerce', 'wc-ajax', 'add-to-cart=', 'woocommerce', 'wc-block'],
    'Shopify': ['.myshopify.com', 'cdn.shopify', '/cart.js', 'shopify'],
    'OpenCart': ['route=product', 'route=checkout', 'opencart', 'catalog/view/theme'],
    'VTEX': ['vtex', '.vteximg.com', '/api/checkout', 'vtexcommercestable'],
    'BigCommerce': ['bigcommerce', 'cdn.bigcommerce.com'],
    'Wix': ['wix.com', 'wixsite.com', '_wix_'],
    'Squarespace': ['squarespace', 'static.squarespace.com'],
}

ECOMMERCE_KEYWORDS = ['cart', 'checkout', 'buy', 'shop', 'store', 'product', 'catalog', 'price', 'order', 'basket', 'purchase', 'payment']
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
        self.last_checkpoint = time.time()
    
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


def print_progress(stats, active_tlds, config, all_tlds=None, checkpoint_mgr=None):
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
        return label + ' ' * max(1, gap) + value
    
    def line(left, right=''):
        left = left[:W].ljust(W)
        right = (right[:W].ljust(W)) if right else BLANK
        print(left + G + right if stats.live_active else left)
    
    print(EQ)
    line('COMMON CRAWL SCANNER v2', 'LIVE CMS CHECKER')
    print(EQ)
    
    line(fmt('URLs Scanned:', f'{stats.total_urls:,}'), fmt('Live Checked:', f'{stats.live_checked:,}'))
    line(fmt('Domains Found:', f'{stats.total_domains:,}'), fmt('CMS Detected:', f'{stats.live_detected:,}'))
    
    det_rate = 100 * stats.live_detected / stats.live_checked if stats.live_checked > 0 else 0
    line(fmt('E-commerce:', f'{stats.ecommerce_count:,}'), fmt('Detection:', f'{det_rate:.1f}%'))
    line(fmt('Speed:', f'{url_rate:.0f}/s'), fmt('Speed:', f'{live_rate:.0f}/s'))
    line(fmt('Queue:', f'{stats.live_queue_size:,}'), fmt('Elapsed:', format_time(elapsed)))
    
    if checkpoint_mgr:
        since_save = time.time() - stats.last_checkpoint
        line(fmt('Checkpoint:', f'{since_save:.0f}s ago'), '')
    
    line(SEP, SEP)
    line('DOMAINS BY TLD:', 'LIVE PLATFORMS:')
    line(SEP, SEP)
    
    if all_tlds:
        tld_list = [(t, stats.domains_by_tld.get(t, 0)) for t in all_tlds]
    else:
        tld_list = list(sorted(stats.domains_by_tld.items(), key=lambda x: -x[1]))[:10]
    
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


def collect_tld(tld, args, stats, active_tlds, exclude_domains, checkpoint_mgr, global_seen):
    country = TLD_COUNTRIES.get(tld, 'Unknown')
    local_seen = {}
    cdx = cdx_toolkit.CDXFetcher(source='cc')
    
    active_tlds.add(tld)
    
    if checkpoint_mgr:
        progress = checkpoint_mgr.get_tld_progress(tld)
        if progress and progress['completed']:
            active_tlds.discard(tld)
            return tld, progress['domains_found']
        existing = checkpoint_mgr.get_domains_for_tld(tld)
        for d in existing:
            local_seen[d] = {'count': 1, 'cms': None, 'ecom': False}
    
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
        
        max_retries = 5
        retry_count = 0
        cdx_iter = None
        
        while retry_count < max_retries:
            try:
                if cdx_iter is None:
                    cdx_iter = cdx.iter(f'*.{tld}/*', **iter_kwargs)
                break
            except Exception as e:
                retry_count += 1
                print(f"\n[!] CDX init error (retry {retry_count}/{max_retries}): {e}")
                time.sleep(5 * retry_count)
                cdx = cdx_toolkit.CDXFetcher(source='cc')
        
        if cdx_iter is None:
            print(f"\n[!] Failed to init CDX for {tld} after {max_retries} retries")
            active_tlds.discard(tld)
            return tld, 0
        
        domains_saved = len(local_seen)
        batch_domains = []
        consecutive_errors = 0
        last_url = ''
        last_timestamp = ''
        
        while True:
            try:
                for obj in cdx_iter:
                    consecutive_errors = 0
                    stats.add_url()
                    
                    if obj.get('page', 0) > args.pages:
                        break
                    
                    url = obj.get('url', '')
                    last_url = url
                    last_timestamp = obj.get('timestamp', '')
                    domain = extract_domain(url)
                    
                    if not domain or not domain.endswith(f'.{tld}'):
                        continue
                    
                    if any(b in domain for b in BAD_PATTERNS):
                        stats.add_skip()
                        continue
                    
                    if domain in exclude_domains:
                        stats.add_skip()
                        continue
                    
                    if domain in global_seen:
                        continue
                    
                    if args.keywords and not matches_keywords(url, args.keywords.split(',')):
                        stats.add_skip()
                        continue
                    
                    ecom = is_ecommerce(url, ECOMMERCE_KEYWORDS)
                    cms = detect_cms(url)
                    
                    if domain in local_seen:
                        local_seen[domain]['count'] += 1
                        if cms and not local_seen[domain]['cms']:
                            local_seen[domain]['cms'] = cms
                        if ecom:
                            local_seen[domain]['ecom'] = True
                        continue
                    
                    local_seen[domain] = {
                        'count': 1,
                        'cms': cms,
                        'ecom': ecom,
                        'timestamp': last_timestamp,
                        'lang': obj.get('languages', '')
                    }
                    global_seen.add(domain)
                    
                    if args.min_urls <= 1:
                        stats.add_domain(tld, cms, ecom)
                        domains_saved += 1
                        
                        if checkpoint_mgr:
                            checkpoint_mgr.save_domain(
                                domain, tld, country, ecom, cms,
                                last_timestamp, local_seen[domain]['lang']
                            )
                            batch_domains.append(domain)
                            
                            if len(batch_domains) >= 500 or checkpoint_mgr.should_save():
                                checkpoint_mgr.save_tld_progress(tld, stats.total_urls, domains_saved, False, last_url, last_timestamp)
                                checkpoint_mgr.save_stats(stats)
                                checkpoint_mgr.commit()
                                stats.last_checkpoint = time.time()
                                batch_domains = []
                    
                    if domains_saved >= args.limit:
                        break
                
                break
                
            except (ConnectionError, TimeoutError, OSError) as e:
                consecutive_errors += 1
                if consecutive_errors >= 5:
                    print(f"\n[!] {tld}: Too many errors, saving checkpoint. Last: {e}")
                    break
                print(f"\n[!] {tld}: Connection error (retry {consecutive_errors}/5): {e}")
                time.sleep(10 * consecutive_errors)
                cdx = cdx_toolkit.CDXFetcher(source='cc')
                cdx_iter = cdx.iter(f'*.{tld}/*', **iter_kwargs)
                
            except StopIteration:
                break
                
            except Exception as e:
                if 'RemoteDisconnected' in str(e) or 'Connection' in str(e):
                    consecutive_errors += 1
                    if consecutive_errors >= 5:
                        print(f"\n[!] {tld}: Too many errors, saving checkpoint.")
                        break
                    print(f"\n[!] {tld}: Reconnecting ({consecutive_errors}/5)...")
                    time.sleep(10 * consecutive_errors)
                    cdx = cdx_toolkit.CDXFetcher(source='cc')
                    cdx_iter = cdx.iter(f'*.{tld}/*', **iter_kwargs)
                else:
                    print(f"\n[!] {tld}: Unexpected error: {e}")
                    break
        
        if checkpoint_mgr:
            checkpoint_mgr.save_tld_progress(tld, stats.total_urls, domains_saved, True, last_url, last_timestamp)
            checkpoint_mgr.save_stats(stats)
            checkpoint_mgr.commit()
            stats.last_checkpoint = time.time()
    
    except Exception as e:
        print(f"\n[!] {tld}: Error: {e}")
        if checkpoint_mgr:
            checkpoint_mgr.save_stats(stats)
            checkpoint_mgr.commit()
    
    if args.min_urls > 1:
        for domain, data in local_seen.items():
            if data['count'] >= args.min_urls:
                stats.add_domain(tld, data['cms'], data['ecom'])
                domains_saved += 1
                
                if checkpoint_mgr:
                    checkpoint_mgr.save_domain(
                        domain, tld, country, data['ecom'], data['cms'],
                        data.get('timestamp', ''), data.get('lang', '')
                    )
                
                if domains_saved >= args.limit:
                    break
        
        if checkpoint_mgr:
            checkpoint_mgr.save_tld_progress(tld, stats.total_urls, domains_saved, True)
            checkpoint_mgr.save_stats(stats)
            checkpoint_mgr.commit()
    
    active_tlds.discard(tld)
    return tld, stats.domains_by_tld.get(tld, 0)


def main():
    parser = argparse.ArgumentParser(
        description='Global E-Commerce Domain Crawler v2 (with checkpoint/resume)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python crawler_v2.py -t us,ch,de -l 5000
  python crawler_v2.py --resume                 # Resume from checkpoint
  python crawler_v2.py -t cl,pe,ar --live-check
  
Resume mode:
  If --resume is used, loads last checkpoint and continues from where it stopped.
  Checkpoints are saved every 500 domains or 60 seconds.
        """
    )
    
    parser.add_argument('-t', '--tld', help='Comma-separated TLDs')
    parser.add_argument('-l', '--limit', type=int, default=100000000, help='Max domains per TLD')
    parser.add_argument('-o', '--output', default='output', help='Output directory')
    parser.add_argument('-w', '--workers', type=int, default=1, help='Parallel workers')
    parser.add_argument('-p', '--pages', type=int, default=80, help='Max CC pages per TLD')
    parser.add_argument('-k', '--keywords', help='Filter by keywords')
    parser.add_argument('-x', '--exclude', help='Exclude list file')
    parser.add_argument('--min', dest='min_urls', type=int, default=1, help='Min URL occurrences')
    parser.add_argument('--from', dest='date_from', help='Start date')
    parser.add_argument('--to', dest='date_to', help='End date')
    parser.add_argument('--mime', default='text/html', help='MIME filter')
    parser.add_argument('--status', help='HTTP status codes')
    parser.add_argument('--resume', action='store_true', help='Resume from checkpoint')
    parser.add_argument('--checkpoint', default='crawler_checkpoint.db', help='Checkpoint file')
    parser.add_argument('--live-check', action='store_true', help='Live domain verification')
    parser.add_argument('--live-threads', type=int, default=50, help='Live check threads')
    parser.add_argument('--live-timeout', type=int, default=10, help='Live check timeout')
    
    args = parser.parse_args()
    
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    checkpoint_path = output_dir / args.checkpoint
    checkpoint_mgr = None
    
    if CHECKPOINT_AVAILABLE:
        checkpoint_mgr = CheckpointManager(str(checkpoint_path))
        
        if args.resume and checkpoint_mgr.has_checkpoint():
            info = checkpoint_mgr.get_resume_info()
            print()
            print('=' * 60)
            print(' RESUMING FROM CHECKPOINT '.center(60))
            print('=' * 60)
            print(f'  Domains saved:    {info["domain_count"]:,}')
            print(f'  Completed TLDs:   {info["completed_tlds"]}')
            if info['pending_tlds']:
                print(f'  Pending TLDs:     {", ".join(info["pending_tlds"])}')
            print()
            
            saved_tlds = checkpoint_mgr.get_metadata('tlds')
            if saved_tlds and not args.tld:
                args.tld = ','.join(saved_tlds)
                print(f'  Using saved TLDs: {args.tld}')
        elif args.resume:
            print("No checkpoint found, starting fresh.")
    
    if not args.tld:
        parser.error('-t/--tld is required (or use --resume)')
    
    tlds = [t.strip() for t in args.tld.split(',')]
    
    if checkpoint_mgr:
        checkpoint_mgr.save_metadata('tlds', tlds)
        checkpoint_mgr.save_metadata('args', {
            'limit': args.limit,
            'pages': args.pages,
            'keywords': args.keywords,
            'min_urls': args.min_urls,
        })
    
    print()
    print('=' * 60)
    print(' GLOBAL E-COMMERCE DOMAIN CRAWLER v2 '.center(60))
    print('=' * 60)
    print()
    print(f'  TLDs:        {", ".join(tlds)}')
    print(f'  Limit:       {args.limit:,} per TLD')
    print(f'  Workers:     {args.workers}')
    print(f'  Checkpoint:  {checkpoint_path}')
    print(f'  Output:      {output_dir}/')
    print()
    print('Starting in 3 seconds... (Ctrl+C to save and exit)')
    time.sleep(3)
    
    exclude_domains = load_exclude_list(args.exclude)
    stats = Stats()
    
    if checkpoint_mgr and args.resume:
        checkpoint_mgr.load_stats(stats)
    
    for t in tlds:
        if t not in stats.domains_by_tld:
            stats.domains_by_tld[t] = 0
    
    active_tlds = set()
    global_seen = set()
    
    if checkpoint_mgr:
        global_seen = checkpoint_mgr.get_all_domains()
    
    live_queue = queue.Queue()
    live_csv_fh = None
    live_writer = None
    crawl_done = threading.Event()
    
    if args.live_check:
        if not DETECTOR_AVAILABLE:
            print('ERROR: detector.py not found.')
            sys.exit(1)
        stats.live_active = True
        live_csv_path = output_dir / 'live_detected.csv'
        live_csv_fh = open(live_csv_path, 'a' if args.resume else 'w', newline='', encoding='utf-8')
        live_writer = csv.DictWriter(live_csv_fh, fieldnames=['domain', 'platform', 'status_code', 'error'])
        if not args.resume:
            live_writer.writeheader()
        live_csv_fh.flush()
    
    lock = threading.Lock()
    
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
    
    def save_and_exit(signum=None, frame=None):
        print("\n\n[!] Saving checkpoint and exiting...")
        if checkpoint_mgr:
            checkpoint_mgr.save_stats(stats)
            checkpoint_mgr.commit()
            checkpoint_mgr.export_to_csv(output_dir / 'crawl_domains.csv')
            checkpoint_mgr.close()
            print(f"[+] Checkpoint saved: {checkpoint_path}")
            print(f"[+] CSV exported: {output_dir / 'crawl_domains.csv'}")
        print(f"[+] Domains saved: {stats.total_domains:,}")
        print("\nUse --resume to continue from this checkpoint.")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, save_and_exit)
    signal.signal(signal.SIGTERM, save_and_exit)
    atexit.register(lambda: checkpoint_mgr.close() if checkpoint_mgr else None)
    
    def progress_thread():
        while active_tlds or stats.total_domains == 0 or (args.live_check and stats.live_queue_size > 0):
            print_progress(stats, active_tlds, {}, tlds, checkpoint_mgr)
            time.sleep(2)
        print_progress(stats, active_tlds, {}, tlds, checkpoint_mgr)
    
    progress = threading.Thread(target=progress_thread, daemon=True)
    progress.start()
    
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = [
            ex.submit(collect_tld, tld, args, stats, active_tlds, exclude_domains, checkpoint_mgr, global_seen)
            for tld in tlds
        ]
        for f in as_completed(futures):
            try:
                f.result()
            except Exception as e:
                print(f"\n[!] Worker error: {e}")
    
    crawl_done.set()
    
    if args.live_check:
        live_queue.join()
        for _ in live_workers:
            live_queue.put(None)
        for t in live_workers:
            t.join(timeout=5)
        if live_csv_fh:
            live_csv_fh.close()
    
    if checkpoint_mgr:
        checkpoint_mgr.save_stats(stats)
        checkpoint_mgr.export_to_csv(output_dir / 'crawl_domains.csv')
        checkpoint_mgr.close()
    
    print()
    print('=' * 60)
    print(' COMPLETED '.center(60))
    print('=' * 60)
    print()
    print(f'  URLs Scanned:   {stats.total_urls:,}')
    print(f'  Domains Found:  {stats.total_domains:,}')
    print(f'  E-commerce:     {stats.ecommerce_count:,}')
    print(f'  Time:           {format_time(time.time() - stats.start_time)}')
    print(f'  Output:         {output_dir}/')
    print()
    
    print('  DOMAINS BY TLD:')
    print('  ' + '-' * 40)
    for tld, count in sorted(stats.domains_by_tld.items(), key=lambda x: -x[1]):
        print(f'    .{tld:12} {count:>8,}')
    print()


if __name__ == '__main__':
    main()
