#!/usr/bin/env python3
"""
Disk-based CDX Crawler - low memory usage with live CMS check
Downloads chunks to disk, processes, deletes
"""

import sys
import gzip
import json
import re
import threading
import time
import requests
import os
import csv
import queue
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from checkpoint import CheckpointManager
except ImportError:
    CheckpointManager = None

try:
    from detector import check_domain
    DETECTOR_AVAILABLE = True
except ImportError:
    DETECTOR_AVAILABLE = False

CC_INDEX = "CC-MAIN-2024-51"
CC_BASE = f"https://data.commoncrawl.org/cc-index/collections/{CC_INDEX}/indexes"

TLD_COUNTRIES = {
    'cl': 'Chile', 'pe': 'Peru', 'ar': 'Argentina', 'au': 'Australia',
    'us': 'United States', 'ca': 'Canada', 'uk': 'United Kingdom',
    'za': 'South Africa', 'jp': 'Japan', 'de': 'Germany',
}

ECOMMERCE_KW = ['cart', 'checkout', 'shop', 'store', 'product', 'buy', 'order']
BAD_PATTERNS = ['example.', 'test.', 'localhost', '.gov.', '.edu.']


class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.domains_by_tld = {}
        self.total_domains = 0
        self.total_urls = 0  # alias for lines, for checkpoint compatibility
        self.ecommerce_count = 0
        self.skipped = 0
        self.cms_counts = {}
        self.start_time = time.time()
        self.chunks_done = 0
        self.chunks_total = 0
        self.live_checked = 0
        self.live_detected = 0
        self.live_platforms = {}
        self.live_queue_size = 0
        self.live_active = False
    
    @property
    def total_lines(self):
        return self.total_urls
    
    @property
    def ecommerce(self):
        return self.ecommerce_count
    
    @property
    def start(self):
        return self.start_time
    
    def add(self, tld, ecom=False):
        with self.lock:
            self.total_domains += 1
            self.domains_by_tld[tld] = self.domains_by_tld.get(tld, 0) + 1
            if ecom:
                self.ecommerce_count += 1
    
    def add_lines(self, n):
        with self.lock:
            self.total_urls += n
    
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


def find_chunks_for_tlds(tlds, cache_path=None):
    print("Downloading cluster.idx to find TLD locations...")
    
    if cache_path and Path(cache_path).exists():
        print(f"  Using cached {cache_path}")
        with open(cache_path, 'r') as f:
            lines = f.readlines()
    else:
        resp = requests.get(f"{CC_BASE}/cluster.idx", timeout=120)
        resp.raise_for_status()
        lines = resp.text.strip().split('\n')
        if cache_path:
            with open(cache_path, 'w') as f:
                f.write(resp.text)
            print(f"  Cached to {cache_path}")
    
    print(f"  Loaded {len(lines):,} cluster entries")
    
    tld_chunks = defaultdict(set)
    chunk_pattern = re.compile(r'cdx-(\d+)\.gz')
    
    for line in lines:
        for tld in tlds:
            if line.startswith(f"{tld},"):
                match = chunk_pattern.search(line)
                if match:
                    chunk_id = int(match.group(1))
                    tld_chunks[tld].add(chunk_id)
                break
    
    all_chunks = set()
    for tld in tlds:
        chunks = sorted(tld_chunks[tld])
        if chunks:
            print(f"  .{tld}: chunks {min(chunks)}-{max(chunks)} ({len(chunks)} files)")
            all_chunks.update(chunks)
        else:
            print(f"  .{tld}: no chunks found")
    
    return sorted(all_chunks), dict(tld_chunks)


def extract_domain_from_url(url):
    m = re.search(r'https?://(?:www\.)?([^/:]+)', url)
    return m.group(1).lower() if m else None


def download_to_disk(url, dest_path):
    resp = requests.get(url, stream=True, timeout=300)
    resp.raise_for_status()
    
    with open(dest_path, 'wb') as f:
        for chunk in resp.iter_content(chunk_size=1024*1024):
            if chunk:
                f.write(chunk)


def process_chunk_from_disk(chunk_path, tld_prefixes, stats, seen, checkpoint, limit, lock, live_queue):
    counts = {tld: 0 for tld in tld_prefixes.values()}
    batch = []
    lines_count = 0
    
    with gzip.open(chunk_path, 'rt', encoding='utf-8', errors='ignore') as f:
        for line in f:
            lines_count += 1
            try:
                line_str = line.strip()
                
                matched_tld = None
                for prefix, tld in tld_prefixes.items():
                    if line_str.startswith(prefix):
                        matched_tld = tld
                        break
                
                if not matched_tld:
                    continue
                
                if stats.domains_by_tld.get(matched_tld, 0) >= limit:
                    continue
                
                parts = line_str.split(' ', 2)
                if len(parts) < 3:
                    continue
                
                surt, timestamp, json_str = parts
                data = json.loads(json_str)
                
                if data.get('status') != '200':
                    continue
                
                if 'html' not in data.get('mime', ''):
                    continue
                
                page_url = data.get('url', '')
                domain = extract_domain_from_url(page_url)
                
                if not domain:
                    continue
                
                if any(b in domain for b in BAD_PATTERNS):
                    continue
                
                with lock:
                    if domain in seen:
                        continue
                    seen.add(domain)
                
                ecom = any(k in page_url.lower() for k in ECOMMERCE_KW)
                stats.add(matched_tld, ecom)
                counts[matched_tld] += 1
                
                if live_queue is not None:
                    live_queue.put(domain)
                    with stats.lock:
                        stats.live_queue_size = live_queue.qsize()
                
                if checkpoint:
                    batch.append((
                        domain, matched_tld, 
                        TLD_COUNTRIES.get(matched_tld, 'Unknown'),
                        ecom, '', timestamp, data.get('languages', '')
                    ))
                    
                    if len(batch) >= 200:
                        for item in batch:
                            checkpoint.save_domain(*item)
                        checkpoint.commit()
                        batch = []
                    
            except Exception:
                continue
    
    stats.add_lines(lines_count)
    
    if checkpoint and batch:
        for item in batch:
            checkpoint.save_domain(*item)
        checkpoint.commit()
    
    return counts


def process_chunk(chunk_id, tld_chunks, stats, seen, checkpoint, limit, lock, temp_dir, live_queue):
    url = f"{CC_BASE}/cdx-{chunk_id:05d}.gz"
    
    tlds_in_chunk = [tld for tld, chunks in tld_chunks.items() if chunk_id in chunks]
    if not tlds_in_chunk:
        with stats.lock:
            stats.chunks_done += 1
        return {}
    
    tld_prefixes = {f"{tld},": tld for tld in tlds_in_chunk}
    chunk_path = Path(temp_dir) / f"cdx-{chunk_id:05d}.gz"
    
    try:
        download_to_disk(url, chunk_path)
        counts = process_chunk_from_disk(chunk_path, tld_prefixes, stats, seen, checkpoint, limit, lock, live_queue)
    finally:
        if chunk_path.exists():
            try:
                os.remove(chunk_path)
            except Exception as e:
                print(f"\n[!] Failed to delete {chunk_path}: {e}")
    
    with stats.lock:
        stats.chunks_done += 1
    
    return counts


def print_status(stats, temp_dir, tlds):
    elapsed = time.time() - stats.start
    rate = stats.total_domains / elapsed if elapsed > 0 else 0
    line_rate = stats.total_lines / elapsed if elapsed > 0 else 0
    live_rate = stats.live_checked / elapsed if elapsed > 0 else 0
    
    disk_mb = sum(f.stat().st_size for f in Path(temp_dir).glob('*.gz') if f.exists()) / 1024 / 1024
    
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
    line('DISK-BASED CDX CRAWLER', 'LIVE CMS CHECKER')
    print(EQ)
    
    line(fmt('Lines Scanned:', f'{stats.total_lines:,}'), fmt('Live Checked:', f'{stats.live_checked:,}'))
    line(fmt('Domains Found:', f'{stats.total_domains:,}'), fmt('CMS Detected:', f'{stats.live_detected:,}'))
    
    det_rate = 100 * stats.live_detected / stats.live_checked if stats.live_checked > 0 else 0
    line(fmt('E-commerce:', f'{stats.ecommerce:,}'), fmt('Detection:', f'{det_rate:.1f}%'))
    line(fmt('Speed:', f'{line_rate:,.0f} lines/s'), fmt('Speed:', f'{live_rate:.0f}/s'))
    line(fmt('Chunks:', f'{stats.chunks_done}/{stats.chunks_total}'), fmt('Queue:', f'{stats.live_queue_size:,}'))
    line(fmt('Disk Cache:', f'{disk_mb:.1f} MB'), fmt('Elapsed:', format_time(elapsed)))
    
    line(SEP, SEP)
    line('DOMAINS BY TLD:', 'LIVE PLATFORMS:')
    line(SEP, SEP)
    
    tld_list = [(t, stats.domains_by_tld.get(t, 0)) for t in tlds]
    plat_list = list(sorted(stats.live_platforms.items(), key=lambda x: -x[1])) if stats.live_active else []
    
    max_rows = max(len(tld_list), len(plat_list), 1)
    for i in range(max_rows):
        left = BLANK
        if i < len(tld_list):
            tld, cnt = tld_list[i]
            status = ' *' if cnt > 0 and stats.chunks_done < stats.chunks_total else ''
            left = fmt(f'.{tld}', f'{cnt:,}{status}')
        
        right = BLANK
        if i < len(plat_list):
            p, c = plat_list[i]
            pct = 100 * c / stats.live_detected if stats.live_detected > 0 else 0
            right = fmt(p, f'{c:,} ({pct:5.1f}%)')
        
        line(left, right)
    
    print()
    sys.stdout.flush()


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Disk-based CDX Crawler with Live CMS Check')
    parser.add_argument('-t', '--tld', required=True, help='TLDs comma-separated')
    parser.add_argument('-l', '--limit', type=int, default=100000, help='Limit per TLD')
    parser.add_argument('-w', '--workers', type=int, default=3, help='Parallel downloads')
    parser.add_argument('-o', '--output', default='output', help='Output dir')
    parser.add_argument('-c', '--cache-size', type=int, default=5, help='Max chunks cached on disk')
    parser.add_argument('--live-check', action='store_true', help='Enable live CMS detection')
    parser.add_argument('--live-threads', type=int, default=50, help='Threads for live checking')
    parser.add_argument('--live-timeout', type=int, default=10, help='Timeout for live requests')
    parser.add_argument('--resume', action='store_true', help='Resume from checkpoint')
    parser.add_argument('--list-chunks', action='store_true', help='Just list chunks')
    args = parser.parse_args()
    
    tlds = [t.strip() for t in args.tld.split(',')]
    output_dir = Path(args.output)
    output_dir.mkdir(exist_ok=True)
    
    temp_dir = output_dir / 'chunk_cache'
    temp_dir.mkdir(exist_ok=True)
    
    for old_chunk in temp_dir.glob('*.gz'):
        old_chunk.unlink()
    
    cluster_cache = output_dir / 'cluster.idx.cache'
    chunks, tld_chunks = find_chunks_for_tlds(tlds, str(cluster_cache))
    
    if not chunks:
        print("\nNo chunks found for specified TLDs!")
        return
    
    print(f"\nTotal: {len(chunks)} chunks to scan")
    
    if args.list_chunks:
        print(f"Chunks: {chunks}")
        return
    
    checkpoint = None
    if CheckpointManager:
        checkpoint = CheckpointManager(str(output_dir / 'crawler_checkpoint.db'))
    
    stats = Stats()
    stats.chunks_total = len(chunks)
    for t in tlds:
        stats.domains_by_tld[t] = 0
    
    seen = set()
    lock = threading.Lock()
    semaphore = threading.Semaphore(args.cache_size)
    
    if checkpoint:
        seen = checkpoint.get_all_domains()
        if seen:
            print(f"Loaded {len(seen):,} existing domains from checkpoint")
            checkpoint.load_stats(stats)
    
    live_queue = None
    live_csv_fh = None
    live_writer = None
    live_workers = []
    crawl_done = threading.Event()
    
    if args.live_check:
        if not DETECTOR_AVAILABLE:
            print("ERROR: detector.py not found. --live-check requires detector module.")
            sys.exit(1)
        
        stats.live_active = True
        live_queue = queue.Queue()
        
        live_csv_path = output_dir / 'live_detected.csv'
        live_csv_fh = open(live_csv_path, 'a' if args.resume else 'w', newline='', encoding='utf-8')
        live_writer = csv.DictWriter(live_csv_fh, fieldnames=['domain', 'platform', 'status_code', 'error'])
        if not args.resume:
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
        
        for _ in range(args.live_threads):
            t = threading.Thread(target=live_worker, daemon=True)
            t.start()
            live_workers.append(t)
    
    print(f"\nLimit: {args.limit:,} per TLD")
    if args.live_check:
        print(f"Live check: {args.live_threads} threads, {args.live_timeout}s timeout")
    print("Starting in 3 seconds...\n")
    time.sleep(3)
    
    running = [True]
    
    def status_thread():
        while running[0]:
            print_status(stats, temp_dir, tlds)
            time.sleep(2)
    
    threading.Thread(target=status_thread, daemon=True).start()
    
    def worker(chunk_id):
        with semaphore:
            return process_chunk(chunk_id, tld_chunks, stats, seen, checkpoint, args.limit, lock, temp_dir, live_queue)
    
    try:
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futures = [ex.submit(worker, cid) for cid in chunks]
            
            for f in as_completed(futures):
                try:
                    f.result()
                except Exception as e:
                    print(f"\n[!] Error: {e}")
                    
    except KeyboardInterrupt:
        print("\n\n[!] Interrupted - saving checkpoint...")
    
    crawl_done.set()
    
    if args.live_check:
        live_queue.join()
        for _ in live_workers:
            live_queue.put(None)
        for t in live_workers:
            t.join(timeout=5)
        if live_csv_fh:
            live_csv_fh.close()
    
    running[0] = False
    time.sleep(0.5)
    
    for old_chunk in temp_dir.glob('*.gz'):
        old_chunk.unlink()
    
    if checkpoint:
        checkpoint.save_stats(stats)
        checkpoint.export_to_csv(output_dir / 'crawl_domains.csv')
        checkpoint.close()
    
    print_status(stats, temp_dir, tlds)
    print("=" * 73)
    print(" DONE ".center(73))
    print("=" * 73)
    print(f"  Total domains: {stats.total_domains:,}")
    if args.live_check:
        print(f"  CMS detected:  {stats.live_detected:,}")
    print(f"  Output: {output_dir}/")


if __name__ == '__main__':
    main()
