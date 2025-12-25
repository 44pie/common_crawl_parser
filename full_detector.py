#!/usr/bin/env python3
"""Fast detector for PrestaShop, OpenCart, Magento - NO HANG version"""
import requests, sys, time, argparse, re, signal, json
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from pathlib import Path

sys.stdout.reconfigure(line_buffering=True)

UA = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
lock = Lock()
stats = {'checked': 0, 'found': 0, 'errors': 0, 'start': 0}
results = {'prestashop': [], 'opencart': [], 'magento': []}
running = True
checkpoint_file = None

SIGNATURES = {
    'prestashop': [
        r'/modules/ps_', r'/themes/classic/', r'prestashop\.js', r'PrestaShop',
        r'/modules/blockcart/', r'var prestashop', r'prestashop-page-cache',
    ],
    'opencart': [
        r'catalog/view/theme', r'route=common/', r'route=product/',
        r'OpenCart', r'catalog/view/javascript', r'/index\.php\?route=',
    ],
    'magento': [
        r'Mage\.Cookies', r'/static/version', r'mage/cookies', r'Magento_',
        r'/pub/static/', r'checkout/cart', r'requirejs/require', r'magento\.js',
    ],
}

def save_checkpoint():
    global results, stats, checkpoint_file
    with lock:
        data = {'stats': stats, 'results': results}
        Path(checkpoint_file).write_text(json.dumps(data))
        for plat, doms in results.items():
            if doms:
                Path(f"{output_prefix}_{plat}.txt").write_text('\n'.join(sorted(set(doms))))
        all_f = results['prestashop'] + results['opencart'] + results['magento']
        if all_f:
            Path(f"{output_prefix}_all.txt").write_text('\n'.join(sorted(set(all_f))))

def signal_handler(sig, frame):
    global running
    print("\n[!] CTRL+C - Saving checkpoint...")
    running = False
    save_checkpoint()
    print(f"[!] Saved to {checkpoint_file}")
    sys.exit(0)

def detect(domain):
    global stats, results, running
    if not running:
        return None
    
    url = f"https://{domain}" if not domain.startswith('http') else domain
    try:
        resp = requests.get(url, headers=UA, timeout=5, allow_redirects=True, verify=False)
        html = resp.text[:50000]
        
        detected = None
        for platform, patterns in SIGNATURES.items():
            for p in patterns:
                if re.search(p, html, re.I):
                    detected = platform
                    break
            if detected:
                break
        
        with lock:
            stats['checked'] += 1
            if detected:
                stats['found'] += 1
                results[detected].append(domain)
            
            if stats['checked'] % 500 == 0:
                elapsed = time.time() - stats['start']
                speed = stats['checked'] / elapsed if elapsed > 0 else 0
                eta = (total_domains - stats['checked']) / speed if speed > 0 else 0
                print(f"[{stats['checked']:,}/{total_domains:,}] Found:{stats['found']:,} Err:{stats['errors']:,} | {speed:.0f}/s | ETA:{eta/60:.0f}m | PS:{len(results['prestashop'])} OC:{len(results['opencart'])} MG:{len(results['magento'])}")
            
            if stats['checked'] % 5000 == 0:
                save_checkpoint()
        
        return detected
    except:
        with lock:
            stats['checked'] += 1
            stats['errors'] += 1
        return None

output_prefix = "detected"
total_domains = 0

def main():
    global checkpoint_file, output_prefix, total_domains, stats, results
    import urllib3
    urllib3.disable_warnings()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    p = argparse.ArgumentParser()
    p.add_argument('input', help='Input file with domains')
    p.add_argument('-o', '--output', default='detected', help='Output prefix')
    p.add_argument('-t', '--threads', type=int, default=100)
    p.add_argument('-r', '--resume', action='store_true', help='Resume from checkpoint')
    args = p.parse_args()
    
    output_prefix = args.output
    checkpoint_file = f"{args.output}_checkpoint.json"
    
    domains = [l.strip() for l in open(args.input) if l.strip() and '.' in l]
    total_domains = len(domains)
    
    # Resume from checkpoint
    skip = 0
    if args.resume and Path(checkpoint_file).exists():
        data = json.loads(Path(checkpoint_file).read_text())
        stats = data['stats']
        results = data['results']
        skip = stats['checked']
        print(f"[RESUME] Skipping {skip:,} already checked")
        domains = domains[skip:]
    
    print(f"{'='*70}")
    print(f"FULL DETECTOR | {len(domains):,} domains | {args.threads} threads | Timeout: 5s")
    print(f"CTRL+C to save checkpoint and exit")
    print(f"{'='*70}")
    
    stats['start'] = time.time()
    
    with ThreadPoolExecutor(max_workers=args.threads) as ex:
        futures = [ex.submit(detect, d) for d in domains]
        for f in as_completed(futures):
            if not running:
                break
    
    save_checkpoint()
    
    elapsed = time.time() - stats['start']
    print(f"\n{'='*70}")
    print(f"DONE in {elapsed:.0f}s ({elapsed/60:.1f}m)")
    print(f"Checked: {stats['checked']:,} | Found: {stats['found']:,} | Errors: {stats['errors']:,}")
    print(f"PrestaShop: {len(results['prestashop']):,}")
    print(f"OpenCart: {len(results['opencart']):,}")
    print(f"Magento: {len(results['magento']):,}")
    print(f"{'='*70}")

if __name__ == '__main__':
    main()
