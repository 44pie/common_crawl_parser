#!/usr/bin/env python3
"""Fast detector for PrestaShop, OpenCart, Magento only"""
import requests, sys, time, argparse, re
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from pathlib import Path

sys.stdout.reconfigure(line_buffering=True)

UA = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
lock = Lock()
stats = {'checked': 0, 'found': 0, 'errors': 0, 'start': 0}
results = {'prestashop': [], 'opencart': [], 'magento': []}

SIGNATURES = {
    'prestashop': [
        r'/modules/ps_',
        r'/themes/classic/',
        r'prestashop\.js',
        r'PrestaShop',
        r'/modules/blockcart/',
        r'var prestashop',
        r'prestashop-page-cache',
    ],
    'opencart': [
        r'catalog/view/theme',
        r'route=common/',
        r'route=product/',
        r'OpenCart',
        r'catalog/view/javascript',
        r'/index\.php\?route=',
    ],
    'magento': [
        r'Mage\.Cookies',
        r'/static/version',
        r'mage/cookies',
        r'Magento_',
        r'/pub/static/',
        r'checkout/cart',
        r'requirejs/require',
        r'magento\.js',
        r'PHPSESSID.*frontend',
    ],
}

def detect(domain):
    global stats, results
    url = f"https://{domain}" if not domain.startswith('http') else domain
    try:
        resp = requests.get(url, headers=UA, timeout=8, allow_redirects=True, verify=False)
        html = resp.text[:100000]  # First 100KB only
        
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
                print(f"[{stats['checked']:,}] Found: {stats['found']:,} | Speed: {speed:.1f}/s | PS:{len(results['prestashop'])} OC:{len(results['opencart'])} MG:{len(results['magento'])}")
        
        return detected
    except Exception as e:
        with lock:
            stats['checked'] += 1
            stats['errors'] += 1
        return None

def main():
    import urllib3
    urllib3.disable_warnings()
    
    p = argparse.ArgumentParser()
    p.add_argument('input', help='Input file with domains')
    p.add_argument('-o', '--output', default='detected', help='Output prefix')
    p.add_argument('-t', '--threads', type=int, default=100)
    p.add_argument('-l', '--limit', type=int, default=0, help='Limit domains (0=all)')
    args = p.parse_args()
    
    domains = [l.strip() for l in open(args.input) if l.strip() and '.' in l]
    if args.limit > 0:
        domains = domains[:args.limit]
    
    print(f"{'='*60}")
    print(f"FAST DETECTOR | {len(domains):,} domains | {args.threads} threads")
    print(f"{'='*60}")
    
    stats['start'] = time.time()
    
    with ThreadPoolExecutor(max_workers=args.threads) as ex:
        list(ex.map(detect, domains))
    
    elapsed = time.time() - stats['start']
    
    # Save results
    for plat, doms in results.items():
        if doms:
            Path(f"{args.output}_{plat}.txt").write_text('\n'.join(sorted(doms)))
    
    all_found = results['prestashop'] + results['opencart'] + results['magento']
    Path(f"{args.output}_all.txt").write_text('\n'.join(sorted(all_found)))
    
    print(f"\n{'='*60}")
    print(f"DONE in {elapsed:.0f}s ({elapsed/60:.1f}m)")
    print(f"Checked: {stats['checked']:,} | Found: {stats['found']:,} | Errors: {stats['errors']:,}")
    print(f"PrestaShop: {len(results['prestashop']):,}")
    print(f"OpenCart: {len(results['opencart']):,}")
    print(f"Magento: {len(results['magento']):,}")
    print(f"Files: {args.output}_*.txt")
    print(f"{'='*60}")

if __name__ == '__main__':
    main()
