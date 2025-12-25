"""
E-commerce Platform Detection Module - FIXED VERSION
"""
import sys, argparse, re, requests, time, signal, json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import urllib3
urllib3.disable_warnings()

REQUEST_TIMEOUT = 10
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36', 'Accept': 'text/html'}

lock = Lock()
running = True
stats = {'checked': 0, 'detected': 0, 'errors': 0, 'total': 0, 'platforms': {}}
results_list = []
checkpoint_file = None
output_file = None

def fetch_url(url, timeout=REQUEST_TIMEOUT):
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    try:
        return requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True, verify=False), ''
    except requests.exceptions.Timeout:
        return None, 'timeout'
    except requests.exceptions.SSLError:
        return None, 'ssl_error'
    except requests.exceptions.ConnectionError:
        return None, 'connection_error'
    except:
        return None, 'error'

def detect_from_headers(headers):
    h = {k.lower(): v.lower() for k, v in headers.items()}
    if 'x-shopify-stage' in h or 'x-shopid' in h or h.get('server','').startswith('shopify'):
        return 'Shopify'
    if 'x-bc-' in str(h): return 'BigCommerce'
    if 'x-magento-' in str(h): return 'Magento'
    if 'x-dw-request-base-id' in h: return 'Demandware'
    if 'x-wix-request-id' in h: return 'Wix'
    return ''

def detect_from_cookies(cookies):
    c = str(cookies).lower()
    if 'woocommerce_' in c: return 'WooCommerce'
    if '_shopify_' in c: return 'Shopify'
    if 'mage-' in c: return 'Magento'
    if 'prestashop' in c: return 'PrestaShop'
    if 'phpsessid' in c and 'currency' in c: return 'OpenCart'
    if 'bitrix_' in c: return 'Bitrix'
    return ''

PLATFORM_CHECKS = [
    (lambda t: 'shopify' in t or 'cdn.shopify.com' in t or 'myshopify.com' in t, 'Shopify'),
    (lambda t: 'woocommerce' in t or 'wc-block' in t or '/wp-content/plugins/woocommerce/' in t, 'WooCommerce'),
    (lambda t: 'magento' in t or '/skin/frontend/' in t or '/static/frontend/' in t or 'mage.cookies' in t, 'Magento'),
    (lambda t: 'bigcommerce' in t or 'cdn.bigcommerce.com' in t, 'BigCommerce'),
    (lambda t: 'prestashop' in t or '/modules/ps_' in t or '/themes/classic/assets/' in t or 'blockcart' in t, 'PrestaShop'),
    (lambda t: 'wix.com' in t or 'wixsite.com' in t or '_wix_' in t, 'Wix'),
    (lambda t: 'squarespace' in t or 'static.squarespace.com' in t, 'Squarespace'),
    (lambda t: 'bigcartel' in t, 'BigCartel'),
    (lambda t: 'opencart' in t or 'index.php?route=' in t or 'catalog/view/theme' in t, 'OpenCart'),
    (lambda t: '3dcart' in t or 'shift4shop' in t, '3DCart'),
    (lambda t: 'volusion' in t, 'Volusion'),
    (lambda t: 'demandware' in t or 'dwvar_' in t or 'dwfrm_' in t, 'Demandware'),
    (lambda t: 'sellfy' in t, 'Sellfy'),
    (lambda t: 'ecwid' in t, 'Ecwid'),
    (lambda t: 'weebly' in t or 'editmysite.com' in t, 'Weebly'),
    (lambda t: 'salesforce' in t and 'commerce' in t, 'SalesforceCommerce'),
    (lambda t: 'vtex' in t, 'VTEX'),
    (lambda t: 'shopware' in t, 'Shopware'),
    (lambda t: 'nopcommerce' in t, 'nopCommerce'),
    (lambda t: 'lightspeed' in t or 'seoshop' in t, 'Lightspeed'),
    (lambda t: 'tilda' in t or 'tildacdn' in t, 'Tilda'),
    (lambda t: 'bitrix' in t or '1c-bitrix' in t, 'Bitrix'),
    (lambda t: 'insales' in t, 'InSales'),
    (lambda t: 'cs-cart' in t or 'cscart' in t, 'CS-Cart'),
]

def detect_platform(text):
    t = text.lower()
    for check, name in PLATFORM_CHECKS:
        try:
            if check(t): return name
        except: pass
    return ''

def check_domain(domain, timeout=REQUEST_TIMEOUT):
    resp, err = fetch_url(domain, timeout)
    if resp is None:
        return {'domain': domain, 'platform': '', 'status_code': 0, 'error': err}
    platform = detect_from_headers(dict(resp.headers))
    if not platform: platform = detect_from_cookies(resp.cookies)
    if not platform: platform = detect_platform(resp.text)
    return {'domain': domain, 'platform': platform, 'status_code': resp.status_code, 'error': ''}

def save_checkpoint():
    global stats, results_list, checkpoint_file, output_file
    with lock:
        Path(checkpoint_file).write_text(json.dumps({'stats': stats, 'checked_count': len(results_list)}))
        if output_file and results_list:
            with open(output_file, 'w') as f:
                f.write('domain,platform,status_code,error\n')
                for r in results_list:
                    f.write(f"{r['domain']},{r['platform']},{r['status_code']},{r['error']}\n")

def signal_handler(sig, frame):
    global running
    print("\n[!] CTRL+C - Saving progress...")
    running = False
    save_checkpoint()
    print(f"[!] Saved {len(results_list):,} results. Use -r to resume.")
    sys.exit(0)

def fmt_time(s):
    return f"{s:.0f}s" if s < 60 else f"{s/60:.1f}m" if s < 3600 else f"{s/3600:.1f}h"

def print_stats(stats, elapsed):
    sys.stdout.write('\033[H\033[J')
    checked = stats['checked']
    detected = stats['detected']
    errors = stats['errors']
    total = stats['total']
    rate = checked / elapsed if elapsed > 0 else 0
    det_pct = 100 * detected / checked if checked > 0 else 0
    err_pct = 100 * errors / checked if checked > 0 else 0
    remaining = total - checked
    prog_pct = 100 * checked / total if total > 0 else 0
    
    print("=" * 73)
    print(f"E-COMMERCE PLATFORM DETECTOR          PLATFORMS DETECTED")
    print("=" * 73)
    print(f"Domains Checked: {checked:>15,}   -----------------------------------")
    print(f"Platforms Found: {detected:>15,}")
    print(f"Detection Rate:  {det_pct:>14.1f}%")
    print(f"Errors:          {errors:>10,} ({err_pct:.1f}%)")
    print(f"Speed:           {rate:>14.1f}/s")
    print(f"Elapsed:         {fmt_time(elapsed):>15}")
    print("-" * 35 + "   " + "-" * 35)
    print("PROGRESS:                             PLATFORMS:")
    print("-" * 35 + "   " + "-" * 35)
    
    plats = sorted(stats['platforms'].items(), key=lambda x: -x[1])
    left = [f"Checked: {checked:>20,}", f"Remaining: {remaining:>18,}", f"Progress: {prog_pct:>18.1f}%"]
    right = [f"{p}: {c:,} ({100*c/detected:.1f}%)" if detected else f"{p}: {c:,}" for p,c in plats]
    
    for i in range(max(len(left), len(right), 3)):
        l = left[i] if i < len(left) else ""
        r = right[i] if i < len(right) else ""
        print(f"{l:<35}   {r}")
    print()
    sys.stdout.flush()

def process_domain(args):
    global stats, results_list, running
    domain, timeout = args
    if not running: return None
    result = check_domain(domain, timeout)
    with lock:
        stats['checked'] += 1
        results_list.append(result)
        if result['error']: stats['errors'] += 1
        if result['platform']:
            stats['detected'] += 1
            stats['platforms'][result['platform']] = stats['platforms'].get(result['platform'], 0) + 1
        if stats['checked'] % 5000 == 0:
            save_checkpoint()
    return result

def main():
    global stats, results_list, checkpoint_file, output_file, running
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    p = argparse.ArgumentParser(description='E-commerce Platform Detector')
    p.add_argument('domain', nargs='?')
    p.add_argument('-f', '--file')
    p.add_argument('-w', '--workers', type=int, default=20)
    p.add_argument('-t', '--timeout', type=int, default=10)
    p.add_argument('-o', '--output')
    p.add_argument('-r', '--resume', action='store_true')
    args = p.parse_args()
    
    if not args.domain and not args.file:
        p.error('Provide domain or -f file')
    
    output_file = args.output
    checkpoint_file = (args.output or 'detector') + '_checkpoint.json'
    
    if args.domain:
        r = check_domain(args.domain, args.timeout)
        print(f"{r['domain']},{r['platform']},{r['status_code']},{r['error']}")
        return
    
    domains = [l.strip() for l in open(args.file) if l.strip()]
    skip = 0
    
    if args.resume and Path(checkpoint_file).exists():
        data = json.loads(Path(checkpoint_file).read_text())
        stats = data['stats']
        skip = data['checked_count']
        if args.output and Path(args.output).exists():
            with open(args.output) as f:
                next(f)
                for line in f:
                    parts = line.strip().split(',')
                    if len(parts) >= 4:
                        results_list.append({'domain': parts[0], 'platform': parts[1], 'status_code': parts[2], 'error': parts[3]})
        print(f"[RESUME] Skipping {skip:,} already checked")
        domains = domains[skip:]
    
    stats['total'] = len(domains) + skip
    start = time.time()
    last_print = 0
    
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = [ex.submit(process_domain, (d, args.timeout)) for d in domains]
        for f in as_completed(futures):
            if not running: break
            elapsed = time.time() - start
            if elapsed - last_print >= 0.5:
                print_stats(stats, elapsed)
                last_print = elapsed
    
    save_checkpoint()
    print_stats(stats, time.time() - start)
    print(f"\nDone! Results: {args.output}")

if __name__ == '__main__':
    main()
