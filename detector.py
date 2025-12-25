"""
E-commerce Platform Detection Module
Multi-level detection: headers -> cookies -> meta tags -> HTML content
FIXED: Checkpoint save, resume, graceful CTRL+C

Usage:
  python detector.py domain.com
  python detector.py -f domains.txt -w 50 -o results.csv
  python detector.py -f domains.txt -w 50 -o results.csv -r  # resume
"""

import sys
import argparse
import re
import requests
import time
import signal
import json
from pathlib import Path
from typing import Optional, Tuple, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

REQUEST_TIMEOUT = 10

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}

# Global state
lock = Lock()
running = True
stats = {'checked': 0, 'detected': 0, 'errors': 0, 'total': 0, 'platforms': {}}
results_list = []
checkpoint_file = None
output_file = None


def fetch_url(url: str, timeout: int = REQUEST_TIMEOUT) -> Tuple[Optional[requests.Response], str]:
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    try:
        response = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True, verify=False)
        return response, ''
    except requests.exceptions.Timeout:
        return None, 'timeout'
    except requests.exceptions.SSLError:
        return None, 'ssl_error'
    except requests.exceptions.ConnectionError:
        return None, 'connection_error'
    except requests.exceptions.TooManyRedirects:
        return None, 'too_many_redirects'
    except requests.exceptions.RequestException as e:
        return None, f'error: {str(e)[:30]}'


def detect_from_headers(headers: Dict) -> str:
    headers_lower = {k.lower(): v.lower() for k, v in headers.items()}
    if 'x-shopify-stage' in headers_lower or 'x-shopid' in headers_lower:
        return 'Shopify'
    if headers_lower.get('server', '').startswith('shopify'):
        return 'Shopify'
    if 'x-bc-' in str(headers_lower):
        return 'BigCommerce'
    if 'x-magento-' in str(headers_lower):
        return 'Magento'
    if 'x-dw-request-base-id' in headers_lower:
        return 'Demandware'
    if 'x-wix-request-id' in headers_lower:
        return 'Wix'
    return ''


def detect_from_cookies(cookies) -> str:
    cookie_str = str(cookies).lower()
    if 'woocommerce_' in cookie_str or 'wp_woocommerce' in cookie_str:
        return 'WooCommerce'
    if '_shopify_' in cookie_str or 'shopify_pay' in cookie_str:
        return 'Shopify'
    if 'mage-' in cookie_str or 'form_key' in cookie_str:
        return 'Magento'
    if 'prestashop' in cookie_str:
        return 'PrestaShop'
    if 'phpsessid' in cookie_str and 'currency' in cookie_str and 'language' in cookie_str:
        return 'OpenCart'
    if 'bitrix_' in cookie_str:
        return 'Bitrix'
    return ''


def detect_from_meta(html: str) -> str:
    generator_match = re.search(r'<meta[^>]*name=["\']generator["\'][^>]*content=["\']([^"\']+)["\']', html, re.IGNORECASE)
    if not generator_match:
        generator_match = re.search(r'<meta[^>]*content=["\']([^"\']+)["\'][^>]*name=["\']generator["\']', html, re.IGNORECASE)
    if generator_match:
        generator = generator_match.group(1).lower()
        mapping = {'woocommerce': 'WooCommerce', 'shopify': 'Shopify', 'magento': 'Magento',
                   'prestashop': 'PrestaShop', 'opencart': 'OpenCart', 'squarespace': 'Squarespace',
                   'wix': 'Wix', 'weebly': 'Weebly', 'tilda': 'Tilda', 'bitrix': 'Bitrix',
                   '1c-bitrix': 'Bitrix', 'shopware': 'Shopware', 'ecwid': 'Ecwid'}
        for k, v in mapping.items():
            if k in generator:
                return v
    return ''


PLATFORM_CHECKS = [
    (lambda t: 'shopify' in t or 'cdn.shopify.com' in t or 'myshopify.com' in t, 'Shopify'),
    (lambda t: 'woocommerce' in t or 'wc-block' in t or '/wp-content/plugins/woocommerce/' in t, 'WooCommerce'),
    (lambda t: 'magento' in t or '/skin/frontend/' in t or '/static/frontend/' in t or 'mage.cookies' in t, 'Magento'),
    (lambda t: 'bigcommerce' in t or 'cdn.bigcommerce.com' in t, 'BigCommerce'),
    (lambda t: 'prestashop' in t or '/modules/ps_' in t or '/themes/classic/assets/' in t or 'blockcart' in t or 'ps_customersignin' in t, 'PrestaShop'),
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


def detect_platform(text: str) -> str:
    text_lower = text.lower()
    for check_func, platform_name in PLATFORM_CHECKS:
        try:
            if check_func(text_lower):
                return platform_name
        except:
            continue
    return ''


def check_domain(domain: str, timeout: int = REQUEST_TIMEOUT) -> dict:
    response, error = fetch_url(domain, timeout)
    if response is None:
        return {'domain': domain, 'platform': '', 'status_code': 0, 'error': error}
    
    platform = ''
    html_text = response.text
    platform = detect_from_headers(dict(response.headers))
    if not platform:
        platform = detect_from_cookies(response.cookies)
    if not platform:
        platform = detect_platform(html_text)
    if not platform:
        platform = detect_from_meta(html_text)
    
    return {'domain': domain, 'platform': platform, 'status_code': response.status_code, 'error': ''}


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


def format_time(seconds):
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}m"
    else:
        return f"{seconds/3600:.1f}h"


def print_stats(stats, elapsed):
    sys.stdout.write('\033[H\033[J')
    W, G, SEP, EQ = 35, '   ', '-' * 35, '=' * 73
    
    def fmt(label, value):
        return label + ' ' * (W - len(label) - len(str(value))) + str(value)
    
    rate = stats['checked'] / elapsed if elapsed > 0 else 0
    det_pct = 100 * stats['detected'] / stats['checked'] if stats['checked'] > 0 else 0
    err_pct = 100 * stats['errors'] / stats['checked'] if stats['checked'] > 0 else 0
    remaining = stats['total'] - stats['checked']
    progress_pct = 100 * stats['checked'] / stats['total'] if stats['total'] > 0 else 0
    
    print(EQ)
    print(f"{'E-COMMERCE PLATFORM DETECTOR':<35}{G}{'PLATFORMS DETECTED':<35}")
    print(EQ)
    print(f"{fmt('Domains Checked:', f'{stats[\"checked\"]:,}'):<35}{G}{SEP}")
    print(f"{fmt('Platforms Found:', f'{stats[\"detected\"]:,}'):<35}")
    print(f"{fmt('Detection Rate:', f'{det_pct:.1f}%'):<35}")
    print(f"{fmt('Errors:', f'{stats[\"errors\"]:,} ({err_pct:.1f}%)'):<35}")
    print(f"{fmt('Speed:', f'{rate:.1f}/s'):<35}")
    print(f"{fmt('Elapsed:', format_time(elapsed)):<35}")
    print(f"{SEP:<35}{G}{SEP}")
    print(f"{'PROGRESS:':<35}{G}{'PLATFORMS:':<35}")
    print(f"{SEP:<35}{G}{SEP}")
    
    plat_list = sorted(stats['platforms'].items(), key=lambda x: -x[1])
    rows_left = [fmt('Checked:', f"{stats['checked']:,}"), fmt('Remaining:', f"{remaining:,}"), fmt('Progress:', f"{progress_pct:.1f}%")]
    rows_right = [fmt(p, f"{c:,} ({100*c/stats['detected']:.1f}%)") if stats['detected'] > 0 else fmt(p, f"{c:,}") for p, c in plat_list]
    
    for i in range(max(len(rows_left), len(rows_right), 1)):
        left = rows_left[i] if i < len(rows_left) else ' ' * W
        right = rows_right[i] if i < len(rows_right) else ' ' * W
        print(f"{left:<35}{G}{right:<35}")
    print()
    sys.stdout.flush()


def process_domain(args):
    global stats, results_list, running
    domain, timeout = args
    if not running:
        return None
    
    result = check_domain(domain, timeout)
    
    with lock:
        stats['checked'] += 1
        results_list.append(result)
        if result['error']:
            stats['errors'] += 1
        if result['platform']:
            stats['detected'] += 1
            stats['platforms'][result['platform']] = stats['platforms'].get(result['platform'], 0) + 1
        
        if stats['checked'] % 5000 == 0:
            save_checkpoint()
    
    return result


def main():
    global stats, results_list, checkpoint_file, output_file, running
    
    import urllib3
    urllib3.disable_warnings()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    parser = argparse.ArgumentParser(description='E-commerce Platform Detector')
    parser.add_argument('domain', nargs='?', help='Single domain to check')
    parser.add_argument('-f', '--file', help='File with domains')
    parser.add_argument('-w', '--workers', type=int, default=20, help='Parallel workers (default: 20)')
    parser.add_argument('-t', '--timeout', type=int, default=10, help='Request timeout (default: 10)')
    parser.add_argument('-o', '--output', help='Output CSV file')
    parser.add_argument('-r', '--resume', action='store_true', help='Resume from checkpoint')
    args = parser.parse_args()
    
    if not args.domain and not args.file:
        parser.error('Provide a domain or -f/--file with domain list')
    
    output_file = args.output
    checkpoint_file = (args.output or 'detector') + '_checkpoint.json'
    
    if args.domain:
        result = check_domain(args.domain, args.timeout)
        print(f"{result['domain']},{result['platform']},{result['status_code']},{result['error']}")
        return
    
    domains = [line.strip() for line in open(args.file) if line.strip()]
    skip = 0
    
    if args.resume and Path(checkpoint_file).exists():
        data = json.loads(Path(checkpoint_file).read_text())
        stats = data['stats']
        skip = data['checked_count']
        if args.output and Path(args.output).exists():
            with open(args.output) as f:
                next(f)  # skip header
                for line in f:
                    parts = line.strip().split(',')
                    if len(parts) >= 4:
                        results_list.append({'domain': parts[0], 'platform': parts[1], 'status_code': parts[2], 'error': parts[3]})
        print(f"[RESUME] Skipping {skip:,} already checked")
        domains = domains[skip:]
    
    stats['total'] = len(domains) + skip
    start_time = time.time()
    last_print = 0
    
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(process_domain, (d, args.timeout)) for d in domains]
        for future in as_completed(futures):
            if not running:
                break
            elapsed = time.time() - start_time
            if elapsed - last_print >= 0.5:
                print_stats(stats, elapsed)
                last_print = elapsed
    
    save_checkpoint()
    elapsed = time.time() - start_time
    print_stats(stats, elapsed)
    print(f"\nDone! Results saved to {args.output or 'stdout'}")


if __name__ == '__main__':
    main()
