"""
E-commerce Platform Detection Module
Multi-level detection: headers -> cookies -> meta tags -> HTML content

Usage:
  python detector.py domain.com
  python detector.py -f domains.txt -w 50 -o results.csv
"""

import sys
import argparse
import re
import requests
from typing import Optional, Tuple, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed


REQUEST_TIMEOUT = 10

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}


def fetch_url(url: str, timeout: int = REQUEST_TIMEOUT) -> Tuple[Optional[requests.Response], str]:
    """
    Safely fetch URL content
    Returns (response, error_message)
    """
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
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
    """Detect platform from HTTP headers"""
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
    """Detect platform from cookies"""
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
    """Detect platform from meta generator tag"""
    generator_match = re.search(
        r'<meta[^>]*name=["\']generator["\'][^>]*content=["\']([^"\']+)["\']',
        html, re.IGNORECASE
    )
    if not generator_match:
        generator_match = re.search(
            r'<meta[^>]*content=["\']([^"\']+)["\'][^>]*name=["\']generator["\']',
            html, re.IGNORECASE
        )
    
    if generator_match:
        generator = generator_match.group(1).lower()
        
        if 'woocommerce' in generator:
            return 'WooCommerce'
        if 'shopify' in generator:
            return 'Shopify'
        if 'magento' in generator:
            return 'Magento'
        if 'prestashop' in generator:
            return 'PrestaShop'
        if 'opencart' in generator:
            return 'OpenCart'
        if 'squarespace' in generator:
            return 'Squarespace'
        if 'wix' in generator:
            return 'Wix'
        if 'weebly' in generator:
            return 'Weebly'
        if 'tilda' in generator:
            return 'Tilda'
        if 'bitrix' in generator or '1c-bitrix' in generator:
            return 'Bitrix'
        if 'shopware' in generator:
            return 'Shopware'
        if 'ecwid' in generator:
            return 'Ecwid'
    
    return ''


def is_shopify(text: str) -> bool:
    return (
        'shopify' in text or
        'cdn.shopify.com' in text or
        'myshopify.com' in text
    )


def is_woocommerce(text: str) -> bool:
    return (
        'woocommerce' in text or
        'wc-block' in text or
        '/wp-content/plugins/woocommerce/' in text
    )


def is_magento(text: str) -> bool:
    return (
        'magento' in text or
        '/skin/frontend/' in text or
        '/static/frontend/' in text or
        'mage.cookies' in text
    )


def is_bigcommerce(text: str) -> bool:
    return (
        'bigcommerce' in text or
        'cdn.bigcommerce.com' in text
    )


def is_prestashop(text: str) -> bool:
    return (
        'prestashop' in text or
        '/modules/ps_' in text or
        '/themes/prestashop' in text or
        'prestashop-ui-kit' in text or
        '/themes/classic/assets/' in text or
        'blockcart' in text or
        'ps_customersignin' in text or
        'ps_shoppingcart' in text or
        'presta-' in text or
        '/modules/productcomments/' in text or
        'id_product' in text and 'id_product_attribute' in text
    )


def is_wix(text: str) -> bool:
    return (
        'wix.com' in text or
        'wixsite.com' in text or
        '_wix_' in text
    )


def is_squarespace(text: str) -> bool:
    return (
        'squarespace' in text or
        'static.squarespace.com' in text
    )


def is_bigcartel(text: str) -> bool:
    return 'bigcartel' in text


def is_opencart(text: str) -> bool:
    return (
        'opencart' in text or
        'index.php?route=' in text or
        'catalog/view/theme' in text
    )


def is_3dcart(text: str) -> bool:
    return '3dcart' in text or 'shift4shop' in text


def is_volusion(text: str) -> bool:
    return 'volusion' in text


def is_demandware(text: str) -> bool:
    return (
        'demandware' in text or
        'dwvar_' in text or
        'dwfrm_' in text
    )


def is_sellfy(text: str) -> bool:
    return 'sellfy' in text


def is_ecwid(text: str) -> bool:
    return 'ecwid' in text


def is_weebly(text: str) -> bool:
    return 'weebly' in text or 'editmysite.com' in text


def is_salesforce_commerce(text: str) -> bool:
    return 'salesforce' in text and 'commerce' in text


def is_vtex(text: str) -> bool:
    return 'vtex' in text


def is_shopware(text: str) -> bool:
    return 'shopware' in text


def is_nopcommerce(text: str) -> bool:
    return 'nopcommerce' in text


def is_lightspeed(text: str) -> bool:
    return 'lightspeed' in text or 'seoshop' in text


def is_tilda(text: str) -> bool:
    return 'tilda' in text or 'tildacdn' in text


def is_bitrix(text: str) -> bool:
    return 'bitrix' in text or '1c-bitrix' in text


def is_insales(text: str) -> bool:
    return 'insales' in text


def is_cs_cart(text: str) -> bool:
    return 'cs-cart' in text or 'cscart' in text


PLATFORM_CHECKS = [
    (is_shopify, 'Shopify'),
    (is_woocommerce, 'WooCommerce'),
    (is_magento, 'Magento'),
    (is_bigcommerce, 'BigCommerce'),
    (is_prestashop, 'PrestaShop'),
    (is_wix, 'Wix'),
    (is_squarespace, 'Squarespace'),
    (is_bigcartel, 'BigCartel'),
    (is_opencart, 'OpenCart'),
    (is_3dcart, '3DCart'),
    (is_volusion, 'Volusion'),
    (is_demandware, 'Demandware'),
    (is_sellfy, 'Sellfy'),
    (is_ecwid, 'Ecwid'),
    (is_weebly, 'Weebly'),
    (is_salesforce_commerce, 'SalesforceCommerce'),
    (is_vtex, 'VTEX'),
    (is_shopware, 'Shopware'),
    (is_nopcommerce, 'nopCommerce'),
    (is_lightspeed, 'Lightspeed'),
    (is_tilda, 'Tilda'),
    (is_bitrix, 'Bitrix'),
    (is_insales, 'InSales'),
    (is_cs_cart, 'CS-Cart'),
]


def detect_platform(text: str) -> str:
    """Detect platform from HTML content"""
    text_lower = text.lower()
    
    for check_func, platform_name in PLATFORM_CHECKS:
        try:
            if check_func(text_lower):
                return platform_name
        except Exception:
            continue
    
    return ''


def check_domain(domain: str, timeout: int = REQUEST_TIMEOUT) -> dict:
    """
    Check single domain for e-commerce platform
    Uses multi-level detection: headers -> cookies -> HTML -> meta
    Returns dict with result
    """
    response, error = fetch_url(domain, timeout)
    
    if response is None:
        return {
            'domain': domain,
            'platform': '',
            'status_code': 0,
            'error': error
        }
    
    platform = ''
    html_text = response.text
    
    platform = detect_from_headers(dict(response.headers))
    
    if not platform:
        platform = detect_from_cookies(response.cookies)
    
    if not platform:
        platform = detect_platform(html_text)
    
    if not platform:
        platform = detect_from_meta(html_text)
    
    return {
        'domain': domain,
        'platform': platform,
        'status_code': response.status_code,
        'error': ''
    }


import time


def format_time(seconds):
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}m"
    else:
        return f"{seconds/3600:.1f}h"


def print_stats(stats, elapsed):
    """Print formatted statistics like crawler.py"""
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
        right = right[:W].ljust(W) if right else BLANK
        print(left + G + right)
    
    rate = stats['checked'] / elapsed if elapsed > 0 else 0
    det_pct = 100 * stats['detected'] / stats['checked'] if stats['checked'] > 0 else 0
    err_pct = 100 * stats['errors'] / stats['checked'] if stats['checked'] > 0 else 0
    
    print(EQ)
    line('E-COMMERCE PLATFORM DETECTOR', 'PLATFORMS DETECTED')
    print(EQ)
    
    line(fmt('Domains Checked:', f"{stats['checked']:,}"), SEP)
    line(fmt('Platforms Found:', f"{stats['detected']:,}"), '')
    line(fmt('Detection Rate:', f"{det_pct:.1f}%"), '')
    line(fmt('Errors:', f"{stats['errors']:,} ({err_pct:.1f}%)"), '')
    line(fmt('Speed:', f"{rate:.1f}/s"), '')
    line(fmt('Elapsed:', format_time(elapsed)), '')
    
    line(SEP, SEP)
    line('PROGRESS:', 'PLATFORMS:')
    line(SEP, SEP)
    
    plat_list = sorted(stats['platforms'].items(), key=lambda x: -x[1])
    
    remaining = stats['total'] - stats['checked']
    progress_pct = 100 * stats['checked'] / stats['total'] if stats['total'] > 0 else 0
    
    rows_left = [
        fmt('Checked:', f"{stats['checked']:,}"),
        fmt('Remaining:', f"{remaining:,}"),
        fmt('Progress:', f"{progress_pct:.1f}%")
    ]
    rows_right = []
    
    for p, c in plat_list:
        pct = 100 * c / stats['detected'] if stats['detected'] > 0 else 0
        rows_right.append(fmt(p, f"{c:,} ({pct:5.1f}%)"))
    
    max_rows = max(len(rows_left), len(rows_right), 1)
    for i in range(max_rows):
        left = rows_left[i] if i < len(rows_left) else BLANK
        right = rows_right[i] if i < len(rows_right) else BLANK
        line(left, right)
    
    print()
    sys.stdout.flush()


def main():
    parser = argparse.ArgumentParser(
        description='E-commerce Platform Detector',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python detector.py example.com
  python detector.py -f domains.txt -w 50
  python detector.py -f domains.txt -o results.csv -t 15

Output: domain, platform, status_code, error
        """
    )
    
    parser.add_argument(
        'domain',
        nargs='?',
        help='Single domain to check'
    )
    
    parser.add_argument(
        '-f', '--file',
        help='File with domains (one per line)'
    )
    
    parser.add_argument(
        '-w', '--workers',
        type=int,
        default=20,
        help='Parallel workers (default: 20)'
    )
    
    parser.add_argument(
        '-t', '--timeout',
        type=int,
        default=10,
        help='Request timeout in seconds (default: 10)'
    )
    
    parser.add_argument(
        '-o', '--output',
        help='Output CSV file (default: print to stdout)'
    )
    
    args = parser.parse_args()
    
    if not args.domain and not args.file:
        parser.error('Provide a domain or -f/--file with domain list')
    
    domains = []
    if args.domain:
        domains = [args.domain]
    elif args.file:
        with open(args.file, 'r') as f:
            domains = [line.strip() for line in f if line.strip()]
    
    if not domains:
        print('No domains to check')
        return
    
    results = []
    
    if len(domains) == 1:
        result = check_domain(domains[0], args.timeout)
        results.append(result)
        if result['platform']:
            print(f"{result['domain']}: {result['platform']}")
        elif result['error']:
            print(f"{result['domain']}: ERROR - {result['error']}")
        else:
            print(f"{result['domain']}: No platform detected")
    else:
        stats = {
            'total': len(domains),
            'checked': 0,
            'detected': 0,
            'errors': 0,
            'platforms': {}
        }
        start_time = time.time()
        
        csv_fh = None
        if args.output:
            csv_fh = open(args.output, 'w')
            csv_fh.write('domain,platform,status_code,error\n')
        
        print_stats(stats, 0)
        
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futures = {ex.submit(check_domain, d, args.timeout): d for d in domains}
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                
                stats['checked'] += 1
                if result['platform']:
                    stats['detected'] += 1
                    p = result['platform']
                    stats['platforms'][p] = stats['platforms'].get(p, 0) + 1
                if result['error']:
                    stats['errors'] += 1
                
                if csv_fh:
                    csv_fh.write(f"{result['domain']},{result['platform']},{result['status_code']},{result['error']}\n")
                    csv_fh.flush()
                
                if stats['checked'] % 10 == 0 or stats['checked'] == len(domains):
                    print_stats(stats, time.time() - start_time)
        
        if csv_fh:
            csv_fh.close()
        
        elapsed = time.time() - start_time
        print_stats(stats, elapsed)
        
        print('=' * 73)
        print(' COMPLETED '.center(73))
        print('=' * 73)
        print()
        if args.output:
            print(f'  Results saved to: {args.output}')
        print(f'  Total domains:    {len(domains):,}')
        print(f'  Platforms found:  {stats["detected"]:,}')
        print(f'  Detection rate:   {100*stats["detected"]/len(domains):.1f}%')
        print(f'  Time elapsed:     {format_time(elapsed)}')
        print()


if __name__ == '__main__':
    main()
