"""
E-commerce Platform Detection Module
Multi-level detection: headers -> cookies -> meta tags -> HTML content
"""

import re
import requests
from typing import Optional, Tuple, Dict


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
