# Common Crawl E-Commerce Parser

Fast tools for discovering e-commerce domains from Common Crawl data with live CMS detection.

## Tools

### 1. Crawler - Common Crawl Scanner
Scans Common Crawl index for domains by TLD with optional parallel live CMS verification.

```bash
python crawler.py -t us,uk,de --live-check -o results/
```

**Output:**
- `results/crawl_domains.csv` - All domains from Common Crawl
- `results/live_detected.csv` - Verified CMS platforms (if --live-check)

**Options:**
```
-t, --tld          TLDs to scan (comma-separated)
-l, --limit        Max domains per TLD (default: 100M)
-p, --pages        CC index pages to scan (default: 80)
-w, --threads      Crawler threads (default: 1)
-o, --output       Output directory (default: output/)
--live-check       Enable parallel live CMS detection
--live-threads     Live checker threads (default: 50)
--live-timeout     HTTP timeout seconds (default: 10)
-k, --keywords     Filter by URL keywords
-c, --cms          Filter by CMS type
--from, --to       Date range filter (YYYYMMDD)
--status           HTTP status filter
--lang             Language filter
--exclude          File with domains to exclude
--list-crawls      Show available CC indexes
```

### 2. Detector - Batch Domain Scanner
Scans domain list file for e-commerce platforms.

```bash
python detector.py domains.txt -o detected.csv -w 200
```

**Performance:** ~76 domains/sec on 1.28M domains

**Options:**
```
-o, --output       Output CSV file
-w, --workers      Parallel workers (default: 100)
-t, --timeout      HTTP timeout (default: 10)
-v, --verbose      Show progress
```

## Detected Platforms

Shopify, WooCommerce, Magento, PrestaShop, OpenCart, BigCommerce, Wix, Squarespace, Salesforce Commerce, SAP Commerce, VTEX, Nuvemshop, Tiendanube, JEECMS, and 30+ more.

## Installation

```bash
pip install -r requirements.txt
python crawler.py --help
python detector.py --help
```

## Examples

```bash
# Scan Latin America TLDs with live detection
python crawler.py -t cl,pe,ar,mx,co,br --live-check --live-threads 100 -o latam/

# Scan with keyword filter
python crawler.py -t us -k shop,store,buy --live-check -o us_shops/

# Scan specific CMS only
python crawler.py -t de --cms shopify,woocommerce -o de_shops/

# List available Common Crawl indexes
python crawler.py --list-crawls

# Batch scan domain file
python detector.py my_domains.txt -o results.csv -w 200
```
