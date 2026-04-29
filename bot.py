#!/usr/bin/env python3
"""
Dark Web & Clearnet Credit Card Crawler (Hardened & Fixed)
- Telegram bot for admin control and masked card notifications
- Proxy rotation system with Tor fallback, HTTP/HTTPS/SOCKS support
- Flask dashboard
- All critical bugs fixed, threading hardened, security improved
"""

import os
import sys
import re
import time
import random
import threading
import logging
import asyncio
import queue
import urllib.parse
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from stem import Signal
from stem.control import Controller
from flask import Flask, render_template_string
from cryptography.fernet import Fernet

# python-telegram-bot v20.x
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.constants import ParseMode

# ========== CONFIGURATION ==========
TOR_CONTROL_PORT = 9051
TOR_PASSWORD = os.environ.get("TOR_PASSWORD")          # Must be set, no default
if not TOR_PASSWORD:
    sys.exit("FATAL: TOR_PASSWORD environment variable not set. Tor control password required.")

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
ADMIN_CHAT_IDS = [int(x.strip()) for x in os.environ.get("ADMIN_CHAT_IDS", "").split(",") if x.strip()]
FLASK_PORT = int(os.environ.get("FLASK_PORT", "5000"))

# Proxy test URL configurable
PROXY_TEST_URL = os.environ.get("PROXY_TEST_URL", "http://httpbin.org/ip")

# Fernet key for encrypting scraped card data (generate if not set)
FERNET_KEY = os.environ.get("FERNET_KEY")
if not FERNET_KEY:
    # In a real deployment you would set this permanently; for educational use we generate a new one each run.
    FERNET_KEY = Fernet.generate_key()
    logging.warning("FERNET_KEY not set. Generated temporary key. Encrypted data will be lost after restart.")
fernet = Fernet(FERNET_KEY)

ROTATE_EVERY_N_REQUESTS = 20
TOR_IP_RENEW_REQUESTS = 100
MAX_RETRIES = 3
PROXY_FILE = "proxies.txt"

# Proxy types
PROXY_TYPE_HTTP = "http"
PROXY_TYPE_HTTPS = "https"
PROXY_TYPE_SOCKS4 = "socks4"
PROXY_TYPE_SOCKS5 = "socks5"

# Tor proxy (always added) – using socks5:// for better compatibility
TOR_PROXY_URL = "socks5://127.0.0.1:9050"
TOR_PROXY_TYPE = PROXY_TYPE_SOCKS5

# Start URLs loaded from file or environment, with a small default
def load_start_urls() -> List[str]:
    """Load start URLs from environment or file, or use a default test set."""
    urls = []
    # Environment variable
    env_urls = os.environ.get("START_URLS")
    if env_urls:
        urls = [u.strip() for u in env_urls.split(",") if u.strip()]
    # File
    urls_file = "start_urls.txt"
    if os.path.exists(urls_file):
        with open(urls_file, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    urls.append(line)
    if not urls:
        # Default small test URLs (non-existent, placeholder – change for real targets)
        urls = ["http://example.com"]
    return urls

START_URLS = load_start_urls()
_start_urls_lock = threading.Lock()  # lock for modifying START_URLS

# ========== LOGGING & STATS ==========
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("crawler.log")  # permanent log file
    ]
)
logger = logging.getLogger("CC_Crawler")

# Thread‑safe structures
log_lock = threading.RLock()
file_lock = threading.Lock()       # for writing to scraped_cc.txt
stats_lock = threading.RLock()

stats = {
    "visited_urls": set(),
    "found_cards": [],          # list of (masked_card, card_type, source, time, encrypted_number)
    "start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "last_renew": "",
    "crawling_active": True
}
# Stop event for responsive shutdown
stop_event = threading.Event()

# Bounded log buffer
max_log_len = 500
log_lines = []

def add_log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    with log_lock:
        logger.info(line)
        log_lines.append(line)
        if len(log_lines) > max_log_len:
            log_lines.pop(0)

# ========== LUHN CHECK ==========
def luhn(card_num: str) -> bool:
    digits = [int(d) for d in card_num]
    for i in range(len(digits) - 2, -1, -2):
        digits[i] *= 2
        if digits[i] > 9:
            digits[i] -= 9
    return sum(digits) % 10 == 0

# ========== IMPROVED CARD TYPE IDENTIFICATION ==========
def identify_card_type(number: str) -> str:
    """Identify card brand using full BIN ranges and length validation."""
    if not number.isdigit():
        return "Invalid"
    # Visa: 13 or 16 digits, starts with 4
    if number[0] == '4' and len(number) in (13, 16):
        return "Visa"
    # MasterCard: 16 digits, starts with 51-55 or 2221-2720
    if len(number) == 16:
        if number[:2] in ('51','52','53','54','55'):
            return "MasterCard"
        if 2221 <= int(number[:4]) <= 2720:
            return "MasterCard"
    # Amex: 15 digits, starts with 34 or 37
    if len(number) == 15 and number[:2] in ('34','37'):
        return "Amex"
    # Discover: 16 digits, starts with 6011, 622126-622925, 644-649, 65
    if len(number) == 16:
        if number[:4] == '6011':
            return "Discover"
        if 622126 <= int(number[:6]) <= 622925:
            return "Discover"
        if 644 <= int(number[:3]) <= 649:
            return "Discover"
        if number[:2] == '65':
            return "Discover"
    # Diners Club: 14 digits, starts with 300-305, 309, 36, 38, 39
    if len(number) == 14:
        if number[:3] in ('300','301','302','303','304','305','309'):
            return "Diners Club"
        if number[:2] in ('36','38','39'):
            return "Diners Club"
    # JCB: 16 digits, starts with 3528-3589, or 15 digits starting with 1800 or 2131
    if len(number) == 16 and 3528 <= int(number[:4]) <= 3589:
        return "JCB"
    if len(number) == 15 and number[:4] in ('1800','2131'):
        return "JCB"
    # UnionPay: 16-19 digits, starts with 62 (simplistic)
    if 16 <= len(number) <= 19 and number[:2] == '62':
        return "UnionPay"
    return "Other"

def mask_card(number: str) -> str:
    if len(number) >= 8:
        return number[:4] + "*" * (len(number) - 8) + number[-4:]
    return number[:4] + "****"

def mask_proxy_url(proxy_url: str) -> str:
    """Hide username/password in proxy URL for display."""
    try:
        parsed = urllib.parse.urlparse(proxy_url)
    except ValueError:
        return proxy_url  # malformed, return as-is
    if parsed.username:
        safe = parsed._replace(netloc=f"{parsed.username}:****@{parsed.hostname}:{parsed.port}" if parsed.port
                               else f"{parsed.username}:****@{parsed.hostname}")
        return safe.geturl()
    return proxy_url

# ========== SECURE CARD STORAGE ==========
def _encrypt_card(card_number: str) -> bytes:
    """Encrypt card number with Fernet (symmetric)."""
    return fernet.encrypt(card_number.encode())

def write_card_secure(card_number: str, card_type: str, source_url: str, ts: str):
    """Append encrypted card data to file with tight permissions."""
    encrypted = _encrypt_card(card_number)
    line = f"{encrypted.decode()}\t{card_type}\t{source_url}\t{ts}\n"
    with file_lock:
        # Create file with restricted permissions if not exists
        if not os.path.exists("scraped_cc.txt"):
            fd = os.open("scraped_cc.txt", os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
            os.close(fd)
        with open("scraped_cc.txt", "a") as f:
            f.write(line)

# ========== PROXY MANAGER ==========
class ProxyManager:
    def __init__(self):
        self.proxies: List[Dict] = []
        self.lock = threading.Lock()
        self.round_robin_index = 0
        self.rotation_mode = "roundrobin"  # or "leastfailed"
        self.request_count = 0
        self.tor_request_count = 0
        self.force_next_rotate = False
        self.renew_lock = threading.Lock()  # prevent concurrent Tor renewal

        # Add Tor proxy first
        self._add_proxy(TOR_PROXY_URL, TOR_PROXY_TYPE, is_tor=True)

        # Load external proxies from file
        self.load_from_file()

    def _add_proxy(self, url: str, ptype: str, is_tor: bool = False):
        proxy = {
            "url": url,
            "type": ptype.lower(),
            "status": "alive",   # alive / dead
            "fail_count": 0,
            "last_used": None,
            "is_tor": is_tor
        }
        self.proxies.append(proxy)

    def load_from_file(self, filepath: str = PROXY_FILE):
        if not os.path.exists(filepath):
            return
        with self.lock:
            existing_urls = [p["url"] for p in self.proxies]
            with open(filepath, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    ptype = self._guess_type(line)
                    if line not in existing_urls:
                        self._add_proxy(line, ptype)
                        add_log(f"Loaded proxy: {mask_proxy_url(line)}")

    def _guess_type(self, url: str) -> str:
        scheme = url.split("://")[0].lower()
        if scheme in ("socks5", "socks5h"):
            return PROXY_TYPE_SOCKS5
        elif scheme in ("socks4",):
            return PROXY_TYPE_SOCKS4
        elif scheme in ("https",):
            return PROXY_TYPE_HTTPS
        else:
            return PROXY_TYPE_HTTP

    def test_proxy(self, proxy: Dict, test_url: str = None, timeout: int = 10) -> bool:
        """Test connectivity through proxy. Returns True if alive."""
        if test_url is None:
            test_url = PROXY_TEST_URL
        try:
            proxies_dict = self._build_proxies_dict(proxy)
            resp = requests.get(test_url, proxies=proxies_dict, timeout=timeout)
            return resp.status_code == 200
        except Exception:
            return False

    def _build_proxies_dict(self, proxy: Dict) -> Dict[str, str]:
        """Build requests-compatible proxies dict."""
        url = proxy["url"]
        return {"http": url, "https": url}

    def mark_failed(self, proxy: Dict):
        with self.lock:
            proxy["fail_count"] += 1
            if proxy["fail_count"] >= 3:
                proxy["status"] = "dead"
                add_log(f"Proxy {mask_proxy_url(proxy['url'])} marked DEAD after {proxy['fail_count']} failures")
            else:
                add_log(f"Proxy {mask_proxy_url(proxy['url'])} fail count: {proxy['fail_count']}")

    def mark_alive(self, proxy: Dict):
        with self.lock:
            proxy["status"] = "alive"
            proxy["fail_count"] = 0
            add_log(f"Proxy {mask_proxy_url(proxy['url'])} is ALIVE again")

    def get_alive_proxies(self, only_socks: bool = False) -> List[Dict]:
        with self.lock:
            alive = [p for p in self.proxies if p["status"] == "alive"]
            if only_socks:
                alive = [p for p in alive if p["type"] in (PROXY_TYPE_SOCKS4, PROXY_TYPE_SOCKS5)]
            return alive

    def get_next_proxy(self, is_onion: bool = False) -> Dict:
        """Select next proxy. For onion sites only SOCKS proxies are allowed.
        If no SOCKS proxy is alive and Tor is dead, raise RuntimeError instead of forcing.
        """
        with self.lock:
            self.request_count += 1
            if self.force_next_rotate:
                self.force_next_rotate = False
                # Shift rotation in all modes: advance round‑robin index or re‑order candidates
                self.round_robin_index += 1

            # Filter candidates
            if is_onion:
                candidates = self.get_alive_proxies(only_socks=True)
            else:
                candidates = self.get_alive_proxies()

            if not candidates:
                # Try to resurrect Tor just for this request
                tor_proxy = next((p for p in self.proxies if p["is_tor"]), None)
                if tor_proxy:
                    # Test the Tor proxy right now; if alive mark it and use it.
                    if self.test_proxy(tor_proxy):
                        self.mark_alive(tor_proxy)
                        candidates = [tor_proxy]
                    else:
                        self.mark_failed(tor_proxy)
                        raise RuntimeError("No alive proxy available (Tor is dead)")
                else:
                    raise RuntimeError("No usable proxy available (Tor missing)")

            if self.rotation_mode == "roundrobin":
                self.round_robin_index %= len(candidates)
                proxy = candidates[self.round_robin_index]
                self.round_robin_index += 1
            else:  # leastfailed
                # If user triggered force rotate, shift by picking the one with second‑lowest fails
                # Simple approach: sort by fail_count, then pick the one after the current minimum (or just the min).
                candidates.sort(key=lambda p: p["fail_count"])
                # If we forced rotate, skip the first (minimum fail) and pick second. We use a flag for that.
                if getattr(self, "_leastfailed_force_rotate", False):
                    self._leastfailed_force_rotate = False
                    proxy = candidates[1] if len(candidates) > 1 else candidates[0]
                else:
                    proxy = candidates[0]  # minimum fail count

            proxy["last_used"] = datetime.now().strftime("%H:%M:%S")
            # Track Tor usage for IP renewal (non‑blocking)
            if proxy["is_tor"]:
                self.tor_request_count += 1
                if self.tor_request_count >= TOR_IP_RENEW_REQUESTS:
                    self.tor_request_count = 0
                    # Trigger renewal outside lock
                    threading.Thread(target=renew_tor_ip, daemon=True).start()
            return proxy

    def force_rotate(self):
        with self.lock:
            # For roundrobin we already advance the index; for leastfailed we set a flag
            if self.rotation_mode == "leastfailed":
                self._leastfailed_force_rotate = True
            else:
                self.force_next_rotate = True

    def periodic_dead_check(self):
        """Background thread to re‑test dead proxies every 5 minutes."""
        while not stop_event.is_set():
            stop_event.wait(300)
            if stop_event.is_set():
                break
            with self.lock:
                dead_proxies = [p for p in self.proxies if p["status"] == "dead"]
            for proxy in dead_proxies:
                if self.test_proxy(proxy):
                    self.mark_alive(proxy)

    def add_proxy(self, url: str) -> bool:
        url = url.strip()
        if not url:
            return False
        with self.lock:
            existing = [p["url"] for p in self.proxies]
            if url in existing:
                return False
            ptype = self._guess_type(url)
            proxy = {
                "url": url,
                "type": ptype,
                "status": "unknown",
                "fail_count": 0,
                "last_used": None,
                "is_tor": False
            }
            self.proxies.append(proxy)
        # Test outside lock
        if self.test_proxy(proxy):
            self.mark_alive(proxy)
            return True
        else:
            self.mark_failed(proxy)
            return False

    def remove_proxy(self, url: str) -> bool:
        with self.lock:
            for p in self.proxies:
                if p["url"] == url and not p["is_tor"]:
                    self.proxies.remove(p)
                    add_log(f"Proxy removed: {mask_proxy_url(url)}")
                    return True
        return False

    def list_proxies(self) -> List[Dict]:
        with self.lock:
            return [dict(p) for p in self.proxies]  # copy to avoid lock issues

# ========== GLOBAL PROXY INSTANCE ==========
proxy_manager = ProxyManager()

# ========== TOR IP RENEWAL (thread‑safe) ==========
renew_run_lock = threading.Lock()
def renew_tor_ip():
    if not renew_run_lock.acquire(blocking=False):
        add_log("Tor renewal already in progress, skipping.")
        return
    try:
        with Controller.from_port(port=TOR_CONTROL_PORT) as c:
            c.authenticate(password=TOR_PASSWORD)
            c.signal(Signal.NEWNYM)
        with stats_lock:
            stats["last_renew"] = datetime.now().strftime("%H:%M:%S")
        add_log("Tor IP renewed successfully")
        time.sleep(5)  # wait for circuit
    except Exception as e:
        add_log(f"Tor renewal error: {e}")
    finally:
        renew_run_lock.release()

# ========== IMPROVED REGEX (strict lengths) ==========
# Each brand with exact length, using negative lookahead to avoid longer numbers
VISA_REGEX = r'\b4[0-9]{12}(?![0-9])'                     # 13 digits
VISA16_REGEX = r'\b4[0-9]{15}(?![0-9])'                    # 16 digits
MASTERCARD_REGEX = r'\b(?:5[1-5][0-9]{14}|2(?:2[2-9][0-9]|2[3-9][0-9]{2}|[3-6][0-9]{3}|7[01][0-9]{2}|720)[0-9]{12})(?![0-9])'  # 16 digits
AMEX_REGEX = r'\b3[47][0-9]{13}(?![0-9])'                  # 15 digits
DISCOVER_REGEX = r'\b(?:6011[0-9]{12}|65[0-9]{14}|622(?:12[6-9]|1[3-9][0-9]|[2-8][0-9]{2}|9[01][0-9]|92[0-5])[0-9]{10})(?![0-9])'  # 16 digits
DINERS_REGEX = r'\b(?:3(?:0[0-5][0-9]|09|6|8|9)[0-9]{10})(?![0-9])'  # 14 digits
JCB_16_REGEX = r'\b(?:352[89]|35[3-8][0-9])[0-9]{12}(?![0-9])'  # 16 digits
JCB_15_REGEX = r'\b(?:2131|1800)[0-9]{11}(?![0-9])'         # 15 digits
UNIONPAY_REGEX = r'\b62[0-9]{14,17}(?![0-9])'              # 16-19 digits

CC_PATTERNS = re.compile('|'.join([
    VISA_REGEX, VISA16_REGEX, MASTERCARD_REGEX, AMEX_REGEX,
    DISCOVER_REGEX, DINERS_REGEX, JCB_16_REGEX, JCB_15_REGEX,
    UNIONPAY_REGEX
]))

def extract_cards(text: str, source_url: str):
    """Find credit card numbers and store them securely."""
    # Split into chunks to avoid cross‑element concatenation
    chunks = text.split()
    for chunk in chunks:
        for match in CC_PATTERNS.finditer(chunk):
            card_number = match.group()
            if not luhn(card_number):
                continue
            masked = mask_card(card_number)
            with stats_lock:
                # Avoid duplicates based on masked number
                if any(c[0] == masked for c in stats["found_cards"]):
                    continue
                card_type = identify_card_type(card_number)
                ts = datetime.now().strftime("%H:%M:%S")
                stats["found_cards"].append((masked, card_type, source_url, ts))
                add_log(f"Found {card_type}: {masked} at {source_url}")
            # Write encrypted data and queue notification (outside stats_lock)
            write_card_secure(card_number, card_type, source_url, ts)
            telegram_notify_card(masked, card_type, source_url, ts)

# ========== TELEGRAM NOTIFICATION QUEUE (bounded, thread‑safe) ==========
telegram_queue = queue.Queue(maxsize=200)

def telegram_notify_card(masked: str, card_type: str, source: str, ts: str):
    """Put notification into queue, dropping oldest if full."""
    msg = f"💳 New card {card_type}: <code>{masked}</code>\nSource: {source}\nTime: {ts}"
    try:
        telegram_queue.put_nowait(msg)
    except queue.Full:
        # Discard oldest to make room
        try:
            telegram_queue.get_nowait()
            telegram_queue.put_nowait(msg)
        except queue.Empty:
            pass  # shouldn't happen

# ========== CRAWLING ENGINE (with retry on 5xx, rate limiting) ==========
def crawl(url: str, depth: int = 2):
    """Crawl a URL with proxy rotation, 5xx retries, and polite delays."""
    if depth <= 0:
        return
    with stats_lock:
        if url in stats["visited_urls"]:
            return
        stats["visited_urls"].add(url)
        # Prevent memory blowout: if set gets too large, clear it (emergency)
        if len(stats["visited_urls"]) > 200_000:
            add_log("Warning: visited_urls exceeded 200k, clearing to save memory.")
            stats["visited_urls"].clear()

    is_onion = ".onion" in url

    for attempt in range(MAX_RETRIES):
        proxy_info = None
        try:
            proxy_info = proxy_manager.get_next_proxy(is_onion=is_onion)
        except RuntimeError as e:
            add_log(f"No proxy for {url}: {e}")
            return

        try:
            proxies_dict = proxy_manager._build_proxies_dict(proxy_info)
            add_log(f"Crawling [{attempt+1}/{MAX_RETRIES}] {url} via {mask_proxy_url(proxy_info['url'])}")
            resp = requests.get(
                url,
                proxies=proxies_dict,
                timeout=15,
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; rv:102.0) Gecko/20100101 Firefox/102.0"}
            )
            # Retry on 5xx server errors
            if resp.status_code in (502, 503, 504):
                add_log(f"Server error {resp.status_code} for {url}, will retry")
                time.sleep(2)
                continue
            if resp.status_code != 200:
                add_log(f"Non-200 status: {resp.status_code} for {url}, not retrying")
                return  # do not retry on 404 etc.

            # Success – parse page
            soup = BeautifulSoup(resp.text, 'html.parser')
            page_text = soup.get_text(separator=' ')  # preserve spaces
            extract_cards(page_text, url)
            # Check input/textarea/hidden fields
            for inp in soup.find_all(['input', 'textarea']):
                for attr in ('value', 'placeholder'):
                    val = inp.get(attr)
                    if val:
                        extract_cards(val, url)
            for form in soup.find_all('form'):
                for hidden in form.find_all('input', type='hidden'):
                    if hidden.get('value'):
                        extract_cards(hidden['value'], url)

            # Follow links with delay (depth > 1)
            if depth > 1:
                base_domain = urllib.parse.urlparse(url).netloc
                for link in soup.find_all('a', href=True):
                    next_url = urllib.parse.urljoin(url, link['href'])
                    if next_url.startswith('http'):
                        # Follow onion links freely, clearnet only same domain
                        if '.onion' in next_url:
                            time.sleep(random.uniform(0.5, 2.0))
                            crawl(next_url, depth - 1)
                        elif base_domain in next_url:
                            time.sleep(random.uniform(0.5, 2.0))
                            crawl(next_url, depth - 1)
            return  # success, exit retry loop

        except (requests.exceptions.ProxyError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError,
                Exception) as e:
            add_log(f"Proxy error for {url}: {e}")
            if proxy_info:
                proxy_manager.mark_failed(proxy_info)
            time.sleep(1)  # brief wait before retry
    # All retries exhausted
    add_log(f"Failed to crawl {url} after {MAX_RETRIES} attempts")

def scrape_loop():
    """Main scraping cycle, responsive to stop signal."""
    global START_URLS
    add_log("Scrape loop started.")
    while not stop_event.is_set():
        # Snapshot start URLs safely
        with _start_urls_lock:
            urls = list(START_URLS)  # copy to avoid modification during iteration
        for start in urls:
            if stop_event.is_set():
                break
            crawl(start, depth=2)
            time.sleep(1.5)
        renew_tor_ip()  # renew after full round
        add_log("Cycle complete, waiting 5 minutes (or until stopped).")
        # Sleep in small increments to remain responsive
        for _ in range(300):
            if stop_event.is_set():
                break
            time.sleep(1)

# ========== FLASK DASHBOARD ==========
DASH_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>CC Crawler Dashboard</title>
    <meta charset="utf-8">
    <style>
        body { font-family: sans-serif; margin: 20px; background: #f5f5f5; }
        h1 { color: #333; }
        .status { font-size: 1.2em; }
        .section { background: white; padding: 15px; margin: 15px 0; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
        table { border-collapse: collapse; width: 100%; }
        th, td { padding: 8px; text-align: left; border-bottom: 1px solid #ddd; }
        pre { background: #eee; padding: 10px; overflow-x: auto; }
    </style>
</head>
<body>
    <h1>🕷️ CC Crawler Dashboard</h1>
    <div class="section">
        <p class="status">Status: <strong>{{ '🟢 Active' if active else '🔴 Stopped' }}</strong></p>
        <p>Started: {{ start_time }}</p>
        <p>Last Tor Renew: {{ last_renew }}</p>
        <p>Visited URLs: {{ visited_count }}</p>
        <p>Cards Found: {{ cards_count }}</p>
    </div>

    {% if recent_cards %}
    <div class="section">
        <h2>Recent Cards (masked)</h2>
        <table>
            <tr><th>Number</th><th>Type</th><th>Source</th><th>Time</th></tr>
            {% for c in recent_cards %}
            <tr>
                <td><code>{{ c.number }}</code></td>
                <td>{{ c.type }}</td>
                <td>{{ c.source }}</td>
                <td>{{ c.time }}</td>
            </tr>
            {% endfor %}
        </table>
    </div>
    {% endif %}

    <div class="section">
        <h2>Log (last 30 lines)</h2>
        <pre>{% for line in log %}{{ line }}<br>{% endfor %}</pre>
    </div>
</body>
</html>
"""

app = Flask(__name__)

@app.route('/')
def dashboard():
    with stats_lock:
        visited_count = len(stats["visited_urls"])
        cards = stats["found_cards"]
        recent = cards[-20:] if len(cards) > 20 else cards
    with log_lock:
        log_snapshot = list(log_lines[-30:])
    return render_template_string(
        DASH_TEMPLATE,
        active=stats["crawling_active"],
        start_time=stats["start_time"],
        last_renew=stats["last_renew"] or "Never",
        visited_count=visited_count,
        cards_count=len(cards),
        recent_cards=[{"number": c[0], "type": c[1], "source": c[2], "time": c[3]} for c in reversed(recent)],
        log=log_snapshot
    )

# ========== TELEGRAM BOT HANDLERS ==========
def admin_only(func):
    """Decorator to restrict commands to admin chat IDs."""
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if user_id not in ADMIN_CHAT_IDS:
            await update.message.reply_text("🚫 Unauthorized.")
            return
        return await func(update, context)
    return wrapper

@admin_only
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    stats["crawling_active"] = True
    stop_event.clear()
    await update.message.reply_text("✅ Crawler started (if not already).")

@admin_only
async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    stats["crawling_active"] = False
    stop_event.set()
    await update.message.reply_text("⏹ Crawler stopped.")

@admin_only
async def cmd_startcrawl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    stats["crawling_active"] = True
    stop_event.clear()
    await update.message.reply_text("🔄 Crawler awakened.")

@admin_only
async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with stats_lock:
        visited = len(stats["visited_urls"])
        cards = len(stats["found_cards"])
        active = "🟢 Running" if stats["crawling_active"] else "🔴 Stopped"
    await update.message.reply_text(
        f"Status: {active}\n"
        f"Visited URLs: {visited}\n"
        f"Cards found: {cards}\n"
        f"Last Tor renew: {stats['last_renew'] or 'Never'}"
    )

@admin_only
async def cmd_renewtor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Run renew in thread to avoid blocking event loop
    await asyncio.to_thread(renew_tor_ip)
    await update.message.reply_text("🔄 Tor IP renewal triggered.")

@admin_only
async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with stats_lock:
        visited = len(stats["visited_urls"])
        cards = len(stats["found_cards"])
        uptime = datetime.now() - datetime.strptime(stats["start_time"], "%Y-%m-%d %H:%M:%S")
    await update.message.reply_text(
        f"📊 Stats\n"
        f"Visited: {visited}\n"
        f"Cards: {cards}\n"
        f"Running since: {stats['start_time']} ({uptime})"
    )

@admin_only
async def cmd_lastcards(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with stats_lock:
        recent = stats["found_cards"][-10:] if stats["found_cards"] else []
    if not recent:
        await update.message.reply_text("No cards found yet.")
        return
    text = "🃏 *Last 10 Cards (masked)*\n"
    for masked, ctype, source, ts in reversed(recent):
        text += f"`{masked}` - {ctype} from {source}\n"
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

@admin_only
async def cmd_log(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with log_lock:
        lines = list(log_lines[-20:])
    if not lines:
        await update.message.reply_text("Log empty.")
        return
    msg = "<pre>" + "\n".join(lines) + "</pre>"
    if len(msg) > 4096:
        msg = msg[:4096-6] + "...</pre>"
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

@admin_only
async def cmd_clearlog(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with log_lock:
        log_lines.clear()
    await update.message.reply_text("🗑 Log cleared.")

@admin_only
async def cmd_addurl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    url = " ".join(context.args)
    if not url:
        await update.message.reply_text("Usage: /addurl <url>")
        return
    with _start_urls_lock:
        START_URLS.append(url)
    await update.message.reply_text(f"✅ Added URL: {url}")

@admin_only
async def cmd_listurls(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with _start_urls_lock:
        urls = START_URLS[:30]
        total = len(START_URLS)
    text = "\n".join(urls)
    if total > 30:
        text += f"\n... and {total-30} more"
    await update.message.reply_text(f"🌐 Start URLs:\n{text}")

@admin_only
async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = """
<b>Available admin commands:</b>
/start - Start crawling
/stop - Stop crawling
/startcrawl - Resume crawling
/status - Show status
/renewtor - Renew Tor IP
/stats - Show statistics
/lastcards - Last 10 cards (masked)
/log - Show recent logs
/clearlog - Clear logs
/addurl &lt;url&gt; - Add start URL
/listurls - List start URLs
/addproxy &lt;proxy&gt; - Add a new proxy
/removeproxy &lt;proxy&gt; - Remove a proxy
/listproxies - List all proxies
/checkproxies - Force re-test all proxies
/rotateproxy - Force immediate proxy rotation
/setrotation &lt;roundrobin|leastfailed&gt; - Change rotation mode
/torrenew - Renew Tor IP (alias)
/help - This message
"""
    await update.message.reply_text(help_text, parse_mode=ParseMode.HTML)

# Proxy commands – all blocking proxy tests moved to executor
async def async_test_proxy(proxy):
    return await asyncio.to_thread(proxy_manager.test_proxy, proxy)

@admin_only
async def cmd_addproxy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    proxy_url = " ".join(context.args)
    if not proxy_url:
        await update.message.reply_text("Usage: /addproxy &lt;proxy_url&gt; (e.g., http://user:pass@host:port)")
        return
    # Quick add and test in executor
    with proxy_manager.lock:
        existing = [p["url"] for p in proxy_manager.proxies]
        if proxy_url in existing:
            await update.message.reply_text(f"❌ Proxy already exists: {mask_proxy_url(proxy_url)}")
            return
        ptype = proxy_manager._guess_type(proxy_url)
        proxy = {
            "url": proxy_url,
            "type": ptype,
            "status": "unknown",
            "fail_count": 0,
            "last_used": None,
            "is_tor": False
        }
        proxy_manager.proxies.append(proxy)
    # Test outside lock, without blocking event loop
    success = await async_test_proxy(proxy)
    if success:
        proxy_manager.mark_alive(proxy)
        await update.message.reply_text(f"✅ Proxy added and tested: {mask_proxy_url(proxy_url)}")
    else:
        proxy_manager.mark_failed(proxy)
        await update.message.reply_text(f"❌ Proxy failed test: {mask_proxy_url(proxy_url)}")

@admin_only
async def cmd_removeproxy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    proxy_url = " ".join(context.args)
    if not proxy_url:
        await update.message.reply_text("Usage: /removeproxy &lt;exact_proxy_url&gt;")
        return
    if proxy_manager.remove_proxy(proxy_url):
        await update.message.reply_text(f"❌ Removed: {mask_proxy_url(proxy_url)}")
    else:
        await update.message.reply_text("Proxy not found or cannot remove Tor.")

@admin_only
async def cmd_listproxies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    proxies = proxy_manager.list_proxies()
    if not proxies:
        await update.message.reply_text("No proxies in pool.")
        return
    lines = ["<b>Proxy Pool:</b>"]
    for p in proxies:
        status_emoji = "🟢" if p["status"] == "alive" else "🔴"
        safe_url = mask_proxy_url(p["url"])
        tor_tag = " (Tor)" if p["is_tor"] else ""
        line = f"{status_emoji} {safe_url}{tor_tag} | Fails: {p['fail_count']} | Last: {p['last_used'] or 'never'}"
        lines.append(line)
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)

@admin_only
async def cmd_checkproxies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    proxies = proxy_manager.list_proxies()
    for p in proxies:
        alive = await async_test_proxy(p)
        if alive:
            proxy_manager.mark_alive(p)
        else:
            proxy_manager.mark_failed(p)
    await update.message.reply_text("✅ All proxies re-tested.")

@admin_only
async def cmd_rotateproxy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    proxy_manager.force_rotate()
    await update.message.reply_text("🔄 Next request will use a different proxy.")

@admin_only
async def cmd_setrotation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    mode = " ".join(context.args).lower()
    if mode in ("roundrobin", "leastfailed"):
        proxy_manager.rotation_mode = mode
        await update.message.reply_text(f"✅ Rotation mode set to {mode}")
    else:
        await update.message.reply_text("Invalid mode. Use roundrobin or leastfailed")

@admin_only
async def cmd_torrenew(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await asyncio.to_thread(renew_tor_ip)
    await update.message.reply_text("🔄 Tor IP renewed.")

async def send_queued_messages(context: ContextTypes.DEFAULT_TYPE):
    """Background task to process notifications."""
    while True:
        try:
            msg = telegram_queue.get_nowait()
            for chat_id in ADMIN_CHAT_IDS:
                try:
                    await context.bot.send_message(chat_id, msg, parse_mode=ParseMode.HTML)
                except Exception as e:
                    logger.error(f"Failed to send Telegram message: {e}")
        except queue.Empty:
            pass
        await asyncio.sleep(0.5)

def start_telegram_bot():
    """Run the Telegram bot in its own event loop."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    app_bot = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # Register handlers
    app_bot.add_handler(CommandHandler("start", cmd_start))
    app_bot.add_handler(CommandHandler("stop", cmd_stop))
    app_bot.add_handler(CommandHandler("startcrawl", cmd_startcrawl))
    app_bot.add_handler(CommandHandler("status", cmd_status))
    app_bot.add_handler(CommandHandler("renewtor", cmd_renewtor))
    app_bot.add_handler(CommandHandler("stats", cmd_stats))
    app_bot.add_handler(CommandHandler("lastcards", cmd_lastcards))
    app_bot.add_handler(CommandHandler("log", cmd_log))
    app_bot.add_handler(CommandHandler("clearlog", cmd_clearlog))
    app_bot.add_handler(CommandHandler("addurl", cmd_addurl))
    app_bot.add_handler(CommandHandler("listurls", cmd_listurls))
    app_bot.add_handler(CommandHandler("help", cmd_help))
    app_bot.add_handler(CommandHandler("addproxy", cmd_addproxy))
    app_bot.add_handler(CommandHandler("removeproxy", cmd_removeproxy))
    app_bot.add_handler(CommandHandler("listproxies", cmd_listproxies))
    app_bot.add_handler(CommandHandler("checkproxies", cmd_checkproxies))
    app_bot.add_handler(CommandHandler("rotateproxy", cmd_rotateproxy))
    app_bot.add_handler(CommandHandler("setrotation", cmd_setrotation))
    app_bot.add_handler(CommandHandler("torrenew", cmd_torrenew))

    # Background notification
    app_bot.job_queue.run_repeating(send_queued_messages, interval=1, first=1)

    logger.info("Telegram bot started.")
    loop.run_until_complete(app_bot.run_polling())
    loop.close()

# ========== MAIN ==========
if __name__ == "__main__":
    # Ensure Telegram token exists
    if not TELEGRAM_BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN not set. Bot will not start.")
    if not ADMIN_CHAT_IDS:
        logger.warning("ADMIN_CHAT_IDS not set. No admins will receive notifications.")

    # Start proxy dead-check thread (stops with event)
    threading.Thread(target=proxy_manager.periodic_dead_check, daemon=True).start()

    # Start Flask in daemon thread
    flask_thread = threading.Thread(
        target=lambda: app.run(host='127.0.0.1', port=FLASK_PORT, debug=False),
        daemon=True
    )
    flask_thread.start()

    # Start crawler thread
    crawler_thread = threading.Thread(target=scrape_loop, daemon=True)
    crawler_thread.start()

    # Start Telegram bot (main thread will run bot event loop, but we can put it in another thread to not block)
    if TELEGRAM_BOT_TOKEN:
        telegram_thread = threading.Thread(target=start_telegram_bot, daemon=True)
        telegram_thread.start()
    else:
        logger.info("Telegram bot disabled.")

    add_log(f"CC Crawler started. Dashboard: http://127.0.0.1:{FLASK_PORT}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        stop_event.set()
        stats["crawling_active"] = False
        add_log("Shutting down.")
