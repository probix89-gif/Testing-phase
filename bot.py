"""
╔══════════════════════════════════════════════════════════╗
║   DORK PARSER BOT v18.1 — STEALTH PARALLEL ARCHITECTURE ║
║   curl_cffi TLS fingerprint spoofing (chrome110)        ║
║   Parallel page fetching per dork (asyncio.gather)      ║
║   Full browser header rotation | Dynamic adaptive delay  ║
║   Proxy rotation (HTTP/SOCKS) | Yahoo + DuckDuckGo       ║
║   CAPTCHA detection hook | DNS caching via libcurl       ║
║   Chunked architecture | Global dedup | Tor rotation     ║
║   Manual proxy management: /addproxy /removeproxy       ║
║   /proxylist /testproxy | Per-chunk proxy fallback      ║
╚══════════════════════════════════════════════════════════╝
"""

import asyncio
import random
import re
import os
import time
import logging
import tempfile
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlparse, parse_qs, unquote, urlencode

# [CHANGED] curl_cffi replaces aiohttp for TLS fingerprint spoofing
from curl_cffi.requests import AsyncSession
from curl_cffi import CurlError

from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters
)

load_dotenv()

# ─── LOGGING ────────────────────────────────────────────────────────────────
Path("logs").mkdir(exist_ok=True)
log_file = f"logs/bot_{datetime.now().strftime('%Y%m%d')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ─── CONFIGURATION ──────────────────────────────────────────────────────────
BOT_TOKEN             = os.environ.get("BOT_TOKEN", "")
N_CHUNKS              = int(os.environ.get("N_CHUNKS", 2))
WORKERS_PER_CHUNK     = int(os.environ.get("WORKERS_PER_CHUNK", 8))
MAX_WORKERS_PER_CHUNK = 20
# [CHANGED] Fast mode delays: reduced to 0.5–1.0s after 3 consecutive hits
MIN_DELAY             = float(os.environ.get("MIN_DELAY", 1.5))
MAX_DELAY             = float(os.environ.get("MAX_DELAY", 3.0))
FAST_MIN_DELAY        = 0.5   # used after 3 consecutive successful fetches
FAST_MAX_DELAY        = 1.0
FAST_STREAK_THRESHOLD = 3     # hits needed to switch to fast mode
MAX_RESULTS           = int(os.environ.get("MAX_RESULTS", 10))
TOR_PROXY             = os.environ.get("TOR_PROXY", "socks5://127.0.0.1:9050")
OUTPUT_DIR            = Path("results")
OUTPUT_DIR.mkdir(exist_ok=True)

ENGINES   = ["yahoo", "duckduckgo"]
MAX_PAGES = 70

# ─── RELIABILITY CONSTANTS ───────────────────────────────────────────────────
WORKER_FETCH_TIMEOUT = 120
JOB_TIMEOUT          = 30 * 60
MAX_RETRIES          = 3
CHUNK_STALL_TIMEOUT  = 60.0
EMPTY_RATE_SLOWDOWN  = 0.50
EMPTY_RATE_RECOVER   = 0.30
CHUNK_STAGGER_DELAY  = (0.8, 2.5)

DEFAULT_SESSION = {
    "workers":     WORKERS_PER_CHUNK,
    "chunks":      N_CHUNKS,
    "engines":     list(ENGINES),
    "max_results": MAX_RESULTS,
    "pages":       [1],
    "tor":         False,
    "min_score":   30,
}

user_sessions:   dict = {}
active_jobs:     dict = {}
active_stop_evs: dict = {}   # chat_id → global_stop_ev for graceful /stop


# ══════════════════════════════════════════════════════════════════════════════
# ─── PROXY MANAGEMENT (v18.1 ENHANCED) ───────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

# [NEW] PROXY_ENABLED — global on/off switch independent of pool contents.
#       Set PROXY_ENABLED=false in .env to disable proxies even if pool is loaded.
PROXY_ENABLED: bool = os.environ.get("PROXY_ENABLED", "true").lower() not in ("false", "0", "no")

# [NEW] Asyncio lock for safe in-memory pool mutations from concurrent commands
_proxy_pool_lock: asyncio.Lock = asyncio.Lock()

# [NEW] Regex for validating proxy URLs: http/https/socks5 with optional auth
_PROXY_URL_RE = re.compile(
    r'^(https?|socks5?)://(?:[^:@/\s]+:[^:@/\s]+@)?[\w\-\.]+:\d{1,5}/?$',
    re.IGNORECASE,
)


def _validate_proxy_url(proxy_url: str) -> bool:
    """
    [NEW] Return True if proxy_url is a valid http/https/socks5 proxy URL.
    Accepts:
      socks5://user:pass@host:port
      http://host:port
      https://host:port
    """
    return bool(_PROXY_URL_RE.match(proxy_url.strip()))


def _parse_proxy_info(proxy_url: str) -> dict:
    """
    [NEW] Parse a proxy URL into its components for display.
    Returns dict with keys: protocol, host, port, auth (bool).
    """
    try:
        parsed = urlparse(proxy_url.strip())
        return {
            "protocol": parsed.scheme.upper() if parsed.scheme else "?",
            "host":     parsed.hostname or "?",
            "port":     parsed.port or "?",
            "auth":     bool(parsed.username),
        }
    except Exception:
        return {"protocol": "?", "host": str(proxy_url)[:30], "port": "?", "auth": False}


def _persist_proxies() -> None:
    """
    [NEW] Write the current in-memory _proxy_pool to proxies.txt.
    Called after every /addproxy or /removeproxy operation.
    """
    try:
        with open("proxies.txt", "w", encoding="utf-8") as f:
            f.write("# Proxy list — managed by /addproxy and /removeproxy\n")
            f.write(f"# Last updated : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"# Total        : {len(_proxy_pool)}\n")
            for p in _proxy_pool:
                f.write(p + "\n")
        log.info(f"[PROXY] Persisted {len(_proxy_pool)} proxies to proxies.txt")
    except Exception as exc:
        log.warning(f"[PROXY] Failed to persist proxies.txt: {exc}")


def _load_proxies() -> list:
    """
    Load proxy list from PROXY_LIST env var (comma-separated) or proxies.txt file.
    Each proxy must be in format: http://host:port or socks5://user:pass@host:port
    """
    proxies = []
    env_list = os.environ.get("PROXY_LIST", "").strip()
    if env_list:
        proxies = [p.strip() for p in env_list.split(",") if p.strip()]
        log.info(f"[PROXY] Loaded {len(proxies)} proxies from PROXY_LIST env var")
        return proxies

    proxy_file = Path("proxies.txt")
    if proxy_file.exists():
        with open(proxy_file, encoding="utf-8") as f:
            proxies = [
                line.strip() for line in f
                if line.strip() and not line.startswith("#")
            ]
        log.info(f"[PROXY] Loaded {len(proxies)} proxies from proxies.txt")
    return proxies


_proxy_pool: list = _load_proxies()


def _get_random_proxy(exclude: str | None = None) -> str | None:
    """
    [CHANGED] Return a random proxy from the pool.
    Respects PROXY_ENABLED global flag.
    Optionally excludes a specific proxy URL (used for fallback rotation
    so we don't retry with the same proxy that just failed).
    """
    if not PROXY_ENABLED or not _proxy_pool:
        return None
    candidates = [p for p in _proxy_pool if p != exclude] if exclude else list(_proxy_pool)
    if not candidates:
        # Only one proxy in pool and it's excluded — fall back to it anyway
        return _proxy_pool[0] if _proxy_pool else None
    return random.choice(candidates)


def _is_proxy_error(exc: Exception) -> bool:
    """
    [NEW] Heuristic: detect if a CurlError is caused by a proxy failure
    rather than a network/server issue, so we know to rotate to a new proxy.
    """
    msg = str(exc).lower()
    proxy_keywords = (
        "proxy", "tunnel", "407", "socks", "authentication",
        "connection refused", "network unreachable", "no route to host",
        "could not connect to proxy", "unable to connect to proxy",
        "recv failure", "ssl handshake", "timed out",
    )
    return any(kw in msg for kw in proxy_keywords)


# ══════════════════════════════════════════════════════════════════════════════


# ─── TOR ROTATION ───────────────────────────────────────────────────────────
_tor_rotation_task = None
tor_enabled_users  = 0

async def rotate_tor_identity() -> None:
    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", 9051)
        await reader.readuntil(b"250 ")
        writer.write(b'AUTHENTICATE ""\r\n')
        await writer.drain()
        resp = await reader.readuntil(b"250 ")
        if b"250" not in resp:
            log.warning("Tor authentication failed")
            writer.close()
            return
        writer.write(b"SIGNAL NEWNYM\r\n")
        await writer.drain()
        resp = await reader.readuntil(b"250 ")
        log.info("Tor IP rotated") if b"250" in resp else log.warning("Tor rotation failed")
        writer.close()
        await writer.wait_closed()
    except Exception as exc:
        log.warning(f"Tor rotation error: {exc}")

async def _tor_rotation_loop() -> None:
    while tor_enabled_users > 0:
        await rotate_tor_identity()
        await asyncio.sleep(120)

def start_tor_rotation() -> None:
    global _tor_rotation_task
    if _tor_rotation_task is None or _tor_rotation_task.done():
        _tor_rotation_task = asyncio.create_task(_tor_rotation_loop())
        log.info("Tor rotation task started")

def stop_tor_rotation() -> None:
    global _tor_rotation_task
    if _tor_rotation_task and not _tor_rotation_task.done():
        _tor_rotation_task.cancel()
        _tor_rotation_task = None
        log.info("Tor rotation task stopped")


# ─── SQL FILTER ENGINE ───────────────────────────────────────────────────────
BLACKLISTED_DOMAINS = {
    "yahoo.uservoice.com", "uservoice.com", "bing.com", "google.com", "googleapis.com",
    "gstatic.com", "youtube.com", "facebook.com", "instagram.com", "twitter.com", "x.com",
    "linkedin.com", "pinterest.com", "reddit.com", "wikipedia.org", "amazon.com",
    "amazon.co", "ebay.com", "shopify.com", "wordpress.com", "blogspot.com", "medium.com",
    "github.com", "stackoverflow.com", "w3schools.com", "microsoft.com", "apple.com",
    "cloudflare.com", "yahoo.com", "msn.com", "live.com", "outlook.com", "mercadolibre.com",
    "aliexpress.com", "alibaba.com", "etsy.com", "walmart.com", "bestbuy.com",
    "capitaloneshopping.com", "onetonline.org", "moodle.", "lyrics.fi", "verkkouutiset.fi",
    "iltalehti.fi", "sapo.pt", "iol.pt", "idealo.", "zalando.", "trovaprezzi.",
    # explicitly blocked per user request
    "whatsapp.com",
}

SQL_HIGH_PARAMS = {
    "id", "uid", "user_id", "userid", "pid", "product_id", "productid",
    "cid", "cat_id", "catid", "category_id", "aid", "article_id",
    "nid", "news_id", "bid", "blog_id", "sid", "fid", "forum_id",
    "tid", "topic_id", "mid", "msg_id", "oid", "order_id",
    "rid", "page_id", "item_id", "itemid", "post_id", "gid",
    "lid", "vid", "did", "doc_id",
}

SQL_MED_PARAMS = {
    "q", "query", "search", "name", "username", "email",
    "page", "p", "type", "action", "do", "module",
    "view", "mode", "from", "date", "code", "ref",
    "file", "path", "url", "data", "value", "param",
    "price", "tag", "section", "content", "lang",
}

VULN_EXTENSIONS = {".php", ".asp", ".aspx", ".cfm", ".jsf", ".do", ".cgi", ".pl", ".jsp"}

_JUNK_RE = re.compile(
    r"aclick\?|uservoice\.com|utm_source=|"
    r"\.pdf$|\.jpg$|\.jpeg$|\.png$|\.gif$|\.webp$|\.avif$|"
    r"\.svg$|\.ico$|\.css$|\.js$|\.mp4$|\.mp3$|\.zip$|"
    r"/static/|/assets/|/images/|/img/|/fonts/|/media/|/cdn-cgi/|"
    r"/wp-content/uploads/",
    re.IGNORECASE,
)


def score_url(url: str) -> int:
    try:
        parsed = urlparse(url)
    except Exception:
        return 0
    if not url.startswith("http"):
        return 0
    domain = parsed.netloc.lower()
    for bd in BLACKLISTED_DOMAINS:
        if bd in domain:
            return 0
    if _JUNK_RE.search(url):
        return 0

    query        = parsed.query
    path         = parsed.path.lower()
    has_vuln_ext = any(path.endswith(ext) for ext in VULN_EXTENSIONS)

    if not query:
        return 25 if has_vuln_ext else 5

    score  = 15
    params = parse_qs(query, keep_blank_values=True)
    pkeys  = {k.lower() for k in params}

    if has_vuln_ext:
        score += 20
    score += len(pkeys & SQL_HIGH_PARAMS) * 15
    score += len(pkeys & SQL_MED_PARAMS)  * 5

    for vals in params.values():
        for v in vals:
            if v.isdigit():
                score += 10
                break

    if len(url) > 300:
        score -= 10
    elif len(url) > 200:
        score -= 5
    if len(params) > 8:
        score -= 5

    return max(0, min(score, 100))


def filter_scored(urls: list, min_score: int) -> list:
    result = [(score_url(u), u) for u in urls]
    result = [(s, u) for s, u in result if s >= min_score]
    result.sort(reverse=True)
    return result


# ─── URL CLEANER MODULE ──────────────────────────────────────────────────────
MAX_URL_LENGTH = 200


def extract_domain(url: str) -> str:
    try:
        netloc = urlparse(url).netloc.lower()
        return netloc[4:] if netloc.startswith("www.") else netloc
    except Exception:
        return ""


def is_blocked(domain: str) -> bool:
    for bd in BLACKLISTED_DOMAINS:
        if bd in domain:
            return True
    return False


def has_query_params(url: str) -> bool:
    try:
        return bool(urlparse(url).query)
    except Exception:
        return False


def is_valid_url(url: str) -> bool:
    try:
        p = urlparse(url)
        return p.scheme in ("http", "https") and bool(p.netloc)
    except Exception:
        return False


def filter_urls(urls: list) -> dict:
    total       = len(urls)
    rm_invalid  = 0
    rm_blocked  = 0
    rm_no_query = 0
    rm_too_long = 0
    seen        = set()
    kept        = []

    for url in urls:
        url = url.strip()
        if not url or url.startswith("#"):
            rm_invalid += 1
            continue
        if not is_valid_url(url):
            rm_invalid += 1
            continue
        if len(url) > MAX_URL_LENGTH:
            rm_too_long += 1
            continue
        domain = extract_domain(url)
        if is_blocked(domain):
            rm_blocked += 1
            continue
        if not has_query_params(url):
            rm_no_query += 1
            continue
        if url in seen:
            continue
        seen.add(url)
        kept.append(url)

    return {
        "total":       total,
        "kept":        kept,
        "rm_invalid":  rm_invalid,
        "rm_blocked":  rm_blocked,
        "rm_no_query": rm_no_query,
        "rm_too_long": rm_too_long,
        "duplicates":  total - rm_invalid - rm_blocked - rm_no_query - rm_too_long - len(kept),
    }


async def process_chunk_urls(
    chunk: list,
    semaphore: asyncio.Semaphore,
    stop_ev: asyncio.Event,
) -> list:
    async with semaphore:
        if stop_ev.is_set():
            return []
        await asyncio.sleep(0)
        return filter_urls(chunk)["kept"]


async def run_url_clean_job(chat_id: int, raw_lines: list, context) -> None:
    CLEAN_CHUNK_SIZE = 500
    MAX_CONCURRENT   = 4

    stop_ev = asyncio.Event()
    active_stop_evs[chat_id] = stop_ev

    total_input = len(raw_lines)
    status_msg  = await context.bot.send_message(
        chat_id,
        f"🧹 URL CLEANER STARTED\n"
        f"{'━'*30}\n"
        f"📥 Input   : {total_input} URLs\n"
        f"🔍 Filters : blocked domains, no-query, >200 chars, invalid\n"
        f"⚡ Workers : {MAX_CONCURRENT} parallel chunks\n"
        f"{'━'*30}\n⏳ Processing...",
    )

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    chunks    = [
        raw_lines[i : i + CLEAN_CHUNK_SIZE]
        for i in range(0, total_input, CLEAN_CHUNK_SIZE)
    ]

    tasks = [
        asyncio.create_task(process_chunk_urls(chunk, semaphore, stop_ev))
        for chunk in chunks
    ]

    try:
        results = await asyncio.gather(*tasks, return_exceptions=True)
    except asyncio.CancelledError:
        stop_ev.set()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        results = []

    seen_final: set  = set()
    final_urls: list = []
    for r in results:
        if isinstance(r, list):
            for u in r:
                if u not in seen_final:
                    seen_final.add(u)
                    final_urls.append(u)

    full_stats = filter_urls(raw_lines)
    removed    = total_input - len(final_urls)
    stopped    = stop_ev.is_set()

    output_path = Path("results") / "cleaned_urls.txt"
    output_path.parent.mkdir(exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(f"# URL Cleaner — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"# Input: {total_input} | Kept: {len(final_urls)} | Removed: {removed}\n")
        f.write("─" * 60 + "\n\n")
        for u in final_urls:
            f.write(u + "\n")

    partial_tag = " (PARTIAL — stopped early)" if stopped else ""
    try:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=status_msg.message_id,
            text=(
                f"{'⏹' if stopped else '✅'} URL CLEANER DONE{partial_tag}\n"
                f"{'━'*30}\n"
                f"📥 Total input  : {total_input}\n"
                f"✅ Kept (clean) : {len(final_urls)}\n"
                f"🗑 Removed total: {removed}\n"
                f"  ├ ❌ Invalid  : {full_stats['rm_invalid']}\n"
                f"  ├ 🚫 Blocked  : {full_stats['rm_blocked']}\n"
                f"  ├ 🔗 No query : {full_stats['rm_no_query']}\n"
                f"  ├ 📏 Too long : {full_stats['rm_too_long']}\n"
                f"  └ 🔁 Dupes    : {full_stats['duplicates']}\n"
                f"{'━'*30}"
            ),
        )
    except Exception:
        pass

    if final_urls:
        with open(output_path, "rb") as f:
            await context.bot.send_document(
                chat_id, f,
                filename="cleaned_urls.txt",
                caption=(
                    f"🧹 Cleaned URLs{' (partial)' if stopped else ''}\n"
                    f"✅ {len(final_urls)} kept from {total_input} input"
                ),
            )
    else:
        await context.bot.send_message(
            chat_id,
            "⚠️ No URLs passed the filters.\n"
            "Check your file — all entries may be blocked, missing query params, or invalid.",
        )

    active_stop_evs.pop(chat_id, None)
    active_jobs.pop(chat_id, None)


# ─── BROWSER PROFILES (Full header sets) ────────────────────────────────────
# [NEW] Rotate complete realistic browser header sets per request
BROWSER_PROFILES = [
    {   # Chrome 110 / Windows
        "User-Agent":                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
        "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language":           "en-US,en;q=0.9",
        "Accept-Encoding":           "gzip, deflate, br",
        "Sec-Ch-Ua":                 '"Chromium";v="110", "Not A(Brand";v="24", "Google Chrome";v="110"',
        "Sec-Ch-Ua-Mobile":          "?0",
        "Sec-Ch-Ua-Platform":        '"Windows"',
        "Sec-Fetch-Dest":            "document",
        "Sec-Fetch-Mode":            "navigate",
        "Sec-Fetch-Site":            "none",
        "Sec-Fetch-User":            "?1",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control":             "max-age=0",
    },
    {   # Chrome 112 / macOS
        "User-Agent":                "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
        "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language":           "en-GB,en;q=0.9",
        "Accept-Encoding":           "gzip, deflate, br",
        "Sec-Ch-Ua":                 '"Chromium";v="112", "Google Chrome";v="112", "Not:A-Brand";v="99"',
        "Sec-Ch-Ua-Mobile":          "?0",
        "Sec-Ch-Ua-Platform":        '"macOS"',
        "Sec-Fetch-Dest":            "document",
        "Sec-Fetch-Mode":            "navigate",
        "Sec-Fetch-Site":            "none",
        "Sec-Fetch-User":            "?1",
        "Upgrade-Insecure-Requests": "1",
    },
    {   # Firefox 124 / Linux
        "User-Agent":                "Mozilla/5.0 (X11; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0",
        "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language":           "en-US,en;q=0.5",
        "Accept-Encoding":           "gzip, deflate, br",
        "Sec-Fetch-Dest":            "document",
        "Sec-Fetch-Mode":            "navigate",
        "Sec-Fetch-Site":            "none",
        "Sec-Fetch-User":            "?1",
        "Upgrade-Insecure-Requests": "1",
        "TE":                        "trailers",
    },
    {   # Edge 110 / Windows
        "User-Agent":                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36 Edg/110.0.1587.63",
        "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language":           "en-US,en;q=0.9",
        "Accept-Encoding":           "gzip, deflate, br",
        "Sec-Ch-Ua":                 '"Chromium";v="110", "Not A(Brand";v="24", "Microsoft Edge";v="110"',
        "Sec-Ch-Ua-Mobile":          "?0",
        "Sec-Ch-Ua-Platform":        '"Windows"',
        "Sec-Fetch-Dest":            "document",
        "Sec-Fetch-Mode":            "navigate",
        "Sec-Fetch-Site":            "none",
        "Sec-Fetch-User":            "?1",
        "Upgrade-Insecure-Requests": "1",
    },
    {   # Safari / macOS
        "User-Agent":                "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
        "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language":           "en-US,en;q=0.9",
        "Accept-Encoding":           "gzip, deflate, br",
        "Sec-Fetch-Dest":            "document",
        "Sec-Fetch-Mode":            "navigate",
        "Sec-Fetch-Site":            "none",
    },
]


def _random_headers() -> dict:
    """Return a randomly chosen complete browser header set."""
    return dict(random.choice(BROWSER_PROFILES))


# ─── SESSION FACTORY (curl_cffi) ─────────────────────────────────────────────
# [CHANGED] Replaces aiohttp ClientSession with curl_cffi AsyncSession.
# curl_cffi uses libcurl internally which provides:
#   • TLS fingerprint spoofing (impersonate="chrome110")
#   • Built-in DNS caching (equivalent to ttl_dns_cache=600)
#   • HTTP/2 keep-alive connection reuse (replaces limit=0, force_close=False)
#   • SOCKS5/HTTP proxy support natively

def _make_isolated_session(use_tor: bool = False, proxy: str | None = None) -> AsyncSession:
    """
    Return a curl_cffi AsyncSession that mimics Chrome 110's TLS fingerprint.
    Each chunk gets its own isolated session (no shared state).
    Proxy priority: Tor > explicit proxy > random from pool (if PROXY_ENABLED).

    [NEW] Stores chosen proxy on session._cur_proxy for fallback rotation logic.
    """
    chosen_proxy = None
    if use_tor:
        chosen_proxy = TOR_PROXY
    elif proxy:
        chosen_proxy = proxy
    elif PROXY_ENABLED and _proxy_pool:          # [CHANGED] respect PROXY_ENABLED
        chosen_proxy = _get_random_proxy()

    kwargs = {
        "impersonate": "chrome110",   # [CHANGED] TLS fingerprint spoofing
        "verify":      False,
        "timeout":     20,
    }
    if chosen_proxy:
        kwargs["proxy"] = chosen_proxy
        log.debug(f"[SESSION] Using proxy: {chosen_proxy}")

    sess = AsyncSession(**kwargs)
    sess._cur_proxy = chosen_proxy   # [NEW] store for per-request fallback
    return sess


def _make_fallback_session(exclude_proxy: str | None = None) -> AsyncSession:
    """
    [NEW] Create a one-shot session with a fresh random proxy, excluding
    the proxy that just failed. Used inside fetch functions on CurlError.
    Returns a session with no proxy if pool is empty or all excluded.
    """
    fb_proxy = _get_random_proxy(exclude=exclude_proxy)
    return _make_isolated_session(proxy=fb_proxy)


# ─── CAPTCHA HANDLING PLACEHOLDER ────────────────────────────────────────────
# [NEW] Hook called whenever CAPTCHA is detected in a response
async def _on_captcha_detected(engine: str, chunk_id: int, session_proxy: str | None) -> None:
    """
    CAPTCHA detection hook.
    Logs the event and applies a penalty backoff.
    In a full implementation: mark current proxy as bad, pull a fresh one,
    or signal Tor to rotate identity.
    """
    log.warning(f"[C{chunk_id}][{engine.upper()}] 🔴 CAPTCHA detected!")
    if session_proxy:
        log.info(f"[C{chunk_id}] Proxy {session_proxy} may be flagged — consider rotating")
    backoff = random.uniform(12.0, 25.0)
    log.info(f"[C{chunk_id}] CAPTCHA backoff {backoff:.1f}s")
    await asyncio.sleep(backoff)


# ─── DEGRADED RESPONSE DETECTION ─────────────────────────────────────────────
_CAPTCHA_RE = re.compile(
    r"captcha|are you a robot|unusual traffic|access denied|"
    r"verify you are human|please verify|too many requests|"
    r"blocked|forbidden|rate limit|temporarily unavailable",
    re.IGNORECASE,
)


def _is_degraded(html: str, engine: str) -> bool:
    if len(html) < 400:
        return True
    if _CAPTCHA_RE.search(html[:4096]):
        return True
    if engine == "yahoo" and 'id="results"' not in html and "searchCenterMiddle" not in html:
        return True
    if engine == "duckduckgo" and "result__a" not in html and "results--main" not in html:
        return True
    return False


def _is_captcha(html: str) -> bool:
    """Return True if the response specifically contains CAPTCHA signals."""
    return bool(_CAPTCHA_RE.search(html[:4096]))


# ─── ROBUST HTML LINK EXTRACTOR ──────────────────────────────────────────────
class _LinkExtractor(HTMLParser):
    __slots__ = ("links", "_in_cite", "_buf")

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.links: list  = []
        self._in_cite: bool = False
        self._buf:     list = []

    def handle_starttag(self, tag: str, attrs):
        if tag == "a":
            adict = dict(attrs)
            for key in ("href", "data-u"):
                val = adict.get(key, "")
                if val.startswith("http"):
                    self.links.append(val)
        elif tag == "cite":
            self._in_cite = True
            self._buf.clear()

    def handle_endtag(self, tag: str):
        if tag == "cite" and self._in_cite:
            text = "".join(self._buf).strip()
            if text.startswith("http"):
                self.links.append(text)
            self._in_cite = False
            self._buf.clear()

    def handle_data(self, data: str):
        if self._in_cite:
            self._buf.append(data)


def _extract_links(html: str) -> list:
    p = _LinkExtractor()
    try:
        p.feed(html)
    except Exception:
        pass
    return p.links


# DDG-specific link extractor (result__a class links)
_DDG_LINK_RE = re.compile(
    r'class="result__a"[^>]*href="(https?://[^"]+)"',
    re.IGNORECASE,
)
_DDG_SNIPPET_RE = re.compile(r'uddg=(https?[^&"]+)', re.IGNORECASE)


def _extract_ddg_links(html: str) -> list:
    """Extract real URLs from DuckDuckGo HTML results page."""
    links = []
    for m in _DDG_LINK_RE.finditer(html):
        links.append(unquote(m.group(1)))
    for m in _DDG_SNIPPET_RE.finditer(html):
        links.append(unquote(m.group(1)))
    return links


# ─── ENGINE NOISE FILTERS ────────────────────────────────────────────────────
_YAHOO_NOISE   = re.compile(r"yimg\.com|yahoo\.com|doubleclick\.net|googleadservices", re.IGNORECASE)
_STATIC_EXT    = re.compile(r"\.(css|js|png|jpg|jpeg|gif|svg|ico|webp|woff2?|ttf|eot)(\?|$)", re.IGNORECASE)
_YAHOO_RU_PATH = re.compile(r"/RU=([^/&]+)")
_DDG_NOISE     = re.compile(r"duckduckgo\.com|duck\.com", re.IGNORECASE)


# ─── YAHOO PAGE FETCH ────────────────────────────────────────────────────────
async def fetch_page_yahoo(
    session: AsyncSession,
    dork: str, page: int, max_res: int,
    chunk_id: int = 0,
) -> tuple:
    """
    [CHANGED] Uses curl_cffi AsyncSession.
    [CHANGED] Full browser header rotation.
    [NEW]     CAPTCHA hook.
    [NEW]     Proxy fallback on CurlError proxy failure (same pattern as Bing).
    """
    params = {
        "p":  dork,
        "b":  (page - 1) * 10 + 1,
        "pz": min(max_res, 10),
        "vl": "lang_en",
    }

    # [NEW] Proxy fallback tracking
    active_session   = session
    fallback_session = None

    try:
        for attempt in range(MAX_RETRIES):
            headers = _random_headers()
            headers["Referer"] = "https://search.yahoo.com/"
            try:
                resp    = await active_session.get(
                    "https://search.yahoo.com/search",
                    params=params,
                    headers=headers,
                    timeout=20,
                )
                status  = resp.status_code
                html    = resp.text
                size_kb = len(html) / 1024

                log.debug(f"[C{chunk_id}][YAHOO] p{page} attempt={attempt+1} status={status} size={size_kb:.1f}KB")

                if status == 429:
                    backoff = (2 ** attempt) * random.uniform(4.0, 8.0)
                    log.warning(f"[C{chunk_id}][YAHOO] p{page} rate-limited (429) — backoff {backoff:.1f}s")
                    await asyncio.sleep(backoff)
                    continue

                if status != 200:
                    log.warning(f"[C{chunk_id}][YAHOO] p{page} non-200 status={status}")
                    return [], False

                if _is_captcha(html):
                    await _on_captcha_detected("yahoo", chunk_id, getattr(active_session, "_cur_proxy", None))
                    continue

                if _is_degraded(html, "yahoo"):
                    log.warning(f"[C{chunk_id}][YAHOO] p{page} degraded ({size_kb:.1f}KB)")
                    if attempt < MAX_RETRIES - 1:
                        await asyncio.sleep((2 ** attempt) * random.uniform(2.0, 5.0))
                        continue
                    return [], True

                raw  = _extract_links(html)
                urls = []
                for u in raw:
                    if not u.startswith("http"):
                        continue
                    if "r.search.yahoo.com" in u or "/r/" in u:
                        parsed = urlparse(u)
                        qs     = parse_qs(parsed.query)
                        if "RU" in qs:
                            real = unquote(qs["RU"][0])
                            if real.startswith(("http://", "https://")):
                                u = real
                        else:
                            m = _YAHOO_RU_PATH.search(parsed.path)
                            if m:
                                real = unquote(m.group(1))
                                if real.startswith(("http://", "https://")):
                                    u = real
                    if _YAHOO_NOISE.search(u) or _STATIC_EXT.search(u):
                        continue
                    urls.append(u)

                urls = list(dict.fromkeys(urls))[:max_res]
                log.info(f"[C{chunk_id}][YAHOO] p{page} → {len(urls)} URLs (attempt={attempt+1})")
                return urls, False

            except asyncio.TimeoutError:
                backoff = (2 ** attempt) * random.uniform(2.0, 4.0)
                log.warning(f"[C{chunk_id}][YAHOO] p{page} timeout attempt={attempt+1} — retry {backoff:.1f}s")
                await asyncio.sleep(backoff)

            except CurlError as exc:
                # [NEW] Proxy fallback on proxy-type CurlError
                if _is_proxy_error(exc) and PROXY_ENABLED and len(_proxy_pool) > 1 and attempt < MAX_RETRIES - 1:
                    cur_proxy = getattr(active_session, "_cur_proxy", None)
                    log.warning(
                        f"[C{chunk_id}][YAHOO] p{page} proxy error on attempt {attempt+1} "
                        f"({exc}) — switching to fallback proxy"
                    )
                    if fallback_session is not None:
                        await fallback_session.close()
                    fallback_session = _make_fallback_session(exclude_proxy=cur_proxy)
                    active_session   = fallback_session
                    await asyncio.sleep(random.uniform(1.0, 2.5))
                    continue
                backoff = (2 ** attempt) * random.uniform(2.0, 4.0)
                log.warning(f"[C{chunk_id}][YAHOO] p{page} CurlError={exc} — retry {backoff:.1f}s")
                await asyncio.sleep(backoff)

            except Exception as exc:
                log.error(f"[C{chunk_id}][YAHOO] p{page} unexpected: {exc}")
                return [], False

        log.warning(f"[C{chunk_id}][YAHOO] p{page} all {MAX_RETRIES} attempts exhausted")
        return [], True

    finally:
        # [NEW] Close fallback session; preserve original chunk session.
        if fallback_session is not None:
            await fallback_session.close()


# ─── DUCKDUCKGO PAGE FETCH ────────────────────────────────────────────────────
# [NEW] DuckDuckGo HTML endpoint — POST-based, no JavaScript, no API key needed
async def fetch_page_duckduckgo(
    session: AsyncSession,
    dork: str, page: int, max_res: int,
    chunk_id: int = 0,
) -> tuple:
    """
    Fetch results from DuckDuckGo's HTML endpoint.
    DDG HTML does not support deep pagination; only page 1 is reliable.
    Uses POST to html.duckduckgo.com/html/.
    [NEW] Proxy fallback on CurlError proxy failure (same pattern as Bing/Yahoo).
    """
    if page > 1:
        return [], False   # DDG HTML doesn't paginate reliably past page 1

    # DDG pagination: 's' param (0 = page1, 30 = page2, etc.) — attempt if page > 1 skipped
    data = {"q": dork, "b": "", "kl": "us-en", "df": ""}

    # [NEW] Proxy fallback tracking
    active_session   = session
    fallback_session = None

    try:
        for attempt in range(MAX_RETRIES):
            headers = _random_headers()
            headers["Referer"]      = "https://duckduckgo.com/"
            headers["Origin"]       = "https://html.duckduckgo.com"
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            try:
                resp    = await active_session.post(
                    "https://html.duckduckgo.com/html/",
                    data=data,
                    headers=headers,
                    timeout=20,
                )
                status  = resp.status_code
                html    = resp.text
                size_kb = len(html) / 1024

                log.debug(f"[C{chunk_id}][DDG] p{page} attempt={attempt+1} status={status} size={size_kb:.1f}KB")

                if status == 429:
                    backoff = (2 ** attempt) * random.uniform(5.0, 10.0)
                    log.warning(f"[C{chunk_id}][DDG] p{page} rate-limited — backoff {backoff:.1f}s")
                    await asyncio.sleep(backoff)
                    continue

                if status != 200:
                    log.warning(f"[C{chunk_id}][DDG] p{page} non-200 status={status}")
                    return [], False

                if _is_captcha(html):
                    await _on_captcha_detected("duckduckgo", chunk_id, getattr(active_session, "_cur_proxy", None))
                    continue

                if _is_degraded(html, "duckduckgo"):
                    log.warning(f"[C{chunk_id}][DDG] p{page} degraded ({size_kb:.1f}KB)")
                    if attempt < MAX_RETRIES - 1:
                        await asyncio.sleep((2 ** attempt) * random.uniform(2.0, 5.0))
                        continue
                    return [], True

                raw  = _extract_ddg_links(html)
                urls = [u for u in raw if u.startswith("http") and not _DDG_NOISE.search(u) and not _STATIC_EXT.search(u)]
                urls = list(dict.fromkeys(urls))[:max_res]
                log.info(f"[C{chunk_id}][DDG] p{page} → {len(urls)} URLs (attempt={attempt+1})")
                return urls, False

            except asyncio.TimeoutError:
                backoff = (2 ** attempt) * random.uniform(2.0, 4.0)
                log.warning(f"[C{chunk_id}][DDG] p{page} timeout attempt={attempt+1} — retry {backoff:.1f}s")
                await asyncio.sleep(backoff)

            except CurlError as exc:
                # [NEW] Proxy fallback on proxy-type CurlError
                if _is_proxy_error(exc) and PROXY_ENABLED and len(_proxy_pool) > 1 and attempt < MAX_RETRIES - 1:
                    cur_proxy = getattr(active_session, "_cur_proxy", None)
                    log.warning(
                        f"[C{chunk_id}][DDG] p{page} proxy error on attempt {attempt+1} "
                        f"({exc}) — switching to fallback proxy"
                    )
                    if fallback_session is not None:
                        await fallback_session.close()
                    fallback_session = _make_fallback_session(exclude_proxy=cur_proxy)
                    active_session   = fallback_session
                    await asyncio.sleep(random.uniform(1.0, 2.5))
                    continue
                backoff = (2 ** attempt) * random.uniform(2.0, 4.0)
                log.warning(f"[C{chunk_id}][DDG] p{page} CurlError={exc} — retry {backoff:.1f}s")
                await asyncio.sleep(backoff)

            except Exception as exc:
                log.error(f"[C{chunk_id}][DDG] p{page} unexpected: {exc}")
                return [], False

        log.warning(f"[C{chunk_id}][DDG] p{page} all {MAX_RETRIES} attempts exhausted")
        return [], True

    finally:
        # [NEW] Close fallback session; preserve original chunk session.
        if fallback_session is not None:
            await fallback_session.close()


# ─── FETCH ALL PAGES (Parallel) ───────────────────────────────────────────────
# [CHANGED] Uses asyncio.gather to fetch multiple pages concurrently per dork
async def fetch_all_pages(
    session: AsyncSession,
    dork: str, engine: str,
    pages: list, max_res: int,
    chunk_id: int = 0,
) -> tuple:
    """
    [CHANGED] Fetches all pages for a dork concurrently using asyncio.gather.
    Pages are staggered with small random delays to avoid synchronized bursts.
    DDG only supports page 1 reliably; additional pages are skipped for it.
    """
    if engine == "duckduckgo":
        sorted_pages = [min(pages)]   # DDG: only first page
    else:
        sorted_pages = sorted(pages)

    fetch_fn = {
        "yahoo":      fetch_page_yahoo,
        "duckduckgo": fetch_page_duckduckgo,
    }[engine]

    # [NEW] Stagger concurrent page requests (0.1–0.4s per page index) to
    # avoid hammering the server with fully-simultaneous requests
    async def _fetch_with_stagger(page: int, idx: int) -> tuple:
        if idx > 0:
            await asyncio.sleep(random.uniform(0.1, 0.4) * idx)
        return await fetch_fn(session, dork, page, max_res, chunk_id)

    tasks   = [_fetch_with_stagger(p, i) for i, p in enumerate(sorted_pages)]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    all_urls       = []
    degraded_total = 0
    for i, res in enumerate(results):
        if isinstance(res, Exception):
            log.warning(f"[C{chunk_id}][{engine.upper()}] page gather error: {res}")
            continue
        urls, degraded = res
        if degraded:
            degraded_total += 1
        all_urls.extend(urls)

    return all_urls, degraded_total


# ─── WORKER ───────────────────────────────────────────────────────────────────
async def dork_worker(
    wid: int,
    chunk_id: int,
    queue: asyncio.Queue,
    results_q: asyncio.Queue,
    engines: list,
    pages: list,
    max_res: int,
    session: AsyncSession,
    min_score: int,
    stop_ev: asyncio.Event,
    slowdown_ev: asyncio.Event,
) -> None:
    """
    [CHANGED] Dynamic adaptive delay:
      - After FAST_STREAK_THRESHOLD consecutive successes → fast mode (0.5–1.0s)
      - On empty/degraded → revert to normal delay + exponential backoff
    """
    eidx               = wid % len(engines)
    empty_streak       = 0
    consecutive_hits   = 0   # [NEW] track consecutive successes for fast mode

    while not stop_ev.is_set():
        try:
            dork = await asyncio.wait_for(queue.get(), timeout=2.0)
        except asyncio.TimeoutError:
            continue

        engine = engines[eidx % len(engines)]
        eidx  += 1
        log.info(f"[C{chunk_id}][W{wid}][{engine.upper()}] {dork[:55]}")

        raw          = []
        degraded_cnt = 0
        try:
            raw, degraded_cnt = await asyncio.wait_for(
                fetch_all_pages(session, dork, engine, pages, max_res, chunk_id),
                timeout=WORKER_FETCH_TIMEOUT,
            )
        except asyncio.TimeoutError:
            log.warning(f"[C{chunk_id}][W{wid}] fetch_all_pages timeout: {dork[:55]}")
        except asyncio.CancelledError:
            try:
                results_q.put_nowait((dork, engine, [], 0, 0))
            except asyncio.QueueFull:
                pass
            queue.task_done()
            raise
        except Exception as exc:
            log.warning(f"[C{chunk_id}][W{wid}] fetch error: {exc}")

        scored = filter_scored(raw, min_score)
        log.info(f"[C{chunk_id}][W{wid}] raw={len(raw)} kept={len(scored)} degraded={degraded_cnt}")

        try:
            results_q.put_nowait((dork, engine, scored, len(raw), degraded_cnt))
        except asyncio.QueueFull:
            await results_q.put((dork, engine, scored, len(raw), degraded_cnt))

        queue.task_done()

        # [CHANGED] Dynamic adaptive delay
        if raw:
            consecutive_hits += 1
            empty_streak       = 0
            # Fast mode after FAST_STREAK_THRESHOLD consecutive hits
            if consecutive_hits >= FAST_STREAK_THRESHOLD:
                delay = random.uniform(FAST_MIN_DELAY, FAST_MAX_DELAY)
                log.debug(f"[C{chunk_id}][W{wid}] FAST mode delay={delay:.2f}s (streak={consecutive_hits})")
            else:
                delay = random.uniform(MIN_DELAY, MAX_DELAY)
        else:
            consecutive_hits = 0
            empty_streak    += 1
            delay = random.uniform(MIN_DELAY, MAX_DELAY)
            if empty_streak >= 3:
                extra = min(empty_streak * 2.0, 15.0)
                log.info(f"[C{chunk_id}][W{wid}] Auto-slowdown +{extra:.1f}s (empty_streak={empty_streak})")
                delay += extra

        # Chunk-level slowdown signal
        if slowdown_ev.is_set():
            delay += random.uniform(2.0, 5.0)

        await asyncio.sleep(delay)


# ─── CHUNK RUNNER ─────────────────────────────────────────────────────────────
async def run_chunk(
    chunk_id: int,
    dorks: list,
    engines: list,
    pages: list,
    max_res: int,
    use_tor: bool,
    min_score: int,
    workers_n: int,
    progress_q: asyncio.Queue,
    global_stop_ev: asyncio.Event,
    proxy: str | None = None,
) -> dict:
    """
    [CHANGED] Accepts an optional per-chunk proxy argument.
    [CHANGED] Session created via curl_cffi _make_isolated_session.
    """
    session     = _make_isolated_session(use_tor=use_tor, proxy=proxy)
    queue       = asyncio.Queue(maxsize=len(dorks) * 2)
    results_q   = asyncio.Queue(maxsize=500)
    stop_ev     = asyncio.Event()
    slowdown_ev = asyncio.Event()

    for d in dorks:
        await queue.put(d)

    total          = len(dorks)
    processed      = 0
    empty_count    = 0
    chunk_raw      = 0
    chunk_degraded = 0
    chunk_scored   = []

    log.info(f"[C{chunk_id}] Starting — {total} dorks | {workers_n} workers | engines={engines}")

    async def _watch_global() -> None:
        while not stop_ev.is_set():
            if global_stop_ev.is_set():
                stop_ev.set()
            await asyncio.sleep(0.5)

    worker_tasks = [
        asyncio.create_task(
            dork_worker(
                i, chunk_id, queue, results_q, engines, pages,
                max_res, session, min_score, stop_ev, slowdown_ev,
            )
        )
        for i in range(workers_n)
    ]
    global_watcher = asyncio.create_task(_watch_global())

    try:
        while processed < total and not stop_ev.is_set():
            try:
                dork, engine, scored, raw_cnt, deg_cnt = await asyncio.wait_for(
                    results_q.get(), timeout=CHUNK_STALL_TIMEOUT
                )
            except asyncio.TimeoutError:
                if all(t.done() for t in worker_tasks):
                    log.warning(
                        f"[C{chunk_id}] All workers done with "
                        f"{total - processed} dorks unaccounted — exiting early"
                    )
                    break
                continue

            processed      += 1
            chunk_raw      += raw_cnt
            chunk_degraded += deg_cnt

            if raw_cnt == 0:
                empty_count += 1

            chunk_scored.extend(scored)

            empty_rate = empty_count / max(processed, 1)
            if empty_rate >= EMPTY_RATE_SLOWDOWN and not slowdown_ev.is_set():
                log.warning(f"[C{chunk_id}] Empty rate {empty_rate:.0%} — enabling chunk slowdown")
                slowdown_ev.set()
            elif empty_rate < EMPTY_RATE_RECOVER and slowdown_ev.is_set():
                log.info(f"[C{chunk_id}] Empty rate recovered to {empty_rate:.0%} — disabling slowdown")
                slowdown_ev.clear()

            try:
                progress_q.put_nowait({
                    "chunk_id":  chunk_id,
                    "processed": processed,
                    "total":     total,
                    "raw":       raw_cnt,
                    "kept":      len(scored),
                })
            except asyncio.QueueFull:
                pass

        # Drain any results workers already placed in results_q before we
        # cancel them — these would otherwise be silently discarded.
        while True:
            try:
                dork, engine, scored, raw_cnt, deg_cnt = results_q.get_nowait()
                processed      += 1
                chunk_raw      += raw_cnt
                chunk_degraded += deg_cnt
                if raw_cnt == 0:
                    empty_count += 1
                chunk_scored.extend(scored)
            except asyncio.QueueEmpty:
                break

        for t in worker_tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*worker_tasks, return_exceptions=True)

    except asyncio.CancelledError:
        stop_ev.set()
        for t in worker_tasks:
            t.cancel()
        await asyncio.gather(*worker_tasks, return_exceptions=True)
        # Drain any results already in the queue before propagating
        while True:
            try:
                dork, engine, scored, raw_cnt, deg_cnt = results_q.get_nowait()
                processed      += 1
                chunk_raw      += raw_cnt
                chunk_degraded += deg_cnt
                if raw_cnt == 0:
                    empty_count += 1
                chunk_scored.extend(scored)
            except asyncio.QueueEmpty:
                break
        raise
    finally:
        global_watcher.cancel()
        await asyncio.gather(global_watcher, return_exceptions=True)
        await session.close()

    success_rate = (processed - empty_count) / max(processed, 1)
    log.info(
        f"[C{chunk_id}] Done — processed={processed}/{total} "
        f"raw={chunk_raw} kept={len(chunk_scored)} "
        f"degraded={chunk_degraded} success_rate={success_rate:.0%}"
    )

    return {
        "chunk_id":       chunk_id,
        "scored":         chunk_scored,
        "raw_count":      chunk_raw,
        "degraded_count": chunk_degraded,
        "processed":      processed,
        "empty_count":    empty_count,
    }


# ─── JOB RUNNER ───────────────────────────────────────────────────────────────
async def run_dork_job(chat_id: int, dorks: list, context) -> None:
    """
    [CHANGED] Passes a randomly assigned proxy per chunk (from pool if available).
    All existing features (chunking, scoring, filtering, Tor, stop, progress) intact.
    """
    sess      = get_session(chat_id)
    engines   = sess.get("engines", list(ENGINES))
    workers_n = min(sess.get("workers", WORKERS_PER_CHUNK), MAX_WORKERS_PER_CHUNK)
    max_res   = sess.get("max_results", MAX_RESULTS)
    pages     = sess.get("pages", [1])
    use_tor   = sess.get("tor", False)
    min_score = sess.get("min_score", 30)
    n_chunks  = max(1, sess.get("chunks", N_CHUNKS))

    total_dorks = len(dorks)
    pages_str   = ", ".join(str(p) for p in pages)
    start_time  = time.time()

    chunk_size    = max(1, -(-total_dorks // n_chunks))
    chunks        = [dorks[i : i + chunk_size] for i in range(0, total_dorks, chunk_size)]
    actual_chunks = len(chunks)

    log.info(
        f"[JOB][{chat_id}] Starting: {total_dorks} dorks → "
        f"{actual_chunks} chunks × {workers_n} workers/chunk | "
        f"delay={MIN_DELAY}–{MAX_DELAY}s (fast={FAST_MIN_DELAY}–{FAST_MAX_DELAY}s)"
    )

    tmp_file = tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", delete=False,
        prefix=f"dork_{chat_id}_", suffix=".txt",
    )
    tmp_path = tmp_file.name
    tmp_file.write(f"# Dork Parser v18.1 — SQL Targeted Results\n")
    tmp_file.write(f"# Date   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    tmp_file.write(f"# Dorks  : {total_dorks} | Pages: {pages_str}\n")
    tmp_file.write(f"# Filter : SQL ≥{min_score} | Chunks: {actual_chunks}\n")
    tmp_file.close()

    # [CHANGED] Show PROXY_ENABLED state in job status header
    if use_tor:
        proxy_info = "🧅 TOR"
    elif PROXY_ENABLED and _proxy_pool:
        proxy_info = f"🔄 {len(_proxy_pool)} proxies (per-chunk rotation)"
    elif not PROXY_ENABLED and _proxy_pool:
        proxy_info = f"⏸ Proxy disabled ({len(_proxy_pool)} loaded, PROXY_ENABLED=false)"
    else:
        proxy_info = "🔓 Direct (no proxy)"

    status_msg = await context.bot.send_message(
        chat_id,
        f"🕷 DORK PARSER v18.1 — STARTED\n"
        f"{'━'*30}\n"
        f"📋 Dorks   : {total_dorks}\n"
        f"📄 Pages   : {pages_str}\n"
        f"⚡ Chunks  : {actual_chunks} (isolated sessions)\n"
        f"⚙️ Workers : {workers_n}/chunk\n"
        f"🔍 Engines : {' + '.join(e.upper() for e in engines)}\n"
        f"🛡 Filter  : SQL ≥{min_score}\n"
        f"🌐 Network : {proxy_info}\n"
        f"🔒 TLS     : Chrome110 fingerprint\n"
        f"{'━'*30}\n⏳ Starting chunks...",
    )

    global_stop_ev = asyncio.Event()
    active_stop_evs[chat_id] = global_stop_ev
    progress_q: asyncio.Queue = asyncio.Queue(maxsize=total_dorks * 2)

    chunk_counters = {
        i: {"processed": 0, "total": len(chunks[i])}
        for i in range(actual_chunks)
    }
    agg_raw  = [0]
    agg_kept = [0]
    last_edit       = [0.0]
    total_processed = [0]

    async def _status_updater() -> None:
        while not global_stop_ev.is_set():
            drained = False
            while True:
                try:
                    ev  = progress_q.get_nowait()
                    cid = ev["chunk_id"]
                    chunk_counters[cid]["processed"] = ev["processed"]
                    agg_raw[0]  += ev["raw"]
                    agg_kept[0] += ev["kept"]
                    total_processed[0] += 1
                    drained = True
                except asyncio.QueueEmpty:
                    break

            if drained and time.time() - last_edit[0] > 4.0:
                proc    = total_processed[0]
                pct     = int(proc / total_dorks * 100) if total_dorks else 100
                bar     = "█" * (pct // 10) + "░" * (10 - pct // 10)
                elapsed = int(time.time() - start_time)
                eta     = int((elapsed / proc) * (total_dorks - proc)) if proc else 0
                cinfo   = " | ".join(
                    f"C{i}:{chunk_counters[i]['processed']}/{chunk_counters[i]['total']}"
                    for i in range(actual_chunks)
                )
                try:
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=status_msg.message_id,
                        text=(
                            f"⚡ PARSING... [{actual_chunks} parallel chunks]\n"
                            f"{'━'*30}\n"
                            f"[{bar}] {pct}%\n"
                            f"✅ Done    : {proc}/{total_dorks}\n"
                            f"🎯 SQL     : {agg_kept[0]}\n"
                            f"🗑 Raw drop: {agg_raw[0] - agg_kept[0]}\n"
                            f"⏱ {elapsed}s | ETA {eta}s\n"
                            f"📦 {cinfo}\n"
                            f"{'━'*30}"
                        ),
                    )
                    last_edit[0] = time.time()
                except Exception:
                    pass

            await asyncio.sleep(0.5)

    async def _job_timeout() -> None:
        await asyncio.sleep(JOB_TIMEOUT)
        log.warning(f"[JOB][{chat_id}] Global timeout ({JOB_TIMEOUT}s) — aborting")
        global_stop_ev.set()

    status_task  = asyncio.create_task(_status_updater())
    timeout_task = asyncio.create_task(_job_timeout())

    # [CHANGED] Each chunk gets a randomly assigned proxy from the pool.
    # When PROXY_ENABLED is False, _get_random_proxy returns None for all chunks.
    chunk_proxies = [_get_random_proxy() if not use_tor else None for _ in range(actual_chunks)]

    chunk_results = []
    try:
        chunk_tasks = []
        for i, chunk_dorks in enumerate(chunks):
            if i > 0:
                stagger = random.uniform(*CHUNK_STAGGER_DELAY)
                log.info(f"[JOB][{chat_id}] Staggering chunk C{i} by {stagger:.1f}s")
                await asyncio.sleep(stagger)

            task = asyncio.create_task(
                run_chunk(
                    chunk_id=i,
                    dorks=chunk_dorks,
                    engines=engines,
                    pages=pages,
                    max_res=max_res,
                    use_tor=use_tor,
                    min_score=min_score,
                    workers_n=workers_n,
                    progress_q=progress_q,
                    global_stop_ev=global_stop_ev,
                    proxy=chunk_proxies[i],
                )
            )
            chunk_tasks.append(task)

        chunk_results = await asyncio.gather(*chunk_tasks, return_exceptions=True)

    except asyncio.CancelledError:
        log.info(f"[JOB][{chat_id}] Job force-cancelled — collecting partial results")
        global_stop_ev.set()
        for t in chunk_tasks:
            t.cancel()
        chunk_results = await asyncio.gather(*chunk_tasks, return_exceptions=True)
        # Fall through to merge so partial results are still saved & sent
    finally:
        global_stop_ev.set()
        timeout_task.cancel()
        status_task.cancel()
        await asyncio.gather(timeout_task, status_task, return_exceptions=True)
        active_jobs.pop(chat_id, None)
        active_stop_evs.pop(chat_id, None)

    # ── Merge + global deduplication ─────────────────────────────────────────
    seen_urls    : set  = set()
    all_scored   : list = []
    total_raw        = 0
    total_degraded   = 0
    failed_chunks    = 0

    for result in chunk_results:
        if isinstance(result, BaseException):
            log.error(f"[JOB][{chat_id}] Chunk raised: {result}")
            failed_chunks += 1
            continue
        for sc, url in result["scored"]:
            if url not in seen_urls:
                seen_urls.add(url)
                all_scored.append((sc, url))
        total_raw      += result["raw_count"]
        total_degraded += result["degraded_count"]

    all_scored.sort(reverse=True)
    unique_cnt   = len(all_scored)
    elapsed      = int(time.time() - start_time)
    success_rate = (total_raw - (total_raw - unique_cnt)) / max(total_raw, 1)

    log.info(
        f"[JOB][{chat_id}] COMPLETE — dorks={total_dorks} raw={total_raw} "
        f"unique={unique_cnt} degraded={total_degraded} "
        f"failed_chunks={failed_chunks} elapsed={elapsed}s "
        f"success_rate={success_rate:.1%}"
    )

    high   = [(sc, u) for sc, u in all_scored if sc >= 70]
    medium = [(sc, u) for sc, u in all_scored if 40 <= sc < 70]
    low    = [(sc, u) for sc, u in all_scored if sc < 40]

    with open(tmp_path, "a", encoding="utf-8") as f:
        if high:
            f.write(f"# ── HIGH VALUE (score ≥70) — {len(high)} URLs\n")
            for sc, u in high:
                f.write(f"{u}\n")
        if medium:
            f.write(f"\n# ── MEDIUM VALUE (score 40–69) — {len(medium)} URLs\n")
            for sc, u in medium:
                f.write(f"{u}\n")
        if low and min_score < 40:
            f.write(f"\n# ── LOW VALUE (score <40) — {len(low)} URLs\n")
            for sc, u in low:
                f.write(f"{u}\n")

    stopped_early = ""
    try:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=status_msg.message_id,
            text=(
                f"🏁 JOB COMPLETE!{stopped_early}\n"
                f"{'━'*30}\n"
                f"📋 Dorks    : {total_dorks}\n"
                f"📄 Pages    : {pages_str}\n"
                f"⚡ Chunks   : {actual_chunks}\n"
                f"🔍 Raw      : {total_raw}\n"
                f"🎯 SQL      : {unique_cnt} unique URLs\n"
                f"🗑 Dropped  : {total_raw - unique_cnt} junk\n"
                f"⚠️ Degraded : {total_degraded} pages\n"
                f"📊 Hit rate : {success_rate:.0%}\n"
                f"⏱ Time     : {elapsed}s\n"
                f"{'━'*30}"
            ),
        )
    except Exception:
        pass

    if all_scored:
        with open(tmp_path, "rb") as f:
            await context.bot.send_document(
                chat_id, f,
                filename=f"sql_{total_dorks}dorks_{unique_cnt}urls.txt",
                caption=(
                    f"📁 SQL Targets\n"
                    f"🎯 {unique_cnt} unique | 🗑 {total_raw - unique_cnt} junk\n"
                    f"📋 {total_dorks} dorks | Pages: {pages_str} | "
                    f"⚡ {actual_chunks} chunks"
                ),
            )
    else:
        await context.bot.send_message(
            chat_id,
            "⚠️ No URLs matched the filter criteria.\n"
            "Try lowering /filter or adding more pages.",
        )

    try:
        os.unlink(tmp_path)
    except OSError:
        pass


# ─── UI HELPERS ──────────────────────────────────────────────────────────────
def get_session(chat_id: int) -> dict:
    if chat_id not in user_sessions:
        user_sessions[chat_id] = dict(DEFAULT_SESSION)
    return user_sessions[chat_id]


def page_keyboard(selected: list) -> InlineKeyboardMarkup:
    rows, row = [], []
    for p in range(1, 71):
        row.append(InlineKeyboardButton(
            f"✅{p}" if p in selected else str(p),
            callback_data=f"pg_{p}",
        ))
        if len(row) == 5:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([
        InlineKeyboardButton("🔁 All (1-70)", callback_data="pg_all"),
        InlineKeyboardButton("❌ Clear",       callback_data="pg_clear"),
        InlineKeyboardButton("✅ Confirm",     callback_data="pg_confirm"),
    ])
    return InlineKeyboardMarkup(rows)


# ══════════════════════════════════════════════════════════════════════════════
# ─── COMMAND HANDLERS ────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kb = [
        [InlineKeyboardButton("📂 Bulk Upload",  callback_data="m_bulk"),
         InlineKeyboardButton("🔍 Single Dork",  callback_data="m_single")],
        [InlineKeyboardButton("📄 Select Pages", callback_data="m_pages"),
         InlineKeyboardButton("⚙️ Settings",     callback_data="m_settings")],
        [InlineKeyboardButton("🧅 Tor On/Off",   callback_data="m_tor"),
         InlineKeyboardButton("🛡 SQL Filter",   callback_data="m_filter")],
        [InlineKeyboardButton("🧹 URL Cleaner",  callback_data="m_clean"),
         InlineKeyboardButton("📖 Help",         callback_data="m_help")],
    ]

    # [CHANGED] Show proxy status with PROXY_ENABLED state
    if PROXY_ENABLED and _proxy_pool:
        proxy_status = f"🔄 {len(_proxy_pool)} proxies loaded (enabled)"
    elif not PROXY_ENABLED and _proxy_pool:
        proxy_status = f"⏸ {len(_proxy_pool)} proxies loaded (DISABLED)"
    else:
        proxy_status = "🔓 No proxy pool"

    await update.message.reply_text(
        "🕷 DORK PARSER v18.1 — STEALTH PARALLEL\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🔒 Chrome110 TLS fingerprint spoofing\n"
        "⚡ Parallel page fetching per dork\n"
        "🔄 Full browser header rotation\n"
        "📈 Dynamic adaptive delay (fast/slow mode)\n"
        "🔍 Yahoo + DuckDuckGo engines\n"
        "🛡 SQL filter | Auto-slowdown | CAPTCHA hook\n"
        f"{proxy_status}\n\n"
        "📌 Core Commands:\n"
        "  /dork <q>   — single dork\n"
        "  /clean      — URL list cleaner mode\n"
        "  /pages      — pick pages 1-70\n"
        "  /workers N  — workers per chunk (1-20)\n"
        "  /chunks N   — parallel chunk count (1-8)\n"
        "  /engine X   — yahoo|duckduckgo|all\n"
        "  /tor        — toggle Tor IP rotation\n"
        "  /filter N   — SQL score filter (0-100)\n"
        "  /stop       — stop & get partial results\n"
        "  Upload .txt — auto-detected (URLs or dorks)\n\n"
        "🔄 Proxy Commands:\n"                                   # [NEW]
        "  /addproxy <url>    — add proxy to pool\n"
        "  /removeproxy [i|url] — remove by index or URL\n"
        "  /proxylist         — view all proxies\n"
        "  /testproxy <url>   — test a proxy manually\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        reply_markup=InlineKeyboardMarkup(kb),
    )


async def cmd_dork(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not context.args:
        await update.message.reply_text("Usage: /dork inurl:login.php?id=")
        return
    if chat_id in active_jobs and not active_jobs[chat_id].done():
        await update.message.reply_text("⚠️ Job running! Use /stop first.")
        return
    dork = " ".join(context.args)
    s    = get_session(chat_id)
    await update.message.reply_text(
        f"🔍 {dork[:60]}\n"
        f"📄 Pages: {', '.join(str(p) for p in s.get('pages', [1]))}"
        f"{'  🧅TOR' if s.get('tor') else ''}"
    )
    active_jobs[chat_id] = asyncio.create_task(run_dork_job(chat_id, [dork], context))


async def cmd_pages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id  = update.effective_chat.id
    selected = get_session(chat_id).get("pages", [1])
    await update.message.reply_text(
        f"📄 SELECT PAGES (1–70)\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Selected: {', '.join(str(p) for p in selected)}\n"
        f"Tap to toggle, then Confirm.",
        reply_markup=page_keyboard(selected),
    )


async def cmd_tor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global tor_enabled_users
    chat_id = update.effective_chat.id
    sess    = get_session(chat_id)

    if context.args and context.args[0].lower() in ("on", "off"):
        new_val = context.args[0].lower() == "on"
    else:
        new_val = not sess.get("tor", False)

    old_val     = sess.get("tor", False)
    sess["tor"] = new_val

    if new_val and not old_val:
        tor_enabled_users += 1
        if tor_enabled_users == 1:
            start_tor_rotation()
        await update.message.reply_text(
            "🧅 TOR ENABLED\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "Tor IP will rotate every 2 minutes.\n"
            "Make sure Tor is running:\n"
            "  sudo apt install tor && sudo service tor start\n\n"
            "⚠️ Speed will be slower."
        )
    elif not new_val and old_val:
        tor_enabled_users = max(0, tor_enabled_users - 1)
        if tor_enabled_users == 0:
            stop_tor_rotation()
        await update.message.reply_text("🔓 TOR DISABLED — Direct connection.")
    else:
        await update.message.reply_text(f"Tor is already {'ON' if new_val else 'OFF'}.")


async def cmd_filter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    sess    = get_session(chat_id)
    try:
        n = max(0, min(int(context.args[0]), 100))
        sess["min_score"] = n
        label = "🟥 High only" if n >= 70 else "🟧 Medium+" if n >= 40 else "🟨 All URLs"
        await update.message.reply_text(f"🛡 SQL Filter: ≥{n} ({label})")
    except Exception:
        cur = sess.get("min_score", 30)
        await update.message.reply_text(
            f"Usage: /filter N (0-100)\nCurrent: {cur}\n\n"
            f"🟥 70+ = high (likely SQLi)\n"
            f"🟧 40+ = medium (default 30)\n"
            f"🟨 0   = accept all"
        )


async def cmd_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    s       = get_session(chat_id)

    # [CHANGED] Show full proxy status including PROXY_ENABLED flag
    if PROXY_ENABLED and _proxy_pool:
        proxy_line = f"🔄 Proxies  : {len(_proxy_pool)} in pool (enabled)\n"
    elif not PROXY_ENABLED and _proxy_pool:
        proxy_line = f"⏸ Proxies  : {len(_proxy_pool)} loaded but DISABLED (PROXY_ENABLED=false)\n"
    else:
        proxy_line = "🔓 Proxies  : none loaded\n"

    await update.message.reply_text(
        f"⚙️ SETTINGS\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚡ Chunks   : {s.get('chunks', N_CHUNKS)} parallel sessions\n"
        f"🔧 Workers  : {s.get('workers', WORKERS_PER_CHUNK)}/chunk (max {MAX_WORKERS_PER_CHUNK})\n"
        f"📄 Pages    : {', '.join(str(p) for p in s.get('pages', [1]))} (1–70)\n"
        f"🔍 Engines  : {'+'.join(e.upper() for e in s.get('engines', ENGINES))}\n"
        f"📊 Max/Page : {s.get('max_results', MAX_RESULTS)}\n"
        f"🛡 SQL ≥    : {s.get('min_score', 30)}\n"
        f"🧅 Tor      : {'ON' if s.get('tor') else 'OFF'}\n"
        f"⏱ Delay    : {MIN_DELAY}–{MAX_DELAY}s | Fast: {FAST_MIN_DELAY}–{FAST_MAX_DELAY}s\n"
        f"🔒 TLS      : Chrome110 fingerprint\n"
        f"{proxy_line}"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"/workers N | /chunks N | /maxres N\n"
        f"/engine X  | /filter N\n"
        f"/pages     | /tor\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"          # [NEW] proxy commands section
        f"🔄 Proxy Management:\n"
        f"/addproxy <url>      — add to pool\n"
        f"/removeproxy [i|url] — remove from pool\n"
        f"/proxylist           — view pool\n"
        f"/testproxy <url>     — test a proxy"
    )


async def cmd_workers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    try:
        n = max(1, min(int(context.args[0]), MAX_WORKERS_PER_CHUNK))
        get_session(chat_id)["workers"] = n
        await update.message.reply_text(f"✅ Workers per chunk: {n} (max {MAX_WORKERS_PER_CHUNK})")
    except Exception:
        await update.message.reply_text(f"Usage: /workers N (1-{MAX_WORKERS_PER_CHUNK})")


async def cmd_chunks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    try:
        n = max(1, min(int(context.args[0]), 8))
        get_session(chat_id)["chunks"] = n
        await update.message.reply_text(
            f"✅ Parallel chunks: {n}\n"
            f"Each chunk uses an isolated session + {get_session(chat_id).get('workers', WORKERS_PER_CHUNK)} workers."
        )
    except Exception:
        cur = get_session(chat_id).get("chunks", N_CHUNKS)
        await update.message.reply_text(f"Usage: /chunks N (1-8)\nCurrent: {cur}")


async def cmd_maxres(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    try:
        n = max(1, min(int(context.args[0]), 50))
        get_session(chat_id)["max_results"] = n
        await update.message.reply_text(f"✅ Max/page: {n}")
    except Exception:
        await update.message.reply_text("Usage: /maxres N (1-50)")


async def cmd_engine(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    try:
        choice = context.args[0].lower()
        engine_map = {
            "yahoo":       ["yahoo"],
            "duckduckgo":  ["duckduckgo"],
            "ddg":         ["duckduckgo"],
            "all":         list(ENGINES),
            "both":        list(ENGINES),
        }
        engines = engine_map.get(choice, list(ENGINES))
        get_session(chat_id)["engines"] = engines
        await update.message.reply_text(f"✅ Engines: {'+'.join(e.upper() for e in engines)}")
    except Exception:
        await update.message.reply_text("Usage: /engine yahoo|duckduckgo|ddg|all")


async def cmd_clean(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🧹 URL CLEANER MODE\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "Upload a .txt file containing one URL per line.\n"
        "The bot will automatically detect it as a URL list and apply:\n\n"
        "  🚫 Blocked domain filter (major platforms + custom list)\n"
        "  🔗 Keep only URLs with query parameters (?param=value)\n"
        "  📏 Remove URLs longer than 200 characters\n"
        "  ❌ Remove invalid/malformed URLs\n"
        "  🔁 Remove duplicates\n\n"
        "📁 Results saved to cleaned_urls.txt and sent back to you.\n"
        "⏹ Use /stop anytime — partial results will be returned.\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "Just upload your .txt file now ↑"
    )


async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    stop_ev = active_stop_evs.get(chat_id)
    job     = active_jobs.get(chat_id)

    if stop_ev and job and not job.done():
        stop_ev.set()
        await update.message.reply_text(
            "⏹ STOP REQUESTED\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "Workers are draining...\n"
            "📦 Partial results will be sent automatically."
        )
    elif job and not job.done():
        job.cancel()
        active_jobs.pop(chat_id, None)
        await update.message.reply_text("🛑 Job force-stopped (no partial results available).")
    else:
        await update.message.reply_text("💤 No active job to stop.")


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    job     = active_jobs.get(chat_id)
    await update.message.reply_text(
        "⚡ Job RUNNING" if job and not job.done() else "💤 No active job"
    )


# ══════════════════════════════════════════════════════════════════════════════
# ─── PROXY MANAGEMENT COMMANDS (NEW in v18.1) ─────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

async def cmd_addproxy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    [NEW] /addproxy <proxy_url>
    Add a new proxy to the in-memory pool and persist to proxies.txt.

    Accepted formats:
      /addproxy socks5://user:pass@host:port
      /addproxy http://host:port
      /addproxy https://host:port
    """
    if not context.args:
        await update.message.reply_text(
            "➕ ADD PROXY\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "Usage: /addproxy <proxy_url>\n\n"
            "Formats:\n"
            "  socks5://user:pass@host:port\n"
            "  http://host:port\n"
            "  https://host:port\n\n"
            f"Current pool size: {len(_proxy_pool)}"
        )
        return

    proxy_url = context.args[0].strip()

    # Validate format
    if not _validate_proxy_url(proxy_url):
        await update.message.reply_text(
            "❌ Invalid proxy format.\n\n"
            "Accepted formats:\n"
            "  socks5://user:pass@host:port\n"
            "  http://host:port\n"
            "  https://host:port\n\n"
            "Example: /addproxy socks5://127.0.0.1:1080"
        )
        return

    # Check for duplicate
    if proxy_url in _proxy_pool:
        await update.message.reply_text(
            f"⚠️ Proxy already in pool.\n"
            f"Index: {_proxy_pool.index(proxy_url) + 1}\n"
            f"Pool size: {len(_proxy_pool)}"
        )
        return

    # Add to in-memory pool and persist
    async with _proxy_pool_lock:
        _proxy_pool.append(proxy_url)
        _persist_proxies()

    info = _parse_proxy_info(proxy_url)
    log.info(f"[PROXY] Added: {proxy_url}")

    await update.message.reply_text(
        f"✅ PROXY ADDED\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔌 Protocol : {info['protocol']}\n"
        f"🌐 Host     : {info['host']}\n"
        f"🔢 Port     : {info['port']}\n"
        f"🔐 Auth     : {'Yes' if info['auth'] else 'No'}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📦 Pool size: {len(_proxy_pool)}\n"
        f"💾 Saved to proxies.txt\n\n"
        f"Use /testproxy {proxy_url}\nto verify it works before using."
    )


async def cmd_removeproxy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    [NEW] /removeproxy [index | proxy_url]
    Remove a proxy from the pool by its 1-based index or exact URL.
    With no argument, show the numbered list of current proxies.
    """
    # No argument → show numbered list for reference
    if not context.args:
        if not _proxy_pool:
            await update.message.reply_text(
                "📭 Proxy pool is empty.\n"
                "Use /addproxy <url> to add one."
            )
            return

        lines = ["📋 PROXY POOL (use index to remove)\n━━━━━━━━━━━━━━━━━━━━━━"]
        for i, p in enumerate(_proxy_pool, start=1):
            info = _parse_proxy_info(p)
            lines.append(f"{i}. [{info['protocol']}] {info['host']}:{info['port']}")
        lines.append(f"━━━━━━━━━━━━━━━━━━━━━━")
        lines.append(f"Usage: /removeproxy <index>  or  /removeproxy <url>")
        await update.message.reply_text("\n".join(lines))
        return

    arg = context.args[0].strip()

    async with _proxy_pool_lock:
        # Try removal by integer index (1-based)
        try:
            idx = int(arg) - 1
            if idx < 0 or idx >= len(_proxy_pool):
                await update.message.reply_text(
                    f"❌ Index out of range.\n"
                    f"Pool has {len(_proxy_pool)} {'proxy' if len(_proxy_pool)==1 else 'proxies'} (1–{len(_proxy_pool)})."
                )
                return
            removed = _proxy_pool.pop(idx)
            _persist_proxies()
            info = _parse_proxy_info(removed)
            log.info(f"[PROXY] Removed index {idx+1}: {removed}")
            await update.message.reply_text(
                f"🗑 PROXY REMOVED\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🔌 Protocol : {info['protocol']}\n"
                f"🌐 Host     : {info['host']}:{info['port']}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📦 Remaining: {len(_proxy_pool)}\n"
                f"💾 proxies.txt updated"
            )
            return
        except ValueError:
            pass  # not an integer — try URL match

        # Try removal by exact URL
        if arg in _proxy_pool:
            _proxy_pool.remove(arg)
            _persist_proxies()
            info = _parse_proxy_info(arg)
            log.info(f"[PROXY] Removed by URL: {arg}")
            await update.message.reply_text(
                f"🗑 PROXY REMOVED\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🔌 Protocol : {info['protocol']}\n"
                f"🌐 Host     : {info['host']}:{info['port']}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📦 Remaining: {len(_proxy_pool)}\n"
                f"💾 proxies.txt updated"
            )
        else:
            await update.message.reply_text(
                f"❌ Proxy not found in pool.\n\n"
                f"Use /removeproxy with no argument to see the numbered list,\n"
                f"then remove by index number."
            )


async def cmd_proxylist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    [NEW] /proxylist
    Display all proxies currently in the pool with their index, protocol, host,
    and port. No status indicators or test results — just the list.
    """
    if not _proxy_pool:
        # [CHANGED] Show PROXY_ENABLED status even when pool is empty
        enabled_note = "" if PROXY_ENABLED else "\n⚠️ Note: PROXY_ENABLED=false — proxies are globally disabled."
        await update.message.reply_text(
            f"📭 Proxy pool is empty.{enabled_note}\n\n"
            f"Add proxies with:\n"
            f"  /addproxy socks5://host:port\n"
            f"  /addproxy http://host:port"
        )
        return

    enabled_tag = "✅ ENABLED" if PROXY_ENABLED else "⏸ DISABLED (PROXY_ENABLED=false)"
    lines = [
        f"🔄 PROXY POOL — {len(_proxy_pool)} {'proxy' if len(_proxy_pool)==1 else 'proxies'} | {enabled_tag}",
        "━━━━━━━━━━━━━━━━━━━━━━",
    ]
    for i, p in enumerate(_proxy_pool, start=1):
        info = _parse_proxy_info(p)
        auth_tag = " 🔐" if info["auth"] else ""
        lines.append(f"{i:>2}. [{info['protocol']:7s}] {info['host']}:{info['port']}{auth_tag}")

    lines += [
        "━━━━━━━━━━━━━━━━━━━━━━",
        "/addproxy <url>   — add proxy",
        "/removeproxy <i>  — remove by index",
        "/testproxy <url>  — test a proxy",
    ]
    await update.message.reply_text("\n".join(lines))


async def cmd_testproxy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    [NEW] /testproxy <proxy_url>
    Manually test a proxy by making a lightweight GET request to httpbin.org/ip
    (or ipinfo.io/ip as fallback). Reports success/failure, latency, and
    the external IP the proxy presents. No automatic testing — manual only.
    """
    if not context.args:
        await update.message.reply_text(
            "🧪 TEST PROXY\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "Usage: /testproxy <proxy_url>\n\n"
            "Example:\n"
            "  /testproxy socks5://127.0.0.1:1080\n"
            "  /testproxy http://user:pass@host:3128\n\n"
            "Tests the proxy against httpbin.org/ip and reports latency + IP."
        )
        return

    proxy_url = context.args[0].strip()

    if not _validate_proxy_url(proxy_url):
        await update.message.reply_text(
            "❌ Invalid proxy format.\n"
            "Expected: http://host:port or socks5://host:port"
        )
        return

    info = _parse_proxy_info(proxy_url)
    wait_msg = await update.message.reply_text(
        f"🧪 Testing proxy...\n"
        f"🔌 {info['protocol']} {info['host']}:{info['port']}\n"
        f"⏳ Connecting to httpbin.org/ip..."
    )

    test_urls = [
        "https://httpbin.org/ip",
        "https://ipinfo.io/ip",
        "https://api.ipify.org",
    ]

    success    = False
    latency_ms = None
    ext_ip     = None
    error_msg  = None

    # [NEW] Create an isolated test session for this proxy only
    test_session = AsyncSession(
        impersonate="chrome110",
        verify=False,
        timeout=15,
        proxy=proxy_url,
    )

    try:
        for test_url in test_urls:
            try:
                t0   = time.monotonic()
                resp = await test_session.get(
                    test_url,
                    headers=_random_headers(),
                    timeout=15,
                )
                latency_ms = int((time.monotonic() - t0) * 1000)

                if resp.status_code == 200:
                    raw_text = resp.text.strip()
                    # Try to parse JSON from httpbin {"origin": "x.x.x.x"}
                    import json as _json
                    try:
                        data   = _json.loads(raw_text)
                        ext_ip = data.get("origin") or data.get("ip") or raw_text
                    except Exception:
                        ext_ip = raw_text[:50]   # plain text IP from ipinfo/ipify
                    success = True
                    break
                else:
                    error_msg = f"HTTP {resp.status_code} from {test_url}"

            except asyncio.TimeoutError:
                error_msg = "Timeout (>15s)"
                continue
            except CurlError as exc:
                error_msg = f"CurlError: {exc}"
                continue
            except Exception as exc:
                error_msg = str(exc)[:80]
                continue
    finally:
        await test_session.close()

    if success:
        result_text = (
            f"✅ PROXY WORKING\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🔌 Protocol  : {info['protocol']}\n"
            f"🌐 Host      : {info['host']}:{info['port']}\n"
            f"🔐 Auth      : {'Yes' if info['auth'] else 'No'}\n"
            f"⏱ Latency   : {latency_ms} ms\n"
            f"🌍 External IP: {ext_ip}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
        )
        # Suggest adding if not already in pool
        if proxy_url not in _proxy_pool:
            result_text += "➕ Not in pool yet — use /addproxy to add it."
        else:
            idx = _proxy_pool.index(proxy_url) + 1
            result_text += f"📦 Already in pool at index {idx}."
    else:
        result_text = (
            f"❌ PROXY FAILED\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🔌 Protocol : {info['protocol']}\n"
            f"🌐 Host     : {info['host']}:{info['port']}\n"
            f"💬 Error    : {error_msg or 'Unknown error'}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Proxy unreachable or misconfigured.\n"
            f"Do not add this proxy to the pool."
        )

    try:
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=wait_msg.message_id,
            text=result_text,
        )
    except Exception:
        await update.message.reply_text(result_text)


# ══════════════════════════════════════════════════════════════════════════════


# ─── FILE DETECTION ──────────────────────────────────────────────────────────
def _looks_like_url_list(lines: list) -> bool:
    non_empty = [l for l in lines if l.strip() and not l.startswith("#")]
    if not non_empty:
        return False
    url_lines = sum(1 for l in non_empty if l.strip().startswith("http"))
    return url_lines / len(non_empty) >= 0.5


# ─── DOCUMENT & TEXT HANDLERS ────────────────────────────────────────────────
async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    doc     = update.message.document
    if chat_id in active_jobs and not active_jobs[chat_id].done():
        await update.message.reply_text("⚠️ Job running! Use /stop first.")
        return
    if not doc.file_name.endswith(".txt"):
        await update.message.reply_text("❌ Send a .txt file (one URL or dork per line).")
        return
    await update.message.reply_text("📥 Reading file...")
    try:
        content = await (await context.bot.get_file(doc.file_id)).download_as_bytearray()
        lines   = content.decode("utf-8", errors="replace").splitlines()

        if _looks_like_url_list(lines):
            raw_urls = [l.strip() for l in lines if l.strip() and not l.startswith("#")]
            if not raw_urls:
                await update.message.reply_text("❌ No URLs found in file.")
                return
            await update.message.reply_text(
                f"🧹 URL LIST detected — {len(raw_urls)} URLs\n"
                f"🚀 Running URL Cleaner..."
            )
            active_jobs[chat_id] = asyncio.create_task(
                run_url_clean_job(chat_id, raw_urls, context)
            )
        else:
            dorks = [l.strip() for l in lines if l.strip() and not l.startswith("#")]
            if not dorks:
                await update.message.reply_text("❌ No dorks found.")
                return
            s = get_session(chat_id)
            await update.message.reply_text(
                f"✅ {len(dorks)} dorks | Pages: {', '.join(str(p) for p in s.get('pages', [1]))}\n"
                f"🛡 SQL ≥{s.get('min_score', 30)} | "
                f"⚡ {s.get('chunks', N_CHUNKS)} chunks | "
                f"{'🧅TOR' if s.get('tor') else '🔓 Direct'}\n🚀 Starting..."
            )
            active_jobs[chat_id] = asyncio.create_task(run_dork_job(chat_id, dorks, context))
    except Exception as exc:
        await update.message.reply_text(f"❌ Error: {exc}")


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    lines   = [
        l.strip()
        for l in update.message.text.splitlines()
        if l.strip() and not l.startswith("#")
    ]
    if len(lines) > 1:
        if chat_id in active_jobs and not active_jobs[chat_id].done():
            await update.message.reply_text("⚠️ Job running! /stop first.")
            return
        s = get_session(chat_id)
        await update.message.reply_text(
            f"✅ {len(lines)} dorks | Pages: {', '.join(str(p) for p in s.get('pages', [1]))}\n🚀 Starting..."
        )
        active_jobs[chat_id] = asyncio.create_task(run_dork_job(chat_id, lines, context))
    else:
        await update.message.reply_text(
            "Use /dork <q> or upload .txt\n/pages | /tor | /filter N | /chunks N"
        )


# ─── CALLBACK HANDLER ────────────────────────────────────────────────────────
async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    await query.answer()
    data    = query.data
    chat_id = query.message.chat_id
    sess    = get_session(chat_id)

    if data.startswith("pg_"):
        cmd      = data[3:]
        selected = list(sess.get("pages", [1]))
        if cmd == "all":
            selected = list(range(1, 71))
        elif cmd == "clear":
            selected = []
        elif cmd == "confirm":
            sess["pages"] = selected or [1]
            try:
                await query.edit_message_text(
                    f"✅ Pages: {', '.join(str(p) for p in sorted(sess['pages']))}\n"
                    f"Run /dork or upload .txt"
                )
            except Exception:
                pass
            return
        else:
            try:
                p = int(cmd)
                selected.remove(p) if p in selected else selected.append(p)
                selected = sorted(selected)
            except ValueError:
                pass
        sess["pages"] = selected
        try:
            await query.edit_message_text(
                f"📄 SELECT PAGES (1–70)\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"Selected: {', '.join(str(p) for p in selected) or 'none'}\n"
                f"Tap to toggle, then Confirm.",
                reply_markup=page_keyboard(selected),
            )
        except Exception:
            pass
        return

    # [CHANGED] Updated inline help and settings to include proxy commands
    replies = {
        "m_bulk":     "📂 Upload a .txt file — URLs or dorks (auto-detected). No limit!",
        "m_single":   "🔍 /dork inurl:login.php?id=\nSet pages with /pages",
        "m_tor":      f"🧅 Tor is {'ON — /tor off to disable' if sess.get('tor') else 'OFF — /tor on to enable'}",
        "m_filter":   f"🛡 SQL Filter ≥{sess.get('min_score', 30)}\n/filter 70=high | /filter 40=medium | /filter 0=all",
        "m_clean": (
            "🧹 URL CLEANER\n━━━━━━━━━━━━━━━━━━━\n"
            "Upload a .txt file with one URL per line.\n"
            "Filters applied:\n"
            "  🚫 Blocked domains | 🔗 Must have query params\n"
            "  📏 Max 200 chars   | ❌ Invalid URLs removed\n"
            "  🔁 Duplicates removed\n\n"
            "Results → cleaned_urls.txt sent to you.\n"
            "Use /stop anytime to get partial results."
        ),
        "m_settings": (
            f"⚙️ Chunks:{sess.get('chunks', N_CHUNKS)} "
            f"Workers:{sess.get('workers', WORKERS_PER_CHUNK)}/chunk "
            f"Pages:{','.join(str(p) for p in sess.get('pages', [1]))} "
            f"Engines:{'+'.join(e.upper() for e in sess.get('engines', ENGINES))} "
            f"Score≥{sess.get('min_score', 30)} Tor:{'ON' if sess.get('tor') else 'OFF'} "
            f"Proxies:{len(_proxy_pool)}({'on' if PROXY_ENABLED else 'off'})"  # [NEW]
        ),
        "m_help": (
            "📖 COMMANDS\n━━━━━━━━━━━━━━━━━━━\n"
            "/dork <q>         — single dork search\n"
            "/clean            — URL list cleaner info\n"
            "/pages            — page selector (1-70)\n"
            "/chunks N         — parallel sessions (1-8)\n"
            "/workers N        — workers per chunk (1-20)\n"
            "/tor              — toggle Tor rotation\n"
            "/engine X         — yahoo|duckduckgo|all\n"
            "/filter N         — SQL score (0-100)\n"
            "/settings         — full config view\n"
            "/maxres N         — results/page (1-50)\n"
            "/stop             — stop & get partial results\n"
            "/status           — job status\n"
            "━━━━━━━━━━━━━━━━━━━\n"
            "🔄 Proxy:\n"                                                         # [NEW]
            "/addproxy <url>   — add proxy to pool\n"
            "/removeproxy [i]  — remove by index or URL\n"
            "/proxylist        — view all proxies\n"
            "/testproxy <url>  — test proxy (latency + IP)\n"
            "━━━━━━━━━━━━━━━━━━━\n"
            "Upload .txt — auto-detected as URL list or dorks!\n"
            "📁 All results saved as a file — no chat spam."
        ),
    }

    if data == "m_pages":
        await query.message.reply_text(
            f"📄 SELECT PAGES (1–70)\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Selected: {', '.join(str(p) for p in sess.get('pages', [1]))}\nTap to toggle.",
            reply_markup=page_keyboard(sess.get("pages", [1])),
        )
    elif data in replies:
        await query.message.reply_text(replies[data])


# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    if not BOT_TOKEN:
        log.critical("BOT_TOKEN not set! Add to .env file or environment.")
        raise SystemExit(1)

    app = Application.builder().token(BOT_TOKEN).build()

    # ── Core command registrations (unchanged) ──────────────────────────────
    for name, handler in [
        ("start",    cmd_start),
        ("help",     cmd_settings),
        ("dork",     cmd_dork),
        ("clean",    cmd_clean),
        ("pages",    cmd_pages),
        ("tor",      cmd_tor),
        ("filter",   cmd_filter),
        ("settings", cmd_settings),
        ("workers",  cmd_workers),
        ("chunks",   cmd_chunks),
        ("maxres",   cmd_maxres),
        ("engine",   cmd_engine),
        ("stop",     cmd_stop),
        ("status",   cmd_status),
    ]:
        app.add_handler(CommandHandler(name, handler))

    # ── [NEW] Proxy management command registrations ─────────────────────────
    for name, handler in [
        ("addproxy",    cmd_addproxy),
        ("removeproxy", cmd_removeproxy),
        ("proxylist",   cmd_proxylist),
        ("testproxy",   cmd_testproxy),
    ]:
        app.add_handler(CommandHandler(name, handler))

    app.add_handler(MessageHandler(filters.Document.ALL,            handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(CallbackQueryHandler(handle_callback))

    async def _shutdown():
        stop_tor_rotation()
    app.shutdown_handler = _shutdown

    log.info("=" * 60)
    log.info("  DORK PARSER v18.1 — STEALTH PARALLEL ARCHITECTURE")
    log.info(f"  Chunks: {N_CHUNKS} | Workers/chunk: {WORKERS_PER_CHUNK}")
    log.info(f"  Delay: {MIN_DELAY}–{MAX_DELAY}s | Fast: {FAST_MIN_DELAY}–{FAST_MAX_DELAY}s")
    log.info(f"  TLS: Chrome110 fingerprint")
    log.info(f"  Proxies: {len(_proxy_pool)} loaded | PROXY_ENABLED={PROXY_ENABLED}")
    log.info(f"  Engines: {', '.join(ENGINES)}")
    log.info(f"  New commands: /addproxy /removeproxy /proxylist /testproxy")
    log.info("=" * 60)
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
