"""
╔══════════════════════════════════════════════════════════════╗
║   DORK PARSER BOT v19.0 — MULTI-THREADED STEALTH ARCHITECTURE║
║                                                              ║
║   • ThreadPoolExecutor with isolated curl_cffi sessions      ║
║   • Dynamic TLS fingerprint rotation (9 impersonations)      ║
║   • Google + Yahoo + DuckDuckGo (Bing removed)               ║
║   • Google consent-page auto-handling + CAPTCHA detection    ║
║   • Fully automatic proxy pool (health checks + cooldown)    ║
║   • Tor as fallback in proxy rotation                        ║
║   • Manual proxy: /addproxy /removeproxy /proxylist /testproxy║
║   • Graceful /stop returns partial results                   ║
║   • Per-thread proxy ownership + thread-safe rotation        ║
╚══════════════════════════════════════════════════════════════╝
"""

import asyncio
import os
import random
import re
import threading
import time
import logging
import tempfile
import queue as queue_module
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlparse, parse_qs, unquote, urlencode

from curl_cffi.requests import Session
from curl_cffi import CurlError

from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters
)

load_dotenv()

# ─── LOGGING ─────────────────────────────────────────────────────────────────
Path("logs").mkdir(exist_ok=True)
log_file = f"logs/bot_{datetime.now().strftime('%Y%m%d')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | [%(threadName)s] | %(message)s",
    handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
)
log = logging.getLogger(__name__)

# ─── CONFIGURATION ───────────────────────────────────────────────────────────
BOT_TOKEN              = os.environ.get("BOT_TOKEN", "")
N_CHUNKS               = int(os.environ.get("N_CHUNKS", 2))
WORKERS_PER_CHUNK      = int(os.environ.get("WORKERS_PER_CHUNK", 4))
MAX_WORKERS_PER_CHUNK  = 16

# Google-tuned delays (slightly more conservative than other engines)
MIN_DELAY              = float(os.environ.get("MIN_DELAY", 2.0))
MAX_DELAY              = float(os.environ.get("MAX_DELAY", 4.0))
FAST_MIN_DELAY         = 1.2
FAST_MAX_DELAY         = 2.2
FAST_STREAK_THRESHOLD  = 5

MAX_RESULTS            = int(os.environ.get("MAX_RESULTS", 10))
TOR_PROXY              = os.environ.get("TOR_PROXY", "socks5://127.0.0.1:9050")
OUTPUT_DIR             = Path("results")
OUTPUT_DIR.mkdir(exist_ok=True)

ENGINES                = ["google", "yahoo", "duckduckgo"]
MAX_PAGES              = 70

WORKER_FETCH_TIMEOUT   = 90
JOB_TIMEOUT            = 30 * 60
MAX_RETRIES            = 3
EMPTY_RATE_SLOWDOWN    = 0.50
EMPTY_RATE_RECOVER     = 0.30
CHUNK_STAGGER_DELAY    = (1.0, 3.0)

# Proxy automation
PROXY_RECHECK_INTERVAL = int(os.environ.get("PROXY_RECHECK_INTERVAL", 600))
PROXY_COOLDOWN         = int(os.environ.get("PROXY_COOLDOWN", 300))
PROXY_TEST_TIMEOUT     = int(os.environ.get("PROXY_TEST_TIMEOUT", 10))
PROXY_TEST_ATTEMPTS    = 2

PROXY_ENABLED: bool = os.environ.get("PROXY_ENABLED", "true").lower() not in ("false", "0", "no")

DEFAULT_SESSION = {
    "workers":     WORKERS_PER_CHUNK,
    "chunks":      N_CHUNKS,
    "engines":     list(ENGINES),
    "max_results": MAX_RESULTS,
    "pages":       [1],
    "tor":         False,
    "min_score":   30,
}

user_sessions:    dict = {}
active_jobs:      dict = {}     # chat_id -> Future
active_stop_evs:  dict = {}     # chat_id -> threading.Event


# ══════════════════════════════════════════════════════════════════════════════
# ─── BROWSER PROFILES (impersonation + matching headers) ─────────────────────
# ══════════════════════════════════════════════════════════════════════════════

# Each profile pairs a curl_cffi impersonation target with a complete, consistent
# HTTP header set. Mismatching impersonation+UA leaks a detectable inconsistency.
BROWSER_PROFILES = [
    {
        "impersonate": "chrome110",
        "headers": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Sec-Ch-Ua": '"Chromium";v="110", "Not A(Brand";v="24", "Google Chrome";v="110"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
        },
    },
    {
        "impersonate": "chrome120",
        "headers": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
        },
    },
    {
        "impersonate": "chrome124",
        "headers": {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"macOS"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
        },
    },
    {
        "impersonate": "edge101",
        "headers": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36 Edg/101.0.1210.53",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Sec-Ch-Ua": '" Not A;Brand";v="99", "Chromium";v="101", "Microsoft Edge";v="101"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
        },
    },
    {
        "impersonate": "edge110",
        "headers": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.1587.63 Safari/537.36 Edg/110.0.1587.63",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Sec-Ch-Ua": '"Chromium";v="110", "Not A(Brand";v="24", "Microsoft Edge";v="110"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
        },
    },
    {
        "impersonate": "firefox120",
        "headers": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "TE": "trailers",
        },
    },
    {
        "impersonate": "firefox124",
        "headers": {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "TE": "trailers",
        },
    },
    {
        "impersonate": "safari17_0",
        "headers": {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
        },
    },
    {
        "impersonate": "safari17_2_ios",
        "headers": {
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
        },
    },
]


def _random_profile() -> dict:
    """Return a random complete browser profile (impersonation + headers)."""
    return random.choice(BROWSER_PROFILES)


# ══════════════════════════════════════════════════════════════════════════════
# ─── PROXY POOL — FULLY AUTOMATIC, THREAD-SAFE ───────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

class ProxyPool:
    """
    Thread-safe automatic proxy pool with health checks, cooldown, and rotation.
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._all: list = []          # all known proxies (active + bad)
        self._active: deque = deque() # rotation queue of healthy proxies
        self._bad: dict = {}          # proxy -> timestamp when marked bad
        self._tested: set = set()     # proxies that passed at least one test
        self._tor_in_pool: bool = False

    # ── Loading ──────────────────────────────────────────────────────────────
    def load_from_sources(self) -> None:
        """Load from PROXY_LIST env var or proxies.txt."""
        proxies = []
        env_list = os.environ.get("PROXY_LIST", "").strip()
        if env_list:
            proxies = [p.strip() for p in env_list.split(",") if p.strip()]
            log.info(f"[PROXY] Loaded {len(proxies)} proxies from PROXY_LIST env var")
        else:
            proxy_file = Path("proxies.txt")
            if proxy_file.exists():
                with open(proxy_file, encoding="utf-8") as f:
                    proxies = [
                        ln.strip() for ln in f
                        if ln.strip() and not ln.startswith("#")
                    ]
                log.info(f"[PROXY] Loaded {len(proxies)} proxies from proxies.txt")

        with self._lock:
            self._all = list(dict.fromkeys(proxies))   # dedupe preserving order
            self._active = deque(self._all)            # all start as candidates

    def persist(self) -> None:
        """Write current pool to proxies.txt."""
        with self._lock:
            try:
                with open("proxies.txt", "w", encoding="utf-8") as f:
                    f.write("# Proxy list — managed by /addproxy and /removeproxy\n")
                    f.write(f"# Last updated : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write(f"# Total        : {len(self._all)}\n")
                    for p in self._all:
                        if p == TOR_PROXY and self._tor_in_pool:
                            continue   # Tor is added dynamically, don't persist
                        f.write(p + "\n")
                log.info(f"[PROXY] Persisted {len(self._all)} proxies to proxies.txt")
            except Exception as exc:
                log.warning(f"[PROXY] Persist failed: {exc}")

    # ── Inspection ───────────────────────────────────────────────────────────
    def all_proxies(self) -> list:
        with self._lock:
            return list(self._all)

    def active_count(self) -> int:
        with self._lock:
            self._recover_bad()
            return len(self._active)

    def total_count(self) -> int:
        with self._lock:
            return len(self._all)

    def is_tested(self, proxy: str) -> bool:
        with self._lock:
            return proxy in self._tested

    # ── Tor integration ──────────────────────────────────────────────────────
    def enable_tor_fallback(self, enable: bool) -> None:
        """Add or remove Tor from the pool as a special fallback entry."""
        with self._lock:
            if enable and not self._tor_in_pool:
                if TOR_PROXY not in self._all:
                    self._all.append(TOR_PROXY)
                self._active.append(TOR_PROXY)
                self._tested.add(TOR_PROXY)
                self._tor_in_pool = True
                log.info(f"[PROXY] Tor added to pool as fallback: {TOR_PROXY}")
            elif not enable and self._tor_in_pool:
                if TOR_PROXY in self._all:
                    self._all.remove(TOR_PROXY)
                try:
                    self._active.remove(TOR_PROXY)
                except ValueError:
                    pass
                self._bad.pop(TOR_PROXY, None)
                self._tested.discard(TOR_PROXY)
                self._tor_in_pool = False
                log.info("[PROXY] Tor removed from pool")

    # ── Mutation ─────────────────────────────────────────────────────────────
    def add(self, proxy: str) -> bool:
        with self._lock:
            if proxy in self._all:
                return False
            self._all.append(proxy)
            self._active.append(proxy)
            return True

    def remove(self, proxy: str) -> bool:
        with self._lock:
            if proxy not in self._all:
                return False
            self._all.remove(proxy)
            try:
                self._active.remove(proxy)
            except ValueError:
                pass
            self._bad.pop(proxy, None)
            self._tested.discard(proxy)
            return True

    def remove_by_index(self, idx: int) -> str | None:
        with self._lock:
            if idx < 0 or idx >= len(self._all):
                return None
            proxy = self._all.pop(idx)
            try:
                self._active.remove(proxy)
            except ValueError:
                pass
            self._bad.pop(proxy, None)
            self._tested.discard(proxy)
            return proxy

    # ── Rotation ─────────────────────────────────────────────────────────────
    def acquire(self, exclude: str | None = None) -> str | None:
        """
        Pop the next healthy proxy (rotated). Returns None if pool is empty.
        Auto-recovers proxies whose cooldown has expired.
        """
        if not PROXY_ENABLED:
            return None
        with self._lock:
            self._recover_bad()
            if not self._active:
                return None
            # Try up to len() times to avoid returning the excluded proxy
            for _ in range(len(self._active)):
                proxy = self._active[0]
                self._active.rotate(-1)   # cycle
                if proxy != exclude:
                    return proxy
            return self._active[0] if self._active else None

    def mark_bad(self, proxy: str) -> None:
        """Mark proxy temporarily bad; will be retried after PROXY_COOLDOWN."""
        if not proxy:
            return
        with self._lock:
            self._bad[proxy] = time.time()
            try:
                self._active.remove(proxy)
            except ValueError:
                pass
            log.warning(f"[PROXY] Marked bad (cooldown {PROXY_COOLDOWN}s): {proxy}")

    def mark_good(self, proxy: str) -> None:
        if not proxy:
            return
        with self._lock:
            self._tested.add(proxy)
            self._bad.pop(proxy, None)
            if proxy not in self._active and proxy in self._all:
                self._active.append(proxy)

    def _recover_bad(self) -> None:
        """Move bad proxies back to active if cooldown elapsed (caller holds lock)."""
        now = time.time()
        recovered = []
        for proxy, ts in list(self._bad.items()):
            if now - ts >= PROXY_COOLDOWN:
                recovered.append(proxy)
        for proxy in recovered:
            del self._bad[proxy]
            if proxy in self._all and proxy not in self._active:
                self._active.append(proxy)
                log.info(f"[PROXY] Recovered after cooldown: {proxy}")

    def purge_dead(self, proxy: str) -> None:
        """Permanently remove a proxy that failed health checks."""
        with self._lock:
            if proxy in self._all:
                self._all.remove(proxy)
            try:
                self._active.remove(proxy)
            except ValueError:
                pass
            self._bad.pop(proxy, None)
            self._tested.discard(proxy)
            log.warning(f"[PROXY] Purged dead proxy: {proxy}")


PROXY_POOL = ProxyPool()


# ── Proxy validation ─────────────────────────────────────────────────────────
_PROXY_URL_RE = re.compile(
    r'^(https?|socks5h?)://(?:[^:@/\s]+:[^:@/\s]+@)?[\w\-\.]+:\d{1,5}/?$',
    re.IGNORECASE,
)


def _validate_proxy_url(proxy_url: str) -> bool:
    return bool(_PROXY_URL_RE.match(proxy_url.strip()))


def _parse_proxy_info(proxy_url: str) -> dict:
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


def _is_proxy_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    keywords = (
        "proxy", "tunnel", "407", "socks", "authentication",
        "connection refused", "network unreachable", "no route to host",
        "could not connect to proxy", "unable to connect to proxy",
        "recv failure", "ssl handshake", "timed out", "timeout",
        "502", "503", "504",
    )
    return any(kw in msg for kw in keywords)


# ── Health checking ──────────────────────────────────────────────────────────
PROXY_TEST_URLS = [
    "https://httpbin.org/ip",
    "https://api.ipify.org?format=json",
    "https://ifconfig.me/ip",
]


def test_proxy(proxy_url: str, attempts: int = PROXY_TEST_ATTEMPTS,
               timeout: int = PROXY_TEST_TIMEOUT) -> tuple[bool, int | None, str | None]:
    """
    Test a proxy synchronously. Returns (success, latency_ms, external_ip).
    Tries multiple test URLs; succeeds if any one returns 200.
    """
    for attempt in range(attempts):
        for test_url in PROXY_TEST_URLS:
            sess = None
            try:
                profile = _random_profile()
                sess = Session(
                    impersonate=profile["impersonate"],
                    verify=False,
                    timeout=timeout,
                    proxy=proxy_url,
                )
                t0 = time.monotonic()
                resp = sess.get(test_url, headers=profile["headers"], timeout=timeout)
                latency_ms = int((time.monotonic() - t0) * 1000)
                if resp.status_code == 200:
                    txt = resp.text.strip()
                    ext_ip = None
                    try:
                        import json as _json
                        data = _json.loads(txt)
                        ext_ip = data.get("origin") or data.get("ip")
                    except Exception:
                        ext_ip = txt[:40]
                    return True, latency_ms, ext_ip
            except Exception:
                pass
            finally:
                if sess:
                    try: sess.close()
                    except Exception: pass
        time.sleep(0.5)
    return False, None, None


def _proxy_health_thread() -> None:
    """Initial health check + periodic recheck loop."""
    log.info("[PROXY-HEALTH] Initial health check starting...")
    proxies = PROXY_POOL.all_proxies()
    if not proxies:
        log.info("[PROXY-HEALTH] Pool empty — health checker idle.")
    else:
        good = bad = 0
        for p in proxies:
            ok, lat, ip = test_proxy(p)
            if ok:
                PROXY_POOL.mark_good(p)
                good += 1
                log.info(f"[PROXY-HEALTH] ✓ {p} — {lat}ms — IP {ip}")
            else:
                PROXY_POOL.purge_dead(p)
                bad += 1
                log.warning(f"[PROXY-HEALTH] ✗ {p} — DEAD (purged)")
        log.info(f"[PROXY-HEALTH] Initial: {good} active / {bad} purged")

    # Periodic recheck loop
    while True:
        time.sleep(PROXY_RECHECK_INTERVAL)
        proxies = PROXY_POOL.all_proxies()
        if not proxies:
            continue
        log.info(f"[PROXY-HEALTH] Periodic recheck of {len(proxies)} proxies...")
        for p in proxies:
            ok, lat, _ = test_proxy(p, attempts=1)
            if ok:
                PROXY_POOL.mark_good(p)
            else:
                PROXY_POOL.purge_dead(p)
        log.info(f"[PROXY-HEALTH] Recheck complete: {PROXY_POOL.active_count()} active")


def start_proxy_health_thread() -> None:
    t = threading.Thread(target=_proxy_health_thread, name="proxy-health", daemon=True)
    t.start()


# ══════════════════════════════════════════════════════════════════════════════
# ─── TOR ROTATION (control port SIGNAL NEWNYM) ───────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

_tor_rotation_thread = None
_tor_stop_event = threading.Event()
tor_enabled_users = 0
_tor_lock = threading.Lock()


def rotate_tor_identity_sync() -> None:
    """Synchronous Tor NEWNYM signal."""
    import socket
    try:
        s = socket.create_connection(("127.0.0.1", 9051), timeout=5)
        s.sendall(b'AUTHENTICATE ""\r\nSIGNAL NEWNYM\r\n')
        time.sleep(0.5)
        data = s.recv(4096)
        s.close()
        if b"250" in data:
            log.info("[TOR] Identity rotated")
        else:
            log.warning(f"[TOR] Rotation response: {data[:100]}")
    except Exception as exc:
        log.warning(f"[TOR] Rotation error: {exc}")


def _tor_rotation_loop() -> None:
    while not _tor_stop_event.is_set():
        rotate_tor_identity_sync()
        if _tor_stop_event.wait(120):
            break


def start_tor_rotation() -> None:
    global _tor_rotation_thread
    with _tor_lock:
        if _tor_rotation_thread is None or not _tor_rotation_thread.is_alive():
            _tor_stop_event.clear()
            _tor_rotation_thread = threading.Thread(
                target=_tor_rotation_loop, name="tor-rotator", daemon=True
            )
            _tor_rotation_thread.start()
            log.info("[TOR] Rotation thread started")


def stop_tor_rotation() -> None:
    global _tor_rotation_thread
    with _tor_lock:
        if _tor_rotation_thread and _tor_rotation_thread.is_alive():
            _tor_stop_event.set()
            log.info("[TOR] Rotation thread stopped")
            _tor_rotation_thread = None


# ══════════════════════════════════════════════════════════════════════════════
# ─── SQL FILTER ENGINE (preserved from v18.1) ────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

BLACKLISTED_DOMAINS = {
    "yahoo.uservoice.com", "uservoice.com", "google.com", "googleapis.com",
    "googleadservices.com", "googleusercontent.com", "googlesyndication.com",
    "gstatic.com", "youtube.com", "facebook.com", "instagram.com", "twitter.com", "x.com",
    "linkedin.com", "pinterest.com", "reddit.com", "wikipedia.org", "amazon.com",
    "amazon.co", "ebay.com", "shopify.com", "wordpress.com", "blogspot.com", "medium.com",
    "github.com", "stackoverflow.com", "w3schools.com", "microsoft.com", "apple.com",
    "cloudflare.com", "yahoo.com", "msn.com", "live.com", "outlook.com", "mercadolibre.com",
    "aliexpress.com", "alibaba.com", "etsy.com", "walmart.com", "bestbuy.com",
    "capitaloneshopping.com", "onetonline.org", "moodle.", "lyrics.fi", "verkkouutiset.fi",
    "iltalehti.fi", "sapo.pt", "iol.pt", "idealo.", "zalando.", "trovaprezzi.",
    "duckduckgo.com", "duck.com", "whatsapp.com", "doubleclick.net",
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

    query = parsed.query
    path = parsed.path.lower()
    has_vuln_ext = any(path.endswith(ext) for ext in VULN_EXTENSIONS)

    if not query:
        return 25 if has_vuln_ext else 5

    score = 15
    params = parse_qs(query, keep_blank_values=True)
    pkeys = {k.lower() for k in params}

    if has_vuln_ext:
        score += 20
    score += len(pkeys & SQL_HIGH_PARAMS) * 15
    score += len(pkeys & SQL_MED_PARAMS) * 5

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


# ══════════════════════════════════════════════════════════════════════════════
# ─── URL CLEANER (preserved) ─────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

MAX_URL_LENGTH = 200


def extract_domain(url: str) -> str:
    try:
        netloc = urlparse(url).netloc.lower()
        return netloc[4:] if netloc.startswith("www.") else netloc
    except Exception:
        return ""


def is_blocked(domain: str) -> bool:
    return any(bd in domain for bd in BLACKLISTED_DOMAINS)


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
    total = len(urls)
    rm_invalid = rm_blocked = rm_no_query = rm_too_long = 0
    seen = set()
    kept = []
    for url in urls:
        url = url.strip()
        if not url or url.startswith("#"):
            rm_invalid += 1; continue
        if not is_valid_url(url):
            rm_invalid += 1; continue
        if len(url) > MAX_URL_LENGTH:
            rm_too_long += 1; continue
        domain = extract_domain(url)
        if is_blocked(domain):
            rm_blocked += 1; continue
        if not has_query_params(url):
            rm_no_query += 1; continue
        if url in seen:
            continue
        seen.add(url)
        kept.append(url)
    return {
        "total": total, "kept": kept,
        "rm_invalid": rm_invalid, "rm_blocked": rm_blocked,
        "rm_no_query": rm_no_query, "rm_too_long": rm_too_long,
        "duplicates": total - rm_invalid - rm_blocked - rm_no_query - rm_too_long - len(kept),
    }


# ══════════════════════════════════════════════════════════════════════════════
# ─── SESSION FACTORY ─────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

def _create_random_session(proxy: str | None = None) -> tuple[Session, dict, str]:
    """
    Create a fresh curl_cffi synchronous Session with a random TLS impersonation
    + matching browser headers + optional proxy.

    Returns (session, headers_dict, proxy_used_or_empty_string).
    """
    profile = _random_profile()
    kwargs = {
        "impersonate": profile["impersonate"],
        "verify":      False,
        "timeout":     20,
    }
    if proxy:
        kwargs["proxy"] = proxy
    sess = Session(**kwargs)
    return sess, dict(profile["headers"]), proxy or ""


# ══════════════════════════════════════════════════════════════════════════════
# ─── DEGRADED RESPONSE / CAPTCHA DETECTION ───────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

_CAPTCHA_RE = re.compile(
    r"captcha|are you a robot|unusual traffic|access denied|"
    r"verify you are human|please verify|too many requests|"
    r"blocked|forbidden|rate limit|temporarily unavailable",
    re.IGNORECASE,
)

_GOOGLE_CAPTCHA_RE = re.compile(
    r"our systems have detected unusual traffic|"
    r"/sorry/|"
    r"unusual traffic from your computer network|"
    r"to continue, please type the characters",
    re.IGNORECASE,
)

_GOOGLE_CONSENT_RE = re.compile(
    r"consent\.google\.|"
    r'action="https://consent\.google|'
    r"before you continue to google",
    re.IGNORECASE,
)


def _is_captcha_generic(html: str) -> bool:
    return bool(_CAPTCHA_RE.search(html[:8192]))


def _is_google_captcha(html: str) -> bool:
    return bool(_GOOGLE_CAPTCHA_RE.search(html[:8192])) or bool(_CAPTCHA_RE.search(html[:4096]))


def _is_google_consent(html: str) -> bool:
    return bool(_GOOGLE_CONSENT_RE.search(html[:8192]))


def _is_degraded(html: str, engine: str) -> bool:
    if len(html) < 400:
        return True
    if engine == "google":
        if _is_google_captcha(html):
            return True
        if 'id="search"' not in html and 'id="rso"' not in html and "/url?q=" not in html:
            return True
    elif engine == "yahoo":
        if _is_captcha_generic(html):
            return True
        if 'id="results"' not in html and "searchCenterMiddle" not in html:
            return True
    elif engine == "duckduckgo":
        if _is_captcha_generic(html):
            return True
        if "result__a" not in html and "results--main" not in html:
            return True
    return False


# ══════════════════════════════════════════════════════════════════════════════
# ─── HTML LINK EXTRACTION ────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

class _LinkExtractor(HTMLParser):
    """Generic <a href> + <cite> text extractor (used for Yahoo)."""
    __slots__ = ("links", "_in_cite", "_buf")

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.links: list = []
        self._in_cite = False
        self._buf: list = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            adict = dict(attrs)
            for key in ("href", "data-u"):
                val = adict.get(key, "")
                if val.startswith("http"):
                    self.links.append(val)
        elif tag == "cite":
            self._in_cite = True
            self._buf.clear()

    def handle_endtag(self, tag):
        if tag == "cite" and self._in_cite:
            text = "".join(self._buf).strip()
            if text.startswith("http"):
                self.links.append(text)
            self._in_cite = False
            self._buf.clear()

    def handle_data(self, data):
        if self._in_cite:
            self._buf.append(data)


def _extract_links_generic(html: str) -> list:
    p = _LinkExtractor()
    try:
        p.feed(html)
    except Exception:
        pass
    return p.links


# Google-specific: extract from /url?q= redirects AND raw href= links
_GOOGLE_URL_Q_RE = re.compile(r'/url\?(?:[^"]*?&)?q=(https?[^&"]+)', re.IGNORECASE)
_GOOGLE_DIRECT_HREF_RE = re.compile(r'<a[^>]+href="(https?://[^"]+)"', re.IGNORECASE)


def _extract_google_links(html: str) -> list:
    """Pull real destination URLs from Google's HTML SERP."""
    links = []
    # 1) /url?q=... redirect form (classic SERP)
    for m in _GOOGLE_URL_Q_RE.finditer(html):
        try:
            links.append(unquote(m.group(1)))
        except Exception:
            continue
    # 2) Direct href= (newer SERPs sometimes use direct links)
    for m in _GOOGLE_DIRECT_HREF_RE.finditer(html):
        url = m.group(1)
        if "google.com" in url or "/url?" in url or "/search?" in url:
            continue
        links.append(url)
    return links


# DDG extraction
_DDG_LINK_RE = re.compile(r'class="result__a"[^>]*href="(https?://[^"]+)"', re.IGNORECASE)
_DDG_UDDG_RE = re.compile(r'uddg=(https?[^&"]+)', re.IGNORECASE)


def _extract_ddg_links(html: str) -> list:
    links = []
    for m in _DDG_LINK_RE.finditer(html):
        links.append(unquote(m.group(1)))
    for m in _DDG_UDDG_RE.finditer(html):
        links.append(unquote(m.group(1)))
    return links


# ── Engine-specific noise filters ────────────────────────────────────────────
_GOOGLE_NOISE = re.compile(
    r"google\.com|gstatic\.com|googleusercontent\.com|googleadservices\.com|"
    r"googlesyndication\.com|youtube\.com/redirect|doubleclick\.net|"
    r"webcache\.googleusercontent",
    re.IGNORECASE,
)
_YAHOO_NOISE = re.compile(r"yimg\.com|yahoo\.com|doubleclick\.net|googleadservices", re.IGNORECASE)
_DDG_NOISE = re.compile(r"duckduckgo\.com|duck\.com", re.IGNORECASE)
_STATIC_EXT = re.compile(r"\.(css|js|png|jpg|jpeg|gif|svg|ico|webp|woff2?|ttf|eot)(\?|$)", re.IGNORECASE)
_YAHOO_RU_PATH = re.compile(r"/RU=([^/&]+)")


# ══════════════════════════════════════════════════════════════════════════════
# ─── GOOGLE SEARCH FETCHER (synchronous, with consent + CAPTCHA handling) ────
# ══════════════════════════════════════════════════════════════════════════════

# Pre-baked consent cookie (well-known accepted-consent value).
GOOGLE_CONSENT_COOKIES = {
    "CONSENT": "YES+srp.gws-20240101-0-RC1.en+FX+999",
    "SOCS":    "CAESHAgBEhJnd3NfMjAyNDAxMjMtMF9SQzIaAmVuIAEaBgiAuJevBg",
}


def fetch_page_google(dork: str, page: int, max_res: int,
                      chunk_id: int, proxy: str | None,
                      stop_ev: threading.Event) -> tuple[list, bool]:
    """
    Synchronous Google SERP scraper.
    - Fresh TLS+headers session per attempt
    - Pre-set consent cookies + auto-handle consent page
    - CAPTCHA detection → mark proxy bad, rotate, exponential backoff
    - Returns (urls, is_degraded)
    """
    start_offset = (page - 1) * 10
    params = {
        "q":     dork,
        "start": start_offset,
        "num":   min(max_res, 10),
        "hl":    "en",
        "gl":    "us",
        "ie":    "UTF-8",
        "oe":    "UTF-8",
        "pws":   "0",   # disable personalization
    }

    current_proxy = proxy
    last_html_size = 0

    for attempt in range(MAX_RETRIES):
        if stop_ev.is_set():
            return [], False

        sess, headers, _ = _create_random_session(current_proxy)
        # Inject consent cookies
        for k, v in GOOGLE_CONSENT_COOKIES.items():
            sess.cookies.set(k, v, domain=".google.com")
        headers["Referer"] = "https://www.google.com/"

        try:
            resp = sess.get(
                "https://www.google.com/search",
                params=params,
                headers=headers,
                timeout=20,
                allow_redirects=True,
            )
            status = resp.status_code
            html = resp.text
            last_html_size = len(html)
            size_kb = last_html_size / 1024

            log.debug(f"[C{chunk_id}][GOOGLE] p{page} attempt={attempt+1} "
                      f"status={status} size={size_kb:.1f}KB proxy={current_proxy or 'direct'}")

            # Rate limit
            if status == 429:
                backoff = (2 ** attempt) * random.uniform(5.0, 10.0)
                log.warning(f"[C{chunk_id}][GOOGLE] p{page} 429 — backoff {backoff:.1f}s")
                if current_proxy:
                    PROXY_POOL.mark_bad(current_proxy)
                    current_proxy = PROXY_POOL.acquire(exclude=current_proxy)
                _sleep_interruptible(backoff, stop_ev)
                continue

            # Hard block
            if status in (403, 503):
                log.warning(f"[C{chunk_id}][GOOGLE] p{page} status={status} — block suspected")
                if current_proxy:
                    PROXY_POOL.mark_bad(current_proxy)
                    current_proxy = PROXY_POOL.acquire(exclude=current_proxy)
                backoff = (2 ** attempt) * random.uniform(4.0, 8.0)
                _sleep_interruptible(backoff, stop_ev)
                continue

            if status != 200:
                log.warning(f"[C{chunk_id}][GOOGLE] p{page} non-200 status={status}")
                return [], False

            # Consent page → re-set cookies and retry
            if _is_google_consent(html):
                log.info(f"[C{chunk_id}][GOOGLE] p{page} consent page detected — retrying with cookies")
                # Cookies are already set; just retry with backoff
                _sleep_interruptible(random.uniform(1.0, 2.5), stop_ev)
                continue

            # CAPTCHA → flag proxy bad, rotate, exponential backoff
            if _is_google_captcha(html):
                log.warning(f"[C{chunk_id}][GOOGLE] p{page} 🔴 CAPTCHA detected!")
                if current_proxy:
                    PROXY_POOL.mark_bad(current_proxy)
                    current_proxy = PROXY_POOL.acquire(exclude=current_proxy)
                # Trigger Tor identity rotation if Tor is being used
                if current_proxy == TOR_PROXY:
                    rotate_tor_identity_sync()
                backoff = (2 ** attempt) * random.uniform(8.0, 18.0)
                _sleep_interruptible(backoff, stop_ev)
                continue

            if _is_degraded(html, "google"):
                log.warning(f"[C{chunk_id}][GOOGLE] p{page} degraded ({size_kb:.1f}KB)")
                if attempt < MAX_RETRIES - 1:
                    _sleep_interruptible((2 ** attempt) * random.uniform(2.0, 5.0), stop_ev)
                    continue
                return [], True

            raw = _extract_google_links(html)
            urls = []
            for u in raw:
                if not u.startswith("http"):
                    continue
                if _GOOGLE_NOISE.search(u) or _STATIC_EXT.search(u):
                    continue
                urls.append(u)
            urls = list(dict.fromkeys(urls))[:max_res]
            log.info(f"[C{chunk_id}][GOOGLE] p{page} → {len(urls)} URLs (attempt={attempt+1})")
            if current_proxy:
                PROXY_POOL.mark_good(current_proxy)
            return urls, False

        except CurlError as exc:
            if _is_proxy_error(exc) and current_proxy:
                log.warning(f"[C{chunk_id}][GOOGLE] p{page} proxy error: {exc} — rotating")
                PROXY_POOL.mark_bad(current_proxy)
                current_proxy = PROXY_POOL.acquire(exclude=current_proxy)
                _sleep_interruptible(random.uniform(1.5, 3.0), stop_ev)
                continue
            backoff = (2 ** attempt) * random.uniform(2.0, 4.0)
            log.warning(f"[C{chunk_id}][GOOGLE] p{page} CurlError={exc} — retry {backoff:.1f}s")
            _sleep_interruptible(backoff, stop_ev)
        except Exception as exc:
            log.error(f"[C{chunk_id}][GOOGLE] p{page} unexpected: {exc}")
            return [], False
        finally:
            try: sess.close()
            except Exception: pass

    log.warning(f"[C{chunk_id}][GOOGLE] p{page} all {MAX_RETRIES} attempts exhausted")
    return [], True


# ══════════════════════════════════════════════════════════════════════════════
# ─── YAHOO FETCHER ───────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

def fetch_page_yahoo(dork: str, page: int, max_res: int,
                     chunk_id: int, proxy: str | None,
                     stop_ev: threading.Event) -> tuple[list, bool]:
    params = {
        "p":  dork,
        "b":  (page - 1) * 10 + 1,
        "pz": min(max_res, 10),
        "vl": "lang_en",
    }
    current_proxy = proxy

    for attempt in range(MAX_RETRIES):
        if stop_ev.is_set():
            return [], False
        sess, headers, _ = _create_random_session(current_proxy)
        headers["Referer"] = "https://search.yahoo.com/"
        try:
            resp = sess.get(
                "https://search.yahoo.com/search",
                params=params, headers=headers, timeout=20,
            )
            status = resp.status_code
            html = resp.text
            size_kb = len(html) / 1024

            if status == 429:
                backoff = (2 ** attempt) * random.uniform(4.0, 8.0)
                if current_proxy:
                    PROXY_POOL.mark_bad(current_proxy)
                    current_proxy = PROXY_POOL.acquire(exclude=current_proxy)
                _sleep_interruptible(backoff, stop_ev)
                continue
            if status != 200:
                return [], False
            if _is_captcha_generic(html):
                if current_proxy:
                    PROXY_POOL.mark_bad(current_proxy)
                    current_proxy = PROXY_POOL.acquire(exclude=current_proxy)
                _sleep_interruptible((2 ** attempt) * random.uniform(5.0, 12.0), stop_ev)
                continue
            if _is_degraded(html, "yahoo"):
                if attempt < MAX_RETRIES - 1:
                    _sleep_interruptible((2 ** attempt) * random.uniform(2.0, 5.0), stop_ev)
                    continue
                return [], True

            raw = _extract_links_generic(html)
            urls = []
            for u in raw:
                if not u.startswith("http"):
                    continue
                if "r.search.yahoo.com" in u or "/r/" in u:
                    parsed = urlparse(u)
                    qs = parse_qs(parsed.query)
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
            if current_proxy:
                PROXY_POOL.mark_good(current_proxy)
            return urls, False

        except CurlError as exc:
            if _is_proxy_error(exc) and current_proxy:
                PROXY_POOL.mark_bad(current_proxy)
                current_proxy = PROXY_POOL.acquire(exclude=current_proxy)
                _sleep_interruptible(random.uniform(1.5, 3.0), stop_ev)
                continue
            _sleep_interruptible((2 ** attempt) * random.uniform(2.0, 4.0), stop_ev)
        except Exception as exc:
            log.error(f"[C{chunk_id}][YAHOO] p{page} unexpected: {exc}")
            return [], False
        finally:
            try: sess.close()
            except Exception: pass

    return [], True


# ══════════════════════════════════════════════════════════════════════════════
# ─── DUCKDUCKGO FETCHER ──────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

def fetch_page_duckduckgo(dork: str, page: int, max_res: int,
                          chunk_id: int, proxy: str | None,
                          stop_ev: threading.Event) -> tuple[list, bool]:
    if page > 1:
        # DDG HTML pagination is unreliable; only return for first page
        return [], False

    data = {"q": dork, "b": "", "kl": "us-en", "df": ""}
    current_proxy = proxy

    for attempt in range(MAX_RETRIES):
        if stop_ev.is_set():
            return [], False
        sess, headers, _ = _create_random_session(current_proxy)
        headers["Referer"] = "https://duckduckgo.com/"
        headers["Origin"] = "https://html.duckduckgo.com"
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        try:
            resp = sess.post(
                "https://html.duckduckgo.com/html/",
                data=data, headers=headers, timeout=20,
            )
            status = resp.status_code
            html = resp.text

            if status == 429:
                if current_proxy:
                    PROXY_POOL.mark_bad(current_proxy)
                    current_proxy = PROXY_POOL.acquire(exclude=current_proxy)
                _sleep_interruptible((2 ** attempt) * random.uniform(5.0, 10.0), stop_ev)
                continue
            if status != 200:
                return [], False
            if _is_captcha_generic(html):
                if current_proxy:
                    PROXY_POOL.mark_bad(current_proxy)
                    current_proxy = PROXY_POOL.acquire(exclude=current_proxy)
                _sleep_interruptible((2 ** attempt) * random.uniform(5.0, 12.0), stop_ev)
                continue
            if _is_degraded(html, "duckduckgo"):
                if attempt < MAX_RETRIES - 1:
                    _sleep_interruptible((2 ** attempt) * random.uniform(2.0, 5.0), stop_ev)
                    continue
                return [], True

            raw = _extract_ddg_links(html)
            urls = [
                u for u in raw
                if u.startswith("http")
                and not _DDG_NOISE.search(u)
                and not _STATIC_EXT.search(u)
            ]
            urls = list(dict.fromkeys(urls))[:max_res]
            log.info(f"[C{chunk_id}][DDG] p{page} → {len(urls)} URLs (attempt={attempt+1})")
            if current_proxy:
                PROXY_POOL.mark_good(current_proxy)
            return urls, False

        except CurlError as exc:
            if _is_proxy_error(exc) and current_proxy:
                PROXY_POOL.mark_bad(current_proxy)
                current_proxy = PROXY_POOL.acquire(exclude=current_proxy)
                _sleep_interruptible(random.uniform(1.5, 3.0), stop_ev)
                continue
            _sleep_interruptible((2 ** attempt) * random.uniform(2.0, 4.0), stop_ev)
        except Exception as exc:
            log.error(f"[C{chunk_id}][DDG] p{page} unexpected: {exc}")
            return [], False
        finally:
            try: sess.close()
            except Exception: pass

    return [], True


# ══════════════════════════════════════════════════════════════════════════════
# ─── HELPERS ─────────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

def _sleep_interruptible(seconds: float, stop_ev: threading.Event) -> None:
    """Sleep up to `seconds` but wake immediately if stop_ev is set."""
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if stop_ev.is_set():
            return
        time.sleep(min(0.3, end - time.monotonic()))


ENGINE_FETCHERS = {
    "google":     fetch_page_google,
    "yahoo":      fetch_page_yahoo,
    "duckduckgo": fetch_page_duckduckgo,
}


def fetch_dork_all_pages(dork: str, engine: str, pages: list, max_res: int,
                         chunk_id: int, proxy: str | None,
                         stop_ev: threading.Event) -> tuple[list, int]:
    """Fetch all requested pages for one dork on one engine."""
    if engine == "duckduckgo":
        target_pages = [min(pages)]
    else:
        target_pages = sorted(pages)

    fetcher = ENGINE_FETCHERS[engine]
    all_urls = []
    degraded_total = 0

    for idx, page in enumerate(target_pages):
        if stop_ev.is_set():
            break
        if idx > 0:
            _sleep_interruptible(random.uniform(0.3, 0.9), stop_ev)
        urls, degraded = fetcher(dork, page, max_res, chunk_id, proxy, stop_ev)
        if degraded:
            degraded_total += 1
        all_urls.extend(urls)

    return all_urls, degraded_total


# ══════════════════════════════════════════════════════════════════════════════
# ─── CHUNK WORKER (runs in thread) ───────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

def run_chunk_thread(chunk_id: int, dorks: list, engines: list, pages: list,
                     max_res: int, use_tor: bool, min_score: int,
                     stop_ev: threading.Event, progress_q: queue_module.Queue) -> dict:
    """
    Process one chunk of dorks in a single thread.
    Each dork rotates engines round-robin; each thread owns one proxy at a time.
    """
    threading.current_thread().name = f"chunk-{chunk_id}"
    total = len(dorks)

    # Acquire initial proxy for this chunk
    if use_tor:
        proxy = TOR_PROXY
    else:
        proxy = PROXY_POOL.acquire() if PROXY_ENABLED else None

    chunk_scored = []
    chunk_raw = 0
    chunk_degraded = 0
    processed = 0
    empty_count = 0
    consecutive_hits = 0
    empty_streak = 0
    eidx = 0

    log.info(f"[C{chunk_id}] Starting — {total} dorks | engines={engines} | proxy={proxy or 'direct'}")

    for dork in dorks:
        if stop_ev.is_set():
            log.info(f"[C{chunk_id}] Stop requested — exiting at {processed}/{total}")
            break

        engine = engines[eidx % len(engines)]
        eidx += 1

        log.info(f"[C{chunk_id}][{engine.upper()}] {dork[:60]}")

        try:
            raw, deg_cnt = fetch_dork_all_pages(
                dork, engine, pages, max_res, chunk_id, proxy, stop_ev
            )
        except Exception as exc:
            log.warning(f"[C{chunk_id}] fetch error on dork: {exc}")
            raw, deg_cnt = [], 0

        scored = filter_scored(raw, min_score)
        chunk_scored.extend(scored)
        chunk_raw += len(raw)
        chunk_degraded += deg_cnt
        processed += 1

        if len(raw) == 0:
            empty_count += 1
            consecutive_hits = 0
            empty_streak += 1
        else:
            consecutive_hits += 1
            empty_streak = 0

        # Push progress
        try:
            progress_q.put_nowait({
                "chunk_id": chunk_id,
                "processed": processed,
                "total": total,
                "raw": len(raw),
                "kept": len(scored),
            })
        except queue_module.Full:
            pass

        # Adaptive delay
        if consecutive_hits >= FAST_STREAK_THRESHOLD:
            delay = random.uniform(FAST_MIN_DELAY, FAST_MAX_DELAY)
        else:
            delay = random.uniform(MIN_DELAY, MAX_DELAY)
        if empty_streak >= 3:
            delay += min(empty_streak * 2.0, 15.0)

        # If empty rate is high, swap proxy proactively
        if processed >= 5:
            empty_rate = empty_count / processed
            if empty_rate >= EMPTY_RATE_SLOWDOWN and PROXY_ENABLED and not use_tor:
                new_proxy = PROXY_POOL.acquire(exclude=proxy)
                if new_proxy and new_proxy != proxy:
                    log.info(f"[C{chunk_id}] Empty rate {empty_rate:.0%} — rotating proxy")
                    proxy = new_proxy

        _sleep_interruptible(delay, stop_ev)

    success_rate = (processed - empty_count) / max(processed, 1)
    log.info(
        f"[C{chunk_id}] Done — processed={processed}/{total} "
        f"raw={chunk_raw} kept={len(chunk_scored)} "
        f"degraded={chunk_degraded} success_rate={success_rate:.0%}"
    )

    return {
        "chunk_id": chunk_id,
        "scored": chunk_scored,
        "raw_count": chunk_raw,
        "degraded_count": chunk_degraded,
        "processed": processed,
        "empty_count": empty_count,
    }


# ══════════════════════════════════════════════════════════════════════════════
# ─── JOB RUNNER (orchestrates ThreadPoolExecutor + Telegram updates) ─────────
# ══════════════════════════════════════════════════════════════════════════════

async def run_dork_job(chat_id: int, dorks: list, context) -> None:
    sess = get_session(chat_id)
    engines = sess.get("engines", list(ENGINES))
    workers_n = min(sess.get("workers", WORKERS_PER_CHUNK), MAX_WORKERS_PER_CHUNK)
    max_res = sess.get("max_results", MAX_RESULTS)
    pages = sess.get("pages", [1])
    use_tor = sess.get("tor", False)
    min_score = sess.get("min_score", 30)
    n_chunks = max(1, sess.get("chunks", N_CHUNKS))

    total_dorks = len(dorks)
    pages_str = ", ".join(str(p) for p in pages)
    start_time = time.time()

    chunk_size = max(1, -(-total_dorks // n_chunks))
    chunks = [dorks[i:i + chunk_size] for i in range(0, total_dorks, chunk_size)]
    actual_chunks = len(chunks)

    # Tor in pool as fallback?
    if use_tor:
        PROXY_POOL.enable_tor_fallback(True)

    log.info(
        f"[JOB][{chat_id}] Starting: {total_dorks} dorks → "
        f"{actual_chunks} chunks (threads) | engines={engines} | "
        f"delay={MIN_DELAY}–{MAX_DELAY}s"
    )

    # Output file
    tmp_file = tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", delete=False,
        prefix=f"dork_{chat_id}_", suffix=".txt",
    )
    tmp_path = tmp_file.name
    tmp_file.write(f"# Dork Parser v19.0 — SQL Targeted Results\n")
    tmp_file.write(f"# Date   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    tmp_file.write(f"# Dorks  : {total_dorks} | Pages: {pages_str}\n")
    tmp_file.write(f"# Filter : SQL ≥{min_score} | Chunks: {actual_chunks}\n")
    tmp_file.close()

    # Status header
    if use_tor:
        proxy_info = "🧅 TOR (fallback)"
    elif PROXY_ENABLED and PROXY_POOL.active_count() > 0:
        proxy_info = f"🔄 {PROXY_POOL.active_count()}/{PROXY_POOL.total_count()} active proxies"
    elif not PROXY_ENABLED:
        proxy_info = "⏸ Proxy disabled"
    else:
        proxy_info = "🔓 Direct (no proxy)"

    status_msg = await context.bot.send_message(
        chat_id,
        f"🕷 DORK PARSER v19.0 — STARTED\n"
        f"{'━'*30}\n"
        f"📋 Dorks   : {total_dorks}\n"
        f"📄 Pages   : {pages_str}\n"
        f"⚡ Threads : {actual_chunks} chunks × workers={workers_n}\n"
        f"🔍 Engines : {' + '.join(e.upper() for e in engines)}\n"
        f"🛡 Filter  : SQL ≥{min_score}\n"
        f"🌐 Network : {proxy_info}\n"
        f"🔒 TLS     : Random rotation ({len(BROWSER_PROFILES)} profiles)\n"
        f"{'━'*30}\n⏳ Spawning threads...",
    )

    stop_ev = threading.Event()
    active_stop_evs[chat_id] = stop_ev
    progress_q: queue_module.Queue = queue_module.Queue(maxsize=total_dorks * 2)

    chunk_counters = {i: {"processed": 0, "total": len(chunks[i])} for i in range(actual_chunks)}
    agg_raw = [0]
    agg_kept = [0]
    last_edit = [0.0]
    total_processed = [0]
    updater_stop = threading.Event()

    # ── Status updater (background thread that updates Telegram) ─────────────
    def _status_updater_thread():
        while not updater_stop.is_set():
            drained = False
            try:
                while True:
                    ev = progress_q.get_nowait()
                    cid = ev["chunk_id"]
                    chunk_counters[cid]["processed"] = ev["processed"]
                    agg_raw[0] += ev["raw"]
                    agg_kept[0] += ev["kept"]
                    total_processed[0] += 1
                    drained = True
            except queue_module.Empty:
                pass

            if drained and time.time() - last_edit[0] > 4.0:
                proc = total_processed[0]
                pct = int(proc / total_dorks * 100) if total_dorks else 100
                bar = "█" * (pct // 10) + "░" * (10 - pct // 10)
                elapsed = int(time.time() - start_time)
                eta = int((elapsed / proc) * (total_dorks - proc)) if proc else 0
                cinfo = " | ".join(
                    f"C{i}:{chunk_counters[i]['processed']}/{chunk_counters[i]['total']}"
                    for i in range(actual_chunks)
                )
                text = (
                    f"⚡ PARSING... [{actual_chunks} thread chunks]\n"
                    f"{'━'*30}\n"
                    f"[{bar}] {pct}%\n"
                    f"✅ Done    : {proc}/{total_dorks}\n"
                    f"🎯 SQL     : {agg_kept[0]}\n"
                    f"🗑 Raw drop: {agg_raw[0] - agg_kept[0]}\n"
                    f"🔄 Proxies : {PROXY_POOL.active_count()}/{PROXY_POOL.total_count()} active\n"
                    f"⏱ {elapsed}s | ETA {eta}s\n"
                    f"📦 {cinfo}\n"
                    f"{'━'*30}"
                )
                # Schedule edit on the asyncio loop
                try:
                    loop = asyncio.get_event_loop()
                except RuntimeError:
                    loop = None
                if loop and loop.is_running():
                    asyncio.run_coroutine_threadsafe(
                        _safe_edit(context, chat_id, status_msg.message_id, text),
                        loop,
                    )
                last_edit[0] = time.time()
            time.sleep(0.5)

    updater_thread = threading.Thread(target=_status_updater_thread, name="status-updater", daemon=True)
    updater_thread.start()

    # ── Submit chunks to ThreadPoolExecutor ──────────────────────────────────
    chunk_results = []
    executor = ThreadPoolExecutor(max_workers=actual_chunks, thread_name_prefix="chunk")
    future_map = {}
    try:
        for i, chunk_dorks in enumerate(chunks):
            if i > 0:
                await asyncio.sleep(random.uniform(*CHUNK_STAGGER_DELAY))
            fut = executor.submit(
                run_chunk_thread,
                i, chunk_dorks, engines, pages, max_res,
                use_tor, min_score, stop_ev, progress_q,
            )
            future_map[fut] = i

        # Wait for all chunks (with global job timeout)
        deadline = time.time() + JOB_TIMEOUT

        async def _await_futures():
            loop = asyncio.get_event_loop()
            for fut in as_completed(future_map):
                if time.time() > deadline:
                    log.warning(f"[JOB][{chat_id}] Global timeout — stopping")
                    stop_ev.set()
                try:
                    res = await loop.run_in_executor(None, fut.result, 1)
                    chunk_results.append(res)
                except Exception as exc:
                    log.error(f"[JOB][{chat_id}] Chunk {future_map[fut]} raised: {exc}")
                    chunk_results.append(exc)

        # Periodically yield to check stop
        wait_task = asyncio.create_task(_await_futures())
        try:
            await wait_task
        except asyncio.CancelledError:
            stop_ev.set()
            await asyncio.sleep(0.5)
            raise

    finally:
        stop_ev.set()
        updater_stop.set()
        executor.shutdown(wait=True, cancel_futures=False)
        active_jobs.pop(chat_id, None)
        active_stop_evs.pop(chat_id, None)

    # ── Merge + global deduplication ─────────────────────────────────────────
    seen_urls: set = set()
    all_scored: list = []
    total_raw = 0
    total_degraded = 0
    failed_chunks = 0

    for result in chunk_results:
        if isinstance(result, Exception):
            failed_chunks += 1
            continue
        for sc, url in result["scored"]:
            if url not in seen_urls:
                seen_urls.add(url)
                all_scored.append((sc, url))
        total_raw += result["raw_count"]
        total_degraded += result["degraded_count"]

    all_scored.sort(reverse=True)
    unique_cnt = len(all_scored)
    elapsed = int(time.time() - start_time)
    success_rate = unique_cnt / max(total_raw, 1)

    log.info(
        f"[JOB][{chat_id}] COMPLETE — dorks={total_dorks} raw={total_raw} "
        f"unique={unique_cnt} degraded={total_degraded} "
        f"failed_chunks={failed_chunks} elapsed={elapsed}s"
    )

    # ── Bucket and write output ──────────────────────────────────────────────
    high   = [(sc, u) for sc, u in all_scored if sc >= 70]
    medium = [(sc, u) for sc, u in all_scored if 40 <= sc < 70]
    low    = [(sc, u) for sc, u in all_scored if sc < 40]

    with open(tmp_path, "a", encoding="utf-8") as f:
        if high:
            f.write(f"# ── HIGH VALUE (score ≥70) — {len(high)} URLs\n")
            for _, u in high: f.write(f"{u}\n")
        if medium:
            f.write(f"\n# ── MEDIUM VALUE (score 40–69) — {len(medium)} URLs\n")
            for _, u in medium: f.write(f"{u}\n")
        if low and min_score < 40:
            f.write(f"\n# ── LOW VALUE (score <40) — {len(low)} URLs\n")
            for _, u in low: f.write(f"{u}\n")

    stopped_tag = " (STOPPED EARLY)" if stop_ev.is_set() and total_processed[0] < total_dorks else ""

    try:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=status_msg.message_id,
            text=(
                f"🏁 JOB COMPLETE!{stopped_tag}\n"
                f"{'━'*30}\n"
                f"📋 Dorks    : {total_dorks}\n"
                f"📄 Pages    : {pages_str}\n"
                f"⚡ Chunks   : {actual_chunks}\n"
                f"🔍 Raw      : {total_raw}\n"
                f"🎯 SQL      : {unique_cnt} unique URLs\n"
                f"🗑 Dropped  : {total_raw - unique_cnt}\n"
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
                    f"⚡ {actual_chunks} threads"
                ),
            )
    else:
        await context.bot.send_message(
            chat_id,
            "⚠️ No URLs matched the filter criteria.\n"
            "Try lowering /filter or adding more pages."
        )

    try: os.unlink(tmp_path)
    except OSError: pass


async def _safe_edit(context, chat_id, msg_id, text):
    try:
        await context.bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text=text)
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# ─── URL CLEANER JOB (preserved, simplified) ─────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

async def run_url_clean_job(chat_id: int, raw_lines: list, context) -> None:
    stop_ev = threading.Event()
    active_stop_evs[chat_id] = stop_ev
    total_input = len(raw_lines)

    status_msg = await context.bot.send_message(
        chat_id,
        f"🧹 URL CLEANER STARTED\n"
        f"{'━'*30}\n"
        f"📥 Input   : {total_input} URLs\n"
        f"⏳ Processing..."
    )

    loop = asyncio.get_event_loop()
    full_stats = await loop.run_in_executor(None, filter_urls, raw_lines)
    final_urls = full_stats["kept"]
    removed = total_input - len(final_urls)

    output_path = Path("results") / "cleaned_urls.txt"
    output_path.parent.mkdir(exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(f"# URL Cleaner — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"# Input: {total_input} | Kept: {len(final_urls)} | Removed: {removed}\n")
        f.write("─" * 60 + "\n\n")
        for u in final_urls:
            f.write(u + "\n")

    try:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=status_msg.message_id,
            text=(
                f"✅ URL CLEANER DONE\n"
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
                caption=f"🧹 Cleaned URLs\n✅ {len(final_urls)} kept from {total_input}",
            )
    else:
        await context.bot.send_message(chat_id, "⚠️ No URLs passed the filters.")

    active_stop_evs.pop(chat_id, None)
    active_jobs.pop(chat_id, None)


# ══════════════════════════════════════════════════════════════════════════════
# ─── UI HELPERS ──────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

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
            rows.append(row); row = []
    if row: rows.append(row)
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

    if PROXY_ENABLED and PROXY_POOL.total_count() > 0:
        proxy_status = f"🔄 {PROXY_POOL.active_count()}/{PROXY_POOL.total_count()} proxies active"
    elif not PROXY_ENABLED:
        proxy_status = "⏸ Proxies DISABLED"
    else:
        proxy_status = "🔓 No proxy pool"

    await update.message.reply_text(
        "🕷 DORK PARSER v19.0 — MULTI-THREADED STEALTH\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🧵 ThreadPoolExecutor architecture\n"
        "🔒 Dynamic TLS rotation (9 profiles)\n"
        "🔍 Google + Yahoo + DuckDuckGo\n"
        "🔄 Auto proxy health checks + cooldown\n"
        f"{proxy_status}\n\n"
        "📌 Core Commands:\n"
        "  /dork <q>   — single dork\n"
        "  /clean      — URL list cleaner mode\n"
        "  /pages      — pick pages 1-70\n"
        "  /workers N  — workers per chunk (1-16)\n"
        "  /chunks N   — parallel chunk count (1-8)\n"
        "  /engine X   — google|yahoo|ddg|all\n"
        "  /tor        — toggle Tor IP rotation\n"
        "  /filter N   — SQL score filter (0-100)\n"
        "  /stop       — stop & get partial results\n\n"
        "🔄 Proxy Commands:\n"
        "  /addproxy <url>      — add + auto-test\n"
        "  /removeproxy [i|url] — remove\n"
        "  /proxylist           — view all\n"
        "  /testproxy <url>     — manual test\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        reply_markup=InlineKeyboardMarkup(kb),
    )


async def cmd_dork(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not context.args:
        await update.message.reply_text("Usage: /dork inurl:login.php?id=")
        return
    if chat_id in active_jobs:
        await update.message.reply_text("⚠️ Job running! Use /stop first.")
        return
    dork = " ".join(context.args)
    s = get_session(chat_id)
    await update.message.reply_text(
        f"🔍 {dork[:60]}\n📄 Pages: {', '.join(str(p) for p in s.get('pages', [1]))}"
        f"{'  🧅TOR' if s.get('tor') else ''}"
    )
    task = asyncio.create_task(run_dork_job(chat_id, [dork], context))
    active_jobs[chat_id] = task


async def cmd_pages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
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
    sess = get_session(chat_id)

    if context.args and context.args[0].lower() in ("on", "off"):
        new_val = context.args[0].lower() == "on"
    else:
        new_val = not sess.get("tor", False)

    old_val = sess.get("tor", False)
    sess["tor"] = new_val

    if new_val and not old_val:
        tor_enabled_users += 1
        if tor_enabled_users == 1:
            start_tor_rotation()
        PROXY_POOL.enable_tor_fallback(True)
        await update.message.reply_text(
            "🧅 TOR ENABLED\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "Tor IP will rotate every 2 minutes.\n"
            "Tor is now part of the proxy fallback pool.\n\n"
            "⚠️ Speed will be slower."
        )
    elif not new_val and old_val:
        tor_enabled_users = max(0, tor_enabled_users - 1)
        if tor_enabled_users == 0:
            stop_tor_rotation()
            PROXY_POOL.enable_tor_fallback(False)
        await update.message.reply_text("🔓 TOR DISABLED — Direct/proxy connection.")
    else:
        await update.message.reply_text(f"Tor is already {'ON' if new_val else 'OFF'}.")


async def cmd_filter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    sess = get_session(chat_id)
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
    s = get_session(chat_id)

    if PROXY_ENABLED and PROXY_POOL.total_count() > 0:
        proxy_line = f"🔄 Proxies  : {PROXY_POOL.active_count()}/{PROXY_POOL.total_count()} active\n"
    elif not PROXY_ENABLED:
        proxy_line = f"⏸ Proxies  : DISABLED ({PROXY_POOL.total_count()} loaded)\n"
    else:
        proxy_line = "🔓 Proxies  : none loaded\n"

    await update.message.reply_text(
        f"⚙️ SETTINGS\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚡ Chunks   : {s.get('chunks', N_CHUNKS)} parallel threads\n"
        f"🔧 Workers  : {s.get('workers', WORKERS_PER_CHUNK)}/chunk (max {MAX_WORKERS_PER_CHUNK})\n"
        f"📄 Pages    : {', '.join(str(p) for p in s.get('pages', [1]))} (1–70)\n"
        f"🔍 Engines  : {'+'.join(e.upper() for e in s.get('engines', ENGINES))}\n"
        f"📊 Max/Page : {s.get('max_results', MAX_RESULTS)}\n"
        f"🛡 SQL ≥    : {s.get('min_score', 30)}\n"
        f"🧅 Tor      : {'ON' if s.get('tor') else 'OFF'}\n"
        f"⏱ Delay    : {MIN_DELAY}–{MAX_DELAY}s | Fast: {FAST_MIN_DELAY}–{FAST_MAX_DELAY}s\n"
        f"🔒 TLS      : {len(BROWSER_PROFILES)} impersonations rotating\n"
        f"{proxy_line}"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"/workers N | /chunks N | /maxres N\n"
        f"/engine X  | /filter N\n"
        f"/pages     | /tor\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔄 Proxy Management:\n"
        f"/addproxy <url>      — add + auto-test\n"
        f"/removeproxy [i|url] — remove\n"
        f"/proxylist           — view pool\n"
        f"/testproxy <url>     — test"
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
            f"Each chunk runs in its own thread."
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
            "google":     ["google"],
            "g":          ["google"],
            "yahoo":      ["yahoo"],
            "y":          ["yahoo"],
            "duckduckgo": ["duckduckgo"],
            "ddg":        ["duckduckgo"],
            "all":        list(ENGINES),
            "both":       ["google", "yahoo"],
        }
        engines = engine_map.get(choice, list(ENGINES))
        get_session(chat_id)["engines"] = engines
        await update.message.reply_text(f"✅ Engines: {'+'.join(e.upper() for e in engines)}")
    except Exception:
        await update.message.reply_text("Usage: /engine google|yahoo|ddg|all|both")


async def cmd_clean(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🧹 URL CLEANER MODE\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "Upload a .txt file containing one URL per line.\n"
        "Filters: blocked domains, no-query, >200 chars, invalid, dupes.\n"
        "📁 Results → cleaned_urls.txt"
    )


async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    stop_ev = active_stop_evs.get(chat_id)
    job = active_jobs.get(chat_id)

    if stop_ev and job and not job.done():
        stop_ev.set()
        await update.message.reply_text(
            "⏹ STOP REQUESTED\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "Threads are draining...\n"
            "📦 Partial results will be sent automatically."
        )
    elif job and not job.done():
        job.cancel()
        active_jobs.pop(chat_id, None)
        await update.message.reply_text("🛑 Job force-stopped.")
    else:
        await update.message.reply_text("💤 No active job to stop.")


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    job = active_jobs.get(chat_id)
    if job and not job.done():
        await update.message.reply_text("⚡ Job RUNNING")
    else:
        await update.message.reply_text("💤 No active job")


# ── Proxy commands ───────────────────────────────────────────────────────────

async def cmd_addproxy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(
            "➕ ADD PROXY\nUsage: /addproxy <proxy_url>\n\n"
            "Formats: socks5://user:pass@host:port, http://host:port, https://host:port\n"
            f"Current pool: {PROXY_POOL.total_count()}"
        )
        return

    proxy_url = context.args[0].strip()
    if not _validate_proxy_url(proxy_url):
        await update.message.reply_text("❌ Invalid proxy format.")
        return
    if proxy_url in PROXY_POOL.all_proxies():
        await update.message.reply_text("⚠️ Proxy already in pool.")
        return

    PROXY_POOL.add(proxy_url)
    PROXY_POOL.persist()
    info = _parse_proxy_info(proxy_url)

    wait_msg = await update.message.reply_text(
        f"✅ Added — testing now...\n"
        f"🔌 {info['protocol']} {info['host']}:{info['port']}"
    )

    # Test in background thread to not block asyncio
    loop = asyncio.get_event_loop()
    ok, latency, ext_ip = await loop.run_in_executor(None, test_proxy, proxy_url)

    if ok:
        PROXY_POOL.mark_good(proxy_url)
        text = (
            f"✅ PROXY ADDED & TESTED\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🔌 {info['protocol']} {info['host']}:{info['port']}\n"
            f"⏱ Latency : {latency} ms\n"
            f"🌍 IP      : {ext_ip}\n"
            f"📦 Pool    : {PROXY_POOL.active_count()}/{PROXY_POOL.total_count()} active"
        )
    else:
        PROXY_POOL.purge_dead(proxy_url)
        PROXY_POOL.persist()
        text = (
            f"❌ PROXY FAILED & REMOVED\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🔌 {info['protocol']} {info['host']}:{info['port']}\n"
            f"💬 Test failed — proxy purged"
        )

    try:
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=wait_msg.message_id,
            text=text,
        )
    except Exception:
        pass


async def cmd_removeproxy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    proxies = PROXY_POOL.all_proxies()
    if not context.args:
        if not proxies:
            await update.message.reply_text("📭 Pool empty.")
            return
        lines = ["📋 PROXY POOL", "━" * 22]
        for i, p in enumerate(proxies, 1):
            info = _parse_proxy_info(p)
            lines.append(f"{i}. [{info['protocol']}] {info['host']}:{info['port']}")
        lines.append("Usage: /removeproxy <index> or <url>")
        await update.message.reply_text("\n".join(lines))
        return

    arg = context.args[0].strip()
    removed = None
    try:
        idx = int(arg) - 1
        removed = PROXY_POOL.remove_by_index(idx)
    except ValueError:
        if PROXY_POOL.remove(arg):
            removed = arg

    if removed:
        PROXY_POOL.persist()
        info = _parse_proxy_info(removed)
        await update.message.reply_text(
            f"🗑 REMOVED\n"
            f"🔌 {info['protocol']} {info['host']}:{info['port']}\n"
            f"📦 Remaining: {PROXY_POOL.total_count()}"
        )
    else:
        await update.message.reply_text("❌ Proxy not found.")


async def cmd_proxylist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    proxies = PROXY_POOL.all_proxies()
    if not proxies:
        note = "" if PROXY_ENABLED else "\n⚠️ PROXY_ENABLED=false"
        await update.message.reply_text(f"📭 Pool empty.{note}\nUse /addproxy <url>")
        return

    enabled_tag = "✅ ENABLED" if PROXY_ENABLED else "⏸ DISABLED"
    active = PROXY_POOL.active_count()
    lines = [
        f"🔄 PROXY POOL — {len(proxies)} proxies | {active} active | {enabled_tag}",
        "━" * 30,
    ]
    for i, p in enumerate(proxies, 1):
        info = _parse_proxy_info(p)
        auth_tag = " 🔐" if info["auth"] else ""
        tested_tag = " ✓" if PROXY_POOL.is_tested(p) else " ?"
        lines.append(f"{i:>2}.{tested_tag} [{info['protocol']:7s}] {info['host']}:{info['port']}{auth_tag}")
    lines.append("━" * 30)
    lines.append("✓ = passed health check")
    await update.message.reply_text("\n".join(lines))


async def cmd_testproxy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("🧪 Usage: /testproxy <proxy_url>")
        return
    proxy_url = context.args[0].strip()
    if not _validate_proxy_url(proxy_url):
        await update.message.reply_text("❌ Invalid format.")
        return

    info = _parse_proxy_info(proxy_url)
    wait_msg = await update.message.reply_text(
        f"🧪 Testing {info['protocol']} {info['host']}:{info['port']}..."
    )

    loop = asyncio.get_event_loop()
    ok, latency, ext_ip = await loop.run_in_executor(None, test_proxy, proxy_url)

    if ok:
        text = (
            f"✅ PROXY WORKING\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🔌 {info['protocol']} {info['host']}:{info['port']}\n"
            f"⏱ Latency  : {latency} ms\n"
            f"🌍 External : {ext_ip}\n"
        )
        if proxy_url not in PROXY_POOL.all_proxies():
            text += "\n➕ Use /addproxy to add it."
    else:
        text = (
            f"❌ PROXY FAILED\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🔌 {info['protocol']} {info['host']}:{info['port']}\n"
            f"💬 Unreachable or misconfigured"
        )

    try:
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=wait_msg.message_id,
            text=text,
        )
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# ─── FILE / TEXT / CALLBACK HANDLERS ─────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

def _looks_like_url_list(lines: list) -> bool:
    non_empty = [l for l in lines if l.strip() and not l.startswith("#")]
    if not non_empty: return False
    url_lines = sum(1 for l in non_empty if l.strip().startswith("http"))
    return url_lines / len(non_empty) >= 0.5


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    doc = update.message.document
    if chat_id in active_jobs:
        await update.message.reply_text("⚠️ Job running! /stop first.")
        return
    if not doc.file_name.endswith(".txt"):
        await update.message.reply_text("❌ Send a .txt file.")
        return
    await update.message.reply_text("📥 Reading file...")
    try:
        content = await (await context.bot.get_file(doc.file_id)).download_as_bytearray()
        lines = content.decode("utf-8", errors="replace").splitlines()

        if _looks_like_url_list(lines):
            raw_urls = [l.strip() for l in lines if l.strip() and not l.startswith("#")]
            if not raw_urls:
                await update.message.reply_text("❌ No URLs found.")
                return
            await update.message.reply_text(
                f"🧹 URL LIST detected — {len(raw_urls)} URLs\n🚀 Running URL Cleaner..."
            )
            task = asyncio.create_task(run_url_clean_job(chat_id, raw_urls, context))
            active_jobs[chat_id] = task
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
                f"{'🧅TOR' if s.get('tor') else '🔓 Direct/Proxy'}\n🚀 Starting..."
            )
            task = asyncio.create_task(run_dork_job(chat_id, dorks, context))
            active_jobs[chat_id] = task
    except Exception as exc:
        await update.message.reply_text(f"❌ Error: {exc}")


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    lines = [
        l.strip() for l in update.message.text.splitlines()
        if l.strip() and not l.startswith("#")
    ]
    if len(lines) > 1:
        if chat_id in active_jobs:
            await update.message.reply_text("⚠️ Job running! /stop first.")
            return
        s = get_session(chat_id)
        await update.message.reply_text(
            f"✅ {len(lines)} dorks | Pages: {', '.join(str(p) for p in s.get('pages', [1]))}\n🚀 Starting..."
        )
        task = asyncio.create_task(run_dork_job(chat_id, lines, context))
        active_jobs[chat_id] = task
    else:
        await update.message.reply_text(
            "Use /dork <q> or upload .txt\n/pages | /tor | /filter N | /chunks N"
        )


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    chat_id = query.message.chat_id
    sess = get_session(chat_id)

    if data.startswith("pg_"):
        cmd = data[3:]
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
            except Exception: pass
            return
        else:
            try:
                p = int(cmd)
                selected.remove(p) if p in selected else selected.append(p)
                selected = sorted(selected)
            except ValueError: pass
        sess["pages"] = selected
        try:
            await query.edit_message_text(
                f"📄 SELECT PAGES (1–70)\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"Selected: {', '.join(str(p) for p in selected) or 'none'}",
                reply_markup=page_keyboard(selected),
            )
        except Exception: pass
        return

    replies = {
        "m_bulk":     "📂 Upload a .txt file — URLs or dorks (auto-detected).",
        "m_single":   "🔍 /dork inurl:login.php?id=\nSet pages with /pages",
        "m_tor":      f"🧅 Tor is {'ON' if sess.get('tor') else 'OFF'}",
        "m_filter":   f"🛡 SQL Filter ≥{sess.get('min_score', 30)}\n/filter 70|40|0",
        "m_clean":    "🧹 Upload a .txt URL list to clean it.",
        "m_settings": (
            f"⚙️ Chunks:{sess.get('chunks', N_CHUNKS)} "
            f"Workers:{sess.get('workers', WORKERS_PER_CHUNK)} "
            f"Engines:{'+'.join(e.upper() for e in sess.get('engines', ENGINES))} "
            f"Score≥{sess.get('min_score', 30)} Tor:{'ON' if sess.get('tor') else 'OFF'} "
            f"Proxies:{PROXY_POOL.active_count()}/{PROXY_POOL.total_count()}"
        ),
        "m_help": (
            "📖 COMMANDS\n━━━━━━━━━━━━━━━━━━━\n"
            "/dork <q>         — single dork\n"
            "/clean            — URL cleaner\n"
            "/pages            — page selector\n"
            "/chunks N         — parallel threads\n"
            "/workers N        — workers/chunk\n"
            "/tor              — toggle Tor\n"
            "/engine X         — google|yahoo|ddg|all\n"
            "/filter N         — SQL score (0-100)\n"
            "/settings         — full config\n"
            "/maxres N         — results/page\n"
            "/stop             — stop & partial\n"
            "/status           — job status\n"
            "🔄 /addproxy /removeproxy /proxylist /testproxy"
        ),
    }

    if data == "m_pages":
        await query.message.reply_text(
            f"📄 SELECT PAGES (1–70)\nSelected: "
            f"{', '.join(str(p) for p in sess.get('pages', [1]))}",
            reply_markup=page_keyboard(sess.get("pages", [1])),
        )
    elif data in replies:
        await query.message.reply_text(replies[data])


# ══════════════════════════════════════════════════════════════════════════════
# ─── MAIN ────────────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

def main():
    if not BOT_TOKEN:
        log.critical("BOT_TOKEN not set!")
        raise SystemExit(1)

    # Load proxies and start health checker
    PROXY_POOL.load_from_sources()
    if PROXY_ENABLED and PROXY_POOL.total_count() > 0:
        start_proxy_health_thread()

    app = Application.builder().token(BOT_TOKEN).build()

    for name, handler in [
        ("start",       cmd_start),
        ("help",        cmd_settings),
        ("dork",        cmd_dork),
        ("clean",       cmd_clean),
        ("pages",       cmd_pages),
        ("tor",         cmd_tor),
        ("filter",      cmd_filter),
        ("settings",    cmd_settings),
        ("workers",     cmd_workers),
        ("chunks",      cmd_chunks),
        ("maxres",      cmd_maxres),
        ("engine",      cmd_engine),
        ("stop",        cmd_stop),
        ("status",      cmd_status),
        ("addproxy",    cmd_addproxy),
        ("removeproxy", cmd_removeproxy),
        ("proxylist",   cmd_proxylist),
        ("testproxy",   cmd_testproxy),
    ]:
        app.add_handler(CommandHandler(name, handler))

    app.add_handler(MessageHandler(filters.Document.ALL,            handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(CallbackQueryHandler(handle_callback))

    log.info("=" * 60)
    log.info("  DORK PARSER v19.0 — MULTI-THREADED STEALTH")
    log.info(f"  Chunks: {N_CHUNKS} | Workers/chunk: {WORKERS_PER_CHUNK}")
    log.info(f"  Delay: {MIN_DELAY}–{MAX_DELAY}s | Fast: {FAST_MIN_DELAY}–{FAST_MAX_DELAY}s")
    log.info(f"  TLS profiles: {len(BROWSER_PROFILES)} (random rotation)")
    log.info(f"  Proxies: {PROXY_POOL.total_count()} loaded | PROXY_ENABLED={PROXY_ENABLED}")
    log.info(f"  Engines: {', '.join(ENGINES)}")
    log.info("=" * 60)
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
