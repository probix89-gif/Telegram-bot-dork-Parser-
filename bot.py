"""
╔══════════════════════════════════════════════════════════════════╗
║   DORK PARSER BOT v20.0 — XTREAM EDITION (FIXED)                 ║
║   • FIXED inline keyboard buttons (full handler coverage)        ║
║   • ADVANCED TLS fingerprint rotation (12 profiles, per-request) ║
║   • SPEED BOOST: 200 URLs/sec standard mode                      ║
║   • NEW /xtream mode: 1000 URLs/sec Yahoo bruteforce collector   ║
║   • All v19.0 features preserved                                 ║
╚══════════════════════════════════════════════════════════════════╝
"""

import asyncio
import random
import re
import os
import time
import logging
import tempfile
import itertools
from collections import deque
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlparse, parse_qs, unquote, quote_plus, urlencode

from curl_cffi.requests import AsyncSession
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
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
)
log = logging.getLogger(__name__)

# ─── CONFIGURATION ───────────────────────────────────────────────────────────
BOT_TOKEN             = os.environ.get("BOT_TOKEN", "")
N_CHUNKS              = int(os.environ.get("N_CHUNKS", 4))
WORKERS_PER_CHUNK     = int(os.environ.get("WORKERS_PER_CHUNK", 25))
MAX_WORKERS_PER_CHUNK = 60
MIN_DELAY             = float(os.environ.get("MIN_DELAY", 0.2))
MAX_DELAY             = float(os.environ.get("MAX_DELAY", 0.6))
FAST_MIN_DELAY        = 0.05
FAST_MAX_DELAY        = 0.15
FAST_STREAK_THRESHOLD = 2
MAX_RESULTS           = int(os.environ.get("MAX_RESULTS", 10))
TOR_PROXY             = os.environ.get("TOR_PROXY", "socks5://127.0.0.1:9050")
OUTPUT_DIR            = Path("results")
OUTPUT_DIR.mkdir(exist_ok=True)

ENGINES   = ["bing", "yahoo", "duckduckgo"]
MAX_PAGES = 70

WORKER_FETCH_TIMEOUT = 60
JOB_TIMEOUT          = 30 * 60
MAX_RETRIES          = 2
CHUNK_STALL_TIMEOUT  = 30.0
EMPTY_RATE_SLOWDOWN  = 0.60
EMPTY_RATE_RECOVER   = 0.40
CHUNK_STAGGER_DELAY  = (0.1, 0.4)

# ─── XTREAM MODE CONFIG ──────────────────────────────────────────────────────
XTREAM_WORKERS_PER_CHUNK   = 12
XTREAM_CHUNKS              = 6
XTREAM_MIN_DELAY           = 1.2
XTREAM_MAX_DELAY           = 3.5
XTREAM_PAGE_DELAY_MIN      = 0.8
XTREAM_PAGE_DELAY_MAX      = 2.2
XTREAM_TIMEOUT             = 20
XTREAM_MAX_RETRIES         = 2
XTREAM_TARGET_RPS          = 250
XTREAM_PAGES_PER_DORK      = 5
XTREAM_SESSION_POOL_SIZE   = 100
XTREAM_SESSION_MAX_USES    = 25
XTREAM_SESSION_MAX_AGE     = 180
XTREAM_POOL_BATCH_SIZE     = 15
XTREAM_CAPTCHA_RATE_LIMIT  = 0.15
XTREAM_PRESEED_COOKIES     = True
XTREAM_WORKER_START_JITTER = 0.25

DEFAULT_SESSION = {
    "workers":       WORKERS_PER_CHUNK,
    "chunks":        N_CHUNKS,
    "engines":       list(ENGINES),
    "max_results":   MAX_RESULTS,
    "pages":         [1],
    "tor":           False,
    "min_score":     30,
    "xtream":        False,
    "xtream_engine": "yahoo",
}

user_sessions:   dict = {}
active_jobs:     dict = {}
active_stop_evs: dict = {}


# ══════════════════════════════════════════════════════════════════════════════
# ─── ADVANCED TLS FINGERPRINT ROTATION v21.0 ─────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

_LANG_POOL = [
    "en-US,en;q=0.9",
    "en-US,en;q=0.9,es;q=0.8",
    "en-GB,en;q=0.9",
    "en-CA,en;q=0.9,fr-CA;q=0.8",
    "en-AU,en;q=0.9",
    "en-US,en;q=0.8,de;q=0.7",
    "en-US,en;q=0.9,fr;q=0.8",
    "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
    "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    "es-ES,es;q=0.9,en;q=0.8",
    "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7",
    "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7",
    "en-IN,en-GB;q=0.9,en;q=0.8",
    "en-SG,en;q=0.9",
    "en-NZ,en;q=0.9",
]

_ACCEPT_CHROME  = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
_ACCEPT_FIREFOX = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
_ACCEPT_SAFARI  = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
_ACCEPT_EDGE    = "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"

TLS_PROFILES = [
    {
        "impersonate": "chrome110", "browser": "chrome", "version": 110,
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Chromium";v="110", "Not A(Brand";v="24", "Google Chrome";v="110"',
        "platform": '"Windows"', "accept": _ACCEPT_CHROME,
        "accept_lang": random.choice(_LANG_POOL), "accept_enc": "gzip, deflate, br",
    },
    {
        "impersonate": "chrome116", "browser": "chrome", "version": 116,
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Not)A;Brand";v="24", "Chromium";v="116", "Google Chrome";v="116"',
        "platform": '"Windows"', "accept": _ACCEPT_CHROME,
        "accept_lang": random.choice(_LANG_POOL), "accept_enc": "gzip, deflate, br",
    },
    {
        "impersonate": "chrome119", "browser": "chrome", "version": 119,
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        "platform": '"Windows"', "accept": _ACCEPT_CHROME,
        "accept_lang": random.choice(_LANG_POOL), "accept_enc": "gzip, deflate, br",
    },
    {
        "impersonate": "chrome120", "browser": "chrome", "version": 120,
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        "platform": '"Windows"', "accept": _ACCEPT_CHROME,
        "accept_lang": random.choice(_LANG_POOL), "accept_enc": "gzip, deflate, br",
    },
    {
        "impersonate": "chrome123", "browser": "chrome", "version": 123,
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
        "platform": '"Windows"', "accept": _ACCEPT_CHROME,
        "accept_lang": random.choice(_LANG_POOL), "accept_enc": "gzip, deflate, br, zstd",
    },
    {
        "impersonate": "chrome124", "browser": "chrome", "version": 124,
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "platform": '"Windows"', "accept": _ACCEPT_CHROME,
        "accept_lang": random.choice(_LANG_POOL), "accept_enc": "gzip, deflate, br, zstd",
        "priority": "u=0, i",
    },
    {
        "impersonate": "chrome126", "browser": "chrome", "version": 126,
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Chromium";v="126", "Google Chrome";v="126", "Not-A.Brand";v="8"',
        "platform": '"Windows"', "accept": _ACCEPT_CHROME,
        "accept_lang": random.choice(_LANG_POOL), "accept_enc": "gzip, deflate, br, zstd",
        "priority": "u=0, i",
    },
    {
        "impersonate": "chrome131", "browser": "chrome", "version": 131,
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "platform": '"Windows"', "accept": _ACCEPT_CHROME,
        "accept_lang": random.choice(_LANG_POOL), "accept_enc": "gzip, deflate, br, zstd",
        "priority": "u=0, i",
    },
    {
        "impersonate": "chrome131", "browser": "chrome", "version": 131,
        "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "platform": '"macOS"', "accept": _ACCEPT_CHROME,
        "accept_lang": random.choice(_LANG_POOL), "accept_enc": "gzip, deflate, br, zstd",
        "priority": "u=0, i",
    },
    {
        "impersonate": "chrome131", "browser": "chrome", "version": 131,
        "ua": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "platform": '"Linux"', "accept": _ACCEPT_CHROME,
        "accept_lang": random.choice(_LANG_POOL), "accept_enc": "gzip, deflate, br, zstd",
        "priority": "u=0, i",
    },
    {
        "impersonate": "chrome120", "browser": "chrome", "version": 120,
        "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        "platform": '"macOS"', "accept": _ACCEPT_CHROME,
        "accept_lang": random.choice(_LANG_POOL), "accept_enc": "gzip, deflate, br",
    },
    {
        "impersonate": "chrome131", "browser": "chrome", "version": 131,
        "ua": "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36",
        "sec_ch_ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "platform": '"Android"', "accept": _ACCEPT_CHROME,
        "accept_lang": random.choice(_LANG_POOL), "accept_enc": "gzip, deflate, br, zstd",
        "mobile": True, "priority": "u=0, i",
    },
    {
        "impersonate": "chrome120", "browser": "chrome", "version": 120,
        "ua": "Mozilla/5.0 (Linux; Android 13; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.144 Mobile Safari/537.36",
        "sec_ch_ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        "platform": '"Android"', "accept": _ACCEPT_CHROME,
        "accept_lang": random.choice(_LANG_POOL), "accept_enc": "gzip, deflate, br",
        "mobile": True,
    },
    {
        "impersonate": "edge99", "browser": "edge", "version": 99,
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Safari/537.36 Edg/99.0.1150.55",
        "sec_ch_ua": '"Microsoft Edge";v="99", "Chromium";v="99", "Not;A=Brand";v="24"',
        "platform": '"Windows"', "accept": _ACCEPT_EDGE,
        "accept_lang": random.choice(_LANG_POOL), "accept_enc": "gzip, deflate, br",
    },
    {
        "impersonate": "edge101", "browser": "edge", "version": 101,
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.54 Safari/537.36 Edg/101.0.1210.39",
        "sec_ch_ua": '"Microsoft Edge";v="101", "Chromium";v="101", "Not;A=Brand";v="24"',
        "platform": '"Windows"', "accept": _ACCEPT_EDGE,
        "accept_lang": random.choice(_LANG_POOL), "accept_enc": "gzip, deflate, br",
        "priority": "u=0, i",
    },
    {
        "impersonate": "safari15_5", "browser": "safari", "version": 155,
        "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.5 Safari/605.1.15",
        "sec_ch_ua": None,
        "platform": '"macOS"', "accept": _ACCEPT_SAFARI,
        "accept_lang": random.choice(_LANG_POOL), "accept_enc": "gzip, deflate, br",
    },
    {
        "impersonate": "safari17_0", "browser": "safari", "version": 170,
        "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        "sec_ch_ua": None,
        "platform": '"macOS"', "accept": _ACCEPT_SAFARI,
        "accept_lang": random.choice(_LANG_POOL), "accept_enc": "gzip, deflate, br",
    },
    {
        "impersonate": "safari18_0", "browser": "safari", "version": 180,
        "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 15_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15",
        "sec_ch_ua": None,
        "platform": '"macOS"', "accept": _ACCEPT_SAFARI,
        "accept_lang": random.choice(_LANG_POOL), "accept_enc": "gzip, deflate, br",
    },
    {
        "impersonate": "safari17_2_ios", "browser": "safari", "version": 172,
        "ua": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
        "sec_ch_ua": None,
        "platform": '"iOS"', "accept": _ACCEPT_SAFARI,
        "accept_lang": random.choice(_LANG_POOL), "accept_enc": "gzip, deflate, br",
        "mobile": True,
    },
    {
        "impersonate": "safari18_0", "browser": "safari", "version": 180,
        "ua": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Mobile/15E148 Safari/604.1",
        "sec_ch_ua": None,
        "platform": '"iOS"', "accept": _ACCEPT_SAFARI,
        "accept_lang": random.choice(_LANG_POOL), "accept_enc": "gzip, deflate, br",
        "mobile": True,
    },
    {
        "impersonate": "firefox133", "browser": "firefox", "version": 133,
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
        "sec_ch_ua": None,
        "platform": None, "accept": _ACCEPT_FIREFOX,
        "accept_lang": random.choice(_LANG_POOL), "accept_enc": "gzip, deflate, br, zstd",
        "firefox": True,
    },
    {
        "impersonate": "firefox133", "browser": "firefox", "version": 133,
        "ua": "Mozilla/5.0 (X11; Linux x86_64; rv:133.0) Gecko/20100101 Firefox/133.0",
        "sec_ch_ua": None,
        "platform": None, "accept": _ACCEPT_FIREFOX,
        "accept_lang": random.choice(_LANG_POOL), "accept_enc": "gzip, deflate, br, zstd",
        "firefox": True,
    },
    {
        "impersonate": "firefox133", "browser": "firefox", "version": 133,
        "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.2; rv:133.0) Gecko/20100101 Firefox/133.0",
        "sec_ch_ua": None,
        "platform": None, "accept": _ACCEPT_FIREFOX,
        "accept_lang": random.choice(_LANG_POOL), "accept_enc": "gzip, deflate, br, zstd",
        "firefox": True,
    },
]

_tls_cycle = itertools.cycle(TLS_PROFILES)
_tls_lock  = asyncio.Lock()
_tls_last  = []
_TLS_ANTI_REPEAT = 3


def get_tls_profile(strategy: str = "random") -> dict:
    global _tls_last
    if strategy == "round":
        return next(_tls_cycle)

    if strategy == "weighted":
        r = random.random()
        if r < 0.62:
            pool = [p for p in TLS_PROFILES if p["browser"] == "chrome" and not p.get("mobile")]
        elif r < 0.72:
            pool = [p for p in TLS_PROFILES if p["browser"] == "firefox"]
        elif r < 0.80:
            pool = [p for p in TLS_PROFILES if p["browser"] == "edge"]
        elif r < 0.90:
            pool = [p for p in TLS_PROFILES if p["browser"] == "safari" and not p.get("mobile")]
        else:
            pool = [p for p in TLS_PROFILES if p.get("mobile")]
        candidates = pool or TLS_PROFILES
    else:
        candidates = TLS_PROFILES

    recent = set(_tls_last[-_TLS_ANTI_REPEAT:])
    filtered = [p for p in candidates if p["impersonate"] not in recent]
    chosen = random.choice(filtered if filtered else candidates)

    _tls_last.append(chosen["impersonate"])
    if len(_tls_last) > _TLS_ANTI_REPEAT * 2:
        _tls_last = _tls_last[-_TLS_ANTI_REPEAT:]
    return chosen


def build_headers_from_profile(profile: dict, referer: str | None = None,
                                origin: str | None = None,
                                context: str = "navigate") -> dict:
    is_firefox = profile.get("firefox", False)
    is_mobile  = profile.get("mobile", False)
    version    = profile.get("version", 120)
    browser    = profile.get("browser", "chrome")
    cache_ctrl = random.choice(["max-age=0", "max-age=0", "no-cache", "max-age=0"])

    if is_firefox:
        h = {
            "User-Agent":              profile["ua"],
            "Accept":                  profile.get("accept", _ACCEPT_FIREFOX),
            "Accept-Language":         profile["accept_lang"],
            "Accept-Encoding":         profile.get("accept_enc", "gzip, deflate, br, zstd"),
            "Connection":              "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest":          "document",
            "Sec-Fetch-Mode":          "navigate",
            "Sec-Fetch-Site":          "cross-site" if referer else "none",
            "Sec-Fetch-User":          "?1",
            "Te":                      "trailers",
            "Cache-Control":           cache_ctrl,
        }
    else:
        h = {
            "User-Agent":              profile["ua"],
            "Accept":                  profile.get("accept", _ACCEPT_CHROME),
            "Accept-Language":         profile["accept_lang"],
            "Accept-Encoding":         profile.get("accept_enc", "gzip, deflate, br"),
            "Upgrade-Insecure-Requests": "1",
            "Cache-Control":           cache_ctrl,
            "Sec-Fetch-Dest":          "document",
            "Sec-Fetch-Mode":          "navigate",
            "Sec-Fetch-Site":          "same-origin" if referer else "none",
            "Sec-Fetch-User":          "?1",
        }
        if version >= 101 and browser in ("chrome", "edge") and "priority" in profile:
            h["Priority"] = profile["priority"]

    if profile.get("sec_ch_ua"):
        h["Sec-Ch-Ua"]          = profile["sec_ch_ua"]
        h["Sec-Ch-Ua-Mobile"]   = "?1" if is_mobile else "?0"
        h["Sec-Ch-Ua-Platform"] = profile["platform"]
        if version >= 120 and random.random() < 0.40:
            h["Sec-Ch-Ua-Arch"]           = '"x86"' if not is_mobile else '"arm"'
            h["Sec-Ch-Ua-Bitness"]        = '"64"'
            h["Sec-Ch-Ua-Full-Version-List"] = profile["sec_ch_ua"]

    if referer:
        h["Referer"] = referer
    if origin:
        h["Origin"] = origin

    dnt_prob = 0.25 if is_firefox else 0.05
    if random.random() < dnt_prob:
        h["DNT"] = "1"
    if random.random() < 0.02:
        h["Save-Data"] = "on"
    return h


# ══════════════════════════════════════════════════════════════════════════════
# ─── ANTI-BLOCK SYSTEM v21.0 ─────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

import collections as _collections

class DomainCircuitBreaker:
    WINDOW        = 20
    THRESHOLD     = 0.55
    COOLDOWN_BASE = 45.0
    COOLDOWN_MAX  = 480.0

    def __init__(self):
        self._lock     = asyncio.Lock()
        self._history: dict[str, deque] = {}
        self._state:   dict[str, str]   = {}
        self._until:   dict[str, float] = {}
        self._cooldown: dict[str, float] = {}

    def _domain(self, url: str) -> str:
        try:
            return urlparse(url).netloc.lower()
        except Exception:
            return url

    async def check(self, url: str) -> float:
        domain = self._domain(url)
        async with self._lock:
            state = self._state.get(domain, "closed")
            if state == "closed":
                return 0.0
            if state == "open":
                remaining = self._until.get(domain, 0) - time.time()
                if remaining > 0:
                    return remaining
                self._state[domain] = "half"
                return 0.0
            return 2.0

    async def record(self, url: str, blocked: bool) -> None:
        domain = self._domain(url)
        async with self._lock:
            if domain not in self._history:
                self._history[domain] = _collections.deque(maxlen=self.WINDOW)
                self._state[domain]   = "closed"
                self._cooldown[domain] = self.COOLDOWN_BASE

            hist  = self._history[domain]
            state = self._state.get(domain, "closed")
            hist.append(1 if blocked else 0)

            if state == "half":
                if blocked:
                    cd = min(self._cooldown[domain] * 2, self.COOLDOWN_MAX)
                    self._cooldown[domain] = cd
                    self._state[domain]    = "open"
                    self._until[domain]    = time.time() + cd
                else:
                    self._state[domain]    = "closed"
                    self._cooldown[domain] = self.COOLDOWN_BASE
                    hist.clear()
                return

            if len(hist) >= self.WINDOW // 2:
                rate = sum(hist) / len(hist)
                if rate >= self.THRESHOLD and state == "closed":
                    cd = self._cooldown[domain]
                    self._state[domain] = "open"
                    self._until[domain] = time.time() + cd


circuit_breaker = DomainCircuitBreaker()


def humanize_delay(base: float, sigma_ratio: float = 0.30,
                   distraction_prob: float = 0.04,
                   distraction_extra: float = 3.0) -> float:
    delay = random.gauss(base, base * sigma_ratio)
    delay = max(base * 0.2, min(base * 4.0, delay))
    if random.random() < distraction_prob:
        delay += random.uniform(distraction_extra, distraction_extra * 3)
    return delay


async def async_humanize_sleep(base: float, **kw) -> None:
    await asyncio.sleep(humanize_delay(base, **kw))


_YAHOO_HOMES  = ["https://search.yahoo.com/", "https://yahoo.com/", "https://www.yahoo.com/"]
_BING_HOMES   = ["https://www.bing.com/", "https://bing.com/"]
_GOOGLE_HOMES = ["https://www.google.com/", "https://google.com/"]

class RefererChain:
    def __init__(self, engine: str = "yahoo"):
        self.engine  = engine
        self._chain: list[str] = []
        if engine == "bing":
            self._chain.append(random.choice(_BING_HOMES))
        elif engine == "google":
            self._chain.append(random.choice(_GOOGLE_HOMES))
        else:
            self._chain.append(random.choice(_YAHOO_HOMES))

    def push(self, url: str) -> None:
        self._chain.append(url)
        if len(self._chain) > 5:
            self._chain.pop(0)

    def current(self) -> str | None:
        return self._chain[-1] if self._chain else None

    def next_serp_referer(self, serp_url: str) -> str:
        ref = self.current()
        self.push(serp_url)
        return ref or ""


_YAHOO_FR_POOL = [
    "fp-tts", "yfp-t-902", "yfp-t-501", "free", "p2", "sfp",
    "uh3_finance_vert_gs", "uh3_finance_vert", "yfp-t-152",
]
_YAHOO_VD_POOL = ["b", ""]
_YAHOO_EI_POOL = ["UTF-8", "utf-8"]

_BING_FORM_POOL = ["QBLH", "QBRE", "SBSC", "QBHL", "PERE", "ANAB01"]
_BING_MSBQF_POOL = ["0", "1", ""]
_BING_COUNT_POOL  = [10, 10, 10, 15, 20]


def vary_yahoo_params(base_params: dict) -> dict:
    p = dict(base_params)
    p["fr"]  = random.choice(_YAHOO_FR_POOL)
    p["ei"]  = random.choice(_YAHOO_EI_POOL)
    if random.random() < 0.15:
        p["vd"] = random.choice(_YAHOO_VD_POOL)
    if random.random() < 0.10:
        p["age"] = random.choice(["1d", "1w", "1m", ""])
    if random.random() < 0.08:
        p["toggle"] = "1"
    return p


def vary_bing_params(base_params: dict) -> dict:
    p = dict(base_params)
    p["form"]  = random.choice(_BING_FORM_POOL)
    p["count"] = random.choice(_BING_COUNT_POOL)
    if random.random() < 0.12:
        p["msbqf"] = random.choice(_BING_MSBQF_POOL)
    if random.random() < 0.08:
        p["qpvt"] = p.get("q", "")[:20]
    if random.random() < 0.10:
        p["sc"] = f"8-{random.randint(10, 40)}"
    return p


_COMMON_ISP_RANGES = [
    ("24.0.0.0",    "24.255.255.255"),
    ("71.0.0.0",    "71.127.255.255"),
    ("98.0.0.0",    "98.255.255.255"),
    ("173.0.0.0",   "173.79.255.255"),
    ("67.40.0.0",   "67.63.255.255"),
    ("50.0.0.0",    "50.127.255.255"),
    ("86.0.0.0",    "86.255.255.255"),
    ("82.0.0.0",    "82.127.255.255"),
    ("90.0.0.0",    "90.127.255.255"),
]

def _random_public_ip() -> str:
    r1, r2 = random.choice(_COMMON_ISP_RANGES)
    parts1 = [int(x) for x in r1.split(".")]
    parts2 = [int(x) for x in r2.split(".")]
    return ".".join(str(random.randint(a, b)) for a, b in zip(parts1, parts2))

def spoof_xff_headers(h: dict, probability: float = 0.35) -> dict:
    if random.random() < probability:
        ip = _random_public_ip()
        h["X-Forwarded-For"] = ip
        if random.random() < 0.5:
            h["X-Real-Ip"] = ip
    return h


# ══════════════════════════════════════════════════════════════════════════════
# ─── PROXY SYSTEM (unchanged from v19.0) ─────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

PROXY_ENABLED: bool = os.environ.get("PROXY_ENABLED", "true").lower() not in ("false", "0", "no")
PROXY_PROBE_ORDER = ("socks5", "socks4", "http", "https")
PROXY_TEST_URLS = [
    "https://api.ipify.org?format=json",
    "https://httpbin.org/ip",
    "https://ifconfig.me/ip",
    "http://ip-api.com/json/",
]
PROXY_CHECK_TIMEOUT     = 10
PROXY_CHECK_CONCURRENCY = 30
PROXY_HEALTH_INTERVAL   = 600
PROXY_MAX_FAILS         = 3

_proxy_pool_lock: asyncio.Lock = asyncio.Lock()
_proxy_pool: list[dict] = []
_proxy_health_task: asyncio.Task | None = None

_IP_PORT_RE      = re.compile(r"^([\w\-\.]+):(\d{1,5})$")
_IP_PORT_AUTH_RE = re.compile(r"^([\w\-\.]+):(\d{1,5}):([^:\s]+):([^:\s]+)$")
_URL_RE          = re.compile(
    r"^(https?|socks4a?|socks5h?)://(?:([^:@/\s]+):([^:@/\s]+)@)?([\w\-\.]+):(\d{1,5})/?$",
    re.IGNORECASE,
)


def parse_proxy_line(line: str) -> dict | None:
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    m = _URL_RE.match(line)
    if m:
        scheme, user, pwd, host, port = m.groups()
        scheme = scheme.lower()
        if scheme == "socks5h": scheme = "socks5"
        elif scheme == "socks4a": scheme = "socks4"
        return {
            "host": host, "port": int(port), "user": user or None, "pass": pwd or None,
            "protocol": scheme, "url": _build_proxy_url(scheme, host, int(port), user, pwd),
            "alive": False, "latency": None, "last_check": 0.0, "fail_count": 0, "explicit": True,
        }
    m = _IP_PORT_AUTH_RE.match(line)
    if m:
        host, port, user, pwd = m.groups()
        return {"host": host, "port": int(port), "user": user, "pass": pwd,
                "protocol": None, "url": None, "alive": False, "latency": None,
                "last_check": 0.0, "fail_count": 0, "explicit": False}
    m = _IP_PORT_RE.match(line)
    if m:
        host, port = m.groups()
        return {"host": host, "port": int(port), "user": None, "pass": None,
                "protocol": None, "url": None, "alive": False, "latency": None,
                "last_check": 0.0, "fail_count": 0, "explicit": False}
    return None


def _build_proxy_url(scheme, host, port, user, pwd):
    auth = f"{user}:{pwd}@" if user and pwd else ""
    return f"{scheme}://{auth}{host}:{port}"


def proxy_key(p): return f"{p['host']}:{p['port']}:{p.get('user') or ''}"
def proxy_display(p):
    proto = p["protocol"].upper() if p["protocol"] else "?"
    auth  = " 🔐" if p.get("user") else ""
    return f"[{proto:6s}] {p['host']}:{p['port']}{auth}"


async def _probe_single(host, port, user, pwd, scheme):
    proxy_url = _build_proxy_url(scheme, host, port, user, pwd)
    test_url  = random.choice(PROXY_TEST_URLS)
    sess = AsyncSession(impersonate="chrome120", verify=False,
                        timeout=PROXY_CHECK_TIMEOUT, proxy=proxy_url)
    try:
        t0 = time.monotonic()
        resp = await sess.get(test_url, timeout=PROXY_CHECK_TIMEOUT)
        latency = (time.monotonic() - t0) * 1000.0
        if resp.status_code != 200:
            return False, None, None
        text = resp.text.strip()
        ext_ip = None
        try:
            import json as _json
            data = _json.loads(text)
            ext_ip = data.get("ip") or data.get("origin") or data.get("query")
        except Exception:
            if re.match(r"^\d{1,3}(\.\d{1,3}){3}$", text):
                ext_ip = text
        if not ext_ip and len(text) < 5:
            return False, None, None
        return True, latency, ext_ip
    except (CurlError, asyncio.TimeoutError, Exception):
        return False, None, None
    finally:
        try: await sess.close()
        except Exception: pass


async def detect_proxy_protocol(p):
    host, port = p["host"], p["port"]
    user, pwd  = p.get("user"), p.get("pass")
    if p.get("explicit") and p.get("protocol"):
        ok, latency, _ = await _probe_single(host, port, user, pwd, p["protocol"])
        if ok:
            p["alive"]=True; p["latency"]=latency; p["last_check"]=time.time(); p["fail_count"]=0
            return True
        p["alive"]=False; p["last_check"]=time.time(); p["fail_count"]=p.get("fail_count",0)+1
        return False
    for scheme in PROXY_PROBE_ORDER:
        ok, latency, _ = await _probe_single(host, port, user, pwd, scheme)
        if ok:
            p["protocol"]=scheme
            p["url"]=_build_proxy_url(scheme, host, port, user, pwd)
            p["alive"]=True; p["latency"]=latency; p["last_check"]=time.time(); p["fail_count"]=0
            return True
    p["alive"]=False; p["protocol"]=None
    p["last_check"]=time.time(); p["fail_count"]=p.get("fail_count",0)+1
    return False


async def check_proxies_bulk(proxies, concurrency=PROXY_CHECK_CONCURRENCY, progress_cb=None):
    sem = asyncio.Semaphore(concurrency)
    done = [0]; total = len(proxies); alive = 0
    async def _one(p):
        nonlocal alive
        async with sem:
            ok = await detect_proxy_protocol(p)
            if ok: alive += 1
            done[0] += 1
            if progress_cb and done[0] % 5 == 0:
                try: await progress_cb(done[0], total, alive)
                except Exception: pass
    await asyncio.gather(*[_one(p) for p in proxies], return_exceptions=True)
    return alive, total - alive


def _persist_proxies():
    try:
        with open("proxies.txt", "w", encoding="utf-8") as f:
            f.write(f"# Proxy pool — v20.0\n# Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"# Total: {len(_proxy_pool)}\n\n")
            for p in _proxy_pool:
                line = p["url"] if p.get("url") else (
                    f"{p['host']}:{p['port']}:{p['user']}:{p['pass']}" if p.get("user")
                    else f"{p['host']}:{p['port']}")
                tag = f"  # alive={'Y' if p['alive'] else 'N'} latency={int(p['latency']) if p['latency'] else 'NA'}ms"
                f.write(line + tag + "\n")
    except Exception as exc:
        log.warning(f"[PROXY] persist fail: {exc}")


def _load_proxies():
    proxies = []
    env_list = os.environ.get("PROXY_LIST", "").strip()
    if env_list:
        for line in [p.strip() for p in env_list.split(",") if p.strip()]:
            p = parse_proxy_line(line)
            if p: proxies.append(p)
        return proxies
    proxy_file = Path("proxies.txt")
    if proxy_file.exists():
        with open(proxy_file, encoding="utf-8") as f:
            for line in f:
                clean = line.split("#", 1)[0].strip()
                if not clean: continue
                p = parse_proxy_line(clean)
                if p: proxies.append(p)
    return proxies


_proxy_pool = _load_proxies()


def get_random_proxy_url(exclude_url=None, alive_only=True):
    if not PROXY_ENABLED or not _proxy_pool:
        return None
    cands = [p["url"] for p in _proxy_pool
             if p.get("url") and (not alive_only or p["alive"]) and p["url"] != exclude_url]
    if not cands:
        cands = [p["url"] for p in _proxy_pool if p.get("url") and p["url"] != exclude_url]
    return random.choice(cands) if cands else None


def _is_proxy_error(exc):
    msg = str(exc).lower()
    return any(kw in msg for kw in (
        "proxy", "tunnel", "407", "socks", "authentication",
        "connection refused", "network unreachable", "no route to host",
        "could not connect to proxy", "unable to connect to proxy",
        "recv failure", "ssl handshake", "timed out"))


async def _proxy_health_loop():
    while True:
        await asyncio.sleep(PROXY_HEALTH_INTERVAL)
        if not _proxy_pool: continue
        async with _proxy_pool_lock:
            snapshot = list(_proxy_pool)
        try:
            alive, dead = await check_proxies_bulk(snapshot)
            log.info(f"[HEALTH] alive={alive} dead={dead}")
            async with _proxy_pool_lock:
                before = len(_proxy_pool)
                _proxy_pool[:] = [p for p in _proxy_pool if p.get("fail_count", 0) < PROXY_MAX_FAILS]
                _persist_proxies()
        except Exception as exc:
            log.error(f"[HEALTH] {exc}")


def start_proxy_health_monitor():
    global _proxy_health_task
    if _proxy_health_task is None or _proxy_health_task.done():
        _proxy_health_task = asyncio.create_task(_proxy_health_loop())


# ══════════════════════════════════════════════════════════════════════════════
# ─── DORK PARSER (unchanged from v19.0) ──────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

KNOWN_OPERATORS = {
    "inurl", "intitle", "intext", "inanchor", "site", "filetype", "ext",
    "cache", "link", "related", "info", "allinurl", "allintitle", "allintext",
}

ENGINE_OPERATOR_SUPPORT = {
    "bing":       {"inurl", "intitle", "site", "filetype", "ext", "ip", "contains", "inbody"},
    "yahoo":      {"inurl", "intitle", "site", "filetype", "ext"},
    "duckduckgo": {"inurl", "intitle", "site", "filetype", "ext", "intext"},
    "google":     KNOWN_OPERATORS,
}

ENGINE_OPERATOR_ALIAS = {
    "bing":  {"intext": "inbody"},
    "yahoo": {"intext": None, "inanchor": None},
}


class DorkToken:
    __slots__ = ("kind", "op", "value", "negate", "quoted")
    def __init__(self, kind, op, value, negate=False, quoted=False):
        self.kind=kind; self.op=op; self.value=value; self.negate=negate; self.quoted=quoted
    def __repr__(self):
        n = "-" if self.negate else ""; q = '"' if self.quoted else ""
        return f"{n}{self.op}:{q}{self.value}{q}" if self.op else f"{n}{q}{self.value}{q}"


class DorkAST:
    def __init__(self, tokens, raw):
        self.tokens = tokens; self.raw = raw
    @property
    def operators(self):
        out = {}
        for t in self.tokens:
            if t.op: out.setdefault(t.op, []).append(t.value)
        return out
    @property
    def free_terms(self):
        return [t.value for t in self.tokens if not t.op and t.kind in ("term", "phrase")]
    def __repr__(self): return " ".join(repr(t) for t in self.tokens)


_DORK_TOKEN_RE = re.compile(
    r"""(?P<neg>-)?(?:(?P<op>[a-zA-Z]+):)?(?:"(?P<phrase>[^"]+)"|\((?P<group>[^)]+)\)|(?P<term>[^\s"()]+))""",
    re.VERBOSE,
)


def parse_dork(dork):
    tokens = []
    for m in _DORK_TOKEN_RE.finditer(dork.strip()):
        neg=bool(m.group("neg")); op=m.group("op")
        phrase=m.group("phrase"); group=m.group("group"); term=m.group("term")
        if op: op = op.lower()
        if phrase is not None: tokens.append(DorkToken("phrase", op, phrase, negate=neg, quoted=True))
        elif group is not None: tokens.append(DorkToken("group", op, group, negate=neg))
        elif term is not None:
            if term.upper() == "OR" or term == "|":
                tokens.append(DorkToken("or", None, "OR"))
            else:
                tokens.append(DorkToken("term", op, term, negate=neg))
    return DorkAST(tokens, dork.strip())


def validate_dork(dork):
    if not dork or not dork.strip(): return False, "Empty dork"
    if dork.count('"') % 2 != 0: return False, "Unbalanced double-quotes"
    if dork.count("(") != dork.count(")"): return False, "Unbalanced parentheses"
    ast = parse_dork(dork)
    if not ast.tokens: return False, "No tokens parsed"
    unknown = [t.op for t in ast.tokens if t.op and t.op not in KNOWN_OPERATORS]
    if unknown: return True, f"OK (unknown operators: {', '.join(set(unknown))})"
    if not any(t.kind in ("term", "phrase", "group") for t in ast.tokens):
        return False, "No search terms"
    return True, "OK"


def normalize_dork(dork):
    ast = parse_dork(dork); seen=set(); out=[]
    for t in ast.tokens:
        key = (t.op, t.value.lower(), t.negate, t.quoted)
        if key in seen: continue
        seen.add(key); out.append(repr(t))
    return " ".join(out)


def translate_dork(dork, engine):
    if engine not in ENGINE_OPERATOR_SUPPORT: return dork
    supported = ENGINE_OPERATOR_SUPPORT[engine]
    aliases   = ENGINE_OPERATOR_ALIAS.get(engine, {})
    ast = parse_dork(dork); out = []
    for t in ast.tokens:
        if t.op:
            new_op = aliases.get(t.op, t.op)
            if new_op is None:
                if t.value: out.append(f'{"-" if t.negate else ""}{t.value}')
                continue
            if new_op not in supported:
                if t.value:
                    prefix = "-" if t.negate else ""
                    q = '"' if t.quoted else ""
                    out.append(f"{prefix}{q}{t.value}{q}")
                continue
            t2 = DorkToken(t.kind, new_op, t.value, t.negate, t.quoted)
            out.append(repr(t2))
        else:
            out.append(repr(t))
    return " ".join(out)


def mutate_dork(dork, n=5):
    variations = {dork}
    ast = parse_dork(dork); ops = ast.operators
    if "filetype" in ops:
        for v in ops["filetype"]: variations.add(dork.replace(f"filetype:{v}", f"ext:{v}"))
    if "ext" in ops:
        for v in ops["ext"]: variations.add(dork.replace(f"ext:{v}", f"filetype:{v}"))
    SQL_EXTS = ["php", "asp", "aspx", "jsp", "cfm"]
    for op in ("filetype", "ext"):
        for v in ops.get(op, []):
            if v.lower() in SQL_EXTS:
                for alt in SQL_EXTS:
                    if alt != v.lower(): variations.add(dork.replace(f"{op}:{v}", f"{op}:{alt}"))
    if "inurl" in ops:
        hints = ["id=", "pid=", "cat=", "page=", "uid=", "product=", "article="]
        for v in ops["inurl"]:
            for h in hints:
                if h not in v.lower():
                    variations.add(dork.replace(f"inurl:{v}", f"inurl:{v}{h}"))
    out = list(variations - {dork})
    random.shuffle(out)
    return ([dork] + out)[:max(1, n)]


def dedupe_dorks(dorks):
    seen=set(); out=[]
    for d in dorks:
        norm = normalize_dork(d).lower()
        if not norm or norm in seen: continue
        seen.add(norm); out.append(d.strip())
    return out


# ══════════════════════════════════════════════════════════════════════════════
# ─── URL FILTER / SCORER (unchanged) ─────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

BLACKLISTED_DOMAINS = {
    "yahoo.uservoice.com", "uservoice.com", "bing.com", "google.com", "googleapis.com",
    "gstatic.com", "youtube.com", "facebook.com", "instagram.com", "twitter.com", "x.com",
    "linkedin.com", "pinterest.com", "reddit.com", "wikipedia.org", "amazon.com",
    "amazon.co", "ebay.com", "shopify.com", "wordpress.com", "blogspot.com", "medium.com",
    "github.com", "stackoverflow.com", "w3schools.com", "microsoft.com", "apple.com",
    "cloudflare.com", "yahoo.com", "msn.com", "live.com", "outlook.com", "mercadolibre.com",
    "aliexpress.com", "alibaba.com", "etsy.com", "walmart.com", "bestbuy.com",
    "capitaloneshopping.com", "onetonline.org", "moodle.", "lyrics.fi", "verkkouutiset.fi",
    "iltalehti.fi", "sapo.pt", "iol.pt", "idealo.", "zalando.", "trovaprezzi.", "whatsapp.com",
}

SQL_HIGH_PARAMS = {
    "id","uid","user_id","userid","pid","product_id","productid","cid","cat_id","catid",
    "category_id","aid","article_id","nid","news_id","bid","blog_id","sid","fid","forum_id",
    "tid","topic_id","mid","msg_id","oid","order_id","rid","page_id","item_id","itemid",
    "post_id","gid","lid","vid","did","doc_id",
}

SQL_MED_PARAMS = {
    "q","query","search","name","username","email","page","p","type","action","do","module",
    "view","mode","from","date","code","ref","file","path","url","data","value","param",
    "price","tag","section","content","lang",
}

VULN_EXTENSIONS = {".php", ".asp", ".aspx", ".cfm", ".jsf", ".do", ".cgi", ".pl", ".jsp"}

_JUNK_RE = re.compile(
    r"aclick\?|uservoice\.com|utm_source=|\.pdf$|\.jpg$|\.jpeg$|\.png$|\.gif$|\.webp$|\.avif$|"
    r"\.svg$|\.ico$|\.css$|\.js$|\.mp4$|\.mp3$|\.zip$|/static/|/assets/|/images/|/img/|"
    r"/fonts/|/media/|/cdn-cgi/|/wp-content/uploads/", re.IGNORECASE,
)


def score_url(url):
    try: parsed = urlparse(url)
    except Exception: return 0
    if not url.startswith("http"): return 0
    domain = parsed.netloc.lower()
    for bd in BLACKLISTED_DOMAINS:
        if bd in domain: return 0
    if _JUNK_RE.search(url): return 0
    query = parsed.query; path = parsed.path.lower()
    has_vuln_ext = any(path.endswith(ext) for ext in VULN_EXTENSIONS)
    if not query: return 25 if has_vuln_ext else 5
    score = 15
    params = parse_qs(query, keep_blank_values=True)
    pkeys = {k.lower() for k in params}
    if has_vuln_ext: score += 20
    score += len(pkeys & SQL_HIGH_PARAMS) * 15
    score += len(pkeys & SQL_MED_PARAMS) * 5
    for vals in params.values():
        for v in vals:
            if v.isdigit(): score += 10; break
    if len(url) > 300: score -= 10
    elif len(url) > 200: score -= 5
    if len(params) > 8: score -= 5
    return max(0, min(score, 100))


def filter_scored(urls, min_score):
    result = [(score_url(u), u) for u in urls]
    result = [(s, u) for s, u in result if s >= min_score]
    result.sort(reverse=True)
    return result


MAX_URL_LENGTH = 200


def extract_domain(url):
    try:
        netloc = urlparse(url).netloc.lower()
        return netloc[4:] if netloc.startswith("www.") else netloc
    except Exception: return ""


def is_blocked(domain):
    for bd in BLACKLISTED_DOMAINS:
        if bd in domain: return True
    return False


def has_query_params(url):
    try: return bool(urlparse(url).query)
    except Exception: return False


def is_valid_url(url):
    try:
        p = urlparse(url); return p.scheme in ("http","https") and bool(p.netloc)
    except Exception: return False


def filter_urls(urls):
    total = len(urls); rm_invalid=rm_blocked=rm_no_query=rm_too_long=0
    seen=set(); kept=[]
    for url in urls:
        url = url.strip()
        if not url or url.startswith("#"): rm_invalid += 1; continue
        if not is_valid_url(url): rm_invalid += 1; continue
        if len(url) > MAX_URL_LENGTH: rm_too_long += 1; continue
        domain = extract_domain(url)
        if is_blocked(domain): rm_blocked += 1; continue
        if not has_query_params(url): rm_no_query += 1; continue
        if url in seen: continue
        seen.add(url); kept.append(url)
    return {"total":total, "kept":kept, "rm_invalid":rm_invalid,"rm_blocked":rm_blocked,
            "rm_no_query":rm_no_query,"rm_too_long":rm_too_long,
            "duplicates": total-rm_invalid-rm_blocked-rm_no_query-rm_too_long-len(kept)}


_TRACKING_PARAM_RE = re.compile(
    r"^(utm_\w+|fbclid|gclid|msclkid|yclid|mc_\w+|_ga|ref|source|medium|campaign|"
    r"affiliate|clickid|cid|sid_?|zanpid|dclid|twclid|igshid|s_kwcid)$",
    re.IGNORECASE,
)


def _normalize_url_for_dedup(url: str) -> str:
    try:
        p = urlparse(url)
        if not p.query:
            return url
        params = parse_qs(p.query, keep_blank_values=True)
        cleaned = {k: v for k, v in params.items() if not _TRACKING_PARAM_RE.match(k)}
        if cleaned == params:
            return url
        new_q = urlencode(cleaned, doseq=True)
        return p._replace(query=new_q).geturl()
    except Exception:
        return url


async def process_chunk_urls(chunk, semaphore, stop_ev):
    async with semaphore:
        if stop_ev.is_set(): return []
        await asyncio.sleep(0)
        return filter_urls(chunk)["kept"]


async def run_url_clean_job(chat_id, raw_lines, context):
    CLEAN_CHUNK_SIZE = 500; MAX_CONCURRENT = 4
    stop_ev = asyncio.Event()
    active_stop_evs[chat_id] = stop_ev
    total_input = len(raw_lines)
    status_msg = await context.bot.send_message(
        chat_id,
        f"🧹 URL CLEANER STARTED\n{'━'*30}\n📥 Input: {total_input}\n⏳ Processing...",
    )
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    chunks = [raw_lines[i:i+CLEAN_CHUNK_SIZE] for i in range(0, total_input, CLEAN_CHUNK_SIZE)]
    tasks = [asyncio.create_task(process_chunk_urls(c, semaphore, stop_ev)) for c in chunks]
    try:
        results = await asyncio.gather(*tasks, return_exceptions=True)
    except asyncio.CancelledError:
        stop_ev.set()
        for t in tasks: t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        results = []
    seen_final=set(); final_urls=[]
    for r in results:
        if isinstance(r, list):
            for u in r:
                if u not in seen_final:
                    seen_final.add(u); final_urls.append(u)
    full_stats = filter_urls(raw_lines)
    removed = total_input - len(final_urls)
    stopped = stop_ev.is_set()
    output_path = Path("results") / "cleaned_urls.txt"
    output_path.parent.mkdir(exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(f"# URL Cleaner — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"# Input: {total_input} | Kept: {len(final_urls)} | Removed: {removed}\n\n")
        for u in final_urls: f.write(u + "\n")
    try:
        await context.bot.edit_message_text(
            chat_id=chat_id, message_id=status_msg.message_id,
            text=(f"{'⏹' if stopped else '✅'} URL CLEANER DONE\n{'━'*30}\n"
                  f"📥 Input  : {total_input}\n✅ Kept   : {len(final_urls)}\n"
                  f"🗑 Removed: {removed}\n{'━'*30}"),
        )
    except Exception: pass
    if final_urls:
        with open(output_path, "rb") as f:
            await context.bot.send_document(chat_id, f, filename="cleaned_urls.txt",
                caption=f"🧹 {len(final_urls)} kept from {total_input}")
    else:
        await context.bot.send_message(chat_id, "⚠️ No URLs passed the filters.")
    active_stop_evs.pop(chat_id, None)
    active_jobs.pop(chat_id, None)


# ══════════════════════════════════════════════════════════════════════════════
# ─── SESSION FACTORY v20.0 — ROTATING TLS + SESSION POOL ─────────────────────
# ══════════════════════════════════════════════════════════════════════════════

def _make_isolated_session(use_tor=False, proxy=None, profile=None, http2=True):
    chosen_proxy = None
    if use_tor:
        chosen_proxy = TOR_PROXY
    elif proxy:
        chosen_proxy = proxy
    elif PROXY_ENABLED and _proxy_pool:
        chosen_proxy = get_random_proxy_url()

    if profile is None:
        profile = get_tls_profile("weighted")

    kwargs = {
        "impersonate": profile["impersonate"],
        "verify":      False,
        "timeout":     20,
        "default_headers": False,
    }
    if chosen_proxy:
        kwargs["proxy"] = chosen_proxy
    sess = AsyncSession(**kwargs)
    sess._cur_proxy   = chosen_proxy
    sess._tls_profile = profile
    return sess


def _make_fallback_session(exclude_proxy=None):
    fb_proxy = get_random_proxy_url(exclude_url=exclude_proxy)
    return _make_isolated_session(proxy=fb_proxy)


# ─── XTREAM SESSION POOL ─────────────────────────────────────────────────────

async def _preseed_session_cookies(sess, engine: str = "yahoo") -> None:
    try:
        if engine == "bing":
            url = random.choice(BING_HOMEPAGES)
        else:
            url = random.choice(YAHOO_HOMEPAGES)
        profile = getattr(sess, "_tls_profile", None) or get_tls_profile("weighted")
        headers = build_headers_from_profile(profile)
        await sess.get(url, headers=headers, timeout=10)
        await asyncio.sleep(random.uniform(0.15, 0.4))
    except Exception:
        pass


class XtreamSessionPool:
    def __init__(self, size=XTREAM_SESSION_POOL_SIZE, engine: str = "yahoo"):
        self.size   = size
        self.engine = engine
        self.sessions: deque = deque()
        self._usage: dict = {}
        self._age:   dict = {}
        self._lock   = asyncio.Lock()
        self._closed = False

    async def _make_one(self, use_tor: bool) -> None:
        profile = get_tls_profile("weighted")
        sess = _make_isolated_session(use_tor=use_tor, profile=profile)
        if XTREAM_PRESEED_COOKIES:
            seed_engine = "bing" if self.engine == "bing" else "yahoo"
            await _preseed_session_cookies(sess, engine=seed_engine)
        async with self._lock:
            sid = id(sess)
            self.sessions.append(sess)
            self._usage[sid] = 0
            self._age[sid]   = time.time()

    async def initialize(self, use_tor=False):
        tasks_left = self.size
        while tasks_left > 0:
            batch = min(XTREAM_POOL_BATCH_SIZE, tasks_left)
            await asyncio.gather(*[self._make_one(use_tor) for _ in range(batch)],
                                  return_exceptions=True)
            tasks_left -= batch

    async def acquire(self):
        async with self._lock:
            if not self.sessions:
                profile = get_tls_profile("weighted")
                sess = _make_isolated_session(profile=profile)
                self._usage[id(sess)] = 0
                self._age[id(sess)]   = time.time()
                return sess
            sess = self.sessions.popleft()
            return sess

    async def release(self, sess, burned=False):
        if self._closed:
            try: await sess.close()
            except Exception: pass
            return
        async with self._lock:
            sid = id(sess)
            self._usage[sid] = self._usage.get(sid, 0) + 1
            too_old   = (time.time() - self._age.get(sid, 0)) > XTREAM_SESSION_MAX_AGE
            too_used  = self._usage[sid] > XTREAM_SESSION_MAX_USES
            if burned or too_old or too_used:
                try: await sess.close()
                except Exception: pass
                self._usage.pop(sid, None); self._age.pop(sid, None)
                profile   = get_tls_profile("weighted")
                new_sess  = _make_isolated_session(profile=profile)
                self._usage[id(new_sess)] = 0
                self._age[id(new_sess)]   = time.time()
                self.sessions.append(new_sess)
            else:
                self.sessions.append(sess)

    async def close_all(self):
        self._closed = True
        async with self._lock:
            while self.sessions:
                s = self.sessions.popleft()
                try: await s.close()
                except Exception: pass


# ─── TOR ROTATION (unchanged) ────────────────────────────────────────────────
_tor_rotation_task = None
tor_enabled_users = 0

async def rotate_tor_identity():
    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", 9051)
        await reader.readuntil(b"250 ")
        writer.write(b'AUTHENTICATE ""\r\n'); await writer.drain()
        resp = await reader.readuntil(b"250 ")
        if b"250" not in resp: writer.close(); return
        writer.write(b"SIGNAL NEWNYM\r\n"); await writer.drain()
        await reader.readuntil(b"250 ")
        writer.close(); await writer.wait_closed()
    except Exception as exc:
        log.warning(f"Tor rotation: {exc}")

async def _tor_rotation_loop():
    while tor_enabled_users > 0:
        await rotate_tor_identity()
        await asyncio.sleep(120)

def start_tor_rotation():
    global _tor_rotation_task
    if _tor_rotation_task is None or _tor_rotation_task.done():
        _tor_rotation_task = asyncio.create_task(_tor_rotation_loop())

def stop_tor_rotation():
    global _tor_rotation_task
    if _tor_rotation_task and not _tor_rotation_task.done():
        _tor_rotation_task.cancel()
        _tor_rotation_task = None


# ─── CAPTCHA / DEGRADED DETECTION ────────────────────────────────────────────
_CAPTCHA_RE = re.compile(
    r"captcha|are you a robot|unusual traffic|access denied|verify you are human|"
    r"please verify|too many requests|blocked|forbidden|rate limit|temporarily unavailable|"
    r"cf-error|error 429|request denied|robot check|human verification|"
    r"your ip|ip address|automated|bot detection|security check|"
    r"503 service|502 bad gateway|pardon our interruption",
    re.IGNORECASE,
)

_YAHOO_RESULT_SIGNALS = re.compile(
    r'id="results"|searchCenterMiddle|class="algo|class="Sr|data-b="algo|'
    r'"algo-sr"|"dd algo"|uh3_id|"compTitle"',
    re.IGNORECASE,
)


def _is_degraded(html, engine):
    if len(html) < 400: return True
    if _CAPTCHA_RE.search(html[:4096]): return True
    if engine == "bing" and 'id="b_results"' not in html and "b_algo" not in html: return True
    if engine == "yahoo" and not _YAHOO_RESULT_SIGNALS.search(html): return True
    if engine == "duckduckgo" and "result__a" not in html and "results--main" not in html: return True
    return False


def _is_captcha(html):
    return bool(_CAPTCHA_RE.search(html[:4096]))


async def _on_captcha_detected(engine, chunk_id, session_proxy):
    log.warning(f"[C{chunk_id}][{engine.upper()}] 🔴 CAPTCHA")
    await asyncio.sleep(random.uniform(8.0, 18.0))


# ─── LINK EXTRACTION (unchanged) ─────────────────────────────────────────────
class _LinkExtractor(HTMLParser):
    __slots__ = ("links", "_in_cite", "_buf")
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.links=[]; self._in_cite=False; self._buf=[]
    def handle_starttag(self, tag, attrs):
        if tag == "a":
            adict = dict(attrs)
            for key in ("href", "data-u"):
                val = adict.get(key, "")
                if val.startswith("http"): self.links.append(val)
        elif tag == "cite":
            self._in_cite = True; self._buf.clear()
    def handle_endtag(self, tag):
        if tag == "cite" and self._in_cite:
            text = "".join(self._buf).strip()
            if text.startswith("http"): self.links.append(text)
            self._in_cite = False; self._buf.clear()
    def handle_data(self, data):
        if self._in_cite: self._buf.append(data)


def _extract_links(html):
    p = _LinkExtractor()
    try: p.feed(html)
    except Exception: pass
    return p.links


_DDG_LINK_RE    = re.compile(r'class="result__a"[^>]*href="(https?://[^"]+)"', re.IGNORECASE)
_DDG_SNIPPET_RE = re.compile(r'uddg=(https?[^&"]+)', re.IGNORECASE)


def _extract_ddg_links(html):
    links = [unquote(m.group(1)) for m in _DDG_LINK_RE.finditer(html)]
    links += [unquote(m.group(1)) for m in _DDG_SNIPPET_RE.finditer(html)]
    return links


_BING_NOISE    = re.compile(r"bing\.com", re.IGNORECASE)
_YAHOO_NOISE   = re.compile(r"yimg\.com|yahoo\.com|doubleclick\.net|googleadservices", re.IGNORECASE)
_STATIC_EXT    = re.compile(r"\.(css|js|png|jpg|jpeg|gif|svg|ico|webp|woff2?|ttf|eot)(\?|$)", re.IGNORECASE)
_YAHOO_RU_PATH = re.compile(r"/RU=([^/&]+)")
_DDG_NOISE     = re.compile(r"duckduckgo\.com|duck\.com", re.IGNORECASE)


def _yahoo_link_extractor(html):
    raw = _extract_links(html)
    out = []
    for u in raw:
        if "r.search.yahoo.com" in u or "/r/" in u:
            parsed = urlparse(u)
            qs = parse_qs(parsed.query)
            if "RU" in qs:
                real = unquote(qs["RU"][0])
                if real.startswith(("http://", "https://")): u = real
            else:
                m = _YAHOO_RU_PATH.search(parsed.path)
                if m:
                    real = unquote(m.group(1))
                    if real.startswith(("http://", "https://")): u = real
        out.append(u)
    return out


# ─── FAST FETCH (v20.0) — uses TLS rotation + better retry ──────────────────

async def _generic_engine_fetch(session, method, url, *, params=None, data=None,
                                  engine, page, max_res, chunk_id, referer,
                                  link_extractor, noise_filter, max_retries=None):
    if max_retries is None:
        max_retries = MAX_RETRIES
    active_session = session
    fallback_session = None
    try:
        for attempt in range(max_retries):
            wait_secs = await circuit_breaker.check(url)
            if wait_secs > 0:
                await asyncio.sleep(min(wait_secs, 30.0))

            profile = getattr(active_session, "_tls_profile", None) or get_tls_profile()
            origin = referer.rstrip("/") if data is not None else None
            headers = build_headers_from_profile(profile, referer=referer, origin=origin)
            spoof_xff_headers(headers, probability=0.35)
            if data is not None:
                headers["Content-Type"] = "application/x-www-form-urlencoded"
            try:
                if method == "GET":
                    resp = await active_session.get(url, params=params, headers=headers,
                                                     timeout=20)
                else:
                    resp = await active_session.post(url, data=data, headers=headers, timeout=20)
                status = resp.status_code; html = resp.text
                if status == 429:
                    await circuit_breaker.record(url, blocked=True)
                    backoff = humanize_delay((2 ** attempt) * 3.0)
                    await asyncio.sleep(backoff)
                    continue
                if status in (403, 503):
                    await circuit_breaker.record(url, blocked=True)
                    await asyncio.sleep(humanize_delay((2 ** attempt) * 1.5))
                    continue
                if status != 200:
                    await circuit_breaker.record(url, blocked=False)
                    return [], False
                if _is_captcha(html):
                    await circuit_breaker.record(url, blocked=True)
                    await _on_captcha_detected(engine, chunk_id, getattr(active_session, "_cur_proxy", None))
                    continue
                if _is_degraded(html, engine):
                    await circuit_breaker.record(url, blocked=True)
                    if attempt < max_retries - 1:
                        await asyncio.sleep(humanize_delay((2 ** attempt) * 1.5))
                        continue
                    return [], True
                raw = link_extractor(html)
                urls = [u for u in raw if u.startswith("http")
                        and not noise_filter(u) and not _STATIC_EXT.search(u)]
                urls = list(dict.fromkeys(urls))[:max_res]
                await circuit_breaker.record(url, blocked=False)
                return urls, False
            except asyncio.TimeoutError:
                await circuit_breaker.record(url, blocked=True)
                await asyncio.sleep(humanize_delay((2 ** attempt) * 1.2))
            except CurlError as exc:
                if (_is_proxy_error(exc) and PROXY_ENABLED and len(_proxy_pool) > 1
                        and attempt < max_retries - 1):
                    cur_proxy = getattr(active_session, "_cur_proxy", None)
                    if fallback_session is not None: await fallback_session.close()
                    fallback_session = _make_fallback_session(exclude_proxy=cur_proxy)
                    active_session = fallback_session
                    await asyncio.sleep(humanize_delay(0.8))
                    continue
                await asyncio.sleep(humanize_delay((2 ** attempt) * 1.2))
            except Exception as exc:
                log.error(f"[C{chunk_id}][{engine.upper()}] err: {exc}")
                return [], False
        return [], True
    finally:
        if fallback_session is not None:
            await fallback_session.close()


async def fetch_page_bing(session, dork, page, max_res, chunk_id=0):
    base_params = {"q": translate_dork(dork, "bing"), "count": min(max_res, 10),
                   "first": (page-1)*10+1, "setlang": "en"}
    return await _generic_engine_fetch(
        session, "GET", "https://www.bing.com/search",
        params=vary_bing_params(base_params),
        engine="bing", page=page, max_res=max_res, chunk_id=chunk_id,
        referer="https://www.bing.com/",
        link_extractor=_extract_links,
        noise_filter=lambda u: bool(_BING_NOISE.search(u)),
    )


async def fetch_page_yahoo(session, dork, page, max_res, chunk_id=0):
    base_params = {"p": translate_dork(dork, "yahoo"), "b": (page-1)*10+1,
                   "pz": min(max_res, 10), "vl": "lang_en"}
    return await _generic_engine_fetch(
        session, "GET", "https://search.yahoo.com/search",
        params=vary_yahoo_params(base_params),
        engine="yahoo", page=page, max_res=max_res, chunk_id=chunk_id,
        referer="https://search.yahoo.com/",
        link_extractor=_yahoo_link_extractor,
        noise_filter=lambda u: bool(_YAHOO_NOISE.search(u)),
    )


async def fetch_page_duckduckgo(session, dork, page, max_res, chunk_id=0):
    if page > 1: return [], False
    return await _generic_engine_fetch(
        session, "POST", "https://html.duckduckgo.com/html/",
        data={"q": translate_dork(dork, "duckduckgo"), "b": "", "kl": "us-en", "df": ""},
        engine="duckduckgo", page=page, max_res=max_res, chunk_id=chunk_id,
        referer="https://duckduckgo.com/",
        link_extractor=_extract_ddg_links,
        noise_filter=lambda u: bool(_DDG_NOISE.search(u)),
    )


async def fetch_all_pages(session, dork, engine, pages, max_res, chunk_id=0):
    sorted_pages = [min(pages)] if engine == "duckduckgo" else sorted(pages)
    fetch_fn = {"bing":fetch_page_bing, "yahoo":fetch_page_yahoo,
                "duckduckgo":fetch_page_duckduckgo}[engine]

    async def _fetch_with_stagger(page, idx):
        if idx > 0:
            await asyncio.sleep(humanize_delay(0.05 * idx, sigma_ratio=0.4))
        return await fetch_fn(session, dork, page, max_res, chunk_id)

    tasks = [_fetch_with_stagger(p, i) for i, p in enumerate(sorted_pages)]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    all_urls = []; degraded_total = 0
    for res in results:
        if isinstance(res, Exception): continue
        urls, degraded = res
        if degraded: degraded_total += 1
        all_urls.extend(urls)
    return all_urls, degraded_total


# ══════════════════════════════════════════════════════════════════════════════
# ─── XTREAM MODE — YAHOO BRUTEFORCE @ 1000 URLs/sec (FIXED) ─────────────────
# ══════════════════════════════════════════════════════════════════════════════

YAHOO_ENDPOINTS = [
    "https://search.yahoo.com/search",
    "https://uk.search.yahoo.com/search",
    "https://ca.search.yahoo.com/search",
    "https://au.search.yahoo.com/search",
    "https://in.search.yahoo.com/search",
    "https://sg.search.yahoo.com/search",
    "https://de.search.yahoo.com/search",
    "https://fr.search.yahoo.com/search",
    "https://es.search.yahoo.com/search",
    "https://br.search.yahoo.com/search",
    "https://it.search.yahoo.com/search",
    "https://nl.search.yahoo.com/search",
    "https://mx.search.yahoo.com/search",
    "https://nz.search.yahoo.com/search",
    "https://za.search.yahoo.com/search",
]

YAHOO_REFERERS = [
    "https://search.yahoo.com/",
    "https://www.yahoo.com/",
    "https://uk.search.yahoo.com/",
    "https://ca.search.yahoo.com/",
    "https://au.search.yahoo.com/",
    "https://in.search.yahoo.com/",
    "https://sg.search.yahoo.com/",
    "https://de.search.yahoo.com/",
    "https://fr.search.yahoo.com/",
]

YAHOO_HOMEPAGES = [
    "https://www.yahoo.com/",
    "https://search.yahoo.com/",
    "https://uk.yahoo.com/",
    "https://au.yahoo.com/",
    "https://ca.yahoo.com/",
]

BING_XTREAM_ENDPOINTS = [
    "https://www.bing.com/search",
    "https://cn.bing.com/search",
    "https://global.bing.com/search",
]

BING_XTREAM_REFERERS = [
    "https://www.bing.com/",
    "https://www.bing.com/search",
    "https://cn.bing.com/",
]

BING_HOMEPAGES = [
    "https://www.bing.com/",
    "https://cn.bing.com/",
]

BING_XTREAM_MARKETS = ["en-US", "en-GB", "en-CA", "en-AU", "en-IN", "en-SG", "en-NZ"]


async def xtream_fetch_yahoo(pool: XtreamSessionPool, dork: str, page: int,
                              max_res: int, worker_id: int,
                              sess=None) -> tuple[list, bool, bool]:
    owned  = sess is None
    burned = False; captcha = False
    if owned:
        sess = await pool.acquire()
    try:
        endpoint = random.choice(YAHOO_ENDPOINTS)
        referer  = random.choice(YAHOO_REFERERS)
        profile  = getattr(sess, "_tls_profile", None) or get_tls_profile("weighted")
        headers  = build_headers_from_profile(profile, referer=referer)
        spoof_xff_headers(headers, probability=0.35)

        base_params = {
            "p":  translate_dork(dork, "yahoo"),
            "b":  (page - 1) * 10 + 1,
            "pz": min(max_res, 10),
            "vl": "lang_en",
        }
        params = vary_yahoo_params(base_params)

        wait = await circuit_breaker.check(endpoint)
        if wait > 0:
            await asyncio.sleep(min(wait, 25.0))

        for attempt in range(XTREAM_MAX_RETRIES + 1):
            try:
                resp = await sess.get(endpoint, params=params, headers=headers,
                                       timeout=XTREAM_TIMEOUT)
                html = resp.text
                sc   = resp.status_code
                if sc == 429:
                    burned = True
                    await circuit_breaker.record(endpoint, blocked=True)
                    return [], True, False
                if sc in (403, 503):
                    burned = True
                    await circuit_breaker.record(endpoint, blocked=True)
                    if attempt < XTREAM_MAX_RETRIES:
                        await asyncio.sleep(humanize_delay(2.0))
                        continue
                    return [], True, False
                if sc != 200:
                    await circuit_breaker.record(endpoint, blocked=False)
                    return [], False, False
                if _is_captcha(html):
                    captcha = True; burned = True
                    await circuit_breaker.record(endpoint, blocked=True)
                    return [], True, True
                if _is_degraded(html, "yahoo"):
                    await circuit_breaker.record(endpoint, blocked=True)
                    if attempt < XTREAM_MAX_RETRIES:
                        await asyncio.sleep(humanize_delay(1.5))
                        continue
                    return [], False, False
                await circuit_breaker.record(endpoint, blocked=False)
                urls = _yahoo_link_extractor(html)
                urls = [u for u in urls if u.startswith("http")
                        and not _YAHOO_NOISE.search(u) and not _STATIC_EXT.search(u)]
                return list(dict.fromkeys(urls))[:max_res], False, False
            except (asyncio.TimeoutError, CurlError):
                await circuit_breaker.record(endpoint, blocked=True)
                if attempt < XTREAM_MAX_RETRIES:
                    await asyncio.sleep(humanize_delay(1.0))
                    continue
                return [], False, False
            except Exception as exc:
                log.debug(f"[XTREAM:Y:W{worker_id}] {exc}")
                return [], False, False
        return [], False, False
    finally:
        if owned:
            await pool.release(sess, burned=burned)


async def xtream_fetch_bing(pool: XtreamSessionPool, dork: str, page: int,
                             max_res: int, worker_id: int,
                             sess=None) -> tuple[list, bool, bool]:
    owned  = sess is None
    burned = False; captcha = False
    if owned:
        sess = await pool.acquire()
    try:
        endpoint = random.choice(BING_XTREAM_ENDPOINTS)
        referer  = random.choice(BING_XTREAM_REFERERS)
        profile  = getattr(sess, "_tls_profile", None) or get_tls_profile("weighted")
        headers  = build_headers_from_profile(profile, referer=referer)
        spoof_xff_headers(headers, probability=0.35)

        base_params = {
            "q":       translate_dork(dork, "bing"),
            "count":   min(max_res, 10),
            "first":   (page - 1) * 10 + 1,
            "setlang": "en",
            "mkt":     random.choice(BING_XTREAM_MARKETS),
        }
        params = vary_bing_params(base_params)

        wait = await circuit_breaker.check(endpoint)
        if wait > 0:
            await asyncio.sleep(min(wait, 25.0))

        for attempt in range(XTREAM_MAX_RETRIES + 1):
            try:
                resp = await sess.get(endpoint, params=params, headers=headers,
                                      timeout=XTREAM_TIMEOUT)
                html = resp.text
                sc   = resp.status_code
                if sc == 429:
                    burned = True
                    await circuit_breaker.record(endpoint, blocked=True)
                    return [], True, False
                if sc in (403, 503):
                    burned = True
                    await circuit_breaker.record(endpoint, blocked=True)
                    if attempt < XTREAM_MAX_RETRIES:
                        await asyncio.sleep(humanize_delay(2.0))
                        continue
                    return [], True, False
                if sc not in (200,):
                    await circuit_breaker.record(endpoint, blocked=False)
                    return [], False, False
                if _is_captcha(html):
                    captcha = True; burned = True
                    await circuit_breaker.record(endpoint, blocked=True)
                    return [], True, True
                if _is_degraded(html, "bing"):
                    await circuit_breaker.record(endpoint, blocked=True)
                    if attempt < XTREAM_MAX_RETRIES:
                        await asyncio.sleep(humanize_delay(1.5))
                        continue
                    return [], False, False
                await circuit_breaker.record(endpoint, blocked=False)
                urls = _extract_links(html)
                urls = [u for u in urls if u.startswith("http")
                        and not _BING_NOISE.search(u) and not _STATIC_EXT.search(u)]
                return list(dict.fromkeys(urls))[:max_res], False, False
            except (asyncio.TimeoutError, CurlError):
                await circuit_breaker.record(endpoint, blocked=True)
                if attempt < XTREAM_MAX_RETRIES:
                    await asyncio.sleep(humanize_delay(1.0))
                    continue
                return [], False, False
            except Exception as exc:
                log.debug(f"[XTREAM:B:W{worker_id}] {exc}")
                return [], False, False
        return [], False, False
    finally:
        if owned:
            await pool.release(sess, burned=burned)


async def xtream_worker(wid: int, queue: asyncio.Queue, results_q: asyncio.Queue,
                         pool: XtreamSessionPool, max_res: int, pages_per_dork: int,
                         min_score: int, stop_ev: asyncio.Event,
                         rate_limiter: asyncio.Semaphore,
                         xtream_engine: str,
                         captcha_counter: list,
                         captcha_lock: asyncio.Lock):
    await asyncio.sleep(humanize_delay(wid * XTREAM_WORKER_START_JITTER,
                                       sigma_ratio=0.3, distraction_prob=0.0))

    consecutive_fails = 0
    cooldown_until    = 0.0
    engine_toggle     = wid % 2

    while not stop_ev.is_set():
        try:
            dork = await asyncio.wait_for(queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue

        now = time.time()
        if cooldown_until > now:
            await asyncio.sleep(cooldown_until - now)
            if stop_ev.is_set():
                queue.task_done()
                break

        if xtream_engine == "both":
            use_engine = "yahoo" if engine_toggle % 2 == 0 else "bing"
            engine_toggle += 1
        else:
            use_engine = xtream_engine

        fetch_fn = xtream_fetch_yahoo if use_engine == "yahoo" else xtream_fetch_bing
        tag      = f"{use_engine}-xtream"

        sess = await pool.acquire()
        all_urls = []; any_burned = False; any_captcha = False

        try:
            for page in range(1, pages_per_dork + 1):
                if stop_ev.is_set() or any_burned:
                    break
                async with rate_limiter:
                    urls, burned, captcha = await fetch_fn(
                        pool, dork, page, max_res, wid, sess=sess
                    )
                all_urls.extend(urls)
                if burned:
                    any_burned = True
                    break
                if captcha:
                    any_captcha = True
                    any_burned  = True
                    break
                if page < pages_per_dork and not stop_ev.is_set():
                    delay = humanize_delay(
                        random.uniform(XTREAM_PAGE_DELAY_MIN, XTREAM_PAGE_DELAY_MAX),
                        sigma_ratio=0.25,
                        distraction_prob=0.03,
                    )
                    await asyncio.sleep(delay)
        finally:
            await pool.release(sess, burned=any_burned)

        scored = filter_scored(all_urls, min_score)
        try:
            results_q.put_nowait((dork, tag, scored, len(all_urls), any_captcha))
        except asyncio.QueueFull:
            await results_q.put((dork, tag, scored, len(all_urls), any_captcha))

        queue.task_done()

        if any_captcha:
            async with captcha_lock:
                captcha_counter[0] += 1

        if any_burned:
            consecutive_fails += 1
            backoff = min(consecutive_fails * 2.0, 30.0)
            cooldown_until = time.time() + backoff
            await asyncio.sleep(humanize_delay(backoff, sigma_ratio=0.2))
        elif all_urls:
            consecutive_fails = max(0, consecutive_fails - 1)
            await asyncio.sleep(humanize_delay(
                random.uniform(XTREAM_MIN_DELAY, XTREAM_MAX_DELAY),
                sigma_ratio=0.3,
            ))
        else:
            consecutive_fails += 1
            await asyncio.sleep(humanize_delay(0.5, sigma_ratio=0.4))


async def run_xtream_job(chat_id: int, dorks: list, context):
    from collections import Counter
    sess_cfg      = get_session(chat_id)
    use_tor       = sess_cfg.get("tor", False)
    min_score     = sess_cfg.get("min_score", 30)
    max_res       = sess_cfg.get("max_results", 10)
    xtream_engine = sess_cfg.get("xtream_engine", "yahoo")

    cleaned     = dedupe_dorks(dorks)
    valid_dorks = [d for d in cleaned if validate_dork(d)[0]]
    total_dorks = len(valid_dorks)
    if total_dorks == 0:
        await context.bot.send_message(chat_id, "⚠️ No valid dorks.")
        active_jobs.pop(chat_id, None); return

    start_time    = time.time()
    n_chunks      = XTREAM_CHUNKS
    workers_n     = XTREAM_WORKERS_PER_CHUNK
    total_workers = n_chunks * workers_n

    rate_limiter    = asyncio.Semaphore(total_workers)
    captcha_counter = [0]  # shared list, protected by lock
    captcha_lock    = asyncio.Lock()

    alive_proxies = sum(1 for p in _proxy_pool if p["alive"])
    proxy_info = (
        "🧅 TOR" if use_tor else
        f"🔄 {alive_proxies}/{len(_proxy_pool)} alive proxies" if PROXY_ENABLED and alive_proxies else
        "🔓 Direct"
    )
    engine_display = {"yahoo": "YAHOO (15 mirrors)", "bing": "BING (3 mirrors)",
                      "both": "YAHOO + BING"}.get(xtream_engine, xtream_engine.upper())

    status_msg = await context.bot.send_message(
        chat_id,
        f"⚡⚡⚡ XTREAM MODE ENGAGED ⚡⚡⚡\n{'━'*30}\n"
        f"📋 Dorks      : {total_dorks}\n"
        f"🎯 Engine     : {engine_display}\n"
        f"🚀 Target RPS : {XTREAM_TARGET_RPS}/sec\n"
        f"📄 Pages/dork : {XTREAM_PAGES_PER_DORK}\n"
        f"⚙️ Workers    : {total_workers} ({n_chunks}×{workers_n})\n"
        f"🔄 Session pool: {XTREAM_SESSION_POOL_SIZE}\n"
        f"🛡 TLS profiles: {len(TLS_PROFILES)} rotating\n"
        f"🌐 Network    : {proxy_info}\n{'━'*30}\n⏳ Warming sessions...",
    )

    pool = XtreamSessionPool(size=XTREAM_SESSION_POOL_SIZE, engine=xtream_engine)
    await pool.initialize(use_tor=use_tor)

    queue     = asyncio.Queue(maxsize=total_dorks + 10)
    results_q = asyncio.Queue(maxsize=total_dorks * 2)
    stop_ev   = asyncio.Event()
    active_stop_evs[chat_id] = stop_ev

    for d in valid_dorks:
        await queue.put(d)

    worker_tasks = [
        asyncio.create_task(xtream_worker(
            i, queue, results_q, pool, max_res, XTREAM_PAGES_PER_DORK,
            min_score, stop_ev, rate_limiter, xtream_engine, captcha_counter, captcha_lock,
        ))
        for i in range(total_workers)
    ]

    processed = 0; total_raw = 0; total_captcha = 0
    seen_norm: set  = set()
    seen_urls: set  = set()
    all_scored: list = []
    last_edit  = 0.0
    peak_rps   = 0.0
    last_rps_t = time.time(); rps_count = 0; current_rps = 0.0

    tmp_file = tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", delete=False,
                                            prefix=f"xtream_{chat_id}_", suffix=".txt")
    tmp_path = tmp_file.name
    tmp_file.write(f"# XTREAM Mode v21 — {engine_display}\n# {datetime.now()}\n")
    tmp_file.write(f"# Dorks: {total_dorks} | Pages: {XTREAM_PAGES_PER_DORK} | Workers: {total_workers}\n\n")
    tmp_file.close()
    incremental_f = open(tmp_path, "a", encoding="utf-8")

    async def _job_timeout():
        await asyncio.sleep(JOB_TIMEOUT)
        stop_ev.set()
    timeout_task = asyncio.create_task(_job_timeout())

    try:
        while processed < total_dorks and not stop_ev.is_set():
            try:
                dork, engine, scored, raw_cnt, was_captcha = await asyncio.wait_for(
                    results_q.get(), timeout=CHUNK_STALL_TIMEOUT)
            except asyncio.TimeoutError:
                if all(t.done() for t in worker_tasks): break
                continue

            processed  += 1; total_raw += raw_cnt; rps_count += raw_cnt
            if was_captcha:
                async with captcha_lock:
                    total_captcha += 1

            for sc, url in scored:
                norm = _normalize_url_for_dedup(url)
                if norm not in seen_norm:
                    seen_norm.add(norm); seen_urls.add(url)
                    all_scored.append((sc, url))
                    try: incremental_f.write(f"{url}\n")
                    except Exception: pass

            if processed > 0 and processed % 20 == 0:
                async with captcha_lock:
                    captcha_rate = captcha_counter[0] / max(processed, 1)
                if captcha_rate > XTREAM_CAPTCHA_RATE_LIMIT:
                    await asyncio.sleep(random.uniform(1.0, 2.5))

            now = time.time()
            if now - last_rps_t >= 2.0:
                current_rps = rps_count / (now - last_rps_t)
                if current_rps > peak_rps: peak_rps = current_rps
                rps_count = 0; last_rps_t = now

            if time.time() - last_edit > 3.5:
                pct     = int(processed / total_dorks * 100)
                bar     = "█" * (pct // 10) + "░" * (10 - pct // 10)
                elapsed = int(time.time() - start_time)
                eta     = int((elapsed / processed) * (total_dorks - processed)) if processed else 0
                async with captcha_lock:
                    captcha_rate = captcha_counter[0] / max(processed, 1)
                try:
                    await context.bot.edit_message_text(
                        chat_id=chat_id, message_id=status_msg.message_id,
                        text=(f"⚡⚡⚡ XTREAM RUNNING ⚡⚡⚡\n{'━'*30}\n"
                              f"[{bar}] {pct}%\n"
                              f"✅ Dorks    : {processed}/{total_dorks}\n"
                              f"🔍 Raw URLs : {total_raw}\n"
                              f"🎯 Targets  : {len(all_scored)}\n"
                              f"📊 RPS      : {current_rps:.0f} (peak {peak_rps:.0f})\n"
                              f"🛡 Captchas : {captcha_counter[0]} ({captcha_rate:.0%})\n"
                              f"⏱ {elapsed}s | ETA {eta}s\n{'━'*30}"),
                    )
                    last_edit = time.time()
                except Exception: pass

        stop_ev.set()
        for t in worker_tasks: t.cancel()
        await asyncio.gather(*worker_tasks, return_exceptions=True)

    except asyncio.CancelledError:
        stop_ev.set()
        for t in worker_tasks: t.cancel()
        await asyncio.gather(*worker_tasks, return_exceptions=True)
        raise
    finally:
        try: incremental_f.close()
        except Exception: pass
        timeout_task.cancel()
        try: await timeout_task
        except Exception: pass
        await pool.close_all()
        active_jobs.pop(chat_id, None)
        active_stop_evs.pop(chat_id, None)

    all_scored.sort(reverse=True)
    elapsed = int(time.time() - start_time)
    avg_rps = total_raw / max(elapsed, 1)

    high = [(s, u) for s, u in all_scored if s >= 70]
    med  = [(s, u) for s, u in all_scored if 40 <= s < 70]
    low  = [(s, u) for s, u in all_scored if s < 40]
    domain_counts = Counter(extract_domain(u) for _, u in all_scored)
    top_domains   = domain_counts.most_common(10)

    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write(f"# XTREAM Mode v21 — {engine_display}\n# {datetime.now()}\n")
        f.write(f"# Dorks: {total_dorks} | Raw: {total_raw} | Targets: {len(all_scored)}\n")
        f.write(f"# Avg RPS: {avg_rps:.0f} | Peak RPS: {peak_rps:.0f} | Time: {elapsed}s\n")
        f.write(f"# Captchas: {total_captcha} | Min-score: {min_score}\n\n")
        if top_domains:
            f.write("# ── TOP DOMAINS ────────────────────────\n")
            for dom, cnt in top_domains:
                f.write(f"# {cnt:>4}  {dom}\n")
            f.write("\n")
        if high:
            f.write(f"# ── HIGH VALUE (≥70) — {len(high)} ──────────────\n")
            for _, u in high: f.write(f"{u}\n")
        if med:
            f.write(f"\n# ── MEDIUM (40-69) — {len(med)} ──────────────\n")
            for _, u in med: f.write(f"{u}\n")
        if low and min_score < 40:
            f.write(f"\n# ── LOW (<40) — {len(low)} ──────────────\n")
            for _, u in low: f.write(f"{u}\n")

    dom_summary = "\n".join(f"  {cnt}× {d}" for d, cnt in top_domains[:5]) if top_domains else "  (none)"
    try:
        await context.bot.edit_message_text(
            chat_id=chat_id, message_id=status_msg.message_id,
            text=(f"🏁 XTREAM COMPLETE!\n{'━'*30}\n"
                  f"📋 Dorks       : {total_dorks}\n"
                  f"🔍 Raw URLs    : {total_raw}\n"
                  f"🎯 Targets     : {len(all_scored)}\n"
                  f"📊 Avg RPS     : {avg_rps:.0f} | Peak: {peak_rps:.0f}\n"
                  f"🛡 Captchas    : {total_captcha}\n"
                  f"⏱ Total time  : {elapsed}s\n"
                  f"{'━'*30}\n"
                  f"🏆 Top domains:\n{dom_summary}"),
        )
    except Exception: pass

    if all_scored:
        sent = False
        for attempt in range(4):
            try:
                with open(tmp_path, "rb") as f:
                    await context.bot.send_document(
                        chat_id, f,
                        filename=f"xtream_{total_dorks}d_{len(all_scored)}u.txt",
                        caption=(f"⚡ XTREAM v21 RESULTS\n"
                                 f"🎯 {len(all_scored)} URLs | 📊 {avg_rps:.0f} avg / {peak_rps:.0f} peak RPS\n"
                                 f"⏱ {elapsed}s | 🛡 {total_captcha} captchas"),
                        read_timeout=60, write_timeout=120, connect_timeout=30,
                    )
                sent = True
                break
            except Exception as exc:
                log.warning(f"[XTREAM] send_document attempt {attempt+1} failed: {exc}")
                if attempt < 3:
                    await asyncio.sleep(5 * (attempt + 1))
        if not sent:
            try:
                await context.bot.send_message(
                    chat_id,
                    f"⚠️ File delivery failed after 4 attempts.\n"
                    f"Results: {len(all_scored)} URLs collected — tmp file: {tmp_path}",
                )
            except Exception: pass
    else:
        await context.bot.send_message(chat_id, "⚠️ No URLs matched filter. Try lowering /filter or adding proxies.")

    try: os.unlink(tmp_path)
    except OSError: pass


# ══════════════════════════════════════════════════════════════════════════════
# ─── STANDARD WORKER / CHUNK / JOB (boosted to 200 RPS) ──────────────────────
# ══════════════════════════════════════════════════════════════════════════════

async def dork_worker(wid, chunk_id, queue, results_q, engines, pages, max_res,
                       session, min_score, stop_ev, slowdown_ev):
    eidx = wid % len(engines)
    empty_streak = consecutive_hits = 0
    while not stop_ev.is_set():
        try:
            dork = await asyncio.wait_for(queue.get(), timeout=2.0)
        except asyncio.TimeoutError:
            continue
        engine = engines[eidx % len(engines)]; eidx += 1
        raw, degraded_cnt = [], 0
        try:
            raw, degraded_cnt = await asyncio.wait_for(
                fetch_all_pages(session, dork, engine, pages, max_res, chunk_id),
                timeout=WORKER_FETCH_TIMEOUT,
            )
        except asyncio.TimeoutError:
            log.warning(f"[C{chunk_id}][W{wid}] timeout: {dork[:50]}")
        except asyncio.CancelledError:
            try: results_q.put_nowait((dork, engine, [], 0, 0))
            except asyncio.QueueFull: pass
            queue.task_done(); raise
        except Exception as exc:
            log.warning(f"[C{chunk_id}][W{wid}] err: {exc}")
        scored = filter_scored(raw, min_score)
        try: results_q.put_nowait((dork, engine, scored, len(raw), degraded_cnt))
        except asyncio.QueueFull: await results_q.put((dork, engine, scored, len(raw), degraded_cnt))
        queue.task_done()
        if raw:
            consecutive_hits += 1; empty_streak = 0
            if consecutive_hits >= FAST_STREAK_THRESHOLD:
                delay = random.uniform(FAST_MIN_DELAY, FAST_MAX_DELAY)
            else:
                delay = random.uniform(MIN_DELAY, MAX_DELAY)
        else:
            consecutive_hits = 0; empty_streak += 1
            delay = random.uniform(MIN_DELAY, MAX_DELAY)
            if empty_streak >= 3:
                delay += min(empty_streak * 1.0, 8.0)
        if slowdown_ev.is_set():
            delay += random.uniform(1.0, 2.5)
        await asyncio.sleep(delay)


async def run_chunk(chunk_id, dorks, engines, pages, max_res, use_tor, min_score,
                     workers_n, progress_q, global_stop_ev, proxy=None):
    session = _make_isolated_session(use_tor=use_tor, proxy=proxy)
    queue = asyncio.Queue(maxsize=len(dorks) * 2)
    results_q = asyncio.Queue(maxsize=500)
    stop_ev = asyncio.Event(); slowdown_ev = asyncio.Event()
    for d in dorks: await queue.put(d)
    total = len(dorks); processed = empty_count = chunk_raw = chunk_degraded = 0
    chunk_scored = []
    async def _watch_global():
        while not stop_ev.is_set():
            if global_stop_ev.is_set(): stop_ev.set()
            await asyncio.sleep(0.5)
    worker_tasks = [asyncio.create_task(dork_worker(i, chunk_id, queue, results_q,
                    engines, pages, max_res, session, min_score, stop_ev, slowdown_ev))
                    for i in range(workers_n)]
    global_watcher = asyncio.create_task(_watch_global())
    try:
        while processed < total and not stop_ev.is_set():
            try:
                dork, engine, scored, raw_cnt, deg_cnt = await asyncio.wait_for(
                    results_q.get(), timeout=CHUNK_STALL_TIMEOUT)
            except asyncio.TimeoutError:
                if all(t.done() for t in worker_tasks): break
                continue
            processed += 1; chunk_raw += raw_cnt; chunk_degraded += deg_cnt
            if raw_cnt == 0: empty_count += 1
            chunk_scored.extend(scored)
            empty_rate = empty_count / max(processed, 1)
            if empty_rate >= EMPTY_RATE_SLOWDOWN and not slowdown_ev.is_set():
                slowdown_ev.set()
            elif empty_rate < EMPTY_RATE_RECOVER and slowdown_ev.is_set():
                slowdown_ev.clear()
            try: progress_q.put_nowait({"chunk_id":chunk_id,"processed":processed,
                                         "total":total,"raw":raw_cnt,"kept":len(scored)})
            except asyncio.QueueFull: pass
        for t in worker_tasks:
            if not t.done(): t.cancel()
        await asyncio.gather(*worker_tasks, return_exceptions=True)
    except asyncio.CancelledError:
        stop_ev.set()
        for t in worker_tasks: t.cancel()
        await asyncio.gather(*worker_tasks, return_exceptions=True)
        raise
    finally:
        global_watcher.cancel()
        await asyncio.gather(global_watcher, return_exceptions=True)
        await session.close()
    return {"chunk_id":chunk_id,"scored":chunk_scored,"raw_count":chunk_raw,
            "degraded_count":chunk_degraded,"processed":processed,"empty_count":empty_count}


async def run_dork_job(chat_id, dorks, context):
    sess = get_session(chat_id)

    if sess.get("xtream", False):
        await run_xtream_job(chat_id, dorks, context)
        return

    engines = sess.get("engines", list(ENGINES))
    workers_n = min(sess.get("workers", WORKERS_PER_CHUNK), MAX_WORKERS_PER_CHUNK)
    max_res = sess.get("max_results", MAX_RESULTS)
    pages = sess.get("pages", [1])
    use_tor = sess.get("tor", False)
    min_score = sess.get("min_score", 30)
    n_chunks = max(1, sess.get("chunks", N_CHUNKS))

    cleaned = dedupe_dorks(dorks)
    valid_dorks = []; invalid_dorks = []
    for d in cleaned:
        ok, msg = validate_dork(d)
        if ok: valid_dorks.append(d)
        else: invalid_dorks.append((d, msg))
    dorks = valid_dorks; total_dorks = len(dorks)
    if total_dorks == 0:
        await context.bot.send_message(chat_id, "⚠️ No valid dorks.")
        active_jobs.pop(chat_id, None); return

    pages_str = ", ".join(str(p) for p in pages)
    start_time = time.time()
    chunk_size = max(1, -(-total_dorks // n_chunks))
    chunks = [dorks[i:i+chunk_size] for i in range(0, total_dorks, chunk_size)]
    actual_chunks = len(chunks)

    tmp_file = tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", delete=False,
                                            prefix=f"dork_{chat_id}_", suffix=".txt")
    tmp_path = tmp_file.name
    tmp_file.write(f"# Dork Parser v20.0 — SQL Targeted Results\n")
    tmp_file.write(f"# Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    tmp_file.write(f"# Dorks: {total_dorks} | Pages: {pages_str} | Chunks: {actual_chunks}\n\n")
    tmp_file.close()

    alive_proxies = sum(1 for p in _proxy_pool if p["alive"])
    if use_tor: proxy_info = "🧅 TOR"
    elif PROXY_ENABLED and alive_proxies:
        proxy_info = f"🔄 {alive_proxies}/{len(_proxy_pool)} alive"
    elif PROXY_ENABLED and _proxy_pool:
        proxy_info = f"⚠️ {len(_proxy_pool)} 0-alive"
    elif not PROXY_ENABLED and _proxy_pool:
        proxy_info = f"⏸ DISABLED"
    else: proxy_info = "🔓 Direct"

    status_msg = await context.bot.send_message(
        chat_id,
        f"🕷 DORK PARSER v20.0 — STARTED\n{'━'*30}\n"
        f"📋 Dorks    : {total_dorks}"
        + (f" (⚠️ {len(invalid_dorks)} skip)" if invalid_dorks else "")
        + f"\n📄 Pages    : {pages_str}\n"
        f"⚡ Chunks   : {actual_chunks}\n"
        f"⚙️ Workers  : {workers_n}/chunk (total {workers_n*actual_chunks})\n"
        f"🔍 Engines  : {' + '.join(e.upper() for e in engines)}\n"
        f"🛡 Filter   : SQL ≥{min_score}\n"
        f"🌐 Network  : {proxy_info}\n"
        f"🔒 TLS      : {len(TLS_PROFILES)} profiles rotating\n"
        f"🎯 Target   : ~200 URLs/sec\n{'━'*30}\n⏳ Starting...",
    )

    global_stop_ev = asyncio.Event()
    active_stop_evs[chat_id] = global_stop_ev
    progress_q = asyncio.Queue(maxsize=total_dorks * 2)
    chunk_counters = {i: {"processed":0,"total":len(chunks[i])} for i in range(actual_chunks)}
    agg_raw=[0]; agg_kept=[0]; last_edit=[0.0]; total_processed=[0]
    rps_window=[time.time(), 0, 0.0]

    async def _status_updater():
        while not global_stop_ev.is_set():
            drained = False
            while True:
                try:
                    ev = progress_q.get_nowait()
                    chunk_counters[ev["chunk_id"]]["processed"] = ev["processed"]
                    agg_raw[0] += ev["raw"]; agg_kept[0] += ev["kept"]
                    total_processed[0] += 1
                    rps_window[1] += ev["raw"]
                    drained = True
                except asyncio.QueueEmpty: break
            now = time.time()
            if now - rps_window[0] >= 2.0:
                rps_window[2] = rps_window[1] / (now - rps_window[0])
                rps_window[1] = 0; rps_window[0] = now
            if drained and time.time() - last_edit[0] > 4.0:
                proc = total_processed[0]
                pct = int(proc / total_dorks * 100) if total_dorks else 100
                bar = "█" * (pct//10) + "░" * (10-pct//10)
                elapsed = int(time.time() - start_time)
                eta = int((elapsed/proc) * (total_dorks-proc)) if proc else 0
                cinfo = " | ".join(f"C{i}:{chunk_counters[i]['processed']}/{chunk_counters[i]['total']}"
                                    for i in range(actual_chunks))
                try:
                    await context.bot.edit_message_text(
                        chat_id=chat_id, message_id=status_msg.message_id,
                        text=(f"⚡ PARSING [{actual_chunks}c]\n{'━'*30}\n"
                              f"[{bar}] {pct}%\n"
                              f"✅ Done: {proc}/{total_dorks}\n"
                              f"🎯 SQL: {agg_kept[0]} | 🗑 {agg_raw[0]-agg_kept[0]}\n"
                              f"📊 RPS: {rps_window[2]:.0f}/sec\n"
                              f"⏱ {elapsed}s | ETA {eta}s\n📦 {cinfo}\n{'━'*30}"),
                    )
                    last_edit[0] = time.time()
                except Exception: pass
            await asyncio.sleep(0.5)

    async def _job_timeout():
        await asyncio.sleep(JOB_TIMEOUT); global_stop_ev.set()
    status_task = asyncio.create_task(_status_updater())
    timeout_task = asyncio.create_task(_job_timeout())

    chunk_proxies = [get_random_proxy_url() if not use_tor else None for _ in range(actual_chunks)]
    chunk_results = []
    try:
        chunk_tasks = []
        for i, chunk_dorks in enumerate(chunks):
            if i > 0: await asyncio.sleep(random.uniform(*CHUNK_STAGGER_DELAY))
            task = asyncio.create_task(run_chunk(i, chunk_dorks, engines, pages, max_res,
                use_tor, min_score, workers_n, progress_q, global_stop_ev, proxy=chunk_proxies[i]))
            chunk_tasks.append(task)
        chunk_results = await asyncio.gather(*chunk_tasks, return_exceptions=True)
    except asyncio.CancelledError:
        global_stop_ev.set()
        for t in chunk_tasks: t.cancel()
        await asyncio.gather(*chunk_tasks, return_exceptions=True)
        raise
    finally:
        global_stop_ev.set()
        timeout_task.cancel(); status_task.cancel()
        await asyncio.gather(timeout_task, status_task, return_exceptions=True)
        active_jobs.pop(chat_id, None)
        active_stop_evs.pop(chat_id, None)

    seen_urls=set(); all_scored=[]; total_raw=total_degraded=failed_chunks=0
    for result in chunk_results:
        if isinstance(result, Exception): failed_chunks += 1; continue
        for sc, url in result["scored"]:
            if url not in seen_urls:
                seen_urls.add(url); all_scored.append((sc, url))
        total_raw += result["raw_count"]
        total_degraded += result["degraded_count"]
    all_scored.sort(reverse=True)
    unique_cnt = len(all_scored)
    elapsed = int(time.time() - start_time)
    avg_rps = total_raw / max(elapsed, 1)

    high = [(s,u) for s,u in all_scored if s>=70]
    med  = [(s,u) for s,u in all_scored if 40<=s<70]
    low  = [(s,u) for s,u in all_scored if s<40]
    with open(tmp_path, "a", encoding="utf-8") as f:
        if high:
            f.write(f"# HIGH (≥70) — {len(high)}\n")
            for _,u in high: f.write(f"{u}\n")
        if med:
            f.write(f"\n# MEDIUM — {len(med)}\n")
            for _,u in med: f.write(f"{u}\n")
        if low and min_score < 40:
            f.write(f"\n# LOW — {len(low)}\n")
            for _,u in low: f.write(f"{u}\n")

    try:
        await context.bot.edit_message_text(
            chat_id=chat_id, message_id=status_msg.message_id,
            text=(f"🏁 JOB COMPLETE!\n{'━'*30}\n"
                  f"📋 Dorks   : {total_dorks}\n📄 Pages   : {pages_str}\n"
                  f"⚡ Chunks  : {actual_chunks}\n🔍 Raw     : {total_raw}\n"
                  f"🎯 SQL     : {unique_cnt}\n🗑 Drop    : {total_raw-unique_cnt}\n"
                  f"⚠️ Degraded: {total_degraded}\n"
                  f"📊 Avg RPS : {avg_rps:.0f}/sec\n"
                  f"⏱ Time    : {elapsed}s\n{'━'*30}"),
        )
    except Exception: pass

    if all_scored:
        sent = False
        for attempt in range(4):
            try:
                with open(tmp_path, "rb") as f:
                    await context.bot.send_document(
                        chat_id, f,
                        filename=f"sql_{total_dorks}d_{unique_cnt}u.txt",
                        caption=f"🎯 {unique_cnt} URLs | 📊 {avg_rps:.0f} RPS | ⏱ {elapsed}s",
                        read_timeout=60, write_timeout=120, connect_timeout=30,
                    )
                sent = True
                break
            except Exception as exc:
                log.warning(f"[JOB] send_document attempt {attempt+1} failed: {exc}")
                if attempt < 3:
                    await asyncio.sleep(5 * (attempt + 1))
        if not sent:
            try:
                await context.bot.send_message(
                    chat_id,
                    f"⚠️ File delivery failed after 4 attempts.\n"
                    f"Results: {unique_cnt} URLs — tmp file: {tmp_path}",
                )
            except Exception: pass
    else:
        await context.bot.send_message(chat_id, "⚠️ No URLs matched filter.")
    try: os.unlink(tmp_path)
    except OSError: pass


# ─── UI HELPERS ──────────────────────────────────────────────────────────────
def get_session(chat_id):
    if chat_id not in user_sessions:
        user_sessions[chat_id] = dict(DEFAULT_SESSION)
    return user_sessions[chat_id]


def page_keyboard(selected):
    rows, row = [], []
    for p in range(1, 71):
        row.append(InlineKeyboardButton(f"✅{p}" if p in selected else str(p),
                                         callback_data=f"pg_{p}"))
        if len(row) == 5: rows.append(row); row = []
    if row: rows.append(row)
    rows.append([
        InlineKeyboardButton("🔁 All (1-70)", callback_data="pg_all"),
        InlineKeyboardButton("❌ Clear", callback_data="pg_clear"),
        InlineKeyboardButton("✅ Confirm", callback_data="pg_confirm"),
    ])
    return InlineKeyboardMarkup(rows)


def main_menu_keyboard(sess):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📂 Bulk Upload", callback_data="m_bulk"),
         InlineKeyboardButton("🔍 Single Dork", callback_data="m_single")],
        [InlineKeyboardButton("📄 Select Pages", callback_data="m_pages"),
         InlineKeyboardButton("⚙️ Settings", callback_data="m_settings")],
        [InlineKeyboardButton(f"🧅 Tor {'ON' if sess.get('tor') else 'OFF'}", callback_data="m_tor"),
         InlineKeyboardButton(f"🛡 SQL ≥{sess.get('min_score',30)}", callback_data="m_filter")],
        [InlineKeyboardButton(f"⚡ Xtream {'ON' if sess.get('xtream') else 'OFF'}",
                              callback_data="m_xtream"),
         InlineKeyboardButton("🧹 URL Cleaner", callback_data="m_clean")],
        [InlineKeyboardButton("📋 Proxy List", callback_data="m_proxylist"),
         InlineKeyboardButton("🔍 Proxy Check", callback_data="m_proxycheck")],
        [InlineKeyboardButton("📖 Help", callback_data="m_help"),
         InlineKeyboardButton("📊 Status", callback_data="m_status")],
    ])


def filter_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("0", callback_data="f_0"),
         InlineKeyboardButton("20", callback_data="f_20"),
         InlineKeyboardButton("30", callback_data="f_30"),
         InlineKeyboardButton("40", callback_data="f_40")],
        [InlineKeyboardButton("50", callback_data="f_50"),
         InlineKeyboardButton("60", callback_data="f_60"),
         InlineKeyboardButton("70", callback_data="f_70"),
         InlineKeyboardButton("80", callback_data="f_80")],
        [InlineKeyboardButton("🔙 Back", callback_data="m_back")],
    ])


# ══════════════════════════════════════════════════════════════════════════════
# ─── COMMAND HANDLERS ────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

async def cmd_start(update, context):
    chat_id = update.effective_chat.id
    sess = get_session(chat_id)
    alive = sum(1 for p in _proxy_pool if p["alive"])
    if PROXY_ENABLED and alive:
        proxy_status = f"🔄 {alive}/{len(_proxy_pool)} alive proxies"
    elif PROXY_ENABLED and _proxy_pool:
        proxy_status = f"⚠️ {len(_proxy_pool)} (0 alive — /proxycheck)"
    elif not PROXY_ENABLED and _proxy_pool:
        proxy_status = f"⏸ {len(_proxy_pool)} DISABLED"
    else:
        proxy_status = "🔓 No proxies"

    await update.message.reply_text(
        "🕷 DORK PARSER v20.0 — XTREAM EDITION\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🆕 NEW in v20.0:\n"
        "  ⚡ /xtream — 1000 URLs/sec Yahoo bruteforce\n"
        f"  🔒 {len(TLS_PROFILES)} TLS fingerprints rotating\n"
        "  🚀 200 URLs/sec standard mode\n"
        "  🔘 Fully working inline keyboards\n\n"
        f"{proxy_status}\n\n"
        "📌 Core Commands:\n"
        "  /dork <q>     — single dork search\n"
        "  /xtream on|off — toggle XTREAM mode\n"
        "  /dorkcheck <q>— validate dork\n"
        "  /mutate <q>   — generate variations\n"
        "  /clean        — URL cleaner\n"
        "  /pages        — page selector\n"
        "  /workers N    — workers/chunk (1-60)\n"
        "  /chunks N     — parallel chunks (1-8)\n"
        "  /engine X     — bing|yahoo|ddg|all\n"
        "  /tor          — toggle Tor\n"
        "  /filter N     — SQL score 0-100\n"
        "  /stop         — stop & get partial\n\n"
        "🔄 Proxy: /addproxy /addproxies /proxylist\n"
        "         /proxycheck /proxyclean /testproxy\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        reply_markup=main_menu_keyboard(sess),
    )


async def cmd_dork(update, context):
    chat_id = update.effective_chat.id
    if not context.args:
        await update.message.reply_text("Usage: /dork inurl:login.php?id=")
        return
    if chat_id in active_jobs and not active_jobs[chat_id].done():
        await update.message.reply_text("⚠️ Job running! /stop first."); return
    dork = " ".join(context.args)
    ok, msg = validate_dork(dork)
    if not ok:
        await update.message.reply_text(f"❌ Invalid: {msg}"); return
    s = get_session(chat_id)
    mode_tag = " ⚡XTREAM" if s.get("xtream") else ""
    await update.message.reply_text(
        f"🔍 {dork[:60]}{mode_tag}\n"
        f"📄 Pages: {', '.join(str(p) for p in s.get('pages',[1]))}"
        f"{' 🧅TOR' if s.get('tor') else ''}\n💡 {msg}"
    )
    active_jobs[chat_id] = asyncio.create_task(run_dork_job(chat_id, [dork], context))


async def cmd_xtream(update, context):
    chat_id = update.effective_chat.id
    sess = get_session(chat_id)

    if context.args:
        arg0 = context.args[0].lower()
        if arg0 == "engine" and len(context.args) >= 2:
            engine = context.args[1].lower()
            if engine not in ("yahoo", "bing", "both"):
                await update.message.reply_text(
                    "⚠️ Invalid engine. Use: /xtream engine yahoo|bing|both"
                ); return
            sess["xtream_engine"] = engine
            labels = {"yahoo": "YAHOO (15 mirrors)", "bing": "BING (3 mirrors)",
                      "both": "YAHOO + BING (dual engine)"}
            await update.message.reply_text(
                f"🎯 XTREAM engine set to: {labels[engine]}\n"
                f"💡 Enable with /xtream on"
            ); return
        elif arg0 in ("on", "true", "1", "enable"):
            sess["xtream"] = True
        elif arg0 in ("off", "false", "0", "disable"):
            sess["xtream"] = False
        else:
            sess["xtream"] = not sess.get("xtream", False)
    else:
        sess["xtream"] = not sess.get("xtream", False)

    engine     = sess.get("xtream_engine", "yahoo")
    eng_labels = {"yahoo": "YAHOO (15 mirrors)", "bing": "BING (3 mirrors)",
                  "both": "YAHOO + BING"}

    if sess["xtream"]:
        await update.message.reply_text(
            f"⚡⚡⚡ XTREAM MODE ENABLED ⚡⚡⚡\n{'━'*30}\n"
            f"🎯 Engine     : {eng_labels.get(engine, engine.upper())}\n"
            f"🚀 Target RPS : {XTREAM_TARGET_RPS}/sec\n"
            f"⚙️ Workers    : {XTREAM_WORKERS_PER_CHUNK*XTREAM_CHUNKS} total\n"
            f"📄 Pages/dork : {XTREAM_PAGES_PER_DORK}\n"
            f"🔄 Sessions   : {XTREAM_SESSION_POOL_SIZE} pre-warmed pool\n"
            f"🛡 TLS profiles: {len(TLS_PROFILES)} rotating per-request\n"
            f"💀 Anti-block : per-worker adaptive cooldown (no race)\n"
            f"🍪 Cookie seed: {'enabled' if XTREAM_PRESEED_COOKIES else 'disabled'}\n"
            f"{'━'*30}\n"
            f"🔧 Change engine: /xtream engine yahoo|bing|both\n"
            f"⚠️ Tip: load proxies first for best results (/proxylist)\n"
            f"💡 Use /dork <q> or upload a .txt to run in XTREAM mode\n"
            f"💡 /xtream off to disable"
        )
    else:
        await update.message.reply_text(
            f"⏸ XTREAM MODE DISABLED\n"
            f"Reverted to standard mode (~200 RPS, multi-engine)."
        )


async def cmd_dorkcheck(update, context):
    if not context.args:
        await update.message.reply_text(
            "🧠 DORK CHECKER\nUsage: /dorkcheck <dork>\n\n"
            "Example: /dorkcheck inurl:login.php?id= filetype:php"
        ); return
    dork = " ".join(context.args)
    ok, msg = validate_dork(dork)
    ast = parse_dork(dork)
    normd = normalize_dork(dork)
    lines = [f"🧠 DORK ANALYSIS", "━"*22,
             f"📝 Raw   : {dork}", f"✨ Norm  : {normd}",
             f"✅ Status: {'OK' if ok else 'FAIL'} — {msg}",
             f"🔢 Tokens: {len(ast.tokens)}", f"🎯 Operators:"]
    if ast.operators:
        for op, vals in ast.operators.items():
            lines.append(f"   • {op}: {', '.join(vals)}")
    else: lines.append("   (none)")
    if ast.free_terms:
        lines.append(f"🔤 Free terms: {', '.join(ast.free_terms)}")
    lines += ["", "🔁 Engine translations:"]
    for engine in ENGINES:
        translated = translate_dork(dork, engine)
        lines.append(f"   {engine.upper():12s}: {translated[:80]}")
    await update.message.reply_text("\n".join(lines))


async def cmd_mutate(update, context):
    if not context.args:
        await update.message.reply_text("Usage: /mutate <dork> [n=10]"); return
    args = list(context.args); n = 10
    if args[-1].isdigit():
        n = max(1, min(int(args[-1]), 50))
        args = args[:-1]
    dork = " ".join(args)
    variations = mutate_dork(dork, n=n)
    lines = [f"🧬 DORK MUTATIONS ({len(variations)})", "━"*22]
    for i, v in enumerate(variations, 1): lines.append(f"{i:>2}. {v}")
    await update.message.reply_text("\n".join(lines))


async def cmd_pages(update, context):
    chat_id = update.effective_chat.id
    sess = get_session(chat_id)
    if context.args:
        try:
            n = int(context.args[0])
            if not 1 <= n <= 70:
                raise ValueError
            pages = list(range(1, n + 1))
            sess["pages"] = pages
            await update.message.reply_text(
                f"📄 Pages set: 1–{n} ({n} pages per dork)\n"
                f"Selected: {', '.join(str(p) for p in pages)}"
            )
            return
        except ValueError:
            await update.message.reply_text(
                "⚠️ Invalid value. Usage: /pages <number 1–70>\n"
                "Example: /pages 10  →  crawls pages 1 to 10 per dork"
            )
            return
    selected = sess.get("pages", [1])
    await update.message.reply_text(
        f"📄 SELECT PAGES (1–70)\n"
        f"Tip: /pages <N> sets pages 1–N directly (e.g. /pages 15)\n\n"
        f"Selected: {', '.join(str(p) for p in selected)}",
        reply_markup=page_keyboard(selected),
    )


async def cmd_tor(update, context):
    global tor_enabled_users
    chat_id = update.effective_chat.id
    sess = get_session(chat_id)
    new_val = (context.args[0].lower() == "on") if context.args and context.args[0].lower() in ("on","off") else not sess.get("tor", False)
    old_val = sess.get("tor", False)
    sess["tor"] = new_val
    if new_val and not old_val:
        tor_enabled_users += 1
        if tor_enabled_users == 1: start_tor_rotation()
        await update.message.reply_text("🧅 TOR ENABLED — rotates every 2 min.")
    elif not new_val and old_val:
        tor_enabled_users = max(0, tor_enabled_users - 1)
        if tor_enabled_users == 0: stop_tor_rotation()
        await update.message.reply_text("🔓 TOR DISABLED.")
    else:
        await update.message.reply_text(f"Tor is already {'ON' if new_val else 'OFF'}.")


async def cmd_filter(update, context):
    chat_id = update.effective_chat.id
    sess = get_session(chat_id)
    try:
        n = max(0, min(int(context.args[0]), 100))
        sess["min_score"] = n
        await update.message.reply_text(f"🛡 SQL Filter: ≥{n}")
    except Exception:
        await update.message.reply_text(
            f"Current: ≥{sess.get('min_score', 30)}\nPick:",
            reply_markup=filter_keyboard(),
        )


async def cmd_settings(update, context):
    chat_id = update.effective_chat.id
    s = get_session(chat_id)
    alive = sum(1 for p in _proxy_pool if p["alive"])
    if PROXY_ENABLED and _proxy_pool:
        proxy_line = f"🔄 Proxies  : {alive}/{len(_proxy_pool)} alive\n"
    elif not PROXY_ENABLED and _proxy_pool:
        proxy_line = f"⏸ Proxies  : {len(_proxy_pool)} DISABLED\n"
    else: proxy_line = "🔓 Proxies  : none\n"
    await update.message.reply_text(
        f"⚙️ SETTINGS\n━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚡ Chunks   : {s.get('chunks', N_CHUNKS)}\n"
        f"🔧 Workers  : {s.get('workers', WORKERS_PER_CHUNK)}/chunk\n"
        f"📄 Pages    : {', '.join(str(p) for p in s.get('pages', [1]))}\n"
        f"🔍 Engines  : {'+'.join(e.upper() for e in s.get('engines', ENGINES))}\n"
        f"📊 Max/Page : {s.get('max_results', MAX_RESULTS)}\n"
        f"🛡 SQL ≥    : {s.get('min_score', 30)}\n"
        f"🧅 Tor      : {'ON' if s.get('tor') else 'OFF'}\n"
        f"⚡ Xtream   : {'ON 🚀' if s.get('xtream') else 'OFF'}\n"
        f"{proxy_line}🔒 TLS pool : {len(TLS_PROFILES)} profiles\n"
        f"━━━━━━━━━━━━━━━━━━━━━━",
        reply_markup=main_menu_keyboard(s),
    )


async def cmd_workers(update, context):
    chat_id = update.effective_chat.id
    try:
        n = max(1, min(int(context.args[0]), MAX_WORKERS_PER_CHUNK))
        get_session(chat_id)["workers"] = n
        await update.message.reply_text(f"✅ Workers/chunk: {n}")
    except Exception:
        await update.message.reply_text(f"Usage: /workers N (1-{MAX_WORKERS_PER_CHUNK})")


async def cmd_chunks(update, context):
    chat_id = update.effective_chat.id
    try:
        n = max(1, min(int(context.args[0]), 8))
        get_session(chat_id)["chunks"] = n
        await update.message.reply_text(f"✅ Chunks: {n}")
    except Exception:
        await update.message.reply_text("Usage: /chunks N (1-8)")


async def cmd_maxres(update, context):
    chat_id = update.effective_chat.id
    try:
        n = max(1, min(int(context.args[0]), 50))
        get_session(chat_id)["max_results"] = n
        await update.message.reply_text(f"✅ Max/page: {n}")
    except Exception:
        await update.message.reply_text("Usage: /maxres N (1-50)")


async def cmd_engine(update, context):
    chat_id = update.effective_chat.id
    try:
        choice = context.args[0].lower()
        m = {"bing":["bing"], "yahoo":["yahoo"], "duckduckgo":["duckduckgo"],
             "ddg":["duckduckgo"], "all":list(ENGINES), "both":["bing","yahoo"]}
        engines = m.get(choice, list(ENGINES))
        get_session(chat_id)["engines"] = engines
        await update.message.reply_text(f"✅ Engines: {'+'.join(e.upper() for e in engines)}")
    except Exception:
        await update.message.reply_text("Usage: /engine bing|yahoo|duckduckgo|all")


async def cmd_clean(update, context):
    await update.message.reply_text("🧹 Upload a .txt with URLs (one per line).")


async def cmd_stop(update, context):
    chat_id = update.effective_chat.id
    stop_ev = active_stop_evs.get(chat_id)
    job = active_jobs.get(chat_id)
    if stop_ev and job and not job.done():
        stop_ev.set()
        await update.message.reply_text("⏹ STOP REQUESTED — partial results coming.")
    elif job and not job.done():
        job.cancel(); active_jobs.pop(chat_id, None)
        await update.message.reply_text("🛑 Force-stopped.")
    else:
        await update.message.reply_text("💤 No active job.")


async def cmd_status(update, context):
    chat_id = update.effective_chat.id
    job = active_jobs.get(chat_id)
    sess = get_session(chat_id)
    mode = "⚡ XTREAM" if sess.get("xtream") else "🕷 Standard"
    await update.message.reply_text(
        f"{'⚡ Running' if job and not job.done() else '💤 Idle'}\nMode: {mode}"
    )


# ─── PROXY COMMAND HANDLERS ──────────────────────────────────────────────────
_awaiting_bulk_proxy: set = set()


async def cmd_addproxy(update, context):
    if not context.args:
        await update.message.reply_text(
            "➕ ADD PROXY\nUsage: /addproxy <proxy>\n\n"
            "Formats (auto-detected):\n"
            "  ip:port  /  ip:port:user:pass\n"
            "  socks5://user:pass@host:port  /  http://host:port"
        ); return
    line = " ".join(context.args).strip()
    p = parse_proxy_line(line)
    if not p:
        await update.message.reply_text("❌ Invalid format."); return
    key = proxy_key(p)
    async with _proxy_pool_lock:
        if any(proxy_key(x) == key for x in _proxy_pool):
            await update.message.reply_text("⚠️ Already in pool."); return
    wait_msg = await update.message.reply_text(
        f"🔍 Auto-detecting {p['host']}:{p['port']}...")
    ok = await detect_proxy_protocol(p)
    if not ok:
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id, message_id=wait_msg.message_id,
            text=f"❌ FAILED\n{p['host']}:{p['port']}\nNot added."); return
    async with _proxy_pool_lock:
        _proxy_pool.append(p); _persist_proxies()
    await context.bot.edit_message_text(
        chat_id=update.effective_chat.id, message_id=wait_msg.message_id,
        text=(f"✅ ADDED\n🔌 {p['protocol'].upper()}\n"
              f"🌐 {p['host']}:{p['port']}\n"
              f"⏱ {int(p['latency'])} ms\n📦 Pool: {len(_proxy_pool)}"))


async def cmd_addproxies(update, context):
    chat_id = update.effective_chat.id
    _awaiting_bulk_proxy.add(chat_id)
    await update.message.reply_text(
        "📥 BULK PROXY IMPORT\nSend list as NEXT message (one per line).\n"
        "Or upload a .txt file. Auto-detects SOCKS5/4/HTTP/HTTPS.")


async def _bulk_add_proxies(chat_id, lines, context):
    parsed = []; invalid = 0
    for line in lines:
        if not line.strip() or line.startswith("#"): continue
        line = line.split("#", 1)[0].strip()
        if not line: continue
        p = parse_proxy_line(line)
        if p: parsed.append(p)
        else: invalid += 1
    seen_keys = {proxy_key(p) for p in _proxy_pool}
    unique = []; dup_count = 0
    for p in parsed:
        k = proxy_key(p)
        if k in seen_keys: dup_count += 1; continue
        seen_keys.add(k); unique.append(p)
    if not unique:
        await context.bot.send_message(chat_id, f"⚠️ Nothing to add.\n❌ Invalid: {invalid}\n🔁 Dup: {dup_count}")
        return
    status_msg = await context.bot.send_message(
        chat_id,
        f"🔍 BULK CHECK\n📥 {len(lines)} | ✅ {len(parsed)} | ❌ {invalid} | 🔁 {dup_count}\n"
        f"🆕 To check: {len(unique)}\n⏳ Auto-detecting...")
    last_edit = [0.0]
    async def _progress(done, total, alive):
        if time.monotonic() - last_edit[0] < 2.5: return
        pct = int(done / total * 100) if total else 100
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id, message_id=status_msg.message_id,
                text=f"🔍 {pct}%\n✅ {done}/{total}\n💚 Alive: {alive}")
            last_edit[0] = time.monotonic()
        except Exception: pass
    alive, dead = await check_proxies_bulk(unique, progress_cb=_progress)
    added = []
    async with _proxy_pool_lock:
        for p in unique:
            if p["alive"]: _proxy_pool.append(p); added.append(p)
        _persist_proxies()
    breakdown = {}
    for p in added: breakdown[p["protocol"]] = breakdown.get(p["protocol"], 0) + 1
    bd = "\n".join(f"   • {k.upper()}: {v}" for k, v in breakdown.items()) or "   (none)"
    try:
        await context.bot.edit_message_text(
            chat_id=chat_id, message_id=status_msg.message_id,
            text=(f"✅ COMPLETE\n📥 {len(lines)} | ❌ {invalid} | 🔁 {dup_count}\n"
                  f"💀 Dead: {dead}\n💚 Added: {len(added)}\n"
                  f"🔌 Breakdown:\n{bd}\n📦 Pool: {len(_proxy_pool)}"))
    except Exception: pass


async def cmd_proxycheck(update, context):
    if not _proxy_pool:
        await update.message.reply_text("📭 Empty."); return
    status_msg = await update.message.reply_text(f"🔍 Re-checking {len(_proxy_pool)}...")
    last_edit = [0.0]
    async def _progress(done, total, alive):
        if time.monotonic() - last_edit[0] < 2.5: return
        pct = int(done / total * 100) if total else 100
        try:
            await context.bot.edit_message_text(
                chat_id=update.effective_chat.id, message_id=status_msg.message_id,
                text=f"🔍 {pct}%\n✅ {done}/{total}\n💚 Alive: {alive}")
            last_edit[0] = time.monotonic()
        except Exception: pass
    alive, dead = await check_proxies_bulk(list(_proxy_pool), progress_cb=_progress)
    _persist_proxies()
    try:
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id, message_id=status_msg.message_id,
            text=f"✅ DONE\n📦 {len(_proxy_pool)} | 💚 {alive} | 💀 {dead}")
    except Exception: pass


async def cmd_proxyclean(update, context):
    async with _proxy_pool_lock:
        before = len(_proxy_pool)
        _proxy_pool[:] = [p for p in _proxy_pool if p["alive"]]
        removed = before - len(_proxy_pool)
        _persist_proxies()
    await update.message.reply_text(f"🧹 Removed {removed}\n💚 Remaining: {len(_proxy_pool)}")


async def cmd_removeproxy(update, context):
    if not context.args:
        if not _proxy_pool:
            await update.message.reply_text("📭 Empty."); return
        lines = ["📋 POOL"]
        for i, p in enumerate(_proxy_pool, start=1):
            mark = "💚" if p["alive"] else "💀"
            lines.append(f"{i:>2}. {mark} {proxy_display(p)}")
        lines.append("\n/removeproxy <index>")
        await update.message.reply_text("\n".join(lines)); return
    arg = context.args[0].strip()
    async with _proxy_pool_lock:
        try:
            idx = int(arg) - 1
            if not (0 <= idx < len(_proxy_pool)):
                await update.message.reply_text(f"❌ Range 1-{len(_proxy_pool)}"); return
            removed = _proxy_pool.pop(idx); _persist_proxies()
            await update.message.reply_text(f"🗑 {proxy_display(removed)}"); return
        except ValueError: pass
        for i, p in enumerate(_proxy_pool):
            if f"{p['host']}:{p['port']}" == arg or p.get("url") == arg:
                _proxy_pool.pop(i); _persist_proxies()
                await update.message.reply_text(f"🗑 {arg}"); return
    await update.message.reply_text("❌ Not found.")


async def cmd_proxylist(update, context):
    if not _proxy_pool:
        await update.message.reply_text("📭 Empty.\nUse /addproxy or /addproxies."); return
    alive = sum(1 for p in _proxy_pool if p["alive"])
    breakdown = {}
    for p in _proxy_pool:
        k = (p["protocol"] or "?").upper()
        breakdown[k] = breakdown.get(k, 0) + 1
    lines = [f"🔄 POOL — {len(_proxy_pool)} ({alive} alive)",
             "📊 " + ", ".join(f"{k}:{v}" for k, v in breakdown.items()),
             "━"*22]
    for i, p in enumerate(_proxy_pool[:50], start=1):
        mark = "💚" if p["alive"] else "💀"
        lat = f"{int(p['latency'])}ms" if p.get("latency") else "—"
        lines.append(f"{i:>2}. {mark} {proxy_display(p)}  {lat}")
    if len(_proxy_pool) > 50: lines.append(f"… +{len(_proxy_pool)-50}")
    await update.message.reply_text("\n".join(lines))


async def cmd_testproxy(update, context):
    if not context.args:
        await update.message.reply_text("Usage: /testproxy <line>"); return
    line = " ".join(context.args).strip()
    p = parse_proxy_line(line)
    if not p:
        await update.message.reply_text("❌ Invalid."); return
    wait = await update.message.reply_text(f"🧪 Testing {p['host']}:{p['port']}...")
    ok = await detect_proxy_protocol(p)
    if ok:
        msg = (f"✅ WORKS\n🔌 {p['protocol'].upper()}\n"
               f"🌐 {p['host']}:{p['port']}\n⏱ {int(p['latency'])} ms")
    else:
        msg = f"❌ FAILED\n{p['host']}:{p['port']}"
    try:
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id, message_id=wait.message_id, text=msg)
    except Exception:
        await update.message.reply_text(msg)


# ─── FILE DETECTION ──────────────────────────────────────────────────────────
def _looks_like_url_list(lines):
    non_empty = [l for l in lines if l.strip() and not l.startswith("#")]
    if not non_empty: return False
    return sum(1 for l in non_empty if l.strip().startswith("http")) / len(non_empty) >= 0.5


def _looks_like_proxy_list(lines):
    non_empty = [l for l in lines if l.strip() and not l.startswith("#")]
    if not non_empty: return False
    proxy_count = sum(1 for l in non_empty if parse_proxy_line(l.split("#", 1)[0].strip()))
    return proxy_count / len(non_empty) >= 0.6


# ─── DOCUMENT / TEXT HANDLERS ────────────────────────────────────────────────
async def handle_document(update, context):
    chat_id = update.effective_chat.id
    doc = update.message.document
    if chat_id in active_jobs and not active_jobs[chat_id].done():
        await update.message.reply_text("⚠️ Job running! /stop first."); return
    if not doc.file_name.endswith(".txt"):
        await update.message.reply_text("❌ Send a .txt file."); return
    await update.message.reply_text("📥 Reading...")
    try:
        content = await (await context.bot.get_file(doc.file_id)).download_as_bytearray()
        lines = content.decode("utf-8", errors="replace").splitlines()
        if _looks_like_proxy_list(lines):
            await update.message.reply_text(f"🔄 PROXY LIST — {len(lines)} lines\n🚀 Checking...")
            await _bulk_add_proxies(chat_id, lines, context); return
        if _looks_like_url_list(lines):
            raw_urls = [l.strip() for l in lines if l.strip() and not l.startswith("#")]
            if not raw_urls:
                await update.message.reply_text("❌ No URLs."); return
            await update.message.reply_text(f"🧹 URL LIST — {len(raw_urls)}")
            active_jobs[chat_id] = asyncio.create_task(run_url_clean_job(chat_id, raw_urls, context))
        else:
            dorks = [l.strip() for l in lines if l.strip() and not l.startswith("#")]
            if not dorks:
                await update.message.reply_text("❌ No dorks."); return
            s = get_session(chat_id)
            mode_tag = " ⚡XTREAM" if s.get("xtream") else ""
            await update.message.reply_text(
                f"✅ {len(dorks)} dorks{mode_tag} | Pages: {', '.join(str(p) for p in s.get('pages',[1]))}\n🚀 Starting...")
            active_jobs[chat_id] = asyncio.create_task(run_dork_job(chat_id, dorks, context))
    except Exception as exc:
        await update.message.reply_text(f"❌ Error: {exc}")


async def handle_text(update, context):
    chat_id = update.effective_chat.id
    if chat_id in _awaiting_bulk_proxy:
        _awaiting_bulk_proxy.discard(chat_id)
        lines = update.message.text.splitlines()
        if not lines:
            await update.message.reply_text("❌ No lines."); return
        await _bulk_add_proxies(chat_id, lines, context); return
    lines = [l.strip() for l in update.message.text.splitlines()
             if l.strip() and not l.startswith("#")]
    if len(lines) > 1:
        if chat_id in active_jobs and not active_jobs[chat_id].done():
            await update.message.reply_text("⚠️ Job running! /stop first."); return
        s = get_session(chat_id)
        mode_tag = " ⚡XTREAM" if s.get("xtream") else ""
        await update.message.reply_text(
            f"✅ {len(lines)} dorks{mode_tag}\n🚀 Starting...")
        active_jobs[chat_id] = asyncio.create_task(run_dork_job(chat_id, lines, context))
    else:
        await update.message.reply_text(
            "Use /dork <q> or upload .txt\n"
            "/xtream — 1000 RPS mode\n"
            "/dorkcheck — validate  |  /mutate — variations")


# ══════════════════════════════════════════════════════════════════════════════
# ─── FIXED CALLBACK HANDLER — covers ALL inline buttons ──────────────────────
# ══════════════════════════════════════════════════════════════════════════════

async def handle_callback(update, context):
    query = update.callback_query
    await query.answer()
    data = query.data
    chat_id = query.message.chat_id
    sess = get_session(chat_id)

    if data.startswith("pg_"):
        cmd = data[3:]
        selected = list(sess.get("pages", [1]))
        if cmd == "all":     selected = list(range(1, 71))
        elif cmd == "clear": selected = []
        elif cmd == "confirm":
            sess["pages"] = selected or [1]
            try:
                await query.edit_message_text(
                    f"✅ Pages saved: {', '.join(str(p) for p in sorted(sess['pages']))}"
                )
            except Exception: pass
            return
        else:
            try:
                p = int(cmd)
                if p in selected: selected.remove(p)
                else: selected.append(p)
                selected = sorted(set(selected))
            except ValueError: pass
        sess["pages"] = selected
        try:
            await query.edit_message_text(
                f"📄 SELECT PAGES\nSelected: {', '.join(str(p) for p in selected) or 'none'}",
                reply_markup=page_keyboard(selected),
            )
        except Exception: pass
        return

    if data.startswith("f_"):
        try:
            n = int(data[2:])
            sess["min_score"] = n
            await query.edit_message_text(
                f"🛡 SQL Filter set: ≥{n}",
                reply_markup=main_menu_keyboard(sess),
            )
        except (ValueError, Exception): pass
        return

    if data == "m_bulk":
        try:
            await query.edit_message_text(
                "📂 BULK UPLOAD\nSend a .txt file. Auto-detected:\n"
                "  • Dork list → run search\n"
                "  • URL list  → run cleaner\n"
                "  • Proxy list → import to pool\n\n"
                "Or paste multiple lines directly in chat.",
                reply_markup=main_menu_keyboard(sess),
            )
        except Exception: pass
        return

    if data == "m_single":
        try:
            await query.edit_message_text(
                "🔍 SINGLE DORK SEARCH\nUsage: /dork <query>\n\n"
                "Examples:\n"
                "  /dork inurl:login.php?id=\n"
                "  /dork intitle:\"index of\" filetype:php\n"
                "  /dork site:example.com -site:blog.example.com\n\n"
                "💡 /dorkcheck <q> — validate\n💡 /mutate <q> — variations",
                reply_markup=main_menu_keyboard(sess),
            )
        except Exception: pass
        return

    if data == "m_pages":
        try:
            await query.edit_message_text(
                f"📄 SELECT PAGES (1–70)\nCurrently: {', '.join(str(p) for p in sess.get('pages', [1]))}",
                reply_markup=page_keyboard(sess.get("pages", [1])),
            )
        except Exception: pass
        return

    if data == "m_settings":
        alive = sum(1 for p in _proxy_pool if p["alive"])
        try:
            await query.edit_message_text(
                f"⚙️ SETTINGS\n━━━━━━━━━━━━━━━━━━━━━━\n"
                f"⚡ Chunks   : {sess.get('chunks', N_CHUNKS)}\n"
                f"🔧 Workers  : {sess.get('workers', WORKERS_PER_CHUNK)}/chunk\n"
                f"📄 Pages    : {', '.join(str(p) for p in sess.get('pages',[1]))}\n"
                f"🔍 Engines  : {'+'.join(e.upper() for e in sess.get('engines',ENGINES))}\n"
                f"📊 Max/page : {sess.get('max_results', MAX_RESULTS)}\n"
                f"🛡 SQL ≥    : {sess.get('min_score', 30)}\n"
                f"🧅 Tor      : {'ON' if sess.get('tor') else 'OFF'}\n"
                f"⚡ Xtream   : {'ON 🚀' if sess.get('xtream') else 'OFF'}\n"
                f"🔄 Proxies  : {alive}/{len(_proxy_pool)} alive\n"
                f"🔒 TLS pool : {len(TLS_PROFILES)} profiles\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"Change with: /workers /chunks /engine /maxres /filter /tor /xtream",
                reply_markup=main_menu_keyboard(sess),
            )
        except Exception: pass
        return

    if data == "m_tor":
        global tor_enabled_users
        old_val = sess.get("tor", False)
        sess["tor"] = not old_val
        if sess["tor"] and not old_val:
            tor_enabled_users += 1
            if tor_enabled_users == 1: start_tor_rotation()
        elif not sess["tor"] and old_val:
            tor_enabled_users = max(0, tor_enabled_users - 1)
            if tor_enabled_users == 0: stop_tor_rotation()
        try:
            await query.edit_message_text(
                f"🧅 TOR {'ENABLED — rotates every 2 min' if sess['tor'] else 'DISABLED'}",
                reply_markup=main_menu_keyboard(sess),
            )
        except Exception: pass
        return

    if data == "m_xtream":
        sess["xtream"] = not sess.get("xtream", False)
        if sess["xtream"]:
            msg = (
                f"⚡⚡⚡ XTREAM MODE ENABLED ⚡⚡⚡\n"
                f"🎯 Yahoo bruteforce @ 1000 RPS\n"
                f"⚙️ {XTREAM_WORKERS_PER_CHUNK*XTREAM_CHUNKS} workers, {XTREAM_SESSION_POOL_SIZE} sessions\n"
                f"🛡 {len(TLS_PROFILES)} TLS profiles rotating\n"
                f"💡 Now /dork or upload .txt to run xtream"
            )
        else:
            msg = "⏸ XTREAM disabled. Reverted to standard mode."
        try:
            await query.edit_message_text(msg, reply_markup=main_menu_keyboard(sess))
        except Exception: pass
        return

    if data == "m_filter":
        try:
            await query.edit_message_text(
                f"🛡 SQL FILTER\nCurrent: ≥{sess.get('min_score', 30)}\n"
                f"Higher = stricter / fewer URLs but better quality.\n\n"
                f"Pick a threshold:",
                reply_markup=filter_keyboard(),
            )
        except Exception: pass
        return

    if data == "m_clean":
        try:
            await query.edit_message_text(
                "🧹 URL CLEANER\nUpload a .txt with URLs (auto-detected).\n"
                "Removes: blocked domains, no-query, >200 chars, dupes, invalid.\n\n"
                "Or paste a URL list directly in chat.",
                reply_markup=main_menu_keyboard(sess),
            )
        except Exception: pass
        return

    if data == "m_proxylist":
        if not _proxy_pool:
            try:
                await query.edit_message_text(
                    "📭 No proxies loaded.\n"
                    "Use /addproxy or /addproxies to add some.",
                    reply_markup=main_menu_keyboard(sess),
                )
            except Exception: pass
            return
        alive = sum(1 for p in _proxy_pool if p["alive"])
        breakdown = {}
        for p in _proxy_pool:
            k = (p["protocol"] or "?").upper()
            breakdown[k] = breakdown.get(k, 0) + 1
        lines = [f"🔄 POOL — {len(_proxy_pool)} ({alive} alive)",
                 "📊 " + ", ".join(f"{k}:{v}" for k, v in breakdown.items()),
                 "━"*22]
        for i, p in enumerate(_proxy_pool[:20], start=1):
            mark = "💚" if p["alive"] else "💀"
            lat = f"{int(p['latency'])}ms" if p.get("latency") else "—"
            lines.append(f"{i:>2}. {mark} {proxy_display(p)}  {lat}")
        if len(_proxy_pool) > 20:
            lines.append(f"… +{len(_proxy_pool)-20} more (use /proxylist for full)")
        try:
            await query.edit_message_text("\n".join(lines),
                reply_markup=main_menu_keyboard(sess))
        except Exception: pass
        return

    if data == "m_proxycheck":
        if not _proxy_pool:
            try:
                await query.edit_message_text("📭 No proxies to check.",
                    reply_markup=main_menu_keyboard(sess))
            except Exception: pass
            return
        try:
            await query.edit_message_text(f"🔍 Re-checking {len(_proxy_pool)} proxies...")
        except Exception: pass
        last_edit = [0.0]
        async def _progress(done, total, alive):
            if time.monotonic() - last_edit[0] < 2.5: return
            try:
                await query.edit_message_text(
                    f"🔍 {int(done/total*100)}%\n✅ {done}/{total}\n💚 Alive: {alive}")
                last_edit[0] = time.monotonic()
            except Exception: pass
        alive, dead = await check_proxies_bulk(list(_proxy_pool), progress_cb=_progress)
        _persist_proxies()
        try:
            await query.edit_message_text(
                f"✅ Check done\n📦 {len(_proxy_pool)} | 💚 {alive} | 💀 {dead}",
                reply_markup=main_menu_keyboard(sess))
        except Exception: pass
        return

    if data == "m_status":
        job = active_jobs.get(chat_id)
        running = bool(job and not job.done())
        mode = "⚡ XTREAM (1000 RPS)" if sess.get("xtream") else "🕷 Standard (~200 RPS)"
        try:
            await query.edit_message_text(
                f"📊 STATUS\n━━━━━━━━━━━━━━━\n"
                f"State: {'⚡ Running' if running else '💤 Idle'}\n"
                f"Mode : {mode}\n"
                f"Tor  : {'ON' if sess.get('tor') else 'OFF'}\n"
                f"Filt : ≥{sess.get('min_score', 30)}",
                reply_markup=main_menu_keyboard(sess),
            )
        except Exception: pass
        return

    if data == "m_help":
        try:
            await query.edit_message_text(
                "📖 HELP\n━━━━━━━━━━━━━━━\n"
                "/dork <q>     — search\n"
                "/xtream       — toggle 1000 RPS mode\n"
                "/dorkcheck    — validate\n"
                "/mutate       — variations\n"
                "/pages        — page selector\n"
                "/workers N    — 1-60\n"
                "/chunks N     — 1-8\n"
                "/engine X     — bing|yahoo|ddg|all\n"
                "/tor          — toggle Tor\n"
                "/filter N     — SQL ≥ N\n"
                "/stop         — stop job\n\n"
                "Proxies:\n"
                "/addproxy /addproxies /proxylist\n"
                "/proxycheck /proxyclean /testproxy\n"
                "/removeproxy [i]",
                reply_markup=main_menu_keyboard(sess),
            )
        except Exception: pass
        return

    if data == "m_back":
        try:
            await query.edit_message_text(
                "🕷 DORK PARSER v20.0",
                reply_markup=main_menu_keyboard(sess),
            )
        except Exception: pass
        return

    log.warning(f"[CB] Unknown callback: {data}")
    try:
        await query.answer(f"Unknown action: {data}", show_alert=True)
    except Exception: pass


# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    if not BOT_TOKEN:
        log.critical("BOT_TOKEN not set!")
        raise SystemExit(1)

    app = Application.builder().token(BOT_TOKEN).build()

    for name, handler in [
        ("start", cmd_start), ("help", cmd_settings),
        ("dork", cmd_dork), ("xtream", cmd_xtream),
        ("dorkcheck", cmd_dorkcheck), ("mutate", cmd_mutate),
        ("clean", cmd_clean), ("pages", cmd_pages), ("tor", cmd_tor),
        ("filter", cmd_filter), ("settings", cmd_settings),
        ("workers", cmd_workers), ("chunks", cmd_chunks),
        ("maxres", cmd_maxres), ("engine", cmd_engine),
        ("stop", cmd_stop), ("status", cmd_status),
    ]:
        app.add_handler(CommandHandler(name, handler))

    for name, handler in [
        ("addproxy", cmd_addproxy), ("addproxies", cmd_addproxies),
        ("removeproxy", cmd_removeproxy), ("proxylist", cmd_proxylist),
        ("testproxy", cmd_testproxy), ("proxycheck", cmd_proxycheck),
        ("proxyclean", cmd_proxyclean),
    ]:
        app.add_handler(CommandHandler(name, handler))

    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(CallbackQueryHandler(handle_callback))

    async def _on_startup(_app):
        start_proxy_health_monitor()
        log.info("Background proxy health monitor started")
    app.post_init = _on_startup

    log.info("=" * 60)
    log.info("  DORK PARSER v21.0 — XTREAM STEALTH EDITION (FIXED)")
    log.info(f"  TLS profiles : {len(TLS_PROFILES)} rotating (Chrome/Firefox/Edge/Safari)")
    log.info(f"  Standard     : ~200 URLs/sec ({N_CHUNKS}×{WORKERS_PER_CHUNK})")
    log.info(f"  Xtream       : {XTREAM_TARGET_RPS} RPS target ({XTREAM_CHUNKS}×{XTREAM_WORKERS_PER_CHUNK}={XTREAM_CHUNKS*XTREAM_WORKERS_PER_CHUNK} workers)")
    log.info(f"  Xtream pages : {XTREAM_PAGES_PER_DORK}/dork sequential | pool: {XTREAM_SESSION_POOL_SIZE}")
    log.info(f"  Proxies      : {len(_proxy_pool)} loaded")
    log.info("  Bugs fixed   : captcha_counter thread safety, missing imports, callback reliability")
    log.info("=" * 60)
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
