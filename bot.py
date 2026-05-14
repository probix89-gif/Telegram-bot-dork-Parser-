"""
╔══════════════════════════════════════════════════════════════════╗
║   DORK PARSER BOT v19.0 — INTELLIGENT EDITION                    ║
║   • Full dork-syntax parser/validator/mutator                    ║
║   • Cross-engine dork translation (Bing/Yahoo/DDG)               ║
║   • Auto-detect proxy protocol (SOCKS5/SOCKS4/HTTPS/HTTP)        ║
║   • Accept ip:port:user:pass + ip:port + URL formats             ║
║   • Automatic proxy health checker + background re-validation    ║
║   • Bulk proxy import (paste or .txt upload)                     ║
║   • Auto-blacklist dead proxies                                  ║
║   • All v18.1 features preserved (TLS, parallel chunks, Tor…)    ║
╚══════════════════════════════════════════════════════════════════╝
"""

import asyncio
import random
import re
import os
import time
import logging
import tempfile
import json as _json
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlparse, parse_qs, unquote, quote_plus

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
N_CHUNKS              = int(os.environ.get("N_CHUNKS", 2))
WORKERS_PER_CHUNK     = int(os.environ.get("WORKERS_PER_CHUNK", 8))
MAX_WORKERS_PER_CHUNK = 20
MIN_DELAY             = float(os.environ.get("MIN_DELAY", 1.5))
MAX_DELAY             = float(os.environ.get("MAX_DELAY", 3.0))
FAST_MIN_DELAY        = 0.5
FAST_MAX_DELAY        = 1.0
FAST_STREAK_THRESHOLD = 3
MAX_RESULTS           = int(os.environ.get("MAX_RESULTS", 10))
TOR_PROXY             = os.environ.get("TOR_PROXY", "socks5://127.0.0.1:9050")
OUTPUT_DIR            = Path("results")
OUTPUT_DIR.mkdir(exist_ok=True)

ENGINES   = ["bing", "yahoo", "duckduckgo"]
MAX_PAGES = 70

WORKER_FETCH_TIMEOUT = 120
JOB_TIMEOUT          = 30 * 60
MAX_RETRIES          = 3
CHUNK_STALL_TIMEOUT  = 60.0
EMPTY_RATE_SLOWDOWN  = 0.50
EMPTY_RATE_RECOVER   = 0.30
CHUNK_STAGGER_DELAY  = (0.8, 2.5)

# TLS FINGERPRINT ROTATION
TLS_IMPERSONATIONS = [
    "chrome107", "chrome110", "chrome116", "chrome120", "chrome124",
    "firefox117", "safari15_5", "safari15_6", "edge99", "edge110",
]

DEFAULT_SESSION = {
    "workers":     WORKERS_PER_CHUNK,
    "chunks":      N_CHUNKS,
    "engines":     list(ENGINES),
    "max_results": MAX_RESULTS,
    "pages":       [1],
    "tor":         False,
    "min_score":   30,
    "speed_mode":  False,    # high-speed 200 URLs/s
    "xtream":      False,    # xtream mode 1000 URLs/s (Yahoo only)
}

user_sessions:   dict = {}
active_jobs:     dict = {}
active_stop_evs: dict = {}


# ══════════════════════════════════════════════════════════════════════════════
# ─── PROXY SYSTEM v19.0 — AUTO-DETECT, AUTO-CHECK, AUTO-MANAGE ───────────────
# ══════════════════════════════════════════════════════════════════════════════
#
# Accepted input formats (all auto-detected):
#   1. ip:port                              → protocol auto-detected
#   2. ip:port:user:pass                    → protocol auto-detected, with auth
#   3. http://[user:pass@]host:port         → URL form
#   4. https://[user:pass@]host:port        → URL form
#   5. socks4://[user:pass@]host:port       → URL form
#   6. socks5://[user:pass@]host:port       → URL form
#
# Each proxy is stored as a dict:
#   {
#     "host":     str,
#     "port":     int,
#     "user":     str | None,
#     "pass":     str | None,
#     "protocol": str | None,   # detected: "socks5"/"socks4"/"https"/"http"
#     "url":      str,          # canonical URL (set after detection)
#     "alive":    bool,
#     "latency":  float | None, # in ms
#     "last_check": float,      # unix ts
#     "fail_count": int,        # consecutive failures
#   }
# ══════════════════════════════════════════════════════════════════════════════

PROXY_ENABLED: bool = os.environ.get("PROXY_ENABLED", "true").lower() not in ("false", "0", "no")

# Probe order — try faster/more common protocols first
PROXY_PROBE_ORDER = ("socks5", "socks4", "http", "https")

# Endpoints used to verify a proxy works + report its external IP
PROXY_TEST_URLS = [
    "https://api.ipify.org?format=json",
    "https://httpbin.org/ip",
    "https://ifconfig.me/ip",
    "http://ip-api.com/json/",   # http fallback (some proxies don't do TLS)
]

PROXY_CHECK_TIMEOUT     = 10        # seconds per probe attempt
PROXY_CHECK_CONCURRENCY = 30        # max parallel checks during bulk validation
PROXY_HEALTH_INTERVAL   = 600       # background re-check every 10 min
PROXY_MAX_FAILS         = 3         # consecutive failures → mark dead/remove

_proxy_pool_lock: asyncio.Lock = asyncio.Lock()
_proxy_pool: list[dict] = []        # list of proxy dicts (see schema above)
_proxy_health_task: asyncio.Task | None = None


# ─── PROXY PARSING ───────────────────────────────────────────────────────────

_IP_PORT_RE       = re.compile(r"^([\w\-\.]+):(\d{1,5})$")
_IP_PORT_AUTH_RE  = re.compile(r"^([\w\-\.]+):(\d{1,5}):([^:\s]+):([^:\s]+)$")
_URL_RE           = re.compile(
    r"^(https?|socks4a?|socks5h?)://(?:([^:@/\s]+):([^:@/\s]+)@)?([\w\-\.]+):(\d{1,5})/?$",
    re.IGNORECASE,
)


def parse_proxy_line(line: str) -> dict | None:
    """
    Parse a single proxy line into a proxy dict.
    Returns None if the line is not a valid proxy.

    Supported formats:
      ip:port
      ip:port:user:pass
      scheme://[user:pass@]host:port
    """
    line = line.strip()
    if not line or line.startswith("#"):
        return None

    # Try URL form first (most specific)
    m = _URL_RE.match(line)
    if m:
        scheme, user, pwd, host, port = m.groups()
        scheme = scheme.lower()
        # Normalize aliases: socks5h → socks5, socks4a → socks4
        if scheme == "socks5h":
            scheme = "socks5"
        elif scheme == "socks4a":
            scheme = "socks4"
        return {
            "host":       host,
            "port":       int(port),
            "user":       user or None,
            "pass":       pwd or None,
            "protocol":   scheme,    # explicitly declared
            "url":        _build_proxy_url(scheme, host, int(port), user, pwd),
            "alive":      False,
            "latency":    None,
            "last_check": 0.0,
            "fail_count": 0,
            "explicit":   True,      # user told us the protocol
        }

    # ip:port:user:pass (4 colon-separated parts)
    m = _IP_PORT_AUTH_RE.match(line)
    if m:
        host, port, user, pwd = m.groups()
        return {
            "host":       host,
            "port":       int(port),
            "user":       user,
            "pass":       pwd,
            "protocol":   None,      # → will be auto-detected
            "url":        None,
            "alive":      False,
            "latency":    None,
            "last_check": 0.0,
            "fail_count": 0,
            "explicit":   False,
        }

    # ip:port
    m = _IP_PORT_RE.match(line)
    if m:
        host, port = m.groups()
        return {
            "host":       host,
            "port":       int(port),
            "user":       None,
            "pass":       None,
            "protocol":   None,      # → will be auto-detected
            "url":        None,
            "alive":      False,
            "latency":    None,
            "last_check": 0.0,
            "fail_count": 0,
            "explicit":   False,
        }

    return None


def _build_proxy_url(scheme: str, host: str, port: int,
                     user: str | None, pwd: str | None) -> str:
    """Build a canonical proxy URL for curl_cffi."""
    auth = f"{user}:{pwd}@" if user and pwd else ""
    return f"{scheme}://{auth}{host}:{port}"


def proxy_key(p: dict) -> str:
    """Unique key for deduplication: host:port:user."""
    return f"{p['host']}:{p['port']}:{p.get('user') or ''}"


def proxy_display(p: dict) -> str:
    """Short human-readable display."""
    proto = p["protocol"].upper() if p["protocol"] else "?"
    auth  = " 🔐" if p.get("user") else ""
    return f"[{proto:6s}] {p['host']}:{p['port']}{auth}"


# ─── PROXY PROTOCOL AUTO-DETECTION ───────────────────────────────────────────

async def _probe_single(host: str, port: int,
                        user: str | None, pwd: str | None,
                        scheme: str) -> tuple[bool, float | None, str | None]:
    """
    Try a single protocol against the proxy. Returns (success, latency_ms, ext_ip).
    """
    proxy_url = _build_proxy_url(scheme, host, port, user, pwd)
    test_url  = random.choice(PROXY_TEST_URLS)

    sess = AsyncSession(
        impersonate=random.choice(TLS_IMPERSONATIONS),
        verify=False,
        timeout=PROXY_CHECK_TIMEOUT,
        proxy=proxy_url,
    )
    try:
        t0   = time.monotonic()
        resp = await sess.get(test_url, timeout=PROXY_CHECK_TIMEOUT)
        latency = (time.monotonic() - t0) * 1000.0

        if resp.status_code != 200:
            return False, None, None

        text = resp.text.strip()
        # Extract IP from any of the supported endpoints
        ext_ip = None
        try:
            data   = _json.loads(text)
            ext_ip = data.get("ip") or data.get("origin") or data.get("query")
        except Exception:
            # ifconfig.me returns plain text
            if re.match(r"^\d{1,3}(\.\d{1,3}){3}$", text):
                ext_ip = text

        # Sanity check — make sure we got *something* meaningful back
        if not ext_ip and len(text) < 5:
            return False, None, None

        return True, latency, ext_ip

    except (CurlError, asyncio.TimeoutError, Exception):
        return False, None, None
    finally:
        try:
            await sess.close()
        except Exception:
            pass


async def detect_proxy_protocol(p: dict) -> bool:
    """
    Auto-detect the proxy protocol by probing in order: SOCKS5 → SOCKS4 → HTTP → HTTPS.
    Mutates `p` in place with detected protocol, latency, and url.
    Returns True if at least one protocol worked.

    If the user explicitly declared the protocol (URL form), we only verify that one.
    """
    host, port = p["host"], p["port"]
    user, pwd  = p.get("user"), p.get("pass")

    # If protocol was explicitly declared, only verify it
    if p.get("explicit") and p.get("protocol"):
        ok, latency, _ = await _probe_single(host, port, user, pwd, p["protocol"])
        if ok:
            p["alive"]      = True
            p["latency"]    = latency
            p["last_check"] = time.time()
            p["fail_count"] = 0
            return True
        p["alive"]      = False
        p["last_check"] = time.time()
        p["fail_count"] = p.get("fail_count", 0) + 1
        return False

    # Auto-detect: try each protocol in order, stop on first success
    for scheme in PROXY_PROBE_ORDER:
        ok, latency, _ = await _probe_single(host, port, user, pwd, scheme)
        if ok:
            p["protocol"]   = scheme
            p["url"]        = _build_proxy_url(scheme, host, port, user, pwd)
            p["alive"]      = True
            p["latency"]    = latency
            p["last_check"] = time.time()
            p["fail_count"] = 0
            log.info(f"[PROXY] Detected {scheme.upper()} for {host}:{port} ({latency:.0f}ms)")
            return True

    # All protocols failed
    p["alive"]      = False
    p["protocol"]   = None
    p["last_check"] = time.time()
    p["fail_count"] = p.get("fail_count", 0) + 1
    return False


async def check_proxies_bulk(proxies: list[dict],
                             concurrency: int = PROXY_CHECK_CONCURRENCY,
                             progress_cb=None) -> tuple[int, int]:
    """
    Run detect_proxy_protocol on many proxies concurrently.
    Returns (alive_count, dead_count).
    """
    sem    = asyncio.Semaphore(concurrency)
    done   = [0]
    total  = len(proxies)
    alive  = 0

    async def _one(p):
        nonlocal alive
        async with sem:
            ok = await detect_proxy_protocol(p)
            if ok:
                alive += 1
            done[0] += 1
            if progress_cb and done[0] % 5 == 0:
                try:
                    await progress_cb(done[0], total, alive)
                except Exception:
                    pass

    await asyncio.gather(*[_one(p) for p in proxies], return_exceptions=True)
    return alive, total - alive


# ─── PROXY POOL PERSISTENCE ──────────────────────────────────────────────────

def _persist_proxies() -> None:
    """Write the pool to proxies.txt in a rich format that preserves metadata."""
    try:
        with open("proxies.txt", "w", encoding="utf-8") as f:
            f.write(f"# Proxy pool — managed by bot v19.0\n")
            f.write(f"# Last updated : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"# Total        : {len(_proxy_pool)}\n")
            f.write(f"# Format       : protocol://[user:pass@]host:port  # alive=Y/N latency=Xms\n\n")
            for p in _proxy_pool:
                if p.get("url"):
                    line = p["url"]
                else:
                    # Fallback to ip:port[:user:pass] for un-detected proxies
                    if p.get("user"):
                        line = f"{p['host']}:{p['port']}:{p['user']}:{p['pass']}"
                    else:
                        line = f"{p['host']}:{p['port']}"
                tag = (f"  # alive={'Y' if p['alive'] else 'N'}"
                       f" latency={int(p['latency']) if p['latency'] else 'NA'}ms")
                f.write(line + tag + "\n")
        log.info(f"[PROXY] Persisted {len(_proxy_pool)} proxies to proxies.txt")
    except Exception as exc:
        log.warning(f"[PROXY] Failed to persist proxies.txt: {exc}")


def _load_proxies() -> list[dict]:
    """Load proxies from PROXY_LIST env var (comma-separated) or proxies.txt."""
    proxies: list[dict] = []
    env_list = os.environ.get("PROXY_LIST", "").strip()
    if env_list:
        lines = [p.strip() for p in env_list.split(",") if p.strip()]
        for line in lines:
            p = parse_proxy_line(line)
            if p:
                proxies.append(p)
        log.info(f"[PROXY] Loaded {len(proxies)} proxies from PROXY_LIST env var")
        return proxies

    proxy_file = Path("proxies.txt")
    if proxy_file.exists():
        with open(proxy_file, encoding="utf-8") as f:
            for line in f:
                # Strip inline comments
                clean = line.split("#", 1)[0].strip()
                if not clean:
                    continue
                p = parse_proxy_line(clean)
                if p:
                    proxies.append(p)
        log.info(f"[PROXY] Loaded {len(proxies)} proxies from proxies.txt")
    return proxies


_proxy_pool = _load_proxies()


# ─── POOL OPERATIONS ─────────────────────────────────────────────────────────

def get_random_proxy_url(exclude_url: str | None = None,
                         alive_only: bool = True) -> str | None:
    """
    Return a random proxy URL from the pool.
    Respects PROXY_ENABLED.  If alive_only=True, only picks proxies marked alive.
    """
    if not PROXY_ENABLED or not _proxy_pool:
        return None

    candidates = [
        p["url"] for p in _proxy_pool
        if p.get("url") and (not alive_only or p["alive"]) and p["url"] != exclude_url
    ]
    if not candidates:
        # Fallback: any proxy with a URL, ignoring alive state
        candidates = [p["url"] for p in _proxy_pool if p.get("url") and p["url"] != exclude_url]
    if not candidates:
        return None
    return random.choice(candidates)


def _is_proxy_error(exc: Exception) -> bool:
    """Heuristic: is this CurlError caused by a proxy failure?"""
    msg = str(exc).lower()
    return any(kw in msg for kw in (
        "proxy", "tunnel", "407", "socks", "authentication",
        "connection refused", "network unreachable", "no route to host",
        "could not connect to proxy", "unable to connect to proxy",
        "recv failure", "ssl handshake", "timed out",
    ))


# ─── BACKGROUND HEALTH MONITOR ───────────────────────────────────────────────

async def _proxy_health_loop() -> None:
    """Periodically re-check all proxies and auto-remove repeatedly-failing ones."""
    while True:
        await asyncio.sleep(PROXY_HEALTH_INTERVAL)
        if not _proxy_pool:
            continue
        log.info(f"[HEALTH] Re-checking {len(_proxy_pool)} proxies...")
        async with _proxy_pool_lock:
            snapshot = list(_proxy_pool)
        try:
            alive, dead = await check_proxies_bulk(snapshot)
            log.info(f"[HEALTH] Re-check done: alive={alive} dead={dead}")
            # Remove proxies that have failed too many consecutive times
            async with _proxy_pool_lock:
                before = len(_proxy_pool)
                _proxy_pool[:] = [p for p in _proxy_pool if p.get("fail_count", 0) < PROXY_MAX_FAILS]
                removed = before - len(_proxy_pool)
                if removed:
                    log.warning(f"[HEALTH] Auto-removed {removed} dead proxies (>{PROXY_MAX_FAILS} fails)")
                _persist_proxies()
        except Exception as exc:
            log.error(f"[HEALTH] Loop error: {exc}")


def start_proxy_health_monitor() -> None:
    global _proxy_health_task
    if _proxy_health_task is None or _proxy_health_task.done():
        _proxy_health_task = asyncio.create_task(_proxy_health_loop())
        log.info(f"[HEALTH] Background monitor started (every {PROXY_HEALTH_INTERVAL}s)")


# ══════════════════════════════════════════════════════════════════════════════
# ─── DORK PARSER v19.0 — FULL SYNTAX SUPPORT ─────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
#
# Supported operators:
#   inurl:term              → URL must contain term
#   intitle:term            → page title must contain term
#   intext:term             → page body must contain term
#   site:domain             → restrict to domain
#   filetype:ext / ext:ext  → restrict to file extension
#   cache:url               → cached version
#   "exact phrase"          → exact match
#   -term                   → exclude term
#   term1 OR term2          → either term
#   term1 | term2           → same as OR
#   (group)                 → grouping (passed through for most engines)
#
# Methods:
#   parse_dork(s)     → DorkAST   (structured representation)
#   normalize_dork(s) → str       (cleaned, deduplicated, lowercased ops)
#   validate_dork(s)  → (ok, msg) (syntax check)
#   translate_dork(s, engine) → str   (rewrite for engine quirks)
#   mutate_dork(s, n) → list[str]      (auto-variations)
# ══════════════════════════════════════════════════════════════════════════════

KNOWN_OPERATORS = {
    "inurl", "intitle", "intext", "inanchor", "site",
    "filetype", "ext", "cache", "link", "related", "info",
    "allinurl", "allintitle", "allintext",
}

# Engine quirks
ENGINE_OPERATOR_SUPPORT = {
    "bing":       {"inurl", "intitle", "site", "filetype", "ext", "ip", "contains", "inbody"},
    "yahoo":      {"inurl", "intitle", "site", "filetype", "ext"},
    "duckduckgo": {"inurl", "intitle", "site", "filetype", "ext", "intext"},
    "google":     KNOWN_OPERATORS,
}

# Operator aliases per engine (e.g., Bing prefers `inbody:` over `intext:`)
ENGINE_OPERATOR_ALIAS = {
    "bing":  {"intext": "inbody"},
    "yahoo": {"intext": None, "inanchor": None},   # not supported → drop
}


class DorkToken:
    """A single token in a parsed dork query."""
    __slots__ = ("kind", "op", "value", "negate", "quoted")

    def __init__(self, kind: str, op: str | None, value: str,
                 negate: bool = False, quoted: bool = False):
        self.kind   = kind        # "operator" | "term" | "phrase" | "or" | "group"
        self.op     = op          # e.g. "inurl" or None
        self.value  = value
        self.negate = negate
        self.quoted = quoted

    def __repr__(self):
        n = "-" if self.negate else ""
        q = '"' if self.quoted else ""
        if self.op:
            return f"{n}{self.op}:{q}{self.value}{q}"
        return f"{n}{q}{self.value}{q}"


class DorkAST:
    """Parsed dork query."""
    def __init__(self, tokens: list[DorkToken], raw: str):
        self.tokens = tokens
        self.raw    = raw

    @property
    def operators(self) -> dict[str, list[str]]:
        """Return {op_name: [values]} for all operator tokens."""
        out: dict[str, list[str]] = {}
        for t in self.tokens:
            if t.op:
                out.setdefault(t.op, []).append(t.value)
        return out

    @property
    def free_terms(self) -> list[str]:
        """All non-operator search terms."""
        return [t.value for t in self.tokens if not t.op and t.kind in ("term", "phrase")]

    def __repr__(self):
        return " ".join(repr(t) for t in self.tokens)


# Tokenizer regex
_DORK_TOKEN_RE = re.compile(
    r"""
    (?P<neg>-)?                                 # optional negation
    (?:(?P<op>[a-zA-Z]+):)?                     # optional operator:
    (?:
        "(?P<phrase>[^"]+)"                     # quoted phrase
        |
        \((?P<group>[^)]+)\)                    # grouped expression
        |
        (?P<term>[^\s"()]+)                     # bare term
    )
    """,
    re.VERBOSE,
)


def parse_dork(dork: str) -> DorkAST:
    """
    Parse a raw dork string into a structured DorkAST.
    Handles operators, quoted phrases, negation, OR/|, and groups.
    """
    tokens: list[DorkToken] = []
    pos = 0
    raw = dork.strip()

    for m in _DORK_TOKEN_RE.finditer(raw):
        neg    = bool(m.group("neg"))
        op     = m.group("op")
        phrase = m.group("phrase")
        group  = m.group("group")
        term   = m.group("term")

        if op:
            op = op.lower()

        if phrase is not None:
            tokens.append(DorkToken("phrase", op, phrase, negate=neg, quoted=True))
        elif group is not None:
            tokens.append(DorkToken("group", op, group, negate=neg))
        elif term is not None:
            # Detect OR / | operators
            if term.upper() == "OR" or term == "|":
                tokens.append(DorkToken("or", None, "OR"))
            else:
                tokens.append(DorkToken("term", op, term, negate=neg))

    return DorkAST(tokens, raw)


def validate_dork(dork: str) -> tuple[bool, str]:
    """
    Validate dork syntax. Returns (ok, message).
    Catches: unmatched quotes/parens, unknown operators with warnings, empty queries.
    """
    if not dork or not dork.strip():
        return False, "Empty dork"

    # Balanced quotes
    if dork.count('"') % 2 != 0:
        return False, "Unbalanced double-quotes"

    # Balanced parens
    if dork.count("(") != dork.count(")"):
        return False, "Unbalanced parentheses"

    ast = parse_dork(dork)
    if not ast.tokens:
        return False, "No tokens parsed"

    # Warn about unknown operators (don't fail — engines may support custom ones)
    unknown = [t.op for t in ast.tokens if t.op and t.op not in KNOWN_OPERATORS]
    if unknown:
        return True, f"OK (unknown operators: {', '.join(set(unknown))})"

    # Must have at least one searchable token
    if not any(t.kind in ("term", "phrase", "group") for t in ast.tokens):
        return False, "No search terms"

    return True, "OK"


def normalize_dork(dork: str) -> str:
    """
    Normalize a dork: lowercase operators, collapse whitespace, dedupe equal operator values.
    Preserves order and quoting.
    """
    ast = parse_dork(dork)

    # Dedupe identical operator:value pairs
    seen: set[tuple] = set()
    out: list[str]  = []
    for t in ast.tokens:
        key = (t.op, t.value.lower(), t.negate, t.quoted)
        if key in seen:
            continue
        seen.add(key)
        out.append(repr(t))

    return " ".join(out)


def translate_dork(dork: str, engine: str) -> str:
    """
    Rewrite a dork to match an engine's supported operator set.
    Drops unsupported operators, applies aliases (e.g. intext: → inbody: on Bing).
    """
    if engine not in ENGINE_OPERATOR_SUPPORT:
        return dork

    supported = ENGINE_OPERATOR_SUPPORT[engine]
    aliases   = ENGINE_OPERATOR_ALIAS.get(engine, {})

    ast  = parse_dork(dork)
    out: list[str] = []

    for t in ast.tokens:
        if t.op:
            new_op = aliases.get(t.op, t.op)
            if new_op is None:
                # Operator is explicitly disabled for this engine — drop, keep value as free term
                if t.value:
                    out.append(f'{"-" if t.negate else ""}{"" if not t.quoted else "" }{t.value}')
                continue
            if new_op not in supported:
                # Drop unsupported operator; keep value as free text
                if t.value:
                    prefix = "-" if t.negate else ""
                    q = '"' if t.quoted else ""
                    out.append(f"{prefix}{q}{t.value}{q}")
                continue
            # Use the aliased operator
            t2 = DorkToken(t.kind, new_op, t.value, t.negate, t.quoted)
            out.append(repr(t2))
        else:
            out.append(repr(t))

    return " ".join(out)


def mutate_dork(dork: str, n: int = 5) -> list[str]:
    """
    Generate up to `n` variations of a dork for broader coverage.
    Strategies:
      • Replace `filetype:` ↔ `ext:`
      • Swap common SQL extensions (.php ↔ .asp ↔ .aspx ↔ .jsp)
      • Add/remove `inurl:` wrapper around free terms
      • Add common SQL param hints
    """
    variations = {dork}

    ast = parse_dork(dork)
    ops = ast.operators

    # 1. filetype ↔ ext swap
    if "filetype" in ops:
        for v in ops["filetype"]:
            variations.add(dork.replace(f"filetype:{v}", f"ext:{v}"))
    if "ext" in ops:
        for v in ops["ext"]:
            variations.add(dork.replace(f"ext:{v}", f"filetype:{v}"))

    # 2. SQL extension rotation
    SQL_EXTS = ["php", "asp", "aspx", "jsp", "cfm"]
    for op in ("filetype", "ext"):
        for v in ops.get(op, []):
            if v.lower() in SQL_EXTS:
                for alt in SQL_EXTS:
                    if alt != v.lower():
                        variations.add(dork.replace(f"{op}:{v}", f"{op}:{alt}"))

    # 3. inurl param hints (only if dork already has inurl:)
    if "inurl" in ops:
        hints = ["id=", "pid=", "cat=", "page=", "uid=", "product=", "article="]
        for v in ops["inurl"]:
            for h in hints:
                if h not in v.lower():
                    new = dork.replace(f"inurl:{v}", f"inurl:{v}{h}")
                    variations.add(new)

    out = list(variations - {dork})
    random.shuffle(out)
    return ([dork] + out)[:max(1, n)]


def dedupe_dorks(dorks: list[str]) -> list[str]:
    """Remove duplicates after normalization (case-insensitive operator handling)."""
    seen: set[str] = set()
    out: list[str] = []
    for d in dorks:
        norm = normalize_dork(d).lower()
        if not norm or norm in seen:
            continue
        seen.add(norm)
        out.append(d.strip())
    return out


# ══════════════════════════════════════════════════════════════════════════════
# ─── FILTER + URL CLEANER + SCORER — unchanged from v18.1 ────────────────────
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
    "iltalehti.fi", "sapo.pt", "iol.pt", "idealo.", "zalando.", "trovaprezzi.",
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
    rm_invalid  = rm_blocked = rm_no_query = rm_too_long = 0
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


async def process_chunk_urls(chunk, semaphore, stop_ev) -> list:
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
    chunks    = [raw_lines[i:i+CLEAN_CHUNK_SIZE] for i in range(0, total_input, CLEAN_CHUNK_SIZE)]
    tasks     = [asyncio.create_task(process_chunk_urls(c, semaphore, stop_ev)) for c in chunks]

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
            chat_id=chat_id, message_id=status_msg.message_id,
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
                chat_id, f, filename="cleaned_urls.txt",
                caption=f"🧹 Cleaned URLs{' (partial)' if stopped else ''}\n"
                        f"✅ {len(final_urls)} kept from {total_input} input",
            )
    else:
        await context.bot.send_message(chat_id, "⚠️ No URLs passed the filters.")

    active_stop_evs.pop(chat_id, None)
    active_jobs.pop(chat_id, None)


# ─── BROWSER PROFILES ────────────────────────────────────────────────────────
BROWSER_PROFILES = [
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Sec-Ch-Ua": '"Chromium";v="110", "Not A(Brand";v="24", "Google Chrome";v="110"',
        "Sec-Ch-Ua-Mobile": "?0", "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "document", "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none", "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1", "Cache-Control": "max-age=0",
    },
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9", "Accept-Encoding": "gzip, deflate, br",
        "Sec-Ch-Ua-Platform": '"macOS"', "Upgrade-Insecure-Requests": "1",
    },
    {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5", "Accept-Encoding": "gzip, deflate, br",
        "Upgrade-Insecure-Requests": "1",
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36 Edg/110.0.1587.63",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9", "Accept-Encoding": "gzip, deflate, br",
        "Upgrade-Insecure-Requests": "1",
    },
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9", "Accept-Encoding": "gzip, deflate, br",
    },
]


def _random_headers() -> dict:
    return dict(random.choice(BROWSER_PROFILES))


# ─── SESSION FACTORY (with advanced TLS rotation) ────────────────────────────

def _random_impersonate() -> str:
    return random.choice(TLS_IMPERSONATIONS)


def _make_isolated_session(use_tor: bool = False, proxy: str | None = None) -> AsyncSession:
    chosen_proxy = None
    if use_tor:
        chosen_proxy = TOR_PROXY
    elif proxy:
        chosen_proxy = proxy
    elif PROXY_ENABLED and _proxy_pool:
        chosen_proxy = get_random_proxy_url()

    impersonate = _random_impersonate()
    kwargs = {
        "impersonate": impersonate,
        "verify": False,
        "timeout": 20,
    }
    if chosen_proxy:
        kwargs["proxy"] = chosen_proxy
        log.debug(f"[SESSION] Proxy: {chosen_proxy}, TLS: {impersonate}")
    else:
        log.debug(f"[SESSION] Direct, TLS: {impersonate}")

    sess = AsyncSession(**kwargs)
    sess._cur_proxy = chosen_proxy
    sess._tls_impersonate = impersonate
    return sess


def _make_fallback_session(exclude_proxy: str | None = None) -> AsyncSession:
    fb_proxy = get_random_proxy_url(exclude_url=exclude_proxy)
    return _make_isolated_session(proxy=fb_proxy)


# ─── TOR ROTATION (unchanged) ────────────────────────────────────────────────
_tor_rotation_task = None
tor_enabled_users  = 0

async def rotate_tor_identity() -> None:
    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", 9051)
        await reader.readuntil(b"250 ")
        writer.write(b'AUTHENTICATE ""\r\n'); await writer.drain()
        resp = await reader.readuntil(b"250 ")
        if b"250" not in resp:
            writer.close(); return
        writer.write(b"SIGNAL NEWNYM\r\n"); await writer.drain()
        await reader.readuntil(b"250 ")
        writer.close(); await writer.wait_closed()
        log.info("Tor IP rotated")
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

def stop_tor_rotation() -> None:
    global _tor_rotation_task
    if _tor_rotation_task and not _tor_rotation_task.done():
        _tor_rotation_task.cancel()
        _tor_rotation_task = None


# ─── CAPTCHA / DEGRADED DETECTION ────────────────────────────────────────────
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
    if engine == "bing" and 'id="b_results"' not in html and "b_algo" not in html:
        return True
    if engine == "yahoo" and 'id="results"' not in html and "searchCenterMiddle" not in html:
        return True
    if engine == "duckduckgo" and "result__a" not in html and "results--main" not in html:
        return True
    return False


def _is_captcha(html: str) -> bool:
    return bool(_CAPTCHA_RE.search(html[:4096]))


async def _on_captcha_detected(engine: str, chunk_id: int, session_proxy: str | None) -> None:
    log.warning(f"[C{chunk_id}][{engine.upper()}] 🔴 CAPTCHA detected!")
    backoff = random.uniform(12.0, 25.0)
    await asyncio.sleep(backoff)


# ─── LINK EXTRACTION ─────────────────────────────────────────────────────────
class _LinkExtractor(HTMLParser):
    __slots__ = ("links", "_in_cite", "_buf")

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.links = []; self._in_cite = False; self._buf = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            adict = dict(attrs)
            for key in ("href", "data-u"):
                val = adict.get(key, "")
                if val.startswith("http"):
                    self.links.append(val)
        elif tag == "cite":
            self._in_cite = True; self._buf.clear()

    def handle_endtag(self, tag):
        if tag == "cite" and self._in_cite:
            text = "".join(self._buf).strip()
            if text.startswith("http"):
                self.links.append(text)
            self._in_cite = False; self._buf.clear()

    def handle_data(self, data):
        if self._in_cite:
            self._buf.append(data)


def _extract_links(html: str) -> list:
    p = _LinkExtractor()
    try:
        p.feed(html)
    except Exception:
        pass
    return p.links


_DDG_LINK_RE    = re.compile(r'class="result__a"[^>]*href="(https?://[^"]+)"', re.IGNORECASE)
_DDG_SNIPPET_RE = re.compile(r'uddg=(https?[^&"]+)', re.IGNORECASE)


def _extract_ddg_links(html: str) -> list:
    links = [unquote(m.group(1)) for m in _DDG_LINK_RE.finditer(html)]
    links += [unquote(m.group(1)) for m in _DDG_SNIPPET_RE.finditer(html)]
    return links


_BING_NOISE    = re.compile(r"bing\.com", re.IGNORECASE)
_YAHOO_NOISE   = re.compile(r"yimg\.com|yahoo\.com|doubleclick\.net|googleadservices", re.IGNORECASE)
_STATIC_EXT    = re.compile(r"\.(css|js|png|jpg|jpeg|gif|svg|ico|webp|woff2?|ttf|eot)(\?|$)", re.IGNORECASE)
_YAHOO_RU_PATH = re.compile(r"/RU=([^/&]+)")
_DDG_NOISE     = re.compile(r"duckduckgo\.com|duck\.com", re.IGNORECASE)


# ─── ENGINE FETCH FUNCTIONS (TLS rotation integrated) ────────────────────────

async def _generic_engine_fetch(
    session: AsyncSession,
    method: str,
    url: str,
    *,
    params: dict | None = None,
    data: dict | None = None,
    engine: str,
    page: int,
    max_res: int,
    chunk_id: int,
    referer: str,
    link_extractor,
    noise_filter,
) -> tuple:
    active_session   = session
    fallback_session = None
    try:
        for attempt in range(MAX_RETRIES):
            headers = _random_headers()
            headers["Referer"] = referer
            if data is not None:
                headers["Content-Type"] = "application/x-www-form-urlencoded"
                headers["Origin"]       = referer.rstrip("/")
            try:
                if method == "GET":
                    resp = await active_session.get(url, params=params, headers=headers, timeout=20)
                else:
                    resp = await active_session.post(url, data=data, headers=headers, timeout=20)

                status = resp.status_code
                html   = resp.text

                if status == 429:
                    await asyncio.sleep((2 ** attempt) * random.uniform(4.0, 8.0))
                    continue
                if status != 200:
                    return [], False
                if _is_captcha(html):
                    await _on_captcha_detected(engine, chunk_id, getattr(active_session, "_cur_proxy", None))
                    continue
                if _is_degraded(html, engine):
                    if attempt < MAX_RETRIES - 1:
                        await asyncio.sleep((2 ** attempt) * random.uniform(2.0, 5.0))
                        continue
                    return [], True

                raw  = link_extractor(html)
                urls = [u for u in raw if u.startswith("http")
                        and not noise_filter(u) and not _STATIC_EXT.search(u)]
                urls = list(dict.fromkeys(urls))[:max_res]
                return urls, False

            except asyncio.TimeoutError:
                await asyncio.sleep((2 ** attempt) * random.uniform(2.0, 4.0))
            except CurlError as exc:
                if (_is_proxy_error(exc) and PROXY_ENABLED and len(_proxy_pool) > 1
                        and attempt < MAX_RETRIES - 1):
                    cur_proxy = getattr(active_session, "_cur_proxy", None)
                    if fallback_session is not None:
                        await fallback_session.close()
                    fallback_session = _make_fallback_session(exclude_proxy=cur_proxy)
                    active_session   = fallback_session
                    await asyncio.sleep(random.uniform(1.0, 2.5))
                    continue
                await asyncio.sleep((2 ** attempt) * random.uniform(2.0, 4.0))
            except Exception as exc:
                log.error(f"[C{chunk_id}][{engine.upper()}] unexpected: {exc}")
                return [], False

        return [], True
    finally:
        if fallback_session is not None:
            await fallback_session.close()


def _yahoo_link_extractor(html: str) -> list:
    raw = _extract_links(html)
    out = []
    for u in raw:
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
        out.append(u)
    return out


async def fetch_page_bing(session, dork, page, max_res, chunk_id=0):
    translated = translate_dork(dork, "bing")
    return await _generic_engine_fetch(
        session, "GET", "https://www.bing.com/search",
        params={"q": translated, "count": min(max_res, 10),
                "first": (page-1)*10+1, "setlang": "en"},
        engine="bing", page=page, max_res=max_res, chunk_id=chunk_id,
        referer="https://www.bing.com/",
        link_extractor=_extract_links,
        noise_filter=lambda u: bool(_BING_NOISE.search(u)),
    )


async def fetch_page_yahoo(session, dork, page, max_res, chunk_id=0):
    translated = translate_dork(dork, "yahoo")
    return await _generic_engine_fetch(
        session, "GET", "https://search.yahoo.com/search",
        params={"p": translated, "b": (page-1)*10+1,
                "pz": min(max_res, 10), "vl": "lang_en"},
        engine="yahoo", page=page, max_res=max_res, chunk_id=chunk_id,
        referer="https://search.yahoo.com/",
        link_extractor=_yahoo_link_extractor,
        noise_filter=lambda u: bool(_YAHOO_NOISE.search(u)),
    )


async def fetch_page_duckduckgo(session, dork, page, max_res, chunk_id=0):
    if page > 1:
        return [], False
    translated = translate_dork(dork, "duckduckgo")
    return await _generic_engine_fetch(
        session, "POST", "https://html.duckduckgo.com/html/",
        data={"q": translated, "b": "", "kl": "us-en", "df": ""},
        engine="duckduckgo", page=page, max_res=max_res, chunk_id=chunk_id,
        referer="https://duckduckgo.com/",
        link_extractor=_extract_ddg_links,
        noise_filter=lambda u: bool(_DDG_NOISE.search(u)),
    )


async def fetch_all_pages(session, dork, engine, pages, max_res, chunk_id=0):
    if engine == "duckduckgo":
        sorted_pages = [min(pages)]
    else:
        sorted_pages = sorted(pages)

    fetch_fn = {
        "bing":       fetch_page_bing,
        "yahoo":      fetch_page_yahoo,
        "duckduckgo": fetch_page_duckduckgo,
    }[engine]

    async def _fetch_with_stagger(page, idx):
        if idx > 0:
            await asyncio.sleep(random.uniform(0.1, 0.4) * idx)
        return await fetch_fn(session, dork, page, max_res, chunk_id)

    tasks   = [_fetch_with_stagger(p, i) for i, p in enumerate(sorted_pages)]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    all_urls, degraded_total = [], 0
    for res in results:
        if isinstance(res, Exception):
            continue
        urls, degraded = res
        if degraded:
            degraded_total += 1
        all_urls.extend(urls)
    return all_urls, degraded_total


# ─── WORKER / CHUNK / JOB ────────────────────────────────────────────────────

async def dork_worker(wid, chunk_id, queue, results_q, engines, pages, max_res,
                      session, min_score, stop_ev, slowdown_ev, speed_mode=False):
    eidx = wid % len(engines)
    empty_streak = consecutive_hits = 0

    while not stop_ev.is_set():
        try:
            dork = await asyncio.wait_for(queue.get(), timeout=2.0)
        except asyncio.TimeoutError:
            continue

        engine = engines[eidx % len(engines)]; eidx += 1
        log.info(f"[C{chunk_id}][W{wid}][{engine.upper()}] {dork[:55]}")

        raw, degraded_cnt = [], 0
        try:
            raw, degraded_cnt = await asyncio.wait_for(
                fetch_all_pages(session, dork, engine, pages, max_res, chunk_id),
                timeout=WORKER_FETCH_TIMEOUT,
            )
        except asyncio.TimeoutError:
            log.warning(f"[C{chunk_id}][W{wid}] fetch timeout: {dork[:55]}")
        except asyncio.CancelledError:
            try: results_q.put_nowait((dork, engine, [], 0, 0))
            except asyncio.QueueFull: pass
            queue.task_done(); raise
        except Exception as exc:
            log.warning(f"[C{chunk_id}][W{wid}] fetch error: {exc}")

        scored = filter_scored(raw, min_score)
        try: results_q.put_nowait((dork, engine, scored, len(raw), degraded_cnt))
        except asyncio.QueueFull: await results_q.put((dork, engine, scored, len(raw), degraded_cnt))
        queue.task_done()

        # Dynamic delay based on speed mode
        if speed_mode:
            delay = random.uniform(0.1, 0.25) if raw else random.uniform(0.3, 0.6)
        else:
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
                    delay += min(empty_streak * 2.0, 15.0)
        if slowdown_ev.is_set():
            delay += random.uniform(2.0, 5.0)
        await asyncio.sleep(delay)


async def run_chunk(chunk_id, dorks, engines, pages, max_res, use_tor, min_score,
                    workers_n, progress_q, global_stop_ev, proxy=None, speed_mode=False):
    session     = _make_isolated_session(use_tor=use_tor, proxy=proxy)
    queue       = asyncio.Queue(maxsize=len(dorks) * 2)
    results_q   = asyncio.Queue(maxsize=500)
    stop_ev     = asyncio.Event()
    slowdown_ev = asyncio.Event()

    for d in dorks:
        await queue.put(d)

    total = len(dorks); processed = empty_count = chunk_raw = chunk_degraded = 0
    chunk_scored = []

    async def _watch_global():
        while not stop_ev.is_set():
            if global_stop_ev.is_set():
                stop_ev.set()
            await asyncio.sleep(0.5)

    worker_tasks = [
        asyncio.create_task(dork_worker(i, chunk_id, queue, results_q, engines, pages,
                                        max_res, session, min_score, stop_ev, slowdown_ev, speed_mode))
        for i in range(workers_n)
    ]
    global_watcher = asyncio.create_task(_watch_global())

    try:
        while processed < total and not stop_ev.is_set():
            try:
                dork, engine, scored, raw_cnt, deg_cnt = await asyncio.wait_for(
                    results_q.get(), timeout=CHUNK_STALL_TIMEOUT)
            except asyncio.TimeoutError:
                if all(t.done() for t in worker_tasks):
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
                slowdown_ev.set()
            elif empty_rate < EMPTY_RATE_RECOVER and slowdown_ev.is_set():
                slowdown_ev.clear()

            try:
                progress_q.put_nowait({"chunk_id": chunk_id, "processed": processed,
                                       "total": total, "raw": raw_cnt, "kept": len(scored)})
            except asyncio.QueueFull:
                pass

        for t in worker_tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*worker_tasks, return_exceptions=True)

    except asyncio.CancelledError:
        stop_ev.set()
        for t in worker_tasks:
            t.cancel()
        await asyncio.gather(*worker_tasks, return_exceptions=True)
        raise
    finally:
        global_watcher.cancel()
        await asyncio.gather(global_watcher, return_exceptions=True)
        await session.close()

    return {"chunk_id": chunk_id, "scored": chunk_scored, "raw_count": chunk_raw,
            "degraded_count": chunk_degraded, "processed": processed,
            "empty_count": empty_count}


async def run_dork_job(chat_id, dorks, context):
    sess      = get_session(chat_id)
    engines   = sess.get("engines", list(ENGINES))
    workers_n = min(sess.get("workers", WORKERS_PER_CHUNK), MAX_WORKERS_PER_CHUNK)
    max_res   = sess.get("max_results", MAX_RESULTS)
    pages     = sess.get("pages", [1])
    use_tor   = sess.get("tor", False)
    min_score = sess.get("min_score", 30)
    n_chunks  = max(1, sess.get("chunks", N_CHUNKS))
    speed_mode = sess.get("speed_mode", False)

    # dedup + validate dorks before running
    cleaned = dedupe_dorks(dorks)
    valid_dorks   = []
    invalid_dorks = []
    for d in cleaned:
        ok, msg = validate_dork(d)
        if ok:
            valid_dorks.append(d)
        else:
            invalid_dorks.append((d, msg))

    dorks       = valid_dorks
    total_dorks = len(dorks)
    if invalid_dorks:
        log.warning(f"[JOB][{chat_id}] Skipped {len(invalid_dorks)} invalid dorks")

    if total_dorks == 0:
        await context.bot.send_message(chat_id, "⚠️ No valid dorks after parsing/dedup. Job aborted.")
        active_jobs.pop(chat_id, None)
        return

    pages_str  = ", ".join(str(p) for p in pages)
    start_time = time.time()

    chunk_size    = max(1, -(-total_dorks // n_chunks))
    chunks        = [dorks[i:i+chunk_size] for i in range(0, total_dorks, chunk_size)]
    actual_chunks = len(chunks)

    tmp_file = tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", delete=False,
                                            prefix=f"dork_{chat_id}_", suffix=".txt")
    tmp_path = tmp_file.name
    tmp_file.write(f"# Dork Parser v19.0 — SQL Targeted Results\n")
    tmp_file.write(f"# Date   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    tmp_file.write(f"# Dorks  : {total_dorks} (skipped invalid: {len(invalid_dorks)})\n")
    tmp_file.write(f"# Pages  : {pages_str} | Filter: SQL ≥{min_score} | Chunks: {actual_chunks}\n")
    tmp_file.close()

    alive_proxies = sum(1 for p in _proxy_pool if p["alive"])
    if use_tor:
        proxy_info = "🧅 TOR"
    elif PROXY_ENABLED and alive_proxies:
        proxy_info = f"🔄 {alive_proxies}/{len(_proxy_pool)} alive proxies (auto-rotation)"
    elif PROXY_ENABLED and _proxy_pool:
        proxy_info = f"⚠️ {len(_proxy_pool)} proxies, 0 alive"
    elif not PROXY_ENABLED and _proxy_pool:
        proxy_info = f"⏸ {len(_proxy_pool)} loaded but DISABLED"
    else:
        proxy_info = "🔓 Direct (no proxy)"

    status_msg = await context.bot.send_message(
        chat_id,
        f"🕷 DORK PARSER v19.0 — STARTED\n{'━'*30}\n"
        f"📋 Dorks   : {total_dorks}"
        + (f" (⚠️ {len(invalid_dorks)} skipped)" if invalid_dorks else "")
        + f"\n📄 Pages   : {pages_str}\n"
        f"⚡ Chunks  : {actual_chunks}\n"
        f"⚙️ Workers : {workers_n}/chunk\n"
        f"🔍 Engines : {' + '.join(e.upper() for e in engines)}\n"
        f"🛡 Filter  : SQL ≥{min_score}\n"
        f"🌐 Network : {proxy_info}\n"
        f"🔒 TLS     : Rotating fingerprints ({len(TLS_IMPERSONATIONS)} profiles)\n"
        f"{'⚡' if speed_mode else '🐢'} Speed Mode : {'ON (200+ URLs/s)' if speed_mode else 'OFF'}\n"
        f"{'━'*30}\n⏳ Starting...",
    )

    global_stop_ev = asyncio.Event()
    active_stop_evs[chat_id] = global_stop_ev
    progress_q: asyncio.Queue = asyncio.Queue(maxsize=total_dorks * 2)
    chunk_counters = {i: {"processed": 0, "total": len(chunks[i])} for i in range(actual_chunks)}
    agg_raw = [0]; agg_kept = [0]; last_edit = [0.0]; total_processed = [0]

    async def _status_updater():
        while not global_stop_ev.is_set():
            drained = False
            while True:
                try:
                    ev = progress_q.get_nowait()
                    chunk_counters[ev["chunk_id"]]["processed"] = ev["processed"]
                    agg_raw[0] += ev["raw"]; agg_kept[0] += ev["kept"]
                    total_processed[0] += 1; drained = True
                except asyncio.QueueEmpty:
                    break
            if drained and time.time() - last_edit[0] > 4.0:
                proc = total_processed[0]
                pct = int(proc / total_dorks * 100) if total_dorks else 100
                bar = "█" * (pct // 10) + "░" * (10 - pct // 10)
                elapsed = int(time.time() - start_time)
                eta = int((elapsed / proc) * (total_dorks - proc)) if proc else 0
                cinfo = " | ".join(f"C{i}:{chunk_counters[i]['processed']}/{chunk_counters[i]['total']}"
                                   for i in range(actual_chunks))
                try:
                    await context.bot.edit_message_text(
                        chat_id=chat_id, message_id=status_msg.message_id,
                        text=(f"⚡ PARSING... [{actual_chunks} chunks]\n{'━'*30}\n"
                              f"[{bar}] {pct}%\n✅ Done: {proc}/{total_dorks}\n"
                              f"🎯 SQL: {agg_kept[0]}\n🗑 Raw drop: {agg_raw[0] - agg_kept[0]}\n"
                              f"⏱ {elapsed}s | ETA {eta}s\n📦 {cinfo}\n{'━'*30}"),
                    )
                    last_edit[0] = time.time()
                except Exception:
                    pass
            await asyncio.sleep(0.5)

    async def _job_timeout():
        await asyncio.sleep(JOB_TIMEOUT)
        global_stop_ev.set()

    status_task  = asyncio.create_task(_status_updater())
    timeout_task = asyncio.create_task(_job_timeout())

    chunk_proxies = [get_random_proxy_url() if not use_tor else None for _ in range(actual_chunks)]

    chunk_results = []
    try:
        chunk_tasks = []
        for i, chunk_dorks in enumerate(chunks):
            if i > 0:
                await asyncio.sleep(random.uniform(*CHUNK_STAGGER_DELAY))
            task = asyncio.create_task(
                run_chunk(i, chunk_dorks, engines, pages, max_res, use_tor, min_score,
                          workers_n, progress_q, global_stop_ev, proxy=chunk_proxies[i],
                          speed_mode=speed_mode)
            )
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

    seen_urls: set = set(); all_scored: list = []
    total_raw = total_degraded = failed_chunks = 0
    for result in chunk_results:
        if isinstance(result, Exception):
            failed_chunks += 1; continue
        for sc, url in result["scored"]:
            if url not in seen_urls:
                seen_urls.add(url); all_scored.append((sc, url))
        total_raw += result["raw_count"]
        total_degraded += result["degraded_count"]

    all_scored.sort(reverse=True)
    unique_cnt = len(all_scored)
    elapsed = int(time.time() - start_time)
    success_rate = (total_raw - (total_raw - unique_cnt)) / max(total_raw, 1)

    high   = [(s, u) for s, u in all_scored if s >= 70]
    medium = [(s, u) for s, u in all_scored if 40 <= s < 70]
    low    = [(s, u) for s, u in all_scored if s < 40]

    with open(tmp_path, "a", encoding="utf-8") as f:
        if high:
            f.write(f"# ── HIGH VALUE (≥70) — {len(high)}\n")
            for _, u in high: f.write(f"{u}\n")
        if medium:
            f.write(f"\n# ── MEDIUM (40-69) — {len(medium)}\n")
            for _, u in medium: f.write(f"{u}\n")
        if low and min_score < 40:
            f.write(f"\n# ── LOW (<40) — {len(low)}\n")
            for _, u in low: f.write(f"{u}\n")

    try:
        await context.bot.edit_message_text(
            chat_id=chat_id, message_id=status_msg.message_id,
            text=(f"🏁 JOB COMPLETE!\n{'━'*30}\n"
                  f"📋 Dorks    : {total_dorks}\n📄 Pages    : {pages_str}\n"
                  f"⚡ Chunks   : {actual_chunks}\n🔍 Raw      : {total_raw}\n"
                  f"🎯 SQL      : {unique_cnt} unique\n"
                  f"🗑 Dropped  : {total_raw - unique_cnt}\n"
                  f"⚠️ Degraded : {total_degraded}\n📊 Hit rate : {success_rate:.0%}\n"
                  f"⏱ Time     : {elapsed}s\n{'━'*30}"),
        )
    except Exception:
        pass

    if all_scored:
        with open(tmp_path, "rb") as f:
            await context.bot.send_document(
                chat_id, f, filename=f"sql_{total_dorks}dorks_{unique_cnt}urls.txt",
                caption=f"📁 SQL Targets\n🎯 {unique_cnt} unique | 🗑 {total_raw - unique_cnt} junk\n"
                        f"📋 {total_dorks} dorks | Pages: {pages_str} | ⚡ {actual_chunks} chunks",
            )
    else:
        await context.bot.send_message(chat_id, "⚠️ No URLs matched filter criteria.")

    try: os.unlink(tmp_path)
    except OSError: pass


# ══════════════════════════════════════════════════════════════════════════════
# ─── XTREAM MODE (1000+ URLs/s via Yahoo) ────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

async def run_xtream_job(chat_id: int, dorks: list[str], context) -> None:
    sess = get_session(chat_id)
    # 1. Generate many mutations from the seed dorks
    all_dorks = []
    for d in dorks:
        variants = mutate_dork(d, n=20)
        all_dorks.extend(variants)
    all_dorks = dedupe_dorks(all_dorks)
    log.info(f"[XTREAM] {len(all_dorks)} unique dorks generated")

    # 2. Prepare rapid-fire fetchers
    semaphore = asyncio.Semaphore(200)  # 200 concurrent requests max
    stop_ev = asyncio.Event()
    active_stop_evs[chat_id] = stop_ev

    results_queue = asyncio.Queue(maxsize=10000)
    min_score = sess["min_score"]

    async def fetch_one(dork):
        if stop_ev.is_set():
            return
        proxy_url = get_random_proxy_url(alive_only=True)
        session = _make_isolated_session(proxy=proxy_url)
        try:
            # Only page 1 for speed
            urls, degraded = await asyncio.wait_for(
                fetch_page_yahoo(session, dork, 1, 10, chunk_id=0),
                timeout=15
            )
            scored = filter_scored(urls, min_score)
            for sc, url in scored:
                await results_queue.put((sc, url))
        except Exception:
            pass
        finally:
            await session.close()

    # 3. Launch all tasks
    tasks = []
    for d in all_dorks:
        async def worker(dork=d):
            async with semaphore:
                await fetch_one(dork)
        tasks.append(asyncio.create_task(worker()))

    # 4. Stream results to file while tasks run
    tmp_path = tempfile.mktemp(suffix=".txt")
    total_urls = 0
    start_time = time.time()
    status_msg = await context.bot.send_message(chat_id, "🔥 Xtream storm started...")

    async def collector():
        nonlocal total_urls
        seen = set()
        with open(tmp_path, "w", encoding="utf-8") as f:
            while not stop_ev.is_set():
                try:
                    sc, url = await asyncio.wait_for(results_queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    if all(t.done() for t in tasks):
                        break
                    continue
                if url not in seen:
                    seen.add(url)
                    f.write(f"{url}\n")
                    total_urls += 1
                    if total_urls % 1000 == 0:
                        elapsed = time.time() - start_time
                        rate = total_urls / elapsed if elapsed > 0 else 0
                        try:
                            await context.bot.edit_message_text(
                                f"🔥 Xtream: {total_urls} URLs ({rate:.0f}/s)",
                                chat_id=chat_id, message_id=status_msg.message_id
                            )
                        except Exception:
                            pass

    collector_task = asyncio.create_task(collector())
    await asyncio.gather(*tasks, return_exceptions=True)
    stop_ev.set()
    await collector_task

    elapsed = time.time() - start_time
    rate = total_urls / elapsed if elapsed else 0

    if total_urls > 0:
        with open(tmp_path, "rb") as f:
            await context.bot.send_document(
                chat_id, f,
                filename=f"xtream_{total_urls}urls.txt",
                caption=f"🔥 Xtream done: {total_urls} URLs in {elapsed:.1f}s ({rate:.0f}/s)"
            )
    else:
        await context.bot.send_message(chat_id, "🔥 No URLs caught.")
    os.unlink(tmp_path)
    active_stop_evs.pop(chat_id, None)
    active_jobs.pop(chat_id, None)


# ─── UI HELPERS ──────────────────────────────────────────────────────────────
def get_session(chat_id: int) -> dict:
    if chat_id not in user_sessions:
        user_sessions[chat_id] = dict(DEFAULT_SESSION)
    return user_sessions[chat_id]


def page_keyboard(selected: list) -> InlineKeyboardMarkup:
    rows, row = [], []
    for p in range(1, 71):
        row.append(InlineKeyboardButton(f"✅{p}" if p in selected else str(p),
                                        callback_data=f"pg_{p}"))
        if len(row) == 5:
            rows.append(row); row = []
    if row: rows.append(row)
    rows.append([
        InlineKeyboardButton("🔁 All (1-70)", callback_data="pg_all"),
        InlineKeyboardButton("❌ Clear", callback_data="pg_clear"),
        InlineKeyboardButton("✅ Confirm", callback_data="pg_confirm"),
    ])
    return InlineKeyboardMarkup(rows)


# ══════════════════════════════════════════════════════════════════════════════
# ─── COMMAND HANDLERS ────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kb = [
        [InlineKeyboardButton("📂 Bulk Upload", callback_data="m_bulk"),
         InlineKeyboardButton("🔍 Single Dork", callback_data="m_single")],
        [InlineKeyboardButton("📄 Select Pages", callback_data="m_pages"),
         InlineKeyboardButton("⚙️ Settings", callback_data="m_settings")],
        [InlineKeyboardButton("🧅 Tor On/Off", callback_data="m_tor"),
         InlineKeyboardButton("🛡 SQL Filter", callback_data="m_filter")],
        [InlineKeyboardButton("🧹 URL Cleaner", callback_data="m_clean"),
         InlineKeyboardButton("📖 Help", callback_data="m_help")],
        [InlineKeyboardButton("⚡ Speed Mode", callback_data="m_speed"),
         InlineKeyboardButton("🔥 Xtream", callback_data="m_xtream")],
    ]

    alive = sum(1 for p in _proxy_pool if p["alive"])
    if PROXY_ENABLED and alive:
        proxy_status = f"🔄 {alive}/{len(_proxy_pool)} alive proxies"
    elif PROXY_ENABLED and _proxy_pool:
        proxy_status = f"⚠️ {len(_proxy_pool)} proxies (0 alive — run /proxycheck)"
    elif not PROXY_ENABLED and _proxy_pool:
        proxy_status = f"⏸ {len(_proxy_pool)} loaded (DISABLED)"
    else:
        proxy_status = "🔓 No proxies"

    await update.message.reply_text(
        "🕷 DORK PARSER v19.0 — INTELLIGENT EDITION\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🧠 Full dork syntax parser + validator + mutator\n"
        "🔁 Cross-engine dork translation\n"
        "🔍 Auto-detect proxy protocol (SOCKS5/4/HTTP/HTTPS)\n"
        "✅ Auto health-check + background re-validation\n"
        "📥 Accepts ip:port, ip:port:user:pass, URL form\n"
        "⚡ Parallel chunks + Chrome110 TLS\n"
        f"🔒 TLS Fingerprints: {len(TLS_IMPERSONATIONS)} rotating\n"
        f"{proxy_status}\n\n"
        "📌 Core Commands:\n"
        "  /dork <q>     — single dork search\n"
        "  /dorkcheck <q>— validate/normalize/preview\n"
        "  /mutate <q>   — generate dork variations\n"
        "  /clean        — URL cleaner mode\n"
        "  /pages        — page selector\n"
        "  /workers N    — workers/chunk\n"
        "  /chunks N     — parallel chunks\n"
        "  /engine X     — bing|yahoo|duckduckgo|all\n"
        "  /tor          — toggle Tor\n"
        "  /filter N     — SQL score 0-100\n"
        "  /stop         — stop & get partial\n"
        "  /speed on|off — high-speed 200 URLs/s\n"
        "  /xtream on|off— Yahoo storm 1000+ URLs/s\n\n"
        "🔄 Proxy Commands:\n"
        "  /addproxy <line>   — auto-detect & add ONE\n"
        "  /addproxies        — bulk paste (next msg)\n"
        "  /proxycheck        — re-check entire pool\n"
        "  /proxylist         — view pool\n"
        "  /removeproxy [i]   — remove by index\n"
        "  /testproxy <line>  — test one without adding\n"
        "  /proxyclean        — drop all dead proxies\n"
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
    ok, msg = validate_dork(dork)
    if not ok:
        await update.message.reply_text(f"❌ Invalid dork: {msg}")
        return
    sess = get_session(chat_id)
    if sess.get("xtream"):
        sess["engines"] = ["yahoo"]
        await update.message.reply_text("🔥 Xtream storm incoming…")
        active_jobs[chat_id] = asyncio.create_task(run_xtream_job(chat_id, [dork], context))
    else:
        await update.message.reply_text(
            f"🔍 {dork[:60]}\n📄 Pages: {', '.join(str(p) for p in sess.get('pages', [1]))}"
            f"{'  🧅TOR' if sess.get('tor') else ''}\n💡 {msg}"
        )
        active_jobs[chat_id] = asyncio.create_task(run_dork_job(chat_id, [dork], context))


async def cmd_dorkcheck(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(
            "🧠 DORK CHECKER\n━━━━━━━━━━━━━━\n"
            "Usage: /dorkcheck <dork>\n\n"
            "Example:\n"
            "  /dorkcheck inurl:login.php?id= filetype:php -site:github.com"
        )
        return

    dork = " ".join(context.args)
    ok, msg = validate_dork(dork)
    ast      = parse_dork(dork)
    normd    = normalize_dork(dork)

    lines = [
        f"🧠 DORK ANALYSIS",
        "━━━━━━━━━━━━━━━━━━━━━━",
        f"📝 Raw   : {dork}",
        f"✨ Norm  : {normd}",
        f"✅ Status: {'OK' if ok else 'FAIL'} — {msg}",
        f"🔢 Tokens: {len(ast.tokens)}",
        f"🎯 Operators:",
    ]
    if ast.operators:
        for op, vals in ast.operators.items():
            lines.append(f"   • {op}: {', '.join(vals)}")
    else:
        lines.append("   (none)")

    if ast.free_terms:
        lines.append(f"🔤 Free terms: {', '.join(ast.free_terms)}")

    lines += ["", "🔁 Engine translations:"]
    for engine in ENGINES:
        translated = translate_dork(dork, engine)
        lines.append(f"   {engine.upper():12s}: {translated[:80]}")

    await update.message.reply_text("\n".join(lines))


async def cmd_mutate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /mutate <dork> [n=10]")
        return

    args = list(context.args)
    n = 10
    if args[-1].isdigit():
        n = max(1, min(int(args[-1]), 50))
        args = args[:-1]
    dork = " ".join(args)

    variations = mutate_dork(dork, n=n)
    lines = [f"🧬 DORK MUTATIONS ({len(variations)})", "━━━━━━━━━━━━━━━━━━━━━━"]
    for i, v in enumerate(variations, 1):
        lines.append(f"{i:>2}. {v}")
    await update.message.reply_text("\n".join(lines))


async def cmd_pages(update, context):
    chat_id  = update.effective_chat.id
    selected = get_session(chat_id).get("pages", [1])
    await update.message.reply_text(
        f"📄 SELECT PAGES (1–70)\n━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Selected: {', '.join(str(p) for p in selected)}",
        reply_markup=page_keyboard(selected),
    )


async def cmd_tor(update, context):
    global tor_enabled_users
    chat_id = update.effective_chat.id
    sess    = get_session(chat_id)
    new_val = (context.args[0].lower() == "on") if context.args and context.args[0].lower() in ("on", "off") else not sess.get("tor", False)
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
        await update.message.reply_text(f"Usage: /filter N (0-100)\nCurrent: {sess.get('min_score', 30)}")


async def cmd_settings(update, context):
    chat_id = update.effective_chat.id
    s = get_session(chat_id)
    alive = sum(1 for p in _proxy_pool if p["alive"])
    if PROXY_ENABLED and _proxy_pool:
        proxy_line = f"🔄 Proxies  : {alive}/{len(_proxy_pool)} alive\n"
    elif not PROXY_ENABLED and _proxy_pool:
        proxy_line = f"⏸ Proxies  : {len(_proxy_pool)} DISABLED\n"
    else:
        proxy_line = "🔓 Proxies  : none\n"
    await update.message.reply_text(
        f"⚙️ SETTINGS\n━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚡ Chunks   : {s.get('chunks', N_CHUNKS)}\n"
        f"🔧 Workers  : {s.get('workers', WORKERS_PER_CHUNK)}/chunk\n"
        f"📄 Pages    : {', '.join(str(p) for p in s.get('pages', [1]))}\n"
        f"🔍 Engines  : {'+'.join(e.upper() for e in s.get('engines', ENGINES))}\n"
        f"📊 Max/Page : {s.get('max_results', MAX_RESULTS)}\n"
        f"🛡 SQL ≥    : {s.get('min_score', 30)}\n"
        f"🧅 Tor      : {'ON' if s.get('tor') else 'OFF'}\n"
        f"{proxy_line}━━━━━━━━━━━━━━━━━━━━━━"
        f"⚡ Speed    : {'ON' if s.get('speed_mode') else 'OFF'}\n"
        f"🔥 Xtream   : {'ON (Yahoo only)' if s.get('xtream') else 'OFF'}"
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
        await update.message.reply_text(f"Usage: /chunks N (1-8)")


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
        m = {"bing": ["bing"], "yahoo": ["yahoo"], "duckduckgo": ["duckduckgo"],
             "ddg": ["duckduckgo"], "all": list(ENGINES), "both": ["bing", "yahoo"]}
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
    job     = active_jobs.get(chat_id)
    if stop_ev and job and not job.done():
        stop_ev.set()
        await update.message.reply_text("⏹ STOP REQUESTED — partial results coming.")
    elif job and not job.done():
        job.cancel()
        active_jobs.pop(chat_id, None)
        await update.message.reply_text("🛑 Force-stopped.")
    else:
        await update.message.reply_text("💤 No active job.")


async def cmd_status(update, context):
    chat_id = update.effective_chat.id
    job = active_jobs.get(chat_id)
    await update.message.reply_text("⚡ Running" if job and not job.done() else "💤 Idle")


# ─── NEW SPEED / XTREAM COMMANDS ─────────────────────────────────────────────

async def cmd_speed(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    sess = get_session(chat_id)
    if not context.args or context.args[0].lower() not in ("on", "off"):
        await update.message.reply_text(
            "Usage: /speed on|off\n"
            "Enables high-speed mode (~200 URLs/s) with aggressive parallelism."
        )
        return
    state = context.args[0].lower() == "on"
    sess["speed_mode"] = state
    if state:
        # Boost workers/chunks for speed
        sess["workers"] = min(sess.get("workers", WORKERS_PER_CHUNK), MAX_WORKERS_PER_CHUNK)
        sess["chunks"] = min(sess.get("chunks", N_CHUNKS), 8)
        sess["max_results"] = min(sess.get("max_results", MAX_RESULTS), 20)
    await update.message.reply_text(f"⚡ Speed mode {'ENABLED' if state else 'DISABLED'}")


async def cmd_xtream(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    sess = get_session(chat_id)
    if not context.args or context.args[0].lower() not in ("on", "off"):
        await update.message.reply_text(
            "🔥 /xtream on|off\n"
            "Xtream mode: 1000+ URLs/s via Yahoo with auto-mutations & proxy storm."
        )
        return
    state = context.args[0].lower() == "on"
    sess["xtream"] = state
    if state:
        # Force best settings for Yahoo
        sess["engines"] = ["yahoo"]
        sess["speed_mode"] = True  # also enable low delays
    await update.message.reply_text(
        f"🔥 Xtream mode {'ENABLED 🚀' if state else 'DISABLED'} on Yahoo"
    )


# ══════════════════════════════════════════════════════════════════════════════
# ─── PROXY COMMAND HANDLERS v19.0 ────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

_awaiting_bulk_proxy: set[int] = set()


async def cmd_addproxy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(
            "➕ ADD PROXY\n━━━━━━━━━━━━━━━━━━━━━━\n"
            "Usage: /addproxy <proxy>\n\n"
            "Accepted formats (auto-detected):\n"
            "  • ip:port\n"
            "  • ip:port:user:pass\n"
            "  • socks5://user:pass@host:port\n"
            "  • http://host:port\n\n"
            "The bot will automatically probe SOCKS5 → SOCKS4 → HTTP → HTTPS\n"
            "to determine the proxy's actual protocol and verify it works."
        )
        return

    line = " ".join(context.args).strip()
    p    = parse_proxy_line(line)
    if not p:
        await update.message.reply_text("❌ Invalid proxy format.")
        return

    key = proxy_key(p)
    async with _proxy_pool_lock:
        if any(proxy_key(x) == key for x in _proxy_pool):
            await update.message.reply_text("⚠️ Proxy already in pool.")
            return

    wait_msg = await update.message.reply_text(
        f"🔍 Auto-detecting protocol for {p['host']}:{p['port']}...\n"
        f"Probing SOCKS5 → SOCKS4 → HTTP → HTTPS"
    )

    ok = await detect_proxy_protocol(p)
    if not ok:
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id, message_id=wait_msg.message_id,
            text=f"❌ PROXY FAILED\n━━━━━━━━━━━━━━━━━━━━━━\n"
                 f"🌐 {p['host']}:{p['port']}\n"
                 f"💬 No protocol responded successfully\n"
                 f"❌ Not added to pool"
        )
        return

    async with _proxy_pool_lock:
        _proxy_pool.append(p)
        _persist_proxies()

    await context.bot.edit_message_text(
        chat_id=update.effective_chat.id, message_id=wait_msg.message_id,
        text=(f"✅ PROXY ADDED\n━━━━━━━━━━━━━━━━━━━━━━\n"
              f"🔌 Protocol  : {p['protocol'].upper()} (auto-detected)\n"
              f"🌐 Host      : {p['host']}:{p['port']}\n"
              f"🔐 Auth      : {'Yes' if p.get('user') else 'No'}\n"
              f"⏱ Latency   : {int(p['latency'])} ms\n"
              f"━━━━━━━━━━━━━━━━━━━━━━\n"
              f"📦 Pool size : {len(_proxy_pool)}")
    )


async def cmd_addproxies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    _awaiting_bulk_proxy.add(chat_id)
    await update.message.reply_text(
        "📥 BULK PROXY IMPORT\n━━━━━━━━━━━━━━━━━━━━━━\n"
        "Send your proxy list as the NEXT message (one per line).\n\n"
        "Accepted per line:\n"
        "  ip:port\n"
        "  ip:port:user:pass\n"
        "  socks5://user:pass@host:port\n"
        "  http://host:port\n\n"
        "All proxies will be:\n"
        "  1. Parsed automatically\n"
        "  2. Protocol auto-detected (SOCKS5/4/HTTP/HTTPS)\n"
        "  3. Tested live\n"
        "  4. Added to pool if alive\n\n"
        "💡 Or just upload a .txt file with the same format."
    )


async def _bulk_add_proxies(chat_id: int, lines: list[str], context) -> None:
    parsed: list[dict] = []
    invalid = 0

    for line in lines:
        if not line.strip() or line.startswith("#"):
            continue
        line = line.split("#", 1)[0].strip()
        if not line:
            continue
        p = parse_proxy_line(line)
        if p:
            parsed.append(p)
        else:
            invalid += 1

    seen_keys = {proxy_key(p) for p in _proxy_pool}
    unique:    list[dict] = []
    dup_count = 0
    for p in parsed:
        k = proxy_key(p)
        if k in seen_keys:
            dup_count += 1; continue
        seen_keys.add(k)
        unique.append(p)

    if not unique:
        await context.bot.send_message(chat_id, f"⚠️ Nothing to add.\n❌ Invalid lines: {invalid}\n🔁 Duplicates: {dup_count}")
        return

    status_msg = await context.bot.send_message(
        chat_id,
        f"🔍 BULK CHECK STARTED\n━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📥 Input lines     : {len(lines)}\n"
        f"✅ Parsed          : {len(parsed)}\n"
        f"❌ Invalid format  : {invalid}\n"
        f"🔁 Duplicates      : {dup_count}\n"
        f"🆕 To check        : {len(unique)}\n"
        f"⏳ Auto-detecting protocols ({PROXY_CHECK_CONCURRENCY} concurrent)..."
    )

    last_edit = [0.0]
    async def _progress(done, total, alive):
        if time.monotonic() - last_edit[0] < 2.5:
            return
        pct = int(done / total * 100) if total else 100
        bar = "█" * (pct // 10) + "░" * (10 - pct // 10)
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id, message_id=status_msg.message_id,
                text=(f"🔍 BULK CHECK IN PROGRESS\n━━━━━━━━━━━━━━━━━━━━━━\n"
                      f"[{bar}] {pct}%\n"
                      f"✅ Checked : {done}/{total}\n"
                      f"💚 Alive   : {alive}\n"
                      f"💀 Dead    : {done - alive}")
            )
            last_edit[0] = time.monotonic()
        except Exception:
            pass

    alive, dead = await check_proxies_bulk(unique, progress_cb=_progress)

    added_alive: list[dict] = []
    async with _proxy_pool_lock:
        for p in unique:
            if p["alive"]:
                _proxy_pool.append(p)
                added_alive.append(p)
        _persist_proxies()

    breakdown: dict[str, int] = {}
    for p in added_alive:
        breakdown[p["protocol"]] = breakdown.get(p["protocol"], 0) + 1
    bd_text = "\n".join(f"   • {k.upper()}: {v}" for k, v in breakdown.items()) or "   (none)"

    try:
        await context.bot.edit_message_text(
            chat_id=chat_id, message_id=status_msg.message_id,
            text=(f"✅ BULK IMPORT COMPLETE\n━━━━━━━━━━━━━━━━━━━━━━\n"
                  f"📥 Input lines    : {len(lines)}\n"
                  f"❌ Invalid format : {invalid}\n"
                  f"🔁 Duplicates     : {dup_count}\n"
                  f"💀 Dead (skipped) : {dead}\n"
                  f"💚 Added (alive)  : {len(added_alive)}\n"
                  f"━━━━━━━━━━━━━━━━━━━━━━\n"
                  f"🔌 Protocol breakdown:\n{bd_text}\n"
                  f"━━━━━━━━━━━━━━━━━━━━━━\n"
                  f"📦 Pool size: {len(_proxy_pool)}")
        )
    except Exception:
        pass


async def cmd_proxycheck(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _proxy_pool:
        await update.message.reply_text("📭 Pool is empty.")
        return

    status_msg = await update.message.reply_text(f"🔍 Re-checking {len(_proxy_pool)} proxies...")
    last_edit = [0.0]
    async def _progress(done, total, alive):
        if time.monotonic() - last_edit[0] < 2.5:
            return
        pct = int(done / total * 100) if total else 100
        try:
            await context.bot.edit_message_text(
                chat_id=update.effective_chat.id, message_id=status_msg.message_id,
                text=f"🔍 Re-check {pct}%\n✅ {done}/{total}\n💚 Alive: {alive}"
            )
            last_edit[0] = time.monotonic()
        except Exception:
            pass

    alive, dead = await check_proxies_bulk(list(_proxy_pool), progress_cb=_progress)
    _persist_proxies()

    try:
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id, message_id=status_msg.message_id,
            text=(f"✅ POOL HEALTH CHECK\n━━━━━━━━━━━━━━━━━━━━━━\n"
                  f"📦 Total : {len(_proxy_pool)}\n"
                  f"💚 Alive : {alive}\n"
                  f"💀 Dead  : {dead}\n"
                  f"━━━━━━━━━━━━━━━━━━━━━━\n"
                  f"Tip: /proxyclean to remove all dead.")
        )
    except Exception:
        pass


async def cmd_proxyclean(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with _proxy_pool_lock:
        before = len(_proxy_pool)
        _proxy_pool[:] = [p for p in _proxy_pool if p["alive"]]
        removed = before - len(_proxy_pool)
        _persist_proxies()
    await update.message.reply_text(f"🧹 PROXY CLEAN\n━━━━━━━━━━━━━━━━━━━━━━\n"
                                    f"🗑 Removed dead : {removed}\n"
                                    f"💚 Remaining    : {len(_proxy_pool)}")


async def cmd_removeproxy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        if not _proxy_pool:
            await update.message.reply_text("📭 Pool empty.")
            return
        lines = ["📋 PROXY POOL", "━━━━━━━━━━━━━━━━━━━━━━"]
        for i, p in enumerate(_proxy_pool, start=1):
            mark = "💚" if p["alive"] else "💀"
            lines.append(f"{i:>2}. {mark} {proxy_display(p)}")
        lines.append(f"━━━━━━━━━━━━━━━━━━━━━━\n/removeproxy <index>")
        await update.message.reply_text("\n".join(lines))
        return

    arg = context.args[0].strip()
    async with _proxy_pool_lock:
        try:
            idx = int(arg) - 1
            if not (0 <= idx < len(_proxy_pool)):
                await update.message.reply_text(f"❌ Index out of range (1-{len(_proxy_pool)}).")
                return
            removed = _proxy_pool.pop(idx)
            _persist_proxies()
            await update.message.reply_text(f"🗑 Removed {proxy_display(removed)}\n📦 Remaining: {len(_proxy_pool)}")
            return
        except ValueError:
            pass
        for i, p in enumerate(_proxy_pool):
            if f"{p['host']}:{p['port']}" == arg or p.get("url") == arg:
                _proxy_pool.pop(i)
                _persist_proxies()
                await update.message.reply_text(f"🗑 Removed {arg}")
                return
    await update.message.reply_text("❌ Not found. Use /removeproxy with no args to see indices.")


async def cmd_proxylist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _proxy_pool:
        await update.message.reply_text("📭 Pool empty.\nAdd proxies with /addproxy or /addproxies.")
        return

    alive = sum(1 for p in _proxy_pool if p["alive"])
    breakdown: dict[str, int] = {}
    for p in _proxy_pool:
        k = (p["protocol"] or "?").upper()
        breakdown[k] = breakdown.get(k, 0) + 1

    lines = [
        f"🔄 PROXY POOL — {len(_proxy_pool)} total ({alive} alive)",
        f"📊 Protocols: " + ", ".join(f"{k}:{v}" for k, v in breakdown.items()),
        "━━━━━━━━━━━━━━━━━━━━━━",
    ]
    for i, p in enumerate(_proxy_pool[:50], start=1):
        mark = "💚" if p["alive"] else "💀"
        lat  = f"{int(p['latency'])}ms" if p.get("latency") else "—"
        lines.append(f"{i:>2}. {mark} {proxy_display(p)}  {lat}")
    if len(_proxy_pool) > 50:
        lines.append(f"… and {len(_proxy_pool) - 50} more")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("/proxycheck — re-test all  |  /proxyclean — drop dead")
    await update.message.reply_text("\n".join(lines))


async def cmd_testproxy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("🧪 TEST PROXY (no add)\nUsage: /testproxy <line>\n\n"
                                        "Accepted formats: same as /addproxy\n"
                                        "Auto-detects protocol.")
        return

    line = " ".join(context.args).strip()
    p    = parse_proxy_line(line)
    if not p:
        await update.message.reply_text("❌ Invalid format.")
        return

    wait = await update.message.reply_text(f"🧪 Testing {p['host']}:{p['port']}...\n"
                                           f"Probing SOCKS5 → SOCKS4 → HTTP → HTTPS")
    ok = await detect_proxy_protocol(p)
    if ok:
        msg = (f"✅ PROXY WORKS\n━━━━━━━━━━━━━━━━━━━━━━\n"
               f"🔌 Protocol : {p['protocol'].upper()} (auto-detected)\n"
               f"🌐 Host     : {p['host']}:{p['port']}\n"
               f"⏱ Latency  : {int(p['latency'])} ms\n"
               f"━━━━━━━━━━━━━━━━━━━━━━\n"
               f"Use /addproxy {line} to add it to the pool.")
    else:
        msg = (f"❌ PROXY FAILED\n━━━━━━━━━━━━━━━━━━━━━━\n"
               f"🌐 {p['host']}:{p['port']}\n"
               f"💬 No protocol responded\n"
               f"Don't add this proxy.")
    try:
        await context.bot.edit_message_text(chat_id=update.effective_chat.id, message_id=wait.message_id, text=msg)
    except Exception:
        await update.message.reply_text(msg)


# ─── FILE DETECTION ──────────────────────────────────────────────────────────
def _looks_like_url_list(lines: list) -> bool:
    non_empty = [l for l in lines if l.strip() and not l.startswith("#")]
    if not non_empty:
        return False
    return sum(1 for l in non_empty if l.strip().startswith("http")) / len(non_empty) >= 0.5


def _looks_like_proxy_list(lines: list) -> bool:
    non_empty = [l for l in lines if l.strip() and not l.startswith("#")]
    if not non_empty:
        return False
    proxy_count = sum(1 for l in non_empty if parse_proxy_line(l.split("#", 1)[0].strip()))
    return proxy_count / len(non_empty) >= 0.6


# ─── DOCUMENT / TEXT HANDLERS ────────────────────────────────────────────────

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    doc     = update.message.document

    if chat_id in active_jobs and not active_jobs[chat_id].done():
        await update.message.reply_text("⚠️ Job running! Use /stop first.")
        return
    if not doc.file_name.endswith(".txt"):
        await update.message.reply_text("❌ Send a .txt file.")
        return

    await update.message.reply_text("📥 Reading file...")
    try:
        content = await (await context.bot.get_file(doc.file_id)).download_as_bytearray()
        lines   = content.decode("utf-8", errors="replace").splitlines()

        if _looks_like_proxy_list(lines):
            await update.message.reply_text(f"🔄 PROXY LIST detected — {len(lines)} lines\n"
                                            f"🚀 Auto-detecting protocols & checking...")
            await _bulk_add_proxies(chat_id, lines, context)
            return

        if _looks_like_url_list(lines):
            raw_urls = [l.strip() for l in lines if l.strip() and not l.startswith("#")]
            if not raw_urls:
                await update.message.reply_text("❌ No URLs found.")
                return
            await update.message.reply_text(f"🧹 URL LIST — {len(raw_urls)} URLs")
            active_jobs[chat_id] = asyncio.create_task(run_url_clean_job(chat_id, raw_urls, context))
        else:
            dorks = [l.strip() for l in lines if l.strip() and not l.startswith("#")]
            if not dorks:
                await update.message.reply_text("❌ No dorks found.")
                return
            s = get_session(chat_id)
            if s.get("xtream"):
                await update.message.reply_text("🔥 Xtream storm incoming…")
                active_jobs[chat_id] = asyncio.create_task(run_xtream_job(chat_id, dorks, context))
            else:
                await update.message.reply_text(
                    f"✅ {len(dorks)} dorks | Pages: {', '.join(str(p) for p in s.get('pages', [1]))}\n🚀 Starting..."
                )
                active_jobs[chat_id] = asyncio.create_task(run_dork_job(chat_id, dorks, context))

    except Exception as exc:
        await update.message.reply_text(f"❌ Error: {exc}")


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    if chat_id in _awaiting_bulk_proxy:
        _awaiting_bulk_proxy.discard(chat_id)
        lines = update.message.text.splitlines()
        if not lines:
            await update.message.reply_text("❌ No lines received.")
            return
        await _bulk_add_proxies(chat_id, lines, context)
        return

    lines = [l.strip() for l in update.message.text.splitlines()
             if l.strip() and not l.startswith("#")]
    if len(lines) > 1:
        if chat_id in active_jobs and not active_jobs[chat_id].done():
            await update.message.reply_text("⚠️ Job running! /stop first.")
            return
        s = get_session(chat_id)
        if s.get("xtream"):
            await update.message.reply_text("🔥 Xtream storm incoming…")
            active_jobs[chat_id] = asyncio.create_task(run_xtream_job(chat_id, lines, context))
        else:
            await update.message.reply_text(
                f"✅ {len(lines)} dorks | Pages: {', '.join(str(p) for p in s.get('pages', [1]))}\n🚀 Starting..."
            )
            active_jobs[chat_id] = asyncio.create_task(run_dork_job(chat_id, lines, context))
    else:
        await update.message.reply_text(
            "Use /dork <q> or upload .txt\n"
            "/dorkcheck — validate dork  |  /mutate — variations\n"
            "/addproxies — bulk proxy import"
        )


# ─── CALLBACK HANDLER (FIXED) ────────────────────────────────────────────────

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    # Always answer immediately to stop the spinning progress
    try:
        await query.answer()
    except Exception as exc:
        log.warning(f"Callback answer failed: {exc}")

    data = query.data
    chat_id = query.message.chat_id
    sess = get_session(chat_id)

    if data.startswith("pg_"):
        cmd = data[3:]
        selected = list(sess.get("pages", [1]))
        if cmd == "all":   selected = list(range(1, 71))
        elif cmd == "clear": selected = []
        elif cmd == "confirm":
            sess["pages"] = selected or [1]
            try:
                await query.edit_message_text(f"✅ Pages: {', '.join(str(p) for p in sorted(sess['pages']))}")
            except Exception: pass
            return
        else:
            try:
                p = int(cmd)
                if p in selected: selected.remove(p)
                else: selected.append(p)
                selected = sorted(selected)
            except ValueError: pass
        sess["pages"] = selected
        try:
            await query.edit_message_text(
                f"📄 SELECT PAGES\nSelected: {', '.join(str(p) for p in selected) or 'none'}",
                reply_markup=page_keyboard(selected),
            )
        except Exception: pass
        return

    # Toggle buttons that actually change settings
    if data == "m_tor":
        global tor_enabled_users
        old = sess.get("tor", False)
        new = not old
        sess["tor"] = new
        if new and not old:
            tor_enabled_users += 1
            if tor_enabled_users == 1: start_tor_rotation()
        elif not new and old:
            tor_enabled_users = max(0, tor_enabled_users - 1)
            if tor_enabled_users == 0: stop_tor_rotation()
        try:
            await query.edit_message_text(
                f"🧅 Tor {'✅ ON' if new else '❌ OFF'}",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton(f"Toggle {'OFF' if new else 'ON'}", callback_data="m_tor")
                ]])
            )
        except Exception:
            await context.bot.send_message(chat_id, f"🧅 Tor → {'ON' if new else 'OFF'}")
        return

    if data == "m_filter":
        await query.edit_message_text(
            f"🛡 SQL Filter ≥ {sess.get('min_score', 30)}\nUse /filter N to change.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("↩ Back", callback_data="m_settings")
            ]])
        )
        return

    if data == "m_speed":
        old = sess.get("speed_mode", False)
        new = not old
        sess["speed_mode"] = new
        if new:
            sess["workers"] = min(sess.get("workers", WORKERS_PER_CHUNK), MAX_WORKERS_PER_CHUNK)
            sess["chunks"] = min(sess.get("chunks", N_CHUNKS), 8)
            sess["max_results"] = min(sess.get("max_results", MAX_RESULTS), 20)
        await query.edit_message_text(
            f"⚡ Speed mode {'ENABLED' if new else 'DISABLED'}"
        )
        return

    if data == "m_xtream":
        old = sess.get("xtream", False)
        new = not old
        sess["xtream"] = new
        if new:
            sess["engines"] = ["yahoo"]
            sess["speed_mode"] = True
        await query.edit_message_text(
            f"🔥 Xtream mode {'ENABLED 🚀' if new else 'DISABLED'} (Yahoo only)"
        )
        return

    # Static replies
    replies = {
        "m_bulk":     "📂 Upload .txt — URLs / dorks / proxies auto-detected.",
        "m_single":   "🔍 /dork inurl:login.php?id=",
        "m_clean":    "🧹 Upload .txt with URLs to clean.",
        "m_settings": "⚙️ Use /settings",
        "m_help":     "Use /start for full command list.",
    }
    if data in replies:
        await query.edit_message_text(replies[data])
    elif data == "m_pages":
        await query.edit_message_text(
            f"📄 SELECT PAGES (1–70)",
            reply_markup=page_keyboard(sess.get("pages", [1]))
        )


# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    if not BOT_TOKEN:
        log.critical("BOT_TOKEN not set!")
        raise SystemExit(1)

    app = Application.builder().token(BOT_TOKEN).build()

    # Core commands
    for name, handler in [
        ("start", cmd_start), ("help", cmd_settings),
        ("dork", cmd_dork), ("dorkcheck", cmd_dorkcheck), ("mutate", cmd_mutate),
        ("clean", cmd_clean), ("pages", cmd_pages), ("tor", cmd_tor),
        ("filter", cmd_filter), ("settings", cmd_settings),
        ("workers", cmd_workers), ("chunks", cmd_chunks),
        ("maxres", cmd_maxres), ("engine", cmd_engine),
        ("stop", cmd_stop), ("status", cmd_status),
        ("speed", cmd_speed), ("xtream", cmd_xtream),  # NEW
    ]:
        app.add_handler(CommandHandler(name, handler))

    # Proxy commands
    for name, handler in [
        ("addproxy",    cmd_addproxy),
        ("addproxies",  cmd_addproxies),
        ("removeproxy", cmd_removeproxy),
        ("proxylist",   cmd_proxylist),
        ("testproxy",   cmd_testproxy),
        ("proxycheck",  cmd_proxycheck),
        ("proxyclean",  cmd_proxyclean),
    ]:
        app.add_handler(CommandHandler(name, handler))

    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(CallbackQueryHandler(handle_callback))

    async def _on_startup(_app):
        start_proxy_health_monitor()
        log.info("Background proxy health monitor scheduled")
    app.post_init = _on_startup

    async def _shutdown():
        stop_tor_rotation()
        if _proxy_health_task and not _proxy_health_task.done():
            _proxy_health_task.cancel()
    app.shutdown_handler = _shutdown

    log.info("=" * 60)
    log.info("  DORK PARSER v19.0 — INTELLIGENT EDITION")
    log.info(f"  Proxies: {len(_proxy_pool)} loaded | PROXY_ENABLED={PROXY_ENABLED}")
    log.info(f"  Probe order: {PROXY_PROBE_ORDER}")
    log.info(f"  Health check every {PROXY_HEALTH_INTERVAL}s")
    log.info(f"  Engines: {', '.join(ENGINES)}")
    log.info(f"  TLS fingerprints: {TLS_IMPERSONATIONS}")
    log.info("  New: /dorkcheck /mutate /addproxies /speed /xtream")
    log.info("=" * 60)
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
