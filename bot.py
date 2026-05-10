#!/usr/bin/env python3

import asyncio
import json
import logging
import os
import random
import re
import time
from collections import deque
from io import BytesIO
from pathlib import Path
from urllib.parse import quote_plus, unquote

from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# =========================================================
# CONFIG
# =========================================================

BOT_TOKEN = "8661725916:AAHBm0_WMPGc_qqk5WuoAO65uuEkWr_VLq0"

MAX_THREADS = 3
DEFAULT_THREADS = 2

DEFAULT_PAGES = 1
MAX_PAGES = 5

REQUEST_TIMEOUT = 25

RESULTS_DIR = Path("results")
RESULTS_DIR.mkdir(exist_ok=True)

PROXY_FILE = Path("proxies.json")

MAX_RESULTS_PER_MESSAGE = 40

# safer request pacing
MIN_DELAY = 2
MAX_DELAY = 6

# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

# =========================================================
# ADVANCED TLS IMPERSONATION
# =========================================================

TLS_PROFILES = [
    {"name": "chrome124", "weight": 30, "http2": True},
    {"name": "chrome120", "weight": 25, "http2": True},
    {"name": "chrome116", "weight": 15, "http2": True},
    {"name": "safari17_0", "weight": 10, "http2": True},
    {"name": "firefox110", "weight": 10, "http2": True},
]

class TLSManager:
    def __init__(self):
        self.failures = {}
        self.cooldowns = {}

    def weighted_choice(self):
        now = time.time()
        available = []
        for profile in TLS_PROFILES:
            name = profile["name"]
            cooldown_until = self.cooldowns.get(name, 0)
            if now >= cooldown_until:
                available.append(profile)

        if not available:
            available = TLS_PROFILES

        weights = [x["weight"] for x in available]
        selected = random.choices(available, weights=weights, k=1)[0]
        return selected

    def mark_failure(self, tls_name):
        self.failures[tls_name] = self.failures.get(tls_name, 0) + 1
        if self.failures[tls_name] >= 3:
            self.cooldowns[tls_name] = time.time() + random.randint(120, 300)
            self.failures[tls_name] = 0

    def mark_success(self, tls_name):
        if tls_name in self.failures:
            self.failures[tls_name] = 0

# =========================================================
# USER AGENTS
# =========================================================

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:115.0) Gecko/20100101 Firefox/115.0",
]

BLOCKED_KEYWORDS = [
    "unusual traffic",
    "captcha",
    "automated queries",
    "detected unusual",
    "enable javascript",
]

# =========================================================
# SIMPLE RATE LIMITER
# =========================================================

class RateLimiter:
    def __init__(self, max_requests=10, period=60):
        self.max_requests = max_requests
        self.period = period
        self.requests = deque()

    async def wait(self):
        now = time.time()
        while self.requests and now - self.requests[0] > self.period:
            self.requests.popleft()

        if len(self.requests) >= self.max_requests:
            sleep_for = self.period - (now - self.requests[0])
            await asyncio.sleep(max(0, sleep_for))

        self.requests.append(time.time())

# =========================================================
# PROXY POOL
# =========================================================

class ProxyPool:
    def __init__(self):
        self.proxies = []
        self.failures = {}
        self.load()

    def load(self):
        if not PROXY_FILE.exists():
            return
        try:
            data = json.loads(PROXY_FILE.read_text())
            self.proxies = data.get("proxies", [])
            self.failures = data.get("failures", {})
        except Exception:
            pass

    def save(self):
        PROXY_FILE.write_text(json.dumps({
            "proxies": self.proxies,
            "failures": self.failures,
        }, indent=2))

    def add(self, proxy):
        proxy = proxy.strip()
        if proxy.startswith("http"):
            proxy_url = proxy
        else:
            parts = proxy.split(":")
            if len(parts) == 2:
                host, port = parts
                proxy_url = f"http://{host}:{port}"
            elif len(parts) == 4:
                host, port, user, password = parts
                proxy_url = f"http://{user}:{password}@{host}:{port}"
            else:
                return False

        if proxy_url not in self.proxies:
            self.proxies.append(proxy_url)
            self.failures[proxy_url] = 0
            self.save()
            return True
        return False

    def random(self):
        if not self.proxies:
            return None
        return random.choice(self.proxies)

    def fail(self, proxy):
        if not proxy:
            return
        self.failures[proxy] = self.failures.get(proxy, 0) + 1
        if self.failures[proxy] >= 3:
            if proxy in self.proxies:
                self.proxies.remove(proxy)
        self.save()

# =========================================================
# PROGRESS BAR DESIGN (added as requested)
# =========================================================

def progress_bar(percent: float, width: int = 20) -> str:
    """Return a string like '[████████░░░░░░░░] 45.0%'."""
    filled = int(width * percent / 100)
    bar = "█" * filled + "░" * (width - filled)
    return f"[{bar}] {percent:.1f}%"

# =========================================================
# GOOGLE PARSER
# =========================================================

class GoogleParser:
    @staticmethod
    def extract_urls(html):
        urls = set()
        patterns = [
            r'/url\\?q=(https?://[^&"]+)',
            r'href="(https?://[^"]+)"',
            r'"url":"(https:\\\\/\\\\/[^\"]+)"',
        ]
        for pattern in patterns:
            matches = re.findall(pattern, html)
            for match in matches:
                url = (
                    match
                    .replace("\\u003d", "=")
                    .replace("\\u0026", "&")
                    .replace("\\/", "/")
                )
                url = unquote(url)
                blocked = [
                    "google.com",
                    "accounts.google",
                    "webcache",
                    "/search?",
                ]
                if url.startswith("http") and not any(x in url for x in blocked):
                    urls.add(url)
        return list(urls)

# =========================================================
# DORK ENGINE
# =========================================================

class DorkEngine:
    limiter = RateLimiter(max_requests=8, period=60)

    @classmethod
    async def search(
        cls,
        dork,
        pages=1,
        proxy=None,
        tls="chrome124",
        retries=2,
        stop_event=None,
    ):
        found = set()
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Referer": "https://www.google.com/",
        }
        cookies = {"CONSENT": "YES+"}

        try:
            async with AsyncSession(
                impersonate=tls,
                timeout=REQUEST_TIMEOUT,
                headers=headers,
                cookies=cookies,
                proxy=proxy,
            ) as session:
                for page in range(pages):
                    if stop_event and stop_event.is_set():
                        break
                    await cls.limiter.wait()
                    start = page * 10
                    url = f"https://www.google.com/search?gbv=1&q={quote_plus(dork)}&start={start}"

                    success = False
                    for attempt in range(retries):
                        try:
                            response = await session.get(url)
                            html = response.text
                            if response.status_code != 200:
                                await asyncio.sleep(2)
                                continue
                            if any(x.lower() in html.lower() for x in BLOCKED_KEYWORDS):
                                logger.warning(f"Blocked: {dork}")
                                return list(found)
                            urls = GoogleParser.extract_urls(html)
                            found.update(urls)
                            success = True
                            break
                        except Exception as e:
                            logger.error(f"{dork} | {e}")
                            await asyncio.sleep(random.uniform(2, 5))

                    if not success:
                        break
                    await asyncio.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

        except Exception as e:
            logger.error(e)

        return list(found)

# =========================================================
# COMMANDS
# =========================================================

async def start(update: Update, context):
    text = (
        "🔍 Advanced Google Dork Bot\n\n"
        "/dork <query>\n"
        "/md – mass dorking (reply to .txt)\n"
        "/addproxy ip:port\n"
        "/threads 1-3\n"
        "/pages 1-5\n"
        "/stop"
    )
    await update.message.reply_text(text)

async def addproxy(update: Update, context):
    if not context.args:
        return
    proxy = " ".join(context.args)
    pool = context.bot_data["proxy_pool"]
    if pool.add(proxy):
        await update.message.reply_text("✅ Proxy added")
    else:
        await update.message.reply_text("❌ Invalid proxy format or duplicate")

# FIXED INDENTATION HERE
async def dork(update: Update, context):
    if not context.args:
        return

    dork_query = " ".join(context.args)
    msg = await update.message.reply_text("🔎 Searching...")

    pool = context.bot_data["proxy_pool"]
    proxy = pool.random()

    # ---------- FIX: correct indentation ----------
    tls_profile = context.bot_data["tls_manager"].weighted_choice()
    tls = tls_profile["name"]

    urls = await DorkEngine.search(
        dork=dork_query,
        pages=context.chat_data.get("pages", DEFAULT_PAGES),
        proxy=proxy,
        tls=tls,
    )

    if not urls:
        await msg.edit_text("❌ No URLs found")
        return

    output = f"Dork: {dork_query}\nURLs: {len(urls)}\n\n"
    output += "\n".join(urls[:MAX_RESULTS_PER_MESSAGE])

    if len(output) > 4000:
        bio = BytesIO("\n".join(urls).encode())
        await update.message.reply_document(
            document=bio,
            filename="results.txt",
        )
    else:
        await msg.edit_text(output)

async def stop(update: Update, context):
    stop_event = context.chat_data.get("stop_event")
    if stop_event:
        stop_event.set()
    await update.message.reply_text("⏹️ Stopping")

async def set_threads(update: Update, context):
    if not context.args:
        return
    try:
        num = int(context.args[0])
        if num < 1 or num > MAX_THREADS:
            return
        context.chat_data["threads"] = num
        await update.message.reply_text(f"Threads = {num}")
    except:
        pass

async def set_pages(update: Update, context):
    if not context.args:
        return
    try:
        num = int(context.args[0])
        if num < 1 or num > MAX_PAGES:
            return
        context.chat_data["pages"] = num
        await update.message.reply_text(f"Pages = {num}")
    except:
        pass

# FIXED INDENTATION HERE (and added progress bar)
async def md(update: Update, context):
    if (
        not update.message.reply_to_message
        or
        not update.message.reply_to_message.document
    ):
        await update.message.reply_text("Reply to a .txt file with /md")
        return

    doc = update.message.reply_to_message.document
    if not doc.file_name.endswith(".txt"):
        return

    file = await context.bot.get_file(doc.file_id)
    temp_file = "dorks.txt"
    await file.download_to_drive(temp_file)

    with open(temp_file, "r", encoding="utf-8") as f:
        dorks = [line.strip() for line in f if line.strip()]

    os.remove(temp_file)
    if not dorks:
        return

    stop_event = asyncio.Event()
    context.chat_data["stop_event"] = stop_event

    # Progress message that will be updated live
    progress = await update.message.reply_text("🚀 Starting mass dorking...")

    sem = asyncio.Semaphore(
        context.chat_data.get("threads", DEFAULT_THREADS)
    )
    results = []
    completed = 0  # mutable counter, safe in async due to single updater

    async def worker(dork_query):
        nonlocal completed
        async with sem:
            if stop_event.is_set():
                return

            pool = context.bot_data["proxy_pool"]
            proxy = pool.random()

            # ---------- FIX: correct indentation ----------
            tls_profile = context.bot_data["tls_manager"].weighted_choice()
            tls = tls_profile["name"]

            urls = await DorkEngine.search(
                dork=dork_query,
                pages=context.chat_data.get("pages", DEFAULT_PAGES),
                proxy=proxy,
                tls=tls,
                stop_event=stop_event,
            )

            completed += 1
            results.append((dork_query, urls))

    async def updater():
        while not stop_event.is_set():
            percent = (completed / len(dorks)) * 100 if dorks else 0
            bar = progress_bar(percent)                     # <-- now using the designed bar
            try:
                await progress.edit_text(
                    f"{bar}\n"
                    f"✅ {completed}/{len(dorks)} dorks completed"
                )
            except Exception:
                pass
            await asyncio.sleep(2)

    updater_task = asyncio.create_task(updater())

    tasks = [asyncio.create_task(worker(dork)) for dork in dorks]
    await asyncio.gather(*tasks)

    stop_event.set()
    updater_task.cancel()

    # Final progress
    percent = (completed / len(dorks)) * 100 if dorks else 0
    bar = progress_bar(percent)
    await progress.edit_text(f"{bar}\n✅ Finished: {completed}/{len(dorks)} dorks processed")

    # Save and send results
    final = []
    for dork_query, urls in results:
        final.append(f"### {dork_query}")
        final.extend(urls)
        final.append("")
    output_file = RESULTS_DIR / f"results_{int(time.time())}.txt"
    output_file.write_text("\n".join(final), encoding="utf-8")

    # Safely send the file (using with open to ensure closure)
    with open(output_file, "rb") as f:
        await update.message.reply_document(
            document=f,
            filename=output_file.name,
        )

# =========================================================
# MAIN
# =========================================================

def main():
    if BOT_TOKEN == "PUT_YOUR_TOKEN":
        print("SET TOKEN")
        return

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .concurrent_updates(True)
        .build()
    )

    app.bot_data["proxy_pool"] = ProxyPool()
    app.bot_data["tls_manager"] = TLSManager()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("dork", dork))
    app.add_handler(CommandHandler("md", md))
    app.add_handler(CommandHandler("stop", stop))
    app.add_handler(CommandHandler("addproxy", addproxy))
    app.add_handler(CommandHandler("threads", set_threads))
    app.add_handler(CommandHandler("pages", set_pages))
    app.add_handler(MessageHandler(filters.COMMAND, start))

    print("BOT RUNNING")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
