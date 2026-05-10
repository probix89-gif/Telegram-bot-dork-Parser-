#!/usr/bin/env python3

"""
Advanced Google Dork Parser Telegram Bot
========================================
Features:
- Google dork searching
- Mass dorking
- Proxy rotation
- TLS fingerprint rotation
- Progress bar
- Multi-threaded async workers
- Stop support
- Improved Google parser
- Google basic HTML mode support
"""

import asyncio
import json
import logging
import random
import re
import os

from io import BytesIO
from pathlib import Path
from typing import List, Optional, Dict
from urllib.parse import quote_plus, unquote

from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession

from telegram import Update
from telegram.constants import ParseMode
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

BOT_TOKEN = "PASTE_YOUR_BOT_TOKEN_HERE"

DEFAULT_THREADS = 2
DEFAULT_PAGES = 1

PROGRESS_BAR_WIDTH = 20
MAX_RESULTS_PER_MESSAGE = 40

PROXIES_FILE = Path("proxies.json")

# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# =========================================================
# TLS FINGERPRINTS
# =========================================================

TLS_FINGERPRINTS = [
    "chrome120",
    "chrome124",
    "chrome110",
    "safari17_0",
    "firefox110",
]

# =========================================================
# PROGRESS BAR
# =========================================================

def progress_bar(percent: float, width: int = PROGRESS_BAR_WIDTH):

    filled = int(width * percent / 100)

    return (
        "[" +
        "█" * filled +
        "░" * (width - filled) +
        f"] {percent:.1f}%"
    )

# =========================================================
# PROXY POOL
# =========================================================

class ProxyPool:

    def __init__(self, filepath=PROXIES_FILE):

        self.filepath = filepath
        self.proxies = []
        self.fail_count = {}

        self.load()

    def load(self):

        if self.filepath.exists():

            try:

                data = json.loads(self.filepath.read_text())

                self.proxies = data.get("proxies", [])
                self.fail_count = data.get("fails", {})

            except Exception:
                pass

    def save(self):

        self.filepath.write_text(
            json.dumps({
                "proxies": self.proxies,
                "fails": self.fail_count
            }, indent=2)
        )

    def add(self, proxy_str: str):

        proxy_str = proxy_str.strip()

        if not proxy_str:
            return False

        if proxy_str.startswith("http"):
            proxy_url = proxy_str

        else:

            parts = proxy_str.split(":")

            if len(parts) == 2:

                host, port = parts
                proxy_url = f"http://{host}:{port}"

            elif len(parts) == 4:

                host, port, user, pw = parts
                proxy_url = f"http://{user}:{pw}@{host}:{port}"

            else:
                return False

        if proxy_url not in self.proxies:

            self.proxies.append(proxy_url)
            self.fail_count[proxy_url] = 0

            self.save()

            return True

        return False

    def get_random(self):

        if not self.proxies:
            return None

        return random.choice(self.proxies)

    def mark_fail(self, proxy):

        self.fail_count[proxy] = self.fail_count.get(proxy, 0) + 1

        if self.fail_count[proxy] >= 3:

            if proxy in self.proxies:
                self.proxies.remove(proxy)

        self.save()

# =========================================================
# GOOGLE DORKER
# =========================================================

class GoogleDorker:

    USER_AGENTS = [

        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",

        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Version/17.0 Safari/605.1.15",

        "Mozilla/5.0 (X11; Linux x86_64; rv:115.0) Gecko/20100101 Firefox/115.0",
    ]

    @staticmethod
    def _extract_urls(html: str):

        urls = set()

        patterns = [

            r'/url\?q=(https?://[^&]+)',

            r'"url":"(https:\\/\\/[^"]+)"',
        ]

        for pattern in patterns:

            matches = re.findall(pattern, html)

            for match in matches:

                url = (
                    match.replace("\\u003d", "=")
                    .replace("\\u0026", "&")
                    .replace("\\/", "/")
                )

                url = unquote(url)

                blocked = [

                    "google.com",
                    "webcache",
                    "/search?",
                    "accounts.google",
                ]

                if url.startswith("http") and not any(x in url for x in blocked):

                    urls.add(url)

        return list(urls)

    @classmethod
    async def search(
        cls,
        dork: str,
        pages: int = 1,
        proxy: Optional[str] = None,
        tls_fingerprint: str = "chrome120",
        stop_event: Optional[asyncio.Event] = None,
    ) -> List[str]:

        all_urls = set()

        blocked_keywords = [

            "unusual traffic",
            "captcha",
            "detected unusual",
            "sorry/index",
            "enable javascript",
        ]

        for page in range(pages):

            if stop_event and stop_event.is_set():
                break

            start = page * 10

            url = (
                "https://www.google.com/search"
                f"?gbv=1&q={quote_plus(dork)}&start={start}"
            )

            headers = {

                "User-Agent": random.choice(cls.USER_AGENTS),

                "Accept": "*/*",

                "Accept-Language": "en-US,en;q=0.9",

                "Cache-Control": "no-cache",

                "Pragma": "no-cache",

                "Referer": "https://www.google.com/",
            }

            cookies = {
                "CONSENT": "YES+"
            }

            try:

                async with AsyncSession(
                    impersonate=tls_fingerprint,
                    headers=headers,
                    cookies=cookies,
                    timeout=30,
                    proxy=proxy,
                ) as session:

                    response = await session.get(url)

                    html = response.text

                    if response.status_code != 200:

                        logger.warning(
                            f"HTTP {response.status_code} | {dork}"
                        )

                        continue

                    if any(
                        k.lower() in html.lower()
                        for k in blocked_keywords
                    ):

                        logger.warning(
                            f"Google blocked request: {dork}"
                        )

                        break

                    urls = cls._extract_urls(html)

                    all_urls.update(urls)

                    logger.info(
                        f"{dork} -> {len(urls)} URLs"
                    )

            except Exception as e:

                logger.error(f"{dork} | {e}")

            if page < pages - 1:

                await asyncio.sleep(
                    random.uniform(2, 5)
                )

        return list(all_urls)

# =========================================================
# COMMANDS
# =========================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):

    text = (
        "🔍 Advanced Google Dork Bot\n\n"

        "/dork <query>\n"
        "/md (reply to txt)\n"
        "/addproxy ip:port\n"
        "/threads <num>\n"
        "/pages <num>\n"
        "/stop"
    )

    await update.message.reply_text(text)

# =========================================================

async def dork_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if not context.args:

        await update.message.reply_text(
            "Usage: /dork <query>"
        )

        return

    dork = " ".join(context.args)

    msg = await update.message.reply_text(
        f"🔎 Searching:\n`{dork}`",
        parse_mode=ParseMode.MARKDOWN
    )

    pool = context.bot_data["proxy_pool"]

    proxy = pool.get_random()

    tls = random.choice(TLS_FINGERPRINTS)

    pages = context.chat_data.get(
        "pages",
        DEFAULT_PAGES
    )

    urls = await GoogleDorker.search(
        dork=dork,
        pages=pages,
        proxy=proxy,
        tls_fingerprint=tls,
    )

    if not urls:

        await msg.edit_text(
            "❌ No URLs found."
        )

        return

    result = (
        f"*Dork:* `{dork}`\n"
        f"*URLs:* {len(urls)}\n\n"
    )

    result += "\n".join(
        urls[:MAX_RESULTS_PER_MESSAGE]
    )

    if len(result) > 4000:

        bio = BytesIO(
            "\n".join(urls).encode()
        )

        await update.message.reply_document(
            document=bio,
            filename="results.txt",
            caption=f"{len(urls)} URLs found"
        )

    else:

        await msg.edit_text(
            result,
            parse_mode=ParseMode.MARKDOWN,
            disable_web_page_preview=True
        )

# =========================================================

async def add_proxy(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if not context.args:

        await update.message.reply_text(
            "Usage:\n/addproxy ip:port"
        )

        return

    proxy = " ".join(context.args)

    pool = context.bot_data["proxy_pool"]

    if pool.add(proxy):

        await update.message.reply_text(
            "✅ Proxy added"
        )

    else:

        await update.message.reply_text(
            "❌ Invalid proxy"
        )

# =========================================================

async def set_threads(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if not context.args:

        return

    try:

        num = int(context.args[0])

        context.chat_data["threads"] = num

        await update.message.reply_text(
            f"✅ Threads = {num}"
        )

    except:
        pass

# =========================================================

async def set_pages(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if not context.args:

        return

    try:

        num = int(context.args[0])

        if num < 1:
            return

        context.chat_data["pages"] = num

        await update.message.reply_text(
            f"✅ Pages = {num}"
        )

    except:
        pass

# =========================================================

async def stop_dork(update: Update, context: ContextTypes.DEFAULT_TYPE):

    stop_event = context.chat_data.get(
        "stop_event"
    )

    if stop_event:

        stop_event.set()

        await update.message.reply_text(
            "⏹️ Stopping..."
        )

# =========================================================

async def mass_dork(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if not update.message.reply_to_message:

        await update.message.reply_text(
            "Reply to txt file."
        )

        return

    document = update.message.reply_to_message.document

    if not document:

        await update.message.reply_text(
            "Reply to txt file."
        )

        return

    if not document.file_name.endswith(".txt"):

        await update.message.reply_text(
            "Only txt supported."
        )

        return

    file = await context.bot.get_file(
        document.file_id
    )

    temp_file = "dorks.txt"

    await file.download_to_drive(temp_file)

    with open(temp_file, "r", encoding="utf-8") as f:

        dorks = [
            x.strip()
            for x in f
            if x.strip()
        ]

    os.remove(temp_file)

    if not dorks:

        await update.message.reply_text(
            "No dorks found."
        )

        return

    stop_event = asyncio.Event()

    context.chat_data["stop_event"] = stop_event

    progress_msg = await update.message.reply_text(
        "🚀 Starting..."
    )

    total = len(dorks)

    completed = 0

    total_urls = 0

    results = []

    threads = context.chat_data.get(
        "threads",
        DEFAULT_THREADS
    )

    sem = asyncio.Semaphore(threads)

    async def worker(dork):

        nonlocal completed
        nonlocal total_urls

        async with sem:

            if stop_event.is_set():
                return

            pool = context.bot_data["proxy_pool"]

            proxy = pool.get_random()

            tls = random.choice(
                TLS_FINGERPRINTS
            )

            urls = await GoogleDorker.search(
                dork=dork,
                pages=context.chat_data.get(
                    "pages",
                    DEFAULT_PAGES
                ),
                proxy=proxy,
                tls_fingerprint=tls,
                stop_event=stop_event,
            )

            completed += 1

            total_urls += len(urls)

            results.append(
                (
                    dork,
                    urls
                )
            )

    async def updater():

        while not stop_event.is_set():

            percent = (
                completed / total
            ) * 100

            text = (
                "🔍 Mass Dorking\n\n"
                f"{progress_bar(percent)}\n\n"
                f"✅ {completed}/{total}\n"
                f"🔗 URLs: {total_urls}"
            )

            try:

                await progress_msg.edit_text(
                    text
                )

            except:
                pass

            await asyncio.sleep(2)

    updater_task = asyncio.create_task(
        updater()
    )

    tasks = [
        asyncio.create_task(worker(d))
        for d in dorks
    ]

    await asyncio.gather(*tasks)

    stop_event.set()

    updater_task.cancel()

    final = []

    for dork, urls in results:

        final.append(
            f"### {dork}\n"
        )

        final.extend(urls)

        final.append("\n")

    if final:

        bio = BytesIO(
            "\n".join(final).encode()
        )

        await update.message.reply_document(
            document=bio,
            filename="mass_results.txt",
            caption=f"{total_urls} URLs found"
        )

    else:

        await update.message.reply_text(
            "❌ No URLs found."
        )

# =========================================================
# MAIN
# =========================================================

def main():

    if BOT_TOKEN == "PASTE_YOUR_BOT_TOKEN_HERE":

        print(
            "\nSET YOUR BOT TOKEN FIRST\n"
        )

        return

    proxy_pool = ProxyPool()

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .concurrent_updates(True)
        .build()
    )

    app.bot_data["proxy_pool"] = proxy_pool

    app.add_handler(
        CommandHandler("start", start)
    )

    app.add_handler(
        CommandHandler("dork", dork_cmd)
    )

    app.add_handler(
        CommandHandler("md", mass_dork)
    )

    app.add_handler(
        CommandHandler("stop", stop_dork)
    )

    app.add_handler(
        CommandHandler("addproxy", add_proxy)
    )

    app.add_handler(
        CommandHandler("threads", set_threads)
    )

    app.add_handler(
        CommandHandler("pages", set_pages)
    )

    app.add_handler(
        MessageHandler(
            filters.COMMAND,
            start
        )
    )

    print("\nBOT RUNNING...\n")

    app.run_polling(
        drop_pending_updates=True
    )

# =========================================================

if __name__ == "__main__":

    main()
