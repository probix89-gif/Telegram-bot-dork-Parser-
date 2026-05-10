#!/usr/bin/env python3
"""
Google Dork Parser Telegram Bot
================================
A professional bot for running Google dorks with proxy rotation,
TLS fingerprint impersonation, multi‑threaded concurrency, and a live progress bar.

Usage:
    export TELEGRAM_BOT_TOKEN="your_bot_token"
    python bot.py
"""

import asyncio
import json
import logging
import random
import re
import os
from pathlib import Path
from typing import List, Optional, Dict, Any
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
from telegram.constants import ParseMode

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEFAULT_THREADS = 5
DEFAULT_PAGES = 1
MAX_RESULTS_PER_MESSAGE = 50
PROGRESS_BAR_WIDTH = 20
STOP_POLL_INTERVAL = 0.3  # seconds between stop checks

# TLS fingerprints for rotation (supported by curl_cffi)
TLS_FINGERPRINTS = [
    "chrome110",
    "chrome116",
    "chrome120",
    "chrome124",
    "safari15_5",
    "safari17_0",
    "edge101",
    "firefox110",
]

# Files for persistent storage
CONFIG_FILE = Path("config.json")
PROXIES_FILE = Path("proxies.json")

# ---------------------------------------------------------------------------
# Helper: Progress bar string
# ---------------------------------------------------------------------------
def progress_bar(percent: float, width: int = PROGRESS_BAR_WIDTH) -> str:
    """Return a text progress bar like '[████████░░░░] 60%'."""
    filled = int(width * percent / 100)
    bar = "█" * filled + "░" * (width - filled)
    return f"[{bar}] {percent:.1f}%"

# ---------------------------------------------------------------------------
# Proxy Pool Manager
# ---------------------------------------------------------------------------
class ProxyPool:
    """Load, save and rotate proxy list."""

    def __init__(self, filepath: Path = PROXIES_FILE):
        self.filepath = filepath
        self.proxies: List[str] = []
        self.fail_count: Dict[str, int] = {}
        self.load()

    def load(self):
        if self.filepath.exists():
            try:
                data = json.loads(self.filepath.read_text())
                self.proxies = data.get("proxies", [])
                self.fail_count = data.get("fails", {})
                logger.info(f"Loaded {len(self.proxies)} proxies.")
            except Exception:
                self.proxies = []

    def save(self):
        self.filepath.write_text(
            json.dumps({"proxies": self.proxies, "fails": self.fail_count}, indent=2)
        )

    def add(self, proxy_str: str) -> bool:
        """Add a proxy. Format: 'ip:port' or 'ip:port:user:pass'. Returns True on success."""
        # Clean and validate
        proxy_str = proxy_str.strip()
        if not proxy_str:
            return False
        # Convert to http:// format for curl_cffi
        if re.match(r'^\d+\.\d+\.\d+\.\d+:\d+', proxy_str):
            # ip:port
            parts = proxy_str.split(':')
            if len(parts) == 2:
                host, port = parts
                proxy_url = f"http://{host}:{port}"
            elif len(parts) == 4:
                host, port, user, pw = parts
                proxy_url = f"http://{user}:{pw}@{host}:{port}"
            else:
                return False
        else:
            # Already a URL
            proxy_url = proxy_str if proxy_str.startswith("http") else f"http://{proxy_str}"

        if proxy_url not in self.proxies:
            self.proxies.append(proxy_url)
            self.fail_count[proxy_url] = 0
            self.save()
            return True
        return False

    def get_random(self) -> Optional[str]:
        """Return a random proxy URL, or None if pool is empty."""
        return random.choice(self.proxies) if self.proxies else None

    def mark_fail(self, proxy_url: str, max_fails: int = 3):
        """Increment fail count and remove proxy if threshold exceeded."""
        self.fail_count[proxy_url] = self.fail_count.get(proxy_url, 0) + 1
        if self.fail_count[proxy_url] >= max_fails:
            self.remove(proxy_url)
        self.save()

    def remove(self, proxy_url: str):
        if proxy_url in self.proxies:
            self.proxies.remove(proxy_url)
            self.fail_count.pop(proxy_url, None)
            self.save()

# ---------------------------------------------------------------------------
# Google Dorker
# ---------------------------------------------------------------------------
class GoogleDorker:
    """Performs Google searches with proxy and TLS fingerprint rotation."""

    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/115.0",
    ]

    @staticmethod
    def _extract_urls(html: str) -> list:
        """Parse Google search result URLs from HTML."""
        urls = []
        soup = BeautifulSoup(html, "lxml")
        # Modern Google result blocks often contain <a> with href="/url?q=..."
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith("/url?q="):
                # Some URLs are truncated; grab everything before '&sa='
                match = re.search(r"/url\?q=(?P<url>[^&]+)", href)
                if match:
                    raw_url = unquote(match.group("url"))
                    if raw_url.startswith("http"):
                        urls.append(raw_url)
        return urls

    @classmethod
    async def search(
        cls,
        dork: str,
        pages: int = 1,
        proxy: Optional[str] = None,
        tls_fingerprint: str = "chrome120",
        stop_event: Optional[asyncio.Event] = None,
    ) -> List[str]:
        """
        Execute a Google dork and return a list of unique result URLs.
        Each page retrieves ~10 results.
        """
        all_urls = set()
        for page in range(pages):
            if stop_event and stop_event.is_set():
                break

            start = page * 10
            url = f"https://www.google.com/search?q={quote_plus(dork)}&start={start}&hl=en"

            headers = {
                "User-Agent": random.choice(cls.USER_AGENTS),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate, br",
            }

            session_kwargs = {
                "impersonate": tls_fingerprint,
                "headers": headers,
                "timeout": 30,
                "proxy": proxy,
            }

            try:
                async with AsyncSession(**session_kwargs) as session:
                    resp = await session.get(url)
                    if resp.status_code != 200:
                        logger.warning(
                            f"HTTP {resp.status_code} for dork '{dork}' (page {page+1})"
                        )
                        # If blocked, we may get a 429 or 503; stop further pages
                        if "unusual traffic" in resp.text.lower() or resp.status_code == 429:
                            logger.error("CAPTCHA / block detected.")
                            break
                        continue

                    page_urls = cls._extract_urls(resp.text)
                    all_urls.update(page_urls)

            except Exception as e:
                logger.error(f"Request failed for dork '{dork}' page {page+1}: {e}")
                # Mark proxy as failed if one was used
                if proxy:
                    # (will be handled by caller, but we don't have pool here)
                    pass
                continue

            # Polite delay between pages (except last)
            if page < pages - 1:
                await asyncio.sleep(random.uniform(1.5, 3.0))

        return list(all_urls)

# ---------------------------------------------------------------------------
# Bot Command Handlers
# ---------------------------------------------------------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send a help message with all commands."""
    help_text = (
        "🔍 *Google Dork Parser Bot*\n\n"
        "Commands:\n"
        "  /dork <query> – Search a single dork\n"
        "  /md – Mass dorking (reply to a .txt file with one dork per line)\n"
        "  /addproxy <ip:port> or <ip:port:user:pass> – Add proxy\n"
        "  /threads <number> – Set concurrent tasks (default 5)\n"
        "  /pages <number> – Set pages per dork (default 1)\n"
        "  /stop – Stop current mass dorking operation\n\n"
        "The bot uses rotating TLS fingerprints and proxies to avoid blocking."
    )
    await update.message.reply_text(help_text, parse_mode=ParseMode.MARKDOWN)

async def dork_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /dork <query>."""
    if not context.args:
        await update.message.reply_text("Usage: /dork <query>")
        return

    dork = " ".join(context.args)
    msg = await update.message.reply_text(f"🔎 Searching: `{dork}` ...", parse_mode=ParseMode.MARKDOWN)

    pool: ProxyPool = context.bot_data.get("proxy_pool")
    proxy = pool.get_random() if pool else None
    pages = context.chat_data.get("pages", DEFAULT_PAGES)

    # Choose a random TLS fingerprint
    tls = random.choice(TLS_FINGERPRINTS)

    try:
        urls = await GoogleDorker.search(dork, pages=pages, proxy=proxy, tls_fingerprint=tls)
    except Exception as e:
        await msg.edit_text(f"❌ Error: {e}")
        return

    if not urls:
        await msg.edit_text("No results found.")
        return

    # Format output
    result_text = f"*Dork:* `{dork}`\n*Results:* {len(urls)}\n\n"
    result_text += "\n".join(urls[:MAX_RESULTS_PER_MESSAGE])
    if len(urls) > MAX_RESULTS_PER_MESSAGE:
        result_text += f"\n... and {len(urls)-MAX_RESULTS_PER_MESSAGE} more."

    # Telegram messages have a 4096 char limit, so we send as a file if too long
    if len(result_text) > 4000:
        # Send as document
        from io import BytesIO
        bio = BytesIO()
        bio.write("\n".join(urls).encode("utf-8"))
        bio.seek(0)
        await update.message.reply_document(
            document=bio,
            filename=f"dork_results_{dork[:30]}.txt",
            caption=f"Dork: {dork} – {len(urls)} results"
        )
    else:
        await msg.edit_text(result_text, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)

async def mass_dork(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Mass dorking: reply to a .txt file with one dork per line."""
    chat_id = update.effective_chat.id
    chat_data = context.chat_data

    # Check if already running
    if chat_data.get("mass_running"):
        await update.message.reply_text("⚠️ A mass dorking operation is already running in this chat. Use /stop first.")
        return

    # Must reply to a document
    if not update.message.reply_to_message or not update.message.reply_to_message.document:
        await update.message.reply_text("ℹ️ Please reply to a `.txt` file containing dorks (one per line) with the /md command.")
        return

    doc = update.message.reply_to_message.document
    if not doc.file_name.endswith(".txt"):
        await update.message.reply_text("Only .txt files are supported.")
        return

    # Download file
    file = await context.bot.get_file(doc.file_id)
    file_path = f"/tmp/dorks_{chat_id}.txt"
    await file.download_to_drive(file_path)

    # Read dorks
    with open(file_path, "r", encoding="utf-8") as f:
        dorks = [line.strip() for line in f if line.strip()]

    os.remove(file_path)

    if not dorks:
        await update.message.reply_text("The file is empty or contains no valid dorks.")
        return

    pool: ProxyPool = context.bot_data.get("proxy_pool")
    pages = chat_data.get("pages", DEFAULT_PAGES)
    threads = chat_data.get("threads", DEFAULT_THREADS)

    # Initialize operation state
    stop_event = asyncio.Event()
    chat_data["mass_running"] = True
    chat_data["stop_event"] = stop_event
    chat_data["total_dorks"] = len(dorks)
    chat_data["completed_dorks"] = 0
    chat_data["total_results"] = 0
    results_collector = []  # list of (dork, urls)

    # Send progress message that we will update
    progress_msg = await update.message.reply_text("🚀 Starting mass dorking...")
    chat_data["progress_msg_id"] = progress_msg.message_id
    chat_data["progress_chat_id"] = chat_id

    # Concurrency limiter
    sem = asyncio.Semaphore(threads)

    async def process_dork(dork: str, tls: str, proxy: Optional[str]):
        nonlocal results_collector
        try:
            async with sem:
                # Check stop before and during pages
                if stop_event.is_set():
                    return
                urls = await GoogleDorker.search(
                    dork,
                    pages=pages,
                    proxy=proxy,
                    tls_fingerprint=tls,
                    stop_event=stop_event,
                )

                # Update global counters (with lock to be safe)
                async with context.bot_data["mass_lock"]:
                    chat_data["completed_dorks"] += 1
                    chat_data["total_results"] += len(urls)
                    results_collector.append((dork, urls))

        except Exception as e:
            logger.error(f"Dork '{dork}' failed: {e}")

    # Start progress updater task
    async def progress_updater():
        """Edit the progress message every 2 seconds."""
        while not stop_event.is_set() and chat_data.get("mass_running"):
            completed = chat_data.get("completed_dorks", 0)
            total = chat_data.get("total_dorks", 1)
            perc = (completed / total) * 100 if total else 0
            bar_str = progress_bar(perc)
            found_total = chat_data.get("total_results", 0)
            text = (
                f"🔍 Mass Dorking in progress...\n"
                f"{bar_str}\n"
                f"✅ Completed: {completed}/{total} dorks\n"
                f"🔗 URLs found: {found_total}"
            )
            try:
                await context.bot.edit_message_text(
                    chat_id=chat_data["progress_chat_id"],
                    message_id=chat_data["progress_msg_id"],
                    text=text,
                )
            except Exception:
                pass
            await asyncio.sleep(2)

    # Launch progress updater and dork tasks
    updater_task = asyncio.create_task(progress_updater())

    # Build tasks: each dork gets a random fingerprint and proxy
    worker_tasks = []
    for dork in dorks:
        tls = random.choice(TLS_FINGERPRINTS)
        proxy = pool.get_random() if pool else None
        worker_tasks.append(asyncio.create_task(process_dork(dork, tls, proxy)))

    try:
        await asyncio.gather(*worker_tasks)
    except asyncio.CancelledError:
        pass

    # Operation finished (naturally or via stop)
    stop_event.set()  # signal updater to stop
    chat_data["mass_running"] = False
    await asyncio.sleep(1)  # give updater a moment to finish
    updater_task.cancel()

    # Final progress message
    completed = chat_data.get("completed_dorks", 0)
    total = chat_data.get("total_dorks", 1)
    perc = (completed / total) * 100 if total else 0
    final_text = (
        f"🏁 Mass Dorking Finished\n"
        f"{progress_bar(perc)}\n"
        f"✅ Completed: {completed}/{total} dorks\n"
        f"🔗 Total URLs found: {chat_data.get('total_results', 0)}"
    )
    await context.bot.edit_message_text(
        chat_id=chat_data["progress_chat_id"],
        message_id=chat_data["progress_msg_id"],
        text=final_text,
    )

    # Combine results and send as file
    all_results = []
    for dork, urls in results_collector:
        all_results.append(f"### Dork: {dork}\n" + "\n".join(urls) + "\n")
    if all_results:
        result_content = "\n".join(all_results)
        from io import BytesIO
        bio = BytesIO(result_content.encode("utf-8"))
        await update.message.reply_document(
            document=bio,
            filename=f"mass_dork_results_{chat_id}.txt",
            caption=f"Mass dorking – {len(dorks)} dorks, {chat_data.get('total_results',0)} URLs"
        )
    else:
        await update.message.reply_text("No results were found for any dork.")

    # Cleanup
    chat_data.pop("mass_running", None)
    chat_data.pop("stop_event", None)

async def stop_dork(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Stop the running mass dorking operation in this chat."""
    chat_data = context.chat_data
    if not chat_data.get("mass_running"):
        await update.message.reply_text("No mass dorking operation is currently running.")
        return

    stop_event: asyncio.Event = chat_data.get("stop_event")
    if stop_event:
        stop_event.set()
    await update.message.reply_text("⏹️ Stopping the mass dorking operation...")

async def add_proxy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add a proxy to the pool."""
    if not context.args:
        await update.message.reply_text("Usage: /addproxy <ip:port> or <ip:port:user:pass>")
        return

    proxy_str = " ".join(context.args)  # in case of spaces?
    pool: ProxyPool = context.bot_data.get("proxy_pool")
    if not pool:
        await update.message.reply_text("Proxy pool not initialised, please restart the bot.")
        return
    if pool.add(proxy_str):
        await update.message.reply_text(f"✅ Proxy added: {proxy_str}")
    else:
        await update.message.reply_text(f"❌ Invalid proxy format or duplicate.")

async def set_threads(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set the number of concurrent dorking threads."""
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Usage: /threads <number>")
        return
    num = int(context.args[0])
    if num < 1:
        await update.message.reply_text("Thread count must be at least 1.")
        return
    context.chat_data["threads"] = num
    await update.message.reply_text(f"🔧 Threads set to {num} (will take effect on next /md).")

async def set_pages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set the number of Google result pages per dork."""
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Usage: /pages <number>")
        return
    num = int(context.args[0])
    if num < 1 or num > 10:
        await update.message.reply_text("Pages must be between 1 and 10.")
        return
    context.chat_data["pages"] = num
    await update.message.reply_text(f"📄 Pages per dork set to {num}.")

# ---------------------------------------------------------------------------
# Bot Initialisation
# ---------------------------------------------------------------------------
def main():
    token = os.getenv("8276628633:AAHGwYFa8dUnRrkWDDBtJ4jY2pltPTDwwog")
    if not token:
        logger.error("TELEGRAM_BOT_TOKEN environment variable not set.")
        return

    # Load proxy pool
    proxy_pool = ProxyPool(PROXIES_FILE)
    # Load global defaults (optional: from config)
    # We'll also store a Lock for counter updates
    mass_lock = asyncio.Lock()

    # Build application
    app = Application.builder().token(token).build()

    # Store shared objects
    app.bot_data["proxy_pool"] = proxy_pool
    app.bot_data["mass_lock"] = mass_lock

    # Register handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("dork", dork_cmd))
    app.add_handler(CommandHandler("md", mass_dork))
    app.add_handler(CommandHandler("stop", stop_dork))
    app.add_handler(CommandHandler("addproxy", add_proxy))
    app.add_handler(CommandHandler("threads", set_threads))
    app.add_handler(CommandHandler("pages", set_pages))
    # Fallback for unknown commands (just a friendly help)
    app.add_handler(MessageHandler(filters.COMMAND, start))

    logger.info("Bot starting. Press Ctrl+C to stop.")
    app.run_polling()

if __name__ == "__main__":
    main()
