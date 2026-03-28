#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import contextlib
import html
import json
import logging
import os
import random
import re
import signal
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote, urljoin, urlparse, parse_qs

import aiohttp
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    Update,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
STATE_PATH = Path(os.getenv("STATE_PATH", "bot_state.json"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

CHECK_LOOP_INTERVAL = int(os.getenv("CHECK_LOOP_INTERVAL", "10"))
WATCH_MIN_INTERVAL = int(os.getenv("WATCH_MIN_INTERVAL", "240"))
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "25"))

HTTP_BASE_DELAY = float(os.getenv("HTTP_BASE_DELAY", "10"))
HTTP_JITTER = float(os.getenv("HTTP_JITTER", "2"))
MAX_BACKOFF = int(os.getenv("MAX_BACKOFF", "1800"))
MAX_ITEMS_PER_CHECK = int(os.getenv("MAX_ITEMS_PER_CHECK", "12"))

MAX_TG_MESSAGE_LEN = 3500

ALLOWED_USER_IDS_RAW = os.getenv("ALLOWED_USER_IDS", "").strip()
ALLOWED_USER_IDS = {
    int(x.strip()) for x in ALLOWED_USER_IDS_RAW.split(",") if x.strip().isdigit()
}

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("avito_bot")


class AvitoRateLimitError(Exception):
    def __init__(self, retry_after: int = 60):
        super().__init__(f"Avito rate limited, retry_after={retry_after}")
        self.retry_after = retry_after


@dataclass
class ParsedItem:
    item_id: str
    title: str
    url: str
    price: Optional[int]
    city: str


def default_state() -> Dict[str, Any]:
    return {
        "subscriptions": {},
        "version": 1,
    }


def load_state() -> Dict[str, Any]:
    if not STATE_PATH.exists():
        state = default_state()
        save_state(state)
        return state

    try:
        with STATE_PATH.open("r", encoding="utf-8") as f:
            data = json.load(f)
            if not isinstance(data, dict):
                raise ValueError("state root is not dict")

            if "subscriptions" not in data or not isinstance(data["subscriptions"], dict):
                data["subscriptions"] = {}

            return data
    except Exception:
        logger.exception("Failed to load state, creating new one")
        state = default_state()
        save_state(state)
        return state


def save_state(state: Dict[str, Any]) -> None:
    tmp_path = STATE_PATH.with_suffix(".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    tmp_path.replace(STATE_PATH)


def ensure_user_bucket(state: Dict[str, Any], user_id: int) -> List[Dict[str, Any]]:
    key = str(user_id)
    if key not in state["subscriptions"] or not isinstance(state["subscriptions"][key], list):
        state["subscriptions"][key] = []
    return state["subscriptions"][key]


def next_watch_id(user_watches: List[Dict[str, Any]]) -> int:
    used = {int(w.get("id", 0)) for w in user_watches if str(w.get("id", "")).isdigit()}
    i = 1
    while i in used:
        i += 1
    return i


def normalize_csv_words(raw: str) -> List[str]:
    if not raw.strip() or raw.strip() == "-":
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]


def parse_optional_int(raw: str) -> Optional[int]:
    raw = raw.strip()
    if raw in {"", "-", "нет", "Нет", "none", "None"}:
        return None
    digits = re.sub(r"[^\d]", "", raw)
    if not digits:
        return None
    return int(digits)


def is_allowed_user(user_id: Optional[int]) -> bool:
    if user_id is None:
        return False
    if not ALLOWED_USER_IDS:
        return True
    return user_id in ALLOWED_USER_IDS


async def guard_access(update: Update) -> bool:
    user = update.effective_user
    user_id = user.id if user else None
    if is_allowed_user(user_id):
        return True

    text = "У тебя нет доступа к этому боту."
    if update.message:
        await update.message.reply_text(text)
    elif update.callback_query:
        await update.callback_query.answer(text, show_alert=True)
    return False


MENU_TEXT = "Выбери действие:"
BTN_ADD = "➕ Добавить фильтр"
BTN_LIST = "📋 Мои фильтры"
BTN_CHECK = "🔍 Проверить сейчас"
BTN_HELP = "ℹ️ Помощь"

MAIN_KEYBOARD = ReplyKeyboardMarkup(
    [
        [KeyboardButton(BTN_ADD), KeyboardButton(BTN_LIST)],
        [KeyboardButton(BTN_CHECK), KeyboardButton(BTN_HELP)],
    ],
    resize_keyboard=True,
)

(
    ADD_URL,
    ADD_TITLE,
    ADD_MINUS,
    ADD_CITIES,
    ADD_PRICE_FROM,
    ADD_PRICE_TO,
) = range(6)


def build_watch_manage_keyboard(watch_id: int, enabled: bool) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "⏸ Выключить" if enabled else "▶️ Включить",
                    callback_data=f"toggle:{watch_id}",
                ),
                InlineKeyboardButton("🗑 Удалить", callback_data=f"delete:{watch_id}"),
            ],
            [
                InlineKeyboardButton("🔍 Проверить", callback_data=f"check:{watch_id}"),
            ],
        ]
    )


def build_watch_summary(watch: Dict[str, Any]) -> str:
    minus_words = ", ".join(watch.get("minus_words", [])) or "—"
    cities = ", ".join(watch.get("cities", [])) or "—"
    pf = watch.get("price_from")
    pt = watch.get("price_to")

    price_parts = []
    if pf is not None:
        price_parts.append(f"от {pf:,}".replace(",", " "))
    if pt is not None:
        price_parts.append(f"до {pt:,}".replace(",", " "))
    price_text = " ".join(price_parts) if price_parts else "—"

    status = "ВКЛ" if watch.get("enabled", True) else "ВЫКЛ"
    errors = watch.get("errors", 0)
    backoff_until = watch.get("backoff_until", 0)
    next_check_ts = watch.get("next_check_ts", 0)

    if backoff_until and backoff_until > time.time():
        next_check_str = f"backoff до {time.strftime('%H:%M:%S', time.localtime(backoff_until))}"
    elif next_check_ts:
        next_check_str = time.strftime("%H:%M:%S", time.localtime(next_check_ts))
    else:
        next_check_str = "сразу"

    return (
        f"<b>#{watch['id']} | {html.escape(watch.get('title') or 'Без названия')}</b>\n"
        f"Статус: <b>{status}</b>\n"
        f"URL: {html.escape(watch.get('url', ''))}\n"
        f"Минус-слова: {html.escape(minus_words)}\n"
        f"Города: {html.escape(cities)}\n"
        f"Цена: {html.escape(price_text)}\n"
        f"Ошибки подряд: {errors}\n"
        f"Следующая проверка: {next_check_str}"
    )


def normalize_avito_url(url: str) -> str:
    url = url.strip()
    if not url:
        return url

    if not url.startswith("http://") and not url.startswith("https://"):
        url = "https://" + url

    parsed = urlparse(url)
    scheme = "https"
    netloc = parsed.netloc.lower()
    path = parsed.path or "/"
    query = parsed.query

    if "avito.ru" not in netloc:
        q = url.strip()
        q = q.replace("https://", "").replace("http://", "")
        q = q.strip("/")
        return f"https://www.avito.ru/all?q={quote(q)}"

    return f"{scheme}://{netloc}{path}" + (f"?{query}" if query else "")


AVITO_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}


async def wait_http_slot(application: Application) -> None:
    lock: asyncio.Lock = application.bot_data["http_lock"]

    async with lock:
        last_ts = float(application.bot_data.get("last_http_ts", 0.0))
        now = time.monotonic()

        delay = HTTP_BASE_DELAY + random.uniform(0, HTTP_JITTER)
        wait_for = delay - (now - last_ts)
        if wait_for > 0:
            await asyncio.sleep(wait_for)

        application.bot_data["last_http_ts"] = time.monotonic()


async def fetch_text(application: Application, url: str) -> str:
    await wait_http_slot(application)
    session: aiohttp.ClientSession = application.bot_data["http_session"]

    async with session.get(url, headers=AVITO_HEADERS, allow_redirects=True) as resp:
        if resp.status == 429:
            retry_after = resp.headers.get("Retry-After")
            try:
                retry_after_int = int(retry_after) if retry_after else 60
            except Exception:
                retry_after_int = 60
            raise AvitoRateLimitError(retry_after=retry_after_int)

        if resp.status >= 500:
            raise RuntimeError(f"Avito server error {resp.status}")

        if resp.status != 200:
            raise RuntimeError(f"Unexpected Avito status {resp.status}")

        return await resp.text()


def cleanup_text(s: str) -> str:
    s = html.unescape(s or "")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def parse_price(raw: str) -> Optional[int]:
    digits = re.sub(r"[^\d]", "", raw or "")
    if not digits:
        return None
    try:
        return int(digits)
    except Exception:
        return None


def parse_items_from_json_chunks(page_html: str) -> List[ParsedItem]:
    found: List[ParsedItem] = []
    seen_ids = set()

    pattern = re.compile(
        r'"id"\s*:\s*"?(?P<id>\d{6,})"?'
        r'.{0,800}?'
        r'"title"\s*:\s*"(?P<title>.*?)"'
        r'.{0,1000}?'
        r'"urlPath"\s*:\s*"(?P<url>.*?)"'
        r'.{0,1000}?'
        r'(?:"price"\s*:\s*(?P<price>\d+)|"priceString"\s*:\s*"(?P<price_str>.*?)")'
        r'.{0,1000}?'
        r'(?:"location"\s*:\s*"(?P<city>.*?)"|'
        r'"geoAddress"\s*:\s*"(?P<city2>.*?)"|'
        r'"address"\s*:\s*"(?P<city3>.*?)")',
        re.DOTALL,
    )

    for m in pattern.finditer(page_html):
        item_id = m.group("id")
        if item_id in seen_ids:
            continue
        seen_ids.add(item_id)

        title = cleanup_text(m.group("title"))
        url_part = cleanup_text(m.group("url"))
        if url_part.startswith("/"):
            url = urljoin("https://www.avito.ru", url_part)
        else:
            url = url_part if url_part.startswith("http") else urljoin("https://www.avito.ru", "/" + url_part)

        price_raw = m.group("price") or m.group("price_str") or ""
        price = parse_price(price_raw)
        city = cleanup_text(m.group("city") or m.group("city2") or m.group("city3") or "")

        if not title or not url:
            continue

        found.append(
            ParsedItem(
                item_id=str(item_id),
                title=title,
                url=url,
                price=price,
                city=city,
            )
        )

    return found


def parse_items_from_links(page_html: str) -> List[ParsedItem]:
    found: List[ParsedItem] = []
    seen_ids = set()

    link_pattern = re.compile(
        r'<a[^>]+href="(?P<href>/[^"]*?_(?P<id>\d{6,})[^"]*)"[^>]*>(?P<body>.*?)</a>',
        re.DOTALL | re.IGNORECASE,
    )

    for m in link_pattern.finditer(page_html):
        item_id = m.group("id")
        if item_id in seen_ids:
            continue

        href = cleanup_text(m.group("href"))
        body = m.group("body")
        text = cleanup_text(re.sub(r"<[^>]+>", " ", body))

        if not text:
            continue

        start = m.end()
        tail = page_html[start:start + 1200]
        tail_text = cleanup_text(re.sub(r"<[^>]+>", " ", tail))

        price = None
        city = ""

        price_match = re.search(r"(\d[\d\s]{2,})\s*₽", tail_text)
        if price_match:
            price = parse_price(price_match.group(1))

        city_match = re.search(r"(?:сегодня|вчера|\d{1,2}:\d{2})\s+([A-Za-zА-Яа-яЁё\-\s]{2,40})", tail_text)
        if city_match:
            city = cleanup_text(city_match.group(1))

        seen_ids.add(item_id)
        found.append(
            ParsedItem(
                item_id=str(item_id),
                title=text[:180],
                url=urljoin("https://www.avito.ru", href),
                price=price,
                city=city,
            )
        )

    return found


def parse_avito_search_page(page_html: str, limit: int = MAX_ITEMS_PER_CHECK) -> List[Dict[str, Any]]:
    items = parse_items_from_json_chunks(page_html)

    if not items:
        items = parse_items_from_links(page_html)

    deduped: List[Dict[str, Any]] = []
    seen = set()

    for item in items:
        if item.item_id in seen:
            continue
        seen.add(item.item_id)
        deduped.append(
            {
                "id": item.item_id,
                "title": item.title,
                "url": item.url,
                "price": item.price,
                "city": item.city,
            }
        )
        if len(deduped) >= limit:
            break

    return deduped


async def fetch_avito_items(application: Application, watch: Dict[str, Any]) -> List[Dict[str, Any]]:
    url = watch["url"]
    page_html = await fetch_text(application, url)
    items = parse_avito_search_page(page_html, limit=MAX_ITEMS_PER_CHECK)
    return items


def item_matches_filters(item: Dict[str, Any], watch: Dict[str, Any]) -> bool:
    title = (item.get("title") or "").lower()
    city = (item.get("city") or "").lower()
    price = item.get("price")

    minus_words = [x.lower() for x in watch.get("minus_words", [])]
    cities = [x.lower() for x in watch.get("cities", [])]
    price_from = watch.get("price_from")
    price_to = watch.get("price_to")

    if minus_words and any(word in title for word in minus_words):
        return False

    if cities:
        if not city:
            return False
        if not any(c in city for c in cities):
            return False

    if price_from is not None and price is not None and price < price_from:
        return False

    if price_to is not None and price is not None and price > price_to:
        return False

    return True


def apply_filters(items: List[Dict[str, Any]], watch: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [item for item in items if item_matches_filters(item, watch)]


def format_item(item: Dict[str, Any]) -> str:
    title = html.escape(item.get("title") or "Без названия")
    url = html.escape(item.get("url") or "")
    city = html.escape(item.get("city") or "—")
    price = item.get("price")
    price_text = f"{price:,} ₽".replace(",", " ") if isinstance(price, int) else "Цена не указана"

    return f"• <b>{title}</b>\n  {price_text}\n  {city}\n  {url}"


async def safe_send_long_text(chat_id: int, text: str, application: Application) -> None:
    chunks = []
    while text:
        if len(text) <= MAX_TG_MESSAGE_LEN:
            chunks.append(text)
            break
        cut = text.rfind("\n", 0, MAX_TG_MESSAGE_LEN)
        if cut == -1:
            cut = MAX_TG_MESSAGE_LEN
        chunks.append(text[:cut])
        text = text[cut:].lstrip()

    for chunk in chunks:
        await application.bot.send_message(
            chat_id=chat_id,
            text=chunk,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )


async def send_new_items(
    application: Application,
    user_id: int,
    fresh_items: List[Dict[str, Any]],
    watch: Dict[str, Any],
) -> None:
    if not fresh_items:
        return

    header = (
        f"🆕 <b>Новые объявления</b>\n"
        f"Фильтр: <b>{html.escape(watch.get('title') or 'Без названия')}</b>\n\n"
    )
    body = "\n\n".join(format_item(item) for item in fresh_items[:10])
    await safe_send_long_text(user_id, header + body, application)


async def process_one_watch(
    application: Application,
    state: Dict[str, Any],
    user_id: int,
    watch: Dict[str, Any],
    force: bool = False,
) -> Tuple[int, int]:
    now = time.time()

    if not force:
        if not watch.get("enabled", True):
            return (0, 0)
        if now < watch.get("backoff_until", 0):
            return (0, 0)
        if now < watch.get("next_check_ts", 0):
            return (0, 0)

    try:
        items = await fetch_avito_items(application, watch)
        items = apply_filters(items, watch)

        seen_ids = set(str(x) for x in watch.get("seen_ids", []))
        fresh_items = [x for x in items if str(x["id"]) not in seen_ids]

        if fresh_items:
            await send_new_items(application, user_id, fresh_items, watch)

        for item in items:
            seen_ids.add(str(item["id"]))

        watch["seen_ids"] = list(seen_ids)[-1000:]
        watch["errors"] = 0
        watch["backoff_until"] = 0
        watch["last_check_ts"] = now
        watch["next_check_ts"] = now + WATCH_MIN_INTERVAL + random.randint(0, 20)

        return (len(items), len(fresh_items))

    except AvitoRateLimitError as e:
        errors = int(watch.get("errors", 0)) + 1
        watch["errors"] = errors

        base = max(int(e.retry_after), 60)
        backoff = min(MAX_BACKOFF, base * (2 ** min(errors - 1, 4)))
        watch["backoff_until"] = now + backoff + random.randint(5, 20)
        watch["next_check_ts"] = watch["backoff_until"]

        logger.warning(
            "429 for watch=%s user=%s -> backoff=%ss",
            watch.get("title"),
            user_id,
            int(backoff),
        )
        return (0, 0)

    except asyncio.CancelledError:
        raise

    except Exception:
        errors = int(watch.get("errors", 0)) + 1
        watch["errors"] = errors
        backoff = min(MAX_BACKOFF, 60 * (2 ** min(errors - 1, 4)))
        watch["backoff_until"] = now + backoff
        watch["next_check_ts"] = watch["backoff_until"]
        logger.exception("Failed watch user=%s watch=%s", user_id, watch.get("title"))
        return (0, 0)


async def run_background_checks(application: Application) -> None:
    state = load_state()
    now = time.time()

    due_jobs: List[Tuple[int, Dict[str, Any]]] = []

    for user_id_str, watches in state.get("subscriptions", {}).items():
        try:
            user_id = int(user_id_str)
        except Exception:
            continue

        if not isinstance(watches, list):
            continue

        for watch in watches:
            if not isinstance(watch, dict):
                continue
            if not watch.get("enabled", True):
                continue
            if now < watch.get("backoff_until", 0):
                continue
            if now < watch.get("next_check_ts", 0):
                continue

            due_jobs.append((user_id, watch))

    for user_id, watch in due_jobs:
        await process_one_watch(application, state, user_id, watch, force=False)

    save_state(state)


async def background_checker(application: Application) -> None:
    stop_event: asyncio.Event = application.bot_data["stop_event"]

    while not stop_event.is_set():
        started = time.monotonic()
        try:
            await run_background_checks(application)
        except asyncio.CancelledError:
            logger.info("background_checker cancelled")
            raise
        except Exception:
            logger.exception("background_checker crashed, loop continues")

        elapsed = time.monotonic() - started
        sleep_for = max(1, CHECK_LOOP_INTERVAL - elapsed)

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=sleep_for)
        except asyncio.TimeoutError:
            pass


async def post_init(application: Application) -> None:
    logger.info("post_init: starting background services")

    application.bot_data["stop_event"] = asyncio.Event()
    application.bot_data["http_lock"] = asyncio.Lock()
    application.bot_data["last_http_ts"] = 0.0

    timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT)
    session = aiohttp.ClientSession(timeout=timeout)
    application.bot_data["http_session"] = session

    task = asyncio.create_task(background_checker(application), name="background_checker")
    application.bot_data["background_task"] = task

    logger.info("background_checker started")


async def post_shutdown(application: Application) -> None:
    logger.info("post_shutdown: stopping background services")

    stop_event = application.bot_data.get("stop_event")
    if stop_event:
        stop_event.set()

    task = application.bot_data.get("background_task")
    if task:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    session = application.bot_data.get("http_session")
    if session and not session.closed:
        await session.close()

    logger.info("background services stopped")


HELP_TEXT = (
    "Бот отслеживает новые объявления Авито по фильтрам.\n\n"
    "Что умеет:\n"
    "• добавлять фильтры\n"
    "• хранить минус-слова\n"
    "• фильтровать по городам\n"
    "• фильтровать по цене\n"
    "• включать/выключать фильтры\n"
    "• вручную проверять фильтр\n"
    "• автоматически проверять фоновой задачей\n\n"
    "Кнопки:\n"
    "• ➕ Добавить фильтр\n"
    "• 📋 Мои фильтры\n"
    "• 🔍 Проверить сейчас\n"
    "• ℹ️ Помощь\n\n"
    "При добавлении:\n"
    "1) вставляешь ссылку Avito\n"
    "2) даёшь название\n"
    "3) минус-слова через запятую или -\n"
    "4) города через запятую или -\n"
    "5) минимальную цену или -\n"
    "6) максимальную цену или -"
)


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await guard_access(update):
        return

    if update.message:
        await update.message.reply_text(
            "Бот запущен.\n\n" + MENU_TEXT,
            reply_markup=MAIN_KEYBOARD,
        )


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await guard_access(update):
        return

    if update.message:
        await update.message.reply_text(HELP_TEXT, reply_markup=MAIN_KEYBOARD)
    elif update.callback_query and update.callback_query.message:
        await update.callback_query.message.reply_text(HELP_TEXT, reply_markup=MAIN_KEYBOARD)


async def cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await guard_access(update):
        return ConversationHandler.END

    context.user_data.pop("new_watch", None)
    if update.message:
        await update.message.reply_text(
            "Добавление фильтра отменено.",
            reply_markup=MAIN_KEYBOARD,
        )
    return ConversationHandler.END


async def add_watch_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await guard_access(update):
        return ConversationHandler.END

    context.user_data["new_watch"] = {}

    if update.message:
        await update.message.reply_text(
            "Пришли ссылку на поиск Avito.\n\n"
            "Пример:\n"
            "https://www.avito.ru/all?q=iphone",
            reply_markup=ReplyKeyboardRemove(),
        )
    return ADD_URL


async def add_watch_url(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    raw = (update.message.text or "").strip()
    url = normalize_avito_url(raw)
    context.user_data["new_watch"]["url"] = url

    await update.message.reply_text(
        "Теперь пришли название фильтра.\n"
        "Например: Айфон Москва"
    )
    return ADD_TITLE


async def add_watch_title(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    title = (update.message.text or "").strip()
    if not title:
        title = "Фильтр"

    context.user_data["new_watch"]["title"] = title

    await update.message.reply_text(
        "Минус-слова через запятую.\n"
        "Если не нужны — отправь: -\n\n"
        "Пример:\n"
        "чехол, стекло, обмен"
    )
    return ADD_MINUS


async def add_watch_minus(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    raw = update.message.text or ""
    context.user_data["new_watch"]["minus_words"] = normalize_csv_words(raw)

    await update.message.reply_text(
        "Города через запятую.\n"
        "Если не нужны — отправь: -\n\n"
        "Пример:\n"
        "Москва, Химки"
    )
    return ADD_CITIES


async def add_watch_cities(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    raw = update.message.text or ""
    context.user_data["new_watch"]["cities"] = normalize_csv_words(raw)

    await update.message.reply_text(
        "Минимальная цена или -\n\n"
        "Пример:\n"
        "30000"
    )
    return ADD_PRICE_FROM


async def add_watch_price_from(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    raw = update.message.text or ""
    context.user_data["new_watch"]["price_from"] = parse_optional_int(raw)

    await update.message.reply_text(
        "Максимальная цена или -\n\n"
        "Пример:\n"
        "80000"
    )
    return ADD_PRICE_TO


async def add_watch_price_to(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    raw = update.message.text or ""
    new_watch = context.user_data.get("new_watch", {})
    new_watch["price_to"] = parse_optional_int(raw)

    user_id = update.effective_user.id
    state = load_state()
    user_watches = ensure_user_bucket(state, user_id)

    watch = {
        "id": next_watch_id(user_watches),
        "title": new_watch.get("title", "Фильтр"),
        "url": new_watch.get("url", ""),
        "minus_words": new_watch.get("minus_words", []),
        "cities": new_watch.get("cities", []),
        "price_from": new_watch.get("price_from"),
        "price_to": new_watch.get("price_to"),
        "enabled": True,
        "errors": 0,
        "seen_ids": [],
        "backoff_until": 0,
        "next_check_ts": 0,
        "last_check_ts": 0,
        "created_ts": int(time.time()),
    }

    user_watches.append(watch)
    save_state(state)
    context.user_data.pop("new_watch", None)

    await update.message.reply_text(
        "Фильтр сохранён.\n\n" + build_watch_summary(watch),
        reply_markup=MAIN_KEYBOARD,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )

    return ConversationHandler.END


async def list_watches(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await guard_access(update):
        return

    user_id = update.effective_user.id
    state = load_state()
    user_watches = ensure_user_bucket(state, user_id)

    if not user_watches:
        await update.message.reply_text(
            "У тебя пока нет фильтров.",
            reply_markup=MAIN_KEYBOARD,
        )
        return

    for watch in user_watches:
        await update.message.reply_text(
            build_watch_summary(watch),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=build_watch_manage_keyboard(
                watch_id=int(watch["id"]),
                enabled=bool(watch.get("enabled", True)),
            ),
        )


async def check_all_now(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await guard_access(update):
        return

    user_id = update.effective_user.id
    state = load_state()
    user_watches = ensure_user_bucket(state, user_id)

    if not user_watches:
        await update.message.reply_text(
            "Нет фильтров для проверки.",
            reply_markup=MAIN_KEYBOARD,
        )
        return

    await update.message.reply_text("Запускаю ручную проверку всех включённых фильтров...")

    total_seen = 0
    total_fresh = 0
    checked_count = 0

    for watch in user_watches:
        if not watch.get("enabled", True):
            continue
        seen_count, fresh_count = await process_one_watch(
            context.application, state, user_id, watch, force=True
        )
        total_seen += seen_count
        total_fresh += fresh_count
        checked_count += 1

    save_state(state)

    await update.message.reply_text(
        f"Готово.\n"
        f"Проверено фильтров: {checked_count}\n"
        f"Найдено объявлений: {total_seen}\n"
        f"Новых: {total_fresh}",
        reply_markup=MAIN_KEYBOARD,
    )


def find_watch(user_watches: List[Dict[str, Any]], watch_id: int) -> Optional[Dict[str, Any]]:
    for w in user_watches:
        try:
            if int(w.get("id")) == int(watch_id):
                return w
        except Exception:
            continue
    return None


async def callback_actions(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await guard_access(update):
        return

    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id
    state = load_state()
    user_watches = ensure_user_bucket(state, user_id)

    data = query.data or ""
    try:
        action, watch_id_raw = data.split(":", 1)
        watch_id = int(watch_id_raw)
    except Exception:
        await query.answer("Некорректная команда", show_alert=True)
        return

    watch = find_watch(user_watches, watch_id)
    if not watch:
        await query.answer("Фильтр не найден", show_alert=True)
        return

    if action == "toggle":
        watch["enabled"] = not bool(watch.get("enabled", True))
        if watch["enabled"]:
            watch["next_check_ts"] = 0
            watch["backoff_until"] = 0
        save_state(state)

        await query.edit_message_text(
            build_watch_summary(watch),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=build_watch_manage_keyboard(
                watch_id=watch_id,
                enabled=bool(watch.get("enabled", True)),
            ),
        )
        return

    if action == "delete":
        state["subscriptions"][str(user_id)] = [w for w in user_watches if int(w.get("id", 0)) != watch_id]
        save_state(state)
        await query.edit_message_text("Фильтр удалён.")
        return

    if action == "check":
        await query.answer("Проверяю фильтр...")
        seen_count, fresh_count = await process_one_watch(
            context.application, state, user_id, watch, force=True
        )
        save_state(state)

        await query.edit_message_text(
            build_watch_summary(watch) + f"\n\nПроверка вручную:\nНайдено: {seen_count}\nНовых: {fresh_count}",
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=build_watch_manage_keyboard(
                watch_id=watch_id,
                enabled=bool(watch.get("enabled", True)),
            ),
        )
        return

    await query.answer("Неизвестное действие", show_alert=True)


async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await guard_access(update):
        return

    text = (update.message.text or "").strip()

    if text == BTN_LIST:
        await list_watches(update, context)
        return

    if text == BTN_CHECK:
        await check_all_now(update, context)
        return

    if text == BTN_HELP:
        await help_cmd(update, context)
        return

    await update.message.reply_text(
        "Не понял команду. Используй кнопки ниже.",
        reply_markup=MAIN_KEYBOARD,
    )


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Unhandled telegram error", exc_info=context.error)


def build_application() -> Application:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is empty. Set environment variable BOT_TOKEN")

    add_watch_conv = ConversationHandler(
        entry_points=[
            CommandHandler("add", add_watch_start),
            MessageHandler(filters.Regex(f"^{re.escape(BTN_ADD)}$"), add_watch_start),
        ],
        states={
            ADD_URL: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_watch_url)],
            ADD_TITLE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_watch_title)],
            ADD_MINUS: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_watch_minus)],
            ADD_CITIES: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_watch_cities)],
            ADD_PRICE_FROM: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_watch_price_from)],
            ADD_PRICE_TO: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_watch_price_to)],
        },
        fallbacks=[CommandHandler("cancel", cancel_cmd)],
        per_chat=True,
        per_user=True,
        per_message=False,
        allow_reentry=True,
    )

    application = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    application.add_error_handler(error_handler)

    application.add_handler(add_watch_conv, group=0)
    application.add_handler(CommandHandler("start", start_cmd), group=1)
    application.add_handler(CommandHandler("help", help_cmd), group=1)
    application.add_handler(CommandHandler("list", list_watches), group=1)
    application.add_handler(CommandHandler("check", check_all_now), group=1)
    application.add_handler(
        CallbackQueryHandler(callback_actions, pattern=r"^(toggle|delete|check):\d+$"),
        group=1,
    )
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_router), group=2)

    return application


def main() -> None:
    logger.info("Starting Avito bot")
    application = build_application()

    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        close_loop=False,
        stop_signals=(signal.SIGINT, signal.SIGTERM, signal.SIGABRT),
    )


if __name__ == "__main__":
    main()
