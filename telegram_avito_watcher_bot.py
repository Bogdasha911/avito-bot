import asyncio
import json
import logging
import os
import random
import re
import time
from dataclasses import dataclass, asdict
from typing import Dict, List
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes

BOT_TOKEN = "8790939388:AAG3OZQb99tF9biykpHbSXg9Dyn0uuHqwJY"
CHECK_INTERVAL_SECONDS = 300
STATE_FILE = "bot_state.json"
REQUEST_TIMEOUT = 25

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.WARNING,
)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)


@dataclass
class SearchItem:
    name: str
    url: str
    sent_ids: List[str]


class StateStore:
    def __init__(self, path: str):
        self.path = path
        self.data: Dict[str, List[Dict]] = {"users": {}}
        self.load()

    def load(self) -> None:
        if not os.path.exists(self.path):
            self.data = {"users": {}}
            self.save()
            return

        try:
            with open(self.path, "r", encoding="utf-8") as f:
                self.data = json.load(f)
        except Exception:
            logger.exception("Не удалось прочитать state, создаю новый файл")
            self.data = {"users": {}}
            self.save()

    def save(self) -> None:
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2)

    def get_user_searches(self, chat_id: int) -> List[SearchItem]:
        raw_items = self.data.get("users", {}).get(str(chat_id), [])
        return [SearchItem(**item) for item in raw_items]

    def set_user_searches(self, chat_id: int, items: List[SearchItem]) -> None:
        self.data.setdefault("users", {})[str(chat_id)] = [asdict(item) for item in items]
        self.save()


store = StateStore(STATE_FILE)


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def build_headers() -> Dict[str, str]:
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    ]
    return {
        "User-Agent": random.choice(user_agents),
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }


def extract_listing_id(url: str) -> str:
    match = re.search(r"_(\d+)(?:\?|$)", url)
    if match:
        return match.group(1)
    return url


def parse_links_generic(search_url: str, html: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    items: List[Dict[str, str]] = []
    seen_ids = set()

    selectors = [
        "a[data-marker='item-title']",
        "a[itemprop='url']",
    ]

    links = []
    for selector in selectors:
        links = soup.select(selector)
        if links:
            break

    for a in links:
        href = (a.get("href") or "").strip()
        title = normalize_text(a.get_text(" ", strip=True))

        if not href or not title:
            continue

        full_url = urljoin("https://www.avito.ru", href)

        if "/item/" not in full_url and "/moskva/" not in full_url and "/rossiya/" not in full_url:
            continue

        if "avito.ru/" not in full_url:
            continue

        if any(
            bad in full_url.lower()
            for bad in [
                "/about",
                "/brands",
                "/profile",
                "/help",
                "/articles",
                "/safety",
                "/legal",
                "/business",
            ]
        ):
            continue

        item_id = extract_listing_id(full_url)
        if item_id in seen_ids:
            continue

        seen_ids.add(item_id)
        items.append(
            {
                "id": item_id,
                "title": title,
                "url": full_url,
            }
        )

    return items[:30]


def fetch_listings(search_url: str) -> List[Dict[str, str]]:
    last_error = None

    for attempt in range(4):
        try:
            time.sleep(random.uniform(1.5, 4.0))

            response = requests.get(
                search_url,
                headers=build_headers(),
                timeout=REQUEST_TIMEOUT,
            )

            if response.status_code == 429:
                wait_time = 20 * (attempt + 1)
                logger.warning("Avito 429 for %s, waiting %s seconds", search_url, wait_time)
                time.sleep(wait_time)
                continue

            response.raise_for_status()
            return parse_links_generic(search_url, response.text)

        except requests.RequestException as e:
            last_error = e
            if attempt < 3:
                wait_time = 10 * (attempt + 1)
                logger.warning("Ошибка запроса %s. Повтор через %s сек.", e, wait_time)
                time.sleep(wait_time)
                continue
            break

    if last_error is not None:
        raise last_error

    raise RuntimeError("Не удалось получить объявления")


HELP_TEXT = """
<b>Бот для отслеживания новых объявлений</b>

Команды:
/start — запуск
/help — помощь
/add Название | Ссылка — добавить поиск
/add Ссылка — добавить поиск без названия
/list — показать мои поиски
/remove Название — удалить поиск
/check — проверить прямо сейчас
/clear — удалить все поиски

<b>Примеры:</b>
<code>/add Айфон | https://www.avito.ru/all?q=iphone</code>
<code>/add https://www.avito.ru/all?q=iphone</code>
""".strip()


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(HELP_TEXT, parse_mode=ParseMode.HTML)


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(HELP_TEXT, parse_mode=ParseMode.HTML)


async def list_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    items = store.get_user_searches(chat_id)

    if not items:
        await update.message.reply_text("У тебя пока нет активных поисков.")
        return

    lines = ["<b>Твои поиски:</b>"]
    for idx, item in enumerate(items, start=1):
        lines.append(f"{idx}. <b>{item.name}</b>\n{item.url}")

    await update.message.reply_text("\n\n".join(lines), parse_mode=ParseMode.HTML)


async def add_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    text = (update.message.text or "").strip()
    payload = text.replace("/add", "", 1).strip()

    if not payload:
        await update.message.reply_text(
            "Неверный формат.\nПример:\n/add Айфон | https://www.avito.ru/all?q=iphone"
        )
        return

    if "|" in payload:
        parts = payload.split("|", 1)
        name = parts[0].strip()
        url = parts[1].strip()
    else:
        name = "Поиск"
        url = payload

    if not url.startswith("http"):
        await update.message.reply_text(
            "Нужна полная ссылка.\nПример:\n/add https://www.avito.ru/all?q=iphone"
        )
        return

    items = store.get_user_searches(chat_id)

    if any(item.url == url for item in items):
        await update.message.reply_text("Такая ссылка уже добавлена.")
        return

    try:
        listings = await asyncio.to_thread(fetch_listings, url)
    except Exception as e:
        await update.message.reply_text(
            f"Не смог открыть ссылку. Ошибка: {e}\nПопробуй позже: Авито мог временно ограничить запросы."
        )
        return

    sent_ids = [x["id"] for x in listings]
    items.append(SearchItem(name=name, url=url, sent_ids=sent_ids))
    store.set_user_searches(chat_id, items)

    await update.message.reply_text(
        f"Добавил поиск: <b>{name}</b>\n"
        f"Сейчас вижу объявлений: <b>{len(listings)}</b>\n"
        f"Новые начнут приходить со следующей проверки.",
        parse_mode=ParseMode.HTML,
    )


async def remove_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    name = (update.message.text or "").replace("/remove", "", 1).strip()

    if not name:
        await update.message.reply_text("Пример: /remove Айфон")
        return

    items = store.get_user_searches(chat_id)
    new_items = [item for item in items if item.name.lower() != name.lower()]

    if len(new_items) == len(items):
        await update.message.reply_text("Не нашел поиск с таким названием.")
        return

    store.set_user_searches(chat_id, new_items)
    await update.message.reply_text(f"Удалил поиск: {name}")


async def clear_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    store.set_user_searches(chat_id, [])
    await update.message.reply_text("Все поиски удалены.")


async def check_one_search(chat_id: int, item: SearchItem, bot) -> int:
    try:
        listings = await asyncio.to_thread(fetch_listings, item.url)
    except Exception:
        logger.exception("Ошибка проверки поиска: %s", item.name)
        return 0

    old_ids = set(item.sent_ids)
    new_items = [x for x in listings if x["id"] not in old_ids]

    if new_items:
        for entry in new_items[:10]:
            text = (
                f"<b>Новое объявление:</b> {item.name}\n"
                f"<b>{entry['title']}</b>\n"
                f"{entry['url']}"
            )
            await bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML)

    item.sent_ids = [entry["id"] for entry in listings[:200]]
    return len(new_items)


async def check_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    items = store.get_user_searches(chat_id)

    if not items:
        await update.message.reply_text("Сначала добавь хотя бы один поиск через /add")
        return

    total = 0
    for item in items:
        total += await check_one_search(chat_id, item, context.bot)

    store.set_user_searches(chat_id, items)
    await update.message.reply_text(f"Проверка завершена. Новых объявлений: {total}")


async def periodic_check(context: ContextTypes.DEFAULT_TYPE) -> None:
    users_map = store.data.get("users", {})
    if not users_map:
        return

    for chat_id_str, raw_items in users_map.items():
        chat_id = int(chat_id_str)
        items = [SearchItem(**item) for item in raw_items]
        changed = False

        for item in items:
            await check_one_search(chat_id, item, context.bot)
            changed = True

        if changed:
            store.set_user_searches(chat_id, items)


def main() -> None:
    application = Application.builder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start_cmd))
    application.add_handler(CommandHandler("help", help_cmd))
    application.add_handler(CommandHandler("add", add_cmd))
    application.add_handler(CommandHandler("list", list_cmd))
    application.add_handler(CommandHandler("remove", remove_cmd))
    application.add_handler(CommandHandler("clear", clear_cmd))
    application.add_handler(CommandHandler("check", check_cmd))

    job_queue = application.job_queue
    if job_queue is None:
        raise RuntimeError(
            'JobQueue не активен. Установи библиотеку так: python -m pip install "python-telegram-bot[job-queue]"'
        )

    job_queue.run_repeating(periodic_check, interval=CHECK_INTERVAL_SECONDS, first=15)

    print("Бот запущен")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
    # test deploy