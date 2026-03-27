import asyncio
import json
import logging
import os
import random
import re
import html
import subprocess
import time
from typing import Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    KeyboardButton,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

BOT_TOKEN = "8790939388:AAGO-1l24rfJNAfeOn8aJKJPJfDLsFO2vLk"
STATE_FILE = "bot_state.json"
CHECK_INTERVAL_SECONDS = 300
REQUEST_TIMEOUT = 25
ADMIN_IDS = {762031814}

MAIN_MENU = ReplyKeyboardMarkup(
    [
        [KeyboardButton("➕ Добавить поиск"), KeyboardButton("📋 Мои поиски")],
        [KeyboardButton("▶️ Проверить сейчас"), KeyboardButton("🧹 Очистить все")],
        [KeyboardButton("❓ Помощь"), KeyboardButton("🛠 Админ")],
    ],
    resize_keyboard=True,
)

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)

session = requests.Session()
session.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
)

http_semaphore = asyncio.Semaphore(2)

COMMON_BAD_WORDS = {
    "чехол",
    "чехлы",
    "бампер",
    "стекло",
    "коробка",
    "коробки",
    "защитное",
    "защитный",
    "пленка",
    "плёнка",
    "кабель",
    "зарядка",
    "зарядное",
    "держатель",
    "аксессуар",
    "аксессуары",
}


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def normalize_text(text: str) -> str:
    text = (text or "").lower().strip()
    text = text.replace("ё", "е")
    text = re.sub(r"[^\w\s\-]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text


def tokenize(text: str) -> List[str]:
    return [x for x in normalize_text(text).split() if x]


def parse_price(text: str) -> Optional[int]:
    if not text:
        return None
    digits = re.sub(r"[^\d]", "", text)
    if not digits:
        return None
    try:
        return int(digits)
    except Exception:
        return None


def html_escape(value: str) -> str:
    return html.escape(value or "")


def ensure_state() -> Dict:
    if not os.path.exists(STATE_FILE):
        return {"users": {}, "drafts": {}}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        data = {}

    if "users" not in data or not isinstance(data["users"], dict):
        data["users"] = {}
    if "drafts" not in data or not isinstance(data["drafts"], dict):
        data["drafts"] = {}
    return data


def save_state(state: Dict) -> None:
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, STATE_FILE)


def ensure_user_searches(state: Dict, user_id: int) -> List[Dict]:
    uid = str(user_id)
    if uid not in state["users"] or not isinstance(state["users"][uid], list):
        state["users"][uid] = []
    return state["users"][uid]


def get_user_draft(state: Dict, user_id: int) -> Dict:
    uid = str(user_id)
    if uid not in state["drafts"] or not isinstance(state["drafts"][uid], dict):
        state["drafts"][uid] = {}
    return state["drafts"][uid]


def reset_draft(state: Dict, user_id: int) -> Dict:
    uid = str(user_id)
    state["drafts"][uid] = {
        "step": None,
        "name": "",
        "url": "",
        "include_words": [],
        "exclude_words": [],
        "min_price": None,
        "max_price": None,
        "cities": [],
    }
    return state["drafts"][uid]


def format_search(search: Dict, index: int) -> str:
    include_words = " ".join(search.get("include_words", [])) or "—"
    exclude_words = ", ".join(search.get("exclude_words", [])) or "—"
    cities = ", ".join(search.get("cities", [])) or "Любые"

    min_price = search.get("min_price")
    max_price = search.get("max_price")
    price_parts = []
    if min_price is not None:
        price_parts.append(f"от {min_price}")
    if max_price is not None:
        price_parts.append(f"до {max_price}")
    price_text = " ".join(price_parts) if price_parts else "Любая"

    return (
        f"{index}. <b>{html_escape(search.get('name', 'Без названия'))}</b>\n"
        f"URL: {html_escape(search.get('url', ''))}\n"
        f"Ключи: {html_escape(include_words)}\n"
        f"Минус-слова: {html_escape(exclude_words)}\n"
        f"Цена: {html_escape(price_text)}\n"
        f"Города: {html_escape(cities)}"
    )


def format_item(item: Dict) -> str:
    title = html_escape(item.get("title", "Без названия"))
    price = html_escape(item.get("price", "Цена не указана"))
    city = html_escape(item.get("city", "Город не указан"))
    url = html_escape(item.get("url", ""))
    return f"<b>{title}</b>\n{price}\n{city}\n{url}"


def fetch_with_backoff(url: str, max_retries: int = 5) -> requests.Response:
    delay = 5
    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            response = session.get(url, timeout=(10, REQUEST_TIMEOUT))

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    wait_for = int(retry_after)
                else:
                    wait_for = delay + random.uniform(1, 3)

                logger.warning("429 для %s, жду %.1f сек", url, wait_for)
                time.sleep(wait_for)
                delay = min(delay * 2, 120)
                continue

            response.raise_for_status()
            return response

        except requests.RequestException as e:
            last_error = e
            if attempt == max_retries:
                raise
            wait_for = delay + random.uniform(1, 3)
            logger.warning("Ошибка запроса %s: %s; retry через %.1f сек", url, e, wait_for)
            time.sleep(wait_for)
            delay = min(delay * 2, 120)

    raise RuntimeError(f"Не удалось загрузить страницу: {last_error}")


def extract_item_id(url: str) -> str:
    if not url:
        return ""
    m = re.search(r"_(\d+)(?:\?|$)", url)
    if m:
        return m.group(1)
    m = re.search(r"/(\d+)(?:\?|$)", url)
    if m:
        return m.group(1)
    return url


def parse_cards_from_html(html_text: str) -> List[Dict]:
    soup = BeautifulSoup(html_text, "html.parser")
    cards = []
    seen = set()

    anchors = soup.select('a[href*="_"], a[href*="/item"]')
    for a in anchors:
        href = (a.get("href") or "").strip()
        if not href:
            continue

        url = urljoin("https://www.avito.ru", href)
        item_id = extract_item_id(url)

        title = (a.get("aria-label") or "").strip()

        if not title:
            h = a.select_one("h3")
            if h:
                title = h.get_text(" ", strip=True)

        if not title:
            title = a.get_text(" ", strip=True)

        title = re.sub(r"\s+", " ", title).strip()
        if len(title) < 3:
            continue

        parent = a
        block_text = ""
        for _ in range(5):
            parent = parent.parent
            if not parent:
                break
            block_text = parent.get_text(" ", strip=True)
            if len(block_text) > len(title) + 20:
                break

        price_match = re.search(r"(\d[\d\s]{2,})\s*₽", block_text)
        price_text = price_match.group(0) if price_match else ""

        city = ""
        city_match = re.search(
            r"(Москва|Санкт-Петербург|СПб|Казань|Екатеринбург|Новосибирск|Краснодар|Самара|Ростов-на-Дону|Нижний Новгород|Челябинск|Уфа|Пермь|Воронеж|Омск|Тюмень|Тула|Тверь|Ижевск|Сочи)",
            block_text,
            flags=re.IGNORECASE,
        )
        if city_match:
            city = city_match.group(1)
        else:
            short_lines = [x.strip() for x in re.split(r"[·\n]", block_text) if x.strip()]
            for line in short_lines[:8]:
                nline = normalize_text(line)
                if 1 <= len(nline.split()) <= 4 and "сегодня" not in nline and "вчера" not in nline and "₽" not in line:
                    if any(ch.isalpha() for ch in line):
                        city = line
                        break

        key = (item_id, title)
        if key in seen:
            continue
        seen.add(key)

        cards.append(
            {
                "id": item_id,
                "title": title,
                "url": url,
                "price": price_text,
                "city": city,
            }
        )

    return cards


def city_matches(card_city: str, search_cities: List[str]) -> bool:
    if not search_cities:
        return True

    current = normalize_text(card_city)
    if not current:
        return False

    for city in search_cities:
        c = normalize_text(city)
        if c and (c in current or current in c):
            return True

    return False


def title_matches(title: str, include_words: List[str], exclude_words: List[str]) -> bool:
    title_n = normalize_text(title)
    title_tokens = set(tokenize(title))

    if not include_words:
        return False

    blocked_words = set(normalize_text(x) for x in exclude_words if x.strip())
    base_words = {normalize_text(x) for x in include_words}
    if base_words & {"телефон", "смартфон", "айфон", "iphone"}:
        blocked_words |= COMMON_BAD_WORDS

    for bad in blocked_words:
        if bad and bad in title_n:
            return False

    for word in include_words:
        word_n = normalize_text(word)
        if not word_n:
            continue
        if word_n not in title_tokens and word_n not in title_n:
            return False

    return True


def price_matches(price_text: str, min_price: Optional[int], max_price: Optional[int]) -> bool:
    price = parse_price(price_text)
    if price is None:
        return True
    if min_price is not None and price < min_price:
        return False
    if max_price is not None and price > max_price:
        return False
    return True


def is_valid_card(card: Dict, search: Dict) -> bool:
    title = card.get("title", "")
    url = card.get("url", "")
    price_text = card.get("price", "")
    city = card.get("city", "")

    if not title or not url:
        return False
    if not title_matches(title, search.get("include_words", []), search.get("exclude_words", [])):
        return False
    if not price_matches(price_text, search.get("min_price"), search.get("max_price")):
        return False
    if not city_matches(city, search.get("cities", [])):
        return False
    return True


async def fetch_cards(url: str) -> List[Dict]:
    async with http_semaphore:
        await asyncio.sleep(random.uniform(1.5, 3.5))
        response = await asyncio.to_thread(fetch_with_backoff, url)
        return parse_cards_from_html(response.text)


async def run_search_once(search: Dict) -> List[Dict]:
    url = search.get("url", "").strip()
    sent_ids = set(search.get("sent_ids", []))

    if not url:
        return []

    cards = await fetch_cards(url)
    new_items = []

    for card in cards:
        item_id = card.get("id") or extract_item_id(card.get("url", ""))
        if not is_valid_card(card, search):
            continue
        if item_id in sent_ids:
            continue

        sent_ids.add(item_id)
        new_items.append(card)

    search["sent_ids"] = list(sent_ids)[-1000:]
    return new_items


async def start_add_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_user:
        return

    state = ensure_state()
    draft = reset_draft(state, update.effective_user.id)
    draft["step"] = "await_url"
    save_state(state)

    cancel_keyboard = ReplyKeyboardMarkup(
        [[KeyboardButton("❌ Отмена")]],
        resize_keyboard=True,
    )

    await update.message.reply_text(
        "Шаг 1/6\nОтправь ссылку поиска Авито.\n\nПример:\nhttps://www.avito.ru/all?q=iphone",
        reply_markup=cancel_keyboard,
    )


async def handle_draft_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if not update.message or not update.effective_user:
        return False

    state = ensure_state()
    draft = get_user_draft(state, update.effective_user.id)
    step = draft.get("step")

    if not step:
        return False

    text = (update.message.text or "").strip()

    if text == "❌ Отмена":
        reset_draft(state, update.effective_user.id)
        save_state(state)
        await update.message.reply_text("Создание поиска отменено.", reply_markup=MAIN_MENU)
        return True

    if step == "await_url":
        if not text.startswith("http"):
            await update.message.reply_text("Ссылка должна начинаться с http или https. Пришли ссылку ещё раз.")
            return True

        draft["url"] = text
        draft["step"] = "await_name"
        save_state(state)
        await update.message.reply_text("Шаг 2/6\nНапиши название поиска.\nНапример: Айфон Москва")
        return True

    if step == "await_name":
        if len(text) < 2:
            await update.message.reply_text("Название слишком короткое. Напиши нормальное название.")
            return True

        draft["name"] = text
        draft["step"] = "await_include"
        save_state(state)
        await update.message.reply_text(
            "Шаг 3/6\nНапиши ключевые слова через пробел.\n\nПример:\niphone 15 pro"
        )
        return True

    if step == "await_include":
        words = tokenize(text)
        if not words:
            await update.message.reply_text("Нужны ключевые слова. Пример: iphone 15 pro")
            return True

        draft["include_words"] = words
        draft["step"] = "await_exclude"
        save_state(state)
        await update.message.reply_text(
            "Шаг 4/6\nНапиши минус-слова через запятую.\nЕсли не нужны — отправь 0\n\nПример:\nчехол, коробка, обмен, ремонт"
        )
        return True

    if step == "await_exclude":
        if text == "0":
            draft["exclude_words"] = []
        else:
            draft["exclude_words"] = [x.strip() for x in text.split(",") if x.strip()]

        draft["step"] = "await_price"
        save_state(state)
        await update.message.reply_text(
            "Шаг 5/6\nНапиши диапазон цены в формате:\nмин макс\n\nПример:\n30000 90000\n\nЕсли цена не важна — отправь 0"
        )
        return True

    if step == "await_price":
        min_price = None
        max_price = None

        if text != "0":
            parts = text.split()
            if len(parts) != 2 or not all(re.sub(r"[^\d]", "", p) for p in parts):
                await update.message.reply_text("Неверный формат. Пример: 30000 90000\nИли отправь 0")
                return True

            min_price = int(re.sub(r"[^\d]", "", parts[0]))
            max_price = int(re.sub(r"[^\d]", "", parts[1]))

        draft["min_price"] = min_price
        draft["max_price"] = max_price
        draft["step"] = "await_cities"
        save_state(state)
        await update.message.reply_text(
            "Шаг 6/6\nНапиши города через запятую.\nЕсли любые города — отправь 0\n\nПример:\nМосква, Санкт-Петербург"
        )
        return True

    if step == "await_cities":
        if text == "0":
            cities = []
        else:
            cities = [x.strip() for x in text.split(",") if x.strip()]

        searches = ensure_user_searches(state, update.effective_user.id)

        search = {
            "name": draft.get("name", "Без названия"),
            "url": draft.get("url", ""),
            "include_words": draft.get("include_words", []),
            "exclude_words": draft.get("exclude_words", []),
            "min_price": draft.get("min_price"),
            "max_price": draft.get("max_price"),
            "cities": cities,
            "sent_ids": [],
        }

        for existing in searches:
            if (
                normalize_text(existing.get("name", "")) == normalize_text(search["name"])
                or existing.get("url", "").strip() == search["url"].strip()
            ):
                reset_draft(state, update.effective_user.id)
                save_state(state)
                await update.message.reply_text(
                    "Такой поиск уже есть по названию или ссылке.",
                    reply_markup=MAIN_MENU,
                )
                return True

        searches.append(search)
        reset_draft(state, update.effective_user.id)
        save_state(state)

        await update.message.reply_text(
            "Поиск сохранён.\n\n" + format_search(search, len(searches)),
            parse_mode="HTML",
            reply_markup=MAIN_MENU,
        )
        return True

    return False


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    text = (
        "Бот для отслеживания новых объявлений.\n\n"
        "Что умеет:\n"
        "• добавление поиска через кнопки\n"
        "• фильтр по ключевым словам\n"
        "• минус-слова\n"
        "• фильтр по цене\n"
        "• фильтр по городам\n"
        "• проверка вручную и в фоне\n\n"
        "Нажми кнопку «➕ Добавить поиск»."
    )
    await update.message.reply_text(text, reply_markup=MAIN_MENU)


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    text = (
        "Кнопки:\n"
        "➕ Добавить поиск — создать новый поиск пошагово\n"
        "📋 Мои поиски — посмотреть и удалить поиски\n"
        "▶️ Проверить сейчас — прямо сейчас проверить все твои поиски\n"
        "🧹 Очистить все — удалить все поиски\n"
        "🛠 Админ — статус / деплой / рестарт\n\n"
        "Во время создания поиска можно отправить:\n"
        "❌ Отмена"
    )
    await update.message.reply_text(text, reply_markup=MAIN_MENU)


async def list_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_user:
        return

    state = ensure_state()
    searches = ensure_user_searches(state, update.effective_user.id)

    if not searches:
        await update.message.reply_text("У тебя пока нет поисков.", reply_markup=MAIN_MENU)
        return

    keyboard = []
    blocks = []

    for i, search in enumerate(searches, 1):
        blocks.append(format_search(search, i))
        keyboard.append(
            [InlineKeyboardButton(f"Удалить: {search.get('name', f'#{i}')}", callback_data=f"del:{i-1}")]
        )

    await update.message.reply_text(
        "\n\n".join(blocks),
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def clear_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_user:
        return

    state = ensure_state()
    state["users"][str(update.effective_user.id)] = []
    save_state(state)
    await update.message.reply_text("Все поиски удалены.", reply_markup=MAIN_MENU)


async def check_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_user:
        return

    state = ensure_state()
    searches = ensure_user_searches(state, update.effective_user.id)

    if not searches:
        await update.message.reply_text("У тебя нет поисков.", reply_markup=MAIN_MENU)
        return

    await update.message.reply_text("Запускаю проверку...")

    total_found = 0
    changed = False

    for search in searches:
        try:
            items = await run_search_once(search)
            if items:
                changed = True
                for item in items[:10]:
                    await update.message.reply_text(
                        format_item(item),
                        parse_mode="HTML",
                        disable_web_page_preview=False,
                    )
                total_found += len(items)
        except Exception as e:
            logger.exception("Ошибка проверки для %s", search.get("name"))
            await update.message.reply_text(
                f"Ошибка в поиске {html_escape(search.get('name', 'Без названия'))}: {html_escape(str(e))}",
                parse_mode="HTML",
            )

    if changed:
        save_state(state)

    if total_found == 0:
        await update.message.reply_text("Новых подходящих объявлений не найдено.", reply_markup=MAIN_MENU)
    else:
        await update.message.reply_text(f"Найдено новых объявлений: {total_found}", reply_markup=MAIN_MENU)


async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_user:
        return

    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Нет доступа.", reply_markup=MAIN_MENU)
        return

    keyboard = [
        [InlineKeyboardButton("📊 Статус", callback_data="admin:status")],
        [InlineKeyboardButton("🔄 Обновить", callback_data="admin:deploy")],
        [InlineKeyboardButton("♻️ Перезапуск", callback_data="admin:restart")],
    ]
    await update.message.reply_text(
        "Админ-панель",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def buttons_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    text = (update.message.text or "").strip()

    handled = await handle_draft_message(update, context)
    if handled:
        return

    if text == "➕ Добавить поиск":
        await start_add_search(update, context)
        return

    if text == "📋 Мои поиски":
        await list_cmd(update, context)
        return

    if text == "▶️ Проверить сейчас":
        await check_cmd(update, context)
        return

    if text == "🧹 Очистить все":
        await clear_cmd(update, context)
        return

    if text == "❓ Помощь":
        await help_cmd(update, context)
        return

    if text == "🛠 Админ":
        await admin_cmd(update, context)
        return

    await update.message.reply_text("Не понял. Используй кнопки ниже.", reply_markup=MAIN_MENU)


async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if not query.from_user:
        return

    data = query.data or ""

    if data.startswith("del:"):
        try:
            index = int(data.split(":", 1)[1])
        except Exception:
            await query.edit_message_text("Ошибка удаления.")
            return

        state = ensure_state()
        searches = ensure_user_searches(state, query.from_user.id)

        if not (0 <= index < len(searches)):
            await query.edit_message_text("Поиск уже удалён или индекс неверный.")
            return

        removed = searches.pop(index)
        save_state(state)
        await query.edit_message_text(f"Удалил поиск: {removed.get('name', 'Без названия')}")
        return

    if data.startswith("admin:"):
        if not is_admin(query.from_user.id):
            await query.edit_message_text("Нет доступа.")
            return

        action = data.split(":", 1)[1]

        if action == "status":
            result = subprocess.run(
                ["systemctl", "is-active", "avito-bot"],
                capture_output=True,
                text=True,
            )
            status = result.stdout.strip() or "unknown"
            await query.edit_message_text(f"Статус бота: {status}")
            return

        if action == "deploy":
            subprocess.Popen(["/bin/bash", "/root/avito-bot/deploy.sh"])
            await query.edit_message_text("Обновление запущено.")
            return

        if action == "restart":
            subprocess.Popen(["systemctl", "restart", "avito-bot"])
            await query.edit_message_text("Перезапуск выполнен.")
            return


async def background_checker(app: Application):
    while True:
        try:
            state = ensure_state()
            changed = False

            for user_id, searches in state.get("users", {}).items():
                for search in searches:
                    try:
                        items = await run_search_once(search)
                        if items:
                            changed = True
                            for item in items[:10]:
                                await app.bot.send_message(
                                    chat_id=int(user_id),
                                    text=format_item(item),
                                    parse_mode="HTML",
                                    disable_web_page_preview=False,
                                )
                    except Exception:
                        logger.exception(
                            "Ошибка фоновой проверки для user_id=%s search=%s",
                            user_id,
                            search.get("name"),
                        )

            if changed:
                save_state(state)

        except Exception:
            logger.exception("Ошибка фоновой проверки")

        await asyncio.sleep(CHECK_INTERVAL_SECONDS)


async def post_init(app: Application):
    app.create_task(background_checker(app))


def main():
    application = Application.builder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start_cmd))
    application.add_handler(CommandHandler("help", help_cmd))
    application.add_handler(CommandHandler("list", list_cmd))
    application.add_handler(CommandHandler("check", check_cmd))
    application.add_handler(CommandHandler("clear", clear_cmd))
    application.add_handler(CommandHandler("admin", admin_cmd))
    application.add_handler(CallbackQueryHandler(callback_router))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, buttons_router))

    application.post_init = post_init
    application.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()