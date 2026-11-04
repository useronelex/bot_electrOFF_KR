import asyncio
import json
import logging
import os
import random
import re
import threading
import time
from collections import defaultdict, deque
from datetime import date, datetime, timedelta, timezone

import httpx
from bs4 import BeautifulSoup
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.error import RetryAfter

logger = logging.getLogger("powercut")

# ──────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID = int(os.environ.get("POWERCUT_CHAT_ID"))
ADMIN_USER_ID = int(os.environ.get("ADMIN_USER_ID") or "0")
CHANNEL = os.environ.get("POWERCUT_CHANNEL", "pat_cherkasyoblenergo")
# Бот парсить УСІ черги (для сторінки), а в групи й користувачам (сповіщення, /graph) виводить лише ці.
# Змінити можна тут або змінною POWERCUT_QUEUES на Render, напр.: 1.1,2.1,3.1,4.2
DISPLAY_QUEUES = ["2.1", "3.1"]
QUEUES = tuple(q.strip() for q in os.environ.get("POWERCUT_QUEUES", ",".join(DISPLAY_QUEUES)).split(",") if q.strip())
POLL_INTERVAL = max(5.0, float(os.environ.get("POWERCUT_POLL_SEC") or "10"))
STATE_PATH = os.environ.get("POWERCUT_STATE_PATH", "powercut_state.json")
NOTIFY_ON_FIRST_RUN = os.environ.get("POWERCUT_NOTIFY_FIRST_RUN", "0") == "1"
PAGE_URL = os.environ.get("POWERCUT_PAGE_URL", "https://bot-v2-n8wt.onrender.com/schedule")
MINIAPP_URL = os.environ.get("POWERCUT_MINIAPP_URL", "")  # https://t.me/<бот>/<короткa_назва> — для кнопки в групах
BUTTON_ENABLED = os.environ.get("POWERCUT_BUTTON", "1") == "1"  # кнопка "Відкрити графік" під повідомленнями з графіком
PAGE_MSG_TTL = int(os.environ.get("POWERCUT_PAGE_TTL") or "60")  # через скільки сек. прибрати повідомлення з кнопкою (0 = не прибирати)
ALERT_AFTER_SEC = 300  # якщо канал недоступний стільки часів поспіль — сповістити адміна

URL = f"https://t.me/s/{CHANNEL}"

try:
    from zoneinfo import ZoneInfo
    KYIV = ZoneInfo("Europe/Kyiv")
except Exception:  # немає tzdata
    KYIV = timezone(timedelta(hours=3))

MONTHS = {
    "січня": 1, "лютого": 2, "березня": 3, "квітня": 4, "травня": 5, "червня": 6,
    "липня": 7, "серпня": 8, "вересня": 9, "жовтня": 10, "листопада": 11, "грудня": 12,
}
MONTH_NAMES = {v: k for k, v in MONTHS.items()}

_DATE_TEXT = re.compile(r"(\d{1,2})\s+(" + "|".join(MONTHS) + r")", re.IGNORECASE)
_DATE_NUM = re.compile(r"(\d{1,2})\.(\d{2})\.(\d{4})")
_QUEUE_LINE = re.compile(r"^\s*(\d\.\d)\s+(.*)$")
_INTERVAL = re.compile(r"(\d{1,2}):(\d{2})\s*(?:[-–—]|до)\s*(\d{1,2}):(\d{2})")

# статус для /health
_status = {"started": None, "last_ok": None, "last_error": None, "restarts": 0}

# кеш останньої успішної вичитки каналу (використовує /graph, щоб не ходити в t.me зайвий раз)
_cache: dict = {"texts": None, "ts": 0.0}
CACHE_MAX_AGE = 120        # сек: старіший кеш не використовуємо
GRAPH_COOLDOWN = 10        # сек: мінімальна пауза між відповідями /graph в одному чаті
_last_graph: dict[int, float] = {}

# облік повідомлень, які надіслав цей модуль (для /del); bot.py веде свій — додається через add_registry()
_sent: dict[int, deque] = defaultdict(lambda: deque(maxlen=200))
_extra_registries: list = []


def _record(chat_id: int, message_id: int) -> None:
    _sent[chat_id].append(message_id)


def add_registry(registry: dict) -> None:
    """Підключити чужий облік повідомлень бота ({chat_id: deque[message_id]}), напр. bot._sent_messages."""
    if registry not in _extra_registries:
        _extra_registries.append(registry)


class _RateLimited(Exception):
    def __init__(self, retry_after: float):
        super().__init__(f"429, retry after {retry_after}s")
        self.retry_after = retry_after


# ──────────────────────────────────────────
# ПАРСИНГ
# ──────────────────────────────────────────
def _now_kyiv() -> datetime:
    return datetime.now(KYIV)


def _parse_date(text: str, today: date) -> date | None:
    m = _DATE_TEXT.search(text)
    try:
        if m:
            day, month = int(m.group(1)), MONTHS[m.group(2).lower()]
            year = today.year
            if month == 1 and today.month == 12:
                year += 1
            elif month == 12 and today.month == 1:
                year -= 1
            return date(year, month, day)
        m = _DATE_NUM.search(text)
        if m:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    except ValueError:
        return None
    return None


def _normalize(pairs: list[list[int]]) -> list[list[int]]:
    """Сортує інтервали й зливає ті, що перетинаються або йдуть впритул."""
    out: list[list[int]] = []
    for s, e in sorted(p for p in pairs if p[0] < p[1]):
        if out and s <= out[-1][1]:
            out[-1][1] = max(out[-1][1], e)
        else:
            out.append([s, e])
    return out


def parse_message(text: str, today: date) -> tuple[date, dict[str, list[list[int]]]] | None:
    """Повертає (дата, {черга: [[початок_хв, кінець_хв], ...]}) лише для потрібних черг."""
    day = _parse_date(text, today)
    if not day:
        return None
    queues: dict[str, list[list[int]]] = {}
    for line in text.splitlines():
        m = _QUEUE_LINE.match(line)
        if not m:
            continue
        pairs = []
        for h1, m1, h2, m2 in _INTERVAL.findall(m.group(2)):
            h1, m1, h2, m2 = int(h1), int(m1), int(h2), int(m2)
            if h1 <= 24 and h2 <= 24 and m1 < 60 and m2 < 60:
                start, end = h1 * 60 + m1, h2 * 60 + m2
                if end == 0 and start > 0:  # "23:00 - 00:00" → кінець о півночі
                    end = 1440
                elif 0 < end < start:  # інтервал через північ ("22:00 - 02:00"): на цю добу рахуємо до 24:00
                    end = 1440
                pairs.append([start, end])
        if pairs:
            queues.setdefault(m.group(1), []).extend(pairs)
    queues = {q: _normalize(p) for q, p in queues.items()}
    return (day, queues) if queues else None


def build_current_ts(msgs: list[tuple[str, float | None]], today: date):
    """Те саме, що build_current, але ще повертає час публікації повідомлення для кожної дати/черги."""
    current: dict[date, dict[str, list[list[int]]]] = {}
    times: dict[date, dict[str, float | None]] = {}
    for text, ts in msgs:
        parsed = parse_message(text, today)
        if parsed:
            d, queues = parsed
            current.setdefault(d, {}).update(queues)
            for q in queues:
                times.setdefault(d, {})[q] = ts
    return current, times


def build_current(texts: list[str], today: date) -> dict[date, dict[str, list[list[int]]]]:
    """Останнє (найновіше) повідомлення для дати перекриває попередні — по кожній черзі."""
    return build_current_ts([(t, None) for t in texts], today)[0]


# ──────────────────────────────────────────
# ФОРМАТУВАННЯ
# ──────────────────────────────────────────
def _t(x: int) -> str:
    return f"{x // 60:02d}:{x % 60:02d}"


def _fmt(intervals: list[list[int]] | None) -> str:
    if intervals is None:
        return "—"
    if not intervals:
        return "відключень немає"
    return ", ".join(f"{_t(s)}–{_t(e)}" for s, e in intervals)


def _fmt_date(d: date) -> str:
    return f"{d.day} {MONTH_NAMES[d.month]}"


def fmt_new(d: date, queues: dict, title: str = "⚡ Графік відключень на") -> str:
    lines = [f"{title} {_fmt_date(d)}", ""]
    lines += [f"{q}: {_fmt(queues[q])}" for q in QUEUES if q in queues]
    return "\n".join(lines)


def fmt_changed(d: date, old: dict, new: dict, diff: list[str]) -> str:
    lines = [f"🔄 Зміна графіка на {_fmt_date(d)}", ""]
    for q in diff:
        lines += [f"{q}:", f"  було: {_fmt(old.get(q))}", f"  стало: {_fmt(new.get(q))}", ""]
    same = [q for q in QUEUES if (q in new or q in old) and q not in diff]
    if same:
        lines.append("Без змін: " + ", ".join(same))
    return "\n".join(lines).strip()


# ──────────────────────────────────────────
# МИНУЛЕ НЕ ВАЖЛИВЕ: порівнюємо лише те, що ще не відбулося
# ──────────────────────────────────────────
def _cmp(intervals, d: date, today: date, now_min: int):
    """Для порівняння: сьогодні — лише від поточної хвилини, майбутні дні — повністю."""
    if intervals is None:
        return []
    if d > today:
        return [list(x) for x in intervals]
    return [[max(s, now_min), e] for s, e in intervals if e > now_min]


def _not_past(intervals, d: date, today: date, now_min: int):
    """Для показу: відкидаємо лише інтервали, що повністю закінчилися (триваючі лишаємо цілими)."""
    if intervals is None:
        return None
    if d > today:
        return [list(x) for x in intervals]
    return [list(x) for x in intervals if x[1] > now_min]


# ──────────────────────────────────────────
# СТАН
# ──────────────────────────────────────────
def _load_state() -> dict:
    try:
        with open(STATE_PATH, encoding="utf-8") as f:
            st = json.load(f)
        st.setdefault("schedules", {})
        st.setdefault("initialized", True)
        return st
    except FileNotFoundError:
        return {"initialized": False, "schedules": {}}
    except Exception as e:
        logger.error(f"Не вдалося прочитати стан, починаю з нуля: {e}")
        return {"initialized": False, "schedules": {}}


def _save_state(state: dict) -> None:
    try:
        tmp = STATE_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False)
        os.replace(tmp, STATE_PATH)
    except Exception as e:
        logger.error(f"Не вдалося зберегти стан: {e}")


# ──────────────────────────────────────────
# МЕРЕЖА / TELEGRAM
# ──────────────────────────────────────────
async def _fetch_messages(client: httpx.AsyncClient) -> list[tuple[str, float | None]]:
    """[(текст, час_публікації_unix | None), ...] від найстарішого до найновішого."""
    r = await client.get(URL)
    if r.status_code == 429:
        try:
            ra = float(r.headers.get("Retry-After", "60"))
        except ValueError:
            ra = 60.0
        raise _RateLimited(ra)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    msgs: list[tuple[str, float | None]] = []

    def _text(el) -> str:
        for br in el.find_all("br"):
            br.replace_with("\n")
        return el.get_text()

    for wrap in soup.select("div.tgme_widget_message"):
        el = wrap.select_one(".tgme_widget_message_text")
        if not el:
            continue
        ts = None
        t = wrap.select_one("a.tgme_widget_message_date time[datetime]") or wrap.select_one("time[datetime]")
        if t:
            try:
                ts = datetime.fromisoformat(t["datetime"]).timestamp()
            except Exception:
                ts = None
        msgs.append((_text(el), ts))
    if not msgs:  # запасний розбір без часу
        msgs = [(_text(el), None) for el in soup.select("div.tgme_widget_message_text")]
    if not msgs:
        raise RuntimeError("На сторінці каналу не знайдено повідомлень")
    return msgs


async def _fetch_texts(client: httpx.AsyncClient) -> list[str]:
    return [t for t, _ in await _fetch_messages(client)]


def _page_markup(private: bool):
    """Кнопка «Відкрити графік»: у приваті — міні-застосунок, у групах (Telegram не дозволяє web_app) — посилання."""
    if not BUTTON_ENABLED:
        return None
    label = "📊 Відкрити графік"
    if private:
        btn = InlineKeyboardButton(label, web_app=WebAppInfo(url=PAGE_URL))
    else:
        btn = InlineKeyboardButton(label, url=MINIAPP_URL or PAGE_URL)
    return InlineKeyboardMarkup([[btn]])


async def _send(bot: Bot, chat_id: int, text: str, reply_markup=None) -> bool:
    for _ in range(3):
        try:
            sent = await bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup)
            _record(chat_id, sent.message_id)
            return True
        except RetryAfter as e:
            ra = e.retry_after
            await asyncio.sleep((ra.total_seconds() if hasattr(ra, "total_seconds") else float(ra)) + 1)
        except Exception as e:
            logger.error(f"Помилка відправки в {chat_id}: {e}")
            await asyncio.sleep(2)
    return False


async def _process(bot: Bot, texts: list[str], state: dict, today: date, now_min: int | None = None) -> None:
    if now_min is None:
        kn = _now_kyiv()
        now_min = kn.hour * 60 + kn.minute
    schedules: dict = state["schedules"]

    for k in list(schedules):  # прибираємо минулі дні
        try:
            if date.fromisoformat(k) < today:
                del schedules[k]
        except ValueError:
            del schedules[k]

    first_run = not state.get("initialized")
    dirty = False

    for d, queues in sorted(build_current(texts, today).items()):
        if d < today:
            continue
        key = d.isoformat()
        old = schedules.get(key)

        if old is None:
            if first_run and not NOTIFY_ON_FIRST_RUN:
                schedules[key] = queues
                dirty = True
                continue
            shown = {q: _not_past(iv, d, today, now_min) for q, iv in queues.items() if q in QUEUES}
            if not any(shown.values()):  # увесь графік уже в минулому — просто запам'ятовуємо
                schedules[key] = queues
                dirty = True
                continue
            msg = fmt_new(d, shown)
        else:
            diff = [
                q for q in QUEUES
                if q in queues and _cmp(old.get(q), d, today, now_min) != _cmp(queues[q], d, today, now_min)
            ]
            if not diff:
                # змінилося лише минуле (або нічого) — тихо оновлюємо збережені дані, без сповіщення
                merged = {**old, **queues}
                if merged != old:
                    schedules[key] = merged
                    dirty = True
                continue
            names = [q for q in QUEUES if q in old or q in queues]
            old_show = {q: _not_past(old.get(q), d, today, now_min) for q in names}
            new_show = {q: _not_past(queues.get(q, old.get(q)), d, today, now_min) for q in names}
            msg = fmt_changed(d, old_show, new_show, diff)

        if await _send(bot, CHAT_ID, msg, _page_markup(CHAT_ID > 0)):
            logger.info(f"Надіслано сповіщення про графік на {key}")
            schedules[key] = {**(old or {}), **queues}
            dirty = True
        else:
            logger.error(f"Не вдалося надіслати сповіщення на {key}, повторю наступного циклу")

    if first_run:
        state["initialized"] = True
        dirty = True
    if dirty:
        _save_state(state)


# ──────────────────────────────────────────
# КОМАНДА /graph (тільки адмін)
# ──────────────────────────────────────────
async def current_schedules_text() -> str:
    """Поточні графіки (на сьогодні й далі) одним текстом. Бере кеш парсера або свіжу вичитку."""
    texts = _cache["texts"] if time.time() - _cache["ts"] <= CACHE_MAX_AGE else None
    if texts is None:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                                 "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"}
        async with httpx.AsyncClient(headers=headers, timeout=15, follow_redirects=True) as client:
            texts = await _fetch_texts(client)
    today = _now_kyiv().date()
    current = {}
    for d, qs in build_current(texts, today).items():
        shown = {q: iv for q, iv in qs.items() if q in QUEUES}
        if d >= today and shown:
            current[d] = shown
    if not current:
        return "Актуальних графіків для черг " + ", ".join(QUEUES) + " у каналі не знайдено."
    return "\n\n".join(fmt_new(d, q, "📋 Поточний графік на") for d, q in sorted(current.items()))


async def _delete_quietly(msg) -> None:
    try:
        await msg.delete()
    except Exception:
        pass  # немає права видаляти або повідомлення вже зникло


async def _notice(bot, chat_id: int, text: str, ttl: int = 5) -> None:
    """Коротке службове повідомлення, яке саме зникає."""
    try:
        m = await bot.send_message(chat_id=chat_id, text=text)
        await asyncio.sleep(ttl)
        await bot.delete_message(chat_id=chat_id, message_id=m.message_id)
    except Exception:
        pass


async def cmd_graph(update, context) -> None:
    """/graph — доступна всім. Команду користувача видаляємо, графік надсилаємо в той самий чат."""
    msg = update.effective_message
    if not msg:
        return
    chat_id = msg.chat_id
    await _delete_quietly(msg)

    now = time.time()
    if now - _last_graph.get(chat_id, 0) < GRAPH_COOLDOWN:
        return  # захист від спаму: мовчки ігноруємо
    _last_graph[chat_id] = now
    try:
        text = await current_schedules_text()
    except Exception as e:
        logger.error(f"/graph: {e}")
        await _notice(context.bot, chat_id, "Не вдалося отримати графік, спробуйте за хвилину.")
        return
    chat = update.effective_chat
    markup = _page_markup(chat is not None and chat.type == "private")
    sent = await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=markup)
    _record(chat_id, sent.message_id)


async def cmd_del(update, context) -> None:
    """/del — видалити останнє повідомлення бота в цьому чаті (графік, сповіщення, відео). Доступна всім."""
    msg = update.effective_message
    if not msg:
        return
    chat_id = msg.chat_id
    await _delete_quietly(msg)

    registries = [_sent, *_extra_registries]
    for _ in range(5):
        best = None  # (message_id, registry)
        for reg in registries:
            dq = reg.get(chat_id)
            if dq:
                mid = max(dq)
                if best is None or mid > best[0]:
                    best = (mid, reg)
        if best is None:
            break
        mid, reg = best
        try:
            reg[chat_id].remove(mid)
        except ValueError:
            pass
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=mid)
            return
        except Exception as e:  # вже видалено вручну / надто старе — пробуємо попереднє
            logger.warning(f"/del: не вдалося видалити {mid}: {e}")
    await _notice(context.bot, chat_id, "Немає повідомлень бота для видалення.")


_bg_tasks: set = set()


async def _delete_later(bot, chat_id: int, message_id: int, ttl: float) -> None:
    await asyncio.sleep(ttl)
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass


async def cmd_web(update, context) -> None:
    """/web — команду видаляємо, натомість кидаємо кнопку, що відкриває сторінку у вбудованому браузері."""
    msg = update.effective_message
    if not msg:
        return
    chat_id = msg.chat_id
    chat = update.effective_chat
    await _delete_quietly(msg)

    label = "📊 Відкрити графік"
    if chat is not None and chat.type == "private":
        button = InlineKeyboardButton(label, web_app=WebAppInfo(url=PAGE_URL))  # міні-застосунок у приваті
    else:
        # у групах web_app-кнопки Telegram не дозволяє; пряме посилання на міні-застосунок відкриє його як застосунок
        button = InlineKeyboardButton(label, url=MINIAPP_URL or PAGE_URL)
    sent = await context.bot.send_message(
        chat_id=chat_id, text="⚡ Графік відключень", reply_markup=InlineKeyboardMarkup([[button]])
    )
    _record(chat_id, sent.message_id)
    if PAGE_MSG_TTL > 0:  # у фоні, щоб не тримати вебхук відкритим
        task = asyncio.create_task(_delete_later(context.bot, chat_id, sent.message_id, PAGE_MSG_TTL))
        _bg_tasks.add(task)
        task.add_done_callback(_bg_tasks.discard)


# ──────────────────────────────────────────
# ГОЛОВНИЙ ЦИКЛ
# ──────────────────────────────────────────
async def _monitor(bot: Bot) -> None:
    global _state_ref
    state = _load_state()
    _state_ref = state
    _publish_api_from_state(state)
    fails = 0
    fail_since: float | None = None
    alerted = False
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
        "Cache-Control": "no-cache",
        "Accept-Language": "uk,en;q=0.8",
    }
    logger.info(f"Моніторинг запущено: {URL} | черги={QUEUES} | чат={CHAT_ID} | інтервал={POLL_INTERVAL}с")

    async with httpx.AsyncClient(headers=headers, timeout=15, follow_redirects=True) as client:
        while True:
            delay = POLL_INTERVAL * random.uniform(0.8, 1.2)
            try:
                msgs = await _fetch_messages(client)
                texts = [t for t, _ in msgs]
                _cache["texts"], _cache["ts"] = texts, time.time()
                _publish_api(msgs)
                if fails:
                    logger.info(f"Канал знову доступний після {fails} помилок")
                fails, fail_since = 0, None
                _status["last_ok"] = time.time()
                if alerted and ADMIN_USER_ID:
                    await _send(bot, ADMIN_USER_ID, "✅ Моніторинг каналу відновлено.")
                alerted = False
                kn = _now_kyiv()
                await _process(bot, texts, state, kn.date(), kn.hour * 60 + kn.minute)
            except asyncio.CancelledError:
                raise
            except _RateLimited as e:
                fails += 1
                fail_since = fail_since or time.time()
                delay = max(e.retry_after, 60)
                logger.warning(f"t.me обмежив запити, пауза {delay:.0f}с")
            except Exception as e:
                fails += 1
                fail_since = fail_since or time.time()
                _status["last_error"] = f"{type(e).__name__}: {e}"
                delay = min(POLL_INTERVAL * 2 ** min(fails, 6), 120)
                logger.warning(f"Помилка опитування ({fails}): {e}")

            if fail_since and not alerted and ADMIN_USER_ID and time.time() - fail_since >= ALERT_AFTER_SEC:
                alerted = await _send(
                    bot, ADMIN_USER_ID,
                    f"⚠️ Не вдається прочитати канал {CHANNEL} вже понад {ALERT_AFTER_SEC // 60} хв.\n"
                    f"Остання помилка: {_status['last_error']}",
                )
            await asyncio.sleep(delay)


async def _supervisor() -> None:
    """Перезапускає моніторинг після будь-якого збою."""
    backoff = 5
    while True:
        started = time.monotonic()
        try:
            async with Bot(BOT_TOKEN) as bot:
                await _monitor(bot)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            _status["last_error"] = f"{type(e).__name__}: {e}"
            _status["restarts"] += 1
            logger.error(f"Моніторинг впав, перезапуск через {backoff}с: {e}", exc_info=True)
        if time.monotonic() - started > 60:
            backoff = 5
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 300)


def _thread_main() -> None:
    while True:  # на випадок, якщо впаде навіть сам asyncio.run
        try:
            asyncio.run(_supervisor())
        except Exception as e:
            logger.error(f"Потік powercut впав, перезапуск: {e}", exc_info=True)
            time.sleep(5)


_thread: threading.Thread | None = None


def start_in_background() -> None:
    """Запускає моніторинг в окремому потоці зі своїм event loop."""
    global _thread
    if _thread and _thread.is_alive():
        return
    if not BOT_TOKEN:
        logger.warning("BOT_TOKEN не встановлено — powercut не запущено")
        return
    _status["started"] = time.time()
    _thread = threading.Thread(target=_thread_main, name="powercut", daemon=True)
    _thread.start()


# ──────────────────────────────────────────
# ДАНІ ДЛЯ ВЕБ-СТОРІНКИ (/api/schedule)
# ──────────────────────────────────────────
_api: dict = {"data": None}
_state_ref: dict | None = None  # посилання на стан моніторингу (тут зберігаємо історію минулих інтервалів)


def _remember_past(today: date, now_min: int, current: dict) -> dict:
    """Якщо канал прибрав інтервал, який уже закінчився, — лишаємо його в історії (для кола на сторінці).
    Інтервали, прибрані до свого завершення (справжня зміна графіка), НЕ зберігаються."""
    st = _state_ref
    if st is None:
        return {}
    hist = st.setdefault("hist", {})
    key = today.isoformat()
    for k in [k for k in hist if k < key]:
        del hist[k]
    prev = st.get("schedules", {}).get(key) or {}
    new = current.get(today, {})
    h = hist.setdefault(key, {})
    changed = False
    for q, ivs in prev.items():
        for iv in ivs:
            if iv[1] <= now_min and iv not in new.get(q, []) and iv not in h.setdefault(q, []):
                h[q].append(list(iv))
                changed = True
    if changed:
        _save_state(st)
    return h


def _ms(ts: float | None) -> int | None:
    return int(ts * 1000) if ts else None


def _qkey(q: str) -> tuple:
    return tuple(int(x) for x in q.split("."))


def build_api(current: dict, times: dict, today: date, checked_at: float | None, hist: dict | None = None) -> dict:
    days: dict = {}
    for d, qs in sorted(current.items()):
        if d < today or d > today + timedelta(days=2):
            continue
        days[d.isoformat()] = {
            q: {
                "slots": [{"s": s, "e": e, "t": "off"} for s, e in iv],
                "updatedAt": _ms(times.get(d, {}).get(q)),
            }
            for q, iv in qs.items()
        }
    if hist:  # інтервали, що вже відбулись, але яких канал більше не публікує (він обрізає минулі години)
        day = days.setdefault(today.isoformat(), {})
        for q, ivs in hist.items():
            rec = day.setdefault(q, {"slots": [], "updatedAt": None})
            have = {(x["s"], x["e"]) for x in rec["slots"]}
            rec["slots"] += [{"s": a, "e": b, "t": "off"} for a, b in ivs if (a, b) not in have]
            rec["slots"].sort(key=lambda x: x["s"])
    found = {q for qs in days.values() for q in qs}
    return {
        "today": today.isoformat(),
        "queues": sorted(set(QUEUES) | found, key=_qkey),
        "displayQueues": list(QUEUES),
        "days": days,
        "checkedAt": _ms(checked_at),
        "pollSec": POLL_INTERVAL,
    }


def _publish_api(msgs: list[tuple[str, float | None]]) -> None:
    try:
        kn = _now_kyiv()
        today = kn.date()
        current, times = build_current_ts(msgs, today)
        hist = _remember_past(today, kn.hour * 60 + kn.minute, current)
        _api["data"] = build_api(current, times, today, time.time(), hist)
    except Exception as e:
        logger.warning(f"Не вдалося оновити дані для веб-сторінки: {e}")


def _publish_api_from_state(state: dict) -> None:
    """Одразу після старту (до першої вичитки каналу) віддаємо те, що збережено в стані."""
    try:
        today = _now_kyiv().date()
        current = {date.fromisoformat(k): v for k, v in state.get("schedules", {}).items()}
        _api["data"] = build_api(current, {}, today, None, state.get("hist", {}).get(today.isoformat()))
    except Exception as e:
        logger.warning(f"Не вдалося підготувати дані зі стану: {e}")


def api_data() -> dict:
    data = _api["data"]
    out = dict(data) if data else {
        "today": _now_kyiv().date().isoformat(), "queues": sorted(QUEUES, key=_qkey), "displayQueues": list(QUEUES), "days": {},
        "checkedAt": None, "pollSec": POLL_INTERVAL,
    }
    out["ready"] = bool(data)
    out["serverTime"] = int(time.time() * 1000)
    return out


def status() -> dict:
    return {
        "running": bool(_thread and _thread.is_alive()),
        "last_ok_sec_ago": round(time.time() - _status["last_ok"]) if _status["last_ok"] else None,
        "last_error": _status["last_error"],
        "restarts": _status["restarts"],
        "miniapp_url": MINIAPP_URL or None,  # якщо None — кнопка /web у групі веде на звичайний сайт
    }


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
    asyncio.run(_supervisor())
