"""Бот графіків відключень: підписка через /start, сповіщення про нові графіки та їх зміни."""
import asyncio
import json
import logging
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta

import httpx
from telegram import Bot, BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, Update, WebAppInfo
from telegram.error import BadRequest, Forbidden, NetworkError, RetryAfter
from telegram.ext import Application, CommandHandler, ContextTypes

import graph

logger = logging.getLogger("bot")

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "").rstrip("/")
ADMIN_USER_ID = int(os.environ.get("ADMIN_USER_ID") or 0)  # необов'язково: особисті сповіщення про збої джерела
POLL_INTERVAL = max(10.0, float(os.environ.get("POLL_SEC") or 30))
STATE_PATH = os.environ.get("STATE_PATH", "state.json")
PAGE_URL = os.environ.get("PAGE_URL") or (f"{WEBHOOK_URL}/schedule" if WEBHOOK_URL else "")
OUTAGE_ALERT_SEC = 300
MINIAPP_URL = os.environ.get("MINIAPP_URL", "")  # https://t.me/<бот>/<назва> — кнопка в групах

_status: dict = {"last_ok": None, "last_error": None}
_api: dict = {"data": None}
_times: graph.Times = {}
_tasks: set[asyncio.Task] = set()


# ──────────────────────────────────────────
# СТАН: підписники, останні відомі графіки
# ──────────────────────────────────────────
REDIS_URL = os.environ.get("UPSTASH_REDIS_REST_URL", "").rstrip("/")
REDIS_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")
REDIS_KEY = os.environ.get("STATE_KEY", "bot_state")
_writer = ThreadPoolExecutor(max_workers=1)  # одна черга запису: порядок збережено, цикл не блокується


def _redis(*cmd: str):
    r = httpx.post(REDIS_URL, json=list(cmd), headers={"Authorization": f"Bearer {REDIS_TOKEN}"}, timeout=10)
    r.raise_for_status()
    return r.json().get("result")


def _load() -> dict:
    try:
        if REDIS_URL:
            st = json.loads(_redis("GET", REDIS_KEY) or "{}")
        else:
            with open(STATE_PATH, encoding="utf-8") as f:
                st = json.load(f)
    except FileNotFoundError:
        st = {}
    except (OSError, ValueError, httpx.HTTPError) as e:
        logger.error(f"Не вдалося прочитати стан, починаю з нуля: {e}")
        st = {}
    return {
        "users": set(st.get("users", [])),
        "schedules": st.get("schedules", {}),  # {"2026-11-04": {"4.2": [[с, к], ...]}}
    }


def _write(data: str) -> None:
    try:
        if REDIS_URL:
            _redis("SET", REDIS_KEY, data)
        else:
            with open(STATE_PATH + ".tmp", "w", encoding="utf-8") as f:
                f.write(data)
            os.replace(STATE_PATH + ".tmp", STATE_PATH)
    except (OSError, httpx.HTTPError) as e:
        logger.error(f"Не вдалося зберегти стан: {e}")


def _save() -> None:
    _writer.submit(_write, json.dumps({**state, "users": sorted(state["users"])}, ensure_ascii=False))


state = _load()


# ──────────────────────────────────────────
# ВІДПРАВКА
# ──────────────────────────────────────────
def _markup(private: bool) -> InlineKeyboardMarkup | None:
    """Кнопка «Відкрити графік»: у приваті — міні-застосунок, у групах (web_app заборонено) — посилання."""
    if not PAGE_URL:
        return None
    label = "📊 Відкрити графік"
    btn = (InlineKeyboardButton(label, web_app=WebAppInfo(url=PAGE_URL)) if private
           else InlineKeyboardButton(label, url=MINIAPP_URL or PAGE_URL))
    return InlineKeyboardMarkup([[btn]])


async def send(bot: Bot, chat_id: int, text: str, button: bool = False) -> bool:
    for _ in range(3):
        try:
            await bot.send_message(chat_id, text, reply_markup=_markup(chat_id > 0) if button else None)
            return True
        except RetryAfter as e:
            ra = e.retry_after
            await asyncio.sleep((ra.total_seconds() if hasattr(ra, "total_seconds") else ra) + 1)
        except (Forbidden, BadRequest):  # бота заблокували / чат видалено
            if chat_id in state["users"]:
                state["users"].discard(chat_id)
                _save()
                logger.info(f"Підписник {chat_id} недоступний — відписано")
            return False
        except NetworkError as e:
            logger.warning(f"Помилка відправки в {chat_id}: {e}")
            await asyncio.sleep(2)
    return False


async def broadcast(bot: Bot, text: str, button: bool = False) -> None:
    sem = asyncio.Semaphore(10)  # ліміт Telegram ≈ 30 повідомлень/с

    async def one(chat_id: int) -> None:
        async with sem:
            await send(bot, chat_id, text, button)

    await asyncio.gather(*(one(c) for c in list(state["users"])))


# ──────────────────────────────────────────
# ЛОГІКА: нові графіки та зміни
# ──────────────────────────────────────────
async def check_changes(bot: Bot, current: graph.Schedule, today: date, now_min: int) -> None:
    """У стан пишемо всі черги (для сторінки), а сповіщаємо лише про graph.QUEUES."""
    sch = state["schedules"]
    for k in [k for k in sch if k < today.isoformat()]:  # прибираємо минулі дні
        del sch[k]
    dirty = False
    for d, qs in sorted(current.items()):
        if d < today or not qs:
            continue
        key = d.isoformat()
        old = sch.get(key)
        notify = {q: iv for q, iv in qs.items() if q in graph.QUEUES}
        msg = None
        if old is None:
            shown = {q: graph.upcoming(iv, d, today, now_min) for q, iv in notify.items()}
            if any(shown.values()):  # якщо весь графік уже в минулому — просто запам'ятовуємо
                msg = graph.fmt_day(d, shown)
        else:
            diff = [q for q in notify
                    if graph.future(old.get(q), d, today, now_min) != graph.future(notify[q], d, today, now_min)]
            if diff:
                msg = graph.fmt_changed(d, old, {**old, **qs}, diff, today, now_min)
        if msg:
            await broadcast(bot, msg, button=True)
            logger.info(f"Сповіщення про графік на {key} надіслано")
        merged = {**(old or {}), **qs}
        if merged != old:
            sch[key] = merged
            dirty = True
    if dirty:
        _save()


def current_text() -> str:
    today, now_min = graph.now()
    parts = []
    for k, qs in sorted(state["schedules"].items()):
        d = date.fromisoformat(k)
        if d >= today:
            shown = {q: graph.upcoming(qs[q], d, today, now_min) for q in graph.QUEUES if q in qs}
            if shown:
                parts.append(graph.fmt_day(d, shown, "📋 Поточний графік на"))
    text = "\n\n".join(parts) or "Графіка ще немає — спробуйте за хвилину."
    return f"{text}\n\n{graph.notice}" if graph.notice else text


# ──────────────────────────────────────────
# МОНІТОРИНГ
# ──────────────────────────────────────────
def _publish(checked_at: float | None) -> None:
    """Для сторінки беремо збережений стан, а не лише останню відповідь джерела."""
    current = {date.fromisoformat(k): v for k, v in state["schedules"].items()}
    _api["data"] = graph.build_api(current, _times, graph.now()[0], checked_at, POLL_INTERVAL, graph.notice)


async def monitor(bot: Bot) -> None:
    _publish(None)
    last = None
    fails, fail_since, alerted = 0, None, False
    logger.info(f"Моніторинг: {graph.SOURCE} | черги={graph.QUEUES} | підписників={len(state['users'])}")

    async with httpx.AsyncClient(headers=graph.HEADERS, timeout=15, follow_redirects=True) as client:
        while True:
            delay = POLL_INTERVAL * random.uniform(0.8, 1.2)
            current = None
            try:
                current, times = await graph.fetch(client)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                fails += 1
                fail_since = fail_since or time.time()
                _status["last_error"] = f"{type(e).__name__}: {e}"
                delay = (max(e.retry_after, 60) if isinstance(e, graph.RateLimited)
                         else min(POLL_INTERVAL * 2 ** min(fails, 6), 120))
                logger.warning(f"Помилка опитування ({fails}): {e}")
            else:
                for d, t in times.items():
                    _times.setdefault(d, {}).update(t)
                _status["last_ok"] = time.time()
                if fails:
                    logger.info(f"Джерело знову доступне після {fails} помилок")
                if alerted and ADMIN_USER_ID:
                    await send(bot, ADMIN_USER_ID, "✅ Джерело графіків знову доступне.")
                fails, fail_since, alerted = 0, None, False

            try:
                today, now_min = graph.now()
                if current is not None and current != last:
                    await check_changes(bot, current, today, now_min)
                    last = current
                _publish(_status["last_ok"])
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Помилка обробки графіка")

            if fail_since and not alerted and ADMIN_USER_ID and time.time() - fail_since >= OUTAGE_ALERT_SEC:
                alerted = await send(
                    bot, ADMIN_USER_ID,
                    f"⚠️ Не вдається отримати графік ({graph.SOURCE}) понад {OUTAGE_ALERT_SEC // 60} хв.\n"
                    f"{_status['last_error']}",
                )
            await asyncio.sleep(delay)


async def supervise(bot: Bot) -> None:
    while True:
        try:
            await monitor(bot)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Моніторинг впав, перезапуск через 10с")
            await asyncio.sleep(10)


# ──────────────────────────────────────────
# КОМАНДИ
# ──────────────────────────────────────────
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    if not chat:
        return
    if chat.id not in state["users"]:
        state["users"].add(chat.id)
        _save()
    await context.bot.send_message(
        chat.id,
        f"✅ Підписано. Надсилатиму графік для черги {', '.join(graph.QUEUES)} та зміни в ньому.\n\n"
        "/graph — поточний графік\n/web — графік на сторінці\n/stop — відписатися",
    )
    await context.bot.send_message(chat.id, current_text(), reply_markup=_markup(chat.id > 0))


async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    if chat:
        state["users"].discard(chat.id)
        _save()
        await context.bot.send_message(chat.id, "Відписано. Щоб підписатися знову — /start.")


async def cmd_graph(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if chat := update.effective_chat:
        await context.bot.send_message(chat.id, current_text(), reply_markup=_markup(chat.id > 0))


async def cmd_web(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if chat := update.effective_chat:
        markup = _markup(chat.id > 0)
        await context.bot.send_message(chat.id, "⚡ Графік відключень" if markup else "Сторінку не налаштовано.",
                                       reply_markup=markup)


async def cmd_set(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Адмін: ручний графік, якщо джерело його не віддає. /set [завтра] 05:00-09:00, 15:30-22:00 | /set нема"""
    chat = update.effective_chat
    if not chat or chat.id != ADMIN_USER_ID:
        return
    text = " ".join(context.args).lower()
    ivs = graph.parse_intervals(text)
    if not ivs and "нема" not in text:
        await context.bot.send_message(chat.id, "Приклад: /set 05:00-09:00, 15:30-22:00\n/set завтра нема")
        return
    today, _ = graph.now()
    d = today + timedelta(days=text.startswith("завтра"))
    state["schedules"].setdefault(d.isoformat(), {})[graph.QUEUES[0]] = ivs
    _save()
    _publish(_status["last_ok"])
    await context.bot.send_message(chat.id, f"✅ {d.isoformat()}: {graph.fmt_intervals(ivs)}")


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    err = context.error
    if isinstance(err, (NetworkError, Forbidden)):  # BadRequest входить у NetworkError
        logger.warning(f"Telegram: {type(err).__name__}: {err}")
    else:
        logger.error("Необроблена помилка", exc_info=err)


# ──────────────────────────────────────────
# ЗАПУСК
# ──────────────────────────────────────────
def create_application() -> Application:
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN не встановлено!")
    app = Application.builder().token(BOT_TOKEN).build()
    for name, fn in (("start", cmd_start), ("stop", cmd_stop), ("graph", cmd_graph), ("web", cmd_web), ("set", cmd_set)):
        app.add_handler(CommandHandler(name, fn))
    app.add_error_handler(on_error)
    return app


async def startup(app: Application) -> None:
    await app.initialize()
    await app.start()
    await app.bot.set_my_commands([
        BotCommand("graph", "Поточний графік"), BotCommand("web", "Графік на сторінці"),
        BotCommand("stop", "Відписатися"),
    ])
    if WEBHOOK_URL:
        await app.bot.set_webhook(f"{WEBHOOK_URL}/{BOT_TOKEN}", allowed_updates=["message"])
    else:
        logger.warning("WEBHOOK_URL не задано — вебхук не встановлено")
    task = asyncio.create_task(supervise(app.bot))
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)


def api_data() -> dict:
    out = dict(_api["data"] or {})
    out["ready"] = bool(out.get("days"))
    out["serverTime"] = int(time.time() * 1000)
    return out


def status() -> dict:
    ok = _status["last_ok"]
    return {
        "source": graph.SOURCE,
        "queues": graph.QUEUES,
        "users": len(state["users"]),
        "last_ok_sec_ago": round(time.time() - ok) if ok else None,
        "last_error": _status["last_error"],
    }
