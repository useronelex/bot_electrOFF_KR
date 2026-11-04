"""Джерела графіків і розбір у єдиний формат: {дата: {черга: [[початок_хв, кінець_хв], ...]}}."""
import json
import logging
import os
import re
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger("graph")
KYIV = ZoneInfo("Europe/Kyiv")
QUEUES = tuple(q.strip() for q in os.environ.get("QUEUES", "4.2").split(",") if q.strip())
SOURCE = os.environ.get("SOURCE", "yasno")  # yasno | dtek | telegram
YASNO_URL = os.environ.get(
    "YASNO_URL", "https://app.yasno.ua/api/blackout-service/public/shutdowns/regions/3/dsos/301/planned-outages")
DTEK_URL = os.environ.get("DTEK_URL", "https://www.dtek-dnem.com.ua/ua/shutdowns")
CHANNEL = os.environ.get("CHANNEL", "pat_cherkasyoblenergo")  # для SOURCE=telegram
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept-Language": "uk,en;q=0.8",
    "Cache-Control": "no-cache",
}

notice: str | None = None  # примітка від джерела (напр. YASNO не публікує плановий графік)
Schedule = dict[date, dict[str, list[list[int]]]]
Times = dict[date, dict[str, float | None]]


class RateLimited(Exception):
    def __init__(self, retry_after: float):
        super().__init__(f"429, retry after {retry_after}s")
        self.retry_after = retry_after


def now() -> tuple[date, int]:
    """(сьогодні, хвилин від початку доби) за Києвом."""
    n = datetime.now(KYIV)
    return n.date(), n.hour * 60 + n.minute


def normalize(pairs: list[list[int]]) -> list[list[int]]:
    """Сортує інтервали й зливає ті, що перетинаються або йдуть впритул."""
    out: list[list[int]] = []
    for s, e in sorted(p for p in pairs if p[0] < p[1]):
        if out and s <= out[-1][1]:
            out[-1][1] = max(out[-1][1], e)
        else:
            out.append([s, e])
    return out


# ──────────────────────────────────────────
# ДЖЕРЕЛО 1: YASNO API (дані ДТЕК Дніпровських електромереж, без захисту від ботів)
# ──────────────────────────────────────────
def parse_yasno(data: dict) -> tuple[Schedule, Times]:
    current: Schedule = {}
    times: Times = {}
    seen = {}
    for q in QUEUES:
        info = data.get(q)
        if not isinstance(info, dict):
            continue
        seen[q] = {k: (info[k].get("status") if isinstance(info.get(k), dict) else None) for k in ("today", "tomorrow")}
        try:
            updated = datetime.fromisoformat(info["updatedOn"]).timestamp()
        except (KeyError, ValueError):
            updated = None
        for day in (info.get("today"), info.get("tomorrow")):
            # інші статуси (WaitingForSchedule, EmergencyShutdowns...) — графіка на цю добу немає
            if not day or day.get("status") not in ("ScheduleApplies", "NoOutages"):
                continue
            d = datetime.fromisoformat(day["date"]).date()
            pairs = [[x["start"], x["end"]] for x in day.get("slots", []) if x.get("type") == "Definite"]
            current.setdefault(d, {})[q] = normalize(pairs)
            times.setdefault(d, {})[q] = updated
    if not seen:
        raise RuntimeError(f"YASNO: у відповіді немає черг {QUEUES}, ключі: {list(data)[:8]}")
    if not current:  # напр. EmergencyShutdowns: планового графіка немає — це не помилка
        global notice
        status = next(iter(seen.values())).get("today")
        notice = f"ℹ️ YASNO зараз не публікує плановий графік (статус: {status}). Показано останній відомий."
        logger.info(f"YASNO: планового графіка немає, статуси: {seen}")
    return current, times


async def _fetch_yasno(client: httpx.AsyncClient) -> tuple[Schedule, Times]:
    r = await client.get(YASNO_URL)
    r.raise_for_status()
    return parse_yasno(r.json())


# ──────────────────────────────────────────
# ДЖЕРЕЛО 2: сайт ДТЕК (на Render блокується Incapsula) (DisconSchedule.fact у HTML сторінки /shutdowns)
# ──────────────────────────────────────────
_DTEK_OFF = {"no": (0, 60), "first": (0, 30), "second": (30, 60)}  # yes/maybe/mfirst/msecond — світло є


def _parse_update(text: str | None) -> float | None:
    try:
        return datetime.strptime(text or "", "%d.%m.%Y %H:%M").replace(tzinfo=KYIV).timestamp()
    except ValueError:
        return None


def parse_dtek(html: str) -> tuple[Schedule, Times]:
    m = re.search(r"DisconSchedule\.fact\s*=\s*", html)
    if not m:
        raise RuntimeError("ДТЕК: на сторінці немає DisconSchedule.fact (змінили сторінку або захист від ботів)")
    fact, _ = json.JSONDecoder().raw_decode(html, m.end())
    updated = _parse_update(fact.get("update"))
    current: Schedule = {}
    times: Times = {}
    for ts, groups in fact.get("data", {}).items():
        d = datetime.fromtimestamp(int(ts), KYIV).date()
        for name, hours in groups.items():
            q = name.removeprefix("GPV")
            if q not in QUEUES:
                continue
            pairs = []
            for h, status in hours.items():
                if off := _DTEK_OFF.get(status):
                    base = (int(h) - 1) * 60
                    pairs.append([base + off[0], base + off[1]])
            current.setdefault(d, {})[q] = normalize(pairs)
            times.setdefault(d, {})[q] = updated
    if not current:
        raise RuntimeError(f"ДТЕК: у даних немає черг {QUEUES}")
    return current, times


async def _fetch_dtek(client: httpx.AsyncClient) -> tuple[Schedule, Times]:
    r = await client.get(DTEK_URL)
    r.raise_for_status()
    return parse_dtek(r.text)


# ──────────────────────────────────────────
# ДЖЕРЕЛО 3: публічний Telegram-канал обленерго (t.me/s/<канал>)
# ──────────────────────────────────────────
MONTHS = {
    "січня": 1, "лютого": 2, "березня": 3, "квітня": 4, "травня": 5, "червня": 6,
    "липня": 7, "серпня": 8, "вересня": 9, "жовтня": 10, "листопада": 11, "грудня": 12,
}
MONTH_NAMES = {v: k for k, v in MONTHS.items()}
_DATE_TEXT = re.compile(r"(\d{1,2})\s+(" + "|".join(MONTHS) + r")", re.IGNORECASE)
_DATE_NUM = re.compile(r"(\d{1,2})\.(\d{2})\.(\d{4})")
_QUEUE_LINE = re.compile(r"^\s*(\d\.\d)\s+(.*)$")
_INTERVAL = re.compile(r"(\d{1,2}):(\d{2})\s*(?:[-–—]|до)\s*(\d{1,2}):(\d{2})")


def _parse_date(text: str, today: date) -> date | None:
    try:
        if m := _DATE_TEXT.search(text):
            month = MONTHS[m.group(2).lower()]
            year = today.year + (month == 1 and today.month == 12) - (month == 12 and today.month == 1)
            return date(year, month, int(m.group(1)))
        if m := _DATE_NUM.search(text):
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    except ValueError:
        pass
    return None


def parse_intervals(text: str) -> list[list[int]]:
    """'05:00-09:00, 15:30-22:00' → [[300, 540], [930, 1320]]."""
    out = []
    for h1, m1, h2, m2 in _INTERVAL.findall(text):
        if int(h1) > 24 or int(h2) > 24 or int(m1) > 59 or int(m2) > 59:
            continue
        start, end = int(h1) * 60 + int(m1), int(h2) * 60 + int(m2)
        if end < start or (end == 0 and start > 0):  # через північ → на цю добу до 24:00
            end = 1440
        out.append([start, end])
    return normalize(out)


def parse_message(text: str, today: date) -> tuple[date, dict[str, list[list[int]]]] | None:
    day = _parse_date(text, today)
    if not day:
        return None
    queues: dict[str, list[list[int]]] = {}
    for line in text.splitlines():
        m = _QUEUE_LINE.match(line)
        if not m or m.group(1) not in QUEUES:
            continue
        if ivs := parse_intervals(m.group(2)):
            queues.setdefault(m.group(1), []).extend(ivs)
    return (day, {q: normalize(p) for q, p in queues.items()}) if queues else None


async def _fetch_telegram(client: httpx.AsyncClient) -> tuple[Schedule, Times]:
    r = await client.get(f"https://t.me/s/{CHANNEL}")
    if r.status_code == 429:
        raise RateLimited(float(r.headers.get("Retry-After", 60)))
    r.raise_for_status()
    today, _ = now()
    current: Schedule = {}
    times: Times = {}
    for wrap in BeautifulSoup(r.text, "html.parser").select("div.tgme_widget_message"):  # від старих до нових
        el = wrap.select_one(".tgme_widget_message_text")
        parsed = parse_message(_text(el), today) if el else None
        if not parsed:
            continue
        d, queues = parsed
        current.setdefault(d, {}).update(queues)  # новіше повідомлення перекриває старіше
        ts = _msg_ts(wrap)
        for q in queues:
            times.setdefault(d, {})[q] = ts
    if not current:
        raise RuntimeError(f"У каналі {CHANNEL} не знайдено графіків для черг {QUEUES}")
    return current, times


def _text(el) -> str:
    for br in el.find_all("br"):
        br.replace_with("\n")
    return el.get_text()


def _msg_ts(wrap) -> float | None:
    t = wrap.select_one("time[datetime]")
    try:
        return datetime.fromisoformat(t["datetime"]).timestamp() if t else None
    except ValueError:
        return None


async def fetch(client: httpx.AsyncClient) -> tuple[Schedule, Times]:
    global notice
    notice = None
    fetcher = {"yasno": _fetch_yasno, "dtek": _fetch_dtek, "telegram": _fetch_telegram}[SOURCE]
    return await fetcher(client)


# ──────────────────────────────────────────
# ПОРІВНЯННЯ: минуле не важливе
# ──────────────────────────────────────────
def future(iv: list[list[int]] | None, d: date, today: date, now_min: int) -> list[list[int]]:
    """Для порівняння: сьогодні — лише від поточної хвилини, майбутні дні — повністю."""
    if not iv:
        return []
    if d > today:
        return [list(x) for x in iv]
    return [[max(s, now_min), e] for s, e in iv if e > now_min]


def upcoming(iv: list[list[int]] | None, d: date, today: date, now_min: int) -> list[list[int]] | None:
    """Для показу: відкидаємо лише інтервали, що повністю закінчилися (триваючі лишаємо цілими)."""
    if iv is None:
        return None
    return [list(x) for x in iv if d > today or x[1] > now_min]


# ──────────────────────────────────────────
# ФОРМАТУВАННЯ
# ──────────────────────────────────────────
def _t(x: int) -> str:
    return f"{x // 60 % 24:02d}:{x % 60:02d}"


def fmt_intervals(iv: list[list[int]] | None) -> str:
    if iv is None:
        return "—"
    return ", ".join(f"{_t(s)}–{_t(e)}" for s, e in iv) or "відключень немає"


def _fmt_date(d: date) -> str:
    return f"{d.day} {MONTH_NAMES[d.month]}"


def fmt_day(d: date, queues: dict, title: str = "⚡ Графік відключень на") -> str:
    lines = [f"{title} {_fmt_date(d)}", ""]
    lines += [f"Черга {q}: {fmt_intervals(iv)}" for q, iv in queues.items()]
    return "\n".join(lines)


def fmt_changed(d: date, old: dict, new: dict, diff: list[str], today: date, now_min: int) -> str:
    lines = [f"🔄 Зміна графіка на {_fmt_date(d)}", ""]
    for q in diff:
        lines += [
            f"Черга {q}:",
            f"  було: {fmt_intervals(upcoming(old.get(q), d, today, now_min))}",
            f"  стало: {fmt_intervals(upcoming(new.get(q), d, today, now_min))}",
            "",
        ]
    return "\n".join(lines).strip()


# ──────────────────────────────────────────
# СПОВІЩЕННЯ ЗА N ХВИЛИН ДО ВИМКНЕННЯ / ВВІМКНЕННЯ
# ──────────────────────────────────────────
def alert_events(schedules: dict, today: date, now_min: int, lead: int) -> list[tuple[str, int, str]]:
    """[(унікальний_ключ, абс_хвилина, текст)] для подій, що настануть протягом `lead` хв.
    Сьогоднішній і завтрашній графіки зшиваються: 22:00–24:00 + 00:00–02:00 — одне відключення."""
    base = today.toordinal() * 1440
    out = []
    for q in QUEUES:
        pairs = []
        for off in (0, 1):
            day = schedules.get((today + timedelta(days=off)).isoformat(), {})
            pairs += [[s + off * 1440, e + off * 1440] for s, e in day.get(q) or []]
        for s, e in normalize(pairs):
            for kind, t in (("off", s), ("on", e)):
                left = t - now_min
                if not 0 < left <= lead:
                    continue
                if kind == "off":
                    text = f"🔴 Через {left} хв вимкнення світла о {_t(t)}\nЧерга {q}: {_t(s)}–{_t(e)}"
                else:
                    text = f"🟢 Через {left} хв ввімкнення світла о {_t(t)}\nЧерга {q}"
                out.append((f"{q}|{kind}|{base + t}", base + t, text))
    return out


# ──────────────────────────────────────────
# ДАНІ ДЛЯ ВЕБ-СТОРІНКИ (/api/schedule)
# ──────────────────────────────────────────
def _ms(ts: float | None) -> int | None:
    return int(ts * 1000) if ts else None


def build_api(current: Schedule, times: Times, today: date, checked_at: float | None, poll: float) -> dict:
    days = {
        d.isoformat(): {
            q: {
                "slots": [{"s": s, "e": e, "t": "off"} for s, e in iv],
                "updatedAt": _ms(times.get(d, {}).get(q)),
            }
            for q, iv in qs.items()
        }
        for d, qs in sorted(current.items())
        if today <= d <= today + timedelta(days=2)
    }
    return {
        "today": today.isoformat(),
        "queues": list(QUEUES),
        "displayQueues": list(QUEUES),
        "days": days,
        "checkedAt": _ms(checked_at),
        "pollSec": poll,
    }
