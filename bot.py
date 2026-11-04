import os
import re
import json
import logging
import asyncio
import subprocess
import tempfile
import time
from collections import deque, defaultdict
from pathlib import Path

from telegram import Update
from telegram.error import BadRequest, Forbidden, NetworkError
from telegram.ext import Application, MessageHandler, CommandHandler, filters, ContextTypes

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")
ADMIN_USER_ID = int(os.environ.get("ADMIN_USER_ID") or "0")
INSTAGRAM_COOKIES_RAW = os.environ.get("INSTAGRAM_COOKIES", "").replace("\\n", "\n").replace("\\t", "\t")
_COOKIES_FILE: str | None = None

REQUEST_LIMIT = 50
REQUEST_WINDOW = 3600
COOLDOWN_TIME = 1800

user_timestamps: dict[int, deque] = {}
user_cooldowns: dict[int, float] = {}
_processing: set[str] = set()
_sent_messages: dict[int, deque] = defaultdict(lambda: deque(maxlen=200))

# ──────────────────────────────────────────
# URL PATTERNS
# ──────────────────────────────────────────
INSTAGRAM_PATTERNS = [
    re.compile(r'https?://(?:www\.)?instagram\.com/(?:p|reel|reels|tv)/([A-Za-z0-9_-]+)/?'),
    re.compile(r'https?://(?:www\.)?instagram\.com/stories/([A-Za-z0-9_.]+)/(\d+)/?'),
    re.compile(r'https?://instagr\.am/(?:p|reel)/([A-Za-z0-9_-]+)/?'),
]

FACEBOOK_PATTERNS = [
    re.compile(r'https?://(?:www\.|m\.|web\.)?facebook\.com/watch/?\?v=[\d]+'),
    re.compile(r'https?://(?:www\.|m\.|web\.)?facebook\.com/[\w.-]+/videos/[\d\w-]+'),
    re.compile(r'https?://(?:www\.|m\.|web\.)?facebook\.com/share/[vr]/[\w-]+'),
    re.compile(r'https?://fb\.watch/[\w-]+'),
    # Facebook Reels
    re.compile(r'https?://(?:www\.|m\.|web\.)?facebook\.com/reels?/[\d]+'),
    re.compile(r'https?://(?:www\.|m\.|web\.)?facebook\.com/[\w.-]+/reels?/[\d\w-]+'),
    re.compile(r'https?://(?:www\.|m\.|web\.)?facebook\.com/share/r/[\w-]+'),
]


def extract_url(text: str) -> tuple[str, str] | None:
    for pattern in INSTAGRAM_PATTERNS:
        match = pattern.search(text)
        if match:
            return match.group(0).split('?')[0].rstrip('/'), "instagram"
    for pattern in FACEBOOK_PATTERNS:
        match = pattern.search(text)
        if match:
            return match.group(0).split('?')[0], "facebook"
    return None

# ──────────────────────────────────────────
# COOKIES
# ──────────────────────────────────────────
def _init_cookies() -> None:
    global _COOKIES_FILE
    if not INSTAGRAM_COOKIES_RAW:
        logger.warning("INSTAGRAM_COOKIES не встановлено")
        return
    path = "/tmp/instagram_cookies.txt"
    with open(path, "w") as f:
        f.write(INSTAGRAM_COOKIES_RAW)
    _COOKIES_FILE = path
    count = sum(1 for l in INSTAGRAM_COOKIES_RAW.splitlines() if l.strip() and not l.startswith("#"))
    logger.info(f"Cookies завантажено: {count} шт.")

# ──────────────────────────────────────────
# МЕТОД 1: yt-dlp
# ──────────────────────────────────────────

def _download_ytdlp(url: str, output_dir: str) -> str | None:
    import yt_dlp
    fmt = (
        "bestvideo[vcodec!=unknown]+bestaudio[ext=m4a]/"
        "bestvideo[vcodec!=unknown]+bestaudio/"
        "best[acodec!=none]/"
        "best"
    )
    
    ydl_opts = {
       "format_sort_force": True,
        "format_sort": ["+acodec", "vcodec:vp9", "vcodec:avc"],
        "ignore_no_formats_error": False,
        "outtmpl": os.path.join(output_dir, "video.%(ext)s"),
        "format": fmt,
        "merge_output_format": "mp4",
        #"verbose": True,
        "quiet": True,
        "no_warnings": True,
        "noprogress": True,
        "socket_timeout": 30,
        "retries": 3,
        "fragment_retries": 3,
        # Паралельне завантаження фрагментів DASH — швидше для Instagram/Facebook
        "concurrent_fragment_downloads": 4,
        # Якщо швидкість впала нижче 50 KB/s — вважаємо throttling і повторюємо
        "throttledratelimit": 50000,
        "prefer_ffmpeg": True,
        "http_headers": {
            "User-Agent": (
                "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 "
                "Mobile/15E148 Safari/604.1"
            ),
        },
        "postprocessors": [{"key": "FFmpegVideoConvertor", "preferedformat": "mp4"}],
        "postprocessor_args": {
            # -movflags +faststart: метадані на початок файлу → миттєвий стрімінг
            "ffmpegmerger": ["-movflags", "+faststart"],
            "ffmpegvideoconvertor": ["-movflags", "+faststart"],
        },
    }
    if _COOKIES_FILE:
        ydl_opts["cookiefile"] = _COOKIES_FILE
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            if not info:
                return None
            for f in Path(output_dir).glob("video.*"):
                if f.stat().st_size > 0:
                    logger.info(f"yt-dlp OK: {f.name} | {f.stat().st_size/1024/1024:.1f}MB | codec={info.get('vcodec','?')}")
                    return str(f)
            return None
    except yt_dlp.utils.DownloadError as e:
        err = str(e).lower()
        if "empty media response" in err:
            logger.warning("yt-dlp: empty media response")
            return None
        if "private" in err or "login" in err or "age" in err or "rate-limit" in err:
            logger.warning(f"yt-dlp: авторизація/ліміт — {str(e)[:120]}")
        elif "not found" in err or "404" in err:
            logger.warning(f"yt-dlp: не знайдено — {str(e)[:120]}")
        elif "no video" in err or "photo" in err:
            logger.info("yt-dlp: фото пост")
        elif "can't be seen" in err or "isn't available" in err or "certain audiences" in err:
            logger.warning("yt-dlp: контент обмежено (18+/гео)")
        elif "requested format is not available" in err:
            logger.warning("yt-dlp: формат недоступний — відео без аудіо з цього IP")
        else:
            logger.error(f"yt-dlp: {str(e)[:200]}")
        return None
    except Exception as e:
        logger.error(f"yt-dlp Exception: {e}", exc_info=True)
        return None

# ──────────────────────────────────────────
# DISPATCH
# ──────────────────────────────────────────
def download_media(url: str, output_dir: str, platform: str) -> str | None:
    logger.info(f"yt-dlp: {'cookies активні' if _COOKIES_FILE else 'без cookies'} | {platform} | {url}")
    return _download_ytdlp(url, output_dir)

# ──────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────
_throttle_until: dict[str, float] = {}
_throttle_skipped: dict[str, int] = {}
_bg: set[asyncio.Task] = set()


def throttled(key: str, interval: float = 60.0) -> int | None:
    """None — подію слід пропустити (недавно вже була); інакше — скільки однакових подій пропущено досі."""
    now = time.monotonic()
    if len(_throttle_until) > 200:  # не даємо словнику рости
        for k in [k for k, t in _throttle_until.items() if t < now]:
            del _throttle_until[k]
    if now < _throttle_until.get(key, 0):
        _throttle_skipped[key] = _throttle_skipped.get(key, 0) + 1
        return None
    _throttle_until[key] = now + interval
    return _throttle_skipped.pop(key, 0)


def _more(n: int) -> str:
    return f" (+{n} подібних)" if n else ""


async def _reply_temp(message, text: str, ttl: float = 5) -> None:
    """Коротка службова відповідь, що сама зникає (хендлер не чекає)."""
    try:
        notice = await message.reply_text(text, reply_to_message_id=message.message_id)
    except Exception as e:
        logger.warning(f"Не вдалося надіслати службову відповідь: {e}")
        return

    async def _drop() -> None:
        await asyncio.sleep(ttl)
        try:
            await notice.delete()
        except Exception:
            pass

    task = asyncio.create_task(_drop())
    _bg.add(task)
    task.add_done_callback(_bg.discard)


def _probe(path: str) -> tuple[int | None, int | None, int | None, bool]:
    """(ширина, висота, тривалість с, є_аудіо) через ffprobe; за збою — порожні значення."""
    def run(*args: str) -> dict:
        r = subprocess.run(["ffprobe", "-v", "error", *args, "-of", "json", path],
                           capture_output=True, text=True, timeout=10)
        if r.returncode:
            raise RuntimeError(r.stderr.strip()[:100])
        return json.loads(r.stdout or "{}")
    try:
        v = run("-select_streams", "v:0", "-show_entries", "stream=width,height:format=duration")
        st = (v.get("streams") or [{}])[0]
        dur = v.get("format", {}).get("duration")
        a = run("-select_streams", "a:0", "-show_entries", "stream=codec_name")
        return st.get("width"), st.get("height"), int(float(dur)) if dur else None, bool(a.get("streams"))
    except Exception as e:
        logger.debug(f"ffprobe недоступний: {e}")
        return None, None, None, True  # не заважаємо відправці

# ──────────────────────────────────────────
# RATE LIMIT
# ──────────────────────────────────────────
def check_rate_limit(user_id: int) -> tuple[bool, int]:
    now = time.time()
    cooldown = user_cooldowns.get(user_id, 0)
    if now < cooldown:
        return False, int((cooldown - now) / 60) + 1
    user_cooldowns.pop(user_id, None)
    ts = user_timestamps.setdefault(user_id, deque())
    while ts and ts[0] < now - REQUEST_WINDOW:
        ts.popleft()
    if len(ts) >= REQUEST_LIMIT:
        user_cooldowns[user_id] = now + COOLDOWN_TIME
        return False, COOLDOWN_TIME // 60
    ts.append(now)
    if len(user_timestamps) > 500:  # прибираємо неактивних
        for uid in [u for u, d in user_timestamps.items() if d[-1] < now - REQUEST_WINDOW]:
            del user_timestamps[uid]
    return True, 0

# ──────────────────────────────────────────
# TYPING
# ──────────────────────────────────────────
async def keep_uploading_action(chat_id: int, bot) -> None:
    try:
        while True:
            await bot.send_chat_action(chat_id=chat_id, action="upload_video")
            await asyncio.sleep(4)
    except asyncio.CancelledError:
        pass
    except Exception:  # індикатор не критичний (напр. бота вилучили з чату)
        pass

# ──────────────────────────────────────────
# HANDLER
# ──────────────────────────────────────────
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.message
    if not message or not message.text or not message.from_user:
        return
    if time.time() - message.date.timestamp() > 30:
        return

    url_info = extract_url(message.text)
    if not url_info:
        return
    media_url, platform = url_info

    user_id = message.from_user.id
    allowed, cooldown_mins = check_rate_limit(user_id)
    if not allowed:
        await _reply_temp(message, f"Забагато запитів. Спробуйте через {cooldown_mins} хв.", ttl=10)
        return

    dedup_key = f"{media_url}:{message.chat_id}"
    if dedup_key in _processing:
        return
    _processing.add(dedup_key)

    logger.info(f"[{platform.upper()}] user={user_id} | {media_url}")
    typing_task = asyncio.create_task(keep_uploading_action(message.chat_id, context.bot))
    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            loop = asyncio.get_running_loop()
            media_path = await loop.run_in_executor(None, download_media, media_url, tmp_dir, platform)
            if not media_path or not Path(media_path).exists():
                await _reply_temp(message, "Не вдалося завантажити: контент приватний, видалений або недоступний.")
                return

            size_mb = Path(media_path).stat().st_size / 1024 / 1024
            if size_mb > 50:
                await _reply_temp(message, f"Файл завеликий ({size_mb:.0f} МБ): Telegram приймає до 50 МБ.")
                return

            width, height, duration, has_audio = await loop.run_in_executor(None, _probe, media_path)
            if not has_audio:
                logger.warning("Відео без аудіопотоку — можливо, Meta заблокувала трек")

            sent = False
            for attempt in range(3):
                try:
                    with open(media_path, "rb") as f:
                        sent_msg = await context.bot.send_video(
                            chat_id=message.chat_id,
                            video=f,
                            supports_streaming=True,
                            width=width,
                            height=height,
                            duration=duration,
                            write_timeout=120,
                            read_timeout=60,
                            connect_timeout=30,
                        )
                    _sent_messages[message.chat_id].append(sent_msg.message_id)
                    logger.info(f"Sent {size_mb:.1f}MB (attempt {attempt+1})")
                    sent = True
                    break
                except Exception as e:
                    logger.warning(f"Send attempt {attempt+1} failed: {e}")
                    if attempt < 2:
                        await asyncio.sleep(3)

            if sent:
                try:
                    await message.delete()
                except Exception:
                    pass
            else:
                await _reply_temp(message, "Не вдалося надіслати відео. Спробуйте пізніше.")
    except Exception as e:  # непередбачена помилка саме цього запиту
        sig = f"{type(e).__name__}: {str(e)[:80]}"
        skipped = throttled("req:" + sig)
        if skipped is not None:
            logger.error(f"[{platform.upper()}] {media_url} — {sig}{_more(skipped)}", exc_info=True)
        await _reply_temp(message, "Не вдалося обробити посилання. Спробуйте пізніше.")
    finally:
        typing_task.cancel()
        _processing.discard(dedup_key)

# ──────────────────────────────────────────
# ADMIN
# ──────────────────────────────────────────
async def cmd_clean(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.message
    if not msg or not update.effective_user:
        return
    if update.effective_user.id != ADMIN_USER_ID:
        await msg.reply_text("Немає доступу.")
        return
    args = context.args or []
    count_arg = args[0] if args else "5"
    try:
        chat_id_arg = int(args[1]) if len(args) > 1 else None
    except ValueError:
        await msg.reply_text("Невірний chat_id. Формат: /clean [N|all] [chat_id]")
        return
    target_chat = chat_id_arg or (next(iter(_sent_messages)) if _sent_messages else None)
    if not target_chat:
        await msg.reply_text("Немає збережених повідомлень.")
        return
    msgs = _sent_messages.get(target_chat)
    if not msgs:
        await msg.reply_text(f"Немає повідомлень для чату {target_chat}.")
        return
    all_msgs = list(msgs)
    to_delete = all_msgs if count_arg.lower() == "all" else all_msgs[-(int(count_arg) if count_arg.isdigit() else 5):]
    deleted = failed = 0
    for msg_id in reversed(to_delete):
        try:
            await context.bot.delete_message(chat_id=target_chat, message_id=msg_id)
            if msg_id in msgs:
                msgs.remove(msg_id)
            deleted += 1
            await asyncio.sleep(0.3)
        except Exception as e:
            logger.warning(f"Cannot delete {msg_id}: {e}")
            failed += 1
    await msg.reply_text(f"Видалено: {deleted} | Не вдалось: {failed}")


async def cmd_chats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user or update.effective_user.id != ADMIN_USER_ID:
        return
    if not _sent_messages:
        await update.message.reply_text("Немає збережених повідомлень.")
        return
    lines = ["Чати:"] + [f"  {cid}: {len(m)} шт." for cid, m in _sent_messages.items()]
    lines.append("\n/clean [N|all] [chat_id]")
    await update.message.reply_text("\n".join(lines))

# ──────────────────────────────────────────
# ERRORS
# ──────────────────────────────────────────
async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Єдина точка для необроблених помилок: лог без спаму + рідкісне сповіщення адміну.
    Користувачу тут нічого не пишемо — відповіді на його запити формує сам хендлер."""
    err = context.error
    sig = f"{type(err).__name__}: {str(err)[:80]}"
    skipped = throttled("log:" + sig)
    if isinstance(err, (BadRequest, Forbidden)):  # напр. повідомлення вже видалено / немає прав
        if skipped is not None:
            logger.warning(f"Telegram API — {sig}{_more(skipped)}")
        return
    if isinstance(err, NetworkError):  # тимчасові збої мережі (TimedOut тощо)
        if skipped is not None:
            logger.warning(f"Мережа — {sig}{_more(skipped)}")
        return
    if skipped is not None:
        logger.error(f"Необроблена помилка — {sig}{_more(skipped)}", exc_info=err)
    if ADMIN_USER_ID and throttled("adm:" + sig, 600) is not None:  # адміну — раз на 10 хв для однакової помилки
        try:
            await context.bot.send_message(ADMIN_USER_ID, f"⚠️ Помилка бота: {sig}")
        except Exception:
            pass

# ──────────────────────────────────────────
# APP FACTORY
# ──────────────────────────────────────────
def create_application() -> Application:
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN не встановлено!")
    _init_cookies()
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CommandHandler("clean", cmd_clean))
    app.add_handler(CommandHandler("chats", cmd_chats))
    app.add_error_handler(on_error)
    logger.info("Бот запущено | yt-dlp (Instagram, Facebook Reels)")
    logger.info(f"Cookies: {'OK' if _COOKIES_FILE else 'НЕ ВСТАНОВЛЕНО'}")
    logger.info(f"Admin: {ADMIN_USER_ID or 'не встановлено'}")
    return app
