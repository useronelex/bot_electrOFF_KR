"""Точка входу для Render: Flask (вебхук + сторінка графіка) і один asyncio-цикл для бота та моніторингу."""
import asyncio
import logging
import os
import threading
from concurrent.futures import Future

from flask import Flask, jsonify, request, send_file
from telegram import Update

import bot

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logging.getLogger("werkzeug").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger("main")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app = Flask(__name__)
loop = asyncio.new_event_loop()
tg = bot.create_application()


def _log_failure(fut: Future) -> None:
    if not fut.cancelled() and fut.exception():
        logger.error("Помилка обробки оновлення", exc_info=fut.exception())


@app.route("/")
def home():
    return "Bot is alive!", 200


@app.route("/health")
def health():
    return bot.status(), 200


@app.route("/schedule")
def schedule_page():
    resp = send_file(os.path.join(BASE_DIR, "schedule.html"), mimetype="text/html")
    resp.headers["Cache-Control"] = "no-cache"
    return resp


@app.route("/api/schedule")
def schedule_api():
    resp = jsonify(bot.api_data())
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.post(f"/{bot.BOT_TOKEN}")
def webhook():
    update = Update.de_json(request.get_json(force=True), tg.bot)
    # відповідаємо Telegram одразу, не чекаючи обробки (інакше він повторює запит)
    asyncio.run_coroutine_threadsafe(tg.process_update(update), loop).add_done_callback(_log_failure)
    return "ok", 200


if __name__ == "__main__":
    threading.Thread(target=loop.run_forever, daemon=True).start()
    asyncio.run_coroutine_threadsafe(bot.startup(tg), loop).result(timeout=60)
    port = int(os.environ.get("PORT", 10000))
    logger.info(f"Flask на порту {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
