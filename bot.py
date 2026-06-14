import asyncio
import os
import time
from collections import deque

import aiohttp
import numpy as np
import orjson
import websockets
from aiohttp import web
import json
from datetime import datetime

# =========================
# CONFIG
# =========================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
PORT = int(os.getenv("PORT", 8080))

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

MAX_DATA_POINTS = 300
COOLDOWN_SECONDS = 3600

market_data = {}
last_alert = {}

# =========================
# TELEGRAM
# =========================

session = None

async def send_telegram(message):

    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }

    try:
        await session.post(url, json=payload)
    except Exception as e:
        print("Telegram error:", e)


# =========================
# GOOGLE SHEETS
# =========================

sheet = None


def load_creds():

    keys = [
        "GOOGLE_CREDENTIALS",
        "GOOGLE_CREDS_JSON",
        "GOOGLE_SERVICE_ACCOUNT",
        "GCP_CREDENTIALS"
    ]

    for k in keys:
        raw = os.getenv(k)
        if raw:
            try:
                return json.loads(raw)
            except:
                return None

    return None


def init_sheets():

    global sheet

    try:
        import gspread
        from google.oauth2.service_account import Credentials

        creds_dict = load_creds()

        if not creds_dict:
            print("Sheets OFF")
            return

        scope = ["https://www.googleapis.com/auth/spreadsheets"]

        creds = Credentials.from_service_account_info(
            creds_dict,
            scopes=scope
        )

        client = gspread.authorize(creds)

        sheet = client.open_by_key(
            "1uJPJ_CFBW_qU9mpqoHVS3oAgPH3Cg6ZzATFv1zd4S64"
        ).sheet1

        print("Sheets connected")

    except Exception as e:
        print("Sheets error:", e)
        sheet = None


def log_sheet(symbol, direction, price, strength, mode):

    try:
        if not sheet:
            return

        sheet.append_row([
            "BOT1",
            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            symbol,
            direction,
            round(price, 4),
            round(strength, 2),
            mode
        ])

    except Exception as e:
        print("Sheet write error:", e)


# =========================
# INDICATORS
# =========================

def ema(arr, period):

    alpha = 2 / (period + 1)
    v = arr[0]

    for p in arr[1:]:
        v = alpha * p + (1 - alpha) * v

    return v


def trend(prices):

    if len(prices) < 200:
        return None

    prices = np.array(prices)

    ema20 = ema(prices[-50:], 20)
    ema50 = ema(prices[-100:], 50)
    ema200 = ema(prices[-200:], 200)

    price = prices[-1]

    bullish = price > ema20 > ema50 > ema200
    bearish = price < ema20 < ema50 < ema200

    strength = abs((ema20 - ema200) / ema200) * 100

    return bullish, bearish, strength, price


# =========================
# SIGNAL ENGINE
# =========================

def evaluate(symbol, prices):

    result = trend(prices)
    if not result:
        return

    bullish, bearish, strength, price = result

    if strength < 1:
        return

    now = time.time()

    if symbol not in last_alert:
        last_alert[symbol] = 0

    if now - last_alert[symbol] < COOLDOWN_SECONDS:
        return

    mode = "DAY TRADE" if strength < 3 else "SWING"

    if bullish:

        log_sheet(symbol, "LONG", price, strength, mode)

        msg = f"""
🚀 *{mode} LONG*

Pair: {symbol}
Price: {price:.2f}
Strength: {strength:.2f}%
"""

        asyncio.create_task(send_telegram(msg))
        last_alert[symbol] = now

    elif bearish:

        log_sheet(symbol, "SHORT", price, strength, mode)

        msg = f"""
📉 *{mode} SHORT*

Pair: {symbol}
Price: {price:.2f}
Strength: {strength:.2f}%
"""

        asyncio.create_task(send_telegram(msg))
        last_alert[symbol] = now


# =========================
# WEBSOCKET
# =========================

async def stream():

    ws_url = "wss://stream.bybit.com/v5/public/linear"

    for s in SYMBOLS:
        market_data[s] = deque(maxlen=MAX_DATA_POINTS)

    payload = {
        "op": "subscribe",
        "args": [f"tickers.{s}" for s in SYMBOLS]
    }

    while True:
        try:
            async with websockets.connect(ws_url) as ws:

                await ws.send(orjson.dumps(payload).decode())

                while True:
                    msg = orjson.loads(await ws.recv())

                    if "topic" not in msg:
                        continue

                    symbol = msg["topic"].split(".")[-1]
                    price = msg.get("data", {}).get("lastPrice")

                    if not price:
                        continue

                    market_data[symbol].append(float(price))
                    evaluate(symbol, list(market_data[symbol]))

        except Exception as e:
            print("WS error:", e)
            await asyncio.sleep(5)


# =========================
# HEALTH SERVER
# =========================

async def health(request):
    return web.Response(text="OK")


async def start_web():

    app = web.Application()
    app.router.add_get("/", health)

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()

    print("Health server running")


# =========================
# MAIN
# =========================

async def main():

    global session

    session = aiohttp.ClientSession()

    init_sheets()

    await start_web()

    await send_telegram("🟢 BOT 1 ONLINE\nBTC / ETH / SOL Momentum Engine")

    asyncio.create_task(stream())

    while True:
        await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(main())
