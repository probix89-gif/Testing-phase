#!/usr/bin/env python3
"""
Educational Market Analysis Bot – PROBIX_XD Admin Edition v2.0
Single-file, fully async, production-grade.
No real money trading – educational alerts only.

FIXED: Polling startup (double-start cancelled the updater)
       Bot now responds to /start for all users, including secondary admins.
"""

import asyncio
import os
import sys
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from functools import wraps
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import aiosqlite
import ta
import websockets
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.error import TelegramError
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
    ApplicationHandlerStop,
)

# ── .env loading ─────────────────────────
env_path_script = Path(__file__).resolve().parent / ".env"
env_path_cwd = Path.cwd() / ".env"
for _p in (env_path_script, env_path_cwd):
    if _p.exists():
        load_dotenv(dotenv_path=str(_p))
        break

# ── Environment Validation ───────────────
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
if len(TELEGRAM_TOKEN) < 10:
    print("FATAL: TELEGRAM_TOKEN not set. Add it to .env:\n  TELEGRAM_TOKEN=123456:ABC-DEF...")
    sys.exit(1)

ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0"))
OLYMP_SSID = os.getenv("OLYMP_SSID", "")
DEMO_MODE = os.getenv("DEMO_MODE", "1") == "1"
DB_PATH = os.getenv("DB_PATH", "ultimate_bot.db")
LOG_FILE = os.getenv("LOG_FILE", "bot.log")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# Secondary admins (by Telegram username, case-insensitive)
SECONDARY_ADMIN_USERNAMES: set[str] = {"PROBIX_XD"}

# ── Logging ──────────────────────────────
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format='{"time":"%(asctime)s","level":"%(levelname)s","name":"%(name)s","msg":"%(message)s"}',
    handlers=[logging.StreamHandler(), logging.FileHandler(LOG_FILE)],
)
logger = logging.getLogger("EDU_BOT")
logging.getLogger("httpx").setLevel(logging.WARNING)

# ── Config ───────────────────────────────
@dataclass
class AppConfig:
    symbols: List[str] = field(default_factory=lambda: ["EURUSD", "GBPUSD", "USDJPY"])
    timeframes: List[int] = field(default_factory=lambda: [1, 5])
    indicator_weights: Dict[str, float] = field(default_factory=lambda: {
        "rsi": 0.15, "macd": 0.15, "bb": 0.10, "stoch": 0.10,
        "adx": 0.05, "ma_cross": 0.05, "atr": 0.05, "volume": 0.05,
    })
    xgboost_weight: float = 0.20
    lstm_weight: float = 0.15
    ensemble_threshold: float = 0.60
    enable_lstm: bool = True
    enable_xgboost: bool = True
    admin_user_id: int = ADMIN_USER_ID
    signal_interval: int = 60       # seconds between signal loop iterations
    signal_cooldown: int = 300      # seconds before same symbol signal repeats

config = AppConfig()

# ── Database ─────────────────────────────
db_pool: Optional[aiosqlite.Connection] = None

async def init_db():
    global db_pool
    db_pool = await aiosqlite.connect(DB_PATH)
    await db_pool.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            user_id  INTEGER PRIMARY KEY,
            username TEXT,
            role     TEXT    DEFAULT 'user',
            joined   TEXT,
            active   INTEGER DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS trades (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id   INTEGER,
            timestamp TEXT,
            symbol    TEXT,
            direction TEXT,
            entry     REAL,
            expiry    TEXT,
            result    TEXT DEFAULT 'PENDING',
            pnl       REAL DEFAULT 0,
            confidence REAL
        );
        CREATE TABLE IF NOT EXISTS signals (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol     TEXT,
            direction  TEXT,
            entry      REAL,
            confidence REAL,
            timestamp  TEXT,
            reasons    TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_trades_user   ON trades(user_id);
        CREATE INDEX IF NOT EXISTS idx_trades_ts     ON trades(timestamp);
        CREATE INDEX IF NOT EXISTS idx_signals_sym   ON signals(symbol);
        CREATE INDEX IF NOT EXISTS idx_users_active  ON users(active);
    """)
    await db_pool.commit()
    logger.info("Database initialised.")

async def db_execute(query: str, params=None):
    async with db_pool.execute(query, params or ()) as cur:
        return await cur.fetchall()

async def db_execute_commit(query: str, params=None):
    await db_pool.execute(query, params or ())
    await db_pool.commit()

# ── User & Role Management ───────────────
def _is_secondary_admin(username: str) -> bool:
    return username.upper() in {u.upper() for u in SECONDARY_ADMIN_USERNAMES}

class UserManager:
    @staticmethod
    async def is_authorized(user_id: int, username: str = "") -> bool:
        if user_id == config.admin_user_id or _is_secondary_admin(username):
            return True
        rows = await db_execute("SELECT active FROM users WHERE user_id=?", (user_id,))
        return bool(rows) and rows[0][0] == 1

    @staticmethod
    async def get_role(user_id: int, username: str = "") -> str:
        if user_id == config.admin_user_id or _is_secondary_admin(username):
            return "admin"
        rows = await db_execute("SELECT role FROM users WHERE user_id=? AND active=1", (user_id,))
        return rows[0][0] if rows else "unauthorised"

    @staticmethod
    async def add_user(user_id: int, username: str, role: str = "user"):
        await db_execute_commit(
            "INSERT OR REPLACE INTO users (user_id, username, role, joined, active) VALUES (?,?,?,?,1)",
            (user_id, username, role, datetime.utcnow().isoformat())
        )

    @staticmethod
    async def set_role(user_id: int, role: str):
        await db_execute_commit("UPDATE users SET role=? WHERE user_id=?", (role, user_id))

    @staticmethod
    async def remove_user(user_id: int):
        await db_execute_commit("UPDATE users SET active=0 WHERE user_id=?", (user_id,))

    @staticmethod
    async def all_active_users() -> List[int]:
        rows = await db_execute("SELECT user_id FROM users WHERE active=1")
        return [r[0] for r in rows]

    @staticmethod
    async def list_users() -> List[Tuple]:
        rows = await db_execute(
            "SELECT user_id, username, role, joined FROM users WHERE active=1 ORDER BY joined DESC"
        )
        return rows

# ── Rate Limiter ─────────────────────────
class RateLimiter:
    def __init__(self, max_calls: int = 15, window: int = 60):
        self.max_calls = max_calls
        self.window = window
        self._ts: Dict[int, List[float]] = {}

    async def check(self, user_id: int) -> bool:
        now = time.time()
        self._ts.setdefault(user_id, [])
        self._ts[user_id] = [t for t in self._ts[user_id] if now - t <= self.window]
        if len(self._ts[user_id]) >= self.max_calls:
            return False
        self._ts[user_id].append(now)
        return True

rate_limiter = RateLimiter()

# ── Middleware & Decorators ──────────────
async def auth_middleware(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        return
    username = user.username or ""

    # Auto-promote secondary admins
    if _is_secondary_admin(username):
        await UserManager.add_user(user.id, username, "admin")

    if user.id == config.admin_user_id or _is_secondary_admin(username):
        _audit(update, user)
        return  # admins bypass rate limits and auth

    # Rate limit
    if not await rate_limiter.check(user.id):
        try:
            await context.bot.send_message(user.id, "⏳ Too many requests. Please slow down.")
        except TelegramError:
            pass
        raise ApplicationHandlerStop()

    # Auth check
    if not await UserManager.is_authorized(user.id, username):
        try:
            await context.bot.send_message(
                user.id,
                "❌ *You are not authorised.*\nAsk an admin to add you via /adduser.",
                parse_mode=ParseMode.MARKDOWN
            )
        except TelegramError:
            pass
        raise ApplicationHandlerStop()

    _audit(update, user)

def _audit(update: Update, user):
    if update.message:
        logger.info(f"AUDIT uid={user.id} @{user.username} cmd={update.message.text}")
    elif update.callback_query:
        logger.info(f"AUDIT uid={user.id} @{user.username} cb={update.callback_query.data}")

def admin_only(func):
    """Decorator – works for both Command handlers and CallbackQuery handlers."""
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        role = await UserManager.get_role(user.id, user.username or "")
        if role != "admin":
            if update.callback_query:
                await update.callback_query.answer("⛔ Admin only.", show_alert=True)
            elif update.message:
                await update.message.reply_text("⛔ Admin only.")
            return
        return await func(update, context)
    return wrapper

# ── Data Providers ───────────────────────
class DataProvider:
    async def start(self): raise NotImplementedError
    async def stop(self):  raise NotImplementedError
    async def get_candle(self, symbol: str, timeframe: int, bars: int = 100) -> pd.DataFrame:
        raise NotImplementedError

class SimulatedProvider(DataProvider):
    def __init__(self):
        self.candles: Dict[str, pd.DataFrame] = {}
        self._running = False
        self._task = None

    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._simulate())
        logger.info("SimulatedProvider started")

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
        logger.info("SimulatedProvider stopped")

    async def _simulate(self):
        base = {"EURUSD": 1.1000, "GBPUSD": 1.3000, "USDJPY": 110.00}
        while self._running:
            for sym in config.symbols:
                for tf in config.timeframes:
                    key = f"{sym}_{tf}"
                    if key not in self.candles or self.candles[key].empty:
                        self.candles[key] = pd.DataFrame(
                            columns=["open","high","low","close","volume","timestamp"])
                        lp = base.get(sym, 1.0)
                    else:
                        lp = self.candles[key]["close"].iloc[-1]
                    step = np.random.normal(0, 0.0003)
                    np_ = max(0.0001, lp + step)
                    o, c = lp, np_
                    h = max(o, c) + abs(np.random.normal(0, 0.0001))
                    l = min(o, c) - abs(np.random.normal(0, 0.0001))
                    v = np.random.randint(50, 500)
                    row = pd.DataFrame([{
                        "open": o, "high": h, "low": l, "close": c,
                        "volume": v, "timestamp": datetime.utcnow()
                    }])
                    self.candles[key] = pd.concat(
                        [self.candles[key], row], ignore_index=True).iloc[-500:]
            await asyncio.sleep(max(1, 60 // max(len(config.symbols), 1)))

    async def get_candle(self, symbol: str, timeframe: int, bars: int = 100) -> pd.DataFrame:
        key = f"{symbol}_{timeframe}"
        df = self.candles.get(key, pd.DataFrame())
        return df.tail(bars).copy() if not df.empty else pd.DataFrame()

class OlympTradeProvider(DataProvider):
    def __init__(self, ssid: str):
        self.ssid = ssid
        self.ws = None
        self.candles: Dict[str, pd.DataFrame] = {}
        self._queue: asyncio.Queue = asyncio.Queue()
        self._buffer: Dict[str, List[Dict]] = {}
        self._running = False
        self._ws_task = None
        self._agg_task = None
        self._last_agg: Dict[str, datetime] = {}

    async def start(self):
        if not self.ssid:
            logger.error("OLYMP_SSID not set.")
            return
        self._running = True
        self._ws_task  = asyncio.create_task(self._ws_connect())
        self._agg_task = asyncio.create_task(self._aggregate())
        logger.info("OlympTradeProvider started")

    async def stop(self):
        self._running = False
        for t in (self._ws_task, self._agg_task):
            if t: t.cancel()
        if self.ws:
            await self.ws.close()
        logger.info("OlympTradeProvider stopped")

    async def _ws_connect(self):
        while self._running:
            try:
                async with websockets.connect("wss://ws.olymptrade.com/ws2") as ws:
                    self.ws = ws
                    await ws.send(json.dumps({"msg": "authorize", "ssid": self.ssid, "demo": True}))
                    resp = await ws.recv()
                    logger.info(f"Olymp auth: {resp}")
                    for sym in config.symbols:
                        await ws.send(json.dumps({"msg": "subscribe_ticks", "symbols": [sym]}))
                    async for msg in ws:
                        data = json.loads(msg)
                        if "d" in data:
                            for tick in data["d"]:
                                await self._queue.put(tick)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Olymp WS error: {e} – reconnect in 5s")
                await asyncio.sleep(5)

    async def _aggregate(self):
        while self._running:
            try:
                tick = await asyncio.wait_for(self._queue.get(), timeout=1)
            except asyncio.TimeoutError:
                continue
            sym = tick.get("p")
            if not sym or sym not in config.symbols:
                continue
            price = tick["q"]
            ts = datetime.utcnow()
            self._buffer.setdefault(sym, []).append({"price": price, "ts": ts})
            for tf in config.timeframes:
                key  = f"{sym}_{tf}"
                slot = ts.replace(second=0, microsecond=0) - timedelta(minutes=ts.minute % tf)
                if self._last_agg.get(key) != slot:
                    buf = self._buffer.get(sym, [])
                    if buf:
                        prices = [b["price"] for b in buf]
                        o, h, l, c = prices[0], max(prices), min(prices), prices[-1]
                        row = pd.DataFrame([{
                            "open": o, "high": h, "low": l, "close": c,
                            "volume": len(buf),
                            "timestamp": self._last_agg.get(key, slot)
                        }])
                        if key in self.candles:
                            self.candles[key] = pd.concat(
                                [self.candles[key], row], ignore_index=True).iloc[-500:]
                        else:
                            self.candles[key] = row
                    self._buffer[sym] = []
                    self._last_agg[key] = slot

    async def get_candle(self, symbol: str, timeframe: int, bars: int = 100) -> pd.DataFrame:
        key = f"{symbol}_{timeframe}"
        df  = self.candles.get(key, pd.DataFrame())
        return df.tail(bars).copy() if not df.empty else pd.DataFrame()

def get_provider() -> DataProvider:
    if DEMO_MODE or not OLYMP_SSID:
        return SimulatedProvider()
    return OlympTradeProvider(OLYMP_SSID)

# ── AI Engine ────────────────────────────
class AIEngine:
    def __init__(self):
        self.xgb_model = None
        self.lstm_model = None
        self._try_load_xgboost()
        self._try_load_lstm()

    def _try_load_xgboost(self):
        if not config.enable_xgboost or not os.path.exists("xgb_model.json"):
            return
        try:
            import xgboost as xgb
            self.xgb_model = xgb.XGBClassifier()
            self.xgb_model.load_model("xgb_model.json")
            logger.info("XGBoost model loaded.")
        except Exception as e:
            logger.warning(f"XGBoost load failed: {e}")

    def _try_load_lstm(self):
        if not config.enable_lstm or not os.path.exists("lstm_model.h5"):
            return
        try:
            from tensorflow.keras.models import load_model
            self.lstm_model = load_model("lstm_model.h5")
            logger.info("LSTM model loaded.")
        except Exception as e:
            logger.warning(f"LSTM load failed: {e}")

    def compute_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or len(df) < 14:
            return df
        df = df.copy()
        df["rsi"] = ta.momentum.RSIIndicator(close=df["close"], window=14).rsi()
        macd = ta.trend.MACD(close=df["close"])
        df["macd"] = macd.macd()
        df["macd_signal"] = macd.macd_signal()
        df["macd_diff"] = macd.macd_diff()
        bb = ta.volatility.BollingerBands(close=df["close"], window=20, window_dev=2)
        df["bb_high"] = bb.bollinger_hband()
        df["bb_low"] = bb.bollinger_lband()
        df["bb_mid"] = bb.bollinger_mavg()
        stoch = ta.momentum.StochasticOscillator(high=df["high"], low=df["low"], close=df["close"])
        df["stoch_k"] = stoch.stoch()
        df["stoch_d"] = stoch.stoch_signal()
        adx = ta.trend.ADXIndicator(high=df["high"], low=df["low"], close=df["close"])
        df["adx"] = adx.adx()
        df["adx_pos"] = adx.adx_pos()
        df["adx_neg"] = adx.adx_neg()
        df["atr"] = ta.volatility.AverageTrueRange(high=df["high"], low=df["low"], close=df["close"]).average_true_range()
        df["ema_9"] = ta.trend.EMAIndicator(close=df["close"], window=9).ema_indicator()
        df["ema_21"] = ta.trend.EMAIndicator(close=df["close"], window=21).ema_indicator()
        df["ema_50"] = ta.trend.EMAIndicator(close=df["close"], window=50).ema_indicator()
        df["cci"] = ta.trend.CCIIndicator(high=df["high"], low=df["low"], close=df["close"]).cci()
        df["volume_sma"] = df["volume"].rolling(10).mean()
        df["volume_ratio"] = df["volume"] / (df["volume_sma"] + 1e-9)
        return df

    def indicator_scores(self, df: pd.DataFrame) -> Tuple[Dict[str, float], List[str]]:
        last = df.iloc[-1]
        scores = {}
        reasons = []

        rsi = last.get("rsi", 50)
        if pd.notna(rsi):
            if rsi < 25:
                scores["rsi"] = 1.0; reasons.append(f"RSI strongly oversold ({rsi:.1f})")
            elif rsi < 35:
                scores["rsi"] = 0.7; reasons.append(f"RSI oversold ({rsi:.1f})")
            elif rsi > 75:
                scores["rsi"] = -1.0; reasons.append(f"RSI strongly overbought ({rsi:.1f})")
            elif rsi > 65:
                scores["rsi"] = -0.7; reasons.append(f"RSI overbought ({rsi:.1f})")
            else:
                scores["rsi"] = (50 - rsi) / 25

        if pd.notna(last.get("macd")) and pd.notna(last.get("macd_signal")):
            diff = last["macd"] - last["macd_signal"]
            if diff > 0:
                scores["macd"] = 1.0; reasons.append("MACD bullish crossover")
            else:
                scores["macd"] = -1.0; reasons.append("MACD bearish crossover")

        if pd.notna(last.get("bb_low")) and pd.notna(last.get("bb_high")):
            band_width = last["bb_high"] - last["bb_low"]
            if band_width > 0:
                pos = (last["close"] - last["bb_low"]) / band_width
                if last["close"] <= last["bb_low"]:
                    scores["bb"] = 1.0; reasons.append("Price pierced lower Bollinger Band")
                elif last["close"] >= last["bb_high"]:
                    scores["bb"] = -1.0; reasons.append("Price pierced upper Bollinger Band")
                else:
                    scores["bb"] = 1 - 2 * pos

        if pd.notna(last.get("stoch_k")) and pd.notna(last.get("stoch_d")):
            if last["stoch_k"] < 20 and last["stoch_d"] < 20:
                scores["stoch"] = 1.0; reasons.append(f"Stochastic oversold K={last['stoch_k']:.1f}")
            elif last["stoch_k"] > 80 and last["stoch_d"] > 80:
                scores["stoch"] = -1.0; reasons.append(f"Stochastic overbought K={last['stoch_k']:.1f}")
            elif last["stoch_k"] > last["stoch_d"]:
                scores["stoch"] = 0.3
            else:
                scores["stoch"] = -0.3

        if pd.notna(last.get("ema_9")) and pd.notna(last.get("ema_21")):
            if last["ema_9"] > last["ema_21"]:
                scores["ma_cross"] = 1.0; reasons.append("EMA9 > EMA21 (bullish trend)")
            else:
                scores["ma_cross"] = -1.0; reasons.append("EMA9 < EMA21 (bearish trend)")

        adx = last.get("adx", 0)
        if pd.notna(adx):
            scores["adx"] = min(1.0, max(0.0, (adx - 15) / 30))
            if adx > 30: reasons.append(f"ADX {adx:.1f} – strong trend")
            elif adx > 20: reasons.append(f"ADX {adx:.1f} – moderate trend")
            else: reasons.append(f"ADX {adx:.1f} – ranging market")

        vol_r = last.get("volume_ratio", 1.0)
        if pd.notna(vol_r):
            if vol_r > 1.5: scores["volume"] = 0.8; reasons.append(f"Volume surge {vol_r:.1f}x avg")
            elif vol_r < 0.5: scores["volume"] = -0.3
            else: scores["volume"] = 0.0

        cci = last.get("cci", 0)
        if pd.notna(cci):
            if cci < -100: reasons.append(f"CCI oversold ({cci:.0f})")
            elif cci > 100: reasons.append(f"CCI overbought ({cci:.0f})")

        return scores, reasons

    def xgboost_predict(self, df: pd.DataFrame) -> Optional[float]:
        if self.xgb_model is None or not config.enable_xgboost:
            return None
        try:
            feats = self._generate_features(df).iloc[-1:].values
            prob = self.xgb_model.predict_proba(feats)[0][1]
            return (prob - 0.5) * 2
        except Exception as e:
            logger.error(f"XGB predict error: {e}")
            return None

    def _generate_features(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df = self.compute_indicators(df)
        df["returns"] = df["close"].pct_change()
        df["log_returns"] = np.log(df["close"] / df["close"].shift(1))
        df = df.fillna(0)
        return df.drop(columns=["open","high","low","close","volume","timestamp"], errors="ignore")

    def lstm_predict(self, df: pd.DataFrame) -> Optional[float]:
        if self.lstm_model is None or not config.enable_lstm:
            return None
        try:
            close = df["close"].values[-60:].astype(float)
            close = (close - np.mean(close)) / (np.std(close) + 1e-9)
            X = close.reshape(1, 60, 1)
            prob = self.lstm_model.predict(X, verbose=0)[0][0]
            return (prob - 0.5) * 2
        except Exception as e:
            logger.error(f"LSTM predict error: {e}")
            return None

    def generate_signal(self, symbol: str, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        if df is None or len(df) < 30:
            return None
        df = self.compute_indicators(df)
        ind_scores, reasons = self.indicator_scores(df)
        last = df.iloc[-1]

        tech_score = sum(
            ind_scores.get(k, 0) * config.indicator_weights.get(k, 0)
            for k in config.indicator_weights
        )
        xgb_s = self.xgboost_predict(df) if config.enable_xgboost else None
        lstm_s = self.lstm_predict(df) if config.enable_lstm else None

        final_score = tech_score
        model_parts = []
        if xgb_s is not None:
            final_score += config.xgboost_weight * xgb_s
            model_parts.append(f"XGBoost: {xgb_s:+.2f}")
        if lstm_s is not None:
            final_score += config.lstm_weight * lstm_s
            model_parts.append(f"LSTM: {lstm_s:+.2f}")
        if model_parts:
            reasons.append("AI: " + " | ".join(model_parts))

        if final_score > config.ensemble_threshold:
            direction = "BUY"
        elif final_score < -config.ensemble_threshold:
            direction = "SELL"
        else:
            return None

        atr = last.get("atr", 0)
        sl = round(last["close"] - atr * 1.5, 5) if direction == "BUY" else round(last["close"] + atr * 1.5, 5)
        tp = round(last["close"] + atr * 2.5, 5) if direction == "BUY" else round(last["close"] - atr * 2.5, 5)

        return {
            "symbol": symbol,
            "direction": direction,
            "entry": last["close"],
            "sl": sl,
            "tp": tp,
            "confidence": abs(final_score),
            "reasons": reasons,
            "timestamp": datetime.utcnow().isoformat(),
        }

# ── Helpers / UI ─────────────────────────
def _signal_strength_bar(confidence: float, length: int = 10) -> str:
    filled = round(confidence * length)
    return "█" * filled + "░" * (length - filled)

def _format_signal(sig: Dict[str, Any]) -> str:
    emoji = "🟢" if sig["direction"] == "BUY" else "🔴"
    bar = _signal_strength_bar(sig["confidence"])
    reasons = "\n".join(f"  • {r}" for r in sig["reasons"])
    return (
        f"{emoji} *{sig['symbol']} – {sig['direction']}*\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Entry:  `{sig['entry']:.5f}`\n"
        f"🛡 S/L:    `{sig['sl']:.5f}`\n"
        f"🎯 T/P:    `{sig['tp']:.5f}`\n"
        f"📶 Conf:   `[{bar}]` {sig['confidence']:.1%}\n"
        f"🕐 Time:   `{sig['timestamp'][11:19]} UTC`\n\n"
        f"📋 *Reasons:*\n{reasons}\n\n"
        f"⚠️ _Educational only. Not financial advice._"
    )

def build_main_menu(role: str) -> InlineKeyboardMarkup:
    kb = [
        [InlineKeyboardButton("📊 Status", callback_data="status"),
         InlineKeyboardButton("📈 Dashboard", callback_data="dashboard")],
        [InlineKeyboardButton("📜 History", callback_data="history"),
         InlineKeyboardButton("⚙️ Settings", callback_data="settings")],
    ]
    if role == "admin":
        kb.append([InlineKeyboardButton("👑 Admin Panel", callback_data="admin")])
    return InlineKeyboardMarkup(kb)

def _back_btn() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("« Back", callback_data="main_menu")]])

# ── Command Handlers ─────────────────────
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    uname = user.username or "unknown"
    role = "admin" if (user.id == config.admin_user_id or _is_secondary_admin(uname)) else "user"
    await UserManager.add_user(user.id, uname, role)
    role = await UserManager.get_role(user.id, uname)

    welcome = (
        f"🤖 *Educational Market Bot v2.0*\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"Welcome, *{user.first_name}*! 👋\n"
        f"Role: `{role}`\n\n"
        f"Use the menu below to navigate.\n"
        f"Type /help for all commands."
    )
    await update.message.reply_text(
        welcome, parse_mode=ParseMode.MARKDOWN,
        reply_markup=build_main_menu(role)
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    role = await UserManager.get_role(user.id, user.username or "")
    is_adm = role == "admin"

    txt = (
        "📖 *Bot Commands*\n"
        "━━━━━━━━━━━━━━━━━━━\n"
        "/start    – Main menu\n"
        "/help     – This message\n"
        "/signal   – On-demand signal scan\n"
        "/stats    – Your personal stats\n"
        "/ping     – Check bot latency\n"
    )
    if is_adm:
        txt += (
            "\n👑 *Admin Commands*\n"
            "/adduser @username  – Add user\n"
            "/removeuser @username – Remove user\n"
            "/listusers          – List all users\n"
            "/setrole @username role – Set role (user/admin)\n"
            "/broadcast msg      – Broadcast message\n"
            "/shutdown           – Stop the bot\n"
        )
    await update.message.reply_text(txt, parse_mode=ParseMode.MARKDOWN)

async def ping_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t0 = time.monotonic()
    msg = await update.message.reply_text("🏓 Pinging...")
    ms = (time.monotonic() - t0) * 1000
    await msg.edit_text(f"🏓 Pong! `{ms:.0f}ms`", parse_mode=ParseMode.MARKDOWN)

async def signal_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    syms = [args[0].upper()] if args else config.symbols
    provider: DataProvider = context.bot_data.get("provider")
    engine: AIEngine = context.bot_data.get("engine")
    if not provider or not engine:
        await update.message.reply_text("⚠️ Engine not ready yet.")
        return

    await update.message.reply_text(f"🔍 Scanning {', '.join(syms)}…")
    found = 0
    for sym in syms:
        df = await provider.get_candle(sym, config.timeframes[0], bars=100)
        if df.empty or len(df) < 30:
            await update.message.reply_text(f"⚠️ Not enough data for {sym} yet.")
            continue
        df = engine.compute_indicators(df)
        sig = engine.generate_signal(sym, df)
        if sig:
            found += 1
            await update.message.reply_text(_format_signal(sig), parse_mode=ParseMode.MARKDOWN)
        else:
            await update.message.reply_text(f"📭 No signal for {sym} (score below threshold).")
    if found == 0 and len(syms) > 1:
        await update.message.reply_text("📭 No high-confidence signals right now.")

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    rows = await db_execute(
        "SELECT COUNT(*), SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END), "
        "SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) FROM trades WHERE user_id=?",
        (uid,)
    )
    total, wins, losses = rows[0] if rows else (0, 0, 0)
    wins = wins or 0
    losses = losses or 0
    wr = wins / total * 100 if total else 0
    last_sigs = await db_execute(
        "SELECT symbol, direction, confidence, timestamp FROM signals ORDER BY id DESC LIMIT 3"
    )
    sig_lines = "\n".join(
        f"  {r[1]} {r[0]} ({r[2]:.0%}) @ {r[3][11:16]}" for r in last_sigs
    ) or "  No signals yet."
    txt = (
        f"📊 *Your Stats*\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"Total trades: `{total}`\n"
        f"Wins:  `{wins}` | Losses: `{losses}`\n"
        f"Win rate: `{wr:.1f}%`\n\n"
        f"🔎 *Last 3 Signals:*\n{sig_lines}"
    )
    await update.message.reply_text(txt, parse_mode=ParseMode.MARKDOWN)

# ── Admin Command Handlers ───────────────
@admin_only
async def add_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /adduser @username")
        return
    username = context.args[0].lstrip("@")
    await update.message.reply_text(
        f"✅ @{username} whitelisted. They'll get access when they /start the bot."
    )
    await db_execute_commit(
        "INSERT OR IGNORE INTO users (user_id, username, role, joined, active) VALUES (?,?,?,?,1)",
        (0, username, "user", datetime.utcnow().isoformat())
    )

@admin_only
async def remove_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /removeuser @username OR /removeuser <user_id>")
        return
    arg = context.args[0].lstrip("@")
    if arg.isdigit():
        uid = int(arg)
        await UserManager.remove_user(uid)
        await update.message.reply_text(f"✅ User {uid} deactivated.")
    else:
        await db_execute_commit("UPDATE users SET active=0 WHERE username=?", (arg,))
        await update.message.reply_text(f"✅ @{arg} deactivated.")

@admin_only
async def list_users_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    users = await UserManager.list_users()
    if not users:
        await update.message.reply_text("No active users.")
        return
    lines = [f"`{r[0]}` @{r[1] or '?'} [{r[2]}]" for r in users]
    txt = f"👥 *Active Users ({len(lines)})*\n" + "\n".join(lines)
    await update.message.reply_text(txt, parse_mode=ParseMode.MARKDOWN)

@admin_only
async def set_role_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /setrole @username user|admin")
        return
    username = context.args[0].lstrip("@")
    role = context.args[1].lower()
    if role not in ("user", "admin"):
        await update.message.reply_text("Role must be `user` or `admin`.")
        return
    await db_execute_commit("UPDATE users SET role=? WHERE username=?", (role, username))
    await update.message.reply_text(f"✅ @{username} role set to `{role}`.", parse_mode=ParseMode.MARKDOWN)

@admin_only
async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /broadcast <message>")
        return
    msg = " ".join(context.args)
    users = await UserManager.all_active_users()
    ok = 0
    for uid in users:
        try:
            await context.bot.send_message(uid, f"📢 *Broadcast*\n{msg}", parse_mode=ParseMode.MARKDOWN)
            ok += 1
        except TelegramError:
            pass
    await update.message.reply_text(f"📢 Sent to {ok}/{len(users)} users.")

@admin_only
async def shutdown_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🛑 Shutting down bot…")
    logger.info("Shutdown triggered by admin.")
    await context.application.stop()

# ── Callback / Inline Button Handlers ────
_admin_state: Dict[int, Dict] = {}

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user_id = query.from_user.id
    uname = query.from_user.username or ""
    role = await UserManager.get_role(user_id, uname)

    if data == "main_menu":
        await query.edit_message_text(
            "🏠 Main Menu", parse_mode=ParseMode.MARKDOWN,
            reply_markup=build_main_menu(role)
        )
        return

    if data == "status":
        symbols_str = ", ".join(config.symbols)
        tf_str = ", ".join(str(t) for t in config.timeframes)
        msg = (
            f"📊 *Bot Status*\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"Symbols:     `{symbols_str}`\n"
            f"Timeframes:  `{tf_str}` min\n"
            f"Threshold:   `{config.ensemble_threshold}`\n"
            f"Signal int:  `{config.signal_interval}s`\n"
            f"XGBoost: {'✅' if config.enable_xgboost else '❌'}  "
            f"LSTM: {'✅' if config.enable_lstm else '❌'}\n"
            f"Mode: `{'DEMO (Simulated)' if DEMO_MODE else 'LIVE'}`"
        )
        await query.edit_message_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=_back_btn())
        return

    if data == "dashboard":
        rows = await db_execute("SELECT COUNT(*), SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) FROM trades")
        total, wins = rows[0] if rows else (0, 0)
        wins = wins or 0
        wr = wins / total * 100 if total else 0
        sig_c = await db_execute("SELECT COUNT(*) FROM signals")
        sc = sig_c[0][0] if sig_c else 0
        user_c = await db_execute("SELECT COUNT(*) FROM users WHERE active=1")
        uc = user_c[0][0] if user_c else 0
        msg = (
            f"📈 *Dashboard*\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"Signals broadcast: `{sc}`\n"
            f"Trades logged:     `{total}`\n"
            f"Win rate:          `{wr:.1f}%`\n"
            f"Active users:      `{uc}`\n\n"
            f"⚠️ _Educational purposes only._"
        )
        await query.edit_message_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=_back_btn())
        return

    if data == "history":
        rows = await db_execute(
            "SELECT symbol, direction, entry, confidence, timestamp FROM signals ORDER BY id DESC LIMIT 8"
        )
        if rows:
            lines = [
                f"{'🟢' if r[1]=='BUY' else '🔴'} {r[0]} {r[1]} "
                f"@ `{r[2]:.4f}` ({r[3]:.0%}) `{r[4][11:16]}`"
                for r in rows
            ]
            msg = "📜 *Recent Signals*\n" + "\n".join(lines)
        else:
            msg = "📭 No signals recorded yet."
        await query.edit_message_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=_back_btn())
        return

    if data == "settings":
        if role != "admin":
            await query.answer("⛔ Admin only.", show_alert=True)
            return
        kb = [
            [InlineKeyboardButton(
                f"XGBoost {'✅' if config.enable_xgboost else '❌'}",
                callback_data="toggle_xgb"
             ),
             InlineKeyboardButton(
                f"LSTM {'✅' if config.enable_lstm else '❌'}",
                callback_data="toggle_lstm"
             )],
            [InlineKeyboardButton("📊 Threshold ▲", callback_data="thresh_up"),
             InlineKeyboardButton("📊 Threshold ▼", callback_data="thresh_dn")],
            [InlineKeyboardButton("⏱ Faster signals", callback_data="interval_dn"),
             InlineKeyboardButton("⏱ Slower signals", callback_data="interval_up")],
            [InlineKeyboardButton("« Back", callback_data="main_menu")],
        ]
        msg = (
            f"⚙️ *Settings*\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"XGBoost: {'ON' if config.enable_xgboost else 'OFF'}\n"
            f"LSTM:    {'ON' if config.enable_lstm else 'OFF'}\n"
            f"Threshold:       `{config.ensemble_threshold:.2f}`\n"
            f"Signal interval: `{config.signal_interval}s`"
        )
        await query.edit_message_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(kb))
        return

    if data == "toggle_xgb" and role == "admin":
        config.enable_xgboost = not config.enable_xgboost
        await query.answer(f"XGBoost {'enabled' if config.enable_xgboost else 'disabled'}")
        update.callback_query.data = "settings"
        await button_handler(update, context)
        return

    if data == "toggle_lstm" and role == "admin":
        config.enable_lstm = not config.enable_lstm
        await query.answer(f"LSTM {'enabled' if config.enable_lstm else 'disabled'}")
        update.callback_query.data = "settings"
        await button_handler(update, context)
        return

    if data == "thresh_up" and role == "admin":
        config.ensemble_threshold = min(0.95, round(config.ensemble_threshold + 0.05, 2))
        await query.answer(f"Threshold → {config.ensemble_threshold}")
        update.callback_query.data = "settings"
        await button_handler(update, context)
        return

    if data == "thresh_dn" and role == "admin":
        config.ensemble_threshold = max(0.30, round(config.ensemble_threshold - 0.05, 2))
        await query.answer(f"Threshold → {config.ensemble_threshold}")
        update.callback_query.data = "settings"
        await button_handler(update, context)
        return

    if data == "interval_up" and role == "admin":
        config.signal_interval = min(600, config.signal_interval + 30)
        await query.answer(f"Interval → {config.signal_interval}s")
        update.callback_query.data = "settings"
        await button_handler(update, context)
        return

    if data == "interval_dn" and role == "admin":
        config.signal_interval = max(15, config.signal_interval - 30)
        await query.answer(f"Interval → {config.signal_interval}s")
        update.callback_query.data = "settings"
        await button_handler(update, context)
        return

    if data == "admin":
        if role != "admin":
            await query.answer("⛔ Admin only.", show_alert=True)
            return
        await _show_admin_panel(query)
        return

    if data == "list_users" and role == "admin":
        users = await UserManager.list_users()
        if not users:
            await query.edit_message_text("No active users.", reply_markup=_back_btn())
            return
        lines = [f"`{r[0]}` @{r[1] or '?'} [{r[2]}]" for r in users]
        txt = f"👥 *Active Users ({len(lines)})*\n" + "\n".join(lines)
        await query.edit_message_text(txt, parse_mode=ParseMode.MARKDOWN, reply_markup=_back_btn())
        return

    if data == "shutdown" and role == "admin":
        await query.edit_message_text("🛑 Shutting down…")
        logger.info("Shutdown via admin panel.")
        await context.application.stop()
        return

    if data in ("add_user", "rem_user", "broadcast") and role == "admin":
        prompts = {
            "add_user":  "Send me the Telegram user_id to add (number):",
            "rem_user":  "Send me the user_id or @username to remove:",
            "broadcast": "Send me the message to broadcast:",
        }
        _admin_state[user_id] = {"action": data}
        await query.edit_message_text(
            f"✏️ {prompts[data]}\n\n_(send /cancel to abort)_",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    await query.edit_message_text("❓ Unknown action.", reply_markup=build_main_menu(role))

async def _show_admin_panel(query):
    total_users = await db_execute("SELECT COUNT(*) FROM users WHERE active=1")
    total_sigs  = await db_execute("SELECT COUNT(*) FROM signals")
    uc = total_users[0][0] if total_users else 0
    sc = total_sigs[0][0]  if total_sigs  else 0
    kb = [
        [InlineKeyboardButton("➕ Add User", callback_data="add_user"),
         InlineKeyboardButton("➖ Remove User", callback_data="rem_user")],
        [InlineKeyboardButton("📋 List Users", callback_data="list_users")],
        [InlineKeyboardButton("📢 Broadcast", callback_data="broadcast")],
        [InlineKeyboardButton("🛑 Shutdown", callback_data="shutdown")],
        [InlineKeyboardButton("« Back", callback_data="main_menu")],
    ]
    await query.edit_message_text(
        f"👑 *Admin Panel*\nUsers: `{uc}` | Signals: `{sc}`",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(kb)
    )

async def admin_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    role = await UserManager.get_role(user_id, update.effective_user.username or "")
    if role != "admin":
        return
    state = _admin_state.get(user_id)
    if not state:
        return

    text = update.message.text.strip()
    action = state["action"]
    _admin_state.pop(user_id, None)

    if text == "/cancel":
        await update.message.reply_text("❌ Cancelled.")
        return

    if action == "add_user":
        if text.isdigit():
            await UserManager.add_user(int(text), "unknown", "user")
            await update.message.reply_text(f"✅ User `{text}` added.", parse_mode=ParseMode.MARKDOWN)
        else:
            await update.message.reply_text("⚠️ Expected a numeric user_id.")

    elif action == "rem_user":
        if text.lstrip("@").isdigit():
            uid = int(text.lstrip("@"))
            await UserManager.remove_user(uid)
            await update.message.reply_text(f"✅ User `{uid}` deactivated.", parse_mode=ParseMode.MARKDOWN)
        else:
            uname = text.lstrip("@")
            await db_execute_commit("UPDATE users SET active=0 WHERE username=?", (uname,))
            await update.message.reply_text(f"✅ @{uname} deactivated.")

    elif action == "broadcast":
        users = await UserManager.all_active_users()
        ok = 0
        for uid in users:
            try:
                await context.bot.send_message(
                    uid, f"📢 *Broadcast*\n{text}", parse_mode=ParseMode.MARKDOWN)
                ok += 1
            except TelegramError:
                pass
        await update.message.reply_text(f"📢 Sent to {ok}/{len(users)} users.")

# ── Error Handler ───────────────────────
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Error: {context.error}", exc_info=context.error)
    if update and hasattr(update, "effective_message") and update.effective_message:
        try:
            await update.effective_message.reply_text("⚠️ Something went wrong. Please try again.")
        except Exception:
            pass

# ── Background Signal Loop ──────────────
async def signal_loop(provider: DataProvider, engine: AIEngine, app: Application):
    last_signal_time: Dict[str, float] = {}
    while True:
        try:
            for sym in config.symbols:
                now = time.time()
                if now - last_signal_time.get(sym, 0) < config.signal_cooldown:
                    continue

                df = await provider.get_candle(sym, config.timeframes[0], bars=100)
                if df.empty or len(df) < 30:
                    continue

                df_ind = engine.compute_indicators(df)
                sig = engine.generate_signal(sym, df_ind)
                if not sig:
                    continue

                last_signal_time[sym] = now
                await db_execute_commit(
                    "INSERT INTO signals (symbol,direction,entry,confidence,timestamp,reasons) "
                    "VALUES (?,?,?,?,?,?)",
                    (sym, sig["direction"], sig["entry"], sig["confidence"],
                     sig["timestamp"], "; ".join(sig["reasons"]))
                )

                msg_txt = _format_signal(sig)
                users = await UserManager.all_active_users()
                ok = 0
                for uid in users:
                    try:
                        await app.bot.send_message(uid, msg_txt, parse_mode=ParseMode.MARKDOWN)
                        ok += 1
                    except TelegramError:
                        pass
                logger.info(f"Signal {sym} {sig['direction']} conf={sig['confidence']:.2%} sent to {ok} users.")
        except Exception as e:
            logger.error(f"Signal loop error: {e}", exc_info=True)
        await asyncio.sleep(config.signal_interval)

# ── Main ────────────────────────────────
async def main():
    await init_db()
    provider = get_provider()
    await provider.start()
    engine = AIEngine()

    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.bot_data["provider"] = provider
    app.bot_data["engine"] = engine

    # Middleware
    app.add_handler(MessageHandler(filters.ALL, auth_middleware), group=-1)

    # Commands
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("ping", ping_command))
    app.add_handler(CommandHandler("signal", signal_command))
    app.add_handler(CommandHandler("stats", stats_command))
    app.add_handler(CommandHandler("adduser", add_user_command))
    app.add_handler(CommandHandler("removeuser", remove_user_command))
    app.add_handler(CommandHandler("listusers", list_users_command))
    app.add_handler(CommandHandler("setrole", set_role_command))
    app.add_handler(CommandHandler("broadcast", broadcast_command))
    app.add_handler(CommandHandler("shutdown", shutdown_command))

    # Inline buttons
    app.add_handler(CallbackQueryHandler(button_handler))

    # Multi-step admin input
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, admin_text_handler), group=1
    )

    app.add_error_handler(error_handler)

    # Start background signal loop
    signal_task = asyncio.create_task(signal_loop(provider, engine, app))

    logger.info(f"Bot starting | Admin: {ADMIN_USER_ID} | Secondary admins: {SECONDARY_ADMIN_USERNAMES}")

    try:
        # This blocks until a stop signal is received (e.g. /shutdown, Ctrl+C)
        await app.run_polling(drop_pending_updates=True)
    finally:
        # Cleanup
        signal_task.cancel()
        try:
            await signal_task
        except asyncio.CancelledError:
            pass
        await provider.stop()
        await app.shutdown()
        logger.info("Bot shut down cleanly.")

if __name__ == "__main__":
    asyncio.run(main())
                    
