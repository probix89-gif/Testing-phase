#!/usr/bin/env python3
"""
Educational Market Analysis Bot – PROBIX_XD Admin Edition
Single-file, fully async, secure, with explainable AI signals.
No real money trading – only educational alerts.
"""

import asyncio
import hashlib
import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

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
    BaseHandler,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# -------------------------------
# 1. Environment & Configuration
# -------------------------------
load_dotenv()

# Validate required variables
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
if not TELEGRAM_TOKEN or len(TELEGRAM_TOKEN) < 10:
    raise SystemExit("FATAL: TELEGRAM_TOKEN not set or invalid in .env")

ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0"))  # Telegram numeric ID
OLYMP_SSID = os.getenv("OLYMP_SSID", "")
DEMO_MODE = os.getenv("DEMO_MODE", "1") == "1"
DB_PATH = os.getenv("DB_PATH", "ultimate_bot.db")
LOG_FILE = os.getenv("LOG_FILE", "bot.log")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# Structured logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format='{"time":"%(asctime)s","level":"%(levelname)s","name":"%(name)s","message":"%(message)s"}',
    handlers=[logging.StreamHandler(), logging.FileHandler(LOG_FILE)],
)
logger = logging.getLogger("EDU_BOT")

# Prevent token from being logged
logging.getLogger("httpx").setLevel(logging.WARNING)


# -------------------------------
# 2. Configuration Dataclass
# -------------------------------
@dataclass
class AppConfig:
    symbols: List[str] = field(default_factory=lambda: ["EURUSD", "GBPUSD", "USDJPY"])
    timeframes: List[int] = field(default_factory=lambda: [1, 5])  # minutes
    indicator_weights: Dict[str, float] = field(default_factory=lambda: {
        "rsi": 0.15, "macd": 0.15, "bb": 0.10, "stoch": 0.10,
        "adx": 0.05, "ma_cross": 0.05, "atr": 0.05, "volume": 0.05
    })
    xgboost_weight: float = 0.2
    lstm_weight: float = 0.15
    ensemble_threshold: float = 0.6
    risk_per_trade: float = 0.0        # educational – no real trades
    enable_lstm: bool = True
    enable_xgboost: bool = True
    # The admin user is loaded from env, not from DB
    admin_user_id: int = ADMIN_USER_ID

config = AppConfig()


# -------------------------------
# 3. Database Layer (SQLite)
# -------------------------------
db_pool = None  # will be set in init_db

async def init_db():
    global db_pool
    db_pool = await aiosqlite.connect(DB_PATH)
    await db_pool.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            role TEXT DEFAULT 'user',
            joined TEXT,
            active INTEGER DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            timestamp TEXT,
            symbol TEXT,
            direction TEXT,
            entry REAL,
            expiry TEXT,
            result TEXT DEFAULT 'PENDING',
            pnl REAL DEFAULT 0,
            confidence REAL
        );
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            direction TEXT,
            entry REAL,
            confidence REAL,
            timestamp TEXT,
            reasons TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_trades_user ON trades(user_id);
        CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades(timestamp);
        CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signals(symbol);
        CREATE INDEX IF NOT EXISTS idx_users_active ON users(active);
    """)
    await db_pool.commit()
    logger.info("Database ready.")

# Repository functions using the pool
async def db_execute(query: str, params=None):
    async with db_pool.execute(query, params or ()) as cursor:
        return await cursor.fetchall()

async def db_execute_commit(query: str, params=None):
    await db_pool.execute(query, params or ())
    await db_pool.commit()


# -------------------------------
# 4. User & Role Management
# -------------------------------
class UserManager:
    @staticmethod
    async def is_authorized(user_id: int) -> bool:
        rows = await db_execute("SELECT active FROM users WHERE user_id=?", (user_id,))
        return bool(rows) and rows[0][0] == 1

    @staticmethod
    async def get_role(user_id: int) -> str:
        if user_id == config.admin_user_id:
            return "admin"
        rows = await db_execute("SELECT role FROM users WHERE user_id=? AND active=1", (user_id,))
        return rows[0][0] if rows else "unauthorised"

    @staticmethod
    async def add_user(user_id: int, username: str, role="user"):
        await db_execute_commit(
            "INSERT OR REPLACE INTO users (user_id, username, role, joined, active) VALUES (?,?,?,?,1)",
            (user_id, username, role, datetime.utcnow().isoformat())
        )

    @staticmethod
    async def remove_user(user_id: int):
        await db_execute_commit("UPDATE users SET active=0 WHERE user_id=?", (user_id,))

    @staticmethod
    async def all_active_users() -> List[int]:
        rows = await db_execute("SELECT user_id FROM users WHERE active=1")
        return [r[0] for r in rows]


# -------------------------------
# 5. Middleware (Auth, Rate Limiting, Audit)
# -------------------------------
class RateLimiter:
    """Simple in-memory per-user rate limit."""
    def __init__(self, max_calls: int = 10, window: int = 60):
        self.max_calls = max_calls
        self.window = window
        self._timestamps: Dict[int, List[float]] = {}

    async def check(self, user_id: int) -> bool:
        now = time.time()
        if user_id not in self._timestamps:
            self._timestamps[user_id] = []
        self._timestamps[user_id] = [t for t in self._timestamps[user_id] if now - t <= self.window]
        if len(self._timestamps[user_id]) >= self.max_calls:
            return False
        self._timestamps[user_id].append(now)
        return True

rate_limiter = RateLimiter()

async def auth_middleware(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Middleware that runs before every handler."""
    user = update.effective_user
    if not user:
        return
    # Skip rate limit for admin
    if user.id != config.admin_user_id:
        if not await rate_limiter.check(user.id):
            await context.bot.send_message(user.id, "⏳ Please slow down.")
            raise SystemExit()
        if not await UserManager.is_authorized(user.id):
            await context.bot.send_message(user.id, "❌ You are not authorised.")
            raise SystemExit()

    # Audit log
    if update.message:
        logger.info(f"AUDIT user={user.id} ({user.username}) cmd={update.message.text}")
    elif update.callback_query:
        logger.info(f"AUDIT user={user.id} ({user.username}) callback={update.callback_query.data}")


# Decorator for admin-only commands
def admin_only(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != config.admin_user_id:
            await update.message.reply_text("⛔ Admin only.")
            return
        return await func(update, context)
    return wrapper


# -------------------------------
# 6. Data Providers
# -------------------------------
class DataProvider:
    async def start(self): raise NotImplementedError
    async def stop(self): raise NotImplementedError
    async def get_candle(self, symbol: str, timeframe: int, bars: int = 100) -> pd.DataFrame:
        raise NotImplementedError

class SimulatedProvider(DataProvider):
    """Synthetic OHLC using geometric Brownian motion – educational."""
    def __init__(self, symbols: List[str] = None):
        self.symbols = symbols or config.symbols
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
        while self._running:
            for sym in self.symbols:
                for tf in config.timeframes:
                    key = f"{sym}_{tf}"
                    if key not in self.candles or self.candles[key].empty:
                        self.candles[key] = pd.DataFrame(columns=["open","high","low","close","volume","timestamp"])
                        last_price = 1.1000 if "EUR" in sym else 1.3000 if "GBP" in sym else 110.00
                    else:
                        last_price = self.candles[key]["close"].iloc[-1]
                    step = np.random.normal(0, 0.0002)
                    new_price = last_price + step
                    o = last_price
                    h = max(last_price, new_price) + np.random.uniform(0, 0.0001)
                    l = min(last_price, new_price) - np.random.uniform(0, 0.0001)
                    c = new_price
                    v = np.random.randint(10, 100)
                    new_row = pd.DataFrame([{
                        "open": o, "high": h, "low": l, "close": c,
                        "volume": v, "timestamp": datetime.utcnow()
                    }])
                    self.candles[key] = pd.concat([self.candles[key], new_row], ignore_index=True).iloc[-500:]
            await asyncio.sleep(60 / len(self.symbols) / max(len(config.timeframes), 1))

    async def get_candle(self, symbol: str, timeframe: int, bars=100) -> pd.DataFrame:
        key = f"{symbol}_{timeframe}"
        df = self.candles.get(key, pd.DataFrame())
        return df.tail(bars).copy() if not df.empty else pd.DataFrame()


class OlympTradeProvider(DataProvider):
    """
    Real Olymp Trade WebSocket data. Requires valid OLYMP_SSID.
    Aggregates ticks into OHLC candles.
    """
    def __init__(self, ssid: str):
        self.ssid = ssid
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.candles: Dict[str, pd.DataFrame] = {}
        self._ticks_queue = asyncio.Queue()
        self._buffer: Dict[str, List[Dict]] = {}
        self._running = False
        self._ws_task = None
        self._aggregator_task = None
        self._last_aggregation: Dict[str, datetime] = {}

    async def start(self):
        if not self.ssid:
            logger.error("OLYMP_SSID not set. Cannot connect.")
            return
        self._running = True
        self._ws_task = asyncio.create_task(self._ws_connect())
        self._aggregator_task = asyncio.create_task(self._aggregate_ticks())
        logger.info("OlympTradeProvider started")

    async def stop(self):
        self._running = False
        if self._ws_task:
            self._ws_task.cancel()
        if self._aggregator_task:
            self._aggregator_task.cancel()
        if self.ws:
            await self.ws.close()
        logger.info("OlympTradeProvider stopped")

    async def _ws_connect(self):
        while self._running:
            try:
                async with websockets.connect("wss://ws.olymptrade.com/ws2") as ws:
                    self.ws = ws
                    # Auth
                    await ws.send(json.dumps({"msg": "authorize", "ssid": self.ssid, "demo": True}))
                    resp = await ws.recv()
                    logger.info(f"Olymp auth response: {resp}")
                    # Subscribe
                    for sym in config.symbols:
                        await ws.send(json.dumps({"msg": "subscribe_ticks", "symbols": [sym]}))
                    async for msg in ws:
                        data = json.loads(msg)
                        if "d" in data:
                            for tick in data["d"]:
                                await self._ticks_queue.put(tick)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Olymp WS error: {e}, reconnect in 5s")
                await asyncio.sleep(5)

    async def _aggregate_ticks(self):
        while self._running:
            try:
                tick = await asyncio.wait_for(self._ticks_queue.get(), timeout=1)
            except asyncio.TimeoutError:
                continue
            symbol = tick.get("p")
            if not symbol or symbol not in config.symbols:
                continue
            price = tick["q"]
            ts = datetime.utcnow()
            # Buffer tick
            if symbol not in self._buffer:
                self._buffer[symbol] = []
            self._buffer[symbol].append({"price": price, "ts": ts})
            # Aggregate per timeframe
            for tf in config.timeframes:
                key = f"{symbol}_{tf}"
                slot = ts.replace(second=0, microsecond=0) - timedelta(minutes=ts.minute % tf)
                if key not in self._last_aggregation or self._last_aggregation[key] != slot:
                    # Close previous candle and create new
                    buffer = self._buffer.get(symbol, [])
                    if buffer:
                        prices = [b["price"] for b in buffer]
                        o, h, l, c = prices[0], max(prices), min(prices), prices[-1]
                        vol = len(buffer)
                        new = pd.DataFrame([{
                            "open": o, "high": h, "low": l, "close": c,
                            "volume": vol, "timestamp": self._last_aggregation.get(key, slot)
                        }])
                        if key in self.candles:
                            self.candles[key] = pd.concat([self.candles[key], new], ignore_index=True).iloc[-500:]
                        else:
                            self.candles[key] = new
                    self._buffer[symbol] = []
                    self._last_aggregation[key] = slot

    async def get_candle(self, symbol: str, timeframe: int, bars=100) -> pd.DataFrame:
        key = f"{symbol}_{timeframe}"
        df = self.candles.get(key, pd.DataFrame())
        return df.tail(bars).copy() if not df.empty else pd.DataFrame()


def get_provider() -> DataProvider:
    if DEMO_MODE or not OLYMP_SSID:
        return SimulatedProvider()
    return OlympTradeProvider(OLYMP_SSID)


# -------------------------------
# 7. AI Engine – Indicators & Ensemble
# -------------------------------
class AIEngine:
    def __init__(self):
        self.xgb_model = None
        self.lstm_model = None
        # Try loading pre-trained models from disk (educational)
        if os.path.exists("xgb_model.json"):
            try:
                import xgboost as xgb
                self.xgb_model = xgb.XGBClassifier()
                self.xgb_model.load_model("xgb_model.json")
                logger.info("XGBoost model loaded.")
            except Exception as e:
                logger.warning(f"Cannot load XGBoost: {e}")
        if os.path.exists("lstm_model.h5") and config.enable_lstm:
            try:
                from tensorflow.keras.models import load_model
                self.lstm_model = load_model("lstm_model.h5")
                logger.info("LSTM model loaded.")
            except Exception as e:
                logger.warning(f"Cannot load LSTM: {e}")

    def compute_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add all technical indicators used for signals and explainability."""
        if df.empty or len(df) < 2:
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
        stoch = ta.momentum.StochasticOscillator(high=df["high"], low=df["low"], close=df["close"])
        df["stoch_k"] = stoch.stoch()
        df["stoch_d"] = stoch.stoch_signal()
        df["adx"] = ta.trend.ADXIndicator(high=df["high"], low=df["low"], close=df["close"]).adx()
        df["atr"] = ta.volatility.AverageTrueRange(high=df["high"], low=df["low"], close=df["close"]).average_true_range()
        df["ema_9"] = ta.trend.EMAIndicator(close=df["close"], window=9).ema_indicator()
        df["ema_21"] = ta.trend.EMAIndicator(close=df["close"], window=21).ema_indicator()
        df["volume_sma"] = df["volume"].rolling(10).mean()
        return df

    def indicator_scores(self, df: pd.DataFrame) -> Dict[str, float]:
        """Score each indicator between -1 and +1."""
        last = df.iloc[-1]
        scores = {}
        # RSI
        rsi = last.get("rsi", 50)
        if pd.notna(rsi):
            if rsi < 30: scores["rsi"] = 1.0
            elif rsi > 70: scores["rsi"] = -1.0
            else: scores["rsi"] = (50 - rsi) / 20
        # MACD
        if pd.notna(last.get("macd")) and pd.notna(last.get("macd_signal")):
            scores["macd"] = 1.0 if last["macd"] > last["macd_signal"] else -1.0
        # Bollinger
        if pd.notna(last.get("bb_low")):
            if last["close"] <= last["bb_low"]: scores["bb"] = 1.0
            elif last["close"] >= last["bb_high"]: scores["bb"] = -1.0
            else: scores["bb"] = 0.0
        # Stochastic
        if pd.notna(last.get("stoch_k")) and pd.notna(last.get("stoch_d")):
            if last["stoch_k"] < 20 and last["stoch_d"] < 20: scores["stoch"] = 1.0
            elif last["stoch_k"] > 80 and last["stoch_d"] > 80: scores["stoch"] = -1.0
            else: scores["stoch"] = 0.0
        # MA cross
        if pd.notna(last.get("ema_9")) and pd.notna(last.get("ema_21")):
            scores["ma_cross"] = 1.0 if last["ema_9"] > last["ema_21"] else -1.0
        # ADX (only gives direction weight)
        adx = last.get("adx", 0)
        if pd.notna(adx):
            scores["adx"] = min(1.0, (adx - 15) / 30)  # scaling
        # ATR and Volume not directional, we skip for scores here.
        return scores

    def xgboost_predict(self, df: pd.DataFrame) -> Optional[float]:
        if self.xgb_model is None:
            return None
        try:
            features = self._generate_features(df).iloc[-1:].values
            prob = self.xgb_model.predict_proba(features)[0][1]
            return (prob - 0.5) * 2
        except Exception as e:
            logger.error(f"XGB predict error: {e}")
            return None

    def _generate_features(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df = self.compute_indicators(df)
        # Add returns etc.
        df['returns'] = df['close'].pct_change()
        df['log_returns'] = np.log(df['close'] / df['close'].shift(1))
        df = df.fillna(0)
        # Use only known features; in a real model we'd have a predefined list.
        return df.drop(columns=['open','high','low','close','volume','timestamp'], errors='ignore')

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

    def generate_educational_signal(self, symbol: str, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        """Returns None or a dict with direction, confidence, and educational reasons."""
        if df is None or len(df) < 30:
            return None
        ind_scores = self.indicator_scores(df)
        # Build explanation
        reasons = []
        last = df.iloc[-1]
        if ind_scores.get("rsi", 0) > 0.5: reasons.append(f"RSI oversold ({last.get('rsi',50):.1f})")
        if ind_scores.get("rsi", 0) < -0.5: reasons.append(f"RSI overbought ({last.get('rsi',50):.1f})")
        if ind_scores.get("macd", 0) > 0: reasons.append("MACD bullish crossover")
        if ind_scores.get("macd", 0) < 0: reasons.append("MACD bearish crossover")
        if ind_scores.get("bb", 0) > 0: reasons.append("Price at lower Bollinger band")
        if ind_scores.get("bb", 0) < 0: reasons.append("Price at upper Bollinger band")
        if ind_scores.get("ma_cross", 0) > 0: reasons.append("EMA 9 > EMA 21 (short-term uptrend)")
        if ind_scores.get("ma_cross", 0) < 0: reasons.append("EMA 9 < EMA 21 (short-term downtrend)")
        adx = last.get("adx", 0)
        if pd.notna(adx) and adx > 25:
            reasons.append(f"ADX {adx:.1f} indicates a trending market")
        else:
            reasons.append("ADX indicates range-bound market")

        # Weighted technical score
        tech_score = sum(ind_scores.get(k,0)*config.indicator_weights.get(k,0) for k in config.indicator_weights)
        # Add model predictions if available
        xgb_s = self.xgboost_predict(df) if config.enable_xgboost else None
        lstm_s = self.lstm_predict(df) if config.enable_lstm else None
        final_score = tech_score
        if xgb_s is not None:
            final_score += config.xgboost_weight * xgb_s
        if lstm_s is not None:
            final_score += config.lstm_weight * lstm_s
        direction = None
        if final_score > config.ensemble_threshold:
            direction = "BUY"
        elif final_score < -config.ensemble_threshold:
            direction = "SELL"
        if direction is None:
            return None

        return {
            "symbol": symbol,
            "direction": direction,
            "entry": last["close"],
            "confidence": abs(final_score),
            "reasons": reasons,
            "timestamp": datetime.utcnow().isoformat()
        }


# -------------------------------
# 8. Telegram Bot Handlers
# -------------------------------
def build_main_menu(user_role: str) -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("📊 Status", callback_data="status"),
         InlineKeyboardButton("📈 Dashboard", callback_data="dashboard")],
        [InlineKeyboardButton("📜 Signal History", callback_data="history"),
         InlineKeyboardButton("⚙ Settings", callback_data="settings")],
    ]
    if user_role == "admin":
        keyboard.append([InlineKeyboardButton("👑 Admin Panel", callback_data="admin")])
    return InlineKeyboardMarkup(keyboard)


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    # Auto-register user (first contact)
    await UserManager.add_user(user.id, user.username or "unknown")
    if user.id == config.admin_user_id:
        await UserManager.add_user(user.id, user.username or "admin", "admin")
    role = await UserManager.get_role(user.id)
    await update.message.reply_text(
        f"🎓 *Educational Market Analysis Bot*\nWelcome, {user.first_name}!",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=build_main_menu(role)
    )


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user_id = query.from_user.id
    role = await UserManager.get_role(user_id)

    if data == "status":
        msg = (
            f"📊 *Bot Status*\n"
            f"Symbols: {', '.join(config.symbols)}\n"
            f"Timeframes: {', '.join(str(t) for t in config.timeframes)} min\n"
            f"Ensemble threshold: {config.ensemble_threshold}\n"
            f"XGBoost: {'✅' if config.enable_xgboost else '❌'}  LSTM: {'✅' if config.enable_lstm else '❌'}"
        )
        await query.edit_message_text(msg, parse_mode=ParseMode.MARKDOWN)
    elif data == "dashboard":
        # Simplified dashboard
        rows = await db_execute("SELECT COUNT(*), SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) FROM trades")
        total, wins = rows[0] if rows else (0,0)
        winrate = (wins/total*100) if total else 0
        msg = f"📈 *Dashboard*\nTotal signals: {total}\nWin rate: {winrate:.1f}%\n⚠️ Educational only"
        await query.edit_message_text(msg, parse_mode=ParseMode.MARKDOWN)
    elif data == "history":
        rows = await db_execute("SELECT * FROM signals ORDER BY id DESC LIMIT 5")
        if rows:
            lines = [f"{r[2]} {r[3]} @ {r[4]:.4f} ({datetime.fromisoformat(r[6]).strftime('%H:%M')})" for r in rows]
            msg = "📜 *Recent Signals*\n" + "\n".join(lines)
        else:
            msg = "No signals yet."
        await query.edit_message_text(msg, parse_mode=ParseMode.MARKDOWN)
    elif data == "settings":
        await query.edit_message_text("Settings module under construction.", reply_markup=build_main_menu(role))
    elif data == "admin" and role == "admin":
        await admin_panel(update, context)
    else:
        await query.edit_message_text("Unknown action.", reply_markup=build_main_menu(role))


@admin_only
async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    kbd = [
        [InlineKeyboardButton("➕ Add User", callback_data="add_user"),
         InlineKeyboardButton("➖ Remove User", callback_data="rem_user")],
        [InlineKeyboardButton("📋 List Users", callback_data="list_users")],
        [InlineKeyboardButton("📢 Broadcast", callback_data="broadcast")],
        [InlineKeyboardButton("🛑 Shutdown", callback_data="shutdown")],
    ]
    await (query.edit_message_text if query else update.message.reply_text)(
        "👑 Admin Panel", reply_markup=InlineKeyboardMarkup(kbd)
    )


async def add_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != config.admin_user_id:
        return
    if not context.args:
        await update.message.reply_text("Usage: /adduser @username")
        return
    username = context.args[0].lstrip("@")
    # Note: Telegram doesn't give user_id by username via bot API easily.
    # This is a simplified placeholder – the real approach requires user to /start first.
    await update.message.reply_text(f"User @{username} added when they /start.")


@admin_only
async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /broadcast <message>")
        return
    msg = " ".join(context.args)
    users = await UserManager.all_active_users()
    success = 0
    for uid in users:
        try:
            await context.bot.send_message(uid, f"📢 {msg}")
            success += 1
        except TelegramError:
            pass
    await update.message.reply_text(f"Broadcast sent to {success}/{len(users)} users.")


@admin_only
async def shutdown_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🛑 Shutting down...")
    await context.application.stop()
    sys.exit(0)


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error(f"Update {update} caused error {context.error}")
    if update and hasattr(update, "effective_message"):
        try:
            await context.bot.send_message(
                update.effective_chat.id, "⚠️ An error occurred. Please try again later."
            )
        except Exception:
            pass


# -------------------------------
# 9. Background Signal Loop
# -------------------------------
async def signal_loop(provider: DataProvider, engine: AIEngine, bot_app):
    """Continuously generate and broadcast educational signals."""
    while True:
        try:
            for sym in config.symbols:
                df = await provider.get_candle(sym, config.timeframes[0], bars=100)
                if df.empty:
                    continue
                signal = engine.generate_educational_signal(sym, df)
                if signal:
                    # Save to DB
                    await db_execute_commit(
                        "INSERT INTO signals (symbol, direction, entry, confidence, timestamp, reasons) VALUES (?,?,?,?,?,?)",
                        (sym, signal["direction"], signal["entry"], signal["confidence"],
                         signal["timestamp"], "; ".join(signal["reasons"]))
                    )
                    # Build educational message
                    reasons_text = "\n".join(f"• {r}" for r in signal["reasons"])
                    msg = (
                        f"🔎 *Educational Alert – {sym}*\n"
                        f"Direction: `{signal['direction']}`\n"
                        f"Entry: {signal['entry']:.5f}\n"
                        f"Confidence: {signal['confidence']:.2%}\n"
                        f"Reasons:\n{reasons_text}\n\n"
                        f"⚠️ *This is not financial advice.* Trading involves risk."
                    )
                    # Send to all authorised users
                    for uid in await UserManager.all_active_users():
                        try:
                            await bot_app.bot.send_message(uid, msg, parse_mode=ParseMode.MARKDOWN)
                        except TelegramError:
                            pass
                    logger.info(f"Signal broadcast: {sym} {signal['direction']}")
        except Exception as e:
            logger.error(f"Signal loop error: {e}")
        await asyncio.sleep(60)


# -------------------------------
# 10. Main Entry Point
# -------------------------------
async def main():
    # Init DB
    await init_db()

    # Create provider
    provider = get_provider()
    await provider.start()

    # AI Engine
    engine = AIEngine()

    # Build Telegram application with middleware
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    # Register custom middleware as first handler
    app.add_handler(MessageHandler(filters.ALL, auth_middleware), group=-1)

    # Register commands
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("adduser", add_user_command))
    app.add_handler(CommandHandler("broadcast", broadcast_command))
    app.add_handler(CommandHandler("shutdown", shutdown_command))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_error_handler(error_handler)

    # Start background signal loop
    signal_task = asyncio.create_task(signal_loop(provider, engine, app))

    # Graceful shutdown
    async def shutdown_hook():
        logger.info("Shutting down...")
        signal_task.cancel()
        await provider.stop()
        await app.stop()
        await app.shutdown()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.ensure_future(shutdown_hook()))
        except NotImplementedError:
            # Windows doesn't support add_signal_handler
            pass

    logger.info("Bot starting...")
    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    await app.idle()

if __name__ == "__main__":
    asyncio.run(main())
