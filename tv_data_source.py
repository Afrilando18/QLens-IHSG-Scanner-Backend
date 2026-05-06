"""
TradingView data sources for IHSG Screener v9.3
================================================

Provides 2 layers:
1. tv_scanner_fetch_all(tickers)  - REST snapshot dari scanner.tradingview.com
                                    (no-auth, 1 request untuk semua ticker)
2. tv_datafeed_fetch(...)         - Realtime tick-level via WebSocket
                                    (butuh akun TV, env: TV_USERNAME / TV_PASSWORD)

Schema output dibikin kompatibel dengan fetch_batch_yf() existing supaya
drop-in replacement.
"""

import os
import time
import logging
import threading
from datetime import datetime
from typing import List, Dict, Optional

import requests

logger = logging.getLogger(__name__)

# ============================================================
# LAYER 1: TradingView Scanner REST API (no-auth, snapshot)
# ============================================================

_SCANNER_URL = "https://scanner.tradingview.com/indonesia/scan"
_SCANNER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/121.0.0.0 Safari/537.36",
    "Content-Type": "application/json",
    "Origin": "https://www.tradingview.com",
    "Referer": "https://www.tradingview.com/",
}

# Field yang kita request dari TV scanner. Sudah precomputed di server TV.
_SCANNER_COLUMNS = [
    "name",                    # ticker
    "description",             # nama perusahaan
    "close",
    "change",                  # change %
    "change_abs",              # change absolute
    "volume",
    "Value.Traded",            # nilai transaksi (Rp)
    "open",
    "high",
    "low",
    "RSI",
    "MACD.macd",
    "MACD.signal",
    "SMA20",
    "SMA50",
    "SMA200",
    "EMA20",
    "BB.upper",
    "BB.lower",
    "ATR",
    "ADX",
    "Stoch.K",
    "market_cap_basic",
    "price_earnings_ttm",
    "price_book_ratio",
    "earnings_per_share_basic_ttm",
    "dividend_yield_recent",
    "Recommend.All",           # technical rating -1..1
    "Volatility.D",
    "average_volume_30d_calc",
    "relative_volume_10d_calc",
]


def tv_scanner_fetch_all(tickers: List[str], timeout: int = 15) -> List[Dict]:
    """
    Fetch snapshot 1-shot untuk SEMUA ticker via TradingView scanner.
    Jauh lebih cepat dari per-ticker Yahoo: ~1-2 detik untuk 450 ticker.
    """
    if not tickers:
        return []

    payload = {
        "symbols": {
            "tickers": [f"IDX:{t}" for t in tickers],
            "query": {"types": []},
        },
        "columns": _SCANNER_COLUMNS,
    }

    try:
        t0 = time.time()
        r = requests.post(_SCANNER_URL, json=payload,
                          headers=_SCANNER_HEADERS, timeout=timeout)
        r.raise_for_status()
        raw = r.json()
        elapsed = time.time() - t0

        rows = raw.get("data", []) or []
        out = []
        now_iso = datetime.now().isoformat()

        for row in rows:
            try:
                sym = row.get("s", "")  # "IDX:BBCA"
                ticker = sym.split(":")[-1]
                d = row.get("d", [])
                if not d or len(d) < len(_SCANNER_COLUMNS):
                    continue

                vals = dict(zip(_SCANNER_COLUMNS, d))
                close = vals.get("close")
                if close is None or close == 0:
                    continue

                change_pct = vals.get("change") or 0
                change_abs = vals.get("change_abs") or 0
                volume = int(vals.get("volume") or 0)
                value = float(vals.get("Value.Traded") or close * volume)

                out.append({
                    "ticker": ticker,
                    "name": vals.get("description") or ticker,
                    "close": round(float(close), 0),
                    "change": round(float(change_abs), 0),
                    "change_pct": round(float(change_pct), 2),
                    "volume": volume,
                    "value": value,
                    "frequency": max(1, int(volume / 1000)),
                    "open": vals.get("open"),
                    "high": vals.get("high"),
                    "low": vals.get("low"),
                    # Indicator bonus dari TV (gratis, sudah dihitung server-side)
                    "rsi": vals.get("RSI"),
                    "macd": vals.get("MACD.macd"),
                    "macd_signal": vals.get("MACD.signal"),
                    "sma20": vals.get("SMA20"),
                    "sma50": vals.get("SMA50"),
                    "sma200": vals.get("SMA200"),
                    "ema20": vals.get("EMA20"),
                    "bb_upper": vals.get("BB.upper"),
                    "bb_lower": vals.get("BB.lower"),
                    "atr": vals.get("ATR"),
                    "adx": vals.get("ADX"),
                    "stoch_k": vals.get("Stoch.K"),
                    "market_cap": vals.get("market_cap_basic"),
                    "pe": vals.get("price_earnings_ttm"),
                    "pbv": vals.get("price_book_ratio"),
                    "eps": vals.get("earnings_per_share_basic_ttm"),
                    "div_yield": vals.get("dividend_yield_recent"),
                    "tv_rating": vals.get("Recommend.All"),
                    "volatility_d": vals.get("Volatility.D"),
                    "avg_vol_30d": vals.get("average_volume_30d_calc"),
                    "rel_vol_10d": vals.get("relative_volume_10d_calc"),
                    "source": "tv_scanner",
                    "timestamp": now_iso,
                })
            except Exception as e:
                logger.debug(f"TV row parse failed for {row}: {e}")
                continue

        logger.info(f"[TV-SCANNER] Fetched {len(out)}/{len(tickers)} in {elapsed:.2f}s")
        return out

    except requests.HTTPError as e:
        logger.error(f"[TV-SCANNER] HTTP error: {e} - body: {getattr(e.response, 'text', '')[:200]}")
        return []
    except Exception as e:
        logger.error(f"[TV-SCANNER] Failed: {e}")
        return []


# ============================================================
# LAYER 2: tvdatafeed (WebSocket realtime, butuh login TV)
# ============================================================

_tv_client = None
_tv_lock = threading.Lock()
_tv_disabled = False  # set True kalau login gagal supaya gak retry terus


def _get_tv_client():
    """Lazy singleton untuk TvDatafeed. Return None kalau gak available."""
    global _tv_client, _tv_disabled
    if _tv_disabled:
        return None
    if _tv_client is not None:
        return _tv_client

    with _tv_lock:
        if _tv_client is not None:
            return _tv_client
        try:
            from tvDatafeed import TvDatafeed  # pip install tvdatafeed
        except ImportError:
            logger.warning("[TVDATAFEED] tvdatafeed not installed - layer 2 disabled. "
                           "Install: pip install --upgrade --no-cache-dir "
                           "git+https://github.com/rongardF/tvdatafeed.git")
            _tv_disabled = True
            return None

        username = os.environ.get("TV_USERNAME")
        password = os.environ.get("TV_PASSWORD")
        try:
            if username and password:
                _tv_client = TvDatafeed(username=username, password=password)
                logger.info("[TVDATAFEED] Logged in as %s", username)
            else:
                # anonymous mode - tetap jalan tapi data terbatas
                _tv_client = TvDatafeed()
                logger.info("[TVDATAFEED] Anonymous mode (no TV_USERNAME/TV_PASSWORD set)")
        except Exception as e:
            logger.error(f"[TVDATAFEED] Login failed: {e} - disabling layer 2")
            _tv_disabled = True
            return None

    return _tv_client


def tv_datafeed_fetch(ticker: str,
                      interval: str = "1m",
                      n_bars: int = 60) -> Optional[List[Dict]]:
    """
    Fetch realtime OHLCV via WebSocket TradingView untuk 1 ticker.
    Return list of bars [{time, open, high, low, close, volume}, ...] atau None.
    """
    client = _get_tv_client()
    if client is None:
        return None

    try:
        from tvDatafeed import Interval
        interval_map = {
            "1m": Interval.in_1_minute,
            "5m": Interval.in_5_minute,
            "15m": Interval.in_15_minute,
            "1h": Interval.in_1_hour,
            "1d": Interval.in_daily,
        }
        iv = interval_map.get(interval, Interval.in_1_minute)
        df = client.get_hist(symbol=ticker, exchange="IDX",
                             interval=iv, n_bars=n_bars)
        if df is None or df.empty:
            return None

        out = []
        for ts, row in df.iterrows():
            out.append({
                "time": str(ts),
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": int(row["volume"]),
            })
        return out
    except Exception as e:
        logger.debug(f"[TVDATAFEED] {ticker} fetch failed: {e}")
        return None


def tv_datafeed_watchlist(tickers: List[str]) -> List[Dict]:
    """
    Realtime snapshot untuk watchlist via tvdatafeed (per-ticker).
    Lebih lambat dari scanner (sequential WebSocket call) tapi tick-level fresh.
    """
    if not tickers:
        return []
    client = _get_tv_client()
    if client is None:
        return []

    out = []
    now_iso = datetime.now().isoformat()
    for t in tickers:
        bars = tv_datafeed_fetch(t, interval="1m", n_bars=2)
        if not bars:
            continue
        last = bars[-1]
        first = bars[0]
        close = last["close"]
        open_ = first["open"]
        change = close - open_
        change_pct = (change / open_ * 100) if open_ else 0
        out.append({
            "ticker": t,
            "close": round(close, 0),
            "change": round(change, 0),
            "change_pct": round(change_pct, 2),
            "volume": last["volume"],
            "value": close * last["volume"],
            "last_bar_time": last["time"],
            "source": "tv_datafeed_ws",
            "timestamp": now_iso,
        })
    return out
