# pip install yfinance pandas numpy requests beautifulsoup4
"""
IDX Stock Scanner — All-in-one (v2.1)
Pantau saham BEI realtime, cari yang undervalue + liquid + momentum kuat,
kirim sinyal ke Telegram Bot setiap 30 menit saat jam bursa.

SETUP:
  1. pip install yfinance pandas numpy requests beautifulsoup4
  2. Isi bagian CONFIG di bawah (Telegram token, chat_id)
  3. python idx_scanner.py

PERINTAH BOT:
  /scan           — scan manual
  /detail BBCA    — sinyal lengkap satu saham (dengan DB fallback)
  /makro          — data makro terkini
  /regime         — market regime IHSG
  /sectors        — sektor momentum ranking
  /history [KODE] — riwayat sinyal
  /winrate        — performa 30 hari
  /top            — top performing tickers
  /backtest BBCA 30 — mini backtest dari data historis
  /holiday        — libur BEI berikutnya
  /status         — scanner health stats
  /setportfolio   — set ukuran portofolio (/setportfolio 25000000)
  /help           — daftar perintah
"""

# ══════════════════════════════════════════════════════════════════════════════
#  IMPORTS
# ══════════════════════════════════════════════════════════════════════════════

import logging
import sqlite3
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Optional

import numpy as np
import pandas as pd
import requests
import yfinance as yf
from bs4 import BeautifulSoup

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIG — WAJIB DIISI
# ══════════════════════════════════════════════════════════════════════════════

TELEGRAM_BOT_TOKEN = "TOKEN_JOB"
TELEGRAM_CHAT_ID   = "6052270268"

FRED_API_KEY = ""

SCAN_INTERVAL_MINUTES = 30
MARKET_OPEN_HOUR,  MARKET_OPEN_MINUTE  = 9,  0
MARKET_CLOSE_HOUR, MARKET_CLOSE_MINUTE = 15, 50

BI_RATE = 6.25  # fallback jika scraping gagal

CONFIG = {
    "ENABLE_MARKET_REGIME":        True,
    "ENABLE_SECTOR_FILTER":        True,
    "ENABLE_CANDLESTICK_PATTERNS": True,
    "ENABLE_FOREIGN_FLOW":         True,
    "ENABLE_UNIVERSE_VALIDATION":  True,
    "SECTOR_MOMENTUM_THRESHOLD":   0.5,
    "DB_PATH":                     "scanner_history.db",
    "BI_RATE_CACHE_HOURS":         24,
    "MAX_WORKERS":                 16,
    "PORTFOLIO_VALUE":             10_000_000,   # Rp 10 juta default
    "HEARTBEAT_INTERVAL_MIN":      60,           # kirim heartbeat tiap 60 menit
}

FILTER = {
    "max_pb"             : 3.0,
    "max_pe"             : 25.0,
    "min_roe"            : 8.0,
    "min_avg_volume_m"   : 50,
    "min_avg_turnover_b" : 20,
    "min_signal_score"   : 4,
    "min_atr_pct"        : 2.0,
    "max_atr_pct"        : 12.0,
}

RISK = {
    "risk_reward_ratio"  : 2.0,
    "stop_loss_atr_mult" : 1.5,
    "target_atr_mult"    : 3.0,
}

MAX_RAW_SCORE = 14.5

# ══════════════════════════════════════════════════════════════════════════════
#  SECTOR MAP
# ══════════════════════════════════════════════════════════════════════════════

SECTOR_MAP = {
    "Banking":    ["BBCA.JK","BBRI.JK","BMRI.JK","BBNI.JK","BNGA.JK","BTPS.JK","ARTO.JK","BRIS.JK","NISP.JK","BDMN.JK","PNBN.JK","MEGA.JK","BJTM.JK"],
    "Commodity":  ["ADRO.JK","PTBA.JK","ITMG.JK","INCO.JK","ANTM.JK","MDKA.JK","BOSS.JK","HRUM.JK","BYAN.JK","ELSA.JK","DOID.JK","MYOH.JK"],
    "Consumer":   ["UNVR.JK","ICBP.JK","INDF.JK","MYOR.JK","CLEO.JK","DLTA.JK","ACES.JK","ERAA.JK","AMRT.JK","LPPF.JK","MAPI.JK"],
    "Telco":      ["TLKM.JK","EXCL.JK","ISAT.JK","DMMX.JK","EMTK.JK","GOTO.JK","BUKA.JK"],
    "Property":   ["BSDE.JK","SMRA.JK","CTRA.JK","PWON.JK","JRPT.JK","DILD.JK","LPKR.JK","ASRI.JK","MTLA.JK"],
    "Healthcare": ["KLBF.JK","SIDO.JK","MIKA.JK","HEAL.JK","SOHO.JK","TSPC.JK","MERK.JK"],
    "Industrial": ["ASII.JK","SMGR.JK","INTP.JK","WIKA.JK","SMSM.JK","WTON.JK","PTPP.JK","WSKT.JK"],
    "Finance":    ["PNLF.JK","MFIN.JK","BFIN.JK"],
    "Agri":       ["AALI.JK","SIMP.JK","LSIP.JK","SSMS.JK","SGRO.JK","TBLA.JK"],
    "Others":     ["JSMR.JK","TOWR.JK","PGAS.JK","AKRA.JK","MNCN.JK","SCMA.JK"],
}

# ══════════════════════════════════════════════════════════════════════════════
#  BEI HOLIDAYS 2026
# ══════════════════════════════════════════════════════════════════════════════

BEI_HOLIDAYS_2026 = [
    "2026-01-01","2026-01-27","2026-01-28","2026-03-28",
    "2026-03-30","2026-04-01","2026-05-01","2026-05-14",
    "2026-05-24","2026-06-01","2026-08-17","2026-09-12",
    "2026-12-25","2026-12-26",
]

BEI_HOLIDAY_NAMES = {
    "2026-01-01":"Tahun Baru","2026-01-27":"Isra Miraj","2026-01-28":"Cuti Bersama Imlek",
    "2026-03-28":"Hari Raya Nyepi","2026-03-30":"Wafat Isa Al-Masih","2026-04-01":"Cuti Bersama Lebaran",
    "2026-05-01":"Hari Buruh","2026-05-14":"Kenaikan Isa Al-Masih","2026-05-24":"Hari Raya Waisak",
    "2026-06-01":"Hari Pancasila","2026-08-17":"HUT RI","2026-09-12":"Maulid Nabi",
    "2026-12-25":"Natal","2026-12-26":"Cuti Bersama Natal",
}

# ══════════════════════════════════════════════════════════════════════════════
#  IDX UNIVERSE
# ══════════════════════════════════════════════════════════════════════════════

IDX_UNIVERSE = list(dict.fromkeys([
    "BBCA.JK","BBRI.JK","BMRI.JK","BBNI.JK","BRIS.JK",
    "TLKM.JK","ASII.JK","UNVR.JK","ICBP.JK","INDF.JK",
    "KLBF.JK","SIDO.JK","CPIN.JK","JPFA.JK","MYOR.JK",
    "PTBA.JK","ADRO.JK","ITMG.JK","INCO.JK","ANTM.JK",
    "MDKA.JK","TINS.JK","MEDC.JK","PGAS.JK","AKRA.JK",
    "SMGR.JK","INTP.JK","GOTO.JK","BUKA.JK","EMTK.JK",
    "MTEL.JK","HEAL.JK","MIKA.JK","BSDE.JK","CTRA.JK",
    "PWON.JK","SMRA.JK","EXCL.JK","ISAT.JK","UNTR.JK",
    "BRPT.JK","TPIA.JK","ACES.JK","MAPI.JK","ERAA.JK",
    "HRUM.JK","BSSR.JK","GEMS.JK","MBMA.JK","PGEO.JK",
    "NISP.JK","BDMN.JK","PNBN.JK","MEGA.JK","BJTM.JK",
    "BOSS.JK","BYAN.JK","ELSA.JK","DOID.JK","MYOH.JK",
    "AMRT.JK","LPPF.JK","DMMX.JK",
    "JRPT.JK","DILD.JK","LPKR.JK","ASRI.JK","MTLA.JK",
    "SOHO.JK","TSPC.JK","MERK.JK",
    "SMSM.JK","WTON.JK","PTPP.JK","WSKT.JK",
    "LSIP.JK","SSMS.JK","SGRO.JK","TBLA.JK",
    "JSMR.JK","TOWR.JK","MNCN.JK","SCMA.JK",
]))

# ══════════════════════════════════════════════════════════════════════════════
#  LOGGING & TIMEZONE
# ══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("scanner.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("idx_scanner")
WIB = timezone(timedelta(hours=7))

# ══════════════════════════════════════════════════════════════════════════════
#  MODULE-LEVEL STATE & CACHES
# ══════════════════════════════════════════════════════════════════════════════

_last_signals           : dict = {}   # ticker → StockSignal (signals from last scan that passed)
_last_scan_signals      : dict = {}   # ticker → {"signal": StockSignal, "ts": str} — persists across scans
_last_scan_timestamp    : str  = ""
_last_macro             : dict = {}
_last_ihsg              : dict = {}
_update_offset          : int  = 0

_regime_cache   : dict = {"regime": None, "timestamp": None, "ema20": None, "ema50": None, "ihsg": None}
_sector_cache   : dict = {"data": None, "timestamp": None}
_foreign_cache  : dict = {"data": None, "timestamp": None}
_bi_rate_cache  : dict = {"rate": None, "timestamp": None}

_active_universe        : list = []
_universe_last_validated       = None
_ticker_fail_counts     : dict = {}

_db_lock        = threading.Lock()
_db_enabled     = True
_outcomes_last_run      = None

_portfolio_value_session: float = CONFIG["PORTFOLIO_VALUE"]  # updated via /setportfolio

_scanner_stats: dict = {
    "total_scans"           : 0,
    "successful_scans"      : 0,
    "last_scan_time"        : None,
    "last_scan_duration_s"  : 0,
    "last_signal_count"     : 0,
    "total_signals_today"   : 0,
    "signals_today_date"    : None,
    "fetch_errors_last_scan": 0,
    "fallback_tickers_last" : 0,
    "scanner_start_time"    : None,
}

# ══════════════════════════════════════════════════════════════════════════════
#  DATA FETCHERS
# ══════════════════════════════════════════════════════════════════════════════

def _yahoo_ohlcv_fallback(ticker: str, period: str = "3mo") -> Optional[pd.DataFrame]:
    """Scrape OHLCV from Yahoo Finance v8 JSON API as yfinance fallback."""
    try:
        range_map = {"3mo": "3mo", "2d": "5d", "5d": "5d", "15d": "1mo", "60d": "3mo"}
        yrange = range_map.get(period, "3mo")
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range={yrange}&interval=1d"
        r = requests.get(url, timeout=15,
                         headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
        data = r.json()
        result = data.get("chart", {}).get("result", [])
        if not result:
            return None
        ts    = result[0]["timestamp"]
        q     = result[0]["indicators"]["quote"][0]
        adjc  = result[0].get("indicators", {}).get("adjclose", [{}])
        close = adjc[0].get("adjclose", q["close"]) if adjc else q["close"]
        df = pd.DataFrame({
            "Open"  : q["open"],
            "High"  : q["high"],
            "Low"   : q["low"],
            "Close" : close,
            "Volume": q["volume"],
        }, index=pd.to_datetime(ts, unit="s", utc=True).tz_convert("Asia/Jakarta"))
        df.index.name = "Date"
        df.dropna(inplace=True)
        if len(df) < 5:
            return None
        logger.debug(f"[{ticker}] Yahoo scrape fallback: {len(df)} rows")
        return df
    except Exception as e:
        logger.debug(f"[{ticker}] Yahoo scrape fallback failed: {e}")
        return None


def fetch_ohlcv(ticker: str, period: str = "3mo") -> tuple:
    """Returns (df, source) where source is 'yfinance' or 'yahoo_scrape'."""
    try:
        df = yf.download(ticker, period=period, interval="1d",
                         progress=False, auto_adjust=True)
        if df is not None and not df.empty and len(df) >= 20:
            _ticker_fail_counts[ticker] = 0
            df.dropna(inplace=True)
            return df, "yfinance"
    except Exception as e:
        logger.debug(f"[{ticker}] yfinance OHLCV error: {e}")

    # Fallback
    df = _yahoo_ohlcv_fallback(ticker, period)
    if df is not None and len(df) >= 20:
        _ticker_fail_counts[ticker] = 0
        return df, "yahoo_scrape"

    _ticker_fail_counts[ticker] = _ticker_fail_counts.get(ticker, 0) + 1
    if _ticker_fail_counts.get(ticker, 0) >= 3 and ticker in _active_universe:
        _active_universe.remove(ticker)
        logger.warning(f"Auto-removed {ticker} from active universe (3 consecutive failures)")
    return None, None


def _stockbit_fundamental_fallback(ticker: str) -> dict:
    """Scrape PBV/PER/ROE from Stockbit public page."""
    result = {}
    try:
        code = ticker.replace(".JK", "")
        url  = f"https://stockbit.com/symbol/{code}"
        r    = requests.get(url, timeout=15,
                            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
        soup = BeautifulSoup(r.text, "html.parser")
        text = soup.get_text()
        import re
        pbv = re.search(r"PBV\s*[:\s]+([0-9]+\.?[0-9]*)", text)
        per = re.search(r"PER\s*[:\s]+([0-9]+\.?[0-9]*)", text)
        roe = re.search(r"ROE\s*[:\s]+([0-9]+\.?[0-9]*)", text)
        if pbv: result["pb"]  = float(pbv.group(1))
        if per: result["pe"]  = float(per.group(1))
        if roe: result["roe"] = float(roe.group(1))
    except Exception as e:
        logger.debug(f"[{ticker}] Stockbit fallback failed: {e}")
    return result


def fetch_fundamental(ticker: str) -> dict:
    result = {"pb": None, "pe": None, "roe": None,
              "market_cap_b": None, "company_name": ticker, "sector": "Unknown",
              "fund_source": "yfinance"}
    try:
        info = yf.Ticker(ticker).info
        result["pb"]           = info.get("priceToBook")
        result["pe"]           = info.get("trailingPE") or info.get("forwardPE")
        roe                    = info.get("returnOnEquity")
        result["roe"]          = round(float(roe) * 100, 2) if roe else None
        mc                     = info.get("marketCap")
        result["market_cap_b"] = round(mc / 1e9, 1) if mc else None
        result["company_name"] = info.get("longName") or info.get("shortName") or ticker
        result["sector"]       = info.get("sector") or "Unknown"
    except Exception:
        pass

    # Stockbit fallback if key fields missing
    if result["pb"] is None and result["pe"] is None and result["roe"] is None:
        fb = _stockbit_fundamental_fallback(ticker)
        if fb:
            result.update(fb)
            result["fund_source"] = "stockbit_fallback"
    return result


def fetch_bi_rate_live() -> float:
    now = datetime.now(WIB)
    cache_hours = CONFIG.get("BI_RATE_CACHE_HOURS", 24)
    if (_bi_rate_cache["rate"] is not None and _bi_rate_cache["timestamp"] is not None and
            (now - _bi_rate_cache["timestamp"]).total_seconds() < cache_hours * 3600):
        return _bi_rate_cache["rate"]
    try:
        url = "https://www.bi.go.id/id/statistik/indikator/data-bi-rate.aspx"
        r = requests.get(url, timeout=15,
                         headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})
        soup = BeautifulSoup(r.text, "html.parser")
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                for cell in row.find_all(["td", "th"]):
                    text = cell.get_text(strip=True)
                    if "%" in text:
                        try:
                            rate = float(text.replace("%", "").replace(",", ".").strip())
                            if 0 < rate < 20:
                                _bi_rate_cache["rate"] = rate
                                _bi_rate_cache["timestamp"] = now
                                return rate
                        except ValueError:
                            continue
    except Exception as e:
        logger.debug(f"BI Rate scraping failed: {e}")
    return BI_RATE


def fetch_foreign_flow() -> Optional[dict]:
    now = datetime.now(WIB)
    if (_foreign_cache["data"] is not None and _foreign_cache["timestamp"] is not None and
            (now - _foreign_cache["timestamp"]).total_seconds() < 1800):
        return _foreign_cache["data"]
    try:
        url = "https://www.idx.co.id/id/data-pasar/ringkasan-perdagangan/ringkasan-saham/"
        r   = requests.get(url, timeout=15,
                           headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})
        soup    = BeautifulSoup(r.text, "html.parser")
        net_val = None
        for table in soup.find_all("table"):
            if not any(k in table.get_text().lower() for k in ("foreign","asing","net")):
                continue
            for row in table.find_all("tr"):
                cells    = row.find_all(["td","th"])
                row_text = " ".join(c.get_text(strip=True) for c in cells).lower()
                if "net" in row_text and any(k in row_text for k in ("buy","sell","asing")):
                    for cell in cells:
                        try:
                            val = float(cell.get_text(strip=True).replace(",","").replace(".",""))
                            if abs(val) > 1_000_000:
                                net_val = val / 1e9
                                break
                        except ValueError:
                            continue
                if net_val is not None:
                    break
            if net_val is not None:
                break
        if net_val is None:
            _foreign_cache.update({"data": None, "timestamp": now})
            return None
        is_pos = net_val >= 0
        label  = f"Net Buy +Rp {abs(net_val):.0f}B" if is_pos else f"Net Sell -Rp {abs(net_val):.0f}B"
        res    = {"net_foreign_b": net_val, "label": label, "is_positive": is_pos}
        _foreign_cache.update({"data": res, "timestamp": now})
        return res
    except Exception as e:
        logger.debug(f"Foreign flow scraping failed: {e}")
        _foreign_cache.update({"data": None, "timestamp": now})
        return None


def fetch_macro() -> dict:
    bi_rate = fetch_bi_rate_live()
    m = {"usdidr": None, "dxy": None, "brent_oil": None, "gold": None,
         "us10y_yield": None, "bi_rate": bi_rate, "macro_sentiment": "NEUTRAL",
         "foreign_flow": None}

    def _yf_last(sym):
        try:
            df = yf.download(sym, period="5d", interval="1d", progress=False, auto_adjust=True)
            return float(df["Close"].iloc[-1]) if not df.empty else None
        except Exception:
            return None

    m["usdidr"]    = _yf_last("USDIDR=X")
    m["dxy"]       = _yf_last("DX-Y.NYB")
    m["brent_oil"] = _yf_last("BZ=F")
    m["gold"]      = _yf_last("GC=F")

    if FRED_API_KEY:
        try:
            url  = (f"https://api.stlouisfed.org/fred/series/observations"
                    f"?series_id=DGS10&api_key={FRED_API_KEY}&sort_order=desc&limit=1&file_type=json")
            data = requests.get(url, timeout=10).json()
            val  = data["observations"][0]["value"]
            m["us10y_yield"] = float(val) if val != "." else None
        except Exception:
            pass
    if m["us10y_yield"] is None:
        m["us10y_yield"] = _yf_last("^TNX")

    if CONFIG.get("ENABLE_FOREIGN_FLOW"):
        m["foreign_flow"] = fetch_foreign_flow()

    score = 0
    if m["usdidr"]:
        score += 1 if m["usdidr"] < 15800 else (-1 if m["usdidr"] > 16200 else 0)
    if m["dxy"]:
        score += 1 if m["dxy"] < 103 else (-1 if m["dxy"] > 106 else 0)
    if m["us10y_yield"]:
        score += 1 if m["us10y_yield"] < 4.0 else (-1 if m["us10y_yield"] > 4.5 else 0)
    if m["bi_rate"] and m["bi_rate"] <= 6.0:
        score += 1
    ff = m.get("foreign_flow")
    if ff is not None:
        score += 1 if ff["net_foreign_b"] > 500 else (-1 if ff["net_foreign_b"] < -500 else 0)

    m["macro_sentiment"] = "POSITIVE" if score >= 2 else "NEGATIVE" if score <= -2 else "NEUTRAL"
    return m


def fetch_ihsg() -> dict:
    result = {"ihsg": None, "ihsg_chg_pct": None}
    try:
        df = yf.download("^JKSE", period="5d", interval="1d", progress=False, auto_adjust=True)
        if not df.empty and len(df) >= 2:
            c = df["Close"]
            result["ihsg"]         = float(c.iloc[-1])
            result["ihsg_chg_pct"] = round((float(c.iloc[-1]) / float(c.iloc[-2]) - 1) * 100, 2)
    except Exception:
        pass
    return result


# ══════════════════════════════════════════════════════════════════════════════
#  MARKET REGIME DETECTION
# ══════════════════════════════════════════════════════════════════════════════

def detect_market_regime() -> str:
    global _regime_cache
    now = datetime.now(WIB)
    if (_regime_cache["regime"] is not None and _regime_cache["timestamp"] is not None and
            (now - _regime_cache["timestamp"]).total_seconds() < 1800):
        return _regime_cache["regime"]
    try:
        df = yf.download("^JKSE", period="60d", interval="1d", progress=False, auto_adjust=True)
        if df is None or df.empty or len(df) < 50:
            return "NEUTRAL"
        c   = df["Close"].squeeze()
        e20 = float(c.ewm(span=20, adjust=False).mean().iloc[-1])
        e50 = float(c.ewm(span=50, adjust=False).mean().iloc[-1])
        price = float(c.iloc[-1])
        regime = "BULL" if (e20 > e50 and price > e20) else "BEAR" if e20 < e50 else "CHOP"
        _regime_cache = {"regime": regime, "timestamp": now,
                         "ema20": round(e20, 0), "ema50": round(e50, 0), "ihsg": round(price, 0)}
        logger.info(f"Market regime: {regime} | EMA20={e20:.0f} EMA50={e50:.0f} IHSG={price:.0f}")
        return regime
    except Exception as e:
        logger.warning(f"detect_market_regime error: {e}")
        return "NEUTRAL"


# ══════════════════════════════════════════════════════════════════════════════
#  SECTOR MOMENTUM
# ══════════════════════════════════════════════════════════════════════════════

def calculate_sector_momentum() -> dict:
    global _sector_cache
    now = datetime.now(WIB)
    if (_sector_cache["data"] is not None and _sector_cache["timestamp"] is not None and
            (now - _sector_cache["timestamp"]).total_seconds() < 3600):
        return _sector_cache["data"]
    result = {}
    try:
        ihsg_df  = yf.download("^JKSE", period="15d", interval="1d", progress=False, auto_adjust=True)
        ihsg_ret = 0.0
        if not ihsg_df.empty and len(ihsg_df) >= 10:
            c = ihsg_df["Close"].squeeze()
            ihsg_ret = (float(c.iloc[-1]) / float(c.iloc[-10]) - 1) * 100
    except Exception:
        ihsg_ret = 0.0

    def _sector_return(sector_name, tickers):
        rets = []
        for t in tickers:
            try:
                df = yf.download(t, period="15d", interval="1d", progress=False, auto_adjust=True)
                if df is not None and not df.empty and len(df) >= 10:
                    c = df["Close"].squeeze()
                    rets.append((float(c.iloc[-1]) / float(c.iloc[-10]) - 1) * 100)
            except Exception:
                pass
        return sector_name, (sum(rets) / len(rets) - ihsg_ret) if rets else 0.0

    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(_sector_return, s, t): s for s, t in SECTOR_MAP.items()}
        for f in as_completed(futs):
            try:
                sn, rel = f.result(timeout=30)
                result[sn] = round(rel, 2)
            except Exception:
                result[futs[f]] = 0.0

    _sector_cache = {"data": result, "timestamp": now}
    return result


def get_ticker_sector(ticker: str) -> str:
    for sector, tickers in SECTOR_MAP.items():
        if ticker in tickers:
            return sector
    return "Unknown"


# ══════════════════════════════════════════════════════════════════════════════
#  UNIVERSE VALIDATION
# ══════════════════════════════════════════════════════════════════════════════

def validate_universe(tickers: list) -> list:
    valid = []

    def _check(t):
        try:
            df = yf.download(t, period="5d", progress=False, auto_adjust=True)
            return t, not (df is None or df.empty)
        except Exception:
            return t, False

    with ThreadPoolExecutor(max_workers=16) as ex:
        futs = {ex.submit(_check, t): t for t in tickers}
        for f in as_completed(futs, timeout=60):
            try:
                t, ok = f.result(timeout=20)
                if ok:
                    valid.append(t)
            except Exception:
                pass

    removed = [t for t in tickers if t not in valid]
    logger.info(f"Universe validated: {len(valid)}/{len(tickers)} active. "
                f"Removed: {', '.join(removed) if removed else 'none'}")
    return valid


# ══════════════════════════════════════════════════════════════════════════════
#  TECHNICAL INDICATORS
# ══════════════════════════════════════════════════════════════════════════════

def _ema(s, n): return s.ewm(span=n, adjust=False).mean()
def _sma(s, n): return s.rolling(n).mean()

def calc_rsi(c, n=14):
    d  = c.diff(); g = d.clip(lower=0); l = -d.clip(upper=0)
    ag = g.ewm(com=n-1, min_periods=n).mean()
    al = l.ewm(com=n-1, min_periods=n).mean()
    return 100 - 100 / (1 + ag / al.replace(0, np.nan))

def calc_macd(c, fast=12, slow=26, sig=9):
    m = _ema(c, fast) - _ema(c, slow); s = _ema(m, sig)
    return m, s, m - s

def calc_bb(c, n=20, k=2.0):
    mid = _sma(c, n); std = c.rolling(n).std()
    up = mid + k*std; lo = mid - k*std
    return up, mid, lo, (c - lo) / (up - lo + 1e-10)

def calc_stoch(h, l, c, kp=14, dp=3):
    lo = l.rolling(kp).min(); hi = h.rolling(kp).max()
    k  = 100 * (c - lo) / (hi - lo + 1e-10)
    return k, _sma(k, dp)

def calc_atr(h, l, c, n=14):
    tr = pd.concat([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
    return tr.ewm(com=n-1, min_periods=n).mean()

def calc_adx(h, l, c, n=14):
    atr = calc_atr(h, l, c, n)
    up  = h.diff(); dn = -l.diff()
    pdm = pd.Series(np.where((up>dn)&(up>0), up, 0.0), index=c.index).ewm(com=n-1, min_periods=n).mean()
    mdm = pd.Series(np.where((dn>up)&(dn>0), dn, 0.0), index=c.index).ewm(com=n-1, min_periods=n).mean()
    pdi = 100 * pdm / atr.replace(0, np.nan)
    mdi = 100 * mdm / atr.replace(0, np.nan)
    dx  = 100 * (pdi-mdi).abs() / (pdi+mdi+1e-10)
    return dx.ewm(com=n-1, min_periods=n).mean(), pdi, mdi

def calc_obv(c, v):
    return (np.sign(c.diff().fillna(0)) * v).cumsum()

def calc_vwap(h, l, c, v, n=20):
    tp = (h+l+c) / 3
    return (tp*v).rolling(n).sum() / v.rolling(n).sum().replace(0, np.nan)

def calc_roc(c, n=10):
    return ((c - c.shift(n)) / c.shift(n).replace(0, np.nan)) * 100

def calc_williams_r(h, l, c, n=14):
    return -100 * (h.rolling(n).max()-c) / (h.rolling(n).max()-l.rolling(n).min()+1e-10)


def technical_score(df: pd.DataFrame) -> dict:
    c = df["Close"].squeeze(); h = df["High"].squeeze()
    l = df["Low"].squeeze();   v = df["Volume"].squeeze()
    price = float(c.iloc[-1])
    raw_score = 0.0; details = {}

    rsi = calc_rsi(c); r = float(rsi.iloc[-1]); details["RSI"] = round(r, 1)
    if 25 <= r <= 45:  raw_score += 2.0
    elif r > 60:       raw_score -= 1.5

    macd, msig, hist = calc_macd(c)
    mh = float(hist.iloc[-1]); mh_prev = float(hist.iloc[-2]) if len(hist) > 1 else 0
    details["MACD_hist"] = round(mh, 4)
    if mh > 0 and mh_prev <= 0: raw_score += 3.0
    elif mh > 0:                 raw_score += 1.5

    _, _, _, pct_b = calc_bb(c); pb = float(pct_b.iloc[-1]); details["BB_%B"] = round(pb, 2)
    if pb < 0.2: raw_score += 1.0

    sk_s, sd_s = calc_stoch(h, l, c)
    sk = float(sk_s.iloc[-1]); sd = float(sd_s.iloc[-1])
    sk_p = float(sk_s.iloc[-2]) if len(sk_s) > 1 else sk
    sd_p = float(sd_s.iloc[-2]) if len(sd_s) > 1 else sd
    details["Stoch_K"] = round(sk, 1)
    if sk < 30 and sk > sd and sk_p <= sd_p: raw_score += 1.0

    adx_s, pdi, mdi = calc_adx(h, l, c)
    adx_v = float(adx_s.iloc[-1]); details["ADX"] = round(adx_v, 1)
    if adx_v > 25 and float(pdi.iloc[-1]) > float(mdi.iloc[-1]): raw_score += 2.0

    obv    = calc_obv(c, v)
    obv_up = float(obv.rolling(5).mean().iloc[-1]) > float(obv.rolling(20).mean().iloc[-1])
    details["OBV"] = "UP" if obv_up else "DOWN"
    if obv_up: raw_score += 1.5

    avg20 = float(v.rolling(20).mean().iloc[-1])
    v_rat = float(v.iloc[-1]) / avg20 if avg20 else 0
    details["Vol_ratio"] = round(v_rat, 2)
    if v_rat > 1.5: raw_score += 2.0

    vwap  = calc_vwap(h, l, c, v); vwap_v = float(vwap.iloc[-1])
    details["VWAP_diff%"] = round((price / vwap_v - 1) * 100, 2) if vwap_v else 0
    if price > vwap_v: raw_score += 1.0

    roc_v = float(calc_roc(c).iloc[-1]); details["ROC_10d"] = round(roc_v, 2)
    if roc_v > 0: raw_score += 0.5

    wr_v = float(calc_williams_r(h, l, c).iloc[-1]); details["Williams_R"] = round(wr_v, 1)
    if -80 <= wr_v <= -50: raw_score += 0.5

    raw_score  = max(0.0, raw_score)
    normalized = round((raw_score / MAX_RAW_SCORE) * 10, 2)
    atr_v      = float(calc_atr(h, l, c).iloc[-1])
    atr_pct    = round((atr_v / price) * 100, 2) if price > 0 else 0
    sma20      = float(_sma(c, 20).iloc[-1])

    return {
        "raw_score": round(raw_score, 2), "score": normalized,
        "details": details, "price": round(price, 0),
        "atr": round(atr_v, 0), "atr_pct": atr_pct,
        "support": round(float(l.rolling(20).min().iloc[-1]), 0),
        "resistance": round(float(h.rolling(20).max().iloc[-1]), 0),
        "vwap": round(vwap_v, 0), "rsi": round(r, 1),
        "macd_hist": round(mh, 4), "stoch_k": round(sk, 1),
        "adx": round(adx_v, 1), "vol_ratio": round(v_rat, 2),
        "obv_up": obv_up, "pct_b": round(pb, 2),
        "avg_vol_m": round(avg20 / 1e6, 0), "sma20": round(sma20, 0),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  CANDLESTICK PATTERN DETECTION
# ══════════════════════════════════════════════════════════════════════════════

def detect_candlestick_patterns(df: pd.DataFrame) -> tuple:
    try:
        if len(df) < 21:
            return (0.0, "")
        c1 = df.iloc[-3]; c2 = df.iloc[-2]; c3 = df.iloc[-1]

        def _get(row, col):
            v = row[col]
            return float(v.iloc[0]) if hasattr(v, "iloc") else float(v)

        o1,h1,l1,cl1 = _get(c1,"Open"),_get(c1,"High"),_get(c1,"Low"),_get(c1,"Close")
        o2,h2,l2,cl2 = _get(c2,"Open"),_get(c2,"High"),_get(c2,"Low"),_get(c2,"Close")
        o3,h3,l3,cl3 = _get(c3,"Open"),_get(c3,"High"),_get(c3,"Low"),_get(c3,"Close")
        price  = cl3
        sma20  = float(df["Close"].squeeze().rolling(20).mean().iloc[-1])
        body1  = abs(cl1-o1); body2 = abs(cl2-o2); body3 = abs(cl3-o3)
        mid1   = (o1+cl1)/2

        if cl1<o1 and body1>0.01*price and (body2<0.3*body1 if body1>0 else False) and cl3>o3 and cl3>mid1:
            return (2.0, "Morning Star ☀️")
        if cl2<o2 and cl3>o3 and cl3>o2 and o3<cl2:
            return (1.5, "Bullish Engulfing 🕯️")
        lw = min(cl3,o3)-l3; uw = h3-max(cl3,o3)
        if body3>0 and lw>=2*body3 and uw<=0.5*body3 and cl3>=o3 and price<sma20:
            return (1.0, "Hammer 🔨")
        if cl2<o2 and body2>0.01*price and cl3>o3 and o3>cl2 and cl3<o2:
            return (0.8, "Bullish Harami")
        if cl2>o2 and cl3<o3 and cl3<o2 and o3>cl2:
            return (-1.5, "Bearish Engulfing ⚠️")
        uw3 = h3-max(cl3,o3); lw3 = min(cl3,o3)-l3
        if body3>0 and uw3>=2*body3 and lw3<=0.5*body3 and price>sma20:
            return (-1.0, "Shooting Star ⭐")
        cr = h3-l3
        if cr>0 and body3<0.05*cr:
            return (-0.3, "Doji")
        return (0.0, "")
    except Exception as e:
        logger.debug(f"Candlestick pattern error: {e}")
        return (0.0, "")


# ══════════════════════════════════════════════════════════════════════════════
#  STOCK SIGNAL DATACLASS
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class StockSignal:
    ticker: str; company_name: str; sector: str
    signal: str; confidence: str
    price: float; entry_low: float; entry_high: float
    stop_loss: float; target_1: float; target_2: float; risk_reward: float
    tech_score: float; tech_details: dict = field(default_factory=dict)
    pb: Optional[float] = None; pe: Optional[float] = None
    roe: Optional[float] = None; market_cap_b: Optional[float] = None
    atr: float = 0; atr_pct: float = 0; vol_ratio: float = 0
    support: float = 0; resistance: float = 0; vwap: float = 0
    rsi: float = 0; adx: float = 0; avg_vol_m: float = 0; reason: str = ""
    raw_score: float = 0.0; normalized_score: float = 0.0
    regime: str = "NEUTRAL"; sector_momentum: float = 0.0
    pattern: str = ""; macro_sentiment: str = "NEUTRAL"
    data_source: str = "yfinance"   # "yfinance" | "yahoo_scrape"


# ══════════════════════════════════════════════════════════════════════════════
#  POSITION SIZING
# ══════════════════════════════════════════════════════════════════════════════

def calculate_position_size(signal: StockSignal, portfolio_value: float) -> Optional[dict]:
    if signal.signal == "WATCH":
        return None
    risk_pct = 2.0 if signal.confidence == "HIGH" else 1.5
    risk_per_share = signal.price - signal.stop_loss
    if risk_per_share <= 0:
        return None
    max_risk       = portfolio_value * risk_pct / 100
    max_shares     = max_risk / risk_per_share
    position_value = max_shares * signal.price
    position_pct   = (position_value / portfolio_value) * 100
    return {
        "risk_pct"       : risk_pct,
        "max_risk"       : round(max_risk, 0),
        "max_shares"     : round(max_shares, 0),
        "position_value" : round(position_value, 0),
        "position_pct"   : round(position_pct, 1),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  SIGNAL GENERATOR — full 14-step pipeline
# ══════════════════════════════════════════════════════════════════════════════

def generate_signal(ticker: str, df: pd.DataFrame, fund: dict,
                    macro_sent: str, current_regime: str = "NEUTRAL",
                    sector_momentum_data: Optional[dict] = None,
                    foreign_flow: Optional[dict] = None,
                    effective_min_score: int = None,
                    data_source: str = "yfinance") -> Optional[StockSignal]:
    try:
        if df is None or len(df) < 30:
            return None

        tech  = technical_score(df)
        price = tech["price"]; atr = tech["atr"]
        if not (FILTER["min_atr_pct"] <= tech["atr_pct"] <= FILTER["max_atr_pct"]):
            return None

        norm_score = tech["score"]; raw_score = tech["raw_score"]

        pattern_bonus, pattern_name = (0.0, "")
        if CONFIG.get("ENABLE_CANDLESTICK_PATTERNS"):
            pattern_bonus, pattern_name = detect_candlestick_patterns(df)
        norm_score = min(10.0, norm_score + pattern_bonus)

        min_score = effective_min_score if effective_min_score is not None else FILTER["min_signal_score"]
        if norm_score < min_score:
            return None

        pb = fund.get("pb"); pe = fund.get("pe"); roe = fund.get("roe")
        if pb  is not None and pb  > FILTER["max_pb"]:  return None
        if pe  is not None and FILTER["max_pe"] > 0 and pe > FILTER["max_pe"]: return None
        if roe is not None and roe < FILTER["min_roe"]: return None

        avg_vol_m  = tech["avg_vol_m"]
        avg_turn_b = avg_vol_m * 1e6 * price / 1e9
        if avg_vol_m  < FILTER["min_avg_volume_m"]:   return None
        if avg_turn_b < FILTER["min_avg_turnover_b"]: return None

        entry_low  = round(price * 0.995, 0)
        entry_high = round(price * 1.002, 0)
        stop_loss  = round(price - RISK["stop_loss_atr_mult"] * atr, 0)
        target_1   = round(price + RISK["target_atr_mult"] * atr, 0)
        target_2   = round(price + RISK["target_atr_mult"] * 1.6 * atr, 0)
        risk       = price - stop_loss
        rr         = round((target_1 - price) / risk, 2) if risk > 0 else 0
        if rr < RISK["risk_reward_ratio"]: return None

        ticker_sector = get_ticker_sector(ticker)
        sec_mom = 0.0
        if CONFIG.get("ENABLE_SECTOR_FILTER") and sector_momentum_data and ticker_sector != "Unknown":
            sec_mom = sector_momentum_data.get(ticker_sector, 0.0)

        adj         = 1 if macro_sent == "POSITIVE" else (-1 if macro_sent == "NEGATIVE" else 0)
        final_score = min(10.0, max(0.0, norm_score + adj))

        if final_score >= 8:   sig, conf = "BUY", "HIGH"
        elif final_score >= 6: sig, conf = "BUY", "MEDIUM"
        elif final_score >= 4: sig, conf = "WATCH", "LOW"
        else:                  return None

        if CONFIG.get("ENABLE_SECTOR_FILTER") and sector_momentum_data and ticker_sector != "Unknown":
            threshold = CONFIG.get("SECTOR_MOMENTUM_THRESHOLD", 0.5)
            if sec_mom < -threshold:
                if sig == "BUY" and conf == "HIGH":    conf = "MEDIUM"
                elif sig == "BUY" and conf == "MEDIUM": sig = "WATCH"; conf = "LOW"
            elif abs(sec_mom) <= threshold:
                if sig == "BUY" and conf == "HIGH":    conf = "MEDIUM"

        if current_regime == "BEAR" and sig == "WATCH":
            return None

        downgrade_note = ""
        if (foreign_flow is not None and
                foreign_flow.get("net_foreign_b", 0) < -1000 and
                sig == "BUY" and conf == "HIGH"):
            conf = "MEDIUM"
            downgrade_note = "⚠️ Downgraded: Massive foreign sell"

        parts = []
        if tech["rsi"] < 40:        parts.append(f"RSI oversold ({tech['rsi']})")
        if tech["macd_hist"] > 0:   parts.append("MACD crossover ↑")
        if tech["pct_b"] < 0.2:     parts.append("dekat BB lower")
        if tech["vol_ratio"] > 1.5: parts.append(f"volume surge ×{tech['vol_ratio']:.1f}")
        if tech["adx"] > 25 and tech["obv_up"]: parts.append("tren kuat + OBV naik")
        if macro_sent == "POSITIVE": parts.append("makro mendukung")
        elif macro_sent == "NEGATIVE": parts.append("⚠ makro negatif")
        if pattern_name:            parts.append(f"pattern: {pattern_name}")
        if downgrade_note:          parts.append(downgrade_note)

        return StockSignal(
            ticker=ticker.replace(".JK", ""), company_name=fund.get("company_name", ticker),
            sector=ticker_sector, signal=sig, confidence=conf,
            price=price, entry_low=entry_low, entry_high=entry_high,
            stop_loss=stop_loss, target_1=target_1, target_2=target_2,
            risk_reward=rr, tech_score=norm_score, tech_details=tech["details"],
            pb=pb, pe=pe, roe=roe, market_cap_b=fund.get("market_cap_b"),
            atr=atr, atr_pct=tech["atr_pct"], vol_ratio=tech["vol_ratio"],
            support=tech["support"], resistance=tech["resistance"],
            vwap=tech["vwap"], rsi=tech["rsi"], adx=tech["adx"],
            avg_vol_m=avg_vol_m, reason=" | ".join(parts),
            raw_score=raw_score, normalized_score=norm_score,
            regime=current_regime, sector_momentum=sec_mom,
            pattern=pattern_name, macro_sentiment=macro_sent,
            data_source=data_source,
        )
    except Exception as e:
        logger.error(f"[{ticker}] signal error: {e}")
        return None


# ══════════════════════════════════════════════════════════════════════════════
#  SQLITE — SIGNAL HISTORY & PERFORMANCE
# ══════════════════════════════════════════════════════════════════════════════

def init_database():
    global _db_enabled
    try:
        with _db_lock:
            con = sqlite3.connect(CONFIG["DB_PATH"])
            con.execute("""
                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT NOT NULL, signal_date TEXT NOT NULL,
                    signal_type TEXT NOT NULL, tech_score REAL, raw_score REAL,
                    entry_price REAL, stop_loss REAL, target1 REAL, target2 REAL,
                    sector TEXT, market_regime TEXT, pattern TEXT, macro_sentiment TEXT
                )""")
            con.execute("""
                CREATE TABLE IF NOT EXISTS outcomes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    signal_id INTEGER, ticker TEXT, entry_date TEXT, check_date TEXT,
                    price_at_check REAL, result_pct REAL,
                    hit_target1 INTEGER DEFAULT 0, hit_stop INTEGER DEFAULT 0,
                    outcome_status TEXT DEFAULT 'PENDING',
                    FOREIGN KEY(signal_id) REFERENCES signals(id)
                )""")
            con.commit(); con.close()
        logger.info("Database initialized.")
    except Exception as e:
        logger.error(f"DB init failed: {e}. DB features disabled.")
        _db_enabled = False


def migrate_db_if_needed():
    """Add outcome_status column if missing (upgrade from v2 schema)."""
    if not _db_enabled:
        return
    try:
        with _db_lock:
            con = sqlite3.connect(CONFIG["DB_PATH"])
            cols = [row[1] for row in con.execute("PRAGMA table_info(outcomes)").fetchall()]
            if "outcome_status" not in cols:
                con.execute("ALTER TABLE outcomes ADD COLUMN outcome_status TEXT DEFAULT 'PENDING'")
                con.commit()
                logger.info("DB migrated: added outcome_status column.")
            con.close()
    except Exception as e:
        logger.error(f"DB migration error: {e}")


def save_signal(signal: StockSignal):
    if not _db_enabled or signal.signal == "WATCH":
        return
    try:
        with _db_lock:
            con = sqlite3.connect(CONFIG["DB_PATH"])
            con.execute("""
                INSERT INTO signals (ticker, signal_date, signal_type, tech_score, raw_score,
                    entry_price, stop_loss, target1, target2, sector, market_regime, pattern, macro_sentiment)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (signal.ticker, datetime.now(WIB).strftime("%Y-%m-%d"),
                  f"{signal.signal} {signal.confidence}", signal.tech_score, signal.raw_score,
                  signal.price, signal.stop_loss, signal.target_1, signal.target_2,
                  signal.sector, signal.regime, signal.pattern, signal.macro_sentiment))
            con.commit(); con.close()
    except Exception as e:
        logger.error(f"DB save_signal error: {e}")


def check_and_save_outcomes():
    """Full outcome check at 15:55 WIB — uses intraday H/L to detect TP/SL hit."""
    if not _db_enabled:
        return
    try:
        cutoff = (datetime.now(WIB) - timedelta(days=7)).strftime("%Y-%m-%d")
        today  = datetime.now(WIB).strftime("%Y-%m-%d")
        expire_cutoff = (datetime.now(WIB) - timedelta(days=7)).strftime("%Y-%m-%d")
        with _db_lock:
            con  = sqlite3.connect(CONFIG["DB_PATH"])
            rows = con.execute("""
                SELECT s.id, s.ticker, s.signal_date, s.entry_price, s.target1, s.stop_loss
                FROM signals s
                LEFT JOIN outcomes o ON o.signal_id = s.id
                WHERE s.signal_date >= ? AND o.id IS NULL
            """, (cutoff,)).fetchall()

        tp_hits=0; sl_hits=0; pending=0; expired=0
        for row in rows:
            sig_id, ticker, sig_date, entry, t1, sl = row
            try:
                # Fetch OHLCV — need today's High/Low for intraday TP/SL check
                df, _ = fetch_ohlcv(ticker + ".JK", period="2d")
                if df is None or df.empty:
                    pending += 1
                    continue
                today_row = df.iloc[-1]
                cur_price = float(today_row["Close"].iloc[0]) if hasattr(today_row["Close"], "iloc") else float(today_row["Close"])
                day_high  = float(today_row["High"].iloc[0])  if hasattr(today_row["High"],  "iloc") else float(today_row["High"])
                day_low   = float(today_row["Low"].iloc[0])   if hasattr(today_row["Low"],   "iloc") else float(today_row["Low"])
                day_open  = float(today_row["Open"].iloc[0])  if hasattr(today_row["Open"],  "iloc") else float(today_row["Open"])

                hit_t1 = 1 if day_high  >= t1 else 0
                hit_sl = 1 if day_low   <= sl else 0

                # Gap resolution: if BOTH hit today
                if hit_t1 and hit_sl:
                    if day_open <= sl:
                        hit_t1 = 0  # gap down → SL wins
                    else:
                        hit_sl = 0  # opened above SL → assume TP hit first

                pct = round((cur_price - entry) / entry * 100, 2) if entry else 0

                # Determine outcome_status
                if hit_t1:
                    status = "TP1_HIT"; tp_hits += 1
                elif hit_sl:
                    status = "SL_HIT";  sl_hits += 1
                else:
                    # Expired if signal_date is exactly cutoff boundary (7 days old)
                    if sig_date <= expire_cutoff:
                        status = "EXPIRED"; expired += 1
                    else:
                        status = "PENDING"; pending += 1

                with _db_lock:
                    con2 = sqlite3.connect(CONFIG["DB_PATH"])
                    con2.execute("""
                        INSERT INTO outcomes (signal_id, ticker, entry_date, check_date,
                            price_at_check, result_pct, hit_target1, hit_stop, outcome_status)
                        VALUES (?,?,?,?,?,?,?,?,?)
                    """, (sig_id, ticker, sig_date, today, cur_price, pct, hit_t1, hit_sl, status))
                    con2.commit(); con2.close()
            except Exception as ex:
                logger.warning(f"Outcome check failed for {ticker}: {ex}")
                pending += 1

        total = len(rows)
        if total > 0:
            tg_send(f"📊 *Outcome check complete: {total} signals evaluated*\n"
                    f"✅ Hit TP1: {tp_hits}  |  🛑 Hit SL: {sl_hits}  |  "
                    f"⏳ Pending: {pending}  |  💨 Expired: {expired}")
        logger.info(f"Outcomes updated: {total} signals — TP1:{tp_hits} SL:{sl_hits} Pending:{pending} Expired:{expired}")
    except Exception as e:
        logger.error(f"check_and_save_outcomes error: {e}")


def db_get_history(ticker: Optional[str] = None) -> list:
    if not _db_enabled:
        return []
    try:
        with _db_lock:
            con = sqlite3.connect(CONFIG["DB_PATH"])
            q = """
                SELECT s.ticker, s.signal_date, s.signal_type, s.tech_score, s.entry_price,
                       o.result_pct, o.hit_target1, o.outcome_status
                FROM signals s LEFT JOIN outcomes o ON o.signal_id = s.id
                {where}
                ORDER BY s.signal_date DESC LIMIT 10
            """
            if ticker:
                rows = con.execute(q.format(where="WHERE s.ticker = ?"), (ticker.upper(),)).fetchall()
            else:
                rows = con.execute(q.format(where="")).fetchall()
            con.close()
            return rows
    except Exception as e:
        logger.error(f"db_get_history error: {e}")
        return []


def db_get_last_signal_for_ticker(ticker: str) -> Optional[tuple]:
    """For /detail DB fallback: returns most recent signal row for a ticker."""
    if not _db_enabled:
        return None
    try:
        cutoff = (datetime.now(WIB) - timedelta(days=7)).strftime("%Y-%m-%d")
        with _db_lock:
            con = sqlite3.connect(CONFIG["DB_PATH"])
            row = con.execute("""
                SELECT ticker, signal_date, signal_type, tech_score,
                       entry_price, stop_loss, target1, target2, sector
                FROM signals WHERE ticker = ? AND signal_date >= ?
                ORDER BY signal_date DESC LIMIT 1
            """, (ticker.upper(), cutoff)).fetchone()
            con.close()
            return row
    except Exception as e:
        logger.error(f"db_get_last_signal_for_ticker error: {e}")
        return None


def db_get_winrate() -> dict:
    if not _db_enabled:
        return {}
    try:
        cutoff = (datetime.now(WIB) - timedelta(days=30)).strftime("%Y-%m-%d")
        with _db_lock:
            con  = sqlite3.connect(CONFIG["DB_PATH"])
            rows = con.execute("""
                SELECT o.result_pct, o.hit_target1, o.hit_stop, s.ticker
                FROM outcomes o JOIN signals s ON s.id = o.signal_id
                WHERE s.signal_date >= ? AND o.outcome_status NOT IN ('PENDING','EXPIRED')
            """, (cutoff,)).fetchall()
            con.close()
        if not rows:
            return {}
        wins      = [r for r in rows if r[0] and r[0] > 0]
        losses    = [r for r in rows if r[0] and r[0] <= 0]
        win_rets  = [r[0] for r in wins];  loss_rets = [r[0] for r in losses]
        avg_win   = round(sum(win_rets)/len(win_rets), 2)   if win_rets  else 0
        avg_loss  = round(sum(loss_rets)/len(loss_rets), 2) if loss_rets else 0
        pf        = round(abs(avg_win/avg_loss), 2)         if avg_loss  else 0
        all_rets  = [(r[0], r[3]) for r in rows if r[0] is not None]
        best      = max(all_rets, key=lambda x: x[0]) if all_rets else (0, "—")
        worst     = min(all_rets, key=lambda x: x[0]) if all_rets else (0, "—")
        return {"total": len(rows), "wins": len(wins), "losses": len(losses),
                "win_rate": round(len(wins)/len(rows)*100) if rows else 0,
                "avg_win": avg_win, "avg_loss": avg_loss, "profit_factor": pf,
                "best": best, "worst": worst}
    except Exception as e:
        logger.error(f"db_get_winrate error: {e}")
        return {}


def db_get_top() -> list:
    if not _db_enabled:
        return []
    try:
        cutoff = (datetime.now(WIB) - timedelta(days=30)).strftime("%Y-%m-%d")
        with _db_lock:
            con  = sqlite3.connect(CONFIG["DB_PATH"])
            rows = con.execute("""
                SELECT s.ticker, COUNT(*) as total,
                       SUM(CASE WHEN o.result_pct > 0 THEN 1 ELSE 0 END) as wins
                FROM signals s JOIN outcomes o ON s.id = o.signal_id
                WHERE s.signal_date >= ? AND o.outcome_status NOT IN ('PENDING','EXPIRED')
                GROUP BY s.ticker HAVING COUNT(*) >= 3
                ORDER BY (wins*1.0/COUNT(*)) DESC LIMIT 5
            """, (cutoff,)).fetchall()
            con.close()
            return rows
    except Exception as e:
        logger.error(f"db_get_top error: {e}")
        return []


def cmd_backtest(ticker: str, days: int) -> str:
    if not _db_enabled:
        return "Database tidak tersedia."
    try:
        cutoff = (datetime.now(WIB) - timedelta(days=days)).strftime("%Y-%m-%d")
        with _db_lock:
            con = sqlite3.connect(CONFIG["DB_PATH"])
            if ticker == "ALL":
                rows = con.execute("""
                    SELECT s.ticker, s.signal_date, s.market_regime, s.pattern,
                           o.result_pct, o.hit_target1, o.hit_stop, o.outcome_status, o.check_date
                    FROM signals s LEFT JOIN outcomes o ON s.id = o.signal_id
                    WHERE s.signal_date >= ?
                """, (cutoff,)).fetchall()
            else:
                rows = con.execute("""
                    SELECT s.ticker, s.signal_date, s.market_regime, s.pattern,
                           o.result_pct, o.hit_target1, o.hit_stop, o.outcome_status, o.check_date
                    FROM signals s LEFT JOIN outcomes o ON s.id = o.signal_id
                    WHERE s.signal_date >= ? AND s.ticker = ?
                """, (cutoff, ticker.upper())).fetchall()
            con.close()

        if not rows:
            return f"Tidak ada data sinyal untuk {ticker} dalam {days} hari terakhir."

        total   = len(rows)
        closed  = [r for r in rows if r[4] is not None and r[7] not in ("PENDING", None)]
        pending = total - len(closed)
        if len(closed) < 3:
            return f"Insufficient data for backtest (need ≥ 3 closed signals, have {len(closed)})."

        wins   = [r for r in closed if r[4] and r[4] > 0]
        losses = [r for r in closed if r[4] and r[4] <= 0]
        all_rets   = [r[4] for r in closed if r[4] is not None]
        win_rets   = [r[4] for r in wins]
        loss_rets  = [r[4] for r in losses]
        avg_win    = round(sum(win_rets)/len(win_rets), 2)   if win_rets  else 0
        avg_loss   = round(sum(loss_rets)/len(loss_rets), 2) if loss_rets else 0
        pf         = round(abs(avg_win/avg_loss), 2)         if avg_loss  else 0
        max_win    = max(all_rets) if all_rets else 0
        max_loss   = min(all_rets) if all_rets else 0
        best_row   = max(closed, key=lambda r: r[4] or 0)
        worst_row  = min(closed, key=lambda r: r[4] or 0)

        # Holding bars (signal_date → check_date)
        holding_days = []
        for r in closed:
            if r[1] and r[8]:
                try:
                    d1 = datetime.strptime(r[1], "%Y-%m-%d")
                    d2 = datetime.strptime(r[8], "%Y-%m-%d")
                    holding_days.append((d2-d1).days)
                except Exception:
                    pass
        avg_hold = round(sum(holding_days)/len(holding_days), 1) if holding_days else 0

        # Regime breakdown
        regime_stats: dict = {}
        for r in closed:
            reg = r[2] or "UNKNOWN"
            if reg not in regime_stats:
                regime_stats[reg] = {"s": 0, "w": 0}
            regime_stats[reg]["s"] += 1
            if r[4] and r[4] > 0:
                regime_stats[reg]["w"] += 1

        # Best pattern
        pattern_stats: dict = {}
        for r in closed:
            pat = r[3] or ""
            if pat:
                if pat not in pattern_stats:
                    pattern_stats[pat] = {"s": 0, "w": 0}
                pattern_stats[pat]["s"] += 1
                if r[4] and r[4] > 0:
                    pattern_stats[pat]["w"] += 1
        best_pat = ""
        if pattern_stats:
            eligible = {p: v for p, v in pattern_stats.items() if v["s"] >= 2}
            if eligible:
                best_pat_key = max(eligible, key=lambda p: eligible[p]["w"]/eligible[p]["s"])
                bps = eligible[best_pat_key]
                best_pat = f"{best_pat_key} ({bps['s']} trades, {round(bps['w']/bps['s']*100)}% WR)"

        label = ticker if ticker != "ALL" else "ALL tickers"
        lines = [
            f"📊 *Backtest: {label} ({days} days)*",
            f"Signals: {total} | Closed: {len(closed)} | Pending: {pending}",
            "─────────────────────",
            f"Win Rate  : {round(len(wins)/len(closed)*100) if closed else 0}% ({len(wins)}W / {len(losses)}L)",
            f"Avg Win   : +{avg_win}%",
            f"Avg Loss  : {avg_loss}%",
            f"Profit Factor: {pf}",
            f"Best Trade: +{max_win:.1f}% ({best_row[1]})",
            f"Worst Trade: {max_loss:.1f}% ({worst_row[1]})",
            f"Avg Hold  : {avg_hold} hari",
        ]
        if ticker != "ALL":
            lines += ["─────────────────────", "Regime Breakdown:"]
            for reg, v in regime_stats.items():
                wr = round(v["w"]/v["s"]*100) if v["s"] else 0
                lines.append(f"  {reg}: {v['s']} signals, {wr}% WR")
            if best_pat:
                lines += ["─────────────────────", f"Best Pattern: {best_pat}"]
        return "\n".join(lines)
    except Exception as e:
        logger.error(f"cmd_backtest error: {e}")
        return f"Backtest error: {e}"


# ══════════════════════════════════════════════════════════════════════════════
#  TELEGRAM FORMAT
# ══════════════════════════════════════════════════════════════════════════════

_EMJ = {"BUY": {"HIGH": "🟢🔥", "MEDIUM": "🟢", "LOW": "🟡"},
        "WATCH": {"HIGH": "👀", "MEDIUM": "👀", "LOW": "👀"}}

def _pct(t, b): return (t / b - 1) * 100 if b else 0
def _bar(conf): return "█" * {"HIGH": 3, "MEDIUM": 2, "LOW": 1}.get(conf, 1) + \
                       "░" * {"HIGH": 0, "MEDIUM": 1, "LOW": 2}.get(conf, 2)
def _regime_emj(r): return {"BULL": "🟢", "BEAR": "🔴", "CHOP": "🟡"}.get(r, "⚪")
def _sec_emj(m):    return "🟢" if m > 0.5 else "🔴" if m < -0.5 else "🟡"


def fmt_detail(s: StockSignal, macro: dict, ihsg: dict,
               scan_ts: str = "", portfolio_value: float = 0) -> str:
    emj  = _EMJ.get(s.signal, {}).get(s.confidence, "📊")
    sent = macro.get("macro_sentiment", "NEUTRAL")
    ig   = ihsg.get("ihsg"); ic = ihsg.get("ihsg_chg_pct", 0)
    ff   = macro.get("foreign_flow")
    pval = portfolio_value or CONFIG["PORTFOLIO_VALUE"]
    def _n(v, fmt=",.0f"): return f"{v:{fmt}}" if v is not None else "N/A"

    L = [
        f"{emj} *{s.signal} — {s.ticker}* ({s.confidence})",
        f"_{s.company_name}_",
        f"📊 Regime: {s.regime} {_regime_emj(s.regime)} | 🏦 {s.sector} | Sector: {s.sector_momentum:+.1f}% {_sec_emj(s.sector_momentum)}",
        "",
        f"🎯 Score: {s.tech_score:.1f}/10 (raw: {s.raw_score:.1f})",
    ]
    if s.pattern:
        bonus_map = {"Morning Star": "+2.0", "Bullish Engulfing": "+1.5", "Hammer": "+1.0",
                     "Bullish Harami": "+0.8", "Bearish Engulfing": "-1.5",
                     "Shooting Star": "-1.0", "Doji": "-0.3"}
        bonus = next((v for k, v in bonus_map.items() if k in s.pattern), "")
        L.append(f"🕯️ Pattern: {s.pattern} {bonus}")
    L += [
        f"Kepercayaan: `{_bar(s.confidence)}`",
        "",
        f"💰 *Harga saat ini :* Rp{s.price:,.0f}",
        f"📥 *Zona Entry      :* Rp{s.entry_low:,.0f} – Rp{s.entry_high:,.0f}",
        f"🛑 *Stop Loss       :* Rp{s.stop_loss:,.0f}  ({_pct(s.stop_loss, s.price):+.1f}%)",
        f"🎯 *Target 1        :* Rp{s.target_1:,.0f}  ({_pct(s.target_1, s.price):+.1f}%)",
        f"🎯 *Target 2        :* Rp{s.target_2:,.0f}  ({_pct(s.target_2, s.price):+.1f}%)",
        f"⚖️  *Risk:Reward     :* 1:{s.risk_reward}",
    ]

    # Position sizing block
    if s.signal == "BUY":
        pos = calculate_position_size(s, pval)
        if pos:
            L += [
                "",
                f"💼 *Position Size* (Rp {pval/1e6:.0f}jt portfolio)",
                f"   Max Shares  : {pos['max_shares']:,.0f} lbr → Rp{pos['position_value']:,.0f}",
                f"   Portfolio % : {pos['position_pct']:.1f}%",
                f"   Max Risk    : Rp{pos['max_risk']:,.0f} ({pos['risk_pct']:.1f}%)",
            ]

    L += [
        "",
        "📊 *Indikator Teknikal*",
        f"  RSI(14)    : `{s.rsi}`  {'🔵 oversold' if s.rsi < 35 else '🟠 netral' if s.rsi < 60 else '🔴 overbought'}",
        f"  MACD hist  : `{s.tech_details.get('MACD_hist','N/A')}`",
        f"  Stoch K    : `{s.tech_details.get('Stoch_K','N/A')}`",
        f"  ADX        : `{s.adx}`  {'tren kuat ✅' if s.adx > 25 else 'tren lemah'}",
        f"  Vol Ratio  : `×{s.vol_ratio}`  {'🚀 surge' if s.vol_ratio > 1.5 else '—'}",
        f"  OBV        : {s.tech_details.get('OBV','—')}",
        f"  ATR(14)    : Rp{s.atr:,.0f}  ({s.atr_pct}%/hari)",
        f"  VWAP       : Rp{s.vwap:,.0f}  ({'> VWAP ✅' if s.price > s.vwap else '< VWAP ⚠'})",
        f"  Williams%R : `{s.tech_details.get('Williams_R','N/A')}`",
        f"  BB %B      : `{s.tech_details.get('BB_%B','N/A')}`",
        "",
        "📐 *Level Kunci*",
        f"  Support    : Rp{s.support:,.0f}",
        f"  Resistance : Rp{s.resistance:,.0f}",
        "",
        "📈 *Fundamental*",
    ]
    if all(x is not None for x in [s.pb, s.pe, s.roe]):
        L.append(f"  PBV {s.pb:.2f}×  |  PER {s.pe:.1f}×  |  ROE {s.roe:.1f}%")
    else:
        L.append("  Data fundamental belum tersedia")
    if s.market_cap_b: L.append(f"  Mkt Cap : Rp{s.market_cap_b:.0f}M")
    L.append(f"  Avg Vol : {s.avg_vol_m:.0f} jt/hari")
    L += [
        "",
        "🌏 *Makro*",
        f"  IHSG     : {_n(ig)} ({ic:+.2f}%)" if ig else "  IHSG     : N/A",
        f"  USD/IDR  : {_n(macro.get('usdidr'))}",
        f"  DXY      : {_n(macro.get('dxy'), '.2f')}",
        f"  Brent    : ${_n(macro.get('brent_oil'), '.1f')}",
        f"  Gold     : ${_n(macro.get('gold'))}",
        f"  US 10Y   : {_n(macro.get('us10y_yield'), '.2f')}%",
        f"  BI Rate  : {macro.get('bi_rate', BI_RATE)}%",
        f"  Sentimen : {'🟢 Positif' if sent == 'POSITIVE' else '🔴 Negatif' if sent == 'NEGATIVE' else '⚪ Netral'}",
    ]
    ff_line = f"  Foreign  : {ff['label']} {'🟢' if ff['is_positive'] else '🔴'}" if ff else "  Foreign  : Data unavailable"
    L.append(ff_line)
    L += [
        "",
        f"💡 *Alasan:* {s.reason or '—'}",
        f"📡 Data: {s.data_source}",
    ]
    if scan_ts:
        L.append(f"🕐 Data from: {scan_ts}")
    L += ["", "⏱ *Horizon*: 1–5 hari kerja", "⚠️ _Bukan rekomendasi investasi. DYOR._"]

    msg = "\n".join(L)
    if len(msg) > 3800:
        try:
            cut_start = L.index("📊 *Indikator Teknikal*")
            cut_end   = L.index("📐 *Level Kunci*")
            L2 = L[:cut_start] + L[cut_end:]
            msg = "\n".join(L2)
        except ValueError:
            msg = msg[:3800]
    return msg


def fmt_summary(signals: list, macro: dict, ihsg: dict,
                scan_time: str, total: int, regime: str = "NEUTRAL") -> str:
    sent = macro.get("macro_sentiment", "NEUTRAL")
    ig   = ihsg.get("ihsg"); ic = ihsg.get("ihsg_chg_pct", 0)
    hi   = [s for s in signals if s.signal == "BUY" and s.confidence == "HIGH"]
    md   = [s for s in signals if s.signal == "BUY" and s.confidence == "MEDIUM"]
    wt   = [s for s in signals if s.signal == "WATCH"]
    L = [
        "📡 *SCAN SAHAM IDX — HASIL TERBARU*",
        f"🕐 {scan_time} WIB  |  Discan: {total} saham",
        f"📊 Regime: {regime} {_regime_emj(regime)}",
        "",
        f"🌏 IHSG: {ig:,.0f} ({ic:+.2f}%)" if ig else "🌏 IHSG: N/A",
        f"💵 USD/IDR: {macro.get('usdidr'):,.0f}" if macro.get("usdidr") else "",
        f"Sentimen makro: {'🟢 Positif' if sent=='POSITIVE' else '🔴 Negatif' if sent=='NEGATIVE' else '⚪ Netral'}",
        "",
        f"✅ Sinyal lolos filter : *{len(signals)}* saham",
        f"🔥 BUY HIGH            : {len(hi)}",
        f"🟢 BUY MEDIUM          : {len(md)}",
        f"👀 WATCH               : {len(wt)}", "",
    ]
    if hi:
        L.append("🔥 *TOP BUY (HIGH confidence)*")
        for s in hi[:5]:
            L.append(f"  • *{s.ticker}* Rp{s.price:,.0f} | TP Rp{s.target_1:,.0f} "
                     f"SL Rp{s.stop_loss:,.0f} | R:R 1:{s.risk_reward} | ★{s.tech_score:.1f}")
        L.append("")
    if md:
        L.append("🟢 *BUY MEDIUM*")
        for s in md[:5]:
            L.append(f"  • *{s.ticker}* Rp{s.price:,.0f} | ★{s.tech_score:.1f}/10")
        L.append("")
    if wt:
        L.append("👀 *WATCH LIST*")
        for s in wt[:5]:
            L.append(f"  • {s.ticker} Rp{s.price:,.0f}")
        L.append("")
    L.append("_Kirim /detail [KODE] untuk sinyal lengkap_")
    return "\n".join(l for l in L if l is not None)


# ══════════════════════════════════════════════════════════════════════════════
#  TELEGRAM SEND
# ══════════════════════════════════════════════════════════════════════════════

_BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

def tg_send(text: str, chat_id: str = TELEGRAM_CHAT_ID) -> bool:
    try:
        r = requests.post(f"{_BASE}/sendMessage",
                          json={"chat_id": chat_id, "text": text,
                                "parse_mode": "Markdown", "disable_web_page_preview": True},
                          timeout=15)
        return r.ok
    except Exception as e:
        logger.error(f"tg_send: {e}"); return False

def tg_send_long(text: str, chat_id: str = TELEGRAM_CHAT_ID, chunk: int = 3800):
    lines = text.split("\n"); buf = []; n = 0
    for ln in lines:
        if n + len(ln) + 1 > chunk:
            tg_send("\n".join(buf), chat_id); buf = [ln]; n = len(ln) + 1
        else:
            buf.append(ln); n += len(ln) + 1
    if buf: tg_send("\n".join(buf), chat_id)

def tg_get_updates(offset=None):
    try:
        p = {"timeout": 30}
        if offset: p["offset"] = offset
        r = requests.get(f"{_BASE}/getUpdates", params=p, timeout=35)
        return r.json().get("result", []) if r.ok else []
    except Exception:
        return []


# ══════════════════════════════════════════════════════════════════════════════
#  HEARTBEAT
# ══════════════════════════════════════════════════════════════════════════════

def send_heartbeat():
    try:
        if is_market_open():
            st   = _scanner_stats
            now  = datetime.now(WIB)
            last = st.get("last_scan_time")
            mins_ago = int((now - last).total_seconds() / 60) if last else -1
            start    = st.get("scanner_start_time")
            uptime   = ""
            if start:
                delta = now - start
                h, m  = divmod(int(delta.total_seconds() // 60), 60)
                uptime = f"{h}h {m}m"
            total   = len(_active_universe)
            errors  = st.get("fetch_errors_last_scan", 0)
            success = round((total - errors) / total * 100) if total else 100
            hi_cnt  = sum(1 for s in _last_signals.values() if getattr(s, "confidence", "") == "HIGH")
            md_cnt  = sum(1 for s in _last_signals.values() if getattr(s, "confidence", "") == "MEDIUM")
            tg_send(
                f"💓 *Scanner Heartbeat*\n"
                f"🕐 Last scan: {last.strftime('%H:%M') if last else '—'} WIB "
                f"({mins_ago}m ago)\n"
                f"⏱ Scan duration: {st.get('last_scan_duration_s', 0):.0f}s\n"
                f"📊 Signals today: {st.get('total_signals_today', 0)} "
                f"({hi_cnt} HIGH, {md_cnt} MED)\n"
                f"✅ Success rate: {success}% ({total-errors}/{total} tickers)\n"
                f"🔄 Uptime: {uptime}"
            )

            # Stuck-scan alert
            if last and (now - last).total_seconds() > SCAN_INTERVAL_MINUTES * 60 * 2:
                mins_stale = int((now - last).total_seconds() / 60)
                tg_send(f"⚠️ Scanner may be stuck — last scan: {mins_stale} minutes ago")
    except Exception as e:
        logger.warning(f"send_heartbeat error: {e}")
    finally:
        interval = CONFIG.get("HEARTBEAT_INTERVAL_MIN", 60) * 60
        t = threading.Timer(interval, send_heartbeat)
        t.daemon = True
        t.start()


def start_heartbeat_timer():
    interval = CONFIG.get("HEARTBEAT_INTERVAL_MIN", 60) * 60
    t = threading.Timer(interval, send_heartbeat)
    t.daemon = True
    t.start()
    logger.info(f"Heartbeat timer started (every {CONFIG.get('HEARTBEAT_INTERVAL_MIN', 60)} min)")


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN SCANNER LOOP
# ══════════════════════════════════════════════════════════════════════════════

def _process(ticker: str, sent: str, regime: str, sector_momentum_data: dict,
             foreign_flow: Optional[dict], effective_min_score: int):
    try:
        df, src = fetch_ohlcv(ticker)
        if df is None: return ticker, None, True   # (ticker, signal, was_error)
        fund = fetch_fundamental(ticker)
        sig  = generate_signal(ticker, df, fund, sent, regime,
                               sector_momentum_data, foreign_flow,
                               effective_min_score, data_source=src or "yfinance")
        return ticker, sig, False
    except Exception as e:
        logger.warning(f"[{ticker}] _process error: {e}")
        return ticker, None, True


def run_scan():
    global _last_macro, _last_ihsg, _last_signals, _last_scan_signals, \
           _last_scan_timestamp, _universe_last_validated, _active_universe, _scanner_stats

    logger.info(f"=== SCAN MULAI: {len(_active_universe)} saham ===")
    t0  = time.time()
    now = datetime.now(WIB)
    _scanner_stats["total_scans"] += 1

    macro = fetch_macro(); ihsg = fetch_ihsg()
    _last_macro = macro; _last_ihsg = ihsg
    sent         = macro.get("macro_sentiment", "NEUTRAL")
    foreign_flow = macro.get("foreign_flow")

    regime = "NEUTRAL"
    if CONFIG.get("ENABLE_MARKET_REGIME"):
        regime = detect_market_regime()

    effective_min_score = (7 if regime == "BEAR" else 6 if regime == "CHOP"
                           else FILTER["min_signal_score"])

    sector_momentum_data = {}
    if CONFIG.get("ENABLE_SECTOR_FILTER"):
        try:
            sector_momentum_data = calculate_sector_momentum()
        except Exception as e:
            logger.warning(f"Sector momentum failed: {e}")

    today = now.date()
    if (today.weekday() == 0 and _universe_last_validated != today
            and CONFIG.get("ENABLE_UNIVERSE_VALIDATION")):
        _active_universe = validate_universe(IDX_UNIVERSE)
        _universe_last_validated = today

    signals    = []
    error_cnt  = 0
    fallback_cnt = 0
    scan_ts    = now.strftime("%d %b %Y %H:%M")

    with ThreadPoolExecutor(max_workers=CONFIG["MAX_WORKERS"]) as ex:
        futs = {ex.submit(_process, t, sent, regime, sector_momentum_data,
                          foreign_flow, effective_min_score): t
                for t in _active_universe}
        for f in as_completed(futs):
            try:
                ticker, sig, was_error = f.result(timeout=60)
                if was_error:
                    error_cnt += 1
                if sig:
                    signals.append(sig)
                    _last_signals[sig.ticker] = sig
                    # Persist to _last_scan_signals (merges, never clears)
                    _last_scan_signals[sig.ticker] = {"signal": sig, "ts": scan_ts}
                    if getattr(sig, "data_source", "yfinance") == "yahoo_scrape":
                        fallback_cnt += 1
                    logger.info(f"  ✅ {sig.ticker}: {sig.signal} ({sig.confidence}) "
                                f"score {sig.tech_score:.1f}/10")
            except Exception as e:
                logger.warning(f"Future result error: {e}")
                error_cnt += 1
            time.sleep(0.05)

    dur = time.time() - t0
    order = {"BUY": 0, "WATCH": 1}; co = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    signals.sort(key=lambda s: (order.get(s.signal, 9), co.get(s.confidence, 9), -s.tech_score))

    # Update stats
    _scanner_stats["successful_scans"]    += 1
    _scanner_stats["last_scan_time"]       = now
    _scanner_stats["last_scan_duration_s"] = round(dur, 1)
    _scanner_stats["last_signal_count"]    = len(signals)
    _scanner_stats["fetch_errors_last_scan"] = error_cnt
    _scanner_stats["fallback_tickers_last"]  = fallback_cnt
    today_date = now.date()
    if _scanner_stats.get("signals_today_date") != today_date:
        _scanner_stats["total_signals_today"] = 0
        _scanner_stats["signals_today_date"]  = today_date
    _scanner_stats["total_signals_today"] += len(signals)
    _last_scan_timestamp = scan_ts

    # High-error-rate alert
    univ_size = len(_active_universe)
    if univ_size > 0 and error_cnt / univ_size > 0.20:
        tg_send(f"⚠️ High error rate: {error_cnt}/{univ_size} tickers failed last scan")

    logger.info(f"=== SCAN SELESAI: {len(signals)} sinyal dalam {dur:.1f}s "
                f"(errors: {error_cnt}, fallbacks: {fallback_cnt}) ===")
    return signals, macro, ihsg, regime


def send_results(signals, macro, ihsg, regime="NEUTRAL"):
    now_str = datetime.now(WIB).strftime("%d %b %Y %H:%M")
    tg_send_long(fmt_summary(signals, macro, ihsg, now_str, len(_active_universe), regime))
    for s in [x for x in signals if x.signal == "BUY"][:5]:
        tg_send_long(fmt_detail(s, macro, ihsg,
                                scan_ts=_last_scan_timestamp,
                                portfolio_value=_portfolio_value_session))
        save_signal(s)
        time.sleep(1)


def _run_and_send():
    try:
        sigs, macro, ihsg, regime = run_scan()
        send_results(sigs, macro, ihsg, regime)
    except Exception as e:
        logger.error(f"run error: {e}", exc_info=True)
        tg_send(f"⚠️ Error scan: {e}")


# ══════════════════════════════════════════════════════════════════════════════
#  TELEGRAM COMMAND HANDLER
# ══════════════════════════════════════════════════════════════════════════════

def handle_commands():
    global _update_offset, _portfolio_value_session
    while True:
        try:
            for upd in tg_get_updates(_update_offset):
                try:
                    _update_offset = upd["update_id"] + 1
                    msg     = upd.get("message", {})
                    text    = msg.get("text", "").strip()
                    chat_id = str(msg.get("chat", {}).get("id", ""))
                    if not text: continue

                    # ── /detail ──────────────────────────────────────────────
                    if text.lower().startswith("/detail"):
                        parts = text.split()
                        if len(parts) < 2:
                            tg_send("Contoh: `/detail BBCA`", chat_id); continue
                        kode = parts[1].upper().replace(".JK", "")

                        # 1. Check _last_scan_signals (persists across scans)
                        if kode in _last_scan_signals:
                            entry = _last_scan_signals[kode]
                            tg_send_long(fmt_detail(entry["signal"], _last_macro, _last_ihsg,
                                                    scan_ts=entry["ts"],
                                                    portfolio_value=_portfolio_value_session), chat_id)
                        else:
                            # 2. DB fallback
                            row = db_get_last_signal_for_ticker(kode)
                            if row:
                                tk, sig_date, sig_type, sc, entry_p, sl, t1, t2, sec = row
                                try:
                                    df2, _ = fetch_ohlcv(kode + ".JK", period="2d")
                                    cur = float(df2["Close"].iloc[-1]) if df2 is not None and not df2.empty else entry_p
                                except Exception:
                                    cur = entry_p
                                risk   = entry_p - sl
                                rr     = round((t1 - entry_p) / risk, 2) if risk > 0 else 0
                                cur_pct = round((cur - entry_p) / entry_p * 100, 2) if entry_p else 0
                                tg_send(
                                    f"📋 *{kode}* — data dari DB ({sig_date})\n"
                                    f"Signal   : {sig_type}  |  Sektor: {sec}\n"
                                    f"Score    : ★{sc:.1f}\n"
                                    f"Entry    : Rp{entry_p:,.0f}\n"
                                    f"SL       : Rp{sl:,.0f}  |  TP1: Rp{t1:,.0f}\n"
                                    f"R:R      : 1:{rr}\n"
                                    f"Harga kini: Rp{cur:,.0f} ({cur_pct:+.2f}%)\n"
                                    f"_Data historis — bukan sinyal aktif_",
                                    chat_id)
                            else:
                                tg_send(f"⚠️ Tidak ada data sinyal untuk *{kode}* dalam 7 hari terakhir.", chat_id)

                    # ── /scan ─────────────────────────────────────────────────
                    elif text.lower() == "/scan":
                        tg_send("🔄 Scan manual dimulai...", chat_id)
                        _run_and_send()

                    # ── /makro ────────────────────────────────────────────────
                    elif text.lower() == "/makro":
                        m = _last_macro; ih = _last_ihsg
                        if not m: tg_send("Belum ada data. Tunggu scan pertama.", chat_id); continue
                        ig = ih.get("ihsg"); ic = ih.get("ihsg_chg_pct", 0)
                        ff = m.get("foreign_flow")
                        def _v(k, fmt=",.0f"): return f"{m[k]:{fmt}}" if m.get(k) else "N/A"
                        lines = [
                            "🌏 *Data Makro Terkini*",
                            f"  IHSG    : {ig:,.0f} ({ic:+.2f}%)" if ig else "  IHSG    : N/A",
                            f"  USD/IDR : {_v('usdidr')}",
                            f"  DXY     : {_v('dxy', '.2f')}",
                            f"  Brent   : ${_v('brent_oil', '.1f')}",
                            f"  Gold    : ${_v('gold')}",
                            f"  US 10Y  : {_v('us10y_yield', '.2f')}%",
                            f"  BI Rate : {m.get('bi_rate', BI_RATE)}%",
                            f"  Sentimen: {'🟢 Positif' if m.get('macro_sentiment')=='POSITIVE' else '🔴 Negatif' if m.get('macro_sentiment')=='NEGATIVE' else '⚪ Netral'}",
                            f"  Foreign : {ff['label']} {'🟢' if ff['is_positive'] else '🔴'}" if ff else "  Foreign : Data unavailable",
                        ]
                        tg_send("\n".join(lines), chat_id)

                    # ── /regime ───────────────────────────────────────────────
                    elif text.lower() == "/regime":
                        r   = _regime_cache.get("regime") or "—"
                        e20 = _regime_cache.get("ema20") or "—"
                        e50 = _regime_cache.get("ema50") or "—"
                        ih  = _regime_cache.get("ihsg")  or "—"
                        tg_send(f"📊 *Market Regime: {r}* {_regime_emj(r)}\n"
                                f"IHSG : {ih:,.0f}\nEMA20: {e20:,.0f} | EMA50: {e50:,.0f}"
                                if isinstance(ih, (int, float))
                                else f"📊 *Market Regime: {r}* {_regime_emj(r)}\n"
                                     f"IHSG : {ih}\nEMA20: {e20} | EMA50: {e50}",
                                chat_id)

                    # ── /sectors ──────────────────────────────────────────────
                    elif text.lower() == "/sectors":
                        sd = _sector_cache.get("data")
                        if not sd:
                            tg_send("Belum ada data sektor. Tunggu scan pertama.", chat_id); continue
                        ranked = sorted(sd.items(), key=lambda x: x[1], reverse=True)
                        lines  = ["📊 *Sector Momentum vs IHSG (10-day)*", ""]
                        for sec, mom in ranked:
                            e = "🟢" if mom > 0.5 else "🔴" if mom < -0.5 else "🟡"
                            lines.append(f"  {e} {sec}: {mom:+.2f}%")
                        tg_send("\n".join(lines), chat_id)

                    # ── /history ──────────────────────────────────────────────
                    elif text.lower().startswith("/history"):
                        parts  = text.split()
                        ticker = parts[1].upper() if len(parts) >= 2 else None
                        rows   = db_get_history(ticker)
                        if not rows:
                            tg_send("Belum ada riwayat sinyal.", chat_id); continue
                        status_badge = {"TP1_HIT": "✅", "SL_HIT": "🛑", "PENDING": "⏳", "EXPIRED": "💨"}
                        lines = [f"📋 *Riwayat Sinyal{' ' + ticker if ticker else ''}*", ""]
                        for row in rows:
                            tk, dt, sig, sc, entry, res, hit, status = row
                            badge    = status_badge.get(status or "PENDING", "⏳")
                            res_str  = f"{res:+.1f}%" if res is not None else "—"
                            stat_str = status or "PENDING"
                            lines.append(f"  {tk} | {sig} | {res_str} | {stat_str} {badge}")
                        tg_send("\n".join(lines), chat_id)

                    # ── /winrate ──────────────────────────────────────────────
                    elif text.lower() == "/winrate":
                        wr = db_get_winrate()
                        if not wr:
                            tg_send("Belum ada data outcome.", chat_id); continue
                        best  = wr.get("best",  (0, "—"))
                        worst = wr.get("worst", (0, "—"))
                        tg_send(f"📊 *30-Day Performance*\n"
                                f"Total Signals : {wr['total']}\n"
                                f"Win Rate      : {wr['win_rate']}%\n"
                                f"Avg Win       : +{wr['avg_win']}%\n"
                                f"Avg Loss      : {wr['avg_loss']}%\n"
                                f"Profit Factor : {wr['profit_factor']}\n"
                                f"Best          : {best[1]} {best[0]:+.1f}%\n"
                                f"Worst         : {worst[1]} {worst[0]:+.1f}%", chat_id)

                    # ── /top ──────────────────────────────────────────────────
                    elif text.lower() == "/top":
                        rows = db_get_top()
                        if not rows:
                            tg_send("Belum cukup data (min 3 sinyal per ticker).", chat_id); continue
                        lines = ["🏆 *Top Performers (30d)*", ""]
                        for i, (tk, total, wins) in enumerate(rows, 1):
                            wr = round(wins / total * 100) if total else 0
                            lines.append(f"  {i}. {tk} — {wr}% WR ({total} signals)")
                        tg_send("\n".join(lines), chat_id)

                    # ── /backtest ─────────────────────────────────────────────
                    elif text.lower().startswith("/backtest"):
                        parts  = text.split()
                        ticker = parts[1].upper() if len(parts) >= 2 else "ALL"
                        days   = int(parts[2]) if len(parts) >= 3 else 30
                        days   = max(7, min(365, days))
                        tg_send(cmd_backtest(ticker, days), chat_id)

                    # ── /holiday ──────────────────────────────────────────────
                    elif text.lower() == "/holiday":
                        today_dt = datetime.now(WIB).date()
                        upcoming = []
                        for d in sorted(BEI_HOLIDAYS_2026):
                            hdate = datetime.strptime(d, "%Y-%m-%d").date()
                            if hdate >= today_dt:
                                upcoming.append((d, BEI_HOLIDAY_NAMES.get(d, "—"), (hdate - today_dt).days))
                            if len(upcoming) >= 3:
                                break
                        if not upcoming:
                            tg_send("Tidak ada libur BEI 2026 berikutnya.", chat_id); continue
                        lines = ["📅 *Upcoming BEI Holidays*", ""]
                        for i, (d, name, days) in enumerate(upcoming, 1):
                            lines.append(f"  {i}. {d} — {name} ({days} days away)")
                        tg_send("\n".join(lines), chat_id)

                    # ── /status ───────────────────────────────────────────────
                    elif text.lower() == "/status":
                        st   = _scanner_stats
                        now2 = datetime.now(WIB)
                        last = st.get("last_scan_time")
                        start = st.get("scanner_start_time")
                        uptime = ""
                        if start:
                            delta = now2 - start
                            h, m  = divmod(int(delta.total_seconds() // 60), 60)
                            uptime = f"{h}h {m}m"
                        mins_ago = int((now2-last).total_seconds()/60) if last else -1
                        total    = len(_active_universe)
                        errors   = st.get("fetch_errors_last_scan", 0)
                        succ     = round((total-errors)/total*100) if total else 100
                        tg_send(f"📡 *Scanner Status*\n"
                                f"🕐 Last scan : {last.strftime('%H:%M') if last else '—'} WIB "
                                f"({mins_ago}m ago)\n"
                                f"⏱ Duration  : {st.get('last_scan_duration_s',0):.0f}s\n"
                                f"📊 Today     : {st.get('total_signals_today',0)} signals\n"
                                f"✅ Scans OK  : {st.get('successful_scans',0)}/{st.get('total_scans',0)}\n"
                                f"🌐 Universe  : {total} tickers active\n"
                                f"📉 Last errs : {errors} ({100-succ}%)\n"
                                f"🔄 Uptime    : {uptime}", chat_id)

                    # ── /setportfolio ─────────────────────────────────────────
                    elif text.lower().startswith("/setportfolio"):
                        parts = text.split()
                        if len(parts) >= 2:
                            try:
                                new_val = float(parts[1].replace(".", "").replace(",", ""))
                                _portfolio_value_session = new_val
                                tg_send(f"✅ Portfolio set to Rp {new_val:,.0f}. "
                                        f"Position sizes updated.", chat_id)
                            except ValueError:
                                tg_send("Format: `/setportfolio 25000000`", chat_id)
                        else:
                            tg_send(f"Portfolio saat ini: Rp {_portfolio_value_session:,.0f}\n"
                                    f"Ganti: `/setportfolio 25000000`", chat_id)

                    # ── /help ─────────────────────────────────────────────────
                    elif text.lower() == "/help":
                        tg_send(
                            "🤖 *IDX Scanner v2.1 — Perintah*\n\n"
                            "/scan              — Scan manual sekarang\n"
                            "/detail BBCA       — Sinyal lengkap + posisi\n"
                            "/makro             — Data makro terkini\n"
                            "/regime            — Market regime IHSG\n"
                            "/sectors           — Sektor momentum ranking\n"
                            "/history [BBCA]    — Riwayat sinyal + outcome\n"
                            "/winrate           — Performa 30 hari\n"
                            "/top               — Top performing tickers\n"
                            "/backtest BBCA 30  — Mini backtest dari DB\n"
                            "/holiday           — Libur BEI berikutnya\n"
                            "/status            — Scanner health stats\n"
                            "/setportfolio      — Set ukuran portofolio\n"
                            "/help              — Perintah ini",
                            chat_id)
                except Exception as e:
                    logger.warning(f"cmd handler inner: {e}")
        except Exception as e:
            logger.warning(f"cmd handler: {e}")
        time.sleep(2)


# ══════════════════════════════════════════════════════════════════════════════
#  MARKET OPEN CHECK
# ══════════════════════════════════════════════════════════════════════════════

def is_market_open():
    now = datetime.now(WIB)
    if now.strftime("%Y-%m-%d") in BEI_HOLIDAYS_2026:
        return False
    if now.weekday() >= 5:
        return False
    op = now.replace(hour=MARKET_OPEN_HOUR,  minute=MARKET_OPEN_MINUTE,  second=0, microsecond=0)
    cl = now.replace(hour=MARKET_CLOSE_HOUR, minute=MARKET_CLOSE_MINUTE, second=0, microsecond=0)
    return op <= now <= cl

def secs_to_next():
    now = datetime.now(WIB); iv = SCAN_INTERVAL_MINUTES * 60
    return iv - (now.minute * 60 + now.second) % iv


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    global _active_universe, _universe_last_validated, _scanner_stats

    logger.info("IDX Stock Scanner v2.1 dimulai")

    init_database()
    migrate_db_if_needed()

    if CONFIG.get("ENABLE_UNIVERSE_VALIDATION"):
        logger.info("Validating universe...")
        _active_universe = validate_universe(IDX_UNIVERSE)
        _universe_last_validated = datetime.now(WIB).date()
    else:
        _active_universe = IDX_UNIVERSE.copy()

    _scanner_stats["scanner_start_time"] = datetime.now(WIB)
    start_heartbeat_timer()

    tg_send(f"🚀 *IDX Stock Scanner v2.1 aktif!*\n"
            f"Universe: {len(_active_universe)} saham  |  Interval: {SCAN_INTERVAL_MINUTES} menit\n"
            f"Features: Regime ✅ | Sector ✅ | Patterns ✅ | DB ✅ | Heartbeat ✅\n"
            "Kirim /help untuk perintah.")

    threading.Thread(target=handle_commands, daemon=True).start()

    first = True
    while True:
        if is_market_open():
            if first:
                first = False
                try:
                    _run_and_send()
                except Exception as e:
                    logger.critical(f"Scan loop crashed: {e}", exc_info=True)
                    tg_send("⚠️ Scanner error — restarting scan loop")
            else:
                w = secs_to_next()
                logger.info(f"Scan berikutnya {w//60}m {w%60}s lagi")
                time.sleep(w)
                if is_market_open():
                    try:
                        _run_and_send()
                    except Exception as e:
                        logger.critical(f"Scan loop crashed: {e}", exc_info=True)
                        tg_send("⚠️ Scanner error — restarting scan loop")

            global _outcomes_last_run
            now = datetime.now(WIB)
            if (now.hour == 15 and now.minute >= 55 and
                    (_outcomes_last_run is None or _outcomes_last_run != now.date())):
                _outcomes_last_run = now.date()
                threading.Thread(target=check_and_save_outcomes, daemon=True).start()
        else:
            logger.info(f"Pasar tutup ({datetime.now(WIB).strftime('%a %H:%M')} WIB). Cek 5 menit lagi.")
            first = True
            time.sleep(300)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Scanner dihentikan.")
        tg_send("🛑 IDX Scanner dihentikan.")
