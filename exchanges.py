from __future__ import annotations
from typing import Dict, List, Optional, Tuple
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from models import (
    Candle, ExchangeSnapshot, LiquidationEvent, OIPoint, OrderBookLevel,
    TopTraderRatio, TradeEvent, BasisPoint, FuturesOIPoint, SpotVsPerpPoint,
)

DEFAULT_TIMEOUT = 10
EXCHANGE_ORDER = ("bybit", "binance", "okx", "hyperliquid")
SUPPORTED_INTERVALS = ("1m", "3m", "5m", "15m", "30m", "1h", "4h", "1d")

BYBIT_CANDLE_INTERVALS  = {"1m":"1","3m":"3","5m":"5","15m":"15","30m":"30","1h":"60","4h":"240","1d":"D"}
BINANCE_CANDLE_INTERVALS= {"1m":"1m","3m":"3m","5m":"5m","15m":"15m","30m":"30m","1h":"1h","4h":"4h","1d":"1d"}
OKX_CANDLE_INTERVALS    = {"1m":"1m","3m":"3m","5m":"5m","15m":"15m","30m":"30m","1h":"1H","4h":"4H","1d":"1Dutc"}
HL_CANDLE_INTERVALS     = {"1m":"1m","3m":"3m","5m":"5m","15m":"15m","30m":"30m","1h":"1h","4h":"4h","1d":"1d"}
BINANCE_OI_INTERVALS    = {"1m":"5m","3m":"5m","5m":"5m","15m":"15m","30m":"30m","1h":"1h","4h":"4h","1d":"1d"}
BYBIT_OI_INTERVALS      = {"1m":"5min","3m":"5min","5m":"5min","15m":"15min","30m":"30min","1h":"1h","4h":"4h","1d":"1d"}
BINANCE_RATIO_PERIODS   = {"1m":"5m","3m":"5m","5m":"5m","15m":"15m","30m":"30m","1h":"1h","4h":"4h","1d":"1d"}


def safe_float(v):
    if v in (None, ""): return None
    try: return float(v)
    except: return None

def safe_int(v):
    if v in (None, ""): return None
    try: return int(v)
    except: return None

def interval_to_millis(interval: str) -> int:
    return {"1m":60000,"3m":180000,"5m":300000,"15m":900000,"30m":1800000,
            "1h":3600000,"4h":14400000,"1d":86400000}.get(interval, 300000)

def default_symbols(coin: str) -> Dict[str, str]:
    c = coin.upper().strip()
    return {"bybit":f"{c}USDT","binance":f"{c}USDT","okx":f"{c}-USDT-SWAP","hyperliquid":c}

def compute_notional(price, size):
    if price is None or size is None: return None
    return price * size

def normalize_depth_limit(exchange_key: str, limit: int) -> int:
    if exchange_key == "binance":
        for s in [5,10,20,50,100,500,1000]:
            if limit <= s: return s
        return 1000
    return limit

def normalize_liquidation_side(value) -> str:
    t = str(value or "").strip().lower()
    if t in {"long","longs","sell"}: return "long"
    if t in {"short","shorts","buy"}: return "short"
    return t or "unknown"


class BaseClient:
    exchange_name = "Unknown"
    base_url = ""

    def __init__(self, timeout: int = DEFAULT_TIMEOUT):
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "exchange-liquidity-gui/3.1"})
        retry = Retry(total=2, connect=2, read=2, backoff_factor=0.35,
                      status_forcelist=(429,500,502,503,504), allowed_methods=("GET","POST"))
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _get(self, path, params=None):
        r = self.session.get(self.base_url + path, params=params, timeout=self.timeout)
        r.raise_for_status(); return r.json()

    def _post(self, path, json=None):
        r = self.session.post(self.base_url + path, json=json, timeout=self.timeout)
        r.raise_for_status(); return r.json()

    def fetch(self, symbol): raise NotImplementedError
    def fetch_candles(self, symbol, interval, limit): raise NotImplementedError
    def fetch_orderbook(self, symbol, limit): raise NotImplementedError
    def fetch_open_interest_history(self, symbol, interval, limit): return []
    def fetch_liquidations(self, symbol, limit): return []
    def fetch_recent_trades(self, symbol, limit): return []
    def fetch_top_trader_ratio(self, symbol, interval, limit): return []
    def fetch_global_long_short_ratio(self, symbol, interval, limit): return []
    def fetch_spot_ticker(self, coin): return None           # returns (price, volume_24h)
    def fetch_futures_oi_list(self, coin): return []         # returns List[FuturesOIPoint]
    def _error(self, symbol, exc):
        return ExchangeSnapshot(exchange=self.exchange_name, symbol=symbol, status="error", error=str(exc))


# ─── Bybit ──────────────────────────────────────────────────────────────────
class BybitClient(BaseClient):
    exchange_name = "Bybit"
    base_url = "https://api.bybit.com"

    def fetch(self, symbol):
        try:
            p = self._get("/v5/market/tickers", {"category":"linear","symbol":symbol})
            items = p.get("result",{}).get("list",[])
            if not items: raise ValueError("empty ticker")
            item = items[0]
            return ExchangeSnapshot(
                exchange=self.exchange_name, symbol=symbol,
                last_price=safe_float(item.get("lastPrice")),
                mark_price=safe_float(item.get("markPrice")),
                index_price=safe_float(item.get("indexPrice")),
                open_interest=safe_float(item.get("openInterest")),
                open_interest_notional=safe_float(item.get("openInterestValue")),
                funding_rate=safe_float(item.get("fundingRate")),
                volume_24h_base=safe_float(item.get("volume24h")),
                volume_24h_notional=safe_float(item.get("turnover24h")),
                timestamp_ms=safe_int(p.get("time")), raw=item)
        except Exception as e: return self._error(symbol, e)

    def fetch_candles(self, symbol, interval, limit):
        p = self._get("/v5/market/kline", {"category":"linear","symbol":symbol,
            "interval":BYBIT_CANDLE_INTERVALS.get(interval,"5"),"limit":min(limit,1000)})
        return [Candle(timestamp_ms=safe_int(r[0]) or 0,
            open=safe_float(r[1]) or 0, high=safe_float(r[2]) or 0,
            low=safe_float(r[3]) or 0, close=safe_float(r[4]) or 0,
            volume=safe_float(r[5]) or 0)
            for r in reversed(p.get("result",{}).get("list",[]))]

    def fetch_orderbook(self, symbol, limit):
        p = self._get("/v5/market/orderbook", {"category":"linear","symbol":symbol,"limit":min(limit,200)})
        r = p.get("result",{})
        levels = [OrderBookLevel(safe_float(pr) or 0, safe_float(sz) or 0, "bid") for pr,sz in r.get("b",[])]
        levels += [OrderBookLevel(safe_float(pr) or 0, safe_float(sz) or 0, "ask") for pr,sz in r.get("a",[])]
        return levels

    def fetch_open_interest_history(self, symbol, interval, limit):
        p = self._get("/v5/market/open-interest", {"category":"linear","symbol":symbol,
            "intervalTime":BYBIT_OI_INTERVALS.get(interval,"5min"),"limit":min(limit,200)})
        return [OIPoint(safe_int(i.get("timestamp")) or 0, safe_float(i.get("openInterest")), None)
                for i in reversed(p.get("result",{}).get("list",[]))]

    def fetch_recent_trades(self, symbol, limit):
        try:
            p = self._get("/v5/market/recent-trade", {"category":"linear","symbol":symbol,"limit":min(limit,1000)})
            trades = []
            for item in p.get("result",{}).get("list",[]):
                price = safe_float(item.get("price")) or 0
                size  = safe_float(item.get("size")) or 0
                side  = "buy" if str(item.get("side","")).lower() == "buy" else "sell"
                trades.append(TradeEvent(exchange=self.exchange_name, symbol=symbol,
                    timestamp_ms=safe_int(item.get("time")) or int(time.time()*1000),
                    price=price, size=size, side=side, notional=price*size, source="rest", raw=item))
            return trades
        except: return []

    def fetch_top_trader_ratio(self, symbol, interval, limit):
        # Bybit: buyRatio = 主动买比例（Taker方向）
        try:
            p = self._get("/v5/market/account-ratio", {"category":"linear","symbol":symbol,
                "period":BINANCE_RATIO_PERIODS.get(interval,"5m"),"limit":min(limit,500)})
            return [TopTraderRatio(timestamp_ms=safe_int(i.get("timestamp")) or 0,
                        bybit_buy_ratio=safe_float(i.get("buyRatio")))
                    for i in p.get("result",{}).get("list",[])]
        except: return []

    def fetch_spot_ticker(self, coin):
        try:
            symbol = f"{coin.upper()}USDT"
            p = self._get("/v5/market/tickers", {"category":"spot","symbol":symbol})
            items = p.get("result",{}).get("list",[])
            if not items: return None
            item = items[0]
            return (safe_float(item.get("lastPrice")), safe_float(item.get("turnover24h")))
        except: return None

    def fetch_futures_oi_list(self, coin):
        """获取所有线性合约（含到期日）的OI列表"""
        try:
            p = self._get("/v5/market/tickers", {"category":"linear"})
            results = []
            coin_upper = coin.upper()
            spot = self.fetch_spot_ticker(coin)
            spot_price = spot[0] if spot else None
            for item in p.get("result",{}).get("list",[]):
                sym = item.get("symbol","")
                if not sym.startswith(coin_upper): continue
                oi_val = safe_float(item.get("openInterestValue"))
                price  = safe_float(item.get("lastPrice"))
                basis_pct = None
                if spot_price and price:
                    basis_pct = (price - spot_price) / spot_price * 100
                # PERP vs dated
                expiry = "PERP" if sym.endswith("USDT") else sym[len(coin_upper):]
                results.append(FuturesOIPoint(expiry=expiry, oi_notional=oi_val,
                    price=price, basis_pct=basis_pct, exchange=self.exchange_name))
            return results
        except: return []


# ─── Binance ────────────────────────────────────────────────────────────────
class BinanceClient(BaseClient):
    exchange_name = "Binance"
    base_url = "https://fapi.binance.com"
    spot_url  = "https://api.binance.com"

    def fetch(self, symbol):
        try:
            stats   = self._get("/fapi/v1/ticker/24hr", {"symbol":symbol})
            premium = self._get("/fapi/v1/premiumIndex", {"symbol":symbol})
            oi_p    = self._get("/fapi/v1/openInterest", {"symbol":symbol})
            last_price = safe_float(stats.get("lastPrice"))
            mark_price = safe_float(premium.get("markPrice"))
            oi = safe_float(oi_p.get("openInterest"))
            oi_notional = oi * mark_price if oi and mark_price else None
            return ExchangeSnapshot(
                exchange=self.exchange_name, symbol=symbol,
                last_price=last_price, mark_price=mark_price,
                index_price=safe_float(premium.get("indexPrice")),
                open_interest=oi, open_interest_notional=oi_notional,
                funding_rate=safe_float(premium.get("lastFundingRate")),
                volume_24h_base=safe_float(stats.get("volume")),
                volume_24h_notional=safe_float(stats.get("quoteVolume")),
                timestamp_ms=safe_int(stats.get("closeTime")),
                raw={"ticker_24h":stats,"premium_index":premium,"open_interest":oi_p})
        except Exception as e: return self._error(symbol, e)

    def fetch_candles(self, symbol, interval, limit):
        p = self._get("/fapi/v1/klines", {"symbol":symbol,
            "interval":BINANCE_CANDLE_INTERVALS.get(interval,"5m"),"limit":min(limit,1500)})
        candles = []
        for row in p:
            total_vol = safe_float(row[5]) or 0
            taker_buy = safe_float(row[9]) if len(row) > 9 else None
            taker_sell = (total_vol - taker_buy) if taker_buy is not None else None
            candles.append(Candle(
                timestamp_ms=safe_int(row[0]) or 0,
                open=safe_float(row[1]) or 0, high=safe_float(row[2]) or 0,
                low=safe_float(row[3]) or 0, close=safe_float(row[4]) or 0,
                volume=total_vol, taker_buy_volume=taker_buy, taker_sell_volume=taker_sell))
        return candles

    def fetch_orderbook(self, symbol, limit):
        norm = normalize_depth_limit("binance", max(5, min(limit, 1000)))
        p = self._get("/fapi/v1/depth", {"symbol":symbol,"limit":norm})
        levels  = [OrderBookLevel(safe_float(pr) or 0, safe_float(sz) or 0, "bid") for pr,sz in p.get("bids",[])]
        levels += [OrderBookLevel(safe_float(pr) or 0, safe_float(sz) or 0, "ask") for pr,sz in p.get("asks",[])]
        return levels

    def fetch_open_interest_history(self, symbol, interval, limit):
        p = self._get("/futures/data/openInterestHist", {"symbol":symbol,
            "period":BINANCE_OI_INTERVALS.get(interval,"5m"),"limit":min(limit,500)})
        return [OIPoint(safe_int(i.get("timestamp")) or 0,
                safe_float(i.get("sumOpenInterest")), safe_float(i.get("sumOpenInterestValue")))
                for i in p]

    def fetch_liquidations(self, symbol, limit):
        try:
            p = self._get("/fapi/v1/allForceOrders", {"symbol":symbol,"limit":min(limit,100)})
            items = p if isinstance(p, list) else p.get("data", [])
            events = []
            for item in items:
                price = safe_float(item.get("avgPrice")) or safe_float(item.get("price"))
                size  = safe_float(item.get("executedQty")) or safe_float(item.get("origQty"))
                events.append(LiquidationEvent(
                    exchange=self.exchange_name, symbol=symbol,
                    timestamp_ms=safe_int(item.get("time")) or 0,
                    side=normalize_liquidation_side(item.get("side")),
                    price=price, size=size,
                    notional=compute_notional(price,size) or safe_float(item.get("cumQuote")),
                    source="rest", raw=item))
            return events
        except: return []

    def fetch_recent_trades(self, symbol, limit):
        try:
            p = self._get("/fapi/v1/trades", {"symbol":symbol,"limit":min(limit,1000)})
            trades = []
            for item in p:
                price = safe_float(item.get("price")) or 0
                size  = safe_float(item.get("qty")) or 0
                side  = "sell" if item.get("isBuyerMaker") else "buy"
                trades.append(TradeEvent(exchange=self.exchange_name, symbol=symbol,
                    timestamp_ms=safe_int(item.get("time")) or int(time.time()*1000),
                    price=price, size=size, side=side, notional=price*size, source="rest", raw=item))
            return trades
        except: return []

    def fetch_top_trader_ratio(self, symbol, interval, limit):
        try:
            period = BINANCE_RATIO_PERIODS.get(interval,"5m")
            pos_data = self._get("/futures/data/topLongShortPositionRatio",
                                  {"symbol":symbol,"period":period,"limit":min(limit,500)})
            acc_data = self._get("/futures/data/topLongShortAccountRatio",
                                  {"symbol":symbol,"period":period,"limit":min(limit,500)})
            acc_map = {safe_int(i.get("timestamp")): safe_float(i.get("longShortRatio")) for i in acc_data}
            return [TopTraderRatio(
                        timestamp_ms=safe_int(i.get("timestamp")) or 0,
                        long_short_ratio=safe_float(i.get("longShortRatio")),
                        long_account_ratio=acc_map.get(safe_int(i.get("timestamp"))))
                    for i in pos_data]
        except: return []

    def fetch_global_long_short_ratio(self, symbol, interval, limit):
        try:
            period = BINANCE_RATIO_PERIODS.get(interval,"5m")
            data = self._get("/futures/data/globalLongShortAccountRatio",
                              {"symbol":symbol,"period":period,"limit":min(limit,500)})
            return [TopTraderRatio(timestamp_ms=safe_int(i.get("timestamp")) or 0,
                        global_ratio=safe_float(i.get("longShortRatio"))) for i in data]
        except: return []


    def fetch_long_short_account_count(self, symbol, interval, limit):
        """Binance 多空账户数比 (takerlongshortRatio 字段含隐式人数信息)"""
        try:
            period = BINANCE_RATIO_PERIODS.get(interval,'5m')
            # globalLongShortAccountRatio: longAccount / shortAccount
            data = self._get('/futures/data/globalLongShortAccountRatio',
                              {'symbol':symbol,'period':period,'limit':min(limit,500)})
            # topLongShortAccountRatio: top trader accounts
            top_data = self._get('/futures/data/topLongShortAccountRatio',
                              {'symbol':symbol,'period':period,'limit':min(limit,500)})
            top_map = {safe_int(i.get('timestamp')): i for i in top_data}
            results = []
            for i in data:
                ts = safe_int(i.get('timestamp')) or 0
                ratio = safe_float(i.get('longShortRatio'))
                # ratio = longAcc / shortAcc, so longPct = ratio/(1+ratio), shortPct=1/(1+ratio)
                long_pct = ratio / (1 + ratio) * 100 if ratio else None
                short_pct = 100 - long_pct if long_pct else None
                td = top_map.get(ts, {})
                top_ratio = safe_float(td.get('longShortRatio'))
                top_long_pct = top_ratio / (1 + top_ratio) * 100 if top_ratio else None
                top_short_pct = 100 - top_long_pct if top_long_pct else None
                results.append({'timestamp_ms': ts, 'global_long_pct': long_pct,
                    'global_short_pct': short_pct, 'top_long_pct': top_long_pct,
                    'top_short_pct': top_short_pct, 'global_ratio': ratio, 'top_ratio': top_ratio})
            return results
        except: return []

    def fetch_taker_long_short_ratio(self, symbol, interval, limit):
        """Binance Taker 买卖量比（主动买/主动卖）"""
        try:
            period = BINANCE_RATIO_PERIODS.get(interval,'5m')
            data = self._get('/futures/data/takerlongshortRatio',
                              {'symbol':symbol,'period':period,'limit':min(limit,500)})
            return [{'timestamp_ms': safe_int(i.get('timestamp')) or 0,
                     'buy_vol': safe_float(i.get('buyVol')),
                     'sell_vol': safe_float(i.get('sellVol')),
                     'ratio': safe_float(i.get('buySellRatio'))} for i in data]
        except: return []

    def fetch_spot_ticker(self, coin):
        try:
            sym = f"{coin.upper()}USDT"
            r = requests.get(f"{self.spot_url}/api/v3/ticker/24hr", params={"symbol":sym}, timeout=self.timeout)
            r.raise_for_status(); d = r.json()
            return (safe_float(d.get("lastPrice")), safe_float(d.get("quoteVolume")))
        except: return None

    def fetch_futures_oi_list(self, coin):
        """所有USDT合约（含到期日）的OI"""
        try:
            # Get all perp + futures tickers
            p = self._get("/fapi/v1/ticker/24hr")
            coin_upper = coin.upper()
            results = []
            spot = self.fetch_spot_ticker(coin)
            spot_price = spot[0] if spot else None
            for item in (p if isinstance(p, list) else []):
                sym = item.get("symbol","")
                if not sym.startswith(coin_upper): continue
                price = safe_float(item.get("lastPrice"))
                # Get OI for each symbol
                try:
                    oi_p = self._get("/fapi/v1/openInterest", {"symbol":sym})
                    oi = safe_float(oi_p.get("openInterest"))
                    oi_notional = oi * price if oi and price else None
                except: oi_notional = None
                basis_pct = None
                if spot_price and price:
                    basis_pct = (price - spot_price) / spot_price * 100
                expiry = "PERP" if sym == f"{coin_upper}USDT" else sym[len(coin_upper):]
                results.append(FuturesOIPoint(expiry=expiry, oi_notional=oi_notional,
                    price=price, basis_pct=basis_pct, exchange=self.exchange_name))
            return results
        except: return []


# ─── OKX ─────────────────────────────────────────────────────────────────────
class OkxClient(BaseClient):
    exchange_name = "OKX"
    base_url = "https://www.okx.com"

    def fetch(self, symbol):
        try:
            tp = self._get("/api/v5/market/ticker", {"instId":symbol})
            mp = self._get("/api/v5/public/mark-price", {"instType":"SWAP","instId":symbol})
            op = self._get("/api/v5/public/open-interest", {"instType":"SWAP","instId":symbol})
            fp = self._get("/api/v5/public/funding-rate", {"instId":symbol})
            ticker  = (tp.get("data") or [{}])[0]
            mark    = (mp.get("data") or [{}])[0]
            oi_item = (op.get("data") or [{}])[0]
            funding = (fp.get("data") or [{}])[0]
            oi      = safe_float(oi_item.get("oi"))
            oi_usd  = safe_float(oi_item.get("oiUsd"))
            mark_px = safe_float(mark.get("markPx"))
            if oi_usd is None and oi and mark_px:
                oi_usd = oi * mark_px
            return ExchangeSnapshot(
                exchange=self.exchange_name, symbol=symbol,
                last_price=safe_float(ticker.get("last")), mark_price=mark_px,
                open_interest=oi, open_interest_notional=oi_usd,
                funding_rate=safe_float(funding.get("fundingRate")),
                volume_24h_base=safe_float(ticker.get("vol24h")),
                volume_24h_notional=safe_float(ticker.get("volCcy24h")),
                timestamp_ms=safe_int(ticker.get("ts")),
                raw={"ticker":ticker,"mark_price":mark,"open_interest":oi_item,"funding_rate":funding})
        except Exception as e: return self._error(symbol, e)

    def fetch_candles(self, symbol, interval, limit):
        p = self._get("/api/v5/market/candles", {"instId":symbol,
            "bar":OKX_CANDLE_INTERVALS.get(interval,"5m"),"limit":min(limit,300)})
        return [Candle(timestamp_ms=safe_int(r[0]) or 0,
            open=safe_float(r[1]) or 0, high=safe_float(r[2]) or 0,
            low=safe_float(r[3]) or 0, close=safe_float(r[4]) or 0,
            volume=safe_float(r[5]) or 0)
            for r in reversed(p.get("data",[]))]

    def fetch_orderbook(self, symbol, limit):
        p = self._get("/api/v5/market/books", {"instId":symbol,"sz":min(limit,400)})
        data = (p.get("data") or [{}])[0]
        levels  = [OrderBookLevel(safe_float(pr) or 0, safe_float(sz) or 0, "bid") for pr,sz,*_ in data.get("bids",[])]
        levels += [OrderBookLevel(safe_float(pr) or 0, safe_float(sz) or 0, "ask") for pr,sz,*_ in data.get("asks",[])]
        return levels

    def fetch_recent_trades(self, symbol, limit):
        try:
            p = self._get("/api/v5/market/trades", {"instId":symbol,"limit":min(limit,500)})
            trades = []
            for item in p.get("data",[]):
                price = safe_float(item.get("px")) or 0
                size  = safe_float(item.get("sz")) or 0
                side  = "buy" if str(item.get("side","")).lower() == "buy" else "sell"
                trades.append(TradeEvent(exchange=self.exchange_name, symbol=symbol,
                    timestamp_ms=safe_int(item.get("ts")) or int(time.time()*1000),
                    price=price, size=size, side=side, notional=price*size, source="rest", raw=item))
            return trades
        except: return []

    def fetch_spot_ticker(self, coin):
        try:
            sym = f"{coin.upper()}-USDT"
            p = self._get("/api/v5/market/ticker", {"instId":sym})
            d = (p.get("data") or [{}])[0]
            return (safe_float(d.get("last")), safe_float(d.get("volCcy24h")))
        except: return None

    def fetch_futures_oi_list(self, coin):
        """OKX各到期日合约OI（SWAP + FUTURES）"""
        try:
            results = []
            spot = self.fetch_spot_ticker(coin)
            spot_price = spot[0] if spot else None
            coin_upper = coin.upper()
            for inst_type in ("SWAP", "FUTURES"):
                try:
                    p = self._get("/api/v5/public/open-interest", {"instType":inst_type})
                    for item in p.get("data",[]):
                        inst_id = item.get("instId","")
                        if not inst_id.startswith(coin_upper): continue
                        oi_usd = safe_float(item.get("oiUsd"))
                        # Get price
                        try:
                            tp = self._get("/api/v5/market/ticker", {"instId":inst_id})
                            price = safe_float((tp.get("data") or [{}])[0].get("last"))
                        except: price = None
                        basis_pct = None
                        if spot_price and price:
                            basis_pct = (price - spot_price) / spot_price * 100
                        expiry = "PERP" if inst_type == "SWAP" else inst_id.split("-")[-1]
                        results.append(FuturesOIPoint(expiry=expiry, oi_notional=oi_usd,
                            price=price, basis_pct=basis_pct, exchange=self.exchange_name))
                except: pass
            return results
        except: return []


# ─── Hyperliquid ──────────────────────────────────────────────────────────────
class HyperliquidClient(BaseClient):
    exchange_name = "Hyperliquid"
    base_url = "https://api.hyperliquid.xyz"

    def fetch(self, symbol):
        try:
            payload = self._post("/info", {"type":"metaAndAssetCtxs"})
            if not isinstance(payload, list) or len(payload) != 2:
                raise ValueError("unexpected response")
            universe = payload[0].get("universe", [])
            ctx_list = payload[1]
            idx = next((i for i,a in enumerate(universe) if a.get("name")==symbol), None)
            if idx is None: raise ValueError(f"{symbol} not found")
            ctx = ctx_list[idx]
            mark_px = safe_float(ctx.get("markPx"))
            oi      = safe_float(ctx.get("openInterest"))
            oi_notional = oi * mark_px if oi and mark_px else None
            return ExchangeSnapshot(
                exchange=self.exchange_name, symbol=symbol,
                last_price=safe_float(ctx.get("midPx")) or mark_px,
                mark_price=mark_px,
                index_price=safe_float(ctx.get("oraclePx")),
                open_interest=oi, open_interest_notional=oi_notional,
                funding_rate=safe_float(ctx.get("funding")),
                volume_24h_base=safe_float(ctx.get("dayBaseVlm")),
                volume_24h_notional=safe_float(ctx.get("dayNtlVlm")),
                timestamp_ms=int(time.time()*1000),
                raw={"asset_context":ctx})
        except Exception as e: return self._error(symbol, e)

    def fetch_candles(self, symbol, interval, limit):
        interval_ms = interval_to_millis(interval)
        end_time = int(time.time()*1000)
        start_time = max(0, end_time - interval_ms * (limit+10))
        payload = self._post("/info", {"type":"candleSnapshot","req":{
            "coin":symbol,"interval":HL_CANDLE_INTERVALS.get(interval,"5m"),
            "startTime":start_time,"endTime":end_time}})
        return [Candle(timestamp_ms=safe_int(r.get("t")) or 0,
            open=safe_float(r.get("o")) or 0, high=safe_float(r.get("h")) or 0,
            low=safe_float(r.get("l")) or 0, close=safe_float(r.get("c")) or 0,
            volume=safe_float(r.get("v")) or 0) for r in payload[-limit:]]

    def fetch_orderbook(self, symbol, limit):
        payload = self._post("/info", {"type":"l2Book","coin":symbol})
        levels_data = payload.get("levels", [[],[]])
        levels  = [OrderBookLevel(safe_float(r.get("px")) or 0, safe_float(r.get("sz")) or 0, "bid") for r in levels_data[0][:limit]]
        levels += [OrderBookLevel(safe_float(r.get("px")) or 0, safe_float(r.get("sz")) or 0, "ask") for r in levels_data[1][:limit]]
        return levels


# ─── Factory ─────────────────────────────────────────────────────────────────
def build_clients(timeout: int = DEFAULT_TIMEOUT) -> Dict[str, BaseClient]:
    return {"bybit":BybitClient(timeout),"binance":BinanceClient(timeout),
            "okx":OkxClient(timeout),"hyperliquid":HyperliquidClient(timeout)}

def fetch_exchange_candles(ek, symbol, interval, limit, timeout=DEFAULT_TIMEOUT):
    return build_clients(timeout)[ek].fetch_candles(symbol, interval, limit)

def fetch_exchange_orderbook(ek, symbol, limit, timeout=DEFAULT_TIMEOUT):
    return build_clients(timeout)[ek].fetch_orderbook(symbol, limit)

def fetch_exchange_oi_history(ek, symbol, interval, limit, timeout=DEFAULT_TIMEOUT):
    return build_clients(timeout)[ek].fetch_open_interest_history(symbol, interval, limit)

def fetch_exchange_liquidations(ek, symbol, limit, timeout=DEFAULT_TIMEOUT):
    return build_clients(timeout)[ek].fetch_liquidations(symbol, limit)

def fetch_exchange_recent_trades(ek, symbol, limit, timeout=DEFAULT_TIMEOUT):
    return build_clients(timeout)[ek].fetch_recent_trades(symbol, limit)

def fetch_exchange_top_trader_ratio(ek, symbol, interval, limit, timeout=DEFAULT_TIMEOUT):
    return build_clients(timeout)[ek].fetch_top_trader_ratio(symbol, interval, limit)

def fetch_exchange_global_long_short_ratio(ek, symbol, interval, limit, timeout=DEFAULT_TIMEOUT):
    return build_clients(timeout)[ek].fetch_global_long_short_ratio(symbol, interval, limit)

def fetch_exchange_spot_ticker(ek, coin, timeout=DEFAULT_TIMEOUT):
    return build_clients(timeout)[ek].fetch_spot_ticker(coin)

def fetch_exchange_futures_oi_list(ek, coin, timeout=DEFAULT_TIMEOUT):
    return build_clients(timeout)[ek].fetch_futures_oi_list(coin)

def fetch_binance_long_short_count(symbol, interval, limit, timeout=DEFAULT_TIMEOUT):
    return BinanceClient(timeout).fetch_long_short_account_count(symbol, interval, limit)

def fetch_binance_taker_ratio(symbol, interval, limit, timeout=DEFAULT_TIMEOUT):
    return BinanceClient(timeout).fetch_taker_long_short_ratio(symbol, interval, limit)


# ══════════════════════════════════════════════════════════════════════════════
# v5 — 全市场批量扫描
# ══════════════════════════════════════════════════════════════════════════════

MARKET_SCAN_COINS = [
    "BTC","ETH","SOL","XRP","BNB","DOGE","ADA","SUI","AVAX",
    "LINK","LTC","TON","HYPE","TAO","PEPE","WIF","TRUMP","FARTCOIN",
    "PENDLE","OP","ARB","TIA","INJ","SEI","APT","NEAR","FIL","ATOM",
]

SPOT_EXCHANGES_EXTRA = {
    "binance_spot": "https://api.binance.com",
    "okx_spot":     "https://www.okx.com",
    "bybit_spot":   "https://api.bybit.com",
}


class MarketScanClient(BaseClient):
    """全市场批量扫描 — 使用 Binance 公开端点，无需 API Key"""
    exchange_name = "MarketScan"
    base_url      = "https://fapi.binance.com"
    spot_base     = "https://api.binance.com"

    def fetch_all_tickers(self) -> Dict[str, dict]:
        """获取所有永续合约 ticker（批量，一次请求）"""
        try:
            data = self._get("/fapi/v1/ticker/24hr")
            if isinstance(data, list):
                return {d["symbol"]: d for d in data}
            return {}
        except: return {}

    def fetch_all_oi(self) -> Dict[str, float]:
        """获取所有永续合约当前OI（逐个取，仅取 USDT 合约）"""
        results = {}
        try:
            tickers = self._get("/fapi/v1/ticker/24hr")
            for t in (tickers if isinstance(tickers, list) else []):
                sym = t.get("symbol", "")
                if not sym.endswith("USDT"): continue
                price = safe_float(t.get("lastPrice"))
                try:
                    oi_r = self._get("/fapi/v1/openInterest", {"symbol": sym})
                    oi   = safe_float(oi_r.get("openInterest"))
                    if oi and price: results[sym] = oi * price
                except: pass
        except: pass
        return results

    def fetch_coin_summary(self, coin: str) -> dict:
        """单币种综合摘要：price, oi, funding, 24h vol, 24h liq"""
        sym = f"{coin.upper()}USDT"
        result = {"coin": coin, "symbol": sym}
        try:
            ticker = self._get("/fapi/v1/ticker/24hr", {"symbol": sym})
            result["price"]       = safe_float(ticker.get("lastPrice"))
            result["price_chg_pct"] = safe_float(ticker.get("priceChangePercent"))
            result["vol_24h"]     = safe_float(ticker.get("quoteVolume"))
            result["high_24h"]    = safe_float(ticker.get("highPrice"))
            result["low_24h"]     = safe_float(ticker.get("lowPrice"))
        except: pass
        try:
            prem = self._get("/fapi/v1/premiumIndex", {"symbol": sym})
            result["funding"]    = safe_float(prem.get("lastFundingRate"))
            result["mark_price"] = safe_float(prem.get("markPrice"))
            result["index_price"]= safe_float(prem.get("indexPrice"))
        except: pass
        try:
            oi_r  = self._get("/fapi/v1/openInterest", {"symbol": sym})
            oi    = safe_float(oi_r.get("openInterest"))
            price = result.get("price") or result.get("mark_price")
            result["oi"] = oi * price if oi and price else None
        except: pass
        try:
            liq_r = self._get("/fapi/v1/allForceOrders", {"symbol": sym, "limit": 100})
            items = liq_r if isinstance(liq_r, list) else []
            now_ms = int(time.time() * 1000)
            cutoff = now_ms - 86_400_000
            long_n = short_n = 0.0
            for item in items:
                ts = safe_int(item.get("time")) or 0
                if ts < cutoff: continue
                price = safe_float(item.get("avgPrice")) or safe_float(item.get("price")) or 0
                qty   = safe_float(item.get("executedQty")) or safe_float(item.get("origQty")) or 0
                n     = price * qty
                side  = str(item.get("side","")).upper()
                if side == "SELL": long_n  += n   # long liquidated = sell order
                else:              short_n += n
            result["liq_long_24h"]  = long_n
            result["liq_short_24h"] = short_n
            result["liq_total_24h"] = long_n + short_n
        except: pass
        try:
            gl_r = self._get("/futures/data/globalLongShortAccountRatio",
                             {"symbol": sym, "period": "5m", "limit": 1})
            if gl_r:
                result["ls_ratio"] = safe_float(gl_r[-1].get("longShortRatio"))
        except: pass
        try:
            spot_r = requests.get(f"{self.spot_base}/api/v3/ticker/24hr",
                                  params={"symbol": sym}, timeout=self.timeout)
            spot_r.raise_for_status()
            sd = spot_r.json()
            result["spot_vol_24h"] = safe_float(sd.get("quoteVolume"))
        except: pass
        # OI history for 1h change
        try:
            oi_hist = self._get("/futures/data/openInterestHist",
                                {"symbol": sym, "period": "1h", "limit": 25})
            if len(oi_hist) >= 2:
                v_now  = safe_float(oi_hist[-1].get("sumOpenInterestValue")) or 0
                v_1h   = safe_float(oi_hist[-2].get("sumOpenInterestValue")) or 0
                v_24h  = safe_float(oi_hist[0].get("sumOpenInterestValue"))  or 0
                result["oi_change_1h_pct"]  = (v_now - v_1h)  / v_1h  * 100 if v_1h  else None
                result["oi_change_24h_pct"] = (v_now - v_24h) / v_24h * 100 if v_24h else None
        except: pass
        return result

    def fetch_market_batch(self, coins: List[str], max_workers: int = 8) -> List[dict]:
        """并发批量扫描多个币种"""
        from concurrent.futures import ThreadPoolExecutor, as_completed
        results = []
        with ThreadPoolExecutor(max_workers=max_workers) as exe:
            futs = {exe.submit(self.fetch_coin_summary, c): c for c in coins}
            for fut in as_completed(futs):
                try: results.append(fut.result())
                except: pass
        results.sort(key=lambda x: x.get("oi") or 0, reverse=True)
        return results


def build_market_scan_client(timeout: int = DEFAULT_TIMEOUT) -> MarketScanClient:
    return MarketScanClient(timeout)
