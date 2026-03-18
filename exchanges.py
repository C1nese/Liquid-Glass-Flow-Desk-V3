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

    def fetch_open_interest_history(self, symbol: str, interval: str, limit: int):
        """OKX OI历史（每隔一段时间的快照）"""
        try:
            okx_bar = {"1m":"1m","3m":"3m","5m":"5m","15m":"15m","30m":"30m",
                       "1h":"1H","4h":"4H","1d":"1Dutc"}.get(interval,"5m")
            p = self._get("/api/v5/rubik/stat/contracts/open-interest-history",
                          {"instId":symbol,"period":okx_bar,"limit":str(min(limit,100))})
            results = []
            for item in (p.get("data") or []):
                ts  = safe_int(item[0]) if item else None
                oi  = safe_float(item[1]) if len(item) > 1 else None
                oi_usd = safe_float(item[2]) if len(item) > 2 else None
                if ts:
                    results.append(OIPoint(timestamp_ms=ts,
                                           open_interest=oi,
                                           open_interest_notional=oi_usd))
            return sorted(results, key=lambda x: x.timestamp_ms)[-limit:]
        except:
            return []

    def fetch_liquidations(self, symbol: str, limit: int):
        """OKX 强平记录"""
        try:
            # OKX liquidation orders endpoint
            p = self._get("/api/v5/public/liquidation-orders",
                          {"instType":"SWAP","instId":symbol,"state":"filled","limit":str(min(limit,100))})
            events = []
            now_ms = int(time.time() * 1000)
            for item in (p.get("data") or []):
                for detail in (item.get("details") or []):
                    side_raw = str(detail.get("side","")).lower()
                    # In OKX: liquidation "buy" means short position was liquidated
                    side = "short" if side_raw == "buy" else "long"
                    price = safe_float(detail.get("bkPx"))
                    size  = safe_float(detail.get("sz"))
                    ts    = safe_int(detail.get("ts")) or now_ms
                    notional = compute_notional(price, size)
                    events.append(LiquidationEvent(
                        exchange=self.exchange_name, symbol=symbol,
                        timestamp_ms=ts, side=side,
                        price=price, size=size, notional=notional,
                        source="rest", raw=detail))
            return sorted(events, key=lambda x: x.timestamp_ms, reverse=True)[:limit]
        except:
            return []

    def fetch_top_trader_ratio(self, symbol: str, interval: str, limit: int):
        """OKX 大户多空持仓比"""
        try:
            okx_period = {"1m":"5m","3m":"5m","5m":"5m","15m":"15m","30m":"30m",
                          "1h":"1H","4h":"4H","1d":"1Dutc"}.get(interval,"5m")
            # Use coin from symbol like BTC-USDT-SWAP -> BTC
            coin = symbol.split("-")[0]
            p = self._get("/api/v5/rubik/stat/contracts/long-short-account-ratio",
                          {"ccy":coin,"period":okx_period,"limit":str(min(limit,100))})
            results = []
            for item in (p.get("data") or []):
                ts    = safe_int(item[0]) if item else None
                ratio = safe_float(item[1]) if len(item) > 1 else None
                if ts and ratio:
                    results.append(TopTraderRatio(
                        timestamp_ms=ts,
                        long_short_ratio=ratio,
                        long_account_ratio=ratio / (1 + ratio) * 100 if ratio else None,
                    ))
            return sorted(results, key=lambda x: x.timestamp_ms)[-limit:]
        except:
            return []

    def fetch_global_long_short_ratio(self, symbol: str, interval: str, limit: int):
        """OKX 全市场多空比（等同于 top trader ratio，OKX API 相同）"""
        return self.fetch_top_trader_ratio(symbol, interval, limit)

    def fetch_taker_long_short_ratio_okx(self, symbol: str, interval: str, limit: int):
        """OKX 吃单买卖比例"""
        try:
            okx_period = {"1m":"5m","3m":"5m","5m":"5m","15m":"15m","30m":"30m",
                          "1h":"1H","4h":"4H","1d":"1Dutc"}.get(interval,"5m")
            coin = symbol.split("-")[0]
            p = self._get("/api/v5/rubik/stat/taker-volume",
                          {"ccy":coin,"instType":"CONTRACTS","period":okx_period,"limit":str(min(limit,100))})
            return p.get("data", [])
        except:
            return []


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


# ══════════════════════════════════════════════════════════════════════════════
# v6 增强 — HyperliquidClient 扩展 & 跨所工具函数
# ══════════════════════════════════════════════════════════════════════════════

class HyperliquidClientV2(HyperliquidClient):
    """扩展版 Hyperliquid 客户端，覆盖所有 /info 端点"""

    def fetch_all_fundings(self) -> Dict[str, float]:
        """返回所有资产的当前资金费率 {coin: rate}"""
        try:
            payload = self._post("/info", {"type": "metaAndAssetCtxs"})
            if not isinstance(payload, list) or len(payload) < 2:
                return {}
            universe = payload[0].get("universe", [])
            ctx_list = payload[1]
            result = {}
            for i, asset in enumerate(universe):
                coin = asset.get("name", "")
                if i < len(ctx_list):
                    fr = safe_float(ctx_list[i].get("funding"))
                    if fr is not None:
                        result[coin] = fr
            return result
        except:
            return {}

    def fetch_all_mids(self) -> Dict[str, float]:
        """返回所有资产的最新中间价 {coin: mid_price}"""
        try:
            raw = self._post("/info", {"type": "allMids"})
            if isinstance(raw, dict):
                return {k: safe_float(v) for k, v in raw.items() if safe_float(v)}
            return {}
        except:
            return {}

    def fetch_open_interest_all(self) -> Dict[str, float]:
        """返回所有资产的持仓量（以币计）"""
        try:
            payload = self._post("/info", {"type": "metaAndAssetCtxs"})
            if not isinstance(payload, list) or len(payload) < 2:
                return {}
            universe = payload[0].get("universe", [])
            ctx_list = payload[1]
            mids = self.fetch_all_mids()
            result = {}
            for i, asset in enumerate(universe):
                coin = asset.get("name", "")
                if i < len(ctx_list):
                    oi    = safe_float(ctx_list[i].get("openInterest"))
                    price = mids.get(coin)
                    if oi and price:
                        result[coin] = oi * price  # notional
            return result
        except:
            return {}

    def fetch_recent_trades(self, symbol: str, limit: int = 100):
        """获取最近成交（Hyperliquid trades endpoint）"""
        try:
            raw = self._post("/info", {"type": "recentTrades", "coin": symbol})
            trades = []
            now_ms = int(time.time() * 1000)
            for t in (raw or [])[-limit:]:
                side = "buy" if str(t.get("side", "")).upper() in ("B", "BUY") else "sell"
                price = safe_float(t.get("px")) or 0.0
                size  = safe_float(t.get("sz")) or 0.0
                trades.append(TradeEvent(
                    exchange=self.exchange_name, symbol=symbol,
                    timestamp_ms=safe_int(t.get("time")) or now_ms,
                    price=price, size=size, side=side,
                    notional=price * size,
                    source="rest",
                ))
            return trades
        except:
            return []

    def fetch_open_interest_history(self, symbol: str, interval: str, limit: int):
        """HL 目前没有直接的 OI 历史 REST，用 metaAndAssetCtxs 生成单点"""
        snap = self.fetch(symbol)
        if snap and snap.open_interest_notional:
            return [OIPoint(
                timestamp_ms=snap.timestamp_ms or int(time.time() * 1000),
                open_interest=snap.open_interest,
                open_interest_notional=snap.open_interest_notional,
            )]
        return []

    def fetch_funding_history_hl(self, symbol: str, limit: int = 100) -> List[Dict]:
        """获取 Hyperliquid 资金费率历史"""
        try:
            end_time   = int(time.time() * 1000)
            start_time = end_time - 86400000 * 7  # 7天
            raw = self._post("/info", {
                "type": "fundingHistory",
                "coin": symbol,
                "startTime": start_time,
                "endTime": end_time,
            })
            return [
                {
                    "timestamp_ms": safe_int(r.get("time")) or 0,
                    "funding_rate": safe_float(r.get("fundingRate")),
                    "premium": safe_float(r.get("premium")),
                }
                for r in (raw or [])[-limit:]
            ]
        except:
            return []


def build_clients_v2(timeout: int = DEFAULT_TIMEOUT) -> Dict[str, BaseClient]:
    """v6 客户端工厂，Hyperliquid 使用扩展版"""
    return {
        "bybit":       BybitClient(timeout),
        "binance":     BinanceClient(timeout),
        "okx":         OkxClient(timeout),
        "hyperliquid": HyperliquidClientV2(timeout),
    }


def fetch_all_exchange_fundings(coin: str,
                                 timeout: int = DEFAULT_TIMEOUT
                                 ) -> Dict[str, Optional[float]]:
    """并发获取 4 所的当前资金费率，返回 {exchange: rate}"""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    clients = build_clients_v2(timeout)
    syms    = default_symbols(coin)
    results: Dict[str, Optional[float]] = {}

    def _fetch(ek):
        try:
            snap = clients[ek].fetch(syms[ek])
            return ek, snap.funding_rate if snap else None
        except:
            return ek, None

    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(_fetch, ek): ek for ek in EXCHANGE_ORDER}
        for fut in as_completed(futs):
            ek, rate = fut.result()
            results[ek] = rate

    return results


def fetch_aggregated_oi(coin: str, timeout: int = DEFAULT_TIMEOUT
                         ) -> Dict[str, Optional[float]]:
    """并发获取4所 OI，返回 {exchange: notional}"""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    clients = build_clients_v2(timeout)
    syms    = default_symbols(coin)
    results: Dict[str, Optional[float]] = {}

    def _fetch(ek):
        try:
            snap = clients[ek].fetch(syms[ek])
            return ek, snap.open_interest_notional if snap else None
        except:
            return ek, None

    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(_fetch, ek): ek for ek in EXCHANGE_ORDER}
        for fut in as_completed(futs):
            ek, oi = fut.result()
            results[ek] = oi

    return results


# ══════════════════════════════════════════════════════════════════════════════
# v8 — 方向1：合约情绪真值层  (Binance 4端点并发 + Bybit Taker)
# ══════════════════════════════════════════════════════════════════════════════

def fetch_contract_sentiment_point(
    binance_symbol: str,
    bybit_symbol: str,
    interval: str = "5m",
    timeout: int = DEFAULT_TIMEOUT,
) -> "ContractSentimentPoint":
    """
    并发拉取 Binance 4 端点 + Bybit buyRatio，用 timestamp 对齐返回一个
    ContractSentimentPoint。OKX 标"暂不支持"，HL 标"无全市场数据"。
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from models import ContractSentimentPoint
    import time as _time

    binance = BinanceClient(timeout)
    bybit   = BybitClient(timeout)
    period  = BINANCE_RATIO_PERIODS.get(interval, "5m")

    results: Dict[str, Any] = {}

    def _binance_global():
        try:
            data = binance._get("/futures/data/globalLongShortAccountRatio",
                                {"symbol": binance_symbol, "period": period, "limit": 1})
            return "binance_global", data[-1] if data else None
        except Exception as e:
            return "binance_global", None

    def _binance_top_account():
        try:
            data = binance._get("/futures/data/topLongShortAccountRatio",
                                {"symbol": binance_symbol, "period": period, "limit": 1})
            return "binance_top_acc", data[-1] if data else None
        except:
            return "binance_top_acc", None

    def _binance_top_position():
        try:
            data = binance._get("/futures/data/topLongShortPositionRatio",
                                {"symbol": binance_symbol, "period": period, "limit": 1})
            return "binance_top_pos", data[-1] if data else None
        except:
            return "binance_top_pos", None

    def _binance_taker():
        try:
            data = binance._get("/futures/data/takerlongshortRatio",
                                {"symbol": binance_symbol, "period": period, "limit": 1})
            return "binance_taker", data[-1] if data else None
        except:
            return "binance_taker", None

    def _bybit_taker():
        try:
            data = bybit._get("/v5/market/account-ratio",
                              {"category": "linear", "symbol": bybit_symbol,
                               "period": BINANCE_RATIO_PERIODS.get(interval, "5m"), "limit": 1})
            items = data.get("result", {}).get("list", [])
            return "bybit_taker", items[0] if items else None
        except:
            return "bybit_taker", None

    tasks = [_binance_global, _binance_top_account, _binance_top_position,
             _binance_taker, _bybit_taker]

    with ThreadPoolExecutor(max_workers=5) as exe:
        futs = [exe.submit(t) for t in tasks]
        for fut in as_completed(futs):
            try:
                k, v = fut.result()
                results[k] = v
            except Exception:
                pass

    now_ms = int(_time.time() * 1000)
    confirmed = []

    # --- Binance global ---
    g = results.get("binance_global") or {}
    g_ratio = safe_float(g.get("longShortRatio"))
    g_long_pct = g_ratio / (1 + g_ratio) * 100 if g_ratio else None
    g_short_pct = (100 - g_long_pct) if g_long_pct is not None else None
    if g_ratio is not None:
        confirmed.append("binance_global_account")

    # --- Binance top account ---
    ta = results.get("binance_top_acc") or {}
    ta_ratio = safe_float(ta.get("longShortRatio"))
    ta_long_pct = ta_ratio / (1 + ta_ratio) * 100 if ta_ratio else None
    ta_short_pct = (100 - ta_long_pct) if ta_long_pct is not None else None
    if ta_ratio is not None:
        confirmed.append("binance_top_account")

    # --- Binance top position ---
    tp = results.get("binance_top_pos") or {}
    tp_ratio = safe_float(tp.get("longShortRatio"))
    tp_long_pct = tp_ratio / (1 + tp_ratio) * 100 if tp_ratio else None
    tp_short_pct = (100 - tp_long_pct) if tp_long_pct is not None else None
    if tp_ratio is not None:
        confirmed.append("binance_top_position")

    # --- Binance taker ---
    tk = results.get("binance_taker") or {}
    tk_buy = safe_float(tk.get("buyVol"))
    tk_sell = safe_float(tk.get("sellVol"))
    tk_ratio = safe_float(tk.get("buySellRatio"))
    tk_buy_pct = tk_buy / (tk_buy + tk_sell) if tk_buy and tk_sell and (tk_buy + tk_sell) > 0 else None
    tk_sell_pct = (1 - tk_buy_pct) if tk_buy_pct is not None else None
    if tk_ratio is not None:
        confirmed.append("binance_taker")

    # --- Bybit taker ---
    byt = results.get("bybit_taker") or {}
    byt_buy = safe_float(byt.get("buyRatio"))
    if byt_buy is not None:
        confirmed.append("bybit_taker")

    return ContractSentimentPoint(
        timestamp_ms=now_ms,
        binance_global_long_pct=g_long_pct,
        binance_global_short_pct=g_short_pct,
        binance_global_ratio=g_ratio,
        binance_top_account_long_pct=ta_long_pct,
        binance_top_account_short_pct=ta_short_pct,
        binance_top_account_ratio=ta_ratio,
        binance_top_position_long_pct=tp_long_pct,
        binance_top_position_short_pct=tp_short_pct,
        binance_top_position_ratio=tp_ratio,
        binance_taker_buy_ratio=tk_buy_pct,
        binance_taker_sell_ratio=tk_sell_pct,
        binance_taker_buy_vol=tk_buy,
        binance_taker_sell_vol=tk_sell,
        bybit_taker_buy_ratio=byt_buy,
        okx_supported=False,     # OKX 暂不支持
        hl_supported=False,      # HL 无全市场数据
        confirmed_sources=confirmed,
        unconfirmed_sources=["okx", "hyperliquid"],
    )


# ── v8 方向4：拆单检测 ────────────────────────────────────────────────────────

def detect_split_orders(
    trades: List[Any],
    window_ms: int = 30_000,
    price_tolerance_pct: float = 0.001,
    min_count: int = 3,
    min_notional_each: float = 10_000,
) -> List["SplitOrderCluster"]:
    """
    在 trades 列表中检测拆单：30s 内同价位 ±0.1% 连续 ≥3 笔大单
    trades 需要有 .timestamp_ms / .price / .notional / .side 属性
    """
    from models import SplitOrderCluster
    import uuid as _uuid

    if not trades:
        return []

    sorted_trades = sorted(trades, key=lambda t: t.timestamp_ms)
    clusters: List[SplitOrderCluster] = []

    i = 0
    while i < len(sorted_trades):
        anchor = sorted_trades[i]
        if (anchor.notional or 0) < min_notional_each:
            i += 1
            continue

        group = [anchor]
        j = i + 1
        while j < len(sorted_trades):
            t = sorted_trades[j]
            if t.timestamp_ms - anchor.timestamp_ms > window_ms:
                break
            if t.side != anchor.side:
                j += 1
                continue
            if (t.notional or 0) < min_notional_each:
                j += 1
                continue
            price_diff_pct = abs(t.price - anchor.price) / anchor.price
            if price_diff_pct <= price_tolerance_pct:
                group.append(t)
            j += 1

        if len(group) >= min_count:
            prices = [t.price for t in group]
            cluster_id = str(_uuid.uuid4())[:8]
            clusters.append(SplitOrderCluster(
                cluster_id=cluster_id,
                exchange=getattr(anchor, "exchange", "unknown"),
                market_type=getattr(anchor, "market_type", "unknown"),
                side=anchor.side,
                price_center=sum(prices) / len(prices),
                price_range_pct=(max(prices) - min(prices)) / anchor.price * 100,
                first_ms=group[0].timestamp_ms,
                last_ms=group[-1].timestamp_ms,
                order_count=len(group),
                total_notional=sum(t.notional or 0 for t in group),
                avg_interval_ms=(group[-1].timestamp_ms - group[0].timestamp_ms) / max(len(group) - 1, 1),
            ))
            i = j
        else:
            i += 1

    return clusters


# ── v8 方向6：风险板数据聚合 ──────────────────────────────────────────────────

def build_risk_radar_point(
    coin: str,
    snapshots: List[Any],
    oi_delta_pts: List[Any],
    liq_events: List[Any],
    sentiment_pt: Optional[Any] = None,
    hl_meta: Optional[Dict[str, Any]] = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> "RiskRadarPoint":
    """
    从现有数据聚合六维风险雷达图数据点。
    HL 贡献三个独占维度：predictedFundings / perpsAtOpenInterestCap / markPx vs oraclePx
    """
    from models import RiskRadarPoint
    import time as _time

    now_ms = int(_time.time() * 1000)
    ok_snaps = [s for s in snapshots if getattr(s, "status", "error") == "ok"]
    if not ok_snaps:
        return RiskRadarPoint(timestamp_ms=now_ms, coin=coin)

    # ── 1. Funding风险 ────────────────────────────────────────────────────────
    rates = [s.funding_rate for s in ok_snaps if s.funding_rate is not None]
    if rates:
        avg_rate = sum(rates) / len(rates)
        # 极端正费率(>0.01%)=多头拥挤=风险高; 极端负费率=空头拥挤
        funding_risk = min(1.0, max(-1.0, avg_rate * 10_000 / 10.0))
    else:
        funding_risk = 0.0

    # ── 2. 基差风险 ────────────────────────────────────────────────────────────
    basis_vals = []
    for s in ok_snaps:
        if hasattr(s, "spot_perp_spread_bps") and s.spot_perp_spread_bps is not None:
            basis_vals.append(abs(s.spot_perp_spread_bps))
    if basis_vals:
        avg_basis = sum(basis_vals) / len(basis_vals)
        basis_risk = min(1.0, avg_basis / 30.0)   # 30bps = 满风险
    else:
        basis_risk = 0.0

    # ── 3. OI压力 ──────────────────────────────────────────────────────────────
    if oi_delta_pts and len(oi_delta_pts) >= 5:
        recent = oi_delta_pts[-10:]
        velocities = [getattr(p, "oi_velocity", 0) for p in recent]
        avg_vel = sum(velocities) / len(velocities)
        oi_pressure = min(1.0, max(-1.0, avg_vel / 1e7))
    else:
        oi_pressure = 0.0

    # ── 4. 清算密度 ────────────────────────────────────────────────────────────
    if liq_events:
        recent_ms = now_ms - 3_600_000  # 1h
        recent_liqs = [e for e in liq_events if getattr(e, "timestamp_ms", 0) > recent_ms]
        total_notional = sum(getattr(e, "notional", 0) or 0 for e in recent_liqs)
        liq_density = min(1.0, total_notional / 50_000_000)  # 5000w=满风险
    else:
        liq_density = 0.0

    # ── 5. ADL/保险基金风险（用OI集中度代理）──────────────────────────────────
    oi_vals = [s.open_interest_notional for s in ok_snaps if s.open_interest_notional]
    if oi_vals and len(oi_vals) > 1:
        mx = max(oi_vals); total = sum(oi_vals)
        adl_insurance_risk = min(1.0, (mx / total - 0.25) * 4) if total > 0 else 0.0
    else:
        adl_insurance_risk = 0.0

    # ── 6. HL资产ctx风险（HL独占）─────────────────────────────────────────────
    hl_risk = 0.0
    hl_predicted_funding = None
    hl_at_oi_cap = None
    hl_mark_oracle_dev = None

    if hl_meta:
        # predictedFundings
        pf = hl_meta.get("predicted_funding")
        if pf is not None:
            hl_predicted_funding = float(pf) * 10_000
            hl_risk += min(0.4, abs(hl_predicted_funding) / 20.0)

        # perpsAtOpenInterestCap
        at_cap = hl_meta.get("at_oi_cap", False)
        hl_at_oi_cap = bool(at_cap)
        if hl_at_oi_cap:
            hl_risk += 0.35

        # markPx vs oraclePx 偏差
        mark_px = hl_meta.get("mark_px")
        oracle_px = hl_meta.get("oracle_px")
        if mark_px and oracle_px and oracle_px > 0:
            dev_pct = abs(mark_px - oracle_px) / oracle_px * 100
            hl_mark_oracle_dev = dev_pct
            hl_risk += min(0.25, dev_pct / 1.0)

        hl_risk = min(1.0, hl_risk)

    pt = RiskRadarPoint(
        timestamp_ms=now_ms, coin=coin,
        funding_risk=round(funding_risk, 4),
        basis_risk=round(basis_risk, 4),
        oi_pressure=round(oi_pressure, 4),
        liq_density=round(liq_density, 4),
        adl_insurance_risk=round(adl_insurance_risk, 4),
        hl_asset_ctx_risk=round(hl_risk, 4),
        hl_predicted_funding_bps=hl_predicted_funding,
        hl_perps_at_oi_cap=hl_at_oi_cap,
        hl_mark_oracle_deviation_pct=hl_mark_oracle_dev,
    )
    pt.compute_composite()
    return pt
