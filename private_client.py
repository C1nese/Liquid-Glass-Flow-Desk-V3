"""
private_client.py — v8 方向5：私有持仓查询

安全规则（严格执行）：
1. API Key 只存 session_state，绝不写入数据库或任何持久化存储
2. 强制只读 GET 方法，不执行任何 POST/PUT/DELETE 操作
3. 所有方法名带 _readonly 后缀，明确语义
4. UI 层有显著安全标注

支持交易所：Binance / Bybit / OKX（只读仓位查询）
"""
from __future__ import annotations

import hashlib
import hmac
import time
import urllib.parse
from typing import Any, Dict, List, Optional

import requests


DEFAULT_TIMEOUT = 10


def _safe_float(v) -> Optional[float]:
    if v in (None, ""): return None
    try: return float(v)
    except: return None


# ── Binance 只读持仓 ────────────────────────────────────────────────────────────

class BinanceReadOnlyClient:
    BASE_URL = "https://fapi.binance.com"

    def __init__(self, api_key: str, api_secret: str, timeout: int = DEFAULT_TIMEOUT):
        self.api_key = api_key
        self.api_secret = api_secret
        self.timeout = timeout
        self._session = requests.Session()
        self._session.headers.update({
            "X-MBX-APIKEY": api_key,
            "User-Agent": "liquidity-terminal-readonly/8.0",
        })

    def _sign(self, params: dict) -> dict:
        params["timestamp"] = int(time.time() * 1000)
        qs = urllib.parse.urlencode(params)
        sig = hmac.new(self.api_secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
        params["signature"] = sig
        return params

    def _get(self, path: str, params: Optional[dict] = None) -> Any:
        """强制只读 GET — 不允许 POST/PUT/DELETE"""
        params = self._sign(params or {})
        r = self._session.get(self.BASE_URL + path, params=params, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def fetch_positions_readonly(self) -> List[Dict[str, Any]]:
        """只读获取 Binance U本位合约持仓"""
        data = self._get("/fapi/v2/positionRisk")
        positions = []
        for item in (data if isinstance(data, list) else []):
            size = _safe_float(item.get("positionAmt"))
            if not size or size == 0:
                continue
            side = "long" if size > 0 else "short"
            positions.append({
                "币种": item.get("symbol", ""),
                "方向": side,
                "数量": abs(size),
                "名义价值": _safe_float(item.get("notional")),
                "开仓价": _safe_float(item.get("entryPrice")),
                "标记价": _safe_float(item.get("markPrice")),
                "未实现盈亏": _safe_float(item.get("unRealizedProfit")),
                "杠杆": _safe_float(item.get("leverage")),
                "清算价": _safe_float(item.get("liquidationPrice")),
                "数据源": "Binance FAPI (只读)",
            })
        return positions


# ── Bybit 只读持仓 ──────────────────────────────────────────────────────────────

class BybitReadOnlyClient:
    BASE_URL = "https://api.bybit.com"

    def __init__(self, api_key: str, api_secret: str, timeout: int = DEFAULT_TIMEOUT):
        self.api_key = api_key
        self.api_secret = api_secret
        self.timeout = timeout
        self._session = requests.Session()

    def _sign(self, params: dict) -> tuple:
        ts = str(int(time.time() * 1000))
        recv = "5000"
        param_str = ts + self.api_key + recv + urllib.parse.urlencode(
            sorted(params.items()))
        sig = hmac.new(self.api_secret.encode(), param_str.encode(), hashlib.sha256).hexdigest()
        return ts, sig, recv

    def _get(self, path: str, params: Optional[dict] = None) -> Any:
        """强制只读 GET"""
        params = params or {}
        ts, sig, recv = self._sign(params)
        headers = {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-TIMESTAMP": ts,
            "X-BAPI-SIGN": sig,
            "X-BAPI-RECV-WINDOW": recv,
        }
        r = self._session.get(
            self.BASE_URL + path, params=params, headers=headers, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def fetch_positions_readonly(self) -> List[Dict[str, Any]]:
        """只读获取 Bybit USDT永续持仓"""
        data = self._get("/v5/position/list", {"category": "linear", "settleCoin": "USDT"})
        positions = []
        for item in (data.get("result", {}).get("list") or []):
            size = _safe_float(item.get("size"))
            if not size or size == 0:
                continue
            side_raw = str(item.get("side", "")).lower()
            side = "long" if side_raw == "buy" else "short"
            positions.append({
                "币种": item.get("symbol", ""),
                "方向": side,
                "数量": size,
                "名义价值": _safe_float(item.get("positionValue")),
                "开仓价": _safe_float(item.get("avgPrice")),
                "标记价": _safe_float(item.get("markPrice")),
                "未实现盈亏": _safe_float(item.get("unrealisedPnl")),
                "杠杆": _safe_float(item.get("leverage")),
                "清算价": _safe_float(item.get("liqPrice")),
                "数据源": "Bybit V5 (只读)",
            })
        return positions


# ── OKX 只读持仓 ────────────────────────────────────────────────────────────────

class OKXReadOnlyClient:
    BASE_URL = "https://www.okx.com"

    def __init__(self, api_key: str, api_secret: str, passphrase: str = "",
                 timeout: int = DEFAULT_TIMEOUT):
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self.timeout = timeout
        self._session = requests.Session()

    def _sign(self, ts: str, method: str, path: str, body: str = "") -> str:
        msg = ts + method.upper() + path + body
        return hmac.new(self.api_secret.encode(), msg.encode(), hashlib.sha256).digest().hex()

    def _get(self, path: str, params: Optional[dict] = None) -> Any:
        """强制只读 GET"""
        import base64
        ts = str(time.time())
        qs = "?" + urllib.parse.urlencode(params) if params else ""
        sig = self._sign(ts, "GET", path + qs)
        sig_b64 = base64.b64encode(bytes.fromhex(sig)).decode()
        headers = {
            "OK-ACCESS-KEY": self.api_key,
            "OK-ACCESS-SIGN": sig_b64,
            "OK-ACCESS-TIMESTAMP": ts,
            "OK-ACCESS-PASSPHRASE": self.passphrase,
            "Content-Type": "application/json",
        }
        r = self._session.get(
            self.BASE_URL + path, params=params, headers=headers, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def fetch_positions_readonly(self) -> List[Dict[str, Any]]:
        """只读获取 OKX 合约持仓"""
        data = self._get("/api/v5/account/positions", {"instType": "SWAP"})
        positions = []
        for item in (data.get("data") or []):
            size = _safe_float(item.get("pos"))
            if not size or size == 0:
                continue
            side_raw = str(item.get("posSide", "")).lower()
            side = "long" if side_raw == "long" else "short"
            positions.append({
                "币种": item.get("instId", ""),
                "方向": side,
                "数量": abs(size),
                "名义价值": _safe_float(item.get("notionalUsd")),
                "开仓价": _safe_float(item.get("avgPx")),
                "标记价": _safe_float(item.get("markPx")),
                "未实现盈亏": _safe_float(item.get("upl")),
                "杠杆": _safe_float(item.get("lever")),
                "清算价": _safe_float(item.get("liqPx")),
                "数据源": "OKX V5 (只读)",
            })
        return positions


# ── 统一入口（工厂函数）──────────────────────────────────────────────────────────

def fetch_positions_readonly(
    exchange: str,
    api_key: str,
    api_secret: str,
    passphrase: str = "",
    timeout: int = DEFAULT_TIMEOUT,
) -> List[Dict[str, Any]]:
    """
    统一只读持仓查询入口。
    - API Key 由调用方从 session_state 传入，此函数不做任何持久化存储
    - 强制只读：只调用 GET 端点
    """
    ex = exchange.lower()
    try:
        if ex == "binance":
            return BinanceReadOnlyClient(api_key, api_secret, timeout).fetch_positions_readonly()
        elif ex == "bybit":
            return BybitReadOnlyClient(api_key, api_secret, timeout).fetch_positions_readonly()
        elif ex == "okx":
            return OKXReadOnlyClient(api_key, api_secret, passphrase, timeout).fetch_positions_readonly()
        else:
            return [{"错误": f"不支持的交易所: {exchange}"}]
    except Exception as e:
        return [{"错误": str(e), "数据源": f"{exchange} (查询失败)"}]
