"""
hl_client.py  —  Hyperliquid 完整 API 封装（v6 增强）
覆盖：鲸鱼持仓追踪 / 排行榜聪明钱 / 预测资金费率 / Vault监控 / 链上清算密度
"""
from __future__ import annotations
import time
import uuid
from typing import Dict, List, Optional, Tuple
import requests

from models import (
    HLWhalePosition, HLLeaderEntry, HLPredictedFunding,
    HLVaultInfo, HLLiquidationDensity,
)

HL_BASE = "https://api.hyperliquid.xyz"
DEFAULT_TIMEOUT = 12


def _post(path: str, payload: dict, timeout: int = DEFAULT_TIMEOUT) -> dict | list:
    r = requests.post(f"{HL_BASE}{path}", json=payload, timeout=timeout)
    r.raise_for_status()
    return r.json()


def safe_float(v) -> Optional[float]:
    if v in (None, "", "null"): return None
    try: return float(v)
    except: return None


def safe_int(v) -> Optional[int]:
    if v in (None, ""): return None
    try: return int(v)
    except: return None


# ── 预测资金费率 ───────────────────────────────────────────────────────────────

def fetch_predicted_fundings(timeout: int = DEFAULT_TIMEOUT) -> List[HLPredictedFunding]:
    """
    获取所有资产的预测资金费率（下一期）及当前费率。
    返回按 |rate_delta_bps| 从大到小排序的列表。
    """
    try:
        meta_ctxs = _post("/info", {"type": "metaAndAssetCtxs"}, timeout)
        pred_raw  = _post("/info", {"type": "predictedFundings"}, timeout)
    except Exception as e:
        return []

    if not isinstance(meta_ctxs, list) or len(meta_ctxs) < 2:
        return []
    universe = meta_ctxs[0].get("universe", [])
    ctx_list = meta_ctxs[1]

    # Build current rate map: coin -> current_rate
    current_map: Dict[str, float] = {}
    for i, asset in enumerate(universe):
        coin = asset.get("name", "")
        if i < len(ctx_list):
            fr = safe_float(ctx_list[i].get("funding"))
            if fr is not None:
                current_map[coin] = fr

    # Build predicted map: coin -> predicted_rate
    predicted_map: Dict[str, float] = {}
    if isinstance(pred_raw, list):
        for item in pred_raw:
            if isinstance(item, list) and len(item) == 2:
                coin, rate_str = item[0], item[1]
                r = safe_float(rate_str)
                if r is not None:
                    predicted_map[coin] = r

    results: List[HLPredictedFunding] = []
    now_ms = int(time.time() * 1000)
    for coin, pred_rate in predicted_map.items():
        curr_rate = current_map.get(coin, 0.0)
        delta_bps = (pred_rate - curr_rate) * 10000
        results.append(HLPredictedFunding(
            coin=coin,
            predicted_rate=pred_rate,
            predicted_rate_bps=pred_rate * 10000,
            current_rate=curr_rate,
            current_rate_bps=curr_rate * 10000,
            rate_delta_bps=delta_bps,
            timestamp_ms=now_ms,
        ))

    results.sort(key=lambda x: abs(x.rate_delta_bps), reverse=True)
    return results


# ── 排行榜 & 聪明钱 ────────────────────────────────────────────────────────────

def fetch_leaderboard(window: str = "month", top_n: int = 20,
                      timeout: int = DEFAULT_TIMEOUT) -> List[HLLeaderEntry]:
    """
    获取 Hyperliquid 排行榜，window: "day"/"week"/"month"/"allTime"
    """
    try:
        raw = _post("/info", {"type": "leaderboard"}, timeout)
    except:
        return []

    if not isinstance(raw, dict):
        return []

    # leaderboardRows is a list of dicts
    rows = raw.get("leaderboardRows", [])
    entries: List[HLLeaderEntry] = []

    for i, row in enumerate(rows[:top_n]):
        prize = row.get("prize") or {}
        window_perf = {}
        for wp in (row.get("windowPerformances") or []):
            if isinstance(wp, list) and len(wp) == 2:
                window_perf[wp[0]] = wp[1]

        perf = window_perf.get(window, {})
        if isinstance(perf, str):
            perf = {}

        pnl    = safe_float(perf.get("pnl"))
        roi    = safe_float(perf.get("roi"))
        vol    = safe_float(perf.get("vlm"))

        eth_addr = row.get("ethAddress", "")
        display  = row.get("displayName") or (eth_addr[:6] + "…" + eth_addr[-4:] if eth_addr else f"#{i+1}")

        entries.append(HLLeaderEntry(
            rank=i + 1,
            address=eth_addr,
            display_name=display,
            pnl_30d=pnl,
            roi_30d=roi,
            volume_30d=vol,
            win_rate=None,
        ))

    return entries


def fetch_whale_positions(address: str,
                          timeout: int = DEFAULT_TIMEOUT) -> List[HLWhalePosition]:
    """
    获取指定地址当前所有持仓（clearinghouseState + allMids）
    """
    try:
        state = _post("/info", {"type": "clearinghouseState", "user": address}, timeout)
        mids_raw = _post("/info", {"type": "allMids"}, timeout)
    except:
        return []

    mids: Dict[str, float] = {}
    if isinstance(mids_raw, dict):
        for k, v in mids_raw.items():
            f = safe_float(v)
            if f: mids[k] = f

    positions = []
    now_ms = int(time.time() * 1000)
    for pos in (state.get("assetPositions") or []):
        p = pos.get("position") or {}
        coin = p.get("coin", "")
        szi  = safe_float(p.get("szi")) or 0.0
        if abs(szi) < 1e-9:
            continue
        entry_px = safe_float(p.get("entryPx"))
        mark_px  = mids.get(coin)
        notional = abs(szi) * mark_px if mark_px else abs(szi) * (entry_px or 0)
        upnl     = safe_float(p.get("unrealizedPnl"))
        leverage_raw = p.get("leverage") or {}
        lev = safe_float(leverage_raw.get("value")) if isinstance(leverage_raw, dict) else safe_float(leverage_raw)
        margin   = safe_float(p.get("marginUsed"))

        positions.append(HLWhalePosition(
            address=address,
            coin=coin,
            side="long" if szi > 0 else "short",
            size=abs(szi),
            notional=notional,
            entry_price=entry_px,
            mark_price=mark_px,
            unrealized_pnl=upnl,
            leverage=lev,
            margin_used=margin,
            timestamp_ms=now_ms,
        ))

    positions.sort(key=lambda x: x.notional, reverse=True)
    return positions


def fetch_top_whale_positions(top_n: int = 10,
                               timeout: int = DEFAULT_TIMEOUT) -> List[HLWhalePosition]:
    """
    获取排行榜前 top_n 地址的持仓，汇总返回。
    """
    leaders = fetch_leaderboard(top_n=top_n, timeout=timeout)
    all_positions: List[HLWhalePosition] = []
    for leader in leaders:
        if not leader.address:
            continue
        try:
            positions = fetch_whale_positions(leader.address, timeout=timeout)
            all_positions.extend(positions)
        except:
            continue
    return all_positions


# ── Vault 金库 ────────────────────────────────────────────────────────────────

def fetch_vault_list(timeout: int = DEFAULT_TIMEOUT) -> List[HLVaultInfo]:
    """
    获取 Hyperliquid 所有 Vault 列表和基本信息。
    """
    try:
        raw = _post("/info", {"type": "vaults"}, timeout)
    except:
        return []

    vaults: List[HLVaultInfo] = []
    now_ms = int(time.time() * 1000)

    items = raw if isinstance(raw, list) else (raw.get("vaults") if isinstance(raw, dict) else [])
    for item in (items or []):
        addr    = item.get("vaultAddress", "")
        summary = item.get("summary", item)
        name    = summary.get("name", addr[:8])
        leader  = summary.get("leader", "")
        tvl     = safe_float(summary.get("tvl")) or 0.0
        apr     = safe_float(summary.get("apr"))
        followers = safe_int(summary.get("numFollowers")) or 0
        pnl_30d = safe_float(summary.get("pnl"))

        vaults.append(HLVaultInfo(
            vault_address=addr,
            name=name,
            leader=leader,
            tvl=tvl,
            apr_30d=apr,
            follower_count=followers,
            net_inflow_24h=0.0,
            pnl_30d=pnl_30d,
            timestamp_ms=now_ms,
        ))

    vaults.sort(key=lambda v: v.tvl, reverse=True)
    return vaults


def fetch_vault_detail(vault_address: str,
                       timeout: int = DEFAULT_TIMEOUT) -> Optional[HLVaultInfo]:
    """获取单个 Vault 详情，包含净流入数据。"""
    try:
        raw = _post("/info", {"type": "vaultDetails",
                              "vaultAddress": vault_address}, timeout)
    except:
        return None

    now_ms = int(time.time() * 1000)
    summary = raw.get("summary", raw)
    name   = summary.get("name", vault_address[:8])
    leader = summary.get("leader", "")
    tvl    = safe_float(summary.get("tvl")) or 0.0
    apr    = safe_float(summary.get("apr"))
    followers = safe_int(summary.get("numFollowers")) or 0
    pnl_30d   = safe_float(summary.get("pnl"))

    # Calculate 24h net inflow from portfolio value history
    port_hist = raw.get("portfolioHistory") or []
    net_inflow = 0.0
    if len(port_hist) >= 2:
        try:
            # Each entry: [timestamp_ms, equity_value]
            now_val  = safe_float(port_hist[-1][1]) or 0.0
            day_ago  = safe_float(port_hist[-min(len(port_hist), 288)][1]) or 0.0
            net_inflow = now_val - day_ago
        except:
            pass

    return HLVaultInfo(
        vault_address=vault_address,
        name=name,
        leader=leader,
        tvl=tvl,
        apr_30d=apr,
        follower_count=followers,
        net_inflow_24h=net_inflow,
        pnl_30d=pnl_30d,
        timestamp_ms=now_ms,
    )


# ── 链上清算价格密度 ───────────────────────────────────────────────────────────

def fetch_liquidation_density(coin: str,
                               price_range_pct: float = 5.0,
                               buckets: int = 40,
                               max_addresses: int = 200,
                               timeout: int = DEFAULT_TIMEOUT) -> List[HLLiquidationDensity]:
    """
    通过 allMids + clearinghouseState(sample addresses) 构建清算价格密度图。
    逻辑：对每个持仓，根据保证金率推算清算价格，分桶统计。
    注意：全量扫描不现实，这里用排行榜地址作为样本。
    """
    try:
        mids_raw = _post("/info", {"type": "allMids"}, timeout)
        mid_price = safe_float((mids_raw or {}).get(coin))
        if not mid_price:
            return []
    except:
        return []

    # Gather sample addresses from leaderboard
    leaders = fetch_leaderboard(top_n=max_addresses, timeout=timeout)
    all_positions: List[HLWhalePosition] = []
    for leader in leaders[:50]:   # limit to 50 to avoid too many requests
        if not leader.address:
            continue
        try:
            poss = fetch_whale_positions(leader.address, timeout=timeout)
            for p in poss:
                if p.coin == coin:
                    all_positions.append(p)
        except:
            continue

    if not all_positions:
        return []

    # Build price buckets
    price_low  = mid_price * (1 - price_range_pct / 100)
    price_high = mid_price * (1 + price_range_pct / 100)
    bucket_size = (price_high - price_low) / buckets

    density_map: Dict[int, Dict] = {}
    for bi in range(buckets):
        bucket_price = price_low + bi * bucket_size + bucket_size / 2
        density_map[bi] = {
            "price": bucket_price,
            "long_notional": 0.0,
            "short_notional": 0.0,
            "address_count": 0,
        }

    for pos in all_positions:
        if pos.entry_price is None:
            continue
        # Estimate liquidation price (simplified: for long, liq ≈ entry * (1 - 1/lev + maint_margin))
        lev = pos.leverage or 10.0
        maint = 0.005  # 0.5% maintenance margin approx
        if pos.side == "long":
            liq_price = pos.entry_price * (1 - (1 / lev) + maint)
        else:
            liq_price = pos.entry_price * (1 + (1 / lev) - maint)

        if liq_price < price_low or liq_price > price_high:
            continue

        bi = int((liq_price - price_low) / bucket_size)
        bi = max(0, min(buckets - 1, bi))
        d = density_map[bi]
        if pos.side == "long":
            d["long_notional"] += pos.notional
        else:
            d["short_notional"] += pos.notional
        d["address_count"] += 1

    result = []
    for bi in range(buckets):
        d = density_map[bi]
        total = d["long_notional"] + d["short_notional"]
        if total > 0:
            result.append(HLLiquidationDensity(
                price=d["price"],
                long_notional=d["long_notional"],
                short_notional=d["short_notional"],
                total_notional=total,
                address_count=d["address_count"],
            ))

    return sorted(result, key=lambda x: x.price)


# ── 资金费率套利 (跨所) ────────────────────────────────────────────────────────

def compare_funding_vs_exchanges(hl_fundings: List[HLPredictedFunding],
                                  exchange_fundings: Dict[str, Dict[str, float]]
                                  ) -> List[Dict]:
    """
    对比 HL 预测资金费率与其他所当前费率，找出套利机会。
    exchange_fundings: {"binance": {"BTC": 0.0001, ...}, "bybit": {...}}
    返回套利机会列表，按年化收益率排序。
    """
    opps = []
    for pf in hl_fundings:
        coin = pf.coin
        hl_rate = pf.predicted_rate

        for ex_name, rates in exchange_fundings.items():
            ex_rate = rates.get(coin)
            if ex_rate is None:
                continue
            diff_bps = (hl_rate - ex_rate) * 10000
            # Annual yield (3 funding periods per day * 365)
            annual_yield = abs(diff_bps) * 3 * 365 / 100  # in %

            if abs(diff_bps) < 2:  # minimum threshold
                continue

            if hl_rate > ex_rate:
                long_ex = ex_name
                short_ex = "hyperliquid"
                long_rate = ex_rate
                short_rate = hl_rate
            else:
                long_ex = "hyperliquid"
                short_ex = ex_name
                long_rate = hl_rate
                short_rate = ex_rate

            severity = "low"
            if abs(diff_bps) > 10: severity = "medium"
            if abs(diff_bps) > 25: severity = "high"

            opps.append({
                "coin": coin,
                "long_exchange": long_ex,
                "short_exchange": short_ex,
                "long_rate_bps": long_rate * 10000,
                "short_rate_bps": short_rate * 10000,
                "net_bps": abs(diff_bps),
                "annual_yield_pct": annual_yield,
                "severity": severity,
            })

    opps.sort(key=lambda x: x["annual_yield_pct"], reverse=True)
    return opps[:30]
