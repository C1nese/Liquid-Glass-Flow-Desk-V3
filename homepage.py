"""
homepage.py — v5 竞品级首页
全市场总览 · 异动榜 · 主结论区
"""
from __future__ import annotations
import math, time
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from exchanges import MarketScanClient, MARKET_SCAN_COINS, safe_float
from models import CoinMarketRow, AnomalyEntry, MarketConclusion

# ── Palette ────────────────────────────────────────────────────────────────────
_BG    = "rgba(14,22,35,0.56)"
_PLOT  = "rgba(255,255,255,0.045)"
_FONT  = dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif")
_GRID  = "rgba(255,255,255,0.08)"

_UP    = "#1dc796"
_DN    = "#ff6868"
_WARN  = "#ffa94d"
_BLUE  = "#62c2ff"
_GOLD  = "#f8d35e"
_PURP  = "#c084fc"

# ── Formatters ─────────────────────────────────────────────────────────────────
def _fc(v):
    if v is None: return "–"
    v = float(v); av = abs(v)
    if av >= 1e9: return f"{v/1e9:.2f}B"
    if av >= 1e6: return f"{v/1e6:.2f}M"
    if av >= 1e3: return f"{v/1e3:.1f}K"
    return f"{v:.2f}"

def _fpct(v, plus=False):
    if v is None: return "–"
    s = f"{v:+.2f}%" if plus else f"{v:.2f}%"
    return s

def _fbps(v):
    return "–" if v is None else f"{v:+.2f}bps"

def _color(v, good_positive=True):
    if v is None: return "#aaa"
    return (_UP if v > 0 else _DN) if good_positive else (_DN if v > 0 else _UP)


# ══════════════════════════════════════════════════════════════════════════════
# Data builder
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=30, show_spinner=False)
def load_market_batch(coins: List[str], timeout: int = 10) -> List[dict]:
    client = MarketScanClient(timeout=timeout)
    return client.fetch_market_batch(coins, max_workers=10)


def build_coin_rows(raw_data: List[dict]) -> List[CoinMarketRow]:
    rows = []
    for d in raw_data:
        coin = d.get("coin", "?")
        oi   = d.get("oi")
        sv   = d.get("spot_vol_24h")
        liq_total = d.get("liq_total_24h", 0) or 0
        liq_long  = d.get("liq_long_24h", 0)  or 0
        liq_long_pct = (liq_long / liq_total * 100) if liq_total > 0 else None
        spot_perp = (sv / oi) if (sv and oi and oi > 0) else None

        # Lead-lag heuristic (from price change vs OI change direction)
        oi_1h  = d.get("oi_change_1h_pct")
        p_chg  = d.get("price_chg_pct")
        if oi_1h is not None and p_chg is not None:
            if abs(p_chg) > 0.3 and abs(oi_1h) < 0.2:
                lead_lag = "🟢 现货先行" if p_chg > 0 else "🔴 现货先跌"
            elif abs(oi_1h) > 0.5 and abs(p_chg) < 0.2:
                lead_lag = "📈 OI先动" if oi_1h > 0 else "📉 OI减仓"
            else:
                lead_lag = "⚖️ 同步"
        else:
            lead_lag = "–"

        funding_bps = (d.get("funding") or 0) * 10000

        # Composite signal heuristic
        score = 0.0
        if p_chg:   score += (p_chg / 5)    * 0.3
        if oi_1h:   score += (oi_1h / 2)    * 0.3
        if funding_bps: score -= (funding_bps / 10) * 0.2  # high funding = bearish pressure
        score = max(-1.0, min(1.0, score))
        if score > 0.35:   comp_label = "偏多推进▲"
        elif score < -0.35: comp_label = "偏空推进▼"
        elif abs(score) < 0.1 and abs(funding_bps) > 5: comp_label = "拥挤衰竭⚡"
        else: comp_label = "中性≈"

        rows.append(CoinMarketRow(
            coin=coin,
            price=d.get("price"),
            price_change_24h_pct=safe_float(d.get("price_chg_pct")),
            oi_total=oi,
            oi_change_1h_pct=oi_1h,
            oi_change_24h_pct=d.get("oi_change_24h_pct"),
            funding_avg=funding_bps,
            liq_24h_total=liq_total,
            liq_long_pct=liq_long_pct,
            long_short_ratio=d.get("ls_ratio"),
            spot_perp_ratio=spot_perp,
            lead_lag_status=lead_lag,
            composite_label=comp_label,
            composite_score=score,
        ))
    return rows


def build_anomaly_list(rows: List[CoinMarketRow]) -> Dict[str, List[AnomalyEntry]]:
    """Five anomaly boards"""
    boards: Dict[str, List[AnomalyEntry]] = {
        "oi_surge": [], "liq_spike": [], "funding_extreme": [],
        "spot_lead": [], "crowd_exhaust": []
    }

    def _entry(rank, row, cat, value, label, direction, detail) -> AnomalyEntry:
        return AnomalyEntry(rank=rank, coin=row.coin, category=cat,
            value=value, value_label=label, direction=direction,
            exchange="Binance", detail=detail)

    # OI激增榜
    oi_sorted = sorted([r for r in rows if r.oi_change_1h_pct is not None],
                       key=lambda r: abs(r.oi_change_1h_pct), reverse=True)[:8]
    for i, r in enumerate(oi_sorted):
        d = r.oi_change_1h_pct
        boards["oi_surge"].append(_entry(i+1, r, "oi_surge", d,
            _fpct(d, plus=True),
            "bull" if d > 0 else "bear",
            f"OI 1h {'增加' if d>0 else '减少'} {abs(d):.2f}%，总OI {_fc(r.oi_total)}"))

    # 爆仓榜
    liq_sorted = sorted([r for r in rows if r.liq_24h_total and r.liq_24h_total > 0],
                        key=lambda r: r.liq_24h_total, reverse=True)[:8]
    for i, r in enumerate(liq_sorted):
        lp = r.liq_long_pct or 50
        dom = "多头" if lp > 55 else "空头" if lp < 45 else "均衡"
        boards["liq_spike"].append(_entry(i+1, r, "liq_spike", r.liq_24h_total,
            _fc(r.liq_24h_total), "bear" if lp > 55 else "bull",
            f"24h爆仓 {_fc(r.liq_24h_total)}，{dom}主导（多头{lp:.0f}%）"))

    # Funding极值榜
    fund_sorted = sorted([r for r in rows if r.funding_avg is not None],
                         key=lambda r: abs(r.funding_avg), reverse=True)[:8]
    for i, r in enumerate(fund_sorted):
        f = r.funding_avg
        boards["funding_extreme"].append(_entry(i+1, r, "funding_extreme", f,
            _fbps(f), "bear" if f > 0 else "bull",
            f"资金费率 {_fbps(f)}，{'多头拥挤付费' if f>0 else '空头拥挤付费'}"))

    # 现货带动榜
    spot_sorted = sorted([r for r in rows if "现货先" in r.lead_lag_status],
                         key=lambda r: abs(r.price_change_24h_pct or 0), reverse=True)[:8]
    for i, r in enumerate(spot_sorted):
        p = r.price_change_24h_pct or 0
        boards["spot_lead"].append(_entry(i+1, r, "spot_lead", p,
            _fpct(p, plus=True), "bull" if p > 0 else "bear",
            f"{r.lead_lag_status}，价格 {_fpct(p, plus=True)}，OI 1h {_fpct(r.oi_change_1h_pct, plus=True)}"))

    # 拥挤但衰竭榜
    exhaust = [r for r in rows if "拥挤衰竭" in r.composite_label or
               (r.funding_avg and abs(r.funding_avg) > 3 and abs(r.price_change_24h_pct or 0) < 0.5)]
    exhaust.sort(key=lambda r: abs(r.funding_avg or 0), reverse=True)
    for i, r in enumerate(exhaust[:8]):
        boards["crowd_exhaust"].append(_entry(i+1, r, "crowd_exhaust",
            r.funding_avg or 0, _fbps(r.funding_avg), "neutral",
            f"费率 {_fbps(r.funding_avg)} 但价格滞涨，拥挤度高+动能衰竭"))

    return boards


def build_market_conclusion(rows: List[CoinMarketRow]) -> MarketConclusion:
    """Synthesise a one-line market conclusion"""
    now_ms = int(time.time() * 1000)
    if not rows:
        return MarketConclusion(now_ms, "数据加载中…", "#aaa", 0.0, [], [])

    scores     = [r.composite_score for r in rows if r.composite_score != 0]
    avg_score  = sum(scores) / len(scores) if scores else 0
    bull_count = sum(1 for s in scores if s > 0.3)
    bear_count = sum(1 for s in scores if s < -0.3)
    fund_vals  = [r.funding_avg for r in rows if r.funding_avg is not None]
    avg_fund   = sum(fund_vals) / len(fund_vals) if fund_vals else 0
    spot_leads = sum(1 for r in rows if "现货先行" in r.lead_lag_status)
    oi_surges  = sum(1 for r in rows if (r.oi_change_1h_pct or 0) > 1.0)

    reasons = []
    watchlist = []

    if avg_score > 0.25 and bull_count > bear_count:
        label, color = "偏多推进 ▲", _UP
        reasons.append(f"{bull_count}/{len(scores)} 个币种合成信号偏多")
    elif avg_score < -0.25 and bear_count > bull_count:
        label, color = "偏空推进 ▼", _DN
        reasons.append(f"{bear_count}/{len(scores)} 个币种合成信号偏空")
    elif abs(avg_fund) > 5 and abs(avg_score) < 0.15:
        label, color = "拥挤回落风险 ⚡", _WARN
        reasons.append(f"平均资金费率 {avg_fund:+.1f}bps，价格动能不足")
    elif spot_leads >= 3:
        label, color = "现货先动 📡", _BLUE
        reasons.append(f"{spot_leads} 个币种现货先于合约异动")
    else:
        label, color = "中性观望 ≈", "#aaa"
        reasons.append("各因子方向分歧，建议观望")

    if oi_surges > 0:
        reasons.append(f"{oi_surges} 个币种 OI 1h 激增 > 1%")
    if avg_fund > 8:
        reasons.append(f"市场整体资金费率偏高 ({avg_fund:+.1f}bps)，多头拥挤")
    if avg_fund < -5:
        reasons.append(f"市场整体资金费率为负 ({avg_fund:+.1f}bps)，空头付费")

    # Watchlist: top abs score coins
    watchlist = [r.coin for r in sorted(rows, key=lambda r: abs(r.composite_score), reverse=True)[:5]]
    confidence = min(1.0, abs(avg_score) * 2 + (bull_count + bear_count) / max(len(scores), 1) * 0.5)

    return MarketConclusion(now_ms, label, color, confidence, reasons, watchlist)


# ══════════════════════════════════════════════════════════════════════════════
# Visualizations
# ══════════════════════════════════════════════════════════════════════════════


def build_fear_greed_index(rows):
    """综合计算恐慌贪婪指数 0-100。"""
    if not rows:
        return {"score": 50, "label": "中性", "color": "#aaaaaa", "factors": {}}
    scores = []
    fr_vals = [r.funding_avg for r in rows if r.funding_avg is not None]
    if fr_vals:
        avg_fr = sum(fr_vals) / len(fr_vals)
        fr_score = 50 + min(50, max(-50, avg_fr * 4))
        scores.append(("资金费率", fr_score, 0.25))
    oi_vals = [r.oi_change_24h_pct for r in rows if r.oi_change_24h_pct is not None]
    if oi_vals:
        avg_oi = sum(oi_vals) / len(oi_vals)
        oi_score = 50 + min(50, max(-50, avg_oi * 2))
        scores.append(("OI变化", oi_score, 0.25))
    liq_vals = [r.liq_long_pct for r in rows if r.liq_long_pct is not None]
    if liq_vals:
        avg_liq = sum(liq_vals) / len(liq_vals)
        scores.append(("爆仓比", 100 - avg_liq, 0.20))
    ls_vals = [r.long_short_ratio for r in rows if r.long_short_ratio is not None]
    if ls_vals:
        avg_ls = sum(ls_vals) / len(ls_vals)
        ls_score = 50 + min(40, max(-40, (avg_ls - 1.0) * 40))
        scores.append(("多空比", ls_score, 0.15))
    sig_vals = [r.composite_score for r in rows if abs(r.composite_score) > 0.01]
    if sig_vals:
        avg_sig = sum(sig_vals) / len(sig_vals)
        sig_score = 50 + min(45, max(-45, avg_sig * 45))
        scores.append(("合成信号", sig_score, 0.15))
    if not scores:
        return {"score": 50, "label": "中性", "color": "#aaaaaa", "factors": {}}
    total_w = sum(w for _, _, w in scores)
    final = max(0, min(100, sum(s * w for _, s, w in scores) / total_w))
    score = int(final)
    if score >= 80:   label, color = "极度贪婪", "#00cc66"
    elif score >= 60: label, color = "贪婪",     "#44cc44"
    elif score >= 45: label, color = "中性",     "#aaaaaa"
    elif score >= 25: label, color = "恐惧",     "#ff8833"
    else:             label, color = "极度恐惧", "#ff4444"
    return {"score": score, "label": label, "color": color,
            "factors": {n: int(s) for n, s, _ in scores}}


def build_fear_greed_html(fg):
    """渲染恐慌贪婪指数仪表盘"""
    import math
    score = fg["score"]
    label = fg["label"]
    color = fg["color"]
    factors = fg.get("factors", {})
    angle = math.radians(-180 + score * 1.8)
    cx, cy, r = 100, 90, 70
    nx = cx + r * math.cos(angle)
    ny = cy + r * math.sin(angle)
    fhtml = "".join(
        '<div style="display:flex;justify-content:space-between;margin:3px 0;">'
        f'<span style="color:#aaa;font-size:11px;">{k}</span>'
        f'<span style="font-size:11px;font-weight:600;color:{"#44cc44" if v>55 else "#ff8833" if v<45 else "#aaa"};">{v}</span>'
        "</div>"
        for k, v in factors.items()
    )
    arc_dash = int(score * 2.2)
    return (
        '<div style="background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.1);'
        'border-radius:16px;padding:16px;text-align:center;">'
        '<div style="font-size:10px;color:#aaa;letter-spacing:0.15em;margin-bottom:8px;">'
        "FEAR &amp; GREED INDEX · 恐慌贪婪指数</div>"
        '<svg width="200" height="100" viewBox="0 0 200 100">'
        "<defs><linearGradient id=\"fg-grad\" x1=\"0%\" y1=\"0%\" x2=\"100%\" y2=\"0%\">"
        '<stop offset="0%"   stop-color="#ff4444"/>'
        '<stop offset="25%"  stop-color="#ff8833"/>'
        '<stop offset="50%"  stop-color="#aaaaaa"/>'
        '<stop offset="75%"  stop-color="#44cc44"/>'
        '<stop offset="100%" stop-color="#00cc66"/>'
        "</linearGradient></defs>"
        '<path d="M 30 90 A 70 70 0 0 1 170 90" fill="none" '
        'stroke="url(#fg-grad)" stroke-width="12" stroke-linecap="round"/>'
        f'<line x1="{cx}" y1="{cy}" x2="{nx:.1f}" y2="{ny:.1f}" '
        f'stroke="{color}" stroke-width="3" stroke-linecap="round"/>'
        f'<circle cx="{cx}" cy="{cy}" r="5" fill="{color}"/>'
        f'<text x="100" y="82" text-anchor="middle" font-size="28" font-weight="800" fill="{color}">{score}</text>'
        "</svg>"
        f'<div style="font-size:15px;font-weight:700;color:{color};margin-top:4px;">{label}</div>'
        f'<div style="margin-top:10px;border-top:1px solid rgba(255,255,255,0.08);padding-top:8px;">{fhtml}</div>'
        "</div>"
    )


def build_market_overview_table(rows: List[CoinMarketRow]) -> pd.DataFrame:
    """Build styled DataFrame for st.dataframe"""
    data = []
    for r in rows:
        p_chg = r.price_change_24h_pct
        oi1h  = r.oi_change_1h_pct
        oi24h = r.oi_change_24h_pct
        lls   = r.long_short_ratio
        data.append({
            "币种":        r.coin,
            "价格":        r.price,
            "24h%":        p_chg,
            "OI总额":      r.oi_total,
            "OI 1h%":     oi1h,
            "OI 24h%":    oi24h,
            "Funding(bps)": r.funding_avg,
            "24h爆仓":     r.liq_24h_total,
            "多头爆%":     r.liq_long_pct,
            "L/S比":       lls,
            "Spot/OI":     r.spot_perp_ratio,
            "Lead/Lag":    r.lead_lag_status,
            "信号":        r.composite_label,
        })
    return pd.DataFrame(data)


def build_oi_bar_chart(rows: List[CoinMarketRow]) -> go.Figure:
    """横向OI排行图，颜色=合成信号"""
    sorted_rows = sorted([r for r in rows if r.oi_total], key=lambda r: r.oi_total, reverse=True)[:20]
    coins  = [r.coin for r in sorted_rows]
    ois    = [r.oi_total / 1e9 for r in sorted_rows]
    colors = []
    for r in sorted_rows:
        if r.composite_score > 0.25:   colors.append(_UP)
        elif r.composite_score < -0.25: colors.append(_DN)
        elif "拥挤" in r.composite_label: colors.append(_WARN)
        else: colors.append(_BLUE)

    fig = go.Figure(go.Bar(
        x=ois, y=coins, orientation="h",
        marker_color=colors, opacity=0.85,
        text=[f"{v:.2f}B" for v in ois],
        textposition="outside",
        hovertemplate="<b>%{y}</b><br>OI: $%{x:.2f}B<extra></extra>"))
    fig.update_layout(
        height=520, margin=dict(l=12, r=60, t=48, b=12),
        paper_bgcolor=_BG, plot_bgcolor=_PLOT, font=_FONT,
        title=dict(text="持仓量排行 (OI Ranking)  ·  颜色=合成信号",
                   x=0.02, y=0.98, xanchor="left", font=dict(size=14, color="#f3f8ff")),
        xaxis=dict(showgrid=True, gridcolor=_GRID, title="OI ($B)"),
        yaxis=dict(showgrid=False, autorange="reversed"))
    return fig


def build_funding_heatmap(rows: List[CoinMarketRow]) -> go.Figure:
    """资金费率热力图 — 所有币种"""
    valid = [r for r in rows if r.funding_avg is not None]
    valid.sort(key=lambda r: r.funding_avg, reverse=True)
    coins  = [r.coin for r in valid]
    fundings = [r.funding_avg for r in valid]
    max_f  = max(abs(f) for f in fundings) if fundings else 1

    colors = []
    for f in fundings:
        if f > 8:    colors.append(_DN)
        elif f > 3:  colors.append(_WARN)
        elif f < -5: colors.append(_UP)
        elif f < -2: colors.append(_BLUE)
        else:        colors.append("#6b7fa3")

    fig = go.Figure(go.Bar(
        x=fundings, y=coins, orientation="h",
        marker_color=colors, opacity=0.85,
        text=[f"{f:+.2f}bps" for f in fundings],
        textposition="outside",
        hovertemplate="<b>%{y}</b><br>Funding: %{x:+.2f}bps<extra></extra>"))
    fig.add_vline(x=0, line_color="rgba(255,255,255,0.3)", line_width=1)
    fig.update_layout(
        height=max(300, len(valid)*18), margin=dict(l=12, r=60, t=48, b=12),
        paper_bgcolor=_BG, plot_bgcolor=_PLOT, font=_FONT,
        title=dict(text="资金费率热力图 (Funding Rate Heatmap)  ·  红=多头拥挤 蓝=空头拥挤",
                   x=0.02, y=0.98, xanchor="left", font=dict(size=14, color="#f3f8ff")),
        xaxis=dict(showgrid=True, gridcolor=_GRID, zeroline=True,
                   zerolinecolor="rgba(255,255,255,0.2)", title="Funding (bps)"),
        yaxis=dict(showgrid=False, autorange="reversed"))
    return fig


def build_oi_change_bubble(rows: List[CoinMarketRow]) -> go.Figure:
    """OI变化 vs 价格变化 气泡图，泡大小=OI总量"""
    valid = [r for r in rows if r.oi_change_1h_pct is not None and r.price_change_24h_pct is not None]
    if not valid:
        fig = go.Figure()
        fig.add_annotation(text="等待数据…", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        fig.update_layout(height=400, paper_bgcolor=_BG, plot_bgcolor=_PLOT, font=_FONT)
        return fig

    x      = [r.price_change_24h_pct for r in valid]
    y      = [r.oi_change_1h_pct for r in valid]
    sizes  = [max(8, min(40, math.log10(max(r.oi_total or 1, 1)) * 5)) for r in valid]
    labels = [r.coin for r in valid]
    colors = []
    for r in valid:
        if r.composite_score > 0.25:    colors.append(_UP)
        elif r.composite_score < -0.25: colors.append(_DN)
        elif "拥挤" in r.composite_label: colors.append(_WARN)
        else: colors.append(_BLUE)

    fig = go.Figure(go.Scatter(
        x=x, y=y, mode="markers+text",
        text=labels, textposition="top center",
        textfont=dict(size=9, color="#cde"),
        marker=dict(size=sizes, color=colors, opacity=0.8,
                    line=dict(color="rgba(255,255,255,0.3)", width=1)),
        hovertemplate="<b>%{text}</b><br>价格24h: %{x:+.2f}%<br>OI 1h: %{y:+.2f}%<extra></extra>"))
    fig.add_hline(y=0, line_color="rgba(255,255,255,0.2)", line_dash="dot")
    fig.add_vline(x=0, line_color="rgba(255,255,255,0.2)", line_dash="dot")
    # Quadrant labels
    for tx, ty, txt, col in [(2.5, 2.5,"多头加仓",_UP),(-2.5,2.5,"空头加仓",_DN),
                              (2.5,-2.5,"空头回补",_BLUE),(-2.5,-2.5,"多头减仓",_WARN)]:
        fig.add_annotation(x=tx, y=ty, text=txt, showarrow=False,
                           font=dict(size=9, color=col), opacity=0.6)
    fig.update_layout(
        height=440, margin=dict(l=12, r=12, t=52, b=12),
        paper_bgcolor=_BG, plot_bgcolor=_PLOT, font=_FONT,
        title=dict(text="OI变化 vs 价格变化 气泡图  ·  四象限分析",
                   x=0.02, y=0.98, xanchor="left", font=dict(size=14, color="#f3f8ff")),
        xaxis=dict(showgrid=True, gridcolor=_GRID, title="价格24h变化 (%)"),
        yaxis=dict(showgrid=True, gridcolor=_GRID, title="OI 1h变化 (%)"))
    return fig


def build_liq_treemap(rows: List[CoinMarketRow]) -> go.Figure:
    """爆仓额 treemap"""
    valid = [r for r in rows if r.liq_24h_total and r.liq_24h_total > 0]
    if not valid:
        fig = go.Figure()
        fig.add_annotation(text="无爆仓数据", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        fig.update_layout(height=300, paper_bgcolor=_BG, font=_FONT)
        return fig

    labels  = [r.coin for r in valid]
    values  = [r.liq_24h_total for r in valid]
    parents = ["" for _ in valid]
    # Color by long-dominated vs short
    colors = []
    for r in valid:
        lp = r.liq_long_pct or 50
        colors.append(_DN if lp > 55 else _UP if lp < 45 else _WARN)

    fig = go.Figure(go.Treemap(
        labels=labels, values=values, parents=parents,
        marker=dict(colors=colors),
        texttemplate="<b>%{label}</b><br>%{value:,.0f}",
        hovertemplate="<b>%{label}</b><br>爆仓额: $%{value:,.0f}<extra></extra>"))
    fig.update_layout(
        height=320, margin=dict(l=0, r=0, t=40, b=0),
        paper_bgcolor=_BG, font=_FONT,
        title=dict(text="24h 爆仓额分布 Treemap  ·  红=多头主导 绿=空头主导",
                   x=0.02, y=0.99, xanchor="left", font=dict(size=14, color="#f3f8ff")))
    return fig


def build_conclusion_card_html(conclusion: MarketConclusion) -> str:
    """Hero conclusion card HTML"""
    conf_w = int(conclusion.confidence * 100)
    conf_col = _UP if conclusion.confidence > 0.6 else _WARN if conclusion.confidence > 0.35 else "#aaa"
    reasons_html = "".join(
        '<div style="padding:4px 0;border-bottom:1px solid rgba(255,255,255,0.07);">'
        '<span style="color:#9ec;font-size:0.82rem;">✦ {r}</span></div>'
        .format(r=r.replace("<","&lt;").replace(">","&gt;"))
        for r in conclusion.reasons)
    watch_html = "".join(
        '<span style="padding:3px 10px;border-radius:999px;background:rgba(255,255,255,0.1);'
        'border:1px solid rgba(255,255,255,0.15);font-size:0.8rem;color:#f0f8ff;">{c}</span> '
        .format(c=c)
        for c in conclusion.watchlist)
    ts = pd.to_datetime(conclusion.timestamp_ms, unit="ms").strftime("%H:%M:%S")
    return f"""
<div style="padding:20px 24px;border-radius:24px;
    border:2px solid {conclusion.color}40;
    background:linear-gradient(135deg,rgba(255,255,255,0.10),rgba(255,255,255,0.04));
    backdrop-filter:blur(30px);box-shadow:0 24px 48px rgba(0,0,0,0.3);">
  <div style="font-size:0.7rem;color:#bcd;text-transform:uppercase;letter-spacing:0.18em;margin-bottom:6px;">
    主结论 · Market Conclusion · {ts}
  </div>
  <div style="font-size:2rem;font-weight:800;color:{conclusion.color};margin-bottom:8px;line-height:1.1;">
    {conclusion.label}
  </div>
  <div style="width:100%;height:5px;background:rgba(255,255,255,0.1);border-radius:3px;margin-bottom:14px;">
    <div style="width:{conf_w}%;height:100%;background:{conf_col};border-radius:3px;transition:width 0.6s;"></div>
  </div>
  <div style="margin-bottom:14px;">{reasons_html}</div>
  <div style="font-size:0.72rem;color:#bcd;margin-bottom:6px;text-transform:uppercase;letter-spacing:0.1em;">重点关注</div>
  <div style="display:flex;flex-wrap:wrap;gap:6px;">{watch_html}</div>
</div>"""


def build_anomaly_board_html(entries: List[AnomalyEntry], title: str, icon: str) -> str:
    """Anomaly board as HTML cards — all double-quote attrs inside single-quoted Python strings"""
    if not entries:
        return '<div style="color:#888;font-size:0.85rem;padding:12px;">暂无异动数据</div>'
    rows_html = ""
    for e in entries:
        col = _UP if e.direction == "bull" else _DN if e.direction == "bear" else _WARN
        detail_esc = e.detail.replace('"', '&quot;').replace('<', '&lt;').replace('>', '&gt;')
        rows_html += (
            '<div style="display:flex;align-items:center;padding:8px 12px;'
            'border-bottom:1px solid rgba(255,255,255,0.06);gap:10px;">'
            '<span style="width:20px;color:#888;font-size:0.75rem;text-align:right;">{rank}</span>'
            '<span style="width:52px;font-weight:700;color:#f0f8ff;font-size:0.95rem;">{coin}</span>'
            '<span style="width:70px;font-weight:700;color:{col};font-size:0.9rem;">{val_label}</span>'
            '<span style="flex:1;color:#aac;font-size:0.78rem;line-height:1.3;">{detail}</span>'
            '</div>'
        ).format(rank=e.rank, coin=e.coin, col=col, val_label=e.value_label, detail=detail_esc)
    title_esc = title.replace('"', '&quot;')
    return (
        '<div style="border-radius:18px;overflow:hidden;'
        'border:1px solid rgba(255,255,255,0.12);'
        'background:rgba(255,255,255,0.05);backdrop-filter:blur(20px);">'
        '<div style="padding:12px 14px;background:rgba(255,255,255,0.08);'
        'font-size:0.85rem;font-weight:700;color:#f0f8ff;">{icon} {title}</div>'
        '{rows}'
        '</div>'
    ).format(icon=icon, title=title_esc, rows=rows_html)

# ══════════════════════════════════════════════════════════════════════════════
# Main render function — called from app.py
# ══════════════════════════════════════════════════════════════════════════════

def render_homepage(coins: List[str], timeout: int = 10):
    """Render the full homepage inside the current Streamlit context"""

    # Header
    st.markdown("""
    <div style="padding:18px 24px 14px;margin-bottom:1rem;border-radius:28px;
        border:1px solid rgba(255,255,255,0.16);
        background:linear-gradient(135deg,rgba(255,255,255,0.12),rgba(255,255,255,0.05));
        backdrop-filter:blur(30px);">
      <div style="font-size:0.72rem;color:#bcd;text-transform:uppercase;letter-spacing:0.2em;">
        Market Intelligence · 全市场情报中心</div>
      <div style="font-size:1.7rem;font-weight:800;color:#fff;margin:4px 0;">
        全市场总览 · OI排行 · 异动榜 · 主结论</div>
      <div style="font-size:0.88rem;color:#c8dcf0;">
        数据来源 Binance 公开 API · 每30秒刷新 · 多维度跨币种实时扫描</div>
    </div>""", unsafe_allow_html=True)

    with st.spinner("扫描全市场数据中…"):
        raw_data = load_market_batch(coins, timeout)

    if not raw_data:
        st.error("无法获取市场数据，请检查网络。"); return

    rows       = build_coin_rows(raw_data)
    anomalies  = build_anomaly_list(rows)
    conclusion = build_market_conclusion(rows)
    fear_greed = build_fear_greed_index(rows)

    # ── 主结论区 ───────────────────────────────────────────────────────────────
    _fg_col, _cc_col = st.columns([1, 3])
    with _fg_col:
        st.markdown(build_fear_greed_html(fear_greed), unsafe_allow_html=True)
    with _cc_col:
        st.markdown(build_conclusion_card_html(conclusion), unsafe_allow_html=True)
    st.markdown('<div style="height:12px"></div>', unsafe_allow_html=True)

    # ── Top KPIs ───────────────────────────────────────────────────────────────
    total_oi  = sum(r.oi_total or 0 for r in rows)
    total_liq = sum(r.liq_24h_total or 0 for r in rows)
    avg_fund  = sum(r.funding_avg or 0 for r in rows) / max(len(rows), 1)
    bull_count= sum(1 for r in rows if r.composite_score > 0.25)
    bear_count= sum(1 for r in rows if r.composite_score < -0.25)
    spot_leads= sum(1 for r in rows if "先行" in r.lead_lag_status)

    kpi_cols = st.columns(6)
    kpi_cols[0].metric("扫描币种", str(len(rows)))
    kpi_cols[1].metric("跨所总OI", _fc(total_oi))
    kpi_cols[2].metric("24h总爆仓", _fc(total_liq))
    kpi_cols[3].metric("平均Funding", f"{avg_fund:+.2f}bps")
    kpi_cols[4].metric("偏多/偏空", f"{bull_count}🟢 / {bear_count}🔴")
    kpi_cols[5].metric("现货先行币种", str(spot_leads))

    st.markdown('<div style="height:8px"></div>', unsafe_allow_html=True)

    # ── 主图区：OI排行 + 气泡图 ───────────────────────────────────────────────
    chart_l, chart_r = st.columns([1, 1.3], gap="large")
    with chart_l:
        st.plotly_chart(build_oi_bar_chart(rows), key="hp_oi_bar", config={'displayModeBar': True, 'scrollZoom': True})
    with chart_r:
        st.plotly_chart(build_oi_change_bubble(rows), key="hp_bubble", config={'displayModeBar': True, 'scrollZoom': True})

    # ── Funding热力图 + 爆仓Treemap ───────────────────────────────────────────
    f_col, t_col = st.columns([1.2, 1], gap="large")
    with f_col:
        fund_fig = build_funding_heatmap(rows)
        st.plotly_chart(fund_fig, key="hp_funding", config={'displayModeBar': True, 'scrollZoom': True})
    with t_col:
        st.plotly_chart(build_liq_treemap(rows), key="hp_liq_tree", config={'displayModeBar': True, 'scrollZoom': True})
        # Lead/Lag summary
        st.markdown(
            '<div style="padding:10px 14px;border-radius:14px;'
            'border:1px solid rgba(255,255,255,0.12);background:rgba(255,255,255,0.05);'
            'font-size:0.82rem;color:#c8dcf0;">'
            '<b>Lead/Lag 说明</b><br/>'
            '🟢 现货先行 = 价格变化领先OI，真实需求驱动<br/>'
            '📈 OI先动 = 合约仓位先变，杠杆驱动<br/>'
            '⚖️ 同步 = 现货合约齐动，趋势延续</div>',
            unsafe_allow_html=True)

    # ── 全市场总览表 ───────────────────────────────────────────────────────────
    st.markdown('<div style="height:10px"></div>', unsafe_allow_html=True)
    st.markdown(
        '<div style="font-size:0.7rem;color:#bcd;text-transform:uppercase;'
        'letter-spacing:0.18em;margin-bottom:6px;">全市场总览表 · Market Overview Table</div>',
        unsafe_allow_html=True)

    df = build_market_overview_table(rows)
    st.dataframe(df, width='stretch', hide_index=True,
        column_config={
            "币种":    st.column_config.TextColumn(width="small"),
            "价格":    st.column_config.NumberColumn(format="%.4f", width="small"),
            "24h%":   st.column_config.NumberColumn(format="%+.2f%%", width="small"),
            "OI总额": st.column_config.NumberColumn(format="%.0f",  width="medium"),
            "OI 1h%": st.column_config.NumberColumn(format="%+.2f%%", width="small"),
            "OI 24h%":st.column_config.NumberColumn(format="%+.2f%%", width="small"),
            "Funding(bps)": st.column_config.NumberColumn(format="%+.2f", width="small"),
            "24h爆仓": st.column_config.NumberColumn(format="%.0f",  width="medium"),
            "多头爆%": st.column_config.NumberColumn(format="%.1f%%", width="small"),
            "L/S比":   st.column_config.NumberColumn(format="%.3f",  width="small"),
            "Spot/OI": st.column_config.NumberColumn(format="%.2f",  width="small"),
            "Lead/Lag":st.column_config.TextColumn(width="medium"),
            "信号":    st.column_config.TextColumn(width="medium"),
        })

    # ── 异动榜 ─────────────────────────────────────────────────────────────────
    st.markdown('<div style="height:14px"></div>', unsafe_allow_html=True)
    st.markdown(
        '<div style="font-size:0.7rem;color:#bcd;text-transform:uppercase;'
        'letter-spacing:0.18em;margin-bottom:10px;">异动榜 · Anomaly Boards</div>',
        unsafe_allow_html=True)

    ab1, ab2, ab3 = st.columns(3, gap="medium")
    with ab1:
        st.markdown(build_anomaly_board_html(
            anomalies["oi_surge"], "OI 激增榜", "📈"), unsafe_allow_html=True)
    with ab2:
        st.markdown(build_anomaly_board_html(
            anomalies["liq_spike"], "爆仓榜", "💥"), unsafe_allow_html=True)
    with ab3:
        st.markdown(build_anomaly_board_html(
            anomalies["funding_extreme"], "Funding 极值榜", "💸"), unsafe_allow_html=True)

    st.markdown('<div style="height:10px"></div>', unsafe_allow_html=True)
    ab4, ab5 = st.columns(2, gap="medium")
    with ab4:
        st.markdown(build_anomaly_board_html(
            anomalies["spot_lead"], "现货带动榜", "📡"), unsafe_allow_html=True)
    with ab5:
        st.markdown(build_anomaly_board_html(
            anomalies["crowd_exhaust"], "拥挤但衰竭榜", "⚡"), unsafe_allow_html=True)

    st.caption(f"数据更新时间：{pd.Timestamp.now().strftime('%H:%M:%S')} · 全部来自 Binance 公开 API · 30秒缓存")
