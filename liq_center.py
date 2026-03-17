"""
liq_center.py — v5 爆仓中心
五个视角：多头/空头 · 单所/跨所 · 连续爆仓簇
"""
from __future__ import annotations
import time
from typing import Dict, List
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from models import LiquidationEvent, LiquidationClusterV2, MultiExchangeLiqSummary

_BG   = "rgba(14,22,35,0.56)"; _PLOT = "rgba(255,255,255,0.045)"
_FONT = dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif")
_GRID = "rgba(255,255,255,0.08)"
_UP   = "#1dc796"; _DN = "#ff6868"; _WARN = "#ffa94d"; _BLUE = "#62c2ff"; _GOLD = "#f8d35e"

EX_COLORS = {"binance":"#f0b90b","bybit":"#e6a817","okx":"#1267fe","hyperliquid":"#8b5cf6"}


def _fc(v):
    if v is None: return "–"
    v = float(v); av = abs(v)
    if av >= 1e9: return f"{v/1e9:.2f}B"
    if av >= 1e6: return f"{v/1e6:.2f}M"
    if av >= 1e3: return f"{v/1e3:.1f}K"
    return f"{v:.2f}"

def _build_liq_summary(liq_by_exchange: Dict[str, List[LiquidationEvent]],
                        clusters: List[LiquidationClusterV2],
                        windows_ms: Dict[str, int]) -> Dict[str, "MultiExchangeLiqSummary"]:
    """Build summary for each time window"""
    now_ms = int(time.time() * 1000)
    summaries = {}
    for wlabel, w_ms in windows_ms.items():
        cutoff = now_ms - w_ms
        long_n = short_n = 0.0
        long_c = short_c = 0
        by_ex  = {}
        for ek, evts in liq_by_exchange.items():
            for e in evts:
                if e.timestamp_ms < cutoff: continue
                n = e.notional or 0
                by_ex[ek] = by_ex.get(ek, 0) + n
                if e.side == "long":  long_n += n; long_c += 1
                else:                 short_n += n; short_c += 1
        w_clusters     = [c for c in clusters if c.start_ms >= cutoff]
        cross_clusters = [c for c in w_clusters if c.cross_exchange]
        peak           = max((c.total_notional for c in w_clusters), default=0)
        dominant       = "long" if long_n >= short_n else "short"
        summaries[wlabel] = MultiExchangeLiqSummary(
            coin="ALL", window_label=wlabel,
            long_notional=long_n, short_notional=short_n,
            long_count=long_c, short_count=short_c,
            by_exchange=by_ex,
            cluster_count=len(w_clusters),
            cross_ex_cluster_count=len(cross_clusters),
            dominant_side=dominant,
            peak_cluster_notional=peak)
    return summaries


def build_long_short_split_figure(liq_by_exchange: Dict[str, List[LiquidationEvent]],
                                   window_ms: int = 3_600_000) -> go.Figure:
    """多头/空头爆仓分组柱状图 — 按交易所"""
    fig = make_subplots(rows=1, cols=2, subplot_titles=("🔴 多头爆仓 Long Liquidations",
                                                          "🟢 空头爆仓 Short Liquidations"))
    now_ms = int(time.time() * 1000); cutoff = now_ms - window_ms
    for col_i, side in enumerate(["long","short"], start=1):
        for ek, evts in liq_by_exchange.items():
            filtered = [e for e in evts if e.side == side and e.timestamp_ms >= cutoff]
            if not filtered: continue
            ts  = [pd.to_datetime(e.timestamp_ms, unit="ms") for e in filtered]
            ns  = [e.notional or 0 for e in filtered]
            ps  = [e.price or 0 for e in filtered]
            szs = [max(6, min(20, (e.notional or 1)/8000)) for e in filtered]
            fig.add_trace(go.Scatter(x=ts, y=ps, mode="markers",
                name=ek.capitalize(), showlegend=(col_i == 1),
                marker=dict(size=szs, color=EX_COLORS.get(ek,"#aaa"),
                            symbol="triangle-down" if side=="long" else "triangle-up",
                            opacity=0.85),
                hovertemplate=f"{ek} {side} @%{{y:.2f}} ${'{ns}'}<extra></extra>"),
                row=1, col=col_i)
    fig.update_layout(height=340, paper_bgcolor=_BG, plot_bgcolor=_PLOT, font=_FONT,
        title=dict(text="多头 / 空头爆仓分视图  ·  Long vs Short Liquidation Split",
                   x=0.02, y=0.98, xanchor="left", font=dict(size=14, color="#f3f8ff")),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridcolor=_GRID, tickformat=".2f")
    return fig


def build_single_vs_cross_figure(clusters: List[LiquidationClusterV2]) -> go.Figure:
    """单所 vs 跨所联动爆仓簇 — 双色散点"""
    fig = go.Figure()
    single = [c for c in clusters if not c.cross_exchange]
    cross  = [c for c in clusters if c.cross_exchange]
    for group, color, name, symbol in [(single,"#62c2ff","单所爆仓簇","circle"),
                                        (cross,  "#f8d35e","🌐 跨所联动","star")]:
        if not group: continue
        ts  = [pd.to_datetime((c.start_ms+c.end_ms)//2, unit="ms") for c in group]
        ns  = [c.total_notional for c in group]
        szs = [max(8, min(40, c.total_notional/30_000)) for c in group]
        dom = [c.dominant_side for c in group]
        col = [_DN if d=="long" else _UP for d in dom]
        exc = ["+".join(sorted(c.exchanges)) for c in group]
        txt = [f"{nm}<br>${_fc(n)}<br>{ex}<br>瀑布{c.cascade_score:.2f}"
               for n, ex, c in zip(ns, exc, group)]
        fig.add_trace(go.Scatter(x=ts, y=ns, mode="markers", name=name,
            marker=dict(size=szs, color=col, symbol=symbol, opacity=0.9,
                        line=dict(color=color, width=1.5 if symbol=="star" else 0.5)),
            text=txt, hovertemplate="%{text}<extra></extra>"))
    if not clusters:
        fig.add_annotation(text="暂无爆仓簇数据", showarrow=False,
                           x=0.5, y=0.5, xref="paper", yref="paper")
    fig.update_layout(height=320, paper_bgcolor=_BG, plot_bgcolor=_PLOT, font=_FONT,
        title=dict(text="单所 vs 跨所联动爆仓簇  ·  星形=跨所联动(黄)，颜色=多头红/空头绿",
                   x=0.02, y=0.98, xanchor="left", font=dict(size=14, color="#f3f8ff")),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridcolor=_GRID, tickformat=".2s", title="总爆仓额")
    return fig


def build_cascade_timeline_figure(liq_by_exchange, clusters, window_ms=300_000):
    """连续爆仓簇甘特 + 各所散点时间轴"""
    fig = go.Figure()
    now_ms = int(time.time()*1000); cutoff = now_ms - window_ms
    EY = {"binance":4,"bybit":3,"okx":2,"hyperliquid":1}
    for ek, evts in liq_by_exchange.items():
        recent = [e for e in evts if e.timestamp_ms >= cutoff]
        if not recent: continue
        color = EX_COLORS.get(ek,"#aaa"); y_val = EY.get(ek,0)
        long_e  = [e for e in recent if e.side == "long"]
        short_e = [e for e in recent if e.side == "short"]
        for grp, sym, dy in [(long_e,"triangle-down",-0.15),(short_e,"triangle-up",0.15)]:
            if not grp: continue
            ts  = [pd.to_datetime(e.timestamp_ms, unit="ms") for e in grp]
            ps  = [e.price or 0 for e in grp]
            szs = [max(5, min(22, (e.notional or 1)/5000)) for e in grp]
            fig.add_trace(go.Scatter(x=ts, y=[y_val+dy]*len(grp), mode="markers",
                showlegend=False, marker=dict(size=szs, color=color, symbol=sym, opacity=0.9),
                hovertemplate=f"{ek} @%{{customdata:.2f}}<extra></extra>",
                customdata=ps))
    # Cluster highlights
    for c in clusters:
        if c.start_ms < cutoff: continue
        ts_s = pd.to_datetime(c.start_ms, unit="ms")
        ts_e = pd.to_datetime(max(c.end_ms, c.start_ms+500), unit="ms")
        bc   = "rgba(248,211,94,0.15)" if c.cross_exchange else "rgba(255,104,104,0.08)"
        lc   = "#f8d35e" if c.cross_exchange else "rgba(255,104,104,0.4)"
        ann  = "🌐" if c.cross_exchange else "●"
        fig.add_vrect(x0=ts_s, x1=ts_e, fillcolor=bc, line_color=lc, line_width=1,
                      annotation_text=ann, annotation_position="top left",
                      annotation_font=dict(size=9, color=lc))
    fig.update_layout(height=280, paper_bgcolor=_BG, plot_bgcolor=_PLOT, font=_FONT,
        title=dict(text="连续爆仓簇时间轴 (近5分钟)  ·  黄色=跨所联动",
                   x=0.02, y=0.98, xanchor="left", font=dict(size=13, color="#f3f8ff")),
        yaxis=dict(tickvals=[1,2,3,4], ticktext=["Hyperliquid","OKX","Bybit","Binance"],
                   showgrid=False, range=[0.3,4.7]))
    fig.update_xaxes(showgrid=False)
    return fig


def build_window_summary_bars(summaries: Dict[str, "MultiExchangeLiqSummary"]) -> go.Figure:
    """多时间窗口爆仓对比柱状图"""
    windows = list(summaries.keys())
    long_ns  = [summaries[w].long_notional  for w in windows]
    short_ns = [summaries[w].short_notional for w in windows]
    fig = go.Figure()
    fig.add_trace(go.Bar(x=windows, y=long_ns,  name="多头爆仓", marker_color=_DN,  opacity=0.85))
    fig.add_trace(go.Bar(x=windows, y=short_ns, name="空头爆仓", marker_color=_UP, opacity=0.85))
    fig.update_layout(height=260, barmode="group",
        paper_bgcolor=_BG, plot_bgcolor=_PLOT, font=_FONT,
        title=dict(text="各时间窗口爆仓对比",
                   x=0.02, y=0.98, xanchor="left", font=dict(size=13, color="#f3f8ff")),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis=dict(showgrid=False), yaxis=dict(showgrid=True, gridcolor=_GRID, tickformat=".2s"))
    return fig


def render_liq_center(liq_by_exchange: Dict, clusters: List, coin: str = "BTC"):
    """Main render entry for liquidation center"""
    st.markdown(f"""
    <div style="padding:14px 20px 10px;border-radius:22px;margin-bottom:12px;
        border:1px solid rgba(255,104,104,0.3);background:rgba(255,68,68,0.06);
        backdrop-filter:blur(20px);">
      <div style="font-size:0.68rem;color:#f9b;text-transform:uppercase;letter-spacing:0.18em;">
        爆仓中心 · Liquidation Intelligence Center</div>
      <div style="font-size:1.4rem;font-weight:800;color:#fff;margin-top:3px;">
        {coin} 五视角爆仓分析</div>
    </div>""", unsafe_allow_html=True)

    WINDOWS = {"5分钟": 300_000, "1小时": 3_600_000,
               "4小时": 14_400_000, "24小时": 86_400_000}
    summaries = _build_liq_summary(liq_by_exchange, clusters, WINDOWS)

    # ── KPI row ────────────────────────────────────────────────────────────────
    kc = st.columns(5)
    total_all = sum(s.long_notional + s.short_notional for s in summaries.values())
    s1h = summaries.get("1小时")
    s24 = summaries.get("24小时")
    kc[0].metric("24h总爆仓", _fc(s24.long_notional + s24.short_notional if s24 else 0))
    kc[1].metric("1h 多头爆仓", _fc(s1h.long_notional  if s1h else 0),
                 delta=f"占{s1h.long_notional/(max(s1h.long_notional+s1h.short_notional,1))*100:.0f}%" if s1h else None)
    kc[2].metric("1h 空头爆仓", _fc(s1h.short_notional if s1h else 0))
    kc[3].metric("爆仓簇", str(len(clusters)))
    kc[4].metric("跨所联动簇", str(sum(1 for c in clusters if c.cross_exchange)))

    # ── Window summary bars ────────────────────────────────────────────────────
    st.plotly_chart(build_window_summary_bars(summaries), key="lc_window_bars", use_container_width=True)

    # ── View 1: Long vs Short split ────────────────────────────────────────────
    st.markdown("#### 视角1：多头爆仓 vs 空头爆仓")
    st.plotly_chart(build_long_short_split_figure(liq_by_exchange), key="lc_ls_split", use_container_width=True)

    # ── View 2+3: Single vs Cross + Timeline ──────────────────────────────────
    v2l, v2r = st.columns(2, gap="large")
    with v2l:
        st.markdown("#### 视角2：单所 vs 跨所联动")
        st.plotly_chart(build_single_vs_cross_figure(clusters), key="lc_cross", use_container_width=True)
    with v2r:
        st.markdown("#### 视角3：连续爆仓簇时间轴")
        st.plotly_chart(build_cascade_timeline_figure(liq_by_exchange, clusters), key="lc_cascade", use_container_width=True)

    # ── View 4: Per-exchange breakdown table ──────────────────────────────────
    st.markdown("#### 视角4：各所爆仓明细")
    tbl_rows = []
    for ek, evts in liq_by_exchange.items():
        now_ms = int(time.time()*1000)
        for wlabel, w_ms in WINDOWS.items():
            cutoff = now_ms - w_ms
            w_evts = [e for e in evts if e.timestamp_ms >= cutoff]
            ln = sum(e.notional or 0 for e in w_evts if e.side == "long")
            sn = sum(e.notional or 0 for e in w_evts if e.side == "short")
            tbl_rows.append({"交易所": ek.capitalize(), "窗口": wlabel,
                "多头爆仓": ln, "空头爆仓": sn, "总计": ln+sn,
                "多头占%": round(ln/(max(ln+sn,1))*100, 1)})
    if tbl_rows:
        st.dataframe(pd.DataFrame(tbl_rows), use_container_width=True, hide_index=True,
            column_config={"多头爆仓": st.column_config.NumberColumn(format="%.0f"),
                "空头爆仓": st.column_config.NumberColumn(format="%.0f"),
                "总计": st.column_config.NumberColumn(format="%.0f"),
                "多头占%": st.column_config.ProgressColumn(format="%.1f%%", min_value=0, max_value=100)})

    # ── View 5: Cluster detail table ──────────────────────────────────────────
    st.markdown("#### 视角5：爆仓簇详情")
    if clusters:
        cluster_rows = [{
            "时间": pd.to_datetime(c.start_ms, unit="ms").strftime("%H:%M:%S"),
            "持续": f"{c.duration_ms//1000}s",
            "交易所": "+".join(sorted(c.exchanges)),
            "🌐跨所": "✅" if c.cross_exchange else "–",
            "多头": _fc(c.long_notional), "空头": _fc(c.short_notional),
            "总额": _fc(c.total_notional), "强度($/s)": _fc(c.intensity),
            "瀑布分": f"{c.cascade_score:.2f}",
            "主导": "多头" if c.dominant_side=="long" else "空头",
        } for c in sorted(clusters, key=lambda c: c.start_ms, reverse=True)[:30]]
        st.dataframe(pd.DataFrame(cluster_rows), use_container_width=True, hide_index=True)
    else:
        st.info("暂无爆仓簇（连续30s内超过$10万的多笔爆仓才触发）")
