"""
ob_center.py — v5 盘口中心
撤单速度 · 假挂单 · 补单 · 墙体寿命 · 近价流动性塌陷
"""
from __future__ import annotations
import time
from typing import Dict, List, Optional
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from models import (OrderBookQualitySnapshot, FakeWallCandidate, WallAbsorptionEvent,
                    WallLifePoint, NearLiquidityCollapse, LargeOrderFlow, LocalOrderBook)

_BG  = "rgba(14,22,35,0.56)"; _PLOT= "rgba(255,255,255,0.045)"
_FONT= dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif")
_GRID= "rgba(255,255,255,0.08)"
_UP  = "#1dc796"; _DN="#ff6868"; _WARN="#ffa94d"; _BLUE="#62c2ff"; _GOLD="#f8d35e"


def _fc(v):
    if v is None: return "–"
    v=float(v); av=abs(v)
    if av>=1e9: return f"{v/1e9:.2f}B"
    if av>=1e6: return f"{v/1e6:.2f}M"
    if av>=1e3: return f"{v/1e3:.1f}K"
    return f"{v:.2f}"


def build_cancel_speed_figure(quality_hist: List[OrderBookQualitySnapshot]) -> go.Figure:
    """撤单速度 vs 新增速度 双轴时序"""
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.05,
                        row_heights=[0.6, 0.4],
                        subplot_titles=("撤单 / 新增名义额 (K USD)", "撤单率 Cancel Ratio"))
    if not quality_hist:
        fig.add_annotation(text="等待盘口数据…", showarrow=False,
                           x=0.5, y=0.5, xref="paper", yref="paper")
        fig.update_layout(height=360, paper_bgcolor=_BG, plot_bgcolor=_PLOT, font=_FONT)
        return fig
    ts      = [pd.to_datetime(q.timestamp_ms, unit="ms") for q in quality_hist]
    b_add   = [q.bid_add_notional/1e3 for q in quality_hist]
    b_canc  = [q.bid_cancel_notional/1e3 for q in quality_hist]
    a_add   = [q.ask_add_notional/1e3 for q in quality_hist]
    a_canc  = [q.ask_cancel_notional/1e3 for q in quality_hist]
    # Cancel ratio = cancel / (add+cancel)
    canc_r  = [(b+a)/max(b+a+ba+aa, 1) for b,a,ba,aa in zip(b_canc,a_canc,b_add,a_add)]

    fig.add_trace(go.Bar(x=ts, y=b_add,  name="买单新增", marker_color=_UP,   opacity=0.8), row=1, col=1)
    fig.add_trace(go.Bar(x=ts, y=a_add,  name="卖单新增", marker_color=_BLUE, opacity=0.8), row=1, col=1)
    fig.add_trace(go.Bar(x=ts, y=[-v for v in b_canc], name="买单撤销", marker_color=_DN,   opacity=0.7), row=1, col=1)
    fig.add_trace(go.Bar(x=ts, y=[-v for v in a_canc], name="卖单撤销", marker_color=_WARN, opacity=0.7), row=1, col=1)
    # Cancel ratio line
    col_r = [_DN if v > 0.6 else _WARN if v > 0.4 else _UP for v in canc_r]
    fig.add_trace(go.Scatter(x=ts, y=canc_r, mode="lines+markers", name="撤单率",
        line=dict(color=_GOLD, width=1.8),
        marker=dict(color=col_r, size=5)), row=2, col=1)
    fig.add_hline(y=0.5, line_color="rgba(255,255,255,0.25)", line_dash="dot", row=2, col=1)
    fig.update_layout(height=380, barmode="relative",
        paper_bgcolor=_BG, plot_bgcolor=_PLOT, font=_FONT,
        title=dict(text="撤单速度监控  ·  Cancel Speed & Flow",
                   x=0.02, y=0.98, xanchor="left", font=dict(size=14, color="#f3f8ff")),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridcolor=_GRID)
    return fig


def build_wall_lifetime_figure(wall_hist: List[WallLifePoint],
                                active_walls: Dict) -> go.Figure:
    """墙体寿命分布 — 存活时间直方图 + 活墙散点"""
    fig = make_subplots(rows=1, cols=2, subplot_titles=("墙体存活时间分布", "当前活跃大墙"))
    if wall_hist:
        ages_s = [w.age_ms/1000 for w in wall_hist if not w.is_alive]
        bid_a  = [w.age_ms/1000 for w in wall_hist if not w.is_alive and w.side=="bid"]
        ask_a  = [w.age_ms/1000 for w in wall_hist if not w.is_alive and w.side=="ask"]
        if bid_a:
            fig.add_trace(go.Histogram(x=bid_a, name="买墙",
                marker_color=_UP, opacity=0.75, nbinsx=20), row=1, col=1)
        if ask_a:
            fig.add_trace(go.Histogram(x=ask_a, name="卖墙",
                marker_color=_DN, opacity=0.75, nbinsx=20), row=1, col=1)
        fig.add_vline(x=8, line_color=_GOLD, line_dash="dot", line_width=1, row=1, col=1)
        fig.add_annotation(x=8, y=0, text="8s假墙线", showarrow=False,
                           font=dict(size=9, color=_GOLD), row=1, col=1)

    if active_walls:
        prices  = list(active_walls.keys())
        walls   = list(active_walls.values())
        ages    = [w.age_ms/1000 for w in walls]
        notionals = [w.notional/1e3 for w in walls]
        sides   = [w.side for w in walls]
        colors  = [_UP if s=="bid" else _DN for s in sides]
        sizes   = [max(6, min(30, n)) for n in notionals]
        fig.add_trace(go.Scatter(x=ages, y=prices, mode="markers",
            name="活跃大墙", marker=dict(size=sizes, color=colors, opacity=0.85),
            hovertemplate="@%{y:.2f}<br>存活%{x:.0f}s<br>$%{customdata:.0f}K<extra></extra>",
            customdata=notionals), row=1, col=2)

    fig.update_layout(height=320, paper_bgcolor=_BG, plot_bgcolor=_PLOT, font=_FONT,
        title=dict(text="墙体寿命分析  ·  Wall Lifetime & Active Walls",
                   x=0.02, y=0.98, xanchor="left", font=dict(size=14, color="#f3f8ff")),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        barmode="overlay")
    fig.update_xaxes(showgrid=False, title_text="存活秒数", row=1, col=1)
    fig.update_xaxes(showgrid=False, title_text="存活秒数", row=1, col=2)
    fig.update_yaxes(showgrid=True, gridcolor=_GRID)
    return fig


def build_near_collapse_figure(collapses: List[NearLiquidityCollapse]) -> go.Figure:
    """近价流动性塌陷事件散点"""
    fig = go.Figure()
    if not collapses:
        fig.add_annotation(text="暂无近价流动性塌陷事件 (距中间价1.5%内)",
                           showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        fig.update_layout(height=240, paper_bgcolor=_BG, plot_bgcolor=_PLOT, font=_FONT)
        return fig
    ts     = [pd.to_datetime(c.timestamp_ms, unit="ms") for c in collapses]
    notional = [c.notional_lost/1e3 for c in collapses]
    dists  = [c.price_pct_from_mid for c in collapses]
    speeds = [c.collapse_speed_ms for c in collapses]
    colors = [_DN if c.side=="bid" else _WARN for c in collapses]
    sizes  = [max(8, min(32, n)) for n in notional]
    text   = [f"{'买墙' if c.side=='bid' else '卖墙'}塌陷<br>距中价 {c.price_pct_from_mid:.2f}%<br>"
              f"消失${c.notional_lost/1e3:.0f}K 速度{c.collapse_speed_ms}ms"
              for c in collapses]
    fig.add_trace(go.Scatter(x=ts, y=dists, mode="markers",
        marker=dict(size=sizes, color=colors, opacity=0.85,
                    line=dict(color="rgba(255,255,255,0.4)", width=1)),
        text=text, hovertemplate="%{text}<extra></extra>"))
    fig.update_layout(height=260, paper_bgcolor=_BG, plot_bgcolor=_PLOT, font=_FONT,
        title=dict(text="近价流动性塌陷  ·  Near-Price Liquidity Collapse (1.5%内大墙消失)",
                   x=0.02, y=0.98, xanchor="left", font=dict(size=13, color="#f3f8ff")))
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridcolor=_GRID, title="距中间价 (%)")
    return fig


def build_large_order_flow_figure(lof_list: List[LargeOrderFlow],
                                   threshold_k: float = 50.0) -> go.Figure:
    """大单流时间轴 — 主动买卖"""
    fig = go.Figure()
    if not lof_list:
        fig.add_annotation(text=f"暂无 >${threshold_k:.0f}K 大单（等待WS成交流…）",
                           showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        fig.update_layout(height=260, paper_bgcolor=_BG, plot_bgcolor=_PLOT, font=_FONT)
        return fig
    buys  = [l for l in lof_list if l.side == "buy"]
    sells = [l for l in lof_list if l.side == "sell"]
    for grp, color, name, sym in [(buys,_UP,"大单主动买▲","triangle-up"),
                                   (sells,_DN,"大单主动卖▼","triangle-down")]:
        if not grp: continue
        ts  = [pd.to_datetime(l.timestamp_ms, unit="ms") for l in grp]
        ps  = [l.price for l in grp]
        ns  = [l.notional for l in grp]
        szs = [max(6, min(28, n/8000)) for n in ns]
        fig.add_trace(go.Scatter(x=ts, y=ps, mode="markers", name=name,
            marker=dict(size=szs, color=color, symbol=sym, opacity=0.9),
            hovertemplate=f"{name} @%{{y:.2f}} $%{{customdata:,.0f}}<extra></extra>",
            customdata=ns))
    fig.update_layout(height=280, paper_bgcolor=_BG, plot_bgcolor=_PLOT, font=_FONT,
        title=dict(text=f"大单流  ·  Large Order Flow  (>${threshold_k:.0f}K)",
                   x=0.02, y=0.98, xanchor="left", font=dict(size=14, color="#f3f8ff")),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridcolor=_GRID, tickformat=".2f")
    return fig


def render_ob_center(quality_hist, fake_walls, absorptions,
                     wall_hist, active_walls,
                     collapses, large_flows, book: Optional[LocalOrderBook] = None,
                     exchange: str = ""):
    """Main render for orderbook center"""
    st.markdown(f"""
    <div style="padding:14px 20px 10px;border-radius:22px;margin-bottom:12px;
        border:1px solid rgba(98,194,255,0.3);background:rgba(30,60,120,0.12);
        backdrop-filter:blur(20px);">
      <div style="font-size:0.68rem;color:#9cf;text-transform:uppercase;letter-spacing:0.18em;">
        盘口中心 · Orderbook Intelligence Center</div>
      <div style="font-size:1.4rem;font-weight:800;color:#fff;margin-top:3px;">
        {exchange} 盘口质量 · 假挂单 · 补单 · 墙体 · 大单流</div>
    </div>""", unsafe_allow_html=True)

    # KPIs
    kc = st.columns(5)
    if quality_hist:
        q  = quality_hist[-1]
        kc[0].metric("盘口质量分", f"{q.quality_score:.2f}")
        kc[1].metric("买单净流", _fc(q.bid_net_notional))
        kc[2].metric("卖单净流", _fc(q.ask_net_notional))
    else:
        for i in range(3): kc[i].metric(["质量分","买净","卖净"][i], "–")
    kc[3].metric("假挂单(累计)", str(len(fake_walls)))
    kc[4].metric("近价塌陷", str(len(collapses)))

    # Row 1: Cancel speed + Wall lifetime
    r1l, r1r = st.columns(2, gap="large")
    with r1l:
        st.markdown("#### 撤单速度监控")
        st.plotly_chart(build_cancel_speed_figure(quality_hist), key="ob_cancel", config={'displayModeBar': True, 'scrollZoom': True})
    with r1r:
        st.markdown("#### 墙体寿命")
        st.plotly_chart(build_wall_lifetime_figure(wall_hist, active_walls), key="ob_wall_life", config={'displayModeBar': True, 'scrollZoom': True})

    # Row 2: Fake walls + Near collapse
    r2l, r2r = st.columns(2, gap="large")
    with r2l:
        from analytics import build_fake_wall_figure
        st.markdown("#### 假挂单 & 大单吸收")
        st.plotly_chart(build_fake_wall_figure(fake_walls, absorptions), key="ob_fake", config={'displayModeBar': True, 'scrollZoom': True})
    with r2r:
        st.markdown("#### 近价流动性塌陷")
        st.plotly_chart(build_near_collapse_figure(collapses), key="ob_collapse", config={'displayModeBar': True, 'scrollZoom': True})

    # Row 3: Large order flow
    st.markdown("#### 大单流 (主动买卖 > $50K)")
    st.plotly_chart(build_large_order_flow_figure(large_flows), key="ob_lof", config={'displayModeBar': True, 'scrollZoom': True})

    # Active walls table
    if active_walls:
        st.markdown("#### 当前活跃大墙（持续存在的挂单）")
        wall_rows = [{"方向": "买墙" if w.side=="bid" else "卖墙",
            "价格": w.price, "金额$K": round(w.notional/1e3, 1),
            "存活秒数": round(w.age_ms/1000, 1)} for w in active_walls.values()]
        wall_rows.sort(key=lambda x: x["金额$K"], reverse=True)
        st.dataframe(pd.DataFrame(wall_rows[:20]), width='stretch', hide_index=True,
            column_config={"价格": st.column_config.NumberColumn(format="%.2f"),
                "金额$K": st.column_config.NumberColumn(format="%.1f"),
                "存活秒数": st.column_config.NumberColumn(format="%.1f")})

    # Fake wall history
    if fake_walls:
        st.markdown("#### 假挂单历史（最近20条）")
        fw_rows = [{"时间": pd.to_datetime(f.timestamp_ms,unit="ms").strftime("%H:%M:%S"),
            "方向": "买" if f.side=="bid" else "卖",
            "价格": f.price, "峰值$K": round(f.peak_notional/1e3,1),
            "存续ms": f.lifespan_ms} for f in list(fake_walls)[-20:]]
        st.dataframe(pd.DataFrame(fw_rows), width='stretch', hide_index=True,
            column_config={"价格": st.column_config.NumberColumn(format="%.2f"),
                "峰值$K": st.column_config.NumberColumn(format="%.1f")})
