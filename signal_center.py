"""
signal_center.py  —  信号增强中心 Tab（v6 → v7）
覆盖：套利信号 / 情绪评分 / VPIN / 微结构 / K线形态 / 回测 / 跨所主导权
P1 升级：合成信号权重可调 UI / 市场热力图扫描 Tab
"""
from __future__ import annotations
import time
from typing import Dict, List, Optional

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from aggregator import (
    detect_arbitrage_signals, detect_funding_arbitrage,
    build_aggregated_oi, build_exchange_dominance,
    compute_sentiment_score, detect_candle_patterns,
    detect_microstructure_anomalies, backtest_candle_signal,
    VPINCalculator,
    # P1 新增
    compute_composite_score, DEFAULT_CS_WEIGHTS,
    build_market_heatmap, build_market_heatmap_figure,
)
from collections import defaultdict
from models import (
    ExchangeSnapshot, Candle, VPINPoint, MicrostructureAnomaly,
    CandlePatternSignal, MarketSentimentScore,
    AggregatedOIPoint, CrossExArbitrageSignal, CrossExFundingArb,
)


# ══════════════════════════════════════════════════════════════════════════════
# 图表
# ══════════════════════════════════════════════════════════════════════════════

def build_sentiment_gauge(score: float, label: str, color: str) -> go.Figure:
    """情绪仪表盘"""
    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=score,
        delta={"reference": 0},
        title={"text": label, "font": {"size": 18, "color": "#e0eeff"}},
        gauge={
            "axis": {"range": [-100, 100], "tickwidth": 1, "tickcolor": "#aaa",
                     "nticks": 9},
            "bar": {"color": color, "thickness": 0.3},
            "bgcolor": "rgba(0,0,0,0)",
            "borderwidth": 0,
            "steps": [
                {"range": [-100, -60], "color": "rgba(255,60,60,0.25)"},
                {"range": [-60, -20], "color": "rgba(255,120,60,0.18)"},
                {"range": [-20, 20],  "color": "rgba(150,150,150,0.12)"},
                {"range": [20, 60],   "color": "rgba(60,200,80,0.18)"},
                {"range": [60, 100],  "color": "rgba(30,240,100,0.25)"},
            ],
            "threshold": {"line": {"color": "#fff", "width": 3}, "value": score},
        },
        number={"font": {"size": 32, "color": color}},
    ))
    fig.update_layout(
        height=260, template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=30, r=30, t=30, b=10),
    )
    return fig


def build_vpin_figure(vpin_pts: List[VPINPoint]) -> go.Figure:
    """VPIN 时序图"""
    if not vpin_pts:
        return go.Figure()
    ts    = [p.timestamp_ms for p in vpin_pts]
    vpin  = [p.vpin for p in vpin_pts]
    alert_ts   = [p.timestamp_ms for p in vpin_pts if p.alert]
    alert_vals = [p.vpin          for p in vpin_pts if p.alert]

    colors = ["#ff5555" if v > 0.7 else "#ffaa44" if v > 0.5 else "#55aaff"
              for v in vpin]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=ts, y=vpin, mode="lines",
        line=dict(color="#88aaff", width=1.5),
        name="VPIN",
        fill="tozeroy", fillcolor="rgba(136,170,255,0.1)",
    ))
    fig.add_hline(y=0.7, line_dash="dash", line_color="#ff5555",
                  annotation_text="高毒性 0.7", annotation_font_color="#ff5555")
    fig.add_hline(y=0.5, line_dash="dot", line_color="#ffaa44",
                  annotation_text="中毒性 0.5", annotation_font_color="#ffaa44")
    if alert_ts:
        fig.add_trace(go.Scatter(
            x=alert_ts, y=alert_vals, mode="markers",
            marker=dict(color="#ff5555", size=8, symbol="triangle-up"),
            name="告警",
        ))
    fig.update_layout(
        height=300, template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        xaxis_title="时间", yaxis_title="VPIN",
        yaxis=dict(range=[0, 1]),
        margin=dict(l=40, r=20, t=30, b=40),
    )
    return fig


def build_arbitrage_figure(signals: List[CrossExArbitrageSignal]) -> go.Figure:
    """套利信号柱状图"""
    if not signals:
        return go.Figure()
    signals = signals[:20]
    coins  = [s.coin for s in signals]
    bps    = [s.spread_bps for s in signals]
    colors = ["#ff4444" if s.severity == "high" else "#ffaa33" if s.severity == "medium" else "#55aaff"
              for s in signals]
    hover = [f"{s.high_exchange} vs {s.low_exchange}<br>高: {s.high_price:.4f}  低: {s.low_price:.4f}"
             for s in signals]
    fig = go.Figure(go.Bar(
        x=coins, y=bps, marker_color=colors,
        text=[f"{b:.1f}" for b in bps], textposition="outside",
        hovertext=hover, hoverinfo="text",
    ))
    fig.add_hline(y=5, line_dash="dash", line_color="#ffaa33",
                  annotation_text="5 bps 阈值")
    fig.update_layout(
        height=350, template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        yaxis_title="价差 (bps)",
        margin=dict(l=40, r=20, t=30, b=60),
    )
    return fig


def build_dominance_figure(dominance_history: List) -> go.Figure:
    """交易所OI份额趋势图"""
    if not dominance_history:
        return go.Figure()
    ex_colors = {
        "binance": "#f0b90b", "bybit": "#ff6b00",
        "okx": "#00b4d8", "hyperliquid": "#7c3aed",
    }
    fig = go.Figure()
    exchanges = ["binance", "bybit", "okx", "hyperliquid"]
    for ex in exchanges:
        ts   = [d.timestamp_ms for d in dominance_history]
        vals = [d.oi_shares.get(ex, 0) for d in dominance_history]
        fig.add_trace(go.Scatter(
            x=ts, y=vals, mode="lines",
            name=ex.capitalize(),
            line=dict(color=ex_colors.get(ex, "#aaa"), width=2),
            stackgroup="one",
            fill="tonexty",
        ))
    fig.update_layout(
        height=320, template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        yaxis_title="OI 份额 (%)", yaxis=dict(range=[0, 100]),
        legend=dict(orientation="h", y=1.05),
        margin=dict(l=40, r=20, t=30, b=40),
    )
    return fig


def build_pattern_figure(signals: List[CandlePatternSignal],
                          candles: List[Candle]) -> go.Figure:
    """K线形态信号叠加图"""
    if not candles:
        return go.Figure()
    fig = go.Figure()

    # Candlestick
    fig.add_trace(go.Candlestick(
        x=[c.timestamp_ms for c in candles],
        open=[c.open for c in candles],
        high=[c.high for c in candles],
        low=[c.low  for c in candles],
        close=[c.close for c in candles],
        name="K线",
        increasing_line_color="#44cc88",
        decreasing_line_color="#ff5555",
    ))

    # Pattern markers
    bullish_sigs = [s for s in signals if s.direction == "bullish"]
    bearish_sigs = [s for s in signals if s.direction == "bearish"]

    if bullish_sigs:
        fig.add_trace(go.Scatter(
            x=[s.timestamp_ms for s in bullish_sigs],
            y=[s.price * 0.998 for s in bullish_sigs],
            mode="markers+text",
            marker=dict(symbol="triangle-up", color="#44cc88", size=12),
            text=[s.pattern.replace("_", " ") for s in bullish_sigs],
            textposition="bottom center",
            name="看涨形态",
        ))
    if bearish_sigs:
        fig.add_trace(go.Scatter(
            x=[s.timestamp_ms for s in bearish_sigs],
            y=[s.price * 1.002 for s in bearish_sigs],
            mode="markers+text",
            marker=dict(symbol="triangle-down", color="#ff5555", size=12),
            text=[s.pattern.replace("_", " ") for s in bearish_sigs],
            textposition="top center",
            name="看跌形态",
        ))

    fig.update_layout(
        height=450, template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        xaxis_rangeslider_visible=False,
        legend=dict(orientation="h", y=1.05),
        margin=dict(l=40, r=20, t=30, b=40),
    )
    return fig


# ══════════════════════════════════════════════════════════════════════════════
# 主渲染
# ══════════════════════════════════════════════════════════════════════════════

def render_signal_center(snapshots: List[ExchangeSnapshot],
                          candles_by_exchange: Dict[str, List[Candle]],
                          vpin_calculators: Dict[str, "VPINCalculator"],
                          liq_events,
                          ob_spread_hist: Dict = None,
                          ob_depth_hist: Dict  = None,
                          dominance_history: List = None):
    """信号增强中心 Tab"""
    st.markdown("""
    <div class="glass-section">
        <div class="glass-kicker">SIGNAL INTELLIGENCE</div>
        <div style="font-size:1.1rem;font-weight:600;color:#e8f4ff;">
            🧬 信号增强中心 v6
        </div>
        <div style="color:#a8c4e0;font-size:0.85rem;margin-top:4px;">
            跨所套利 · 资金费率套利 · 多因子情绪 · VPIN毒性 · K线形态 · 微结构异常 · 交易所主导权
        </div>
    </div>
    """, unsafe_allow_html=True)

    sub_tabs = st.tabs([
        "⚡ 套利监控",
        "🌡️ 情绪评分",
        "🧪 VPIN毒性",
        "📊 K线形态",
        "🏛️ 交易所主导权",
        "🔬 微结构异常",
        "💥 聚合爆仓流",
        "🐋 大单流量聚合",
        "🎭 墙体消失告警",
        "📈 多币种横向对比",
        "⚖️ 信号权重调节",   # P1 新增
        "🗺️ 市场热力扫描",   # P1 新增
    ])

    # ── 套利监控 ──
    with sub_tabs[0]:
        st.markdown("#### 实时套利机会扫描")

        c1, c2 = st.columns(2)
        with c1:
            min_bps = st.slider("最小价差阈值 (bps)", 1.0, 30.0, 5.0, key="arb_min_bps")
        with c2:
            min_fr_bps = st.slider("最小费率差阈值 (bps)", 1.0, 20.0, 3.0, key="arb_min_fr")

        arb_sigs = detect_arbitrage_signals(snapshots, min_spread_bps=min_bps)
        fr_sigs  = detect_funding_arbitrage(snapshots, min_net_bps=min_fr_bps)

        st.markdown("##### 价格套利信号")
        if arb_sigs:
            st.plotly_chart(build_arbitrage_figure(arb_sigs), config={'displayModeBar': True, 'scrollZoom': True})
            arb_rows = [{
                "": {"high": "🔴", "medium": "🟠", "low": "🟡"}.get(s.severity, "⚪"),
                "币种": s.coin,
                "高价所": s.high_exchange,
                "低价所": s.low_exchange,
                "高价": f"{s.high_price:.4f}",
                "低价": f"{s.low_price:.4f}",
                "价差(bps)": f"{s.spread_bps:.2f}",
                "套利空间": f"{s.arbitrage_pct:.3f}%",
            } for s in arb_sigs[:15]]
            st.dataframe(pd.DataFrame(arb_rows), width='stretch', hide_index=True)
        else:
            st.info("当前无显著价差套利机会")

        st.markdown("---")
        st.markdown("##### 资金费率套利信号")
        if fr_sigs:
            fr_rows = [{
                "": {"high": "🔴", "medium": "🟠", "low": "🟡"}.get(s.severity, "⚪"),
                "币种": s.coin,
                "做多所": s.long_exchange,
                "做空所": s.short_exchange,
                "多头费率(bps)": f"{s.long_rate_bps:.2f}",
                "空头费率(bps)": f"{s.short_rate_bps:.2f}",
                "净收益(bps/期)": f"{s.net_rate_bps:.2f}",
                "年化估算": f"{s.annual_yield_pct:.1f}%",
            } for s in fr_sigs[:15]]
            st.dataframe(pd.DataFrame(fr_rows), width='stretch', hide_index=True)
        else:
            st.info("当前无显著资金费率套利机会")

    # ── 情绪评分 ──
    with sub_tabs[1]:
        st.markdown("#### 多因子市场情绪评分")
        st.caption("综合 OI变化 / 资金费率 / 多空比 / 爆仓比 / VPIN 五大因子，得分 -100（极度恐惧）到 +100（极度贪婪）")

        score_cols = st.columns(min(4, len(snapshots)) or 1)
        for i, snap in enumerate(snapshots[:4]):
            if snap.status != "ok":
                continue
            ex = snap.exchange
            oi_pts  = []  # Would be injected from service in real usage
            liq_evs = [e for e in (liq_events or []) if e.exchange == ex]
            vpin_val = None
            if ex in vpin_calculators:
                vpin_val = vpin_calculators[ex].current_vpin()

            score = compute_sentiment_score(snap, oi_pts, liq_evs, vpin_val,
                                             snap.long_short_ratio)
            with score_cols[i % 4]:
                st.markdown(f"**{ex.capitalize()}**")
                st.plotly_chart(
                    build_sentiment_gauge(score.composite, score.label, score.color),
                    key=f"sg_{ex}",
                    config={'displayModeBar': True, 'scrollZoom': True})
                factor_data = {
                    "因子": ["OI", "资金费率", "多空比", "爆仓比", "VPIN"],
                    "得分": [
                        f"{score.oi_score*100:.0f}",
                        f"{score.funding_score*100:.0f}",
                        f"{score.ls_score*100:.0f}",
                        f"{score.liq_score*100:.0f}",
                        f"{score.vpin_score*100:.0f}",
                    ],
                }
                st.dataframe(pd.DataFrame(factor_data), width='stretch', hide_index=True)

    # ── VPIN ──
    with sub_tabs[2]:
        st.markdown("#### VPIN — 订单流毒性指标")
        st.caption("VPIN > 0.7 = 高毒性，知情交易者可能入场，方向性行情概率上升")

        if not vpin_calculators:
            st.info("VPIN 需要 WebSocket 实时交易数据流。请确保 WS 服务已启动。")
        else:
            ex_sel = st.selectbox("选择交易所", list(vpin_calculators.keys()),
                                   key="vpin_ex_sel")
            if ex_sel and ex_sel in vpin_calculators:
                vpin_hist = vpin_calculators[ex_sel].get_history()
                if vpin_hist:
                    cur_vpin = vpin_calculators[ex_sel].current_vpin()
                    c1, c2, c3 = st.columns(3)
                    c1.metric("当前 VPIN", f"{cur_vpin:.3f}" if cur_vpin else "-")
                    alert_count = sum(1 for p in vpin_hist if p.alert)
                    c2.metric("告警次数", alert_count)
                    c3.metric("样本桶数", len(vpin_hist))
                    st.plotly_chart(build_vpin_figure(vpin_hist), config={'displayModeBar': True, 'scrollZoom': True})
                else:
                    st.info("正在积累交易数据以计算 VPIN（需要至少50个成交量桶）")

    # ── K线形态 ──
    with sub_tabs[3]:
        st.markdown("#### K线技术形态识别")

        ex_sel_p = st.selectbox("选择交易所", list(candles_by_exchange.keys()) or ["binance"],
                                 key="pattern_ex")
        candles = candles_by_exchange.get(ex_sel_p, [])

        if candles:
            from exchanges import default_symbols
            symbol  = ""
            if snapshots:
                snap = next((s for s in snapshots if s.exchange == ex_sel_p), snapshots[0])
                symbol = snap.symbol

            patterns = detect_candle_patterns(candles, ex_sel_p, symbol)

            if patterns:
                st.success(f"检测到 {len(patterns)} 个形态信号")
                st.plotly_chart(build_pattern_figure(patterns, candles[-100:]), config={'displayModeBar': True, 'scrollZoom': True})

                p_rows = [{
                    "时间": pd.Timestamp(p.timestamp_ms, unit="ms").strftime("%m-%d %H:%M"),
                    "形态": p.pattern.replace("_", " "),
                    "方向": "🟢 看涨" if p.direction == "bullish" else ("🔴 看跌" if p.direction == "bearish" else "⚪ 中性"),
                    "置信度": f"{p.confidence:.0%}",
                    "价格": f"{p.price:.4f}",
                } for p in sorted(patterns, key=lambda x: x.timestamp_ms, reverse=True)]
                st.dataframe(pd.DataFrame(p_rows), width='stretch', hide_index=True)

                # Quick backtest
                st.markdown("---")
                st.markdown("##### 快速回测（持有 N 根K线）")
                hold_bars = st.slider("持有根数", 1, 10, 3, key="bt_hold")
                if st.button("运行回测", key="run_bt"):
                    result = backtest_candle_signal(candles, patterns, hold_bars,
                                                     ex_sel_p, "", "")
                    if result:
                        r1, r2, r3, r4 = st.columns(4)
                        r1.metric("总信号数", result.total_signals)
                        r2.metric("胜率", f"{result.win_rate:.1%}")
                        r3.metric("平均收益", f"{result.avg_return_pct:.2f}%")
                        r4.metric("Sharpe", f"{result.sharpe:.2f}")
                        st.metric("最大回撤", f"{result.max_drawdown_pct:.2f}%")
                    else:
                        st.warning("信号数量不足，无法回测")
            else:
                st.info("当前 K 线数据中未检测到显著形态")
        else:
            st.info("请先在深度终端加载 K 线数据")

    # ── 交易所主导权 ──
    with sub_tabs[4]:
        st.markdown("#### 交易所市场份额动态")
        st.caption("OI 份额反映资金在哪里集中；份额快速转移可能预示跨所套利或流动性迁移")

        sel_coin_dom = st.selectbox("选择币种", ["BTC", "ETH", "SOL", "XRP"],
                                     key="dom_coin")
        agg = build_aggregated_oi(snapshots, sel_coin_dom)
        if agg:
            cols = st.columns(4 + 1)
            cols[0].metric("总 OI", f"${agg.total_notional/1e9:.3f}B")
            for i, ex in enumerate(["binance", "bybit", "okx", "hyperliquid"]):
                v = agg.by_exchange.get(ex, 0)
                pct = v / agg.total_notional * 100 if agg.total_notional else 0
                cols[i + 1].metric(ex.capitalize(), f"${v/1e9:.3f}B", f"{pct:.1f}%")

        if dominance_history:
            st.plotly_chart(build_dominance_figure(dominance_history), config={'displayModeBar': True, 'scrollZoom': True})
        else:
            # Build current snapshot bar
            if agg:
                fig_dom = go.Figure()
                ex_colors = {
                    "binance": "#f0b90b", "bybit": "#ff6b00",
                    "okx": "#00b4d8", "hyperliquid": "#7c3aed",
                }
                for ex, oi in agg.by_exchange.items():
                    pct = oi / agg.total_notional * 100 if agg.total_notional else 0
                    fig_dom.add_trace(go.Bar(
                        x=[ex.capitalize()], y=[pct],
                        name=ex.capitalize(),
                        marker_color=ex_colors.get(ex, "#aaa"),
                        text=[f"{pct:.1f}%"], textposition="outside",
                    ))
                fig_dom.update_layout(
                    height=320, template="plotly_dark",
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    yaxis_title="OI 份额 (%)",
                    showlegend=False,
                    margin=dict(l=40, r=20, t=30, b=40),
                )
                st.plotly_chart(fig_dom, config={'displayModeBar': True, 'scrollZoom': True})

    # ── 微结构异常 ──
    with sub_tabs[5]:
        st.markdown("#### 市场微结构异常检测")
        st.caption("价差突扩 / 深度塌陷 = 流动性危机信号；通常先于大行情出现")

        if ob_spread_hist and ob_depth_hist:
            anomalies = detect_microstructure_anomalies(snapshots, ob_spread_hist, ob_depth_hist)
            if anomalies:
                st.error(f"⚠️ 检测到 {len(anomalies)} 个微结构异常！")
                a_rows = [{
                    "交易所": a.exchange,
                    "类型": a.anomaly_type,
                    "严重度": {"high": "🔴 高", "medium": "🟠 中", "low": "🟡 低"}.get(a.severity, a.severity),
                    "详情": a.detail,
                } for a in anomalies]
                st.dataframe(pd.DataFrame(a_rows), width='stretch', hide_index=True)
            else:
                st.success("✅ 当前无微结构异常")
        else:
            st.info("微结构异常检测需要盘口价差和深度历史数据，请确保 WS 服务已运行")

    # ── 聚合爆仓流 ──
    with sub_tabs[6]:
        st.markdown("#### 四所聚合爆仓事件流")
        st.caption("将 Binance / Bybit / OKX / Hyperliquid 的爆仓事件合并，按时间倒序展示")

        all_liq = liq_events or []
        if all_liq:
            # Sort by time desc
            sorted_liq = sorted(all_liq, key=lambda e: e.timestamp_ms or 0, reverse=True)

            # Stats
            now_ms = int(__import__('time').time() * 1000)
            liq_1h  = [e for e in sorted_liq if (e.timestamp_ms or 0) > now_ms - 3600_000]
            long_1h  = sum(e.notional or 0 for e in liq_1h if e.side == "long")
            short_1h = sum(e.notional or 0 for e in liq_1h if e.side == "short")

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("1h 总爆仓", f"${(long_1h+short_1h)/1e6:.2f}M")
            c2.metric("1h 多头爆", f"${long_1h/1e6:.2f}M", delta_color="inverse")
            c3.metric("1h 空头爆", f"${short_1h/1e6:.2f}M")
            c4.metric("事件总数", len(liq_1h))

            # Aggregated bar chart by exchange
            by_ex = defaultdict(float)
            for e in liq_1h:
                by_ex[e.exchange] += e.notional or 0

            if by_ex:
                ex_colors = {"Binance":"#f0b90b","bybit":"#ff6b00","Bybit":"#ff6b00",
                             "binance":"#f0b90b","okx":"#00b4d8","OKX":"#00b4d8",
                             "Hyperliquid":"#7c3aed","hyperliquid":"#7c3aed"}
                fig_liq_agg = go.Figure(go.Bar(
                    x=list(by_ex.keys()),
                    y=[v/1e6 for v in by_ex.values()],
                    marker_color=[ex_colors.get(ex, "#aaa") for ex in by_ex.keys()],
                    text=[f"${v/1e6:.2f}M" for v in by_ex.values()],
                    textposition="outside",
                ))
                fig_liq_agg.update_layout(
                    height=300, template="plotly_dark",
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    yaxis_title="爆仓额 (百万$)", showlegend=False,
                    margin=dict(l=40,r=20,t=30,b=40),
                )
                st.plotly_chart(fig_liq_agg, key="agg_liq_bar",
                                config={'displayModeBar': True, 'scrollZoom': True})

            # Event table
            min_notional = st.slider("最小爆仓额过滤 ($)", 0, 500000, 10000,
                                      step=10000, key="agg_liq_min",
                                      format="$%d")
            rows = [{
                "时间": pd.Timestamp(e.timestamp_ms, unit="ms").strftime("%H:%M:%S"),
                "交易所": e.exchange,
                "方向": "🔴 多头" if e.side=="long" else "🟢 空头",
                "价格": f"{e.price:.4f}" if e.price else "-",
                "数量": f"{e.size:.4f}" if e.size else "-",
                "名义值": f"${(e.notional or 0)/1e3:.1f}K",
            } for e in sorted_liq[:200] if (e.notional or 0) >= min_notional]
            if rows:
                st.dataframe(pd.DataFrame(rows), width='stretch', hide_index=True)
            else:
                st.info(f"暂无超过 ${min_notional:,} 的爆仓记录")
        else:
            st.info("等待 WebSocket 爆仓数据... 请确保 WS 服务已启动")

    # ── 大单流量聚合 ──
    with sub_tabs[7]:
        st.markdown("#### 跨所大单流量聚合 — 鲸鱼踪迹")
        st.caption("汇聚四所的大额成交单，识别鲸鱼主动吃单方向")

        # Collect large order flows from ob_center data passed in
        # Use liq_events as proxy, or show from service if available
        col1, col2 = st.columns(2)
        with col1:
            min_whale = st.number_input("大单阈值 (美元)", value=100000, step=50000,
                                         key="whale_threshold", format="%d")
        with col2:
            whale_window = st.selectbox("时间窗口", ["5分钟", "15分钟", "1小时"],
                                         key="whale_window")

        window_ms = {"5分钟": 300_000, "15分钟": 900_000, "1小时": 3_600_000}[whale_window]
        now_ms = int(__import__('time').time() * 1000)
        cutoff_ms = now_ms - window_ms

        # Filter large liquidations as proxy for large directional moves
        large_events = [e for e in (liq_events or [])
                        if (e.notional or 0) >= min_whale
                        and (e.timestamp_ms or 0) > cutoff_ms]

        if large_events:
            buy_total  = sum(e.notional or 0 for e in large_events if e.side == "short")  # short liq = forced buy
            sell_total = sum(e.notional or 0 for e in large_events if e.side == "long")   # long liq = forced sell

            c1, c2, c3 = st.columns(3)
            c1.metric("大单买入压力", f"${buy_total/1e6:.2f}M")
            c2.metric("大单卖出压力", f"${sell_total/1e6:.2f}M")
            net = buy_total - sell_total
            c3.metric("净方向", f"${net/1e6:+.2f}M",
                      delta="偏多" if net > 0 else "偏空",
                      delta_color="normal" if net > 0 else "inverse")

            # Bubble chart
            import plotly.express as px
            bubble_data = []
            for e in sorted(large_events, key=lambda x: x.notional or 0, reverse=True)[:50]:
                bubble_data.append({
                    "时间": pd.Timestamp(e.timestamp_ms, unit="ms"),
                    "名义值(M)": (e.notional or 0) / 1e6,
                    "交易所": e.exchange,
                    "方向": "强制买入" if e.side == "short" else "强制卖出",
                    "颜色": "#44cc88" if e.side == "short" else "#ff5555",
                })
            df_bubble = pd.DataFrame(bubble_data)
            fig_bubble = go.Figure()
            for direction, color in [("强制买入", "#44cc88"), ("强制卖出", "#ff5555")]:
                sub = df_bubble[df_bubble["方向"] == direction]
                if not sub.empty:
                    fig_bubble.add_trace(go.Scatter(
                        x=sub["时间"], y=sub["名义值(M)"],
                        mode="markers",
                        name=direction,
                        marker=dict(
                            size=sub["名义值(M)"].apply(lambda v: max(8, min(40, v*10))),
                            color=color, opacity=0.8,
                        ),
                        text=sub["交易所"],
                        hovertemplate="%{text}<br>$%{y:.2f}M<extra></extra>",
                    ))
            fig_bubble.update_layout(
                height=380, template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                xaxis_title="时间", yaxis_title="名义值 (百万$)",
                legend=dict(orientation="h", y=1.05),
                margin=dict(l=40,r=20,t=30,b=40),
            )
            st.plotly_chart(fig_bubble, key="whale_bubble",
                            config={'displayModeBar': True, 'scrollZoom': True})

            # Table
            rows = [{
                "时间": pd.Timestamp(e.timestamp_ms, unit="ms").strftime("%H:%M:%S"),
                "交易所": e.exchange,
                "方向": "🟢 强制买入" if e.side=="short" else "🔴 强制卖出",
                "名义值": f"${(e.notional or 0)/1e6:.3f}M",
                "价格": f"{e.price:.4f}" if e.price else "-",
            } for e in sorted(large_events, key=lambda x: x.notional or 0, reverse=True)[:30]]
            st.dataframe(pd.DataFrame(rows), width='stretch', hide_index=True)
        else:
            st.info(f"窗口内暂无超过 ${min_whale:,} 的大单记录")

    # ── 墙体消失告警 ──
    with sub_tabs[8]:
        st.markdown("#### 墙体消失 + 吸收事件告警")
        st.caption("大挂单瞬间撤离（fake wall）或被吃掉后快速补单（absorption）是重要的方向信号")

        # Get fake walls and absorption events from ob data
        fake_walls_all = []
        absorb_all = []

        # Try to get from service via session state
        for ek in ["binance", "bybit", "okx", "hyperliquid"]:
            fw = st.session_state.get(f"fake_walls_{ek}", [])
            ab = st.session_state.get(f"absorb_{ek}", [])
            fake_walls_all.extend(fw)
            absorb_all.extend(ab)

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("##### 🎭 疑似假挂单（出现后快速撤离）")
            if fake_walls_all:
                fw_rows = [{
                    "交易所": fw.exchange,
                    "方向": "买墙" if fw.side=="bid" else "卖墙",
                    "价格": f"{fw.price:.4f}",
                    "峰值名义": f"${fw.peak_notional/1e3:.0f}K",
                    "存续时间": f"{fw.lifespan_ms/1000:.1f}s",
                    "时间": pd.Timestamp(fw.timestamp_ms, unit="ms").strftime("%H:%M:%S"),
                } for fw in sorted(fake_walls_all, key=lambda x: x.timestamp_ms, reverse=True)[:20]]
                st.dataframe(pd.DataFrame(fw_rows), width='stretch', hide_index=True)
            else:
                st.info("暂无假挂单记录（需要盘口 WS 数据）")

        with col2:
            st.markdown("##### 🧲 墙体吸收事件（被吃掉后快速补单）")
            if absorb_all:
                ab_rows = [{
                    "交易所": ab.exchange,
                    "方向": "买墙" if ab.side=="bid" else "卖墙",
                    "价格": f"{ab.price:.4f}",
                    "被吃名义": f"${ab.absorbed_notional/1e3:.0f}K",
                    "补单名义": f"${ab.refill_notional/1e3:.0f}K",
                    "补单延迟": f"{ab.refill_delay_ms}ms",
                } for ab in sorted(absorb_all, key=lambda x: x.timestamp_ms, reverse=True)[:20]]
                st.dataframe(pd.DataFrame(ab_rows), width='stretch', hide_index=True)
            else:
                st.info("暂无吸收事件记录（需要盘口 WS 数据）")

        st.markdown("---")
        st.info("💡 提示：假挂单和吸收事件由 `ob_center.py` 的 WS 盘口追踪模块实时计算，"
                "请在「盘口中心」Tab 中查看详细可视化分析")

    # ── 多币种横向对比 ──
    with sub_tabs[9]:
        st.markdown("#### 多币种横向对比分析")
        st.caption("同框对比 BTC / ETH / SOL 等主流币的关键指标，快速发现结构分化")

        coins_input = st.multiselect(
            "选择对比币种",
            ["BTC","ETH","SOL","XRP","BNB","DOGE","ADA","SUI","AVAX","LINK","HYPE","TAO"],
            default=["BTC","ETH","SOL"],
            key="multi_coin_sel",
        )
        metric_sel = st.selectbox(
            "对比指标",
            ["资金费率(bps)", "OI 名义值", "24h 成交量", "价格变化%", "多空比"],
            key="multi_coin_metric",
        )

        if coins_input and snapshots:
            # Build comparison data from current snapshots
            metric_map = defaultdict(dict)
            for snap in snapshots:
                if snap.status != "ok": continue
                coin = snap.symbol.replace("USDT","").replace("-USDT-SWAP","")
                if coin not in coins_input: continue
                if metric_sel == "资金费率(bps)":
                    v = (snap.funding_rate or 0) * 10000
                elif metric_sel == "OI 名义值":
                    v = (snap.open_interest_notional or 0) / 1e9
                elif metric_sel == "24h 成交量":
                    v = (snap.volume_24h_notional or 0) / 1e9
                elif metric_sel == "多空比":
                    v = snap.long_short_ratio or 0
                else:
                    v = 0
                metric_map[coin][snap.exchange] = v

            if metric_map:
                # Grouped bar chart
                fig_mc = go.Figure()
                ex_colors = {"binance":"#f0b90b","bybit":"#ff6b00","okx":"#00b4d8","hyperliquid":"#7c3aed"}
                for ex in ["binance","bybit","okx","hyperliquid"]:
                    vals = [metric_map.get(coin,{}).get(ex, 0) for coin in coins_input]
                    fig_mc.add_trace(go.Bar(
                        name=ex.capitalize(),
                        x=coins_input, y=vals,
                        marker_color=ex_colors.get(ex,"#aaa"),
                    ))
                y_label = {"资金费率(bps)":"费率(bps)","OI 名义值":"OI(十亿$)",
                           "24h 成交量":"成交量(十亿$)","多空比":"多空比","价格变化%":"%"}
                fig_mc.update_layout(
                    barmode="group", height=380, template="plotly_dark",
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    yaxis_title=y_label.get(metric_sel, metric_sel),
                    legend=dict(orientation="h", y=1.08),
                    margin=dict(l=40,r=20,t=40,b=40),
                )
                st.plotly_chart(fig_mc, key="multi_coin_bar",
                                config={'displayModeBar': True, 'scrollZoom': True})

                # Summary table
                summary_rows = []
                for coin in coins_input:
                    row = {"币种": coin}
                    for ex in ["binance","bybit","okx","hyperliquid"]:
                        v = metric_map.get(coin,{}).get(ex)
                        if metric_sel == "资金费率(bps)":
                            row[ex] = f"{v:.3f}" if v is not None else "-"
                        elif metric_sel in ("OI 名义值","24h 成交量"):
                            row[ex] = f"${v:.3f}B" if v else "-"
                        else:
                            row[ex] = f"{v:.4f}" if v else "-"
                    summary_rows.append(row)
                st.dataframe(pd.DataFrame(summary_rows), width='stretch', hide_index=True)

                # Heatmap view
                st.markdown("---")
                st.markdown("##### 热力矩阵视图")
                import plotly.express as px
                heat_data = []
                for coin in coins_input:
                    for ex in ["binance","bybit","okx","hyperliquid"]:
                        v = metric_map.get(coin,{}).get(ex, 0)
                        heat_data.append({"币种":coin,"交易所":ex.capitalize(),"值":v})
                if heat_data:
                    df_heat = pd.DataFrame(heat_data).pivot(index="币种",columns="交易所",values="值")
                    fig_heat = go.Figure(go.Heatmap(
                        z=df_heat.values.tolist(),
                        x=list(df_heat.columns),
                        y=list(df_heat.index),
                        colorscale="RdYlGn",
                        text=[[f"{v:.3f}" for v in row] for row in df_heat.values],
                        texttemplate="%{text}",
                    ))
                    fig_heat.update_layout(
                        height=max(200, len(coins_input)*60+100),
                        template="plotly_dark",
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)",
                        margin=dict(l=60,r=20,t=30,b=60),
                    )
                    st.plotly_chart(fig_heat, key="multi_coin_heat",
                                    config={'displayModeBar': True, 'scrollZoom': True})
            else:
                st.info("当前快照中暂无所选币种数据")
        else:
            st.info("请选择至少一个币种并等待数据加载")

    # ══════════════════════════════════════════════════════════════════════════
    # P1 新增 Tab 10 — 合成信号权重调节器
    # ══════════════════════════════════════════════════════════════════════════
    with sub_tabs[10]:
        st.markdown("#### ⚖️ 合成信号权重调节")
        st.caption(
            "调节各因子在合成信号中的权重，实时预览不同权重方案下的信号结果。"
            "调整后权重会存储在 session_state 中，供当前会话的其他模块参考。"
        )

        st.markdown("---")
        col_w1, col_w2 = st.columns([1, 1])

        with col_w1:
            st.markdown("##### 权重设置（合计自动归一）")
            w_price   = st.slider("价格动能  Price",   0, 50, int(DEFAULT_CS_WEIGHTS["price"]   * 100), 5, key="cw_price")
            w_oi      = st.slider("OI 方向   OI",      0, 50, int(DEFAULT_CS_WEIGHTS["oi"]      * 100), 5, key="cw_oi")
            w_cvd     = st.slider("CVD 流向  CVD",     0, 50, int(DEFAULT_CS_WEIGHTS["cvd"]     * 100), 5, key="cw_cvd")
            w_funding = st.slider("资金费率  Funding", 0, 50, int(DEFAULT_CS_WEIGHTS["funding"] * 100), 5, key="cw_funding")
            w_crowd   = st.slider("拥挤度    Crowd",   0, 50, int(DEFAULT_CS_WEIGHTS["crowd"]   * 100), 5, key="cw_crowd")

        raw_total = w_price + w_oi + w_cvd + w_funding + w_crowd
        if raw_total == 0:
            raw_total = 1
        custom_weights = {
            "price":   w_price   / raw_total,
            "oi":      w_oi      / raw_total,
            "cvd":     w_cvd     / raw_total,
            "funding": w_funding / raw_total,
            "crowd":   w_crowd   / raw_total,
        }
        # 写入 session_state 供 realtime.py 的 _CS_WEIGHTS 参考
        st.session_state["composite_weights"] = custom_weights

        with col_w2:
            st.markdown("##### 当前归一化权重")
            w_rows = [
                {"因子": "价格动能", "权重": f"{custom_weights['price']:.1%}"},
                {"因子": "OI 方向",  "权重": f"{custom_weights['oi']:.1%}"},
                {"因子": "CVD 流向", "权重": f"{custom_weights['cvd']:.1%}"},
                {"因子": "资金费率", "权重": f"{custom_weights['funding']:.1%}"},
                {"因子": "拥挤度",   "权重": f"{custom_weights['crowd']:.1%}"},
            ]
            st.dataframe(pd.DataFrame(w_rows), width="stretch", hide_index=True)

            # 预设方案
            st.markdown("##### 快速预设方案")
            preset_col1, preset_col2, preset_col3 = st.columns(3)
            if preset_col1.button("📊 均衡", key="preset_balanced"):
                for k in ["cw_price","cw_oi","cw_cvd","cw_funding","cw_crowd"]:
                    st.session_state[k] = 20
                st.rerun()
            if preset_col2.button("🌊 流动性优先", key="preset_flow"):
                st.session_state["cw_price"]   = 10
                st.session_state["cw_oi"]       = 30
                st.session_state["cw_cvd"]      = 40
                st.session_state["cw_funding"]  = 10
                st.session_state["cw_crowd"]    = 10
                st.rerun()
            if preset_col3.button("💰 费率优先", key="preset_funding"):
                st.session_state["cw_price"]   = 15
                st.session_state["cw_oi"]       = 20
                st.session_state["cw_cvd"]      = 15
                st.session_state["cw_funding"]  = 35
                st.session_state["cw_crowd"]    = 15
                st.rerun()

        st.markdown("---")
        st.markdown("##### 当前权重下的实时预览")

        # 用当前快照因子重新计算合成信号
        preview_cols = st.columns(min(4, len(snapshots)) or 1)
        for i, snap in enumerate([s for s in snapshots if s.status == "ok"][:4]):
            # 构造近似因子得分（与 realtime.py 逻辑一致）
            fr = snap.funding_rate or 0.0
            funding_score = max(-1., min(1., -fr * 3000))
            crowd_score   = max(-1., min(1., -fr * 5000))
            # price_score / oi_score / cvd_score 无历史，此处用 0 作占位
            composite, confidence, label, color = compute_composite_score(
                price_score=0.0, oi_score=0.0, cvd_score=0.0,
                funding_score=funding_score, crowd_score=crowd_score,
                weights=custom_weights,
            )
            with preview_cols[i % 4]:
                st.markdown(
                    f'<div style="padding:10px;border-radius:14px;border:1px solid {color}40;'
                    f'background:rgba(255,255,255,0.05);text-align:center;">'
                    f'<div style="font-size:0.75rem;color:#bcd;">{snap.exchange.capitalize()}</div>'
                    f'<div style="font-size:1.1rem;font-weight:800;color:{color};">{label}</div>'
                    f'<div style="font-size:0.78rem;color:#aac;">'
                    f'合成 <b style="color:{color};">{composite:+.2f}</b>  '
                    f'置信 <b>{confidence:.0%}</b></div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
        st.info("💡 注意：预览仅含资金费率/拥挤度因子（实时历史因子需 WS 服务积累数据后方可完整计算）")

    # ══════════════════════════════════════════════════════════════════════════
    # P1 新增 Tab 11 — 市场热力扫描
    # ══════════════════════════════════════════════════════════════════════════
    with sub_tabs[11]:
        st.markdown("#### 🗺️ 全市场热力扫描")
        st.caption(
            "将全市场扫描结果以 Treemap 热力图呈现：块面积 = 指标绝对值大小，"
            "颜色 = 方向（绿=正向/多头，红=负向/空头）。"
        )

        hm_metric = st.selectbox(
            "热力指标",
            options=[
                ("oi_change_1h_pct",  "OI 1h变化%"),
                ("funding_bps",       "资金费率(bps)"),
                ("liq_1h_notional",   "1h爆仓额"),
                ("vol_change_pct",    "成交量变化%"),
                ("price_change_pct",  "价格变化%"),
            ],
            format_func=lambda x: x[1],
            key="hm_metric_sel",
        )
        metric_key = hm_metric[0] if isinstance(hm_metric, tuple) else hm_metric

        # 尝试从 session_state 取上次扫描结果（由 homepage 的 MarketScanClient 填充）
        _scan_rows = st.session_state.get("last_market_scan_rows", [])

        if _scan_rows:
            hm_data = build_market_heatmap(_scan_rows, metric=metric_key)
            hm_fig  = build_market_heatmap_figure(hm_data)
            if hm_fig:
                st.plotly_chart(hm_fig, use_container_width=True,
                                config={"displayModeBar": False})

                # 明细表格
                if hm_data:
                    tbl_rows = [
                        {"币种": c, "值": t, "排名": i + 1}
                        for i, (c, t) in enumerate(zip(hm_data["coins"], hm_data["texts"]))
                    ]
                    st.dataframe(pd.DataFrame(tbl_rows), width="stretch", hide_index=True)
            else:
                st.warning("热力图数据构建失败，请检查市场扫描数据格式")
        else:
            st.info(
                "暂无市场扫描数据。请先在「全市场首页」Tab 触发一次扫描，"
                "或等待自动扫描完成（约 30s 缓存）。"
            )

        st.markdown("---")
        st.caption(
            "数据来源：`exchanges.MarketScanClient.fetch_market_batch`，"
            "缓存 TTL 30s。热力图仅展示当前快照，历史回溯请使用「推送&历史数据」Tab。"
        )
