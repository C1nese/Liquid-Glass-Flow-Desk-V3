"""
hl_center.py  —  Hyperliquid 专属数据中心 Tab（v6）
覆盖：预测资金费率 / 排行榜聪明钱 / Vault监控 / 链上清算密度 / 鲸鱼持仓
"""
from __future__ import annotations
import time
from typing import List, Optional

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from hl_client import (
    fetch_predicted_fundings, fetch_leaderboard, fetch_whale_positions,
    fetch_vault_list, fetch_vault_detail, fetch_liquidation_density,
    compare_funding_vs_exchanges,
)
from exchanges import fetch_all_exchange_fundings
from models import HLPredictedFunding, HLLeaderEntry, HLVaultInfo, HLLiquidationDensity


# ══════════════════════════════════════════════════════════════════════════════
# 图表构建
# ══════════════════════════════════════════════════════════════════════════════

def build_predicted_funding_figure(items: List[HLPredictedFunding],
                                    top_n: int = 30) -> go.Figure:
    """预测资金费率 vs 当前费率对比图"""
    items = sorted(items, key=lambda x: abs(x.predicted_rate_bps), reverse=True)[:top_n]
    coins  = [i.coin for i in items]
    pred   = [i.predicted_rate_bps for i in items]
    curr   = [i.current_rate_bps   for i in items]
    deltas = [i.rate_delta_bps      for i in items]

    colors = ["#ff5555" if d > 0 else "#55aaff" for d in deltas]

    fig = make_subplots(rows=2, cols=1, subplot_titles=["资金费率 (bps)", "预测变化量 (bps)"],
                        vertical_spacing=0.12)
    fig.add_trace(go.Bar(x=coins, y=curr,  name="当前费率", marker_color="#5588ff", opacity=0.7), row=1, col=1)
    fig.add_trace(go.Bar(x=coins, y=pred,  name="预测费率", marker_color="#ffaa33", opacity=0.85), row=1, col=1)
    fig.add_trace(go.Bar(x=coins, y=deltas, name="变化量",
                         marker_color=colors, opacity=0.9), row=2, col=1)
    fig.add_hline(y=0, line_dash="dash", line_color="rgba(255,255,255,0.3)", row=1, col=1)
    fig.add_hline(y=0, line_dash="dash", line_color="rgba(255,255,255,0.3)", row=2, col=1)
    fig.update_layout(
        height=520, barmode="group", template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", y=1.08),
        margin=dict(l=40, r=20, t=50, b=40),
    )
    return fig


def build_vault_tvl_figure(vaults: List[HLVaultInfo], top_n: int = 15) -> go.Figure:
    """Vault TVL 柱状图"""
    vaults = sorted(vaults, key=lambda v: v.tvl, reverse=True)[:top_n]
    names  = [v.name[:16] for v in vaults]
    tvls   = [v.tvl / 1e6 for v in vaults]
    inflows = [v.net_inflow_24h / 1e6 for v in vaults]
    apr    = [v.apr_30d or 0 for v in vaults]

    inflow_colors = ["#44cc88" if x >= 0 else "#ff5555" for x in inflows]

    fig = make_subplots(rows=1, cols=2, subplot_titles=["TVL (百万$)", "24h 净流入 (百万$)"],
                        horizontal_spacing=0.08)
    fig.add_trace(go.Bar(x=names, y=tvls, marker_color="#6688ff",
                         name="TVL", text=[f"${t:.2f}M" for t in tvls],
                         textposition="outside"), row=1, col=1)
    fig.add_trace(go.Bar(x=names, y=inflows, marker_color=inflow_colors,
                         name="净流入"), row=1, col=2)
    fig.update_layout(
        height=400, template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        showlegend=False, margin=dict(l=40, r=20, t=50, b=80),
    )
    fig.update_xaxes(tickangle=-45)
    return fig


def build_liquidation_density_figure(density: List[HLLiquidationDensity],
                                      current_price: float = None) -> go.Figure:
    """链上清算价格密度图"""
    if not density:
        return go.Figure()

    prices = [d.price for d in density]
    longs  = [d.long_notional / 1e6  for d in density]
    shorts = [d.short_notional / 1e6 for d in density]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=longs, y=prices, orientation="h",
        name="多头清算", marker_color="#ff5555",
        text=[f"${v:.2f}M" if v > 0.1 else "" for v in longs],
        textposition="outside",
    ))
    fig.add_trace(go.Bar(
        x=[-s for s in shorts], y=prices, orientation="h",
        name="空头清算", marker_color="#55aaff",
    ))

    if current_price:
        fig.add_hline(y=current_price, line_dash="dash", line_color="#ffcc33",
                      annotation_text=f"当前价 {current_price:.2f}",
                      annotation_font_color="#ffcc33")

    fig.update_layout(
        height=550, barmode="relative",
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        xaxis_title="清算金额 (百万$)", yaxis_title="价格",
        legend=dict(orientation="h", y=1.05),
        margin=dict(l=60, r=20, t=50, b=40),
    )
    return fig


def build_whale_positions_figure(positions, top_n: int = 20) -> go.Figure:
    """鲸鱼持仓气泡图（按名义价值）"""
    if not positions:
        return go.Figure()

    positions = sorted(positions, key=lambda p: p.notional, reverse=True)[:top_n]
    longs  = [p for p in positions if p.side == "long"]
    shorts = [p for p in positions if p.side == "short"]

    fig = go.Figure()
    if longs:
        fig.add_trace(go.Scatter(
            x=[p.notional / 1e6 for p in longs],
            y=[p.unrealized_pnl / 1e3 if p.unrealized_pnl else 0 for p in longs],
            mode="markers+text",
            text=[p.coin for p in longs],
            textposition="top center",
            marker=dict(
                size=[min(40, max(8, p.notional / 2e5)) for p in longs],
                color="#44cc88", opacity=0.8,
                line=dict(width=1, color="#88ffbb")
            ),
            name="多头持仓",
            hovertemplate=(
                "<b>%{text}</b><br>"
                "名义: $%{x:.2f}M<br>"
                "未实现盈亏: $%{y:.1f}K<br>"
                "<extra></extra>"
            ),
        ))
    if shorts:
        fig.add_trace(go.Scatter(
            x=[p.notional / 1e6 for p in shorts],
            y=[p.unrealized_pnl / 1e3 if p.unrealized_pnl else 0 for p in shorts],
            mode="markers+text",
            text=[p.coin for p in shorts],
            textposition="top center",
            marker=dict(
                size=[min(40, max(8, p.notional / 2e5)) for p in shorts],
                color="#ff5555", opacity=0.8,
                line=dict(width=1, color="#ff8888")
            ),
            name="空头持仓",
            hovertemplate=(
                "<b>%{text}</b><br>"
                "名义: $%{x:.2f}M<br>"
                "未实现盈亏: $%{y:.1f}K<br>"
                "<extra></extra>"
            ),
        ))
    fig.add_hline(y=0, line_dash="dash", line_color="rgba(255,255,255,0.3)")
    fig.update_layout(
        height=450, template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        xaxis_title="持仓名义价值 (百万$)", yaxis_title="未实现盈亏 (千$)",
        legend=dict(orientation="h", y=1.05),
        margin=dict(l=50, r=20, t=50, b=40),
    )
    return fig


# ══════════════════════════════════════════════════════════════════════════════
# 主渲染函数
# ══════════════════════════════════════════════════════════════════════════════

def render_hl_center():
    """Hyperliquid 专属数据中心，嵌入 app.py 的新 Tab"""
    st.markdown("""
    <div class="glass-section">
        <div class="glass-kicker">HYPERLIQUID EXCLUSIVE</div>
        <div style="font-size:1.1rem;font-weight:600;color:#e8f4ff;">
            ⛓️ Hyperliquid 链上专属数据中心
        </div>
        <div style="color:#a8c4e0;font-size:0.85rem;margin-top:4px;">
            预测资金费率 · 排行榜聪明钱 · Vault金库监控 · 链上清算密度 · 鲸鱼持仓追踪
        </div>
    </div>
    """, unsafe_allow_html=True)

    sub_tabs = st.tabs([
        "💰 预测资金费率",
        "🏆 排行榜/聪明钱",
        "🏦 Vault 金库",
        "🔥 链上清算密度",
        "🐳 鲸鱼持仓",
    ])

    # ── 预测资金费率 ──
    with sub_tabs[0]:
        st.markdown("#### HL 预测资金费率 vs 当前费率")
        st.caption("预测费率为下一期（8h）预测值，正值=多头付空头，负值=空头付多头")

        col_btn, col_cfg = st.columns([1, 3])
        with col_btn:
            top_n = st.slider("显示数量", 10, 50, 25, key="hl_fr_topn")
        with col_cfg:
            show_arb = st.checkbox("显示跨所套利机会", value=True, key="hl_show_arb")

        with st.spinner("获取 HL 预测费率..."):
            pred_fundings = fetch_predicted_fundings()

        if pred_fundings:
            st.plotly_chart(build_predicted_funding_figure(pred_fundings, top_n), config={'displayModeBar': True, 'scrollZoom': True})

            # Table
            rows = []
            for pf in pred_fundings[:top_n]:
                delta_str = f"{pf.rate_delta_bps:+.2f}"
                rows.append({
                    "币种": pf.coin,
                    "当前费率(bps)": f"{pf.current_rate_bps:.3f}",
                    "预测费率(bps)": f"{pf.predicted_rate_bps:.3f}",
                    "预测变化": delta_str,
                    "方向": "↑ 上调" if pf.rate_delta_bps > 0 else "↓ 下调",
                })
            df = pd.DataFrame(rows)
            st.dataframe(df, width='stretch', hide_index=True,
                         column_config={
                             "预测变化": st.column_config.TextColumn("预测变化"),
                         })

            if show_arb:
                st.markdown("---")
                st.markdown("#### 跨所资金费率套利机会")
                st.caption("基于 HL 预测费率 vs 其他交易所当前费率对比")
                # Get other exchanges funding for selected coins
                top_coins = [pf.coin for pf in pred_fundings[:10]]
                ex_fundings: dict = {}
                for coin in top_coins[:5]:  # limit to avoid too many requests
                    rates = fetch_all_exchange_fundings(coin)
                    for ex, rate in rates.items():
                        if rate is not None:
                            ex_fundings.setdefault(ex, {})[coin] = rate

                arb_opps = compare_funding_vs_exchanges(pred_fundings[:20], ex_fundings)
                if arb_opps:
                    arb_rows = []
                    for opp in arb_opps[:15]:
                        sev_icon = {"low": "🟡", "medium": "🟠", "high": "🔴"}.get(opp["severity"], "⚪")
                        arb_rows.append({
                            "": sev_icon,
                            "币种": opp["coin"],
                            "做多所": opp["long_exchange"],
                            "做空所": opp["short_exchange"],
                            "多头费率(bps)": f"{opp['long_rate_bps']:.2f}",
                            "空头费率(bps)": f"{opp['short_rate_bps']:.2f}",
                            "净收益(bps/期)": f"{opp['net_bps']:.2f}",
                            "年化收益率": f"{opp['annual_yield_pct']:.1f}%",
                        })
                    st.dataframe(pd.DataFrame(arb_rows), width='stretch', hide_index=True)
                else:
                    st.info("暂无显著套利机会（净差 < 3 bps）")
        else:
            st.warning("无法获取 HL 预测资金费率数据")

    # ── 排行榜/聪明钱 ──
    with sub_tabs[1]:
        st.markdown("#### Hyperliquid 排行榜 — 聪明钱追踪")

        col1, col2 = st.columns([1, 2])
        with col1:
            window = st.selectbox("时间窗口", ["day", "week", "month", "allTime"],
                                   index=2, key="hl_lb_window",
                                   format_func=lambda x: {"day":"今日","week":"本周","month":"本月","allTime":"全部"}.get(x,x))
            top_n_lb = st.slider("显示数量", 10, 50, 20, key="hl_lb_topn")
        with col2:
            st.info("💡 点击地址可查看该鲸鱼的当前持仓明细（在下方鲸鱼持仓Tab）")

        with st.spinner("获取排行榜数据..."):
            leaders = fetch_leaderboard(window=window, top_n=top_n_lb)

        if leaders:
            lb_rows = []
            for l in leaders:
                pnl_str = f"${l.pnl_30d/1e6:.2f}M" if l.pnl_30d else "-"
                roi_str = f"{l.roi_30d*100:.1f}%" if l.roi_30d else "-"
                vol_str = f"${l.volume_30d/1e6:.1f}M" if l.volume_30d else "-"
                lb_rows.append({
                    "排名": l.rank,
                    "名称": l.display_name,
                    "地址": l.address[:12] + "…" if l.address else "-",
                    "期间盈亏": pnl_str,
                    "ROI": roi_str,
                    "交易量": vol_str,
                })
            st.dataframe(pd.DataFrame(lb_rows), width='stretch', hide_index=True)

            # Quick select to view positions
            st.markdown("---")
            st.markdown("##### 查看指定地址持仓")
            addr_input = st.text_input("输入 ETH 地址（0x...）或从上方表格复制",
                                        key="hl_whale_addr")
            if addr_input and addr_input.startswith("0x"):
                with st.spinner(f"获取 {addr_input[:12]}... 持仓..."):
                    positions = fetch_whale_positions(addr_input)
                if positions:
                    pos_rows = []
                    for p in positions:
                        pos_rows.append({
                            "币种": p.coin,
                            "方向": "🟢 多" if p.side == "long" else "🔴 空",
                            "数量": f"{p.size:.4f}",
                            "名义值": f"${p.notional/1e6:.3f}M",
                            "开仓价": f"{p.entry_price:.4f}" if p.entry_price else "-",
                            "当前价": f"{p.mark_price:.4f}" if p.mark_price else "-",
                            "未实现盈亏": f"${p.unrealized_pnl:.2f}" if p.unrealized_pnl else "-",
                            "杠杆": f"{p.leverage:.0f}x" if p.leverage else "-",
                        })
                    st.dataframe(pd.DataFrame(pos_rows), width='stretch', hide_index=True)
                    fig_pos = build_whale_positions_figure(positions)
                    st.plotly_chart(fig_pos, config={'displayModeBar': True, 'scrollZoom': True})
                else:
                    st.info("该地址无持仓或数据获取失败")
        else:
            st.warning("无法获取排行榜数据")

    # ── Vault 金库 ──
    with sub_tabs[2]:
        st.markdown("#### Hyperliquid Vault 金库监控")
        st.caption("TVL = 总锁仓价值；净流入24h = 今日资金流动（正=流入）")

        with st.spinner("获取 Vault 列表..."):
            vaults = fetch_vault_list()

        if vaults:
            st.plotly_chart(build_vault_tvl_figure(vaults), config={'displayModeBar': True, 'scrollZoom': True})

            # Table
            v_rows = []
            for v in vaults[:20]:
                v_rows.append({
                    "名称": v.name[:20],
                    "TVL": f"${v.tvl/1e6:.3f}M",
                    "30d APR": f"{v.apr_30d*100:.1f}%" if v.apr_30d else "-",
                    "跟随者": v.follower_count,
                    "30d PnL": f"${v.pnl_30d/1e6:.3f}M" if v.pnl_30d else "-",
                    "地址": v.vault_address[:12] + "…",
                })
            st.dataframe(pd.DataFrame(v_rows), width='stretch', hide_index=True)

            # Vault detail
            st.markdown("---")
            st.markdown("##### 查看 Vault 详情（包含净流入）")
            vault_addr = st.text_input("输入 Vault 地址", key="hl_vault_addr")
            if vault_addr:
                with st.spinner("获取详情..."):
                    detail = fetch_vault_detail(vault_addr)
                if detail:
                    cols = st.columns(4)
                    cols[0].metric("TVL", f"${detail.tvl/1e6:.3f}M")
                    cols[1].metric("24h净流入", f"${detail.net_inflow_24h/1e6:.3f}M",
                                   delta_color="normal")
                    cols[2].metric("30d APR", f"{detail.apr_30d*100:.1f}%" if detail.apr_30d else "-")
                    cols[3].metric("跟随者", detail.follower_count)
        else:
            st.warning("无法获取 Vault 数据（API 可能已更新）")

    # ── 链上清算密度 ──
    with sub_tabs[3]:
        st.markdown("#### 链上清算价格密度图")
        st.caption("基于排行榜地址的持仓反推清算价格分布（样本数据，非全量）")

        col1, col2, col3 = st.columns(3)
        with col1:
            hl_coin = st.selectbox("选择币种", ["BTC", "ETH", "SOL", "ARB", "AVAX"],
                                    key="hl_density_coin")
        with col2:
            range_pct = st.slider("价格范围 (%)", 2.0, 15.0, 5.0, key="hl_density_range")
        with col3:
            buckets = st.slider("桶数", 20, 60, 40, key="hl_density_buckets")

        if st.button("计算清算密度", key="hl_calc_density"):
            with st.spinner(f"计算 {hl_coin} 清算密度（需要约30秒）..."):
                density = fetch_liquidation_density(hl_coin, range_pct, buckets)

            if density:
                # Get current price
                from hl_client import _post as _hl_post
                try:
                    mids = _hl_post("/info", {"type": "allMids"})
                    cur_price = float(mids.get(hl_coin, 0)) if mids else None
                except:
                    cur_price = None

                fig_den = build_liquidation_density_figure(density, cur_price)
                st.plotly_chart(fig_den, config={'displayModeBar': True, 'scrollZoom': True})

                total_long  = sum(d.long_notional  for d in density)
                total_short = sum(d.short_notional for d in density)
                c1, c2, c3 = st.columns(3)
                c1.metric("多头清算密度总量", f"${total_long/1e6:.2f}M")
                c2.metric("空头清算密度总量", f"${total_short/1e6:.2f}M")
                c3.metric("样本地址数", sum(d.address_count for d in density))
            else:
                st.warning("数据不足，无法绘制密度图")

    # ── 鲸鱼持仓 ──
    with sub_tabs[4]:
        st.markdown("#### 排行榜 Top 持仓聚合")
        st.caption("获取 HL 排行榜前 N 名地址的持仓，聚合展示市场方向")

        top_n_whale = st.slider("追踪 Top N 地址", 5, 30, 10, key="hl_whale_topn")

        if st.button("扫描鲸鱼持仓", key="hl_scan_whale"):
            with st.spinner(f"扫描 Top {top_n_whale} 地址持仓（约需1分钟）..."):
                leaders = fetch_leaderboard(top_n=top_n_whale)
                all_positions = []
                progress = st.progress(0)
                for i, leader in enumerate(leaders):
                    if leader.address:
                        positions = fetch_whale_positions(leader.address)
                        all_positions.extend(positions)
                    progress.progress((i + 1) / len(leaders))

            if all_positions:
                st.success(f"共获取 {len(all_positions)} 个持仓记录")

                fig_whale = build_whale_positions_figure(all_positions, top_n=25)
                st.plotly_chart(fig_whale, config={'displayModeBar': True, 'scrollZoom': True})

                # Aggregate by coin
                from collections import defaultdict
                coin_long  = defaultdict(float)
                coin_short = defaultdict(float)
                for p in all_positions:
                    if p.side == "long":  coin_long[p.coin]  += p.notional
                    else:                  coin_short[p.coin] += p.notional

                all_coins = set(list(coin_long.keys()) + list(coin_short.keys()))
                agg_rows = []
                for coin in sorted(all_coins, key=lambda c: coin_long.get(c, 0) + coin_short.get(c, 0), reverse=True)[:20]:
                    total = coin_long.get(coin, 0) + coin_short.get(coin, 0)
                    long_pct = coin_long.get(coin, 0) / total * 100 if total > 0 else 0
                    agg_rows.append({
                        "币种": coin,
                        "多头名义": f"${coin_long.get(coin,0)/1e6:.2f}M",
                        "空头名义": f"${coin_short.get(coin,0)/1e6:.2f}M",
                        "总敞口": f"${total/1e6:.2f}M",
                        "多空比": f"{long_pct:.0f}% / {100-long_pct:.0f}%",
                        "倾向": "🟢 偏多" if long_pct > 60 else ("🔴 偏空" if long_pct < 40 else "⚖️ 中性"),
                    })
                st.dataframe(pd.DataFrame(agg_rows), width='stretch', hide_index=True)
            else:
                st.warning("未获取到持仓数据")
