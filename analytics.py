from __future__ import annotations

import math
import time
from typing import Dict, List, Optional, Tuple

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from models import (
    Candle, CVDPoint, ExchangeSnapshot, FuturesOIPoint, IcebergAlert,
    LiquidationEvent, LiquidityGap, LocalOrderBook, OIDeltaPoint, OIPoint,
    OrderBookLevel, TopTraderRatio, TradeEvent,
)

EMPTY_FRAME_COLS = ["价格区间","价格中位","热度","方向","归因"]

_CHART_BG  = "rgba(14,22,35,0.56)"
_PLOT_BG   = "rgba(255,255,255,0.045)"
_FONT      = dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif")
_LEGEND    = dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1.0)
_MARGIN    = dict(l=12, r=12, t=58, b=12)
_GRID_COL  = "rgba(255,255,255,0.08)"

_QUADRANT_COLOR = {"Long Add":"#1dc796","Short Cover":"#62c2ff","Short Add":"#ff6868","Long Unwind":"#ffa94d"}
_QUADRANT_CN    = {"Long Add":"多头加仓 ↑价↑OI","Short Cover":"空头回补 ↑价↓OI",
                   "Short Add":"空头加仓 ↓价↑OI","Long Unwind":"多头减仓 ↓价↓OI"}


def clamp01(v: float) -> float:
    return max(0.0, min(1.0, v))


def _normalize(values: List[float]) -> List[float]:
    if not values: return []
    mx = max(values)
    if mx <= 0: return [0.0]*len(values)
    return [v/mx for v in values]


def _layout(**kwargs) -> dict:
    base = dict(paper_bgcolor=_CHART_BG, plot_bgcolor=_PLOT_BG, font=_FONT,
                legend=_LEGEND, margin=_MARGIN,
                transition=dict(duration=280, easing="cubic-in-out"))
    base.update(kwargs)
    return base


def _parse_side_label(side: str) -> str:
    return {"long":"多头爆仓","short":"空头爆仓"}.get(side, side or "未知")


# ══════════════════════════════════════════════════════════════════════════
# 1. Orderbook summary
# ══════════════════════════════════════════════════════════════════════════

def summarize_orderbook(levels: List[OrderBookLevel], ref: Optional[float]) -> Dict:
    bids = [l for l in levels if l.side=="bid" and l.size>0]
    asks = [l for l in levels if l.side=="ask" and l.size>0]
    if not bids and not asks:
        return dict(bid_size=None,ask_size=None,bid_notional=None,ask_notional=None,imbalance_pct=None,spread_bps=None)
    bid_sz = sum(l.size for l in bids); ask_sz = sum(l.size for l in asks)
    bid_n  = sum(l.price*l.size for l in bids); ask_n = sum(l.price*l.size for l in asks)
    total  = bid_n + ask_n
    imbal  = (bid_n-ask_n)/total*100 if total>0 else None
    top_bid = max((l.price for l in bids), default=None)
    top_ask = min((l.price for l in asks), default=None)
    spread_bps = (top_ask-top_bid)/ref*10000 if (top_bid and top_ask and ref) else None
    return dict(bid_size=bid_sz,ask_size=ask_sz,bid_notional=bid_n,ask_notional=ask_n,
                imbalance_pct=imbal,spread_bps=spread_bps)


# ══════════════════════════════════════════════════════════════════════════
# 2. Local orderbook depth figure  (WebSocket维护)
# ══════════════════════════════════════════════════════════════════════════

def build_local_book_figure(book: LocalOrderBook, depth: int = 30) -> go.Figure:
    """绘制本地WebSocket维护的实时订单簿深度图。
    蓝色=买盘 Bid / 红色=卖盘 Ask。条越长=该价位挂单越厚。
    """
    fig = go.Figure()
    if not book.is_ready:
        fig.add_annotation(text="等待本地订单簿建立 (WebSocket快照初始化中…)", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        fig.update_layout(height=380, **_layout(title=dict(text="实时本地订单簿  ·  Local Order Book (WebSocket)", x=0.03, y=0.98, xanchor="left", font=dict(size=17, color="#f3f8ff"))))
        return fig
    bids = sorted(((p,s) for p,s in book.bids.items() if s>0), reverse=True)[:depth]
    asks = sorted(((p,s) for p,s in book.asks.items() if s>0))[:depth]
    if not bids and not asks:
        fig.add_annotation(text="订单簿暂无数据", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        fig.update_layout(height=380, **_layout())
        return fig
    # Bid bars (horizontal, positive x)
    bid_prices  = [p for p,s in bids]
    bid_sizes   = [s for p,s in bids]
    bid_notional= [p*s for p,s in bids]
    ask_prices  = [p for p,s in asks]
    ask_sizes   = [s for p,s in asks]
    ask_notional= [p*s for p,s in asks]
    fig.add_trace(go.Bar(y=bid_prices, x=bid_notional, orientation="h",
        name="买盘 Bid", marker_color="rgba(39,156,255,0.65)",
        customdata=[[sz, pr] for pr,sz in bids],
        hovertemplate="价格 %{y:,.2f}<br>名义 %{x:,.0f}<br>数量 %{customdata[0]:,.4f}<extra></extra>"))
    fig.add_trace(go.Bar(y=ask_prices, x=[-n for n in ask_notional], orientation="h",
        name="卖盘 Ask", marker_color="rgba(216,69,45,0.65)",
        customdata=[[sz, pr] for pr,sz in asks],
        hovertemplate="价格 %{y:,.2f}<br>名义 %{customdata[1]:,.0f}<br>数量 %{customdata[0]:,.4f}<extra></extra>"))
    mid = book.mid_price()
    if mid: fig.add_hline(y=mid, line_color="#f8d35e", line_dash="dot", line_width=1)
    spread = book.spread_bps()
    spread_txt = f"  价差 {spread:.2f} bps" if spread else ""
    fig.update_layout(height=420, barmode="overlay",
        **_layout(title=dict(text=f"本地实时订单簿  ·  WebSocket Orderbook{spread_txt}", x=0.03, y=0.98, xanchor="left", font=dict(size=17, color="#f3f8ff"))))
    fig.update_xaxes(title="名义挂单（左卖右买）", showgrid=True, gridcolor=_GRID_COL)
    fig.update_yaxes(showgrid=False, side="right")
    return fig


# ══════════════════════════════════════════════════════════════════════════
# 3. CVD  累积成交量差
# ══════════════════════════════════════════════════════════════════════════

def build_cvd_from_candles(candles: List[Candle]) -> List[CVDPoint]:
    """从Binance K线taker字段构建CVD（最准确来源）"""
    points = []; running = 0.0
    for c in candles:
        if c.taker_buy_volume is None: continue
        buy = c.taker_buy_volume; sell = c.volume - buy if c.volume > 0 else 0
        delta = buy - sell; running += delta
        points.append(CVDPoint(timestamp_ms=c.timestamp_ms, cvd=running, delta=delta,
            buy_volume=buy, sell_volume=sell, price=c.close))
    return points


def build_cvd_from_trades(trades: List[TradeEvent], bin_seconds: int = 30) -> List[CVDPoint]:
    """从实时成交流构建CVD（WebSocket来源）"""
    if not trades: return []
    trades_sorted = sorted(trades, key=lambda t: t.timestamp_ms)
    bin_ms = bin_seconds * 1000
    buckets: Dict[int, dict] = {}
    for t in trades_sorted:
        key = (t.timestamp_ms // bin_ms) * bin_ms
        if key not in buckets: buckets[key] = {"buy":0.0,"sell":0.0,"price":t.price}
        buckets[key]["buy" if t.side=="buy" else "sell"] += t.notional
        buckets[key]["price"] = t.price
    points = []; running = 0.0
    for ts in sorted(buckets):
        b = buckets[ts]; delta = b["buy"] - b["sell"]; running += delta
        points.append(CVDPoint(timestamp_ms=ts, cvd=running, delta=delta,
            buy_volume=b["buy"], sell_volume=b["sell"], price=b["price"]))
    return points


def build_cvd_figure(cvd_points: List[CVDPoint], title: str = "CVD") -> go.Figure:
    """CVD = Cumulative Volume Delta 累积成交量差。
    正值/上升=买方主导 Taker Buy Dominant；负值/下降=卖方主导 Taker Sell Dominant。
    柱=单期净买入Delta；折线=累积CVD趋势。
    """
    fig = go.Figure()
    if not cvd_points:
        fig.add_annotation(text="等待成交流 (WebSocket aggTrade / Binance Kline taker字段)", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        fig.update_layout(height=300, **_layout(title=dict(text=f"{title}  ·  CVD 累积成交量差", x=0.03, y=0.98, xanchor="left", font=dict(size=17, color="#f3f8ff"))))
        return fig
    df = pd.DataFrame({"ts":pd.to_datetime([p.timestamp_ms for p in cvd_points],unit="ms"),
        "cvd":[p.cvd for p in cvd_points],"delta":[p.delta for p in cvd_points],
        "buy":[p.buy_volume for p in cvd_points],"sell":[p.sell_volume for p in cvd_points]})
    colors = ["#1dc796" if d>=0 else "#ff6868" for d in df["delta"]]
    fig.add_trace(go.Bar(x=df["ts"],y=df["delta"],name="净买入量 Delta",
        marker_color=colors,opacity=0.65,
        hovertemplate="时间 %{x}<br>净买入 %{y:,.0f}<extra></extra>"))
    fig.add_trace(go.Scatter(x=df["ts"],y=df["cvd"],name="CVD累积",
        line=dict(color="#62c2ff",width=2.2),yaxis="y2",
        hovertemplate="CVD %{y:,.0f}<extra></extra>"))
    fig.add_hline(y=0,line_color="rgba(255,255,255,0.2)",line_width=1)
    fig.update_layout(height=300, barmode="overlay",
        yaxis=dict(title="单期净买入",showgrid=True,gridcolor=_GRID_COL),
        yaxis2=dict(title="CVD累积",overlaying="y",side="right",showgrid=False),
        **_layout(title=dict(text=f"{title}  ·  CVD 累积成交量差  |  正=买方主导  负=卖方主导", x=0.03, y=0.98, xanchor="left", font=dict(size=16, color="#f3f8ff"))))
    fig.update_xaxes(showgrid=False)
    return fig


# ══════════════════════════════════════════════════════════════════════════
# 4. OI Delta 四象限 + OI Velocity
# ══════════════════════════════════════════════════════════════════════════

def build_oi_delta_points(oi_history: List[OIPoint], candles: List[Candle]) -> List[OIDeltaPoint]:
    if len(oi_history) < 2: return []
    candle_map = {c.timestamp_ms: c.close for c in candles}
    def price_near(ts_ms):
        if not candle_map: return None
        closest = min(candle_map, key=lambda k: abs(k-ts_ms))
        return candle_map[closest] if abs(closest-ts_ms) < 3_600_000 else None

    points = []
    for i in range(1, len(oi_history)):
        prev, curr = oi_history[i-1], oi_history[i]
        pv = prev.open_interest_notional or prev.open_interest or 0
        cv = curr.open_interest_notional or curr.open_interest or 0
        oi_delta = cv - pv
        pp = price_near(prev.timestamp_ms); cp = price_near(curr.timestamp_ms)
        price_delta_pct = (cp-pp)/pp*100 if pp and cp and pp>0 else 0
        # OI velocity (per minute)
        dt_min = (curr.timestamp_ms - prev.timestamp_ms) / 60000
        velocity = oi_delta / dt_min if dt_min > 0 else 0
        up = price_delta_pct >= 0; oi_up = oi_delta >= 0
        if up and oi_up:     q, qcn = "Long Add",    _QUADRANT_CN["Long Add"]
        elif up and not oi_up:  q, qcn = "Short Cover", _QUADRANT_CN["Short Cover"]
        elif not up and oi_up:  q, qcn = "Short Add",   _QUADRANT_CN["Short Add"]
        else:                    q, qcn = "Long Unwind",_QUADRANT_CN["Long Unwind"]
        points.append(OIDeltaPoint(timestamp_ms=curr.timestamp_ms,
            oi_notional=cv, oi_delta=oi_delta, oi_velocity=velocity,
            price=cp, price_delta_pct=price_delta_pct, quadrant=q, quadrant_cn=qcn))
    return points


def build_oi_delta_figure(points: List[OIDeltaPoint]) -> go.Figure:
    """OI Delta四象限散点图。
    横轴=价格变化%，纵轴=OI变化量，颜色=象限方向。
    多头加仓 Long Add（绿）/ 空头回补 Short Cover（蓝）/ 空头加仓 Short Add（红）/ 多头减仓 Long Unwind（橙）
    """
    fig = go.Figure()
    if not points:
        fig.add_annotation(text="等待OI历史采样（需至少2个点）", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        fig.update_layout(height=340, **_layout(title=dict(text="OI Delta 四象限  ·  加仓减仓分析", x=0.03, y=0.98, xanchor="left", font=dict(size=17, color="#f3f8ff"))))
        return fig
    df = pd.DataFrame([{"ts":pd.to_datetime(p.timestamp_ms,unit="ms"),
        "price_delta_pct":p.price_delta_pct,"oi_delta":p.oi_delta,
        "velocity":p.oi_velocity,"quadrant":p.quadrant,"qcn":p.quadrant_cn,
        "price":p.price or 0} for p in points])
    for q in ["Long Add","Short Cover","Short Add","Long Unwind"]:
        sub = df[df["quadrant"]==q]
        if sub.empty: continue
        fig.add_trace(go.Scatter(x=sub["price_delta_pct"],y=sub["oi_delta"],mode="markers",
            name=f"{q} {_QUADRANT_CN[q]}",
            marker=dict(size=9,color=_QUADRANT_COLOR[q],opacity=0.78,
                line=dict(width=1,color="rgba(7,17,27,0.8)")),
            customdata=sub[["ts","price","qcn","velocity"]],
            hovertemplate="时间 %{customdata[0]}<br>价格 %{customdata[1]:,.2f}<br>%{customdata[2]}<br>OI速率 %{customdata[3]:,.0f}/min<extra></extra>"))
    fig.add_vline(x=0,line_color="rgba(255,255,255,0.3)",line_width=1)
    fig.add_hline(y=0,line_color="rgba(255,255,255,0.3)",line_width=1)
    for txt,xr,yr,xa in [("多头加仓",0.98,0.98,"right"),("空头加仓",0.02,0.98,"left"),
                          ("空头回补",0.98,0.02,"right"),("多头减仓",0.02,0.02,"left")]:
        col = {"多头加仓":"#1dc796","空头加仓":"#ff6868","空头回补":"#62c2ff","多头减仓":"#ffa94d"}[txt]
        fig.add_annotation(text=txt,x=xr,y=yr,xref="paper",yref="paper",showarrow=False,
            font=dict(color=col,size=11),xanchor=xa)
    fig.update_layout(height=340,
        **_layout(title=dict(text="OI Delta 四象限  ·  加仓减仓分析 (Open Interest Delta)", x=0.03, y=0.98, xanchor="left", font=dict(size=17, color="#f3f8ff"))))
    fig.update_xaxes(title="价格变化 (%)",showgrid=True,gridcolor=_GRID_COL)
    fig.update_yaxes(title="OI变化量",showgrid=True,gridcolor=_GRID_COL,side="right")
    return fig


def build_oi_velocity_figure(points: List[OIDeltaPoint]) -> go.Figure:
    """OI Velocity = OI变化速率（每分钟）。
    正值=加仓中，负值=减仓中，绝对值越大=变化越剧烈。
    可用于识别加仓加速（Acceleration）事件。
    """
    fig = go.Figure()
    if not points:
        fig.add_annotation(text="等待OI数据", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        fig.update_layout(height=220, **_layout(title=dict(text="OI Velocity 加仓速率", x=0.03, y=0.98, xanchor="left", font=dict(size=16, color="#f3f8ff"))))
        return fig
    df = pd.DataFrame({"ts":pd.to_datetime([p.timestamp_ms for p in points],unit="ms"),
        "velocity":[p.oi_velocity for p in points]})
    colors = ["#1dc796" if v>=0 else "#ff6868" for v in df["velocity"]]
    fig.add_trace(go.Bar(x=df["ts"],y=df["velocity"],marker_color=colors,
        name="OI变化速率/min",
        hovertemplate="时间 %{x}<br>速率 %{y:,.0f}/min<extra></extra>"))
    fig.add_hline(y=0,line_color="rgba(255,255,255,0.2)",line_width=1)
    fig.update_layout(height=220,
        **_layout(title=dict(text="OI Velocity  ·  加仓速率/分钟  |  正=加仓中  负=减仓中", x=0.03, y=0.98, xanchor="left", font=dict(size=16, color="#f3f8ff"))))
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True,gridcolor=_GRID_COL,side="right")
    return fig


def build_oi_delta_summary(points: List[OIDeltaPoint], lookback: int = 20) -> Dict:
    recent = points[-lookback:] if len(points)>lookback else points
    counts = {"Long Add":0,"Short Cover":0,"Short Add":0,"Long Unwind":0}
    for p in recent: counts[p.quadrant] = counts.get(p.quadrant,0)+1
    total = max(sum(counts.values()),1)
    dominant = max(counts, key=lambda k: counts[k])
    velocities = [p.oi_velocity for p in recent]
    avg_velocity = sum(velocities)/len(velocities) if velocities else 0
    return {
        "dominant": dominant, "dominant_cn": _QUADRANT_CN.get(dominant, dominant),
        "long_add_pct": counts["Long Add"]/total*100,
        "short_add_pct": counts["Short Add"]/total*100,
        "short_cover_pct": counts["Short Cover"]/total*100,
        "long_unwind_pct": counts["Long Unwind"]/total*100,
        "total_points": len(recent),
        "avg_velocity": avg_velocity,
    }


# ══════════════════════════════════════════════════════════════════════════
# 5. 多空比矩阵  Long/Short Ratio Matrix
# ══════════════════════════════════════════════════════════════════════════

def build_top_trader_figure(ratios: List[TopTraderRatio], global_ratios: List[TopTraderRatio],
                             bybit_ratios: List[TopTraderRatio]) -> go.Figure:
    """大户多空比矩阵。
    Top Trader Position Ratio = 大户持仓多空比（Binance，按账户权益top20%统计）
    Top Trader Account Ratio  = 大户账户多空比
    Global Long/Short Ratio   = 全市场账户多空比
    Bybit Buy Ratio            = Bybit主动买比例（Taker方向）
    ratio > 1 = 多头占优；< 1 = 空头占优；基准线 = 1.0
    """
    fig = go.Figure()
    has_data = False
    if ratios:
        df = pd.DataFrame({"ts":pd.to_datetime([r.timestamp_ms for r in ratios],unit="ms"),
            "pos":[r.long_short_ratio or float("nan") for r in ratios],
            "acc":[r.long_account_ratio or float("nan") for r in ratios]})
        fig.add_trace(go.Scatter(x=df["ts"],y=df["pos"],mode="lines",
            name="大户持仓多空比 (Top Trader Position Ratio)",
            line=dict(color="#62c2ff",width=2.0)))
        fig.add_trace(go.Scatter(x=df["ts"],y=df["acc"],mode="lines",
            name="大户账户多空比 (Top Trader Account Ratio)",
            line=dict(color="#ffa94d",width=1.5,dash="dot")))
        has_data = True
    if global_ratios:
        gdf = pd.DataFrame({"ts":pd.to_datetime([r.timestamp_ms for r in global_ratios],unit="ms"),
            "ratio":[r.global_ratio or float("nan") for r in global_ratios]})
        fig.add_trace(go.Scatter(x=gdf["ts"],y=gdf["ratio"],mode="lines",
            name="全市场多空比 (Global L/S Ratio)",
            line=dict(color="#c084fc",width=1.5,dash="dash")))
        has_data = True
    if bybit_ratios:
        bdf = pd.DataFrame({"ts":pd.to_datetime([r.timestamp_ms for r in bybit_ratios],unit="ms"),
            "ratio":[r.bybit_buy_ratio or float("nan") for r in bybit_ratios]})
        fig.add_trace(go.Scatter(x=bdf["ts"],y=bdf["ratio"],mode="lines",
            name="Bybit主动买比例 (Taker Buy Ratio)",
            line=dict(color="#1dc796",width=1.5,dash="dashdot")))
        has_data = True
    if not has_data:
        fig.add_annotation(text="暂无多空比数据（大户多空比仅Binance支持）", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
    fig.add_hline(y=1.0,line_color="rgba(255,255,255,0.3)",line_dash="dot",line_width=1)
    fig.update_layout(height=280,
        **_layout(title=dict(text="多空比矩阵  ·  Long/Short Ratio Matrix  |  >1=多头占优  <1=空头占优", x=0.03, y=0.98, xanchor="left", font=dict(size=16, color="#f3f8ff"))))
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True,gridcolor=_GRID_COL,side="right")
    return fig


# ══════════════════════════════════════════════════════════════════════════
# 6. Basis / Spot vs Perp / 期限结构
# ══════════════════════════════════════════════════════════════════════════

def build_basis_figure(snapshots: List[ExchangeSnapshot], spot_prices: Dict[str,float]) -> go.Figure:
    """Basis = 合约价 - 现货价（或现货参考价）。
    正Basis = 合约溢价，市场偏多头；负Basis = 合约折价，常见于恐慌或空头主导期。
    Premium % = (合约价 - 现货价) / 现货价 × 100
    """
    fig = go.Figure()
    rows = []
    for s in snapshots:
        if s.status != "ok": continue
        perp_price = s.last_price or s.mark_price
        spot_price = spot_prices.get(s.exchange) or s.index_price
        basis = None; basis_pct = None
        if perp_price and spot_price:
            basis = perp_price - spot_price
            basis_pct = basis / spot_price * 100
        rows.append({"交易所":s.exchange,"合约价":perp_price,"现货参考":spot_price,
            "Basis绝对值":basis,"Basis%":basis_pct})
    if not rows:
        fig.add_annotation(text="暂无Basis数据", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        fig.update_layout(height=220, **_layout(title=dict(text="Basis 合约溢价率", x=0.03, y=0.98, xanchor="left", font=dict(size=16, color="#f3f8ff"))))
        return fig
    df = pd.DataFrame(rows).dropna(subset=["Basis%"])
    if df.empty:
        fig.add_annotation(text="暂无现货参考价用于Basis计算", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        fig.update_layout(height=220, **_layout())
        return fig
    colors = ["#1dc796" if v>=0 else "#ff6868" for v in df["Basis%"]]
    fig.add_trace(go.Bar(x=df["交易所"],y=df["Basis%"],marker_color=colors,
        text=[f"{v:+.4f}%" for v in df["Basis%"]],textposition="outside",
        customdata=df[["合约价","现货参考","Basis绝对值"]],
        hovertemplate="%{x}<br>合约价 %{customdata[0]:,.4f}<br>现货参考 %{customdata[1]:,.4f}<br>Basis %{customdata[2]:+,.4f}<extra></extra>"))
    fig.add_hline(y=0,line_color="rgba(255,255,255,0.3)",line_width=1)
    fig.update_layout(height=240,
        **_layout(title=dict(text="Basis 合约溢价率  ·  正=合约溢价(多头拥挤)  负=合约折价(空头拥挤)", x=0.03, y=0.98, xanchor="left", font=dict(size=15, color="#f3f8ff"))))
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True,gridcolor=_GRID_COL,side="right",title="Basis %")
    return fig


def build_term_structure_figure(futures_oi: List[FuturesOIPoint]) -> go.Figure:
    """期限结构图：各到期日合约的OI和相对现货的溢价率。
    Term Structure = 不同到期日合约的价格/OI分布。
    正Basis%=近月溢价；负=近月折价。PERP=永续合约。
    """
    fig = make_subplots(rows=1, cols=2, subplot_titles=["各到期日OI (USD)", "相对现货溢价率 Basis%"],
                        horizontal_spacing=0.12)
    if not futures_oi:
        fig.add_annotation(text="暂无期限结构数据", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        fig.update_layout(height=300, **_layout(title=dict(text="期限结构  ·  Term Structure", x=0.03, y=0.98, xanchor="left", font=dict(size=17, color="#f3f8ff"))))
        return fig
    # Group by exchange
    exchanges = list(dict.fromkeys(p.exchange for p in futures_oi))
    colors_map = {"Bybit":"#62c2ff","Binance":"#ffa94d","OKX":"#c084fc","Hyperliquid":"#1dc796"}
    for exch in exchanges:
        pts = sorted([p for p in futures_oi if p.exchange==exch],
                     key=lambda p: (0 if p.expiry=="PERP" else 1, p.expiry))
        expiries   = [p.expiry for p in pts]
        oi_vals    = [p.oi_notional or 0 for p in pts]
        basis_vals = [p.basis_pct or 0 for p in pts]
        col = colors_map.get(exch, "#dfe8f1")
        fig.add_trace(go.Bar(x=expiries,y=oi_vals,name=f"{exch} OI",
            marker_color=col,opacity=0.75), row=1, col=1)
        fig.add_trace(go.Scatter(x=expiries,y=basis_vals,mode="lines+markers",
            name=f"{exch} Basis%",line=dict(color=col,width=1.8),
            marker=dict(size=6)), row=1, col=2)
    fig.add_hline(y=0,line_color="rgba(255,255,255,0.2)",line_width=1,row=1,col=2)
    fig.update_layout(height=320,
        **_layout(title=dict(text="期限结构  ·  Term Structure  |  各到期日OI与溢价率", x=0.03, y=0.98, xanchor="left", font=dict(size=17, color="#f3f8ff"))))
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True,gridcolor=_GRID_COL,tickformat=".2s",row=1,col=1)
    fig.update_yaxes(showgrid=True,gridcolor=_GRID_COL,title="Basis %",row=1,col=2)
    return fig


def build_spot_vs_perp_figure(snapshots: List[ExchangeSnapshot], spot_volumes: Dict[str,float]) -> go.Figure:
    """现货成交量 vs 合约持仓量对比。
    比值高 = 现货活跃度高于合约（偏真实需求）
    比值低 = 合约主导（偏杠杆投机）
    """
    fig = go.Figure()
    rows = []
    for s in snapshots:
        if s.status != "ok": continue
        spot_vol = spot_volumes.get(s.exchange)
        perp_oi  = s.open_interest_notional
        ratio    = spot_vol / perp_oi if spot_vol and perp_oi and perp_oi > 0 else None
        rows.append({"交易所":s.exchange,"现货24h成交额":spot_vol,"合约OI":perp_oi,"现货/OI比":ratio})
    df = pd.DataFrame(rows)
    valid = df.dropna(subset=["现货24h成交额","合约OI"])
    if valid.empty:
        fig.add_annotation(text="暂无现货vs合约对比数据", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        fig.update_layout(height=240, **_layout(title=dict(text="现货 vs 合约持仓  ·  Spot vs Perp OI", x=0.03, y=0.98, xanchor="left", font=dict(size=16, color="#f3f8ff"))))
        return fig
    fig.add_trace(go.Bar(x=valid["交易所"],y=valid["现货24h成交额"],name="现货24h成交额 Spot Volume",
        marker_color="rgba(98,194,255,0.75)",
        hovertemplate="%{x}<br>现货成交额 %{y:,.0f}<extra></extra>"))
    fig.add_trace(go.Bar(x=valid["交易所"],y=valid["合约OI"],name="合约持仓金额 Perp OI",
        marker_color="rgba(192,132,252,0.75)",
        hovertemplate="%{x}<br>合约OI %{y:,.0f}<extra></extra>"))
    fig.update_layout(height=260, barmode="group",
        **_layout(title=dict(text="现货 vs 合约持仓  ·  Spot Volume / Perp OI  |  比值高=现货活跃  低=杠杆主导", x=0.03, y=0.98, xanchor="left", font=dict(size=15, color="#f3f8ff"))))
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True,gridcolor=_GRID_COL,tickformat=".2s",side="right")
    return fig


# ══════════════════════════════════════════════════════════════════════════
# 7. 爆仓瀑布 + 簇检测
# ══════════════════════════════════════════════════════════════════════════

def build_liquidation_cascade_figure(events_by_exchange: Dict[str, List[LiquidationEvent]]) -> go.Figure:
    """跨所爆仓瀑布：四所爆仓事件叠加，气泡大小=名义金额。
    Liquidation Cascade = 短时间内多所连续大额强平。
    多头爆仓 Long Liquidation ▼ = 做多者被强平；
    空头爆仓 Short Liquidation ▲ = 做空者被强平。
    """
    fig = go.Figure()
    has_data = False
    exchange_colors = {"bybit":"#62c2ff","binance":"#ffa94d","okx":"#c084fc","hyperliquid":"#1dc796"}
    for exch, evs in events_by_exchange.items():
        if not evs: continue
        for side, sym_marker, label in [("long","triangle-down","多头爆仓"),("short","triangle-up","空头爆仓")]:
            side_evs = [e for e in evs if e.side==side]
            if not side_evs: continue
            xs = pd.to_datetime([e.timestamp_ms for e in side_evs], unit="ms")
            ys = [e.notional or 0 for e in side_evs]
            max_y = max(ys) if ys else 1
            sizes = [9+22*math.sqrt(v/max(max_y,1)) for v in ys]
            col = exchange_colors.get(exch,"#dfe8f1")
            fig.add_trace(go.Scatter(x=xs,y=ys,mode="markers",name=f"{exch} {label}",
                marker=dict(size=sizes,color=col,symbol=sym_marker,opacity=0.78,
                    line=dict(width=1,color="rgba(7,17,27,0.85)")),
                customdata=[(e.exchange,e.price or 0) for e in side_evs],
                hovertemplate="交易所 %{customdata[0]}<br>价格 %{customdata[1]:,.2f}<br>名义 %{y:,.0f}<extra></extra>"))
            has_data = True
    if not has_data:
        fig.add_annotation(text="暂无跨所爆仓数据", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
    fig.update_layout(height=300,
        **_layout(title=dict(text="跨所爆仓瀑布  ·  Cross-Exchange Liquidation Cascade", x=0.03, y=0.98, xanchor="left", font=dict(size=17, color="#f3f8ff"))))
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(title="名义金额",showgrid=True,gridcolor=_GRID_COL,side="right")
    return fig


def detect_liquidation_clusters(events: List[LiquidationEvent], window_ms: int = 60_000,
                                  min_notional: float = 500_000) -> List[Dict]:
    if not events: return []
    sorted_evs = sorted(events, key=lambda e: e.timestamp_ms)
    clusters = []; i = 0
    while i < len(sorted_evs):
        t0 = sorted_evs[i].timestamp_ms
        window = [e for e in sorted_evs if t0 <= e.timestamp_ms < t0+window_ms]
        total = sum(e.notional or 0 for e in window)
        if total >= min_notional and len(window) >= 3:
            long_n = sum(e.notional or 0 for e in window if e.side=="long")
            short_n = sum(e.notional or 0 for e in window if e.side=="short")
            clusters.append({"start_ms":t0,"count":len(window),"total_notional":total,
                "long_notional":long_n,"short_notional":short_n,
                "dominant":"多头爆仓" if long_n>=short_n else "空头爆仓"})
            i += len(window)
        else: i += 1
    return clusters


# ══════════════════════════════════════════════════════════════════════════
# 8. 冰山单 & 流动性缺口
# ══════════════════════════════════════════════════════════════════════════

def build_iceberg_figure(alerts: List[IcebergAlert]) -> go.Figure:
    """冰山单检测：同价位30秒内反复成交4次以上。
    Iceberg Order = 大单拆成小单反复在同一价位挂单/成交，伪装真实单量。
    气泡越大=总名义金额越高，说明隐藏单量越大。
    """
    fig = go.Figure()
    if not alerts:
        fig.add_annotation(text="暂未检测到冰山单（需要实时WebSocket成交流）", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        fig.update_layout(height=260, **_layout(title=dict(text="冰山单检测  ·  Iceberg Order Detection", x=0.03, y=0.98, xanchor="left", font=dict(size=16, color="#f3f8ff"))))
        return fig
    recent = sorted(alerts, key=lambda a: a.timestamp_ms, reverse=True)[:50]
    xs = pd.to_datetime([a.timestamp_ms for a in recent], unit="ms")
    ys = [a.price for a in recent]
    sizes = [8 + 20*min(a.refill_count/10, 1) for a in recent]
    colors= ["#62c2ff" if a.side=="buy" else "#ff6868" for a in recent]
    fig.add_trace(go.Scatter(x=xs,y=ys,mode="markers",
        marker=dict(size=sizes,color=colors,opacity=0.8,line=dict(width=1,color="rgba(7,17,27,0.8)")),
        customdata=[(a.refill_count, a.total_notional, a.side) for a in recent],
        hovertemplate="价格 %{y:,.2f}<br>重复次数 %{customdata[0]}<br>总名义 %{customdata[1]:,.0f}<br>方向 %{customdata[2]}<extra></extra>",
        name="冰山单"))
    fig.update_layout(height=260,
        **_layout(title=dict(text="冰山单检测  ·  Iceberg Order Detection  |  蓝=买方  红=卖方", x=0.03, y=0.98, xanchor="left", font=dict(size=16, color="#f3f8ff"))))
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=False,side="right",title="价格")
    return fig


def build_liquidity_gap_frame(gaps: List[LiquidityGap]) -> pd.DataFrame:
    """流动性缺口：某价位挂单量骤减超过50%。
    Liquidity Gap / Void = 盘口某区域挂单突然消失，可能是大单被吃掉或主动撤单。
    """
    if not gaps: return pd.DataFrame(columns=["时间","方向","价格","前挂单","后挂单","消失比例","交易所"])
    rows = []
    for g in sorted(gaps, key=lambda x: x.timestamp_ms, reverse=True)[:40]:
        rows.append({"时间":pd.to_datetime(g.timestamp_ms,unit="ms"),
            "方向":"买盘" if g.side=="bid" else "卖盘",
            "价格":f"{g.price_low:,.2f}",
            "前挂单":g.prev_notional,"后挂单":g.curr_notional,
            "消失比例":g.drop_pct,"交易所":g.exchange})
    return pd.DataFrame(rows)


# ══════════════════════════════════════════════════════════════════════════
# 9. 资金费率对比 & 综合情绪
# ══════════════════════════════════════════════════════════════════════════

def build_funding_comparison_figure(snapshots: List[ExchangeSnapshot]) -> go.Figure:
    """资金费率对比。
    Funding Rate > 0 = 多头支付空头（多头拥挤）；< 0 = 空头支付多头（空头拥挤）。
    单位：bps（1 bps = 0.01%）。标准资金费率8小时一次。
    """
    fig = go.Figure()
    valid = [(s.exchange, s.funding_bps) for s in snapshots if s.funding_bps is not None and s.status=="ok"]
    if not valid:
        fig.add_annotation(text="暂无资金费率数据", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        fig.update_layout(height=220, **_layout(title=dict(text="资金费率对比  ·  Funding Rate", x=0.03, y=0.98, xanchor="left", font=dict(size=16, color="#f3f8ff"))))
        return fig
    exchanges = [v[0] for v in valid]; values = [v[1] for v in valid]
    colors = ["#1dc796" if v>=0 else "#ff6868" for v in values]
    fig.add_trace(go.Bar(x=exchanges,y=values,marker_color=colors,
        text=[f"{v:+.4f} bps" for v in values],textposition="outside",
        hovertemplate="%{x}<br>费率 %{y:+.4f} bps<extra></extra>"))
    fig.add_hline(y=0,line_color="rgba(255,255,255,0.3)",line_width=1)
    fig.update_layout(height=230,
        **_layout(title=dict(text="资金费率对比  ·  Funding Rate (bps)  |  正=多头拥挤  负=空头拥挤", x=0.03, y=0.98, xanchor="left", font=dict(size=15, color="#f3f8ff"))))
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True,gridcolor=_GRID_COL,side="right",title="费率 (bps)")
    return fig


def build_market_sentiment_summary(snapshots, oi_delta_summary, liq_metrics,
                                    cvd_points, top_trader_ratios, basis_data=None) -> Dict[str,str]:
    signals = {}
    funding_vals = [s.funding_bps for s in snapshots if s.funding_bps is not None]
    if funding_vals:
        avg_f = sum(funding_vals)/len(funding_vals)
        if avg_f > 1.0:   signals["资金费率"] = f"偏多头拥挤 {avg_f:+.2f}bps"
        elif avg_f < -0.5: signals["资金费率"] = f"偏空头拥挤 {avg_f:+.2f}bps"
        else:               signals["资金费率"] = f"中性 {avg_f:+.2f}bps"
    dom_cn = oi_delta_summary.get("dominant_cn","")
    if dom_cn: signals["OI动向"] = dom_cn
    vel = oi_delta_summary.get("avg_velocity", 0)
    if abs(vel) > 1000: signals["OI速率"] = f"{'加仓' if vel>0 else '减仓'}加速 {vel:+,.0f}/min"
    dom_liq = liq_metrics.get("dominant")
    if dom_liq: signals["近期爆仓"] = f"{dom_liq}主导"
    if cvd_points:
        trend = cvd_points[-1].cvd - (cvd_points[-10].cvd if len(cvd_points)>=10 else 0)
        signals["CVD趋势"] = "买方主导 ↑" if trend > 0 else "卖方主导 ↓"
    if top_trader_ratios:
        r = top_trader_ratios[-1].long_short_ratio
        if r: signals["大户持仓"] = f"{'偏多' if r>1.1 else '偏空' if r<0.9 else '中性'} {r:.2f}"
    return signals


# ══════════════════════════════════════════════════════════════════════════
# 10. 原有模块（保留）
# ══════════════════════════════════════════════════════════════════════════

def merge_liquidation_events(backfill, session):
    merged = {}
    for e in backfill + session:
        key = (e.exchange, e.symbol, e.timestamp_ms, e.side, round(e.price or 0,6), round(e.size or 0,6))
        merged[key] = e
    return sorted(merged.values(), key=lambda x: x.timestamp_ms)


def build_liquidation_metrics(events, now_ms=None, window_minutes=60):
    if now_ms is None: now_ms = int(time.time()*1000)
    cutoff = now_ms - window_minutes*60_000
    recent = [e for e in events if e.timestamp_ms >= cutoff]
    total = sum(e.notional or 0 for e in recent)
    long_n = sum(1 for e in recent if e.side=="long")
    short_n = sum(1 for e in recent if e.side=="short")
    dominant = ("多头" if long_n>short_n else "空头" if short_n>long_n else "均衡") if recent else None
    return {"count":len(recent),"notional":total,"long_count":long_n,"short_count":short_n,"dominant":dominant}


def build_liquidation_frame(events, limit=36):
    if not events: return pd.DataFrame(columns=["时间","爆仓方向","价格","数量","名义金额","来源"])
    rows = []
    for e in sorted(events, key=lambda x: x.timestamp_ms, reverse=True)[:limit]:
        rows.append({"时间":pd.to_datetime(e.timestamp_ms,unit="ms"),
            "爆仓方向":_parse_side_label(e.side),"价格":e.price,
            "数量":e.size,"名义金额":e.notional,"来源":e.source})
    return pd.DataFrame(rows)


def build_liquidation_figure(events):
    fig = go.Figure()
    if not events:
        fig.add_annotation(text="等待爆仓事件流",showarrow=False,x=0.5,y=0.5,xref="paper",yref="paper")
        fig.update_layout(height=320,margin=dict(l=12,r=12,t=24,b=12))
        return fig
    rows = []
    for e in events:
        if e.price is None: continue
        rows.append({"ts":pd.to_datetime(e.timestamp_ms,unit="ms"),"price":e.price,
            "notional":e.notional or 0,"side":e.side,"label":_parse_side_label(e.side)})
    df = pd.DataFrame(rows)
    if df.empty:
        fig.add_annotation(text="爆仓事件缺少价格字段",showarrow=False,x=0.5,y=0.5,xref="paper",yref="paper")
        fig.update_layout(height=320,**_layout()); return fig
    max_n = max(df["notional"].max(), 1)
    df["ms"] = df["notional"].apply(lambda v: 9+24*math.sqrt(v/max_n) if v>0 else 10)
    color_map = {"long":"#ff7b7b","short":"#5bc0ff"}
    sym_map   = {"long":"triangle-down","short":"triangle-up"}
    for side in ("long","short"):
        sf = df[df["side"]==side]
        if sf.empty: continue
        fig.add_trace(go.Scatter(x=sf["ts"],y=sf["price"],mode="markers",name=_parse_side_label(side),
            marker=dict(size=sf["ms"],color=color_map.get(side,"#dfe8f1"),symbol=sym_map.get(side,"circle"),opacity=0.82,line=dict(width=1,color="rgba(7,17,27,0.85)")),
            customdata=sf[["notional"]],
            hovertemplate="时间 %{x}<br>价格 %{y:,.2f}<br>名义 %{customdata[0]:,.0f}<extra></extra>"))
    fig.update_layout(height=320,**_layout(title=dict(text="Executed Liquidations  ·  已发生爆仓",x=0.03,y=0.98,xanchor="left",font=dict(size=17,color="#f7fbff"))))
    fig.update_xaxes(showgrid=False); fig.update_yaxes(showgrid=True,gridcolor=_GRID_COL,side="right")
    return fig


def build_mbo_profile_frame(levels, ref, rows_per_side=14):
    if not levels or ref is None or ref<=0:
        return pd.DataFrame(columns=["方向","价格","挂单量","名义金额","累积名义金额","距现价(bps)","盘口占比","队列压力","吸收分数","signed_notional","side"])
    bid_levels = sorted([l for l in levels if l.side=="bid" and l.size>0],key=lambda l:l.price,reverse=True)[:rows_per_side]
    ask_levels = sorted([l for l in levels if l.side=="ask" and l.size>0],key=lambda l:l.price)[:rows_per_side]
    bid_total = sum(l.price*l.size for l in bid_levels); ask_total = sum(l.price*l.size for l in ask_levels)
    rows = []
    for levels_list, side, total, direction in [(bid_levels,"bid",bid_total,"买盘"),(ask_levels,"ask",ask_total,"卖盘")]:
        cumulative = 0
        for l in levels_list:
            notional = l.price*l.size; cumulative += notional
            dist_bps = (l.price-ref)/ref*10000
            share = notional/total if total>0 else 0
            rows.append({"方向":direction,"价格":l.price,"挂单量":l.size,"名义金额":notional,
                "累积名义金额":cumulative,"距现价(bps)":dist_bps,"盘口占比":share,
                "队列压力":l.size/max(abs(dist_bps),1),"吸收分数":0,
                "signed_notional":notional if side=="bid" else -notional,"side":side})
    df = pd.DataFrame(rows)
    if df.empty: return df
    pn = _normalize(df["队列压力"].tolist()); sn = _normalize(df["盘口占比"].tolist())
    df["吸收分数"] = [0.55*s+0.45*p for s,p in zip(sn,pn)]
    return df.sort_values("价格",ascending=False).reset_index(drop=True)


def _mbo_color(side, intensity):
    if side=="bid": r,g,b=39,156,255
    else: r,g,b=216,69,45
    a = 0.35+0.55*clamp01(intensity)
    return f"rgba({r},{g},{b},{a:.3f})"


def build_mbo_figure(frame, ref):
    fig = go.Figure()
    if frame.empty:
        fig.add_annotation(text="等待盘口深度",showarrow=False,x=0.5,y=0.5,xref="paper",yref="paper")
        fig.update_layout(height=420,**_layout()); return fig
    fig.add_vline(x=0,line_width=1,line_color="rgba(223,232,241,0.22)")
    for side in ("bid","ask"):
        sf = frame[frame["side"]==side].sort_values("价格")
        if sf.empty: continue
        fig.add_trace(go.Bar(x=sf["signed_notional"],y=sf["价格"],orientation="h",
            name="买盘梯级" if side=="bid" else "卖盘梯级",
            marker_color=[_mbo_color(side,v) for v in sf["吸收分数"]],
            customdata=sf[["挂单量","名义金额","盘口占比","队列压力","吸收分数"]],
            hovertemplate="价格 %{y:,.2f}<br>挂单量 %{customdata[0]:,.2f}<br>名义 %{customdata[1]:,.0f}<br>占比 %{customdata[2]:.2%}<br>吸收分数 %{customdata[4]:.2f}<extra></extra>"))
    if ref: fig.add_hline(y=ref,line_color="#f8d35e",line_dash="dot",line_width=1)
    fig.update_layout(height=420,barmode="overlay",
        **_layout(title=dict(text="Queue Pressure & Absorption  ·  队列压力与吸收分数",x=0.03,y=0.98,xanchor="left",font=dict(size=17,color="#f7fbff"))))
    fig.update_xaxes(showgrid=True,gridcolor=_GRID_COL,title="名义挂单（左卖右买）")
    fig.update_yaxes(showgrid=False,side="right")
    return fig


def build_probability_heatmap_frame(candles, orderbook, snapshot, scenario, ref, window_pct=8.0, bucket_count=28):
    if ref is None or ref<=0 or bucket_count<=0:
        return pd.DataFrame(columns=EMPTY_FRAME_COLS)
    lower = ref*(1-window_pct/100); upper = ref*(1+window_pct/100)
    bucket_width = (upper-lower)/max(bucket_count,1)
    bid_density=[0.0]*bucket_count; ask_density=[0.0]*bucket_count
    swing_high=[0.0]*bucket_count; swing_low=[0.0]*bucket_count

    def bidx(price):
        if price<lower or price>upper: return None
        return min(bucket_count-1,max(0,int((price-lower)/(upper-lower)*bucket_count)))

    for l in orderbook:
        if l.size<=0: continue
        idx = bidx(l.price)
        if idx is None: continue
        n = l.price*l.size
        if l.side=="bid": bid_density[idx]+=n
        else: ask_density[idx]+=n
    for c in candles:
        rng = max(c.high-c.low,abs(c.close-c.open),ref*0.0005)
        w = 1+rng/max(ref*0.002,1e-9)
        hi=bidx(c.high); lo=bidx(c.low)
        if hi is not None: swing_high[hi]+=w
        if lo is not None: swing_low[lo]+=w

    momentum = (candles[-1].close - candles[max(0,len(candles)-20)].close)/max(candles[max(0,len(candles)-20)].close,1e-9) if candles else 0
    bd=_normalize(bid_density); ad=_normalize(ask_density); sh=_normalize(swing_high); sl=_normalize(swing_low)
    fr = snapshot.funding_rate or 0
    long_crowd=clamp01(fr*6000+0.18); short_crowd=clamp01(-fr*6000+0.18)
    up_bias=clamp01(0.5+momentum*8); dn_bias=clamp01(0.5-momentum*8)
    oi_scale = 1.0
    if snapshot.open_interest_notional and snapshot.open_interest_notional>0:
        oi_scale = min(1.9, 0.75+max(0,math.log10(snapshot.open_interest_notional)-6)*0.22)
    rows=[]; raw_scores=[]
    for i in range(bucket_count):
        mid = lower+bucket_width*(i+0.5)
        pl,ph = lower+bucket_width*i, lower+bucket_width*(i+1)
        below = mid<ref
        same_book = bd[i] if below else ad[i]
        swing = sl[i] if below else sh[i]
        dist = clamp01(abs(mid-ref)/ref/max(window_pct/100,0.0001))
        near = 1-dist; thin=1-same_book
        if scenario=="liquidation":
            crowd=long_crowd if below else short_crowd
            rs=(0.42*swing+0.23*dist+0.20*same_book+0.15*thin)*(0.8+crowd*1.3)*oi_scale
            reason="推断下方多头爆仓风险" if below else "推断上方空头爆仓风险"
        elif scenario=="tp":
            dbias=up_bias if not below else dn_bias; crowd=long_crowd if not below else short_crowd
            rs=(0.46*swing+0.32*same_book+0.22*near)*(0.75+dbias*0.9+crowd*0.25)*oi_scale
            reason="推断上方多头止盈密集" if not below else "推断下方空头止盈密集"
        else:
            crowd=long_crowd if below else short_crowd
            rs=(0.48*swing+0.32*thin+0.20*dist)*(0.75+crowd*0.85)*oi_scale
            reason="推断下方多头止损池" if below else "推断上方空头止损池"
        if pl<=ref<=ph: rs*=0.35
        raw_scores.append(rs)
        rows.append({"价格区间":f"{pl:,.2f} - {ph:,.2f}","价格中位":mid,
            "热度":rs,"方向":"下方" if below else "上方","归因":reason})
    mx = max(raw_scores,default=0)
    if mx<=0: return pd.DataFrame(columns=EMPTY_FRAME_COLS)
    for r in rows: r["热度"]=r["热度"]/mx
    return pd.DataFrame(rows)


def build_heat_zone_frame(frame, limit=6):
    if frame.empty: return pd.DataFrame(columns=["价格区间","热度","归因"])
    return frame.sort_values(["热度","价格中位"],ascending=[False,True]).head(limit)[["价格区间","热度","归因"]].reset_index(drop=True)


def build_heatmap_figure(frame, title, ref, colorscale, empty_text):
    fig = go.Figure()
    if frame.empty:
        fig.add_annotation(text=empty_text,showarrow=False,x=0.5,y=0.5,xref="paper",yref="paper")
        fig.update_layout(height=380,**_layout()); return fig
    text_matrix=[[f"{pr}<br>{re}"] for pr,re in zip(frame["价格区间"],frame["归因"])]
    fig.add_trace(go.Heatmap(z=[[v] for v in frame["热度"]],x=["风险层"],y=frame["价格中位"],
        text=text_matrix,hovertemplate="%{text}<br>热度 %{z:.2f}<extra></extra>",
        colorscale=colorscale,showscale=False))
    if ref: fig.add_hline(y=ref,line_color="#f8d35e",line_dash="dot",line_width=1)
    fig.update_layout(height=380,**_layout(title=dict(text=title,x=0.03,y=0.98,xanchor="left",font=dict(size=17,color="#f7fbff"))))
    fig.update_xaxes(showgrid=False); fig.update_yaxes(showgrid=False,side="right")
    return fig


LIQUIDATION_COLORSCALE = [(0.0,"#081a2b"),(0.25,"#12466f"),(0.55,"#2ca7ff"),(1.0,"#ffd76b")]
TP_COLORSCALE   = [(0.0,"#102016"),(0.28,"#1f5d32"),(0.6,"#57b06b"),(1.0,"#f1ff9b")]
STOP_COLORSCALE = [(0.0,"#260f14"),(0.25,"#5f1d28"),(0.58,"#d45454"),(1.0,"#ffd7b1")]


# ══════════════════════════════════════════════════════════════════════════════
# NEW: Orderbook Quality & Cancel/Add Delta figure
# ══════════════════════════════════════════════════════════════════════════════

def build_ob_quality_figure(quality_history: list) -> go.Figure:
    """盘口质量时序图：新增/撤单/净变化 + 质量得分"""
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        vertical_spacing=0.06, row_heights=[0.65, 0.35],
                        subplot_titles=("新增/撤单名义额 (Add vs Cancel)", "盘口质量得分 Quality Score"))
    if not quality_history:
        fig.add_annotation(text="等待盘口数据…", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        fig.update_layout(height=380, **_layout()); return fig

    ts       = [pd.to_datetime(q.timestamp_ms, unit="ms") for q in quality_history]
    bid_add  = [q.bid_add_notional / 1e3 for q in quality_history]
    bid_canc = [-q.bid_cancel_notional / 1e3 for q in quality_history]
    ask_add  = [q.ask_add_notional / 1e3 for q in quality_history]
    ask_canc = [-q.ask_cancel_notional / 1e3 for q in quality_history]
    scores   = [q.quality_score for q in quality_history]

    fig.add_trace(go.Bar(x=ts, y=bid_add,  name="买单新增", marker_color="#1dc796", opacity=0.8), row=1, col=1)
    fig.add_trace(go.Bar(x=ts, y=ask_add,  name="卖单新增", marker_color="#62c2ff", opacity=0.8), row=1, col=1)
    fig.add_trace(go.Bar(x=ts, y=bid_canc, name="买单撤销", marker_color="#ff6868", opacity=0.7), row=1, col=1)
    fig.add_trace(go.Bar(x=ts, y=ask_canc, name="卖单撤销", marker_color="#ffa94d", opacity=0.7), row=1, col=1)

    # Quality score line
    score_colors = ["#ff6868" if s < 0.35 else "#ffa94d" if s < 0.6 else "#1dc796" for s in scores]
    fig.add_trace(go.Scatter(x=ts, y=scores, mode="lines+markers", name="质量分",
        line=dict(color="#f8d35e", width=2),
        marker=dict(color=score_colors, size=5)), row=2, col=1)
    fig.add_hline(y=0.5, line_color="rgba(255,255,255,0.25)", line_dash="dot", row=2, col=1)

    fig.update_layout(height=400, barmode="relative",
        **_layout(title=dict(text="盘口质量监控  ·  Orderbook Quality", x=0.02, y=0.98,
                             xanchor="left", font=dict(size=15, color="#f3f8ff"))))
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridcolor=_GRID_COL)
    return fig


def build_fake_wall_figure(fake_walls: list, absorptions: list) -> go.Figure:
    """假挂单 + 吸收事件 气泡图"""
    fig = go.Figure()
    if fake_walls:
        fw_ts   = [pd.to_datetime(f.timestamp_ms, unit="ms") for f in fake_walls]
        fw_p    = [f.price for f in fake_walls]
        fw_n    = [f.peak_notional for f in fake_walls]
        fw_life = [f.lifespan_ms for f in fake_walls]
        fw_side = [f.side for f in fake_walls]
        fw_sz   = [max(6, min(30, n/20000)) for n in fw_n]
        fw_col  = ["#ffa94d" if s=="bid" else "#ff6868" for s in fw_side]
        fw_text = [f"{'买' if s=='bid' else '卖'}侧假墙 @{p:.2f}<br>峰值 ${n/1e3:.0f}K 存续{l}ms"
                   for s,p,n,l in zip(fw_side,fw_p,fw_n,fw_life)]
        fig.add_trace(go.Scatter(x=fw_ts, y=fw_p, mode="markers", name="⚠️ 疑似假挂单",
            marker=dict(color=fw_col, size=fw_sz, symbol="diamond", opacity=0.85,
                        line=dict(color="rgba(255,255,255,0.5)", width=1)),
            text=fw_text, hovertemplate="%{text}<extra></extra>"))
    if absorptions:
        ab_ts  = [pd.to_datetime(a.timestamp_ms, unit="ms") for a in absorptions]
        ab_p   = [a.price for a in absorptions]
        ab_n   = [a.absorbed_notional for a in absorptions]
        ab_sz  = [max(6, min(30, n/20000)) for n in ab_n]
        ab_text= [f"{'买' if a.side=='bid' else '卖'}侧吸收 @{a.price:.2f}<br>吃掉${a.absorbed_notional/1e3:.0f}K 补单${a.refill_notional/1e3:.0f}K 延迟{a.refill_delay_ms}ms"
                  for a in absorptions]
        fig.add_trace(go.Scatter(x=ab_ts, y=ab_p, mode="markers", name="✅ 大单被吃/快速补单",
            marker=dict(color="#1dc796", size=ab_sz, symbol="star", opacity=0.85),
            text=ab_text, hovertemplate="%{text}<extra></extra>"))
    if not fake_walls and not absorptions:
        fig.add_annotation(text="等待假挂单/吸收事件…", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
    fig.update_layout(height=320,
        **_layout(title=dict(text="假挂单检测 & 大单吸收  ·  Fake Wall & Absorption",
                             x=0.02, y=0.98, xanchor="left", font=dict(size=14, color="#f3f8ff"))))
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridcolor=_GRID_COL, tickformat=".2f")
    return fig


def build_ob_delta_heatmap(delta_history: list, price_buckets: int = 20) -> go.Figure:
    """盘口撤单/新增热力图（按价格分层）"""
    fig = go.Figure()
    if not delta_history:
        fig.add_annotation(text="等待盘口差量数据…", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        fig.update_layout(height=300, **_layout()); return fig

    prices    = [d.price for d in delta_history]
    p_min, p_max = min(prices), max(prices)
    if p_max <= p_min: p_max = p_min + 1
    bucket_w  = (p_max - p_min) / price_buckets
    add_by_bucket    = [0.0] * price_buckets
    cancel_by_bucket = [0.0] * price_buckets
    for d in delta_history:
        idx = min(price_buckets-1, int((d.price - p_min) / bucket_w))
        if d.delta_notional > 0: add_by_bucket[idx]    += d.delta_notional
        else:                    cancel_by_bucket[idx]  += abs(d.delta_notional)

    mid_prices = [p_min + bucket_w * (i + 0.5) for i in range(price_buckets)]
    net        = [a - c for a, c in zip(add_by_bucket, cancel_by_bucket)]
    colors     = ["#1dc796" if n > 0 else "#ff6868" for n in net]

    fig.add_trace(go.Bar(x=[f"{p:.1f}" for p in mid_prices], y=add_by_bucket,
        name="新增", marker_color="#1dc796", opacity=0.8))
    fig.add_trace(go.Bar(x=[f"{p:.1f}" for p in mid_prices], y=[-c for c in cancel_by_bucket],
        name="撤单", marker_color="#ff6868", opacity=0.8))
    fig.update_layout(height=300, barmode="relative",
        **_layout(title=dict(text="分价位 新增/撤单 分布", x=0.02, y=0.98,
                             xanchor="left", font=dict(size=14, color="#f3f8ff"))))
    fig.update_xaxes(showgrid=False, title="价格")
    fig.update_yaxes(showgrid=True, gridcolor=_GRID_COL, tickformat=".2s")
    return fig


# ══════════════════════════════════════════════════════════════════════════════
# NEW: Composite Signal figure
# ══════════════════════════════════════════════════════════════════════════════

_CS_LABELS_CN = {
    "price_score": "价格动能", "oi_score": "OI方向",
    "cvd_score": "CVD流向", "funding_score": "资金费率",
    "crowd_score": "拥挤度"}


def build_composite_signal_figure(signals_by_exchange: Dict[str, list]) -> go.Figure:
    """各交易所合成信号时序折线图（含置信度）"""
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.06,
                        row_heights=[0.65, 0.35],
                        subplot_titles=("合成信号 Composite Score (-1偏空 ~ +1偏多)", "置信度 Confidence"))
    colors_map = {"binance":"#f0b90b","bybit":"#e6a817","okx":"#1267fe","hyperliquid":"#8b5cf6"}
    has_data = False
    for ek, sigs in signals_by_exchange.items():
        if not sigs: continue
        has_data = True
        ts  = [pd.to_datetime(s.timestamp_ms, unit="ms") for s in sigs]
        cs  = [s.composite_score for s in sigs]
        conf= [s.confidence for s in sigs]
        color = colors_map.get(ek, "#aaa")
        fig.add_trace(go.Scatter(x=ts, y=cs, mode="lines", name=ek.capitalize(),
            line=dict(color=color, width=2.2)), row=1, col=1)
        fig.add_trace(go.Scatter(x=ts, y=conf, mode="lines", name=f"{ek.capitalize()} 置信度",
            line=dict(color=color, width=1.2, dash="dot"), showlegend=False), row=2, col=1)

    if not has_data:
        fig.add_annotation(text="等待合成信号…", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")

    fig.add_hline(y=0,    line_color="rgba(255,255,255,0.2)", line_dash="dot", row=1, col=1)
    fig.add_hline(y=0.45, line_color="rgba(29,199,150,0.35)", line_dash="dash", line_width=1, row=1, col=1)
    fig.add_hline(y=-0.45,line_color="rgba(255,104,104,0.35)",line_dash="dash", line_width=1, row=1, col=1)
    fig.update_layout(height=420,
        **_layout(title=dict(text="OI+CVD+Funding+Crowd 合成信号  ·  Composite Signal Engine",
                             x=0.02, y=0.98, xanchor="left", font=dict(size=15, color="#f3f8ff"))))
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridcolor=_GRID_COL, row=1, col=1, range=[-1.1, 1.1])
    fig.update_yaxes(showgrid=True, gridcolor=_GRID_COL, row=2, col=1, range=[0, 1])
    return fig


def build_composite_radar_html(signal: "CompositeSignal") -> str:
    """五因子合成信号条形可视化"""
    if signal is None:
        return '<div style="color:#aaa;font-size:0.85rem;">等待合成信号数据…</div>'
    factors = [
        ("价格动能", signal.price_score,  "#62c2ff"),
        ("OI方向",   signal.oi_score,     "#1dc796"),
        ("CVD流向",  signal.cvd_score,    "#a8ff78"),
        ("资金费率", signal.funding_score,"#ffa94d"),
        ("拥挤度",   signal.crowd_score,  "#ff9a9a"),
    ]
    rows_html = ""
    for name, score, color in factors:
        pct_pos   = max(0, score) * 50
        pct_neg   = max(0, -score) * 50
        score_str = "{:+.2f}".format(score)
        fill_right = (
            '<div style="width:{:.1f}%;height:100%;background:{};'
            'border-radius:0 3px 3px 0;"></div>'
        ).format(pct_pos, color)
        fill_left = (
            '<div style="width:{:.1f}%;height:100%;background:#ff6868;'
            'border-radius:3px 0 0 3px;margin-left:auto;"></div>'
        ).format(pct_neg)
        rows_html += (
            '<div style="display:flex;align-items:center;margin-bottom:5px;gap:6px;">'
            '<span style="width:60px;font-size:0.75rem;color:#bcd;text-align:right;">{name}</span>'
            '<div style="flex:1;height:8px;background:rgba(255,255,255,0.08);'
            'border-radius:4px;display:flex;overflow:hidden;">'
            '<div style="width:50%;display:flex;justify-content:flex-end;">{fl}</div>'
            '<div style="width:1px;background:rgba(255,255,255,0.3);"></div>'
            '<div style="width:50%;display:flex;">{fr}</div>'
            '</div>'
            '<span style="width:36px;font-size:0.75rem;color:{color};font-weight:600;">{score}</span>'
            '</div>'
        ).format(name=name, fl=fill_left, fr=fill_right, color=color, score=score_str)
    label     = signal.signal_label
    sig_col   = signal.signal_color
    comp_str  = "{:+.3f}".format(signal.composite_score)
    conf_pct  = "{:.0%}".format(signal.confidence)
    conf_w    = int(signal.confidence * 100)
    conf_col  = "#1dc796" if signal.confidence > 0.6 else "#ffa94d" if signal.confidence > 0.35 else "#ff6868"
    return (
        '<div style="padding:14px 16px;border-radius:18px;'
        'border:1px solid rgba(255,255,255,0.14);'
        'background:rgba(255,255,255,0.06);backdrop-filter:blur(20px);">'
        '<div style="font-size:0.7rem;color:#bcd;text-transform:uppercase;'
        'letter-spacing:0.1em;margin-bottom:6px;">合成信号 Composite</div>'
        '<div style="font-size:1.4rem;font-weight:800;color:{sig_col};margin-bottom:8px;">{label}</div>'
        '<div style="font-size:0.8rem;color:#aac;margin-bottom:10px;">'
        '综合分 {comp} | 置信度 {conf_pct}</div>'
        '<div style="width:100%;height:4px;background:rgba(255,255,255,0.1);'
        'border-radius:2px;margin-bottom:12px;">'
        '<div style="width:{conf_w}%;height:100%;background:{conf_col};'
        'border-radius:2px;transition:width 0.4s;"></div>'
        '</div>'
        '{rows}'
        '</div>'
    ).format(sig_col=sig_col, label=label, comp=comp_str, conf_pct=conf_pct,
             conf_w=conf_w, conf_col=conf_col, rows=rows_html)

# ══════════════════════════════════════════════════════════════════════════════
# NEW: Liquidation Cluster V2 figure
# ══════════════════════════════════════════════════════════════════════════════

def build_liq_cluster_v2_figure(clusters: list) -> go.Figure:
    """爆仓簇V2：按时间轴 + 跨所联动标注"""
    fig = go.Figure()
    if not clusters:
        fig.add_annotation(text="暂无爆仓簇（等待连续爆仓事件…）",
                           showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        fig.update_layout(height=340, **_layout()); return fig

    sorted_c = sorted(clusters, key=lambda c: c.start_ms)
    for c in sorted_c:
        ts_s = pd.to_datetime(c.start_ms, unit="ms")
        ts_e = pd.to_datetime(c.end_ms,   unit="ms")
        color = "#ff6868" if c.dominant_side=="long" else "#1dc796"
        alpha = 0.2 + c.cascade_score * 0.5
        exch_str = "+".join(sorted(c.exchanges))
        label = f"{'🌐跨所' if c.cross_exchange else '单所'} {exch_str}<br>多{c.long_count}单 ${c.long_notional/1e3:.0f}K<br>空{c.short_count}单 ${c.short_notional/1e3:.0f}K<br>强度 {c.intensity/1e3:.0f}K/s"
        # Bubble at midpoint
        ts_mid = pd.to_datetime((c.start_ms+c.end_ms)//2, unit="ms")
        sz = max(10, min(50, c.total_notional / 50_000))
        border = "#f8d35e" if c.cross_exchange else "rgba(255,255,255,0.3)"
        fig.add_trace(go.Scatter(
            x=[ts_mid], y=[c.total_notional / 1e3],
            mode="markers+text",
            marker=dict(size=sz, color=color,
                        opacity=0.85,
                        line=dict(color=border, width=2 if c.cross_exchange else 1)),
            text=[f"{'🌐' if c.cross_exchange else ''}{c.dominant_side[:1].upper()}"],
            textposition="middle center",
            textfont=dict(size=10, color="#fff"),
            name=f"{'跨所' if c.cross_exchange else '单所'}爆仓簇",
            hovertemplate=label+"<extra></extra>",
            showlegend=False))

    # Add legend traces
    fig.add_trace(go.Scatter(x=[None], y=[None], mode="markers", name="🌐 跨所联动",
        marker=dict(size=12, color="#ffa94d", line=dict(color="#f8d35e",width=2))))
    fig.add_trace(go.Scatter(x=[None], y=[None], mode="markers", name="多头爆仓簇",
        marker=dict(size=12, color="#ff6868")))
    fig.add_trace(go.Scatter(x=[None], y=[None], mode="markers", name="空头爆仓簇",
        marker=dict(size=12, color="#1dc796")))

    fig.update_layout(height=340,
        **_layout(title=dict(text="爆仓簇 V2  ·  Liquidation Clusters & Cross-Exchange Cascade",
                             x=0.02, y=0.98, xanchor="left", font=dict(size=15,color="#f3f8ff"))))
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridcolor=_GRID_COL, title="总爆仓额 ($K)", tickformat=".0f")
    return fig


def build_liq_cross_ex_timeline(clusters: list, liq_by_exchange: Dict[str, list]) -> go.Figure:
    """跨所爆仓联动时间轴（甘特图风格）"""
    fig = go.Figure()
    ex_colors = {"binance":"#f0b90b","bybit":"#e6a817","okx":"#1267fe","hyperliquid":"#8b5cf6"}
    ex_y      = {"binance":4,"bybit":3,"okx":2,"hyperliquid":1}
    now_ms    = int(time.time()*1000)
    cutoff    = now_ms - 300_000  # last 5 min

    for ek, evts in liq_by_exchange.items():
        recent = [e for e in evts if e.timestamp_ms >= cutoff]
        if not recent: continue
        color = ex_colors.get(ek, "#aaa")
        y_val = ex_y.get(ek, 0)
        for e in recent:
            sz = max(6, min(24, (e.notional or 1)/10000))
            sym = "triangle-down" if e.side=="long" else "triangle-up"
            fig.add_trace(go.Scatter(
                x=[pd.to_datetime(e.timestamp_ms, unit="ms")],
                y=[y_val + (0.1 if e.side=="short" else -0.1)],
                mode="markers", showlegend=False,
                marker=dict(size=sz, color=color, symbol=sym, opacity=0.9),
                hovertemplate=f"{ek} {e.side} @{e.price:.2f} ${(e.notional or 0)/1e3:.1f}K<extra></extra>"))

    # Cross-exchange cluster highlights
    for c in clusters:
        if c.start_ms < cutoff: continue
        if c.cross_exchange:
            ts_s = pd.to_datetime(c.start_ms, unit="ms")
            ts_e = pd.to_datetime(max(c.end_ms, c.start_ms+1000), unit="ms")
            fig.add_vrect(x0=ts_s, x1=ts_e, fillcolor="rgba(248,211,94,0.12)",
                          line_color="rgba(248,211,94,0.5)", line_width=1,
                          annotation_text="🌐跨所", annotation_position="top left",
                          annotation_font=dict(size=9, color="#f8d35e"))

    fig.update_layout(
        height=280,
        **_layout(title=dict(text="跨所爆仓联动时间轴 (近5分钟)",
                             x=0.02,y=0.98,xanchor="left",font=dict(size=14,color="#f3f8ff"))),
        yaxis=dict(tickvals=[1,2,3,4],ticktext=["Hyperliquid","OKX","Bybit","Binance"],
                   showgrid=False, range=[0.3,4.7]))
    fig.update_xaxes(showgrid=False)
    return fig


# ══════════════════════════════════════════════════════════════════════════════
# NEW: Alert Timeline figure
# ══════════════════════════════════════════════════════════════════════════════

_SEVERITY_COLOR = {"strong":"#ff4444","medium":"#ffa94d","weak":"#62c2ff"}
_ATYPE_SYMBOL   = {
    "spot_lead_up":"triangle-up","spot_lead_down":"triangle-down",
    "oi_up_cvd_weak":"diamond","oi_down_cvd_up":"diamond-open",
    "crowd_liq_combo":"star","fake_wall":"x","composite_signal":"circle",
    "diverge_extreme":"star-triangle-up",}

def build_alert_timeline_figure(timeline: list) -> go.Figure:
    """告警时间线：各类型用不同形状+颜色"""
    fig = go.Figure()
    if not timeline:
        fig.add_annotation(text="暂无已确认告警（等待连续触发…）",
                           showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        fig.update_layout(height=280, **_layout()); return fig

    # Group by type for legend
    from collections import defaultdict
    groups: Dict[str, list] = defaultdict(list)
    for a in timeline: groups[a.alert_type].append(a)

    _ATYPE_CN = {
        "spot_lead_up":"现货先拉↑","spot_lead_down":"现货先跌↓",
        "oi_up_cvd_weak":"OI升/买弱","oi_down_cvd_up":"OI降/轧空",
        "crowd_liq_combo":"拥挤+爆仓","fake_wall":"假挂单",
        "composite_signal":"合成信号","diverge_extreme":"极端乖离"}

    for atype, alerts in groups.items():
        color  = _SEVERITY_COLOR.get(alerts[-1].severity, "#aaa")
        symbol = _ATYPE_SYMBOL.get(atype, "circle")
        ts     = [pd.to_datetime(a.timestamp_ms, unit="ms") for a in alerts]
        scores = [a.score for a in alerts]
        texts  = [a.message[:60]+"…" if len(a.message)>60 else a.message for a in alerts]
        sizes  = [max(8, min(24, s*20)) for s in scores]
        fig.add_trace(go.Scatter(
            x=ts, y=scores, mode="markers",
            name=_ATYPE_CN.get(atype, atype),
            marker=dict(size=sizes, color=color, symbol=symbol, opacity=0.9,
                        line=dict(color="rgba(255,255,255,0.4)",width=1)),
            text=texts, hovertemplate="%{text}<br>强度 %{y:.2f}<extra></extra>"))

    fig.add_hline(y=0.6, line_color="rgba(255,68,68,0.4)", line_dash="dot", line_width=1)
    fig.add_hline(y=0.35,line_color="rgba(255,165,0,0.4)", line_dash="dot", line_width=1)
    fig.update_layout(height=300,
        **_layout(title=dict(text="告警时间线  ·  Confirmed Alert Timeline (去抖动后)",
                             x=0.02,y=0.98,xanchor="left",font=dict(size=14,color="#f3f8ff"))))
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridcolor=_GRID_COL, range=[0,1.05], title="强度 Score")
    return fig


# ══════════════════════════════════════════════════════════════════════════════
# NEW: Replay/Playback helper
# ══════════════════════════════════════════════════════════════════════════════

def build_replay_price_figure(frames: list, speed_label: str = "1x") -> go.Figure:
    """回放价格+CVD+爆仓 合一图"""
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        vertical_spacing=0.05, row_heights=[0.7, 0.3],
                        subplot_titles=("价格回放 (各所)", "CVD 回放"))
    if not frames:
        fig.add_annotation(text="暂无录制帧（点击 开始录制 后等待数据）",
                           showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        fig.update_layout(height=380, **_layout()); return fig

    ex_colors = {"binance":"#f0b90b","bybit":"#e6a817","okx":"#1267fe","hyperliquid":"#8b5cf6"}
    ts = [pd.to_datetime(f.timestamp_ms, unit="ms") for f in frames]

    for ek in ("binance","bybit","okx","hyperliquid"):
        prices = [f.prices.get(ek) for f in frames]
        if any(p is not None for p in prices):
            fig.add_trace(go.Scatter(x=ts, y=prices, mode="lines", name=ek.capitalize(),
                line=dict(color=ex_colors.get(ek,"#aaa"), width=1.8)), row=1, col=1)
        cvds = [f.cvd_values.get(ek, 0) for f in frames]
        fig.add_trace(go.Scatter(x=ts, y=cvds, mode="lines", name=f"{ek.capitalize()} CVD",
            line=dict(color=ex_colors.get(ek,"#aaa"), width=1.2, dash="dot"),
            showlegend=False), row=2, col=1)

    # Liquidation dots on price chart
    for f in frames:
        for e in f.liq_events:
            if e.price is None: continue
            col = "#ff6868" if e.side=="long" else "#1dc796"
            sym = "triangle-down" if e.side=="long" else "triangle-up"
            fig.add_trace(go.Scatter(
                x=[pd.to_datetime(f.timestamp_ms, unit="ms")], y=[e.price],
                mode="markers", showlegend=False,
                marker=dict(size=max(6, min(16, (e.notional or 1)/8000)),
                            color=col, symbol=sym, opacity=0.9),
                hovertemplate=f"{e.exchange} {e.side} @{e.price:.2f}<extra></extra>"), row=1, col=1)

    fig.update_layout(height=420,
        **_layout(title=dict(text=f"📼 回放模式 {speed_label}  ·  Replay (价格+CVD+爆仓)",
                             x=0.02,y=0.98,xanchor="left",font=dict(size=15,color="#f3f8ff"))))
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridcolor=_GRID_COL)
    return fig


# ══════════════════════════════════════════════════════════════════════════════
# v7 新增：价格关口识别 / 多空力量面板 / 热点币发现
# ══════════════════════════════════════════════════════════════════════════════

def detect_price_levels(candles: list, ref_price: float,
                         lookback: int = 120, min_touches: int = 2,
                         tolerance_pct: float = 0.15) -> list:
    """
    自动识别支撑/阻力关口。
    算法：找历史高低点聚集区，合并相近价位，按触碰次数排序。
    返回 [{"price": float, "type": "support"/"resistance", "touches": int, "strength": float}]
    """
    if not candles or ref_price <= 0:
        return []

    candles = candles[-lookback:]
    tol = ref_price * tolerance_pct / 100

    # Collect pivot highs and lows
    pivots = []
    for i in range(2, len(candles) - 2):
        h = candles[i].high
        l = candles[i].low
        # Pivot high: higher than neighbors
        if h > candles[i-1].high and h > candles[i-2].high and            h > candles[i+1].high and h > candles[i+2].high:
            pivots.append(("resistance", h))
        # Pivot low: lower than neighbors
        if l < candles[i-1].low and l < candles[i-2].low and            l < candles[i+1].low and l < candles[i+2].low:
            pivots.append(("support", l))

    # Add round number levels (整数关口)
    magnitude = 10 ** (len(str(int(ref_price))) - 2)
    round_base = round(ref_price / magnitude) * magnitude
    for m in range(-5, 6):
        rnd = round_base + m * magnitude
        if rnd > 0 and abs(rnd - ref_price) / ref_price < 0.05:
            pivots.append(("round", rnd))

    # Cluster nearby pivots
    clusters = []
    used = set()
    for i, (ptype, price) in enumerate(pivots):
        if i in used:
            continue
        cluster_prices = [price]
        cluster_types  = [ptype]
        for j, (ptype2, price2) in enumerate(pivots):
            if j != i and j not in used and abs(price2 - price) <= tol:
                cluster_prices.append(price2)
                cluster_types.append(ptype2)
                used.add(j)
        used.add(i)
        avg_price = sum(cluster_prices) / len(cluster_prices)
        touches   = len(cluster_prices)
        # Determine type by majority or position vs ref
        if "round" in cluster_types:
            lvl_type = "resistance" if avg_price > ref_price else "support"
        else:
            lvl_type = max(set(cluster_types), key=cluster_types.count)
        clusters.append({
            "price":    round(avg_price, 2),
            "type":     lvl_type,
            "touches":  touches,
            "strength": min(1.0, touches / 5.0),
            "is_round": "round" in cluster_types,
        })

    # Filter by min touches, sort by distance from ref
    result = [c for c in clusters if c["touches"] >= min_touches]
    result.sort(key=lambda x: abs(x["price"] - ref_price))
    return result[:12]


def build_price_levels_annotations(fig, levels: list, ref_price: float,
                                    row: int = 1, col: int = 1):
    """把价格关口标注到 plotly 图上（水平线 + 文字标签）"""
    for lvl in levels:
        price   = lvl["price"]
        ltype   = lvl["type"]
        touches = lvl["touches"]
        is_rnd  = lvl.get("is_round", False)

        color = "#ff8866" if ltype == "resistance" else "#66ccff"
        dash  = "dot" if is_rnd else "dash"
        width = 0.8 + lvl["strength"] * 0.8

        dist_pct = (price - ref_price) / ref_price * 100
        label = f"{'R' if ltype=='resistance' else 'S'} {price:,.1f} ({dist_pct:+.2f}%)"
        if is_rnd:
            label = f"🔵 {label}"

        fig.add_hline(
            y=price, line_color=color, line_dash=dash, line_width=width,
            annotation_text=label,
            annotation_font=dict(color=color, size=10),
            annotation_position="right",
            row=row, col=col,
        )
    return fig


def build_bull_bear_power_figure(snapshots: list, ob_levels: dict,
                                  cvd_history: dict) -> "go.Figure":
    """
    多空力量实时面板：
    - 买盘深度 vs 卖盘深度（盘口前N档名义值）
    - CVD速率（最近N秒）
    - 综合力量得分 -100 ~ +100
    """
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    fig = make_subplots(
        rows=1, cols=3,
        subplot_titles=["盘口力量", "CVD速率", "综合力量"],
        column_widths=[0.35, 0.35, 0.30],
        specs=[[{"type": "xy"}, {"type": "xy"}, {"type": "indicator"}]],
    )

    ex_colors = {"binance":"#f0b90b","bybit":"#ff6b00","okx":"#00b4d8","hyperliquid":"#7c3aed",
                 "Binance":"#f0b90b","Bybit":"#ff6b00","OKX":"#00b4d8","Hyperliquid":"#7c3aed"}

    # 1. Bid/Ask depth bars per exchange
    for snap in (snapshots or []):
        if snap.status != "ok":
            continue
        ek = snap.exchange.lower()
        levels = ob_levels.get(ek, [])
        if not levels:
            continue
        bid_notional = sum(l.price * l.size for l in levels
                           if l.side == "bid" and l.size > 0)
        ask_notional = sum(l.price * l.size for l in levels
                           if l.side == "ask" and l.size > 0)
        color = ex_colors.get(snap.exchange, "#aaa")
        fig.add_trace(go.Bar(
            name=snap.exchange, x=[snap.exchange],
            y=[bid_notional / 1e6], marker_color="#1dc796",
            showlegend=False, offsetgroup=snap.exchange,
        ), row=1, col=1)
        fig.add_trace(go.Bar(
            name=snap.exchange, x=[snap.exchange],
            y=[-ask_notional / 1e6], marker_color="#ff6868",
            showlegend=False, offsetgroup=snap.exchange,
        ), row=1, col=1)

    # 2. CVD velocity (last 10 points slope)
    for ek, pts in (cvd_history or {}).items():
        if len(pts) < 2:
            continue
        recent = list(pts)[-10:]
        if len(recent) < 2:
            continue
        deltas = [p.delta for p in recent]
        velocity = sum(deltas) / len(deltas)
        color = ex_colors.get(ek, "#aaa")
        fig.add_trace(go.Bar(
            x=[ek.capitalize()], y=[velocity / 1e3],
            marker_color="#1dc796" if velocity > 0 else "#ff6868",
            name=ek, showlegend=False,
        ), row=1, col=2)

    # 3. Composite power score gauge
    scores = []
    for snap in (snapshots or []):
        if snap.status != "ok":
            continue
        ek = snap.exchange.lower()
        # Bid/Ask imbalance
        levels = ob_levels.get(ek, [])
        if levels:
            bid_n = sum(l.price*l.size for l in levels if l.side=="bid")
            ask_n = sum(l.price*l.size for l in levels if l.side=="ask")
            total = bid_n + ask_n
            if total > 0:
                scores.append((bid_n - ask_n) / total * 100)
    composite = sum(scores) / len(scores) if scores else 0
    color = "#1dc796" if composite > 5 else "#ff6868" if composite < -5 else "#aaaaaa"

    fig.add_trace(go.Indicator(
        mode="gauge+number+delta",
        value=composite,
        delta={"reference": 0},
        gauge={
            "axis": {"range": [-100, 100]},
            "bar":  {"color": color, "thickness": 0.3},
            "steps": [
                {"range": [-100, -20], "color": "rgba(255,104,104,0.2)"},
                {"range": [-20, 20],   "color": "rgba(170,170,170,0.1)"},
                {"range": [20, 100],   "color": "rgba(29,199,150,0.2)"},
            ],
        },
        number={"font": {"size": 28, "color": color}},
        title={"text": "买卖力量", "font": {"size": 12}},
    ), row=1, col=3)

# 用 add_shape 代替 add_hline，避免 Indicator 子图无 xaxis 导致的报错
    for _col, _yref in [(1, "y"), (2, "y2")]:
        fig.add_shape(
            type="line", xref="paper", yref=_yref,
            x0=0, x1=1, y0=0, y1=0,
            line=dict(color="rgba(255,255,255,0.2)", width=1),
        )
    fig.update_layout(
        height=320, barmode="relative",
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=20, r=20, t=40, b=20),
        legend=dict(orientation="h", y=1.08),
    )
    fig.update_yaxes(title_text="名义值(M$)", row=1, col=1)
    fig.update_yaxes(title_text="速率(K$)", row=1, col=2)
    return fig


def detect_hot_coins(market_rows: list, top_n: int = 5) -> list:
    """
    热点币自动发现：综合 OI变化 / Vol变化 / Liq异常增速
    返回 [{"coin", "reason", "score", "direction"}]
    """
    if not market_rows:
        return []

    scored = []
    for row in market_rows:
        score = 0.0
        reasons = []

        # OI异常增速
        oi_chg = row.oi_change_1h_pct or 0
        if abs(oi_chg) > 5:
            score += min(40, abs(oi_chg) * 2)
            reasons.append(f"OI{oi_chg:+.1f}%/1h")

        # 资金费率极端
        fr = row.funding_avg or 0
        if abs(fr) > 8:
            score += min(30, abs(fr) * 2)
            reasons.append(f"FR{fr:+.1f}bps")

        # 爆仓集中
        liq = row.liq_24h_total or 0
        if liq > 5_000_000:
            score += min(30, liq / 1_000_000)
            reasons.append(f"爆仓${liq/1e6:.1f}M")

        if score > 10:
            direction = "bull" if oi_chg > 0 and fr > 0 else                         "bear" if oi_chg < 0 else "neutral"
            scored.append({
                "coin":      row.coin,
                "score":     round(score, 1),
                "reason":    " · ".join(reasons),
                "direction": direction,
                "oi_chg_1h": oi_chg,
                "fr_bps":    fr,
                "liq_total": liq,
            })

    scored.sort(key=lambda x: x["score"], reverse=True)
    return scored[:top_n]
