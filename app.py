from __future__ import annotations
import time, uuid
from typing import Dict, List, Tuple, Optional
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from analytics import (
    summarize_orderbook, build_local_book_figure,
    build_cvd_from_candles, build_cvd_from_trades, build_cvd_figure,
    build_oi_delta_points, build_oi_delta_figure, build_oi_velocity_figure,
    build_oi_delta_summary,
    build_top_trader_figure,
    build_basis_figure, build_term_structure_figure, build_spot_vs_perp_figure,
    build_liquidation_cascade_figure, detect_liquidation_clusters,
    build_iceberg_figure, build_liquidity_gap_frame,
    build_funding_comparison_figure, build_market_sentiment_summary,
    merge_liquidation_events, build_liquidation_metrics,
    build_liquidation_frame, build_liquidation_figure,
    build_mbo_profile_frame, build_mbo_figure,
    build_probability_heatmap_frame, build_heat_zone_frame, build_heatmap_figure,
    LIQUIDATION_COLORSCALE, TP_COLORSCALE, STOP_COLORSCALE,
    build_ob_quality_figure, build_fake_wall_figure, build_ob_delta_heatmap,  # v4
    build_composite_signal_figure, build_composite_radar_html,
    build_liq_cluster_v2_figure, build_liq_cross_ex_timeline,
    build_alert_timeline_figure,
    build_replay_price_figure,
)
from exchanges import (
    EXCHANGE_ORDER, SUPPORTED_INTERVALS, default_symbols,
    fetch_exchange_candles, fetch_exchange_liquidations,
    fetch_exchange_oi_history, fetch_exchange_orderbook,
    fetch_exchange_recent_trades, fetch_exchange_top_trader_ratio,
    fetch_exchange_global_long_short_ratio,
    fetch_exchange_spot_ticker, fetch_exchange_futures_oi_list,
    interval_to_millis,
)
from models import (
    Candle, ExchangeSnapshot, LiquidationEvent, OIPoint, OrderBookLevel,
    AlertRule, AlertEvent, LocalOrderBook,
)
from realtime import LiveTerminalService

try:
    from exchanges import fetch_binance_long_short_count, fetch_binance_taker_ratio
    _HAS_LS_COUNT = True
except ImportError:
    _HAS_LS_COUNT = False

POPULAR_COINS = ["BTC","ETH","SOL","XRP","BNB","DOGE","ADA","SUI","AVAX",
                 "LINK","LTC","HYPE","TAO","PEPE","PENDLE","WIF","TRUMP","FARTCOIN"]
WATCHLIST_DEFAULT = ["BTC","ETH","SOL","XRP","BNB"]
# Binance first
EXCHANGE_ORDER_UI = ("binance", "bybit", "okx", "hyperliquid")
EXCHANGE_TITLES   = {"bybit":"Bybit","binance":"Binance","okx":"OKX","hyperliquid":"Hyperliquid"}
BID_PALETTE = ["#dff8ff","#bdefff","#92ddff","#5fc0ff","#279cff","#1768d3"]
ASK_PALETTE = ["#fff1db","#ffd9ad","#ffbe80","#ff9a59","#ff6938","#d8452d"]
CARD_STATUS = {"ok":"正常","error":"异常"}

ALERT_METRICS    = {"price":"最新价 (Price)","oi":"持仓金额 (OI Notional)","funding":"资金费率 (Funding Rate)",
                    "liq_notional":"爆仓额-60min","cvd":"CVD累积","spread_bps":"价差bps",
                    "oi_velocity":"OI加仓速率/min"}
ALERT_CONDITIONS = {"above":"超过 (Above)","below":"低于 (Below)",
                    "cross_up":"向上穿越 (Cross Up)","cross_down":"向下穿越 (Cross Down)"}

# Spot-perp alert type labels
SPOT_PERP_ALERT_LABELS = {
    "spot_lead_up":    ("🟢 现货先拉↑", "#1dc796"),
    "spot_lead_down":  ("🔴 现货先跌↓", "#ff6868"),
    "oi_up_cvd_weak":  ("⚠️ OI升/买弱",  "#ffa94d"),
    "oi_down_cvd_up":  ("🔵 OI降/轧空",  "#62c2ff"),
    "diverge_extreme": ("🚨 极端乖离",    "#ff4444"),
    "crowd_liq_combo": ("⚡ 拥挤+爆仓",  "#ff8c00"),
}

st.set_page_config(page_title="多交易所流动性终端 v4", layout="wide", initial_sidebar_state="expanded")
st.markdown("""
<style>
:root{--gs:rgba(255,255,255,0.09);--gb:rgba(255,255,255,0.18);--shadow:0 22px 52px rgba(6,11,21,0.28);}
html,body,[class*="css"]{font-family:"SF Pro Display","Segoe UI",sans-serif;}
.stApp{background:radial-gradient(circle at 8% 14%,rgba(148,195,255,0.34),transparent 26%),radial-gradient(circle at 90% 10%,rgba(255,204,158,0.22),transparent 24%),radial-gradient(circle at 78% 82%,rgba(134,234,221,0.16),transparent 20%),linear-gradient(140deg,#0f1828 0%,#122038 42%,#101925 100%);color:#f8fbff;background-attachment:fixed;}
.stApp::before{content:"";position:fixed;inset:0;background:linear-gradient(180deg,rgba(255,255,255,0.06),transparent 24%),radial-gradient(circle at 50% 0%,rgba(255,255,255,0.10),transparent 34%);pointer-events:none;z-index:0;}
header[data-testid="stHeader"]{background:rgba(10,16,27,0.24);border-bottom:1px solid rgba(255,255,255,0.08);backdrop-filter:blur(20px);}
.block-container{position:relative;z-index:1;padding-top:1.1rem;padding-bottom:2rem;max-width:1580px;}
section[data-testid="stSidebar"]{background:linear-gradient(180deg,rgba(16,26,42,0.82),rgba(12,20,33,0.72));border-right:1px solid rgba(255,255,255,0.12);backdrop-filter:blur(30px);}
section[data-testid="stSidebar"]>div{background:transparent;}
section[data-testid="stSidebar"] .stMarkdown,section[data-testid="stSidebar"] label,section[data-testid="stSidebar"] span,section[data-testid="stSidebar"] p,section[data-testid="stSidebar"] [data-testid="stWidgetLabel"]*{color:#dce7f7!important;}
.hero-shell{position:relative;overflow:hidden;padding:24px 28px 20px;margin-bottom:1rem;border-radius:32px;border:1px solid rgba(255,255,255,0.18);background:linear-gradient(135deg,rgba(255,255,255,0.16),rgba(255,255,255,0.07));box-shadow:var(--shadow);backdrop-filter:blur(34px);}
.hero-kicker{color:#d7e7ff;font-size:0.74rem;text-transform:uppercase;letter-spacing:0.2em;margin-bottom:0.4rem;}
.hero-title{color:#fff;font-size:1.95rem;font-weight:720;line-height:1.05;margin-bottom:0.36rem;}
.hero-sub{max-width:900px;color:#e1ebf8;font-size:0.94rem;line-height:1.58;}
.helper-bar{display:flex;flex-wrap:wrap;gap:0.5rem;margin-top:0.8rem;}
.helper-pill{padding:0.4rem 0.82rem;border-radius:999px;border:1px solid rgba(255,255,255,0.14);background:rgba(255,255,255,0.10);color:#f5f9ff;font-size:0.82rem;backdrop-filter:blur(18px);}
.glass-section{margin:0.9rem 0 0.56rem;padding:0.82rem 0.92rem;border-radius:22px;border:1px solid rgba(255,255,255,0.13);background:linear-gradient(135deg,rgba(255,255,255,0.10),rgba(255,255,255,0.045));box-shadow:var(--shadow);backdrop-filter:blur(26px);}
.glass-kicker{color:#bfd5f2;font-size:0.68rem;text-transform:uppercase;letter-spacing:0.18em;margin-bottom:0.24rem;}
.glass-title{color:#fff;font-size:1.08rem;font-weight:680;margin-bottom:0.16rem;}
.glass-title::after{content:"";display:block;width:44px;height:1px;margin-top:0.5rem;background:linear-gradient(90deg,rgba(255,255,255,0.82),rgba(255,255,255,0.12));}
.glass-sub{color:#dce8f6;font-size:0.88rem;line-height:1.5;margin-top:0.5rem;}
.status-strip{margin:0.2rem 0 0.8rem;padding:0.72rem 0.92rem;border-radius:16px;border:1px solid rgba(255,255,255,0.12);background:rgba(255,255,255,0.08);backdrop-filter:blur(26px);color:#eef5ff;font-size:0.88rem;font-weight:540;}
.signal-card{display:inline-block;margin:0.18rem 0.28rem 0.18rem 0;padding:0.28rem 0.68rem;border-radius:999px;border:1px solid rgba(255,255,255,0.14);background:rgba(255,255,255,0.08);color:#f5f9ff;font-size:0.82rem;backdrop-filter:blur(14px);}
.alert-badge{display:inline-block;padding:0.24rem 0.6rem;border-radius:999px;border:1px solid rgba(255,120,100,0.4);background:rgba(255,80,60,0.15);color:#ffb3a7;font-size:0.8rem;font-weight:600;backdrop-filter:blur(12px);}
.sp-alert-high{border:1px solid rgba(255,68,68,0.5)!important;background:rgba(255,68,68,0.12)!important;}
.sp-alert-medium{border:1px solid rgba(255,165,0,0.5)!important;background:rgba(255,165,0,0.10)!important;}
.sp-alert-low{border:1px solid rgba(98,194,255,0.4)!important;background:rgba(98,194,255,0.08)!important;}
.liq-real{border-left:3px solid #ff6868!important;}
.liq-inferred{border-left:3px solid #ffa94d!important;}
.stMarkdown p,.stCaption,label,[data-testid="stWidgetLabel"] p{color:#dce8f6!important;}
div[data-testid="stMetric"]{background:linear-gradient(145deg,rgba(255,255,255,0.15),rgba(255,255,255,0.07));border:1px solid rgba(255,255,255,0.16);border-radius:20px;padding:0.82rem 0.92rem;box-shadow:var(--shadow);backdrop-filter:blur(28px);}
div[data-testid="stMetric"]:hover,div[data-testid="stPlotlyChart"]:hover,div[data-testid="stDataFrame"]:hover{transform:translateY(-1px);border-color:rgba(255,255,255,0.22);box-shadow:0 26px 58px rgba(6,12,22,0.30);}
div[data-testid="stMetricLabel"]*{color:#d7e6f8!important;font-weight:600;}
div[data-testid="stMetricValue"]{color:#fff;}
.stTabs [data-baseweb="tab-list"]{gap:0.34rem;padding:0.34rem;margin-bottom:0.8rem;border-radius:999px;border:1px solid rgba(255,255,255,0.12);background:rgba(255,255,255,0.07);backdrop-filter:blur(24px);}
.stTabs [data-baseweb="tab"]{height:2.5rem;border-radius:999px;color:#d3e0f2;background:transparent;font-weight:600;}
.stTabs [aria-selected="true"]{background:linear-gradient(135deg,rgba(255,255,255,0.22),rgba(255,255,255,0.10));color:#fff!important;}
div[data-baseweb="select"]>div,.stTextInput input{background:rgba(255,255,255,0.10)!important;border:1px solid rgba(255,255,255,0.14)!important;border-radius:12px!important;color:#f8fbff!important;backdrop-filter:blur(18px);}
.stButton>button{border-radius:999px;border:1px solid rgba(255,255,255,0.15);background:linear-gradient(135deg,rgba(255,255,255,0.17),rgba(255,255,255,0.08));color:#fff;backdrop-filter:blur(16px);transition:transform 220ms ease;}
.stButton>button:hover{transform:translateY(-1px);}
div[data-testid="stDataFrame"],div[data-testid="stPlotlyChart"],details[data-testid="stExpander"]{border-radius:20px;overflow:hidden;border:1px solid rgba(255,255,255,0.12);box-shadow:var(--shadow);background:rgba(255,255,255,0.05);backdrop-filter:blur(24px);}
div[data-testid="stAlert"]{border-radius:16px;border:1px solid rgba(255,255,255,0.13);background:rgba(255,255,255,0.08);backdrop-filter:blur(22px);color:#f6fbff;}
.oi-vis-bar{display:inline-block;height:10px;border-radius:5px;vertical-align:middle;}
</style>""", unsafe_allow_html=True)


# ── Cached loaders ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=15,show_spinner=False)
def load_candles(ek,sym,iv,lim,to):
    try: return fetch_exchange_candles(ek,sym,iv,lim,timeout=to)
    except: return []

@st.cache_data(ttl=5,show_spinner=False)
def load_orderbook(ek,sym,lim,to):
    try: return fetch_exchange_orderbook(ek,sym,lim,timeout=to)
    except: return []

@st.cache_data(ttl=90,show_spinner=False)
def load_oi_backfill(ek,sym,iv,lim,to):
    try: return fetch_exchange_oi_history(ek,sym,iv,lim,timeout=to)
    except: return []

@st.cache_data(ttl=10,show_spinner=False)
def load_liquidations(ek,sym,lim,to):
    try: return fetch_exchange_liquidations(ek,sym,lim,timeout=to)
    except: return []

@st.cache_data(ttl=30,show_spinner=False)
def load_trades(ek,sym,lim,to):
    try: return fetch_exchange_recent_trades(ek,sym,lim,timeout=to)
    except: return []

@st.cache_data(ttl=60,show_spinner=False)
def load_top_trader(ek,sym,iv,lim,to):
    try: return fetch_exchange_top_trader_ratio(ek,sym,iv,lim,timeout=to)
    except: return []

@st.cache_data(ttl=60,show_spinner=False)
def load_global_ratio(ek,sym,iv,lim,to):
    try: return fetch_exchange_global_long_short_ratio(ek,sym,iv,lim,timeout=to)
    except: return []

@st.cache_data(ttl=60,show_spinner=False)
def load_spot_ticker(ek,coin,to):
    try: return fetch_exchange_spot_ticker(ek,coin,timeout=to)
    except: return None

@st.cache_data(ttl=120,show_spinner=False)
def load_futures_oi_list(ek,coin,to):
    try: return fetch_exchange_futures_oi_list(ek,coin,timeout=to)
    except: return []

@st.cache_data(ttl=45,show_spinner=False)
def load_ls_count(sym,iv,lim,to):
    if not _HAS_LS_COUNT: return []
    try: return fetch_binance_long_short_count(sym,iv,lim,to)
    except: return []

@st.cache_data(ttl=45,show_spinner=False)
def load_taker_ratio(sym,iv,lim,to):
    if not _HAS_LS_COUNT: return []
    try: return fetch_binance_taker_ratio(sym,iv,lim,to)
    except: return []


# ── Formatters ─────────────────────────────────────────────────────────────────
def fp(v): return "-" if v is None else f"{v:,.2f}"
def fc(v):
    if v is None: return "-"
    v=float(v); av=abs(v)
    if av>=1e9: return f"{v/1e9:.2f}B"
    if av>=1e6: return f"{v/1e6:.2f}M"
    if av>=1e3: return f"{v/1e3:.2f}K"
    return f"{v:.2f}"
def fbps(v): return "-" if v is None else f"{v:.2f} bps"
def fpct(v): return "-" if v is None else f"{v:.2f}%"
def frate(v): return "-" if v is None else f"{v:+.6f}"

def render_section(title, subtitle="", kicker="Desk"):
    sub = f"<div class='glass-sub'>{subtitle}</div>" if subtitle else ""
    st.markdown(f"<div class='glass-section'><div class='glass-kicker'>{kicker}</div><div class='glass-title'>{title}</div>{sub}</div>", unsafe_allow_html=True)

def status_caption(s): return f"{s.exchange}: 正常" if s.status=="ok" else f"{s.exchange}: {s.error or '异常'}"

def resolve_service(symbol_map, timeout, sample_seconds, force_restart) -> LiveTerminalService:
    key = (tuple(sorted(symbol_map.items())), timeout, sample_seconds)
    svc = st.session_state.get("live_service")
    cur_key = st.session_state.get("live_service_key")
    if force_restart and svc: svc.stop(); svc=None; cur_key=None
    if svc is None or cur_key != key:
        if svc: svc.stop()
        svc = LiveTerminalService(symbol_map, timeout=timeout, sample_seconds=sample_seconds)
        st.session_state["live_service"] = svc
        st.session_state["live_service_key"] = key
    return svc

def merge_oi_points(backfill, session):
    merged = {}
    for p in backfill+session:
        ex = merged.get(p.timestamp_ms)
        if ex is None: merged[p.timestamp_ms] = OIPoint(p.timestamp_ms, p.open_interest, p.open_interest_notional)
        else:
            if ex.open_interest is None and p.open_interest is not None: ex.open_interest = p.open_interest
            if ex.open_interest_notional is None and p.open_interest_notional is not None: ex.open_interest_notional = p.open_interest_notional
    return sorted(merged.values(), key=lambda x: x.timestamp_ms)

def rgb_from_hex(h):
    h=h.lstrip("#"); return int(h[0:2],16),int(h[2:4],16),int(h[4:6],16)

def rgba_from_hex(h,a):
    r,g,b=rgb_from_hex(h); return f"rgba({r},{g},{b},{a:.3f})"

def palette_color(side, intensity):
    p = BID_PALETTE if side=="bid" else ASK_PALETTE
    return p[int(round(max(0,min(1,intensity))*(len(p)-1)))]

def aggregate_heat_bars(levels, ref, window_pct, buckets_per_side, bars_per_side):
    if not levels: return []
    if ref is None or ref<=0:
        bids=[l.price for l in levels if l.side=="bid"]; asks=[l.price for l in levels if l.side=="ask"]
        ref=(max(bids)+min(asks))*0.5 if bids and asks else (max(bids) if bids else min(asks) if asks else None)
    if not ref or ref<=0: return []
    lo=ref*(1-window_pct/100); hi=ref*(1+window_pct/100)
    def bucketize(side,sl,sh):
        sl=[l for l in levels if l.side==side and sl<=l.price<=sh and l.size>0]
        if not sl: return []
        step=max((sh-lo)/max(buckets_per_side,1), ref*0.00015)
        buckets={}
        for l in sl:
            idx=max(0,min(buckets_per_side-1,int((l.price-lo)/step)))
            b=buckets.setdefault(idx,{"side":side,"price_low":lo+idx*step,"price_high":lo+(idx+1)*step,"size":0.0})
            b["size"]+=l.size
        ranked=sorted(buckets.values(),key=lambda x:x["size"],reverse=True)[:bars_per_side]
        ranked.sort(key=lambda x:x["price_high"],reverse=(side=="bid"))
        return ranked
    all_bars=bucketize("bid",lo,ref)+bucketize("ask",ref,hi)
    if not all_bars: return []
    mx=max(b["size"] for b in all_bars)
    for b in all_bars:
        b["mid_price"]=(b["price_low"]+b["price_high"])*0.5
        b["distance_pct"]=abs(b["mid_price"]-ref)/ref*100
        b["intensity"]=0 if mx<=0 else b["size"]/mx
    return all_bars

def build_terminal_chart(candles, heat_bars, snapshot, interval):
    fig = make_subplots(rows=2,cols=1,shared_xaxes=True,vertical_spacing=0.04,row_heights=[0.78,0.22])
    if not candles:
        fig.add_annotation(text="没有可用K线数据",showarrow=False,x=0.5,y=0.5,xref="paper",yref="paper"); return fig
    df=pd.DataFrame({"ts":pd.to_datetime([c.timestamp_ms for c in candles],unit="ms"),
        "o":[c.open for c in candles],"h":[c.high for c in candles],
        "l":[c.low for c in candles],"c":[c.close for c in candles],"v":[c.volume for c in candles]})
    up,dn="#1dc796","#ff6868"
    fig.add_trace(go.Candlestick(x=df["ts"],open=df["o"],high=df["h"],low=df["l"],close=df["c"],
        increasing_line_color=up,increasing_fillcolor=up,decreasing_line_color=dn,decreasing_fillcolor=dn,name="K线"),row=1,col=1)
    vcols=[up if c>=o else dn for o,c in zip(df["o"],df["c"])]
    fig.add_trace(go.Bar(x=df["ts"],y=df["v"],marker_color=vcols,name="成交量",opacity=0.55),row=2,col=1)
    x_end=candles[-1].timestamp_ms+interval_to_millis(interval)
    span=max(x_end-candles[0].timestamp_ms,interval_to_millis(interval)*20)
    for bar in heat_bars:
        it=bar["intensity"]; x0=int(x_end-span*(0.28+0.72*it)); ch=palette_color(str(bar["side"]),it)
        fig.add_shape(type="rect",x0=pd.to_datetime(x0,unit="ms"),x1=pd.to_datetime(x_end,unit="ms"),
            y0=bar["price_low"],y1=bar["price_high"],fillcolor=rgba_from_hex(ch,0.24+0.34*it),
            line_width=0,layer="below",row=1,col=1)
    lp=snapshot.last_price or (candles[-1].close if candles else None)
    if lp: fig.add_hline(y=lp,line_color="#f8d35e",line_dash="dot",line_width=1,row=1,col=1)
    if snapshot.mark_price: fig.add_hline(y=snapshot.mark_price,line_color="#8fd3ff",line_dash="dash",line_width=1,row=1,col=1)
    if snapshot.spot_price: fig.add_hline(y=snapshot.spot_price,line_color="#a8ff78",line_dash="dashdot",line_width=1.2,row=1,col=1)
    fig.update_layout(height=760,margin=dict(l=12,r=12,t=62,b=12),paper_bgcolor="rgba(14,22,35,0.56)",
        plot_bgcolor="rgba(255,255,255,0.045)",font=dict(color="#f6f9ff",family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="Price Structure & Liquidity  ·  价格结构与流动性",x=0.02,y=0.98,xanchor="left",font=dict(size=19,color="#f8fbff")),
        xaxis_rangeslider_visible=False,transition=dict(duration=280,easing="cubic-in-out"))
    fig.update_xaxes(showgrid=False,zeroline=False)
    fig.update_yaxes(showgrid=True,gridcolor="rgba(255,255,255,0.08)",side="right")
    return fig

def build_snapshot_frame(snapshots):
    rows = []
    for s in snapshots:
        sp_bps = s.spot_perp_spread_bps if hasattr(s, 'spot_perp_spread_bps') else None
        rows.append({"交易所":s.exchange,"合约":s.symbol,
            "最新价":s.last_price,"现货价":getattr(s,'spot_price',None),
            "标记价":s.mark_price,"溢价%":s.premium_pct,
            "现货-合约(bps)":sp_bps,
            "持仓量":s.open_interest,"持仓金额":s.open_interest_notional,
            "费率bps":s.funding_bps,"24h成交额":s.volume_24h_notional,
            "状态":CARD_STATUS.get(s.status,s.status),"异常":s.error})
    return pd.DataFrame(rows)

def build_oi_figure(points):
    fig=go.Figure()
    if not points:
        fig.add_annotation(text="等待OI历史",showarrow=False,x=0.5,y=0.5,xref="paper",yref="paper")
        fig.update_layout(height=260,margin=dict(l=12,r=12,t=24,b=12)); return fig,"OI历史"
    df=pd.DataFrame({"ts":pd.to_datetime([p.timestamp_ms for p in points],unit="ms"),
        "oi":[p.open_interest for p in points],"oin":[p.open_interest_notional for p in points]})
    use_n=df["oin"].notna().sum()>=max(3,len(df)//4); col="oin" if use_n else "oi"
    label="持仓金额 (OI Notional)" if use_n else "持仓量 (Open Interest)"
    df=df.dropna(subset=[col])
    if not df.empty:
        fig.add_trace(go.Scatter(x=df["ts"],y=df[col],mode="lines",
            line=dict(color="#62c2ff",width=2.2),fill="tozeroy",fillcolor="rgba(98,194,255,0.16)",name=label))
    fig.update_layout(height=260,margin=dict(l=12,r=12,t=54,b=10),paper_bgcolor="rgba(14,22,35,0.56)",
        plot_bgcolor="rgba(255,255,255,0.045)",font=dict(color="#f6f9ff",family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text=label,x=0.03,y=0.98,xanchor="left",font=dict(size=15,color="#f3f8ff")),showlegend=False)
    fig.update_xaxes(showgrid=False); fig.update_yaxes(showgrid=True,gridcolor="rgba(255,255,255,0.08)",tickformat=".2s")
    return fig, label

def build_heat_frame(heat_bars):
    return pd.DataFrame([{"方向":"下方买盘墙" if b["side"]=="bid" else "上方卖盘墙",
        "价格区间":f"{b['price_low']:,.2f} - {b['price_high']:,.2f}",
        "挂单量":b["size"],"热度":b["intensity"],"离现价%":b["distance_pct"]} for b in heat_bars])

# ── Spot-Perp Spread figure ───────────────────────────────────────────────────
def build_spot_perp_realtime_figure(spread_hist_by_exchange: Dict) -> go.Figure:
    """三所实时现货-合约价差折线图"""
    fig = go.Figure()
    colors = {"binance":"#f0b90b","bybit":"#e6a817","okx":"#1267fe"}
    names  = {"binance":"Binance","bybit":"Bybit","okx":"OKX"}
    has_data = False
    for ek, pts in spread_hist_by_exchange.items():
        if not pts: continue
        has_data = True
        ts  = [pd.to_datetime(p.timestamp_ms, unit="ms") for p in pts]
        bps = [p.spread_bps for p in pts]
        fig.add_trace(go.Scatter(x=ts, y=bps, mode="lines",
            name=names.get(ek, ek),
            line=dict(color=colors.get(ek,"#aaa"), width=1.8)))
    if not has_data:
        fig.add_annotation(text="等待现货WS连接…", showarrow=False, x=0.5, y=0.5,
                           xref="paper", yref="paper", font=dict(color="#aaa"))
    fig.add_hline(y=0, line_color="rgba(255,255,255,0.3)", line_dash="dot", line_width=1)
    fig.update_layout(
        height=280, margin=dict(l=12,r=12,t=52,b=10),
        paper_bgcolor="rgba(14,22,35,0.56)", plot_bgcolor="rgba(255,255,255,0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="现货-合约实时价差 Spot-Perp Spread (bps)  · 正值=合约溢价",
                   x=0.02, y=0.97, xanchor="left", font=dict(size=14, color="#f3f8ff")),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridcolor="rgba(255,255,255,0.08)", zeroline=True,
                     zerolinecolor="rgba(255,255,255,0.2)")
    return fig

# ── OI + Long/Short + Ratio visualization ────────────────────────────────────
def build_binance_oi_perp_figure(oi_points, long_short_data, taker_data) -> go.Figure:
    """Binance 合约: OI变化 + 多空比 + Taker买卖 四合一图"""
    rows = 3; row_heights = [0.45, 0.3, 0.25]
    fig = make_subplots(rows=rows, cols=1, shared_xaxes=True,
                        vertical_spacing=0.04, row_heights=row_heights,
                        subplot_titles=("持仓量 OI (Notional)", "多空账户比 L/S Account Ratio", "Taker 买卖量比"))
    # Row 1: OI
    if oi_points:
        ts = [pd.to_datetime(p.timestamp_ms, unit="ms") for p in oi_points]
        oi_vals = [p.open_interest_notional or p.open_interest or 0 for p in oi_points]
        # Color bars by change direction
        colors = []
        for i, v in enumerate(oi_vals):
            if i == 0: colors.append("#62c2ff")
            else: colors.append("#1dc796" if v >= oi_vals[i-1] else "#ff6868")
        fig.add_trace(go.Bar(x=ts, y=oi_vals, marker_color=colors,
                             name="OI", opacity=0.85), row=1, col=1)
        fig.add_trace(go.Scatter(x=ts, y=oi_vals, mode="lines",
                                 line=dict(color="#62c2ff", width=1.5),
                                 name="OI趋势", showlegend=False), row=1, col=1)
    # Row 2: Long/Short ratio
    if long_short_data:
        ts2 = [pd.to_datetime(d["timestamp_ms"], unit="ms") for d in long_short_data]
        global_l = [d.get("global_long_pct") for d in long_short_data]
        global_s = [d.get("global_short_pct") for d in long_short_data]
        top_l    = [d.get("top_long_pct") for d in long_short_data]
        top_s    = [d.get("top_short_pct") for d in long_short_data]
        fig.add_trace(go.Scatter(x=ts2, y=global_l, mode="lines", name="全市场多头%",
            line=dict(color="#1dc796", width=2)), row=2, col=1)
        fig.add_trace(go.Scatter(x=ts2, y=global_s, mode="lines", name="全市场空头%",
            line=dict(color="#ff6868", width=2)), row=2, col=1)
        fig.add_trace(go.Scatter(x=ts2, y=top_l, mode="lines", name="大户多头%",
            line=dict(color="#a8ff78", width=1.5, dash="dash")), row=2, col=1)
        fig.add_trace(go.Scatter(x=ts2, y=top_s, mode="lines", name="大户空头%",
            line=dict(color="#ff9a9a", width=1.5, dash="dash")), row=2, col=1)
        fig.add_hline(y=50, line_color="rgba(255,255,255,0.25)", line_dash="dot", row=2, col=1)
    # Row 3: Taker ratio
    if taker_data:
        ts3 = [pd.to_datetime(d["timestamp_ms"], unit="ms") for d in taker_data]
        buy  = [d.get("buy_vol") or 0 for d in taker_data]
        sell = [d.get("sell_vol") or 0 for d in taker_data]
        fig.add_trace(go.Bar(x=ts3, y=buy,  name="主动买量", marker_color="#1dc796", opacity=0.8), row=3, col=1)
        fig.add_trace(go.Bar(x=ts3, y=[-s for s in sell], name="主动卖量", marker_color="#ff6868", opacity=0.8), row=3, col=1)
    fig.update_layout(
        height=580, margin=dict(l=12,r=12,t=60,b=12),
        paper_bgcolor="rgba(14,22,35,0.56)", plot_bgcolor="rgba(255,255,255,0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        barmode="overlay",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridcolor="rgba(255,255,255,0.07)", tickformat=".2s")
    return fig

def build_ls_gauge_html(long_pct, short_pct, label="全市场", top_long=None, top_short=None) -> str:
    """HTML 多空拥挤度仪表盘"""
    if long_pct is None: long_pct = 50.0
    if short_pct is None: short_pct = 100 - long_pct
    lp = max(0, min(100, long_pct))
    sp = max(0, min(100, short_pct))
    # Color based on ratio
    if lp > 65: bg_l, bg_s = "#1dc796", "#ff6868"
    elif lp < 35: bg_l, bg_s = "#ff6868", "#1dc796"
    else: bg_l, bg_s = "#62c2ff", "#ffa94d"
    top_row = ""
    if top_long is not None:
        tl = max(0, min(100, top_long))
        ts_ = max(0, min(100, top_short or 100-tl))
        top_row = f"""
        <div style="margin-top:8px;font-size:0.75rem;color:#aac">大户持仓</div>
        <div style="display:flex;width:100%;height:8px;border-radius:4px;overflow:hidden;margin-top:3px;">
          <div style="width:{tl:.1f}%;background:#a8ff78;"></div>
          <div style="width:{ts_:.1f}%;background:#ff9a9a;"></div>
        </div>
        <div style="display:flex;justify-content:space-between;font-size:0.72rem;margin-top:2px;">
          <span style="color:#a8ff78">{tl:.1f}%</span><span style="color:#ff9a9a">{ts_:.1f}%</span>
        </div>"""
    return f"""
    <div style="padding:12px 14px;border-radius:16px;border:1px solid rgba(255,255,255,0.13);
        background:rgba(255,255,255,0.06);backdrop-filter:blur(20px);margin-bottom:8px;">
      <div style="font-size:0.78rem;color:#bfd5f2;text-transform:uppercase;letter-spacing:0.1em;margin-bottom:6px;">{label}</div>
      <div style="display:flex;width:100%;height:14px;border-radius:7px;overflow:hidden;">
        <div style="width:{lp:.1f}%;background:{bg_l};transition:width 0.4s;"></div>
        <div style="width:{sp:.1f}%;background:{bg_s};transition:width 0.4s;"></div>
      </div>
      <div style="display:flex;justify-content:space-between;margin-top:4px;font-size:0.85rem;font-weight:600;">
        <span style="color:{bg_l}">多头 {lp:.1f}%</span>
        <span style="color:{bg_s}">空头 {sp:.1f}%</span>
      </div>
      {top_row}
    </div>"""

def build_oi_change_visual(oi_pts, window=20) -> str:
    """OI变化可视化 HTML"""
    if not oi_pts or len(oi_pts) < 2: return "<div style='color:#aaa;font-size:0.85rem;'>等待OI数据…</div>"
    recent = oi_pts[-min(window, len(oi_pts)):]
    vals   = [p.open_interest_notional or p.open_interest or 0 for p in recent]
    if not vals or vals[0] == 0: return ""
    total_change = vals[-1] - vals[0]
    pct_change   = total_change / vals[0] * 100 if vals[0] else 0
    max_v = max(vals); min_v = min(vals); rng = max_v - min_v
    arrow = "▲" if total_change > 0 else "▼"
    color = "#1dc796" if total_change > 0 else "#ff6868"
    bars_html = ""
    for v in vals:
        h = int((v - min_v) / rng * 28) if rng > 0 else 14
        h = max(4, h)
        c = "#1dc796" if v >= vals[0] else "#ff6868"
        bars_html += f"<div style='width:4px;height:{h}px;background:{c};border-radius:2px;display:inline-block;margin:0 1px;vertical-align:bottom;'></div>"
    return f"""
    <div style="padding:10px 14px;border-radius:14px;border:1px solid rgba(255,255,255,0.12);
        background:rgba(255,255,255,0.05);margin-bottom:6px;">
      <div style="font-size:0.72rem;color:#bfd5f2;text-transform:uppercase;letter-spacing:0.1em;">OI变化 近{len(recent)}采样</div>
      <div style="margin:6px 0 4px;display:flex;align-items:flex-end;gap:1px;">{bars_html}</div>
      <div style="font-size:1.0rem;font-weight:700;color:{color};">{arrow} {pct_change:+.2f}%
        <span style="font-size:0.8rem;font-weight:400;color:#aac;margin-left:8px;">{fc(vals[-1])}</span>
      </div>
    </div>"""

# ── Liquidation split view (real vs inferred) ─────────────────────────────────
def build_split_liq_tables(liq_events, limit_each=10):
    """上方10条多头爆仓 / 下方10条空头爆仓，左右分栏"""
    long_evts  = sorted([e for e in liq_events if e.side == "long"],
                        key=lambda x: x.timestamp_ms, reverse=True)[:limit_each]
    short_evts = sorted([e for e in liq_events if e.side == "short"],
                        key=lambda x: x.timestamp_ms, reverse=True)[:limit_each]
    def to_df(evts, label):
        if not evts: return pd.DataFrame()
        return pd.DataFrame([{
            "时间": pd.to_datetime(e.timestamp_ms, unit="ms").strftime("%H:%M:%S"),
            "方向": f"{'多头爆↓' if e.side=='long' else '空头爆↑'}",
            "价格": e.price, "数量": e.size,
            "金额$": e.notional, "来源": e.source} for e in evts])
    return to_df(long_evts, "多头"), to_df(short_evts, "空头")

def build_liq_reality_split_figure(liq_events) -> go.Figure:
    """已发生爆仓真值 vs 推断爆仓区 — 分开标注"""
    fig = go.Figure()
    real_long  = [e for e in liq_events if e.side == "long"  and e.source in ("ws","rest")]
    real_short = [e for e in liq_events if e.side == "short" and e.source in ("ws","rest")]
    if real_long:
        fig.add_trace(go.Scatter(
            x=[pd.to_datetime(e.timestamp_ms, unit="ms") for e in real_long],
            y=[e.price for e in real_long],
            mode="markers", name="✅ 多头已爆 (真实)",
            marker=dict(color="#ff6868", size=[max(6, min(22, (e.notional or 1)/5000)) for e in real_long],
                        symbol="triangle-down", opacity=0.9)))
    if real_short:
        fig.add_trace(go.Scatter(
            x=[pd.to_datetime(e.timestamp_ms, unit="ms") for e in real_short],
            y=[e.price for e in real_short],
            mode="markers", name="✅ 空头已爆 (真实)",
            marker=dict(color="#1dc796", size=[max(6, min(22, (e.notional or 1)/5000)) for e in real_short],
                        symbol="triangle-up", opacity=0.9)))
    if not real_long and not real_short:
        fig.add_annotation(text="暂无爆仓数据", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
    fig.update_layout(
        height=320, margin=dict(l=12,r=12,t=52,b=10),
        paper_bgcolor="rgba(14,22,35,0.56)", plot_bgcolor="rgba(255,255,255,0.045)",
        font=dict(color="#f6f9ff"), title=dict(
            text="✅ 已发生爆仓真值 (Real Liquidations)  — 来自WS/REST真实数据",
            x=0.02, y=0.97, xanchor="left", font=dict(size=13, color="#f3f8ff")),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridcolor="rgba(255,255,255,0.08)", tickformat=".2f")
    return fig

# ── Spot-Perp Alert rendering ─────────────────────────────────────────────────
def render_spot_perp_alerts(alerts, max_show=20):
    if not alerts:
        st.info("暂无现货-合约乖离告警。等待现货WS连接后自动检测。")
        return
    recent = sorted(alerts, key=lambda a: a.timestamp_ms, reverse=True)[:max_show]
    for a in recent:
        label_info = SPOT_PERP_ALERT_LABELS.get(a.alert_type, ("📢 告警", "#aaa"))
        label, color = label_info
        sev_class = f"sp-alert-{a.severity}"
        ts = pd.to_datetime(a.timestamp_ms, unit="ms").strftime("%H:%M:%S")
        extra = ""
        if a.spread_bps is not None: extra += f" | 价差 {a.spread_bps:+.1f}bps"
        if a.oi_change_pct is not None: extra += f" | OI变化 {a.oi_change_pct:+.2f}%"
        st.markdown(f"""
        <div class='glass-section {sev_class}' style='margin:4px 0;padding:10px 14px;'>
          <span style='color:{color};font-weight:700;font-size:0.9rem;'>{label}</span>
          <span style='color:#aac;font-size:0.78rem;margin-left:8px;'>{ts}</span>
          <span style='color:#aac;font-size:0.78rem;margin-left:6px;'>{extra}</span><br/>
          <span style='color:#e8f0ff;font-size:0.85rem;'>{a.message}</span>
        </div>""", unsafe_allow_html=True)


# ── Sidebar ────────────────────────────────────────────────────────────────────
preset_coin = st.sidebar.selectbox("常用币种", POPULAR_COINS, index=0)
custom_coin = st.sidebar.text_input("自定义币种", value="", placeholder="如 PEPE / TAO / FARTCOIN")
base_coin = custom_coin.strip().upper() if custom_coin.strip() else preset_coin
base_defaults = default_symbols(base_coin)
if st.session_state.get("symbol_base_coin") != base_coin:
    for k in EXCHANGE_ORDER: st.session_state[f"symbol_{k}"] = base_defaults[k]
    st.session_state["symbol_base_coin"] = base_coin

with st.sidebar:
    st.header("终端参数")
    st.caption(f"当前基础币种: **{base_coin}**")
    selected_exchange = st.selectbox("主图交易所", list(EXCHANGE_ORDER), format_func=lambda k: EXCHANGE_TITLES[k])
    interval       = st.selectbox("K线周期", list(SUPPORTED_INTERVALS), index=2)
    candle_limit   = st.slider("K线数量", 120, 480, 240, 30)
    depth_limit    = st.slider("盘口深度", 50, 400, 160, 10)
    heat_window    = st.slider("挂单热力窗口 (%)", 2.0, 12.0, 5.0, 0.5)
    heat_buckets   = st.slider("挂单热力分桶", 10, 36, 22, 2)
    heat_bars_side = st.slider("每侧热力条数", 4, 16, 8, 1)
    risk_window    = st.slider("风险热图窗口 (%)", 4.0, 18.0, 8.0, 0.5)
    risk_buckets   = st.slider("风险热图分桶", 16, 40, 28, 2)
    liq_limit      = st.slider("爆仓回看条数", 20, 160, 60, 10)
    liq_window_min = st.slider("爆仓统计分钟", 15, 240, 60, 15)
    mbo_rows       = st.slider("MBO档位", 8, 24, 14, 2)
    trade_limit    = st.slider("成交流条数 (CVD)", 100, 2000, 500, 100)
    ratio_limit    = st.slider("多空比历史条数", 20, 200, 80, 10)
    refresh_secs   = st.slider("界面刷新秒数", 1, 10, 2, 1)
    sample_secs    = st.slider("持仓采样秒数", 5, 60, 15, 5)
    req_timeout    = st.slider("请求超时秒数", 5, 20, 10, 1)
    st.markdown("---")
    st.subheader("📡 多币种轮巡")
    watchlist_input = st.text_input("监控币种（逗号分隔）", value=",".join(WATCHLIST_DEFAULT))
    watchlist_coins = [c.strip().upper() for c in watchlist_input.split(",") if c.strip()]
    st.markdown("---")
    st.subheader("合约映射")
    bybit_sym  = st.text_input("Bybit 合约",       key="symbol_bybit")
    binance_sym= st.text_input("Binance 合约",     key="symbol_binance")
    okx_sym    = st.text_input("OKX 合约",         key="symbol_okx")
    hyper_sym  = st.text_input("Hyperliquid 币种", key="symbol_hyperliquid")
    col1, col2 = st.columns(2)
    restore = col1.button("恢复默认")
    restart = col2.button("重连流")
    if st.button("清空缓存"): st.cache_data.clear()

if restore:
    for k in EXCHANGE_ORDER: st.session_state[f"symbol_{k}"] = base_defaults[k]
    st.rerun()

symbol_map = {"bybit":bybit_sym.strip().upper(),"binance":binance_sym.strip().upper(),
              "okx":okx_sym.strip().upper(),"hyperliquid":hyper_sym.strip().upper()}
service = resolve_service(symbol_map, req_timeout, sample_secs, restart)

# ── Hero ───────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="hero-shell">
<div class="hero-kicker">Liquid Glass Flow Desk · v4</div>
<div class="hero-title">{base_coin} 多交易所流动性终端</div>
<div class="hero-sub">三所Spot WS + 合约WS · 现货-合约实时价差 · Spot-Perp Lead/Lag告警 · OI可视化 · 多空拥挤度 · 爆仓真值/推断分栏 · CVD · 冰山单 · 预警系统</div>
<div class="helper-bar">
<div class="helper-pill">主图 {EXCHANGE_TITLES[selected_exchange]}</div>
<div class="helper-pill">周期 {interval}</div>
<div class="helper-pill">刷新 {refresh_secs}s</div>
<div class="helper-pill">爆仓窗口 {liq_window_min}m</div>
<div class="helper-pill">监控 {len(watchlist_coins)} 币</div>
</div></div>""", unsafe_allow_html=True)


# ── Main fragment ──────────────────────────────────────────────────────────────
@st.fragment(run_every=refresh_secs)
def render_terminal():
    snapshots    = service.current_snapshots()
    ok_snaps     = [s for s in snapshots if s.status=="ok"]
    status_text  = " · ".join(status_caption(s) for s in snapshots)
    st.markdown(f"<div class='status-strip'>{status_text}</div>", unsafe_allow_html=True)

    alert_events     = service.get_alert_events()
    sp_alerts        = service.get_spot_perp_alerts()
    spread_hist_all  = service.get_all_spread_history()

    # Top alert badges
    all_badge_events = list(alert_events)[-3:] + list(sp_alerts)[-3:]
    if all_badge_events:
        html = ""
        for e in list(alert_events)[-3:]:
            html += f"<span class='alert-badge'>🔔 {e.name}</span> "
        for a in list(sp_alerts)[-3:]:
            lbl = SPOT_PERP_ALERT_LABELS.get(a.alert_type, ("📢","#aaa"))[0]
            sev_color = "#ff6868" if a.severity=="high" else "#ffa94d"
            html += f"<span class='alert-badge' style='border-color:rgba(255,100,50,0.5);color:{sev_color};'>{lbl} {a.exchange}</span> "
        st.markdown(f"<div style='margin-bottom:0.7rem;'>{html}</div>", unsafe_allow_html=True)

    if not ok_snaps: st.error("当前没有交易所返回可用数据，请检查合约名或网络。"); return

    snap_by_key  = dict(zip(EXCHANGE_ORDER, snapshots))
    sel_snap     = snap_by_key[selected_exchange]
    sel_sym      = symbol_map[selected_exchange]

    # ── Data loading ────────────────────────────────────────────────────────────
    candles   = load_candles(selected_exchange, sel_sym, interval, candle_limit, req_timeout)
    local_book= service.get_local_book(selected_exchange)
    if local_book.is_ready:
        orderbook = service.get_local_book_levels(selected_exchange, depth_limit)
    else:
        orderbook = load_orderbook(selected_exchange, sel_sym, depth_limit, req_timeout)

    backfill_oi = load_oi_backfill(selected_exchange, sel_sym, interval, max(60,candle_limit//2), req_timeout)
    merged_oi   = merge_oi_points(backfill_oi, service.get_oi_history(selected_exchange))
    ref_price   = sel_snap.last_price or sel_snap.mark_price or (candles[-1].close if candles else None)
    heat_bars   = aggregate_heat_bars(orderbook, ref_price, heat_window, heat_buckets, heat_bars_side)
    book_sum    = summarize_orderbook(orderbook, ref_price)

    sess_liqs   = service.get_liquidation_history(selected_exchange)
    rest_liqs   = load_liquidations(selected_exchange, sel_sym, liq_limit, req_timeout)
    liq_events  = merge_liquidation_events(rest_liqs, sess_liqs)
    liq_metrics = build_liquidation_metrics(liq_events, sel_snap.timestamp_ms or int(time.time()*1000), liq_window_min)
    liq_hf      = build_probability_heatmap_frame(candles, orderbook, sel_snap, "liquidation", ref_price, risk_window, risk_buckets)
    tp_hf       = build_probability_heatmap_frame(candles, orderbook, sel_snap, "tp",          ref_price, risk_window, risk_buckets)
    stop_hf     = build_probability_heatmap_frame(candles, orderbook, sel_snap, "stop",        ref_price, risk_window, risk_buckets)
    mbo_frame   = build_mbo_profile_frame(orderbook, ref_price, mbo_rows)

    # CVD
    ws_trades   = service.get_trade_history(selected_exchange)
    rest_trades = load_trades(selected_exchange, sel_sym, min(trade_limit,500), req_timeout)
    all_trades  = sorted(set((t.timestamp_ms,t.price,t.size) for t in ws_trades+rest_trades), key=lambda x: x[0])
    all_trades_obj = {(t.timestamp_ms,t.price,t.size): t for t in ws_trades+rest_trades}
    unique_trades = [all_trades_obj[k] for k in all_trades]
    ws_cvd = service.get_cvd_history(selected_exchange)
    if len(ws_cvd) >= 20:
        cvd_points = ws_cvd
    elif any(c.taker_buy_volume is not None for c in candles):
        cvd_points = build_cvd_from_candles(candles)
    elif unique_trades:
        cvd_points = build_cvd_from_trades(unique_trades[-trade_limit:], bin_seconds=max(30, interval_to_millis(interval)//2000))
    else:
        cvd_points = []

    oi_delta_pts  = build_oi_delta_points(merged_oi, candles)
    oi_delta_summ = build_oi_delta_summary(oi_delta_pts, lookback=20)

    # Ratios
    top_trader      = load_top_trader("binance", symbol_map["binance"], interval, ratio_limit, req_timeout)
    global_r        = load_global_ratio("binance", symbol_map["binance"], interval, ratio_limit, req_timeout)
    bybit_ratio_raw = load_top_trader("bybit", symbol_map["bybit"], interval, ratio_limit, req_timeout)
    ls_count_data   = load_ls_count(symbol_map["binance"], interval, ratio_limit, req_timeout)
    taker_ratio_data= load_taker_ratio(symbol_map["binance"], interval, ratio_limit, req_timeout)

    # Spot data
    spot_prices = {}; spot_volumes = {}
    for ek in ("bybit","binance","okx"):
        # Prefer WS spot price from service
        sp_ws = service._spot_price.get(ek)
        if sp_ws:
            spot_prices[EXCHANGE_TITLES[ek]] = sp_ws
            sv_ws = service._spot_vol24h.get(ek)
            if sv_ws: spot_volumes[EXCHANGE_TITLES[ek]] = sv_ws
        else:
            sp = load_spot_ticker(ek, base_coin, req_timeout)
            if sp: spot_prices[EXCHANGE_TITLES[ek]] = sp[0]; spot_volumes[EXCHANGE_TITLES[ek]] = sp[1]

    futures_oi_list = load_futures_oi_list("binance", base_coin, req_timeout)
    if not futures_oi_list:
        futures_oi_list = load_futures_oi_list("bybit", base_coin, req_timeout)

    all_liq_by_exch = {}
    for ek in EXCHANGE_ORDER:
        r_liqs = load_liquidations(ek, symbol_map[ek], liq_limit, req_timeout)
        w_liqs = service.get_liquidation_history(ek)
        all_liq_by_exch[ek] = merge_liquidation_events(r_liqs, w_liqs)

    iceberg_alerts = service.get_iceberg_alerts()
    liq_gaps       = service.get_liquidity_gaps()

    # NEW: upgrade data
    confirmed_alerts  = service.get_confirmed_alerts()
    alert_timeline    = service.get_alert_timeline()
    liq_clusters_v2   = service.get_liq_clusters_v2()
    composite_sigs_by_ex = {ek: service.get_composite_signals(ek) for ek in EXCHANGE_ORDER}
    ob_quality_sel    = service.get_ob_quality_history(selected_exchange)
    ob_delta_sel      = service.get_ob_delta_history(selected_exchange)
    fake_walls_sel    = service.get_fake_walls(selected_exchange)
    absorb_sel        = service.get_absorption_events(selected_exchange)
    recorded_frames   = service.get_recorded_frames()
    latest_comp_sig   = composite_sigs_by_ex.get(selected_exchange, [])
    latest_comp_sig   = latest_comp_sig[-1] if latest_comp_sig else None

    mref  = pd.Series([s.mark_price for s in ok_snaps if s.mark_price]).median()
    total_oi = sum(s.open_interest_notional or 0 for s in ok_snaps)
    avg_fund = pd.Series([s.funding_bps for s in ok_snaps if s.funding_bps is not None]).mean() if any(s.funding_bps for s in ok_snaps) else None
    sentiment = build_market_sentiment_summary(ok_snaps, oi_delta_summ, liq_metrics, cvd_points, top_trader)

    # ── Top metrics ─────────────────────────────────────────────────────────────
    c = st.columns(6)
    c[0].metric("在线交易所", str(len(ok_snaps)))
    c[1].metric("市场参考价", fp(mref))
    c[2].metric("持仓总金额", fc(total_oi))
    c[3].metric("主图费率", frate(sel_snap.funding_rate))
    c[4].metric("OI四象限", oi_delta_summ.get("dominant_cn","-"))
    c[5].metric("CVD累积", fc(cvd_points[-1].cvd if cvd_points else None))
    c2 = st.columns(5)
    c2[0].metric("买盘挂单额", fc(book_sum.get("bid_notional")))
    c2[1].metric("卖盘挂单额", fc(book_sum.get("ask_notional")))
    c2[2].metric("盘口失衡", fpct(book_sum.get("imbalance_pct")))
    c2[3].metric("价差 Spread", fbps(book_sum.get("spread_bps") or (local_book.spread_bps() if local_book.is_ready else None)))
    c2[4].metric("OI速率/min", fc(oi_delta_summ.get("avg_velocity")))

    if sentiment:
        html = "".join(f"<span class='signal-card'>📊 <b>{k}</b>：{v}</span>" for k,v in sentiment.items())
        st.markdown(f"<div style='margin:0.3rem 0 0.7rem;'>{html}</div>", unsafe_allow_html=True)

    # Exchange cards — Binance first
    cards = st.columns(len(EXCHANGE_ORDER_UI))
    for col, ek in zip(cards, EXCHANGE_ORDER_UI):
        s = snap_by_key[ek]
        sp_bps = s.spot_perp_spread_bps if hasattr(s,'spot_perp_spread_bps') else None
        delta_str = f"OI {fc(s.open_interest_notional)}"
        if sp_bps is not None: delta_str += f" | 现货差 {sp_bps:+.1f}bps"
        col.metric(s.exchange, fp(s.last_price), delta=delta_str)

    # ── Tabs ───────────────────────────────────────────────────────────────────
    tabs = st.tabs(["📈 专业终端","💧 CVD主动买卖","🔲 本地WS订单簿",
                    "👥 OI四象限+速率","📊 多空比矩阵",
                    "🔗 Spot-Perp 现货-合约","📐 Basis+期限结构","💥 爆仓瀑布",
                    "🔍 冰山单+流动性缺口","🧠 合成信号引擎",
                    "📋 盘口质量+假挂单","⏰ 告警时间线","📼 回放复盘",
                    "🌐 全市场对比","📡 多币种轮巡","🔔 预警系统","🔧 调试"])

    # ══════ TAB 0: 专业终端 ══════
    with tabs[0]:
        render_section(f"{sel_snap.exchange} {sel_sym}", "K线 · 成交量 · OI曲线 · 盘口热力区  |  绿线=现货参考价")
        left, right = st.columns([3.1,1.35], gap="large")
        with left:
            st.plotly_chart(build_terminal_chart(candles, heat_bars, sel_snap, interval), key="pc_main", use_container_width=True)
        with right:
            # OI change visual
            oi_html = build_oi_change_visual(service.get_oi_history(selected_exchange))
            if oi_html: st.markdown(oi_html, unsafe_allow_html=True)
            oi_fig, oi_lbl = build_oi_figure(merged_oi)
            st.plotly_chart(oi_fig, key="pc_oi", use_container_width=True)
            st.caption(oi_lbl)
            hf = build_heat_frame(heat_bars)
            if hf.empty: st.info("盘口深度不足，暂无热力条。")
            else:
                st.dataframe(hf, use_container_width=True, hide_index=True,
                    column_config={"挂单量":st.column_config.NumberColumn(format="%.2f"),
                        "热度":st.column_config.ProgressColumn(format="%.2f",min_value=0,max_value=1),
                        "离现价%":st.column_config.NumberColumn(format="%.2f%%")})
        render_section("资金费率对比  ·  Funding Rate", "正=多头拥挤付费；负=空头拥挤付费。")
        st.plotly_chart(build_funding_comparison_figure(ok_snaps), key="pc_funding_main", use_container_width=True)
        render_section("挂单量 + 已发生爆仓  ·  Liquidity + Executed Liquidations")
        fc4 = st.columns(4)
        fc4[0].metric(f"近{liq_window_min}m爆仓额", fc(liq_metrics.get("notional")))
        fc4[1].metric(f"近{liq_window_min}m爆仓单数", str(liq_metrics.get("count",0)))
        fc4[2].metric("主导方向", liq_metrics.get("dominant") or "-")
        fc4[3].metric("四所平均费率", fbps(avg_fund))
        ll, lr = st.columns([2.1,1.35], gap="large")
        with ll: st.plotly_chart(build_liquidation_figure(liq_events), key="pc_liq_main", use_container_width=True)
        with lr:
            lf = build_liquidation_frame(liq_events, limit=24)
            if lf.empty: st.info("暂无爆仓事件。")
            else: st.dataframe(lf, use_container_width=True, hide_index=True,
                    column_config={"价格":st.column_config.NumberColumn(format="%.2f"),
                        "数量":st.column_config.NumberColumn(format="%.4f"),
                        "名义金额":st.column_config.NumberColumn(format="%.2f")})
        render_section("Risk Maps  ·  风险区推断", "基于Orderbook+Candles+OI+Funding推断。⚠️ 推断区与真实爆仓分开展示。")
        hcols = st.columns(3, gap="large")
        specs = [(hcols[0],liq_hf,"⚠️ 推断爆仓区 (Inferred)",LIQUIDATION_COLORSCALE,"数据不足"),
                 (hcols[1],tp_hf, "推断止盈区 (Est. TP)",    TP_COLORSCALE,"数据不足"),
                 (hcols[2],stop_hf,"推断止损区 (Est. Stop)", STOP_COLORSCALE,"数据不足")]
        for _hi,(col,fdata,title,cs,etxt) in enumerate(specs):
            with col:
                st.plotly_chart(build_heatmap_figure(fdata,title,ref_price,cs,etxt), key=f"pc_heat_{_hi}", use_container_width=True)
                st.dataframe(build_heat_zone_frame(fdata), use_container_width=True, hide_index=True,
                    column_config={"热度":st.column_config.ProgressColumn(format="%.2f",min_value=0,max_value=1)})
        render_section("MBO Profile  ·  盘口队列画像", "吸收分数高=该价位承接力强。")
        ml, mr = st.columns([2.1,1.35], gap="large")
        with ml: st.plotly_chart(build_mbo_figure(mbo_frame, ref_price), key="pc_mbo", use_container_width=True)
        with mr:
            if mbo_frame.empty: st.info("盘口深度不足。")
            else: st.dataframe(mbo_frame[["方向","价格","挂单量","名义金额","盘口占比","吸收分数"]],
                use_container_width=True, hide_index=True,
                column_config={"价格":st.column_config.NumberColumn(format="%.2f"),
                    "盘口占比":st.column_config.ProgressColumn(format="%.2f",min_value=0,max_value=1),
                    "吸收分数":st.column_config.ProgressColumn(format="%.2f",min_value=0,max_value=1)})

    # ══════ TAB 1: CVD ══════
    with tabs[1]:
        render_section("CVD 累积成交量差  ·  Cumulative Volume Delta",
            "CVD正值/上升=主动买方Taker Buy主导；负值=卖方主导。Binance从K线taker字段获取最准确。", kicker="Flow")
        st.plotly_chart(build_cvd_figure(cvd_points, f"{base_coin} CVD"), key="pc_cvd", use_container_width=True)
        if unique_trades:
            tc1, tc2 = st.columns([1.6,1])
            with tc1:
                st.caption(f"实时成交流（最新{min(50,len(unique_trades))}笔）| 绿=主动买▲ / 红=主动卖▼")
                trows=[{"时间":pd.to_datetime(t.timestamp_ms,unit="ms").strftime("%H:%M:%S"),
                    "方向":"主动买▲" if t.side=="buy" else "主动卖▼",
                    "价格":t.price,"数量":t.size,"名义金额":t.notional,"交易所":t.exchange}
                    for t in sorted(unique_trades,key=lambda x:x.timestamp_ms,reverse=True)[:50]]
                st.dataframe(pd.DataFrame(trows), use_container_width=True, hide_index=True,
                    column_config={"价格":st.column_config.NumberColumn(format="%.2f"),
                        "数量":st.column_config.NumberColumn(format="%.4f"),
                        "名义金额":st.column_config.NumberColumn(format="%.2f")})
            with tc2:
                recent100 = unique_trades[-100:]
                buy_v  = sum(t.notional for t in recent100 if t.side=="buy")
                sell_v = sum(t.notional for t in recent100 if t.side=="sell")
                tot    = buy_v + sell_v
                st.metric("近100笔主动买量", fc(buy_v))
                st.metric("近100笔主动卖量", fc(sell_v))
                st.metric("买卖比 Buy/Sell Ratio", f"{buy_v/max(tot,1)*100:.1f}% 买" if tot>0 else "-")
                st.metric("WebSocket实时成交数", str(len(ws_trades)))
        else:
            st.info("等待实时成交流（WebSocket）或REST成交历史…")

    # ══════ TAB 2: 本地WS订单簿 ══════
    with tabs[2]:
        render_section("本地实时订单簿  ·  Local WebSocket Orderbook",
            "Perp订单簿（WS）+ Bybit/OKX Spot订单簿（WS）。流动性缺口与冰山单检测。", kicker="OrderBook")
        book_ready = local_book.is_ready
        bk_c = st.columns(5)
        bk_c[0].metric("合约WS状态", "✅ 已建立" if book_ready else "⏳ 初始化中")
        bk_c[1].metric("最优买价", fp(local_book.best_bid()) if book_ready else "-")
        bk_c[2].metric("最优卖价", fp(local_book.best_ask()) if book_ready else "-")
        bk_c[3].metric("实时价差", fbps(local_book.spread_bps()) if book_ready else "-")
        # Spot book status
        spot_bybit = service.get_spot_book("bybit")
        spot_okx   = service.get_spot_book("okx")
        spot_status = []
        if spot_bybit and spot_bybit.is_ready: spot_status.append("Bybit Spot✅")
        else: spot_status.append("Bybit Spot⏳")
        if spot_okx and spot_okx.is_ready: spot_status.append("OKX Spot✅")
        else: spot_status.append("OKX Spot⏳")
        bk_c[4].metric("现货WS", " / ".join(spot_status))
        st.plotly_chart(build_local_book_figure(local_book, depth=min(depth_limit,50)), key="pc_localbook", use_container_width=True)
        st.caption("蓝色=买盘Bid；红色=卖盘Ask。条长=名义挂单金额。黄线=中间价。")

    # ══════ TAB 3: OI四象限+速率 ══════
    with tabs[3]:
        render_section("OI Delta 四象限  ·  加仓减仓分析 + 速率",
            "价格↑+OI↑=多头主动加仓｜价格↑+OI↓=空头回补｜价格↓+OI↑=空头加仓｜价格↓+OI↓=多头减仓", kicker="OI")
        oi_cols = st.columns(5)
        oi_cols[0].metric("多头加仓 Long Add",   f"{oi_delta_summ.get('long_add_pct',0):.1f}%")
        oi_cols[1].metric("空头加仓 Short Add",  f"{oi_delta_summ.get('short_add_pct',0):.1f}%")
        oi_cols[2].metric("空头回补 Short Cover",f"{oi_delta_summ.get('short_cover_pct',0):.1f}%")
        oi_cols[3].metric("多头减仓 Long Unwind",f"{oi_delta_summ.get('long_unwind_pct',0):.1f}%")
        oi_cols[4].metric("平均OI速率/min",       fc(oi_delta_summ.get("avg_velocity")))
        st.plotly_chart(build_oi_delta_figure(oi_delta_pts), key="pc_oi_delta", use_container_width=True)
        st.plotly_chart(build_oi_velocity_figure(oi_delta_pts), key="pc_oi_velocity", use_container_width=True)

    # ══════ TAB 4: 多空比矩阵 + Binance OI/多空拥挤度 ══════
    with tabs[4]:
        render_section("多空比矩阵  ·  Long/Short Ratio Matrix + 拥挤度",
            "Binance: 大户持仓多空比 / 大户账户比 / 全市场账户比 / Taker买卖量。Bybit: 主动买比例。", kicker="Crowd")

        # Binance OI+多空比综合图
        render_section("Binance 合约 OI + 多空人数 + Taker 可视化", kicker="Binance")
        oi_binance = service.get_oi_history("binance")
        st.plotly_chart(build_binance_oi_perp_figure(
            oi_binance, ls_count_data, taker_ratio_data),
            key="pc_binance_oi_ls", use_container_width=True)

        # Gauge widgets
        r1c1, r1c2, r1c3 = st.columns(3)
        # Global L/S gauge
        if ls_count_data:
            ld = ls_count_data[-1]
            gl = ld.get("global_long_pct"); gs = ld.get("global_short_pct")
            tl = ld.get("top_long_pct");   ts_ = ld.get("top_short_pct")
            with r1c1:
                st.markdown(build_ls_gauge_html(gl, gs, label="全市场账户多空 (Binance)", top_long=tl, top_short=ts_), unsafe_allow_html=True)
                st.metric("全市场多空比", f"{ld.get('global_ratio',0):.3f}" if ld.get('global_ratio') else "-")
        # Latest top trader ratio gauge
        if top_trader:
            lr_ = top_trader[-1]
            lsr = lr_.long_short_ratio
            if lsr:
                lp_ = lsr / (1+lsr) * 100
                sp_ = 100 - lp_
                with r1c2:
                    st.markdown(build_ls_gauge_html(lp_, sp_, label="大户持仓多空 (Binance Top Trader)"), unsafe_allow_html=True)
                    st.metric("大户持仓多空比", f"{lsr:.3f}")
        # Bybit buy ratio
        if bybit_ratio_raw:
            br = bybit_ratio_raw[-1].bybit_buy_ratio
            if br:
                with r1c3:
                    st.markdown(build_ls_gauge_html(br*100, (1-br)*100, label="Bybit 主动买比例 (Taker)"), unsafe_allow_html=True)
                    st.metric("Bybit主动买比", f"{br:.3f}")

        st.plotly_chart(build_top_trader_figure(top_trader, global_r, bybit_ratio_raw), key="pc_ratio", use_container_width=True)

        # Taker ratio summary
        if taker_ratio_data:
            td = taker_ratio_data[-1]
            tc1, tc2, tc3 = st.columns(3)
            tc1.metric("Taker主动买量", fc(td.get("buy_vol")))
            tc2.metric("Taker主动卖量", fc(td.get("sell_vol")))
            tc3.metric("买卖比", f"{td.get('ratio',0):.3f}" if td.get('ratio') else "-")
        else:
            st.info("大户比/Taker数据来自Binance，当前暂无数据（仅Binance合约支持）。")

    # ══════ TAB 5: Spot-Perp 现货-合约 ══════
    with tabs[5]:
        render_section("现货-合约实时价差  ·  Spot-Perp Spread & Lead/Lag",
            "三所实时现货价（WS）vs 合约价。价差正=合约溢价（多头拥挤）；负=合约折价。现货先行=真实需求驱动；合约先行=杠杆情绪。", kicker="SpotPerp")

        # Summary metrics
        spm_cols = st.columns(4)
        for i, ek in enumerate(("binance","bybit","okx")):
            s = snap_by_key.get(ek)
            if s:
                sp_bps = s.spot_perp_spread_bps if hasattr(s,'spot_perp_spread_bps') else None
                spm_cols[i].metric(f"{EXCHANGE_TITLES[ek]} 现货-合约价差",
                    fbps(sp_bps) if sp_bps is not None else "等待WS…",
                    delta=f"现货 {fp(getattr(s,'spot_price',None))}" if getattr(s,'spot_price',None) else None)
        # WS status
        spot_ws_status = []
        bybit_sp = service.get_spot_book("bybit")
        okx_sp   = service.get_spot_book("okx")
        if bybit_sp and bybit_sp.is_ready: spot_ws_status.append("Bybit✅")
        else: spot_ws_status.append("Bybit⏳")
        if okx_sp and okx_sp.is_ready: spot_ws_status.append("OKX✅")
        else: spot_ws_status.append("OKX⏳")
        binance_idx = service._spot_price.get("binance")
        spot_ws_status.append("Binance(IndexPx)✅" if binance_idx else "Binance⏳")
        spm_cols[3].metric("现货WS状态", " / ".join(spot_ws_status))

        # Realtime spread chart
        st.plotly_chart(build_spot_perp_realtime_figure(spread_hist_all), key="pc_spread_rt", use_container_width=True)

        # Spot-Perp alerts
        render_section("Spot-Perp 实时告警  ·  Lead/Lag & Diverge Alerts",
            "现货先拉/合约未跟 · OI上升买卖转弱 · 账户拥挤+爆仓联动 · 极端乖离", kicker="Alerts")
        render_spot_perp_alerts(sp_alerts)

        # Historical spread table
        if any(spread_hist_all.values()):
            render_section("价差历史记录 (最新50条)", kicker="SpotPerp")
            rows = []
            for ek, pts in spread_hist_all.items():
                for p in list(pts)[-20:]:
                    rows.append({"交易所":EXCHANGE_TITLES.get(ek,ek),
                        "时间":pd.to_datetime(p.timestamp_ms,unit="ms").strftime("%H:%M:%S"),
                        "现货价":p.spot_price, "合约价":p.perp_price,
                        "价差bps":round(p.spread_bps,2)})
            if rows:
                df_sp = pd.DataFrame(rows).sort_values("时间", ascending=False).head(50)
                st.dataframe(df_sp, use_container_width=True, hide_index=True,
                    column_config={"现货价":st.column_config.NumberColumn(format="%.2f"),
                        "合约价":st.column_config.NumberColumn(format="%.2f"),
                        "价差bps":st.column_config.NumberColumn(format="%.2f")})

    # ══════ TAB 6: Basis+期限结构 ══════
    with tabs[6]:
        render_section("Basis 合约溢价率  ·  Spot vs Futures Basis",
            "Basis = 合约价 - 现货价。正值=合约溢价（市场偏多）；负值=合约折价（恐慌/空头主导）。", kicker="Basis")
        st.plotly_chart(build_basis_figure(ok_snaps, spot_prices), key="pc_basis", use_container_width=True)
        render_section("期限结构  ·  Term Structure",
            "各到期日合约OI分布和相对现货溢价率。PERP=永续合约。", kicker="Term")
        st.plotly_chart(build_term_structure_figure(futures_oi_list), key="pc_term", use_container_width=True)
        render_section("现货 vs 合约持仓  ·  Spot Volume vs Perp OI",
            "现货/OI比值高=现货活跃度高于合约（偏真实需求）；比值低=合约主导（偏杠杆投机）。", kicker="SpotPerp")
        st.plotly_chart(build_spot_vs_perp_figure(ok_snaps, spot_volumes), key="pc_spotperp", use_container_width=True)

    # ══════ TAB 7: 爆仓瀑布 ══════
    with tabs[7]:
        render_section("跨所爆仓瀑布  ·  Cross-Exchange Liquidation Cascade",
            "四所爆仓事件叠加时间轴。多头爆仓▼=做多者被强平；空头爆仓▲=做空者被强平。", kicker="Cascade")
        st.plotly_chart(build_liquidation_cascade_figure(all_liq_by_exch), key="pc_cascade", use_container_width=True)
        clusters = detect_liquidation_clusters([e for ev in all_liq_by_exch.values() for e in ev])
        csc = st.columns(4)
        csc[0].metric("跨所总爆仓额", fc(sum(e.notional or 0 for ev in all_liq_by_exch.values() for e in ev)))
        csc[1].metric("Binance爆仓单", str(len(all_liq_by_exch.get("binance",[]))))
        csc[2].metric("Bybit爆仓单",   str(len(all_liq_by_exch.get("bybit",[]))))
        csc[3].metric("OKX爆仓单",     str(len(all_liq_by_exch.get("okx",[]))))

        # ✅ 已发生爆仓真值 vs ⚠️ 推断区 分开展示
        render_section("✅ 已发生爆仓真值  vs  ⚠️ 推断爆仓区", "真实WS/REST爆仓数据与模型推断区分开展示，不混排。", kicker="Cascade")

        real_col, infer_col = st.columns(2)
        with real_col:
            st.markdown("#### ✅ 已发生爆仓真值 (WS/REST)")
            st.plotly_chart(build_liq_reality_split_figure(liq_events), key="pc_liq_real", use_container_width=True)
        with infer_col:
            st.markdown("#### ⚠️ 推断爆仓区 (Inferred)")
            st.plotly_chart(build_heatmap_figure(liq_hf,"推断爆仓区 (Inferred Zone)",ref_price,LIQUIDATION_COLORSCALE,"数据不足"),
                            key="pc_liq_infer", use_container_width=True)

        # 多头止损 / 空头止损 左右两栏 上方10条 + 下方10条
        render_section("多头止损 / 空头止损  ·  上方10条 + 下方10条", "左栏=多头爆仓（做多被强平） / 右栏=空头爆仓（做空被强平）", kicker="Cascade")
        long_df, short_df = build_split_liq_tables(liq_events, limit_each=10)
        liq_l, liq_r = st.columns(2)
        with liq_l:
            st.markdown("##### 🔴 多头爆仓 Long Liquidations (最新10条)")
            if long_df.empty: st.info("暂无多头爆仓记录")
            else:
                st.dataframe(long_df, use_container_width=True, hide_index=True,
                    column_config={"价格":st.column_config.NumberColumn(format="%.2f"),
                        "金额$":st.column_config.NumberColumn(format="%.0f"),
                        "数量":st.column_config.NumberColumn(format="%.4f")})
        with liq_r:
            st.markdown("##### 🟢 空头爆仓 Short Liquidations (最新10条)")
            if short_df.empty: st.info("暂无空头爆仓记录")
            else:
                st.dataframe(short_df, use_container_width=True, hide_index=True,
                    column_config={"价格":st.column_config.NumberColumn(format="%.2f"),
                        "金额$":st.column_config.NumberColumn(format="%.0f"),
                        "数量":st.column_config.NumberColumn(format="%.4f")})

        if clusters:
            render_section("爆仓簇检测  ·  Liquidation Cluster (60s > 50万)", kicker="Cascade")
            st.dataframe(pd.DataFrame([{"时间":pd.to_datetime(c["start_ms"],unit="ms"),
                "单数":c["count"],"总额":c["total_notional"],"多头爆仓额":c["long_notional"],
                "空头爆仓额":c["short_notional"],"主导":c["dominant"]} for c in sorted(clusters,key=lambda x:x["start_ms"],reverse=True)[:20]]),
                use_container_width=True, hide_index=True,
                column_config={"总额":st.column_config.NumberColumn(format="%.0f"),
                    "多头爆仓额":st.column_config.NumberColumn(format="%.0f"),
                    "空头爆仓额":st.column_config.NumberColumn(format="%.0f")})

        # NEW: Cluster V2 + Cross-Exchange Timeline
        render_section("爆仓簇 V2  ·  跨所联动 & 单所分析", "🌐=跨所联动（黄色边框）；大气泡=总额大；强度=每秒爆仓额。", kicker="Cascade")
        st.plotly_chart(build_liq_cluster_v2_figure(liq_clusters_v2), key="pc_cluster_v2", use_container_width=True)
        render_section("跨所爆仓联动时间轴 (近5分钟)", "黄色高亮区=检测到跨所联动爆仓簇。▼=多头爆 ▲=空头爆", kicker="Cascade")
        st.plotly_chart(build_liq_cross_ex_timeline(liq_clusters_v2, all_liq_by_exch), key="pc_cross_ex_timeline", use_container_width=True)

        # Cluster V2 metrics
        if liq_clusters_v2:
            cv2_c = st.columns(4)
            cross_clusters = [c for c in liq_clusters_v2 if c.cross_exchange]
            max_c = max(liq_clusters_v2, key=lambda c: c.total_notional) if liq_clusters_v2 else None
            cv2_c[0].metric("总爆仓簇", str(len(liq_clusters_v2)))
            cv2_c[1].metric("跨所联动簇", str(len(cross_clusters)))
            cv2_c[2].metric("最大单簇金额", fc(max_c.total_notional) if max_c else "-")
            cv2_c[3].metric("最高瀑布得分", f"{max(c.cascade_score for c in liq_clusters_v2):.2f}" if liq_clusters_v2 else "-")

    # ══════ TAB 8: 冰山单+缺口 ══════
    with tabs[8]:
        render_section("冰山单检测  ·  Iceberg Order Detection",
            "30秒内同价位成交4次以上触发检测。气泡越大=总名义金额越高。", kicker="Iceberg")
        st.plotly_chart(build_iceberg_figure(iceberg_alerts), key="pc_iceberg", use_container_width=True)
        render_section("流动性缺口  ·  Liquidity Gap / Void",
            "某价位挂单量骤减>50%。可能是大单被吃掉或撤单。", kicker="Gap")
        gap_df = build_liquidity_gap_frame(liq_gaps)
        if gap_df.empty: st.info("暂未检测到流动性缺口（需WebSocket订单簿建立后开始检测）。")
        else:
            st.dataframe(gap_df, use_container_width=True, hide_index=True,
                column_config={"消失比例":st.column_config.ProgressColumn(format="%.0%",min_value=0,max_value=1),
                    "前挂单":st.column_config.NumberColumn(format="%.0f"),
                    "后挂单":st.column_config.NumberColumn(format="%.0f")})

    # ══════ TAB 9: 合成信号引擎 ══════
    with tabs[9]:
        render_section("OI + CVD + Funding + 拥挤度  ·  Composite Signal Engine",
            "四因子加权合成信号。偏多推进▲ / 偏空推进▼ / 拥挤衰竭⚡ / 吸收中≈ / 弱势→←。置信度=各因子方向一致性。", kicker="Signal")

        # Latest signal widget per exchange
        sig_cols = st.columns(len(EXCHANGE_ORDER))
        for col, ek in zip(sig_cols, EXCHANGE_ORDER):
            sig_list = composite_sigs_by_ex.get(ek, [])
            sig = sig_list[-1] if sig_list else None
            if sig:
                col.markdown(f"""
                <div style="padding:10px;border-radius:14px;border:1px solid rgba(255,255,255,0.13);
                    background:rgba(255,255,255,0.06);text-align:center;">
                  <div style="font-size:0.7rem;color:#bcd;">{EXCHANGE_TITLES.get(ek,ek)}</div>
                  <div style="font-size:1.1rem;font-weight:800;color:{sig.signal_color};">{sig.signal_label}</div>
                  <div style="font-size:0.78rem;color:#aac;">分 {sig.composite_score:+.2f} | 信 {sig.confidence:.0%}</div>
                </div>""", unsafe_allow_html=True)
            else:
                col.metric(EXCHANGE_TITLES.get(ek,ek), "等待数据…")

        # Radar for selected exchange
        st.markdown(build_composite_radar_html(latest_comp_sig), unsafe_allow_html=True)

        # Time series
        st.plotly_chart(build_composite_signal_figure(composite_sigs_by_ex), key="pc_composite", use_container_width=True)
        st.caption("绿虚线=+0.45（强多阈值） / 红虚线=-0.45（强空阈值）。穿越=合成信号强转。置信度=各因子方向一致性，>60%=可信。")

        # Factor breakdown table
        if latest_comp_sig:
            render_section("当前因子详情", kicker="Signal")
            factor_rows = [
                {"因子":"价格动能", "得分":f"{latest_comp_sig.price_score:+.3f}", "权重":"20%", "说明":"近期价格变化幅度"},
                {"因子":"OI方向",   "得分":f"{latest_comp_sig.oi_score:+.3f}",   "权重":"25%", "说明":"OI变化与价格方向共振"},
                {"因子":"CVD流向",  "得分":f"{latest_comp_sig.cvd_score:+.3f}",  "权重":"25%", "说明":"主动买卖净额方向"},
                {"因子":"资金费率", "得分":f"{latest_comp_sig.funding_score:+.3f}","权重":"15%","说明":"负费率=空头付费=偏多"},
                {"因子":"拥挤度",   "得分":f"{latest_comp_sig.crowd_score:+.3f}", "权重":"15%", "说明":"基于费率的多空拥挤推断"},
                {"因子":"合成总分", "得分":f"{latest_comp_sig.composite_score:+.3f}","权重":"100%","说明":latest_comp_sig.signal_label},
            ]
            st.dataframe(pd.DataFrame(factor_rows), use_container_width=True, hide_index=True)

    # ══════ TAB 10: 盘口质量+假挂单 ══════
    with tabs[10]:
        render_section("盘口质量监控  ·  Orderbook Quality & Cancel Speed",
            "实时追踪每笔挂单变化：新增/撤单/净流入。假挂单=大单短暂出现(<8秒)后撤离。吸收=大单被成交后快速补单。", kicker="OB")

        ob_c = st.columns(4)
        if ob_quality_sel:
            last_q = ob_quality_sel[-1]
            ob_c[0].metric("盘口质量分", f"{last_q.quality_score:.2f}")
            ob_c[1].metric("买单净增", fc(last_q.bid_net_notional))
            ob_c[2].metric("卖单净增", fc(last_q.ask_net_notional))
            ob_c[3].metric("假挂单数", str(len(fake_walls_sel)))
        else:
            for i in range(4): ob_c[i].metric(["质量分","买单净","卖单净","假挂单"][i], "等待…")

        st.plotly_chart(build_ob_quality_figure(ob_quality_sel), key="pc_ob_quality", use_container_width=True)

        ql, qr = st.columns(2)
        with ql:
            render_section("分价位 新增/撤单 分布", kicker="OB")
            st.plotly_chart(build_ob_delta_heatmap(ob_delta_sel), key="pc_ob_delta_heat", use_container_width=True)
        with qr:
            render_section("假挂单 & 大单吸收", kicker="OB")
            st.plotly_chart(build_fake_wall_figure(fake_walls_sel, absorb_sel), key="pc_fake_wall", use_container_width=True)

        # Fake wall table
        if fake_walls_sel:
            render_section(f"疑似假挂单记录（近{len(fake_walls_sel)}条）", kicker="OB")
            fw_rows = [{"时间":pd.to_datetime(f.timestamp_ms,unit="ms").strftime("%H:%M:%S"),
                "方向":"买" if f.side=="bid" else "卖","价格":f.price,
                "峰值金额$K":round(f.peak_notional/1e3,1),"存续ms":f.lifespan_ms} for f in list(fake_walls_sel)[-20:]]
            st.dataframe(pd.DataFrame(fw_rows), use_container_width=True, hide_index=True,
                column_config={"价格":st.column_config.NumberColumn(format="%.2f"),
                    "峰值金额$K":st.column_config.NumberColumn(format="%.1f"),
                    "存续ms":st.column_config.NumberColumn(format="%.0f")})

        st.caption("质量分>0.6=健康流动性；<0.35=大量撤单可能为流动性退出信号。假挂单=撤单速度快于8秒的大额挂单。")

    # ══════ TAB 11: 告警时间线 ══════
    with tabs[11]:
        render_section("告警时间线  ·  Confirmed Alert Timeline",
            "去抖动后已确认的告警。强=2次连续触发；中=3次；弱=4次。不会因单次噪声误报。", kicker="Alerts")

        # Summary metrics
        al_c = st.columns(4)
        al_c[0].metric("已确认告警", str(len(confirmed_alerts)))
        al_c[1].metric("强级告警", str(sum(1 for a in confirmed_alerts if a.severity=="strong")))
        al_c[2].metric("中级告警", str(sum(1 for a in confirmed_alerts if a.severity=="medium")))
        al_c[3].metric("弱级告警",  str(sum(1 for a in confirmed_alerts if a.severity=="weak")))

        st.plotly_chart(build_alert_timeline_figure(alert_timeline), key="pc_alert_timeline", use_container_width=True)

        # Confirmed alerts list
        if confirmed_alerts:
            render_section("已确认告警详情", kicker="Alerts")
            sev_icon = {"strong":"🔴","medium":"🟡","weak":"🔵"}
            for ca in sorted(confirmed_alerts, key=lambda x: x.confirmed_at_ms, reverse=True)[:20]:
                ts_str = pd.to_datetime(ca.confirmed_at_ms, unit="ms").strftime("%H:%M:%S")
                icon   = sev_icon.get(ca.severity, "⚪")
                sev_cn = {"strong":"强","medium":"中","weak":"弱"}.get(ca.severity, ca.severity)
                st.markdown(f"""
                <div style="padding:10px 14px;border-radius:12px;margin:3px 0;
                    border:1px solid rgba(255,255,255,0.1);background:rgba(255,255,255,0.05);">
                  <span style="font-size:0.9rem;">{icon} <b>[{sev_cn}级 ×{ca.confirm_count}确认]</b>
                  {ca.exchange} — {ts_str}</span><br/>
                  <span style="color:#e0e8ff;font-size:0.85rem;">{ca.message}</span>
                  <span style="color:#888;font-size:0.75rem;margin-left:8px;">强度 {ca.score:.2f}</span>
                </div>""", unsafe_allow_html=True)
        else:
            st.info("暂无已确认告警。连续触发达到阈值后自动确认（强级需2次，中级3次，弱级4次）。")

        st.caption("📌 确认机制：同类告警连续触发 N 次后才正式发出，避免一闪而过的噪声。冷却期：强90s / 中180s / 弱300s。")

    # ══════ TAB 12: 回放复盘 ══════
    with tabs[12]:
        render_section("回放复盘  ·  Event Replay & Review",
            "录制每秒快照，支持回放价格/CVD/爆仓。可用于策略复盘、爆仓前后分析、盘口回看。", kicker="Replay")

        rec_c = st.columns(4)
        is_rec = service.recorder_active
        rec_c[0].metric("录制状态", "🔴 录制中" if is_rec else "⏹ 已停止")
        rec_c[1].metric("已录制帧数", str(len(recorded_frames)))
        rec_c[2].metric("录制时长", f"{len(recorded_frames):.0f}s" if recorded_frames else "0s")
        max_frames_info = "最多3600帧 (1h)"
        rec_c[3].metric("容量", max_frames_info)

        btn_c1, btn_c2, btn_c3 = st.columns(3)
        if btn_c1.button("▶ 开始录制" if not is_rec else "⏸ 暂停录制"):
            if is_rec: service.stop_recording()
            else: service.start_recording()
            st.rerun()
        if btn_c2.button("🗑 清空录制"):
            service.clear_recording(); st.rerun()

        if not recorded_frames:
            st.info("点击「开始录制」后，系统每秒录制一帧（价格+CVD+爆仓+资金费率+现货-合约价差）。")
        else:
            # Speed selector
            speed = st.select_slider("回放速度", options=["1x","5x","20x"], value="1x")
            # Frame slice for display
            n_show = {"1x": len(recorded_frames), "5x": len(recorded_frames)//5+1, "20x": len(recorded_frames)//20+1}.get(speed, len(recorded_frames))
            step   = {"1x":1,"5x":5,"20x":20}.get(speed,1)
            frames_to_show = list(recorded_frames)[::step][-min(300, n_show):]

            st.plotly_chart(build_replay_price_figure(frames_to_show, speed), key="pc_replay", use_container_width=True)

            # Replay table
            render_section("帧数据预览（最新20帧）", kicker="Replay")
            frame_rows = []
            for f in list(recorded_frames)[-20:]:
                row = {"时间": pd.to_datetime(f.timestamp_ms,unit="ms").strftime("%H:%M:%S")}
                for ek in ("binance","bybit","okx"):
                    row[f"{ek[:3].upper()}价"] = f"{f.prices.get(ek,0) or 0:.2f}" if f.prices.get(ek) else "-"
                    row[f"{ek[:3].upper()}CVD"] = f"{f.cvd_values.get(ek,0)/1e3:.1f}K"
                row["爆仓数"] = len(f.liq_events)
                frame_rows.append(row)
            st.dataframe(pd.DataFrame(frame_rows), use_container_width=True, hide_index=True)

    # ══════ TAB 13: 全市场对比 ══════
    with tabs[13]:
        render_section("全市场对比  ·  Cross-Exchange Snapshot", "四所价格、持仓、费率、成交额横向对比（Binance排首位）。")
        ordered_snaps = sorted(snapshots, key=lambda s: 0 if s.exchange=="Binance" else 1)
        st.dataframe(build_snapshot_frame(ordered_snaps), use_container_width=True, hide_index=True,
            column_config={"最新价":st.column_config.NumberColumn(format="%.2f"),
                "现货价":st.column_config.NumberColumn(format="%.2f"),
                "现货-合约(bps)":st.column_config.NumberColumn(format="%.2f"),
                "持仓金额":st.column_config.NumberColumn(format="%.0f"),
                "费率bps":st.column_config.NumberColumn(format="%.4f")})
        st.plotly_chart(build_funding_comparison_figure(ok_snaps), key="pc_funding_compare", use_container_width=True)

    # ══════ TAB 14: 多币种轮巡 ══════
    with tabs[14]:
        render_section("多币种轮巡  ·  Multi-Coin Watchlist", f"快速扫描 {len(watchlist_coins)} 个币种。", kicker="Watch")
        if not watchlist_coins: st.info("请在左侧侧边栏输入监控币种。")
        else:
            from exchanges import BinanceClient
            client = BinanceClient(timeout=req_timeout)
            wrows = []
            for coin in watchlist_coins[:15]:
                try:
                    s = client.fetch(f"{coin}USDT")
                    if s.status=="ok":
                        wrows.append({"币种":coin,"最新价":s.last_price,"持仓金额":s.open_interest_notional,
                            "资金费率bps":s.funding_bps,"24h成交额":s.volume_24h_notional,"状态":"✅"})
                    else: wrows.append({"币种":coin,"最新价":None,"持仓金额":None,"资金费率bps":None,"24h成交额":None,"状态":"❌"})
                except: wrows.append({"币种":coin,"最新价":None,"持仓金额":None,"资金费率bps":None,"24h成交额":None,"状态":"⚠️"})
            if wrows:
                st.dataframe(pd.DataFrame(wrows), use_container_width=True, hide_index=True,
                    column_config={"最新价":st.column_config.NumberColumn(format="%.4f"),
                        "持仓金额":st.column_config.NumberColumn(format="%.0f"),
                        "资金费率bps":st.column_config.NumberColumn(format="%.4f"),
                        "24h成交额":st.column_config.NumberColumn(format="%.0f")})

    # ══════ TAB 15: 预警 ══════
    with tabs[15]:
        render_section("预警系统  ·  Alert System", "价格/OI/费率/CVD/OI速率等指标触发条件。去抖动后才显示。", kicker="Alerts")
        if sp_alerts:
            st.subheader(f"🔗 Spot-Perp 告警（{len(sp_alerts)}条，含去抖动确认）")
            render_spot_perp_alerts(sp_alerts, max_show=10)
            st.markdown("---")
        if "alert_rules" not in st.session_state: st.session_state["alert_rules"] = []
        with st.expander("➕ 添加新预警规则", expanded=False):
            a1,a2,a3 = st.columns(3)
            n   = a1.text_input("规则名称", placeholder="如：BTC破7万")
            exk = a2.selectbox("交易所", list(EXCHANGE_ORDER), format_func=lambda k: EXCHANGE_TITLES[k], key="al_exch")
            met = a3.selectbox("监控指标", list(ALERT_METRICS.keys()), format_func=lambda k: ALERT_METRICS[k], key="al_met")
            a4,a5,a6 = st.columns(3)
            cond  = a4.selectbox("条件", list(ALERT_CONDITIONS.keys()), format_func=lambda k: ALERT_CONDITIONS[k], key="al_cond")
            thresh= a5.number_input("阈值", value=0.0, format="%.6f", key="al_thresh")
            if a6.button("添加"):
                if n.strip():
                    st.session_state["alert_rules"].append(AlertRule(rule_id=str(uuid.uuid4())[:8],
                        name=n.strip(), exchange=exk, symbol=symbol_map[exk],
                        metric=met, condition=cond, threshold=thresh))
                    service.set_alert_rules(st.session_state["alert_rules"])
                    st.success(f"已添加：{n}"); st.rerun()
        if st.session_state["alert_rules"]: service.set_alert_rules(st.session_state["alert_rules"])
        for i, rule in enumerate(st.session_state.get("alert_rules",[])):
            rc1,rc2,rc3,rc4,rc5 = st.columns([2,2,2,1,1])
            rc1.write(f"**{rule.name}**")
            rc2.write(f"{EXCHANGE_TITLES.get(rule.exchange,rule.exchange)} · {ALERT_METRICS.get(rule.metric,rule.metric)}")
            rc3.write(f"{ALERT_CONDITIONS.get(rule.condition,rule.condition)} {rule.threshold:.4g}")
            rc4.write("🔔 已触发" if rule.triggered else "⏳ 监听中")
            if rc5.button("删除", key=f"del_{i}"):
                st.session_state["alert_rules"].pop(i); service.set_alert_rules(st.session_state["alert_rules"]); st.rerun()
        if not st.session_state.get("alert_rules"):
            st.info('暂无预警规则。点击"添加新预警规则"创建。')
        if alert_events:
            st.subheader(f"预警历史（{len(alert_events)}条）")
            arows=[{"触发时间":pd.to_datetime(e.triggered_at_ms,unit="ms"),"规则":e.name,
                "交易所":EXCHANGE_TITLES.get(e.exchange,e.exchange),"实际值":e.actual_value,"阈值":e.threshold,"消息":e.message}
                for e in reversed(list(alert_events)[-50:])]
            st.dataframe(pd.DataFrame(arows), use_container_width=True, hide_index=True,
                column_config={"实际值":st.column_config.NumberColumn(format="%.4g"),
                    "阈值":st.column_config.NumberColumn(format="%.4g")})

    # ══════ TAB 16: 调试 ══════
    with tabs[16]:
        render_section("接口调试  ·  API Debug")
        st.write(f"主图: `{sel_snap.exchange}` | 合约: `{sel_sym}`")
        st.write(f"WS订单簿: `{'已建立' if local_book.is_ready else '初始化中'}` | OI点数: `{len(service.get_oi_history(selected_exchange))}` | WS成交: `{len(ws_trades)}` | CVD点数: `{len(cvd_points)}`")
        st.write(f"已确认告警: `{len(confirmed_alerts)}` | 告警时间线: `{len(alert_timeline)}` | 爆仓簇v2: `{len(liq_clusters_v2)}`")
        st.write(f"盘口质量点: `{len(ob_quality_sel)}` | 假挂单: `{len(fake_walls_sel)}` | 录制帧: `{len(recorded_frames)}`")
        st.write(f"合成信号: `{ {ek: len(v) for ek,v in composite_sigs_by_ex.items()} }`")
        st.write(f"Spot WS: `{ {k: f'{v:.2f}' if v else 'None' for k,v in service._spot_price.items()} }`")
        for s in snapshots:
            with st.expander(f"{s.exchange} | {s.symbol} | {CARD_STATUS.get(s.status,s.status)}"):
                if s.error: st.error(s.error)
                st.json(s.raw)


render_terminal()
