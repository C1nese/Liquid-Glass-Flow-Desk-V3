from __future__ import annotations
import logging
import time, uuid
from typing import Dict, List, Optional
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

# ── P2: 统一日志记录器 ────────────────────────────────────────────────────────
_logger = logging.getLogger("liquidity_terminal")
if not _logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    _logger.addHandler(_h)
_logger.setLevel(logging.WARNING)

# ── Standard library analytics & exchanges ────────────────────────────────────
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
    build_ob_quality_figure, build_fake_wall_figure, build_ob_delta_heatmap,
    build_composite_signal_figure, build_composite_radar_html,
    build_liq_cluster_v2_figure, build_liq_cross_ex_timeline,
    build_alert_timeline_figure,
    build_replay_price_figure,
    detect_price_levels, build_price_levels_annotations,
    build_bull_bear_power_figure, detect_hot_coins,
    # P1 新增：MACD / ATR
    build_macd_atr_figure, calc_macd, calc_atr,
    # v8 新增
    build_contract_sentiment_figure, build_sentiment_gauge_html,
    build_spot_flow_figure, build_perp_flow_figure, build_combined_flow_figure,
    build_liq_confidence_heatmap,
    build_whale_heatmap_figure,
    build_risk_radar_figure, build_risk_history_figure,
)
from exchanges import (
    EXCHANGE_ORDER, SUPPORTED_INTERVALS, default_symbols,
    fetch_exchange_candles, fetch_exchange_liquidations,
    fetch_exchange_oi_history, fetch_exchange_orderbook,
    fetch_exchange_recent_trades, fetch_exchange_top_trader_ratio,
    fetch_exchange_global_long_short_ratio,
    fetch_exchange_spot_ticker, fetch_exchange_futures_oi_list,
    interval_to_millis, MARKET_SCAN_COINS as _SCAN_COINS,
    fetch_contract_sentiment_point, detect_split_orders, build_risk_radar_point,
)
from models import (
    Candle, ExchangeSnapshot, LiquidationEvent, OIPoint, OrderBookLevel,
    AlertRule, AlertEvent, LocalOrderBook,
    ContractSentimentPoint, LiquidationWithConfidence,
    SpotFlowSnapshot, PerpFlowSnapshot, CombinedFlowView,
    SpotLargeOrderFlow, PerpLargeOrderFlow, SplitOrderCluster, RiskRadarPoint,
)
from realtime import LiveTerminalService

# ── v6 增强模块 ────────────────────────────────────────────────────────────────
try:
    from hl_center import render_hl_center
    _HAS_HL_CENTER = True
except ImportError:
    _HAS_HL_CENTER = False

try:
    from signal_center import render_signal_center, VPINCalculator
    from aggregator import detect_arbitrage_signals, detect_funding_arbitrage
    _HAS_SIGNAL_CENTER = True
except ImportError:
    _HAS_SIGNAL_CENTER = False

try:
    from push_settings import render_push_settings
    from notifier import get_notifier, BROWSER_NOTIFICATION_JS
    from storage import (
        init_db, insert_oi_from_snapshots, insert_funding_from_snapshots,
        insert_alert_history,
    )
    _HAS_PUSH_STORAGE = True
except ImportError:
    _HAS_PUSH_STORAGE = False

try:
    from exchanges import fetch_binance_long_short_count, fetch_binance_taker_ratio
    _HAS_LS_COUNT = True
except ImportError:
    _HAS_LS_COUNT = False

# ── v8 模块 ────────────────────────────────────────────────────────────────────
try:
    from exchanges import fetch_contract_sentiment_point as _fcs
    _HAS_V8_SENTIMENT = True
except ImportError:
    _HAS_V8_SENTIMENT = False

try:
    import private_client as _private_client_mod
    _HAS_PRIVATE_CLIENT = True
except ImportError:
    _HAS_PRIVATE_CLIENT = False

# ── 资金费率倒计时 + 智能刷新 ─────────────────────────────────────────────────

def get_funding_countdown() -> tuple:
    """返回 (距下次结算秒数, 格式化字符串, 是否临近结算<5min)"""
    import math
    now_ts = time.time()
    # Funding settles at 00:00, 08:00, 16:00 UTC every day
    period = 8 * 3600  # 8 hours in seconds
    next_settlement = math.ceil(now_ts / period) * period
    secs_left = int(next_settlement - now_ts)
    h = secs_left // 3600
    m = (secs_left % 3600) // 60
    s = secs_left % 60
    is_near = secs_left < 300   # within 5 minutes
    is_very_near = secs_left < 60
    label = f"{h:02d}:{m:02d}:{s:02d}"
    return secs_left, label, is_near, is_very_near

def compute_smart_refresh(snapshots, base_refresh: int = 2) -> int:
    """
    根据市场波动自动调整刷新频率：
    - 剧烈波动（价差大 / OI速变）→ 最快 1s
    - 平静市场 → 最慢 5s
    """
    if not snapshots:
        return base_refresh
    # Check spread and OI velocity as volatility proxy
    spreads = [abs(getattr(s, "spot_perp_spread_bps", 0) or 0) for s in snapshots if s.status == "ok"]
    avg_spread = sum(spreads) / len(spreads) if spreads else 0

    # High spread = volatile = faster refresh
    if avg_spread > 20:
        return 1
    elif avg_spread > 8:
        return 2
    else:
        # Also check funding countdown - near settlement = faster
        secs_left, _, is_near, _ = get_funding_countdown()
        if is_near:
            return 1
        return min(base_refresh, 4)

# ── Constants ─────────────────────────────────────────────────────────────────
POPULAR_COINS = ["BTC","ETH","SOL","XRP","BNB","DOGE","ADA","SUI","AVAX",
                 "LINK","LTC","HYPE","TAO","PEPE","PENDLE","WIF","TRUMP","FARTCOIN"]
WATCHLIST_DEFAULT   = ["BTC","ETH","SOL","XRP","BNB"]
EXCHANGE_ORDER_UI   = ("binance","bybit","okx","hyperliquid")
EXCHANGE_TITLES     = {"bybit":"Bybit","binance":"Binance","okx":"OKX","hyperliquid":"Hyperliquid"}
BID_PALETTE = ["#dff8ff","#bdefff","#92ddff","#5fc0ff","#279cff","#1768d3"]
ASK_PALETTE = ["#fff1db","#ffd9ad","#ffbe80","#ff9a59","#ff6938","#d8452d"]
CARD_STATUS = {"ok":"正常","error":"异常"}
ALERT_METRICS    = {"price":"最新价","oi":"持仓金额","funding":"资金费率",
                    "liq_notional":"爆仓额-60min","cvd":"CVD累积",
                    "spread_bps":"价差bps","oi_velocity":"OI速率/min"}
ALERT_CONDITIONS = {"above":"超过","below":"低于","cross_up":"向上穿越","cross_down":"向下穿越"}
SPOT_PERP_ALERT_LABELS = {
    "spot_lead_up":    ("🟢 现货先拉↑", "#1dc796"),
    "spot_lead_down":  ("🔴 现货先跌↓", "#ff6868"),
    "oi_up_cvd_weak":  ("⚠️ OI升/买弱",  "#ffa94d"),
    "oi_down_cvd_up":  ("🔵 OI降/轧空",  "#62c2ff"),
    "diverge_extreme": ("🚨 极端乖离",   "#ff4444"),
    "crowd_liq_combo": ("⚡ 拥挤+爆仓", "#ff8c00"),
    "fake_wall":       ("🎭 假挂单",     "#ffa94d"),
    "composite_signal":("🧠 合成信号",  "#c084fc"),
}

# ── Page config — MUST be first Streamlit call ────────────────────────────────
st.set_page_config(
    page_title="多交易所流动性终端 v5",
    layout="wide",
    initial_sidebar_state="expanded"
)
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
section[data-testid="stSidebar"] .stMarkdown,section[data-testid="stSidebar"] label,section[data-testid="stSidebar"] span,section[data-testid="stSidebar"] p{color:#dce7f7!important;}
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
.stMarkdown p,.stCaption,label,[data-testid="stWidgetLabel"] p{color:#dce8f6!important;}
div[data-testid="stMetric"]{background:linear-gradient(145deg,rgba(255,255,255,0.15),rgba(255,255,255,0.07));border:1px solid rgba(255,255,255,0.16);border-radius:20px;padding:0.82rem 0.92rem;box-shadow:var(--shadow);backdrop-filter:blur(28px);}
div[data-testid="stMetric"]:hover{transform:translateY(-1px);}
div[data-testid="stMetricLabel"]*{color:#d7e6f8!important;font-weight:600;}
div[data-testid="stMetricValue"]{color:#fff;}
.stTabs [data-baseweb="tab-list"]{gap:0.28rem;padding:0.38rem 0.28rem;margin-bottom:0.8rem;border-radius:20px;border:1px solid rgba(255,255,255,0.12);background:rgba(255,255,255,0.07);backdrop-filter:blur(24px);flex-wrap:wrap!important;height:auto!important;max-height:none!important;}
.stTabs [data-baseweb="tab"]{height:2.3rem;border-radius:999px;color:#d3e0f2;background:transparent;font-weight:600;font-size:0.82rem;white-space:nowrap;}
.stTabs [aria-selected="true"]{background:linear-gradient(135deg,rgba(255,255,255,0.22),rgba(255,255,255,0.10));color:#fff!important;}
div[data-baseweb="select"]>div,.stTextInput input{background:rgba(255,255,255,0.10)!important;border:1px solid rgba(255,255,255,0.14)!important;border-radius:12px!important;color:#f8fbff!important;}
.stButton>button{border-radius:999px;border:1px solid rgba(255,255,255,0.15);background:linear-gradient(135deg,rgba(255,255,255,0.17),rgba(255,255,255,0.08));color:#fff;backdrop-filter:blur(16px);transition:transform 220ms ease;}
.stButton>button:hover{transform:translateY(-1px);}
div[data-testid="stDataFrame"],div[data-testid="stPlotlyChart"],details[data-testid="stExpander"]{border-radius:20px;overflow:hidden;border:1px solid rgba(255,255,255,0.12);box-shadow:var(--shadow);background:rgba(255,255,255,0.05);backdrop-filter:blur(24px);}
div[data-testid="stAlert"]{border-radius:16px;border:1px solid rgba(255,255,255,0.13);background:rgba(255,255,255,0.08);backdrop-filter:blur(22px);color:#f6fbff;}
</style>""", unsafe_allow_html=True)


# ── Cached loaders ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=15, show_spinner=False, max_entries=32)
def load_candles(ek, sym, iv, lim, to):
    try:
        return fetch_exchange_candles(ek, sym, iv, lim, timeout=to)
    except Exception as e:
        _logger.warning("load_candles %s/%s: %s", ek, sym, e)
        return []

@st.cache_data(ttl=5, show_spinner=False, max_entries=16)
def load_orderbook(ek, sym, lim, to):
    try:
        return fetch_exchange_orderbook(ek, sym, lim, timeout=to)
    except Exception as e:
        _logger.warning("load_orderbook %s/%s: %s", ek, sym, e)
        return []

@st.cache_data(ttl=90, show_spinner=False, max_entries=16)
def load_oi_backfill(ek, sym, iv, lim, to):
    try:
        return fetch_exchange_oi_history(ek, sym, iv, lim, timeout=to)
    except Exception as e:
        _logger.warning("load_oi_backfill %s/%s: %s", ek, sym, e)
        return []

@st.cache_data(ttl=10, show_spinner=False)
def load_liquidations(ek, sym, lim, to):
    try:
        return fetch_exchange_liquidations(ek, sym, lim, timeout=to)
    except Exception as e:
        _logger.warning("load_liquidations %s/%s: %s", ek, sym, e)
        return []

@st.cache_data(ttl=30, show_spinner=False)
def load_trades(ek, sym, lim, to):
    try:
        return fetch_exchange_recent_trades(ek, sym, lim, timeout=to)
    except Exception as e:
        _logger.warning("load_trades %s/%s: %s", ek, sym, e)
        return []

@st.cache_data(ttl=60, show_spinner=False)
def load_top_trader(ek, sym, iv, lim, to):
    try:
        return fetch_exchange_top_trader_ratio(ek, sym, iv, lim, timeout=to)
    except Exception as e:
        _logger.warning("load_top_trader %s/%s: %s", ek, sym, e)
        return []

@st.cache_data(ttl=60, show_spinner=False)
def load_global_ratio(ek, sym, iv, lim, to):
    try:
        return fetch_exchange_global_long_short_ratio(ek, sym, iv, lim, timeout=to)
    except Exception as e:
        _logger.warning("load_global_ratio %s/%s: %s", ek, sym, e)
        return []

@st.cache_data(ttl=60, show_spinner=False)
def load_spot_ticker(ek, coin, to):
    try:
        return fetch_exchange_spot_ticker(ek, coin, timeout=to)
    except Exception as e:
        _logger.warning("load_spot_ticker %s/%s: %s", ek, coin, e)
        return None

@st.cache_data(ttl=120, show_spinner=False)
def load_futures_oi_list(ek, coin, to):
    try:
        return fetch_exchange_futures_oi_list(ek, coin, timeout=to)
    except Exception as e:
        _logger.warning("load_futures_oi_list %s/%s: %s", ek, coin, e)
        return []

@st.cache_data(ttl=45, show_spinner=False)
def load_ls_count(sym, iv, lim, to):
    if not _HAS_LS_COUNT:
        return []
    try:
        return fetch_binance_long_short_count(sym, iv, lim, to)
    except Exception as e:
        _logger.warning("load_ls_count %s: %s", sym, e)
        return []

@st.cache_data(ttl=45, show_spinner=False)
def load_taker_ratio(sym, iv, lim, to):
    if not _HAS_LS_COUNT:
        return []
    try:
        return fetch_binance_taker_ratio(sym, iv, lim, to)
    except Exception as e:
        _logger.warning("load_taker_ratio %s: %s", sym, e)
        return []

@st.cache_data(ttl=30, show_spinner=False)
def load_market_batch_cached(coins_tuple: tuple, timeout: int):
    """Cached market scan — coins as tuple for hashability"""
    try:
        from exchanges import MarketScanClient
        client = MarketScanClient(timeout=timeout)
        return client.fetch_market_batch(list(coins_tuple), max_workers=10)
    except Exception as e:
        _logger.warning("load_market_batch_cached: %s", e)
        return []


# ── v8 캐시된 로더 ────────────────────────────────────────────────────────────
@st.cache_data(ttl=30, show_spinner=False)
def load_contract_sentiment(binance_sym: str, bybit_sym: str, interval: str, to: int):
    """v8 방향1: 합약 감정 진실층 — Binance 4端点 + Bybit Taker 병렬"""
    try:
        return fetch_contract_sentiment_point(binance_sym, bybit_sym, interval, timeout=to)
    except Exception as e:
        _logger.warning("load_contract_sentiment: %s", e)
        return None


# ── Formatters ─────────────────────────────────────────────────────────────────
def fp(v): return "-" if v is None else f"{v:,.2f}"

def load_snapshots_concurrent(clients, symbol_map, timeout=10):
    """4所快照并发请求，带超时熔断，避免单所卡死全局"""
    from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutTimeout
    results = {}
    def _fetch(ek):
        try:
            sym = symbol_map.get(ek, "")
            if not sym: return ek, None
            return ek, clients[ek].fetch(sym)
        except Exception as e:
            return ek, None
    with ThreadPoolExecutor(max_workers=4) as exe:
        futs = {exe.submit(_fetch, ek): ek for ek in clients}
        for fut in as_completed(futs, timeout=timeout + 2):
            try:
                ek, snap = fut.result(timeout=1)
                results[ek] = snap
            except Exception:
                results[futs[fut]] = None
    return results
def fc(v):
    if v is None: return "-"
    v=float(v); av=abs(v)
    if av>=1e9: return f"{v/1e9:.2f}B"
    if av>=1e6: return f"{v/1e6:.2f}M"
    if av>=1e3: return f"{v/1e3:.1f}K"
    return f"{v:.2f}"
def fbps(v): return "-" if v is None else f"{v:.2f} bps"
def fpct(v): return "-" if v is None else f"{v:.2f}%"
def frate(v): return "-" if v is None else f"{v:+.6f}"

def render_section(title, subtitle="", kicker="Desk"):
    sub = ('<div class="glass-sub">{}</div>'.format(subtitle)) if subtitle else ""
    st.markdown(
        '<div class="glass-section">'
        '<div class="glass-kicker">{kicker}</div>'
        '<div class="glass-title">{title}</div>'
        '{sub}</div>'.format(kicker=kicker, title=title, sub=sub),
        unsafe_allow_html=True)

def status_caption(s):
    return f"{s.exchange}: 正常" if s.status=="ok" else f"{s.exchange}: {s.error or '异常'}"

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

def _calc_ma(series, n):
    """计算简单移动平均"""
    return series.rolling(window=n, min_periods=1).mean()

def _calc_bollinger(series, n=20, k=2.0):
    """计算布林带：中轨/上轨/下轨"""
    mid = series.rolling(window=n, min_periods=1).mean()
    std = series.rolling(window=n, min_periods=1).std(ddof=0).fillna(0)
    return mid, mid + k*std, mid - k*std

def _calc_rsi(series, n=14):
    """计算 RSI"""
    delta = series.diff()
    gain  = delta.clip(lower=0).rolling(n, min_periods=1).mean()
    loss  = (-delta.clip(upper=0)).rolling(n, min_periods=1).mean()
    rs    = gain / loss.replace(0, float("nan"))
    return 100 - 100 / (1 + rs)

def build_terminal_chart(candles, heat_bars, snapshot, interval,
                          show_ma=True, show_bb=True, show_rsi=True,
                          ma_periods=(5, 20, 60)):
    """K线图 + 技术指标（MA / 布林带 / RSI） + 热力盘口"""
    # Layout: row1=K线+指标, row2=成交量, row3=RSI（可选）
    row_heights = [0.62, 0.18, 0.20] if show_rsi else [0.78, 0.22]
    rows = 3 if show_rsi else 2
    fig = make_subplots(rows=rows, cols=1, shared_xaxes=True,
                        vertical_spacing=0.03, row_heights=row_heights)

    if not candles:
        fig.add_annotation(text="没有可用K线数据", showarrow=False,
                           x=0.5, y=0.5, xref="paper", yref="paper")
        return fig

    df = pd.DataFrame({
        "ts": pd.to_datetime([c.timestamp_ms for c in candles], unit="ms"),
        "o":  [c.open  for c in candles],
        "h":  [c.high  for c in candles],
        "l":  [c.low   for c in candles],
        "c":  [c.close for c in candles],
        "v":  [c.volume for c in candles],
        "tb": [getattr(c,"taker_buy_volume",None) for c in candles],
    })
    up, dn = "#1dc796", "#ff6868"

    # ── K线主图 ──────────────────────────────────────────────────────────────
    fig.add_trace(go.Candlestick(
        x=df["ts"], open=df["o"], high=df["h"], low=df["l"], close=df["c"],
        increasing_line_color=up, increasing_fillcolor=up,
        decreasing_line_color=dn, decreasing_fillcolor=dn,
        name="K线", line_width=1.2), row=1, col=1)

    # ── 均线 MA ──────────────────────────────────────────────────────────────
    if show_ma:
        ma_colors = {5: "#f8d35e", 20: "#62c2ff", 60: "#ff8c66"}
        for p in ma_periods:
            if len(df) >= p:
                ma = _calc_ma(df["c"], p)
                fig.add_trace(go.Scatter(
                    x=df["ts"], y=ma, mode="lines", name=f"MA{p}",
                    line=dict(color=ma_colors.get(p, "#aaa"), width=1.2, dash="solid"),
                    opacity=0.85), row=1, col=1)

    # ── 布林带 BB ─────────────────────────────────────────────────────────────
    if show_bb and len(df) >= 20:
        bb_mid, bb_up, bb_dn = _calc_bollinger(df["c"], 20, 2.0)
        fig.add_trace(go.Scatter(
            x=df["ts"], y=bb_up, mode="lines", name="BB上轨",
            line=dict(color="rgba(180,150,255,0.6)", width=1, dash="dot"),
            showlegend=False), row=1, col=1)
        fig.add_trace(go.Scatter(
            x=df["ts"], y=bb_dn, mode="lines", name="BB下轨",
            line=dict(color="rgba(180,150,255,0.6)", width=1, dash="dot"),
            fill="tonexty", fillcolor="rgba(150,120,255,0.05)",
            showlegend=False), row=1, col=1)
        fig.add_trace(go.Scatter(
            x=df["ts"], y=bb_mid, mode="lines", name="BB中轨",
            line=dict(color="rgba(180,150,255,0.45)", width=0.8),
            showlegend=False), row=1, col=1)

    # ── 热力盘口背景 ─────────────────────────────────────────────────────────
    x_end  = candles[-1].timestamp_ms + interval_to_millis(interval)
    span   = max(x_end - candles[0].timestamp_ms, interval_to_millis(interval) * 20)
    for bar in heat_bars:
        it = bar["intensity"]
        x0 = int(x_end - span * (0.28 + 0.72 * it))
        ch = palette_color(str(bar["side"]), it)
        fig.add_shape(type="rect",
            x0=pd.to_datetime(x0, unit="ms"), x1=pd.to_datetime(x_end, unit="ms"),
            y0=bar["price_low"], y1=bar["price_high"],
            fillcolor=rgba_from_hex(ch, 0.24 + 0.34 * it),
            line_width=0, layer="below", row=1, col=1)

    # ── 价格参考线 ───────────────────────────────────────────────────────────
    lp = snapshot.last_price or (candles[-1].close if candles else None)
    if lp:
        fig.add_hline(y=lp, line_color="#f8d35e", line_dash="dot",
                      line_width=1.2, row=1, col=1)
    if snapshot.mark_price:
        fig.add_hline(y=snapshot.mark_price, line_color="#8fd3ff",
                      line_dash="dash", line_width=1, row=1, col=1)
    sp = getattr(snapshot, "spot_price", None)
    if sp:
        fig.add_hline(y=sp, line_color="#a8ff78", line_dash="dashdot",
                      line_width=1.2, row=1, col=1)

    # ── 成交量（主动买卖颜色区分）────────────────────────────────────────────
    has_taker = df["tb"].notna().sum() > len(df) * 0.3
    if has_taker:
        # 已知主动买量：绿=主动买，红=主动卖
        buy_vol  = df["tb"].fillna(0)
        sell_vol = (df["v"] - buy_vol).clip(lower=0)
        fig.add_trace(go.Bar(x=df["ts"], y=buy_vol,  name="主动买",
                             marker_color="#1dc796", opacity=0.7), row=2, col=1)
        fig.add_trace(go.Bar(x=df["ts"], y=sell_vol, name="主动卖",
                             marker_color="#ff6868", opacity=0.7), row=2, col=1)
    else:
        # 用K线方向近似主动方向
        vcols = [up if c >= o else dn for o, c in zip(df["o"], df["c"])]
        fig.add_trace(go.Bar(x=df["ts"], y=df["v"],
                             marker_color=vcols, name="成交量", opacity=0.6), row=2, col=1)

    # ── RSI ──────────────────────────────────────────────────────────────────
    if show_rsi and len(df) >= 14:
        rsi = _calc_rsi(df["c"], 14)
        rsi_colors = [
            "#ff4444" if v >= 70 else "#44cc88" if v <= 30 else "#8888ff"
            for v in rsi.fillna(50)
        ]
        fig.add_trace(go.Scatter(
            x=df["ts"], y=rsi, mode="lines", name="RSI(14)",
            line=dict(color="#c084fc", width=1.5)), row=3, col=1)
        # Overbought / oversold zones
        fig.add_hrect(y0=70, y1=100, fillcolor="rgba(255,68,68,0.07)",
                      line_width=0, row=3, col=1)
        fig.add_hrect(y0=0, y1=30, fillcolor="rgba(68,204,136,0.07)",
                      line_width=0, row=3, col=1)
        fig.add_hline(y=70, line_color="rgba(255,68,68,0.4)",
                      line_dash="dash", line_width=0.8, row=3, col=1)
        fig.add_hline(y=30, line_color="rgba(68,204,136,0.4)",
                      line_dash="dash", line_width=0.8, row=3, col=1)
        fig.add_hline(y=50, line_color="rgba(255,255,255,0.15)",
                      line_dash="dot",  line_width=0.6, row=3, col=1)

    # ── Layout ───────────────────────────────────────────────────────────────
    chart_h = 820 if show_rsi else 720
    fig.update_layout(
        height=chart_h,
        margin=dict(l=12, r=12, t=62, b=12),
        paper_bgcolor="rgba(14,22,35,0.56)",
        plot_bgcolor="rgba(255,255,255,0.045)",
        font=dict(color="#f6f9ff", family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="Price Structure & Liquidity  ·  价格结构与流动性",
                   x=0.02, y=0.98, xanchor="left",
                   font=dict(size=19, color="#f8fbff")),
        xaxis_rangeslider_visible=False,
        barmode="stack",
        legend=dict(orientation="h", y=1.04, x=0.5, xanchor="center",
                    font=dict(size=10), bgcolor="rgba(0,0,0,0)"),
    )
    fig.update_xaxes(showgrid=False, zeroline=False)
    fig.update_yaxes(showgrid=True, gridcolor="rgba(255,255,255,0.08)", side="right")
    if show_rsi:
        fig.update_yaxes(range=[0, 100], row=3, col=1)
    return fig

def build_snapshot_frame(snapshots):
    rows=[]
    for s in snapshots:
        sp_bps = s.spot_perp_spread_bps if hasattr(s,'spot_perp_spread_bps') else None
        rows.append({"交易所":s.exchange,"合约":s.symbol,
            "最新价":s.last_price,"现货价":getattr(s,'spot_price',None),
            "标记价":s.mark_price,"溢价%":s.premium_pct,
            "现货-合约(bps)":sp_bps,
            "持仓量":s.open_interest,"持仓金额":s.open_interest_notional,
            "费率bps":s.funding_bps,"24h成交额":s.volume_24h_notional,
            "状态":CARD_STATUS.get(s.status,s.status)})
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

def build_split_liq_tables(liq_events, limit_each=10):
    long_evts  = sorted([e for e in liq_events if e.side=="long"],  key=lambda x:x.timestamp_ms,reverse=True)[:limit_each]
    short_evts = sorted([e for e in liq_events if e.side=="short"], key=lambda x:x.timestamp_ms,reverse=True)[:limit_each]
    def to_df(evts):
        if not evts: return pd.DataFrame()
        return pd.DataFrame([{"时间":pd.to_datetime(e.timestamp_ms,unit="ms").strftime("%H:%M:%S"),
            "方向":"多头爆↓" if e.side=="long" else "空头爆↑",
            "价格":e.price,"数量":e.size,"金额$":e.notional,"来源":e.source} for e in evts])
    return to_df(long_evts), to_df(short_evts)

def build_liq_reality_split_figure(liq_events):
    fig=go.Figure()
    real_long  = [e for e in liq_events if e.side=="long"  and e.source in ("ws","rest")]
    real_short = [e for e in liq_events if e.side=="short" and e.source in ("ws","rest")]
    if real_long:
        fig.add_trace(go.Scatter(
            x=[pd.to_datetime(e.timestamp_ms,unit="ms") for e in real_long],
            y=[e.price for e in real_long], mode="markers", name="✅ 多头已爆 (真实)",
            marker=dict(color="#ff6868",size=[max(6,min(22,(e.notional or 1)/5000)) for e in real_long],
                        symbol="triangle-down",opacity=0.9)))
    if real_short:
        fig.add_trace(go.Scatter(
            x=[pd.to_datetime(e.timestamp_ms,unit="ms") for e in real_short],
            y=[e.price for e in real_short], mode="markers", name="✅ 空头已爆 (真实)",
            marker=dict(color="#1dc796",size=[max(6,min(22,(e.notional or 1)/5000)) for e in real_short],
                        symbol="triangle-up",opacity=0.9)))
    if not real_long and not real_short:
        fig.add_annotation(text="暂无爆仓数据",showarrow=False,x=0.5,y=0.5,xref="paper",yref="paper")
    fig.update_layout(height=320,margin=dict(l=12,r=12,t=52,b=10),
        paper_bgcolor="rgba(14,22,35,0.56)",plot_bgcolor="rgba(255,255,255,0.045)",
        font=dict(color="#f6f9ff"),
        title=dict(text="✅ 已发生爆仓真值 (Real Liquidations)",x=0.02,y=0.97,xanchor="left",font=dict(size=13,color="#f3f8ff")),
        legend=dict(orientation="h",yanchor="bottom",y=1.02,xanchor="right",x=1))
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True,gridcolor="rgba(255,255,255,0.08)",tickformat=".2f")
    return fig

def build_ls_gauge_html(long_pct, short_pct, label="全市场", top_long=None, top_short=None):
    if long_pct is None: long_pct=50.0
    if short_pct is None: short_pct=100-long_pct
    lp=max(0,min(100,long_pct)); sp=max(0,min(100,short_pct))
    if lp>65:   bg_l,bg_s="#1dc796","#ff6868"
    elif lp<35: bg_l,bg_s="#ff6868","#1dc796"
    else:       bg_l,bg_s="#62c2ff","#ffa94d"
    top_row=""
    if top_long is not None:
        tl=max(0,min(100,top_long)); ts_=max(0,min(100,top_short or 100-tl))
        top_row=(
            '<div style="margin-top:8px;font-size:0.75rem;color:#aac;">大户持仓</div>'
            '<div style="display:flex;width:100%;height:8px;border-radius:4px;overflow:hidden;margin-top:3px;">'
            '<div style="width:{tl:.1f}%;background:#a8ff78;"></div>'
            '<div style="width:{ts:.1f}%;background:#ff9a9a;"></div>'
            '</div>'
            '<div style="display:flex;justify-content:space-between;font-size:0.72rem;margin-top:2px;">'
            '<span style="color:#a8ff78;">{tl:.1f}%</span>'
            '<span style="color:#ff9a9a;">{ts:.1f}%</span>'
            '</div>'
        ).format(tl=tl,ts=ts_)
    return (
        '<div style="padding:12px 14px;border-radius:16px;'
        'border:1px solid rgba(255,255,255,0.13);'
        'background:rgba(255,255,255,0.06);backdrop-filter:blur(20px);margin-bottom:8px;">'
        '<div style="font-size:0.78rem;color:#bfd5f2;text-transform:uppercase;'
        'letter-spacing:0.1em;margin-bottom:6px;">{label}</div>'
        '<div style="display:flex;width:100%;height:14px;border-radius:7px;overflow:hidden;">'
        '<div style="width:{lp:.1f}%;background:{bg_l};transition:width 0.4s;"></div>'
        '<div style="width:{sp:.1f}%;background:{bg_s};transition:width 0.4s;"></div>'
        '</div>'
        '<div style="display:flex;justify-content:space-between;margin-top:4px;'
        'font-size:0.85rem;font-weight:600;">'
        '<span style="color:{bg_l};">多头 {lp:.1f}%</span>'
        '<span style="color:{bg_s};">空头 {sp:.1f}%</span>'
        '</div>'
        '{top_row}'
        '</div>'
    ).format(label=label,lp=lp,sp=sp,bg_l=bg_l,bg_s=bg_s,top_row=top_row)

def build_oi_change_visual(oi_pts, window=20):
    if not oi_pts or len(oi_pts)<2:
        return '<div style="color:#aaa;font-size:0.85rem;">等待OI数据…</div>'
    recent=oi_pts[-min(window,len(oi_pts)):]
    vals=[p.open_interest_notional or p.open_interest or 0 for p in recent]
    if not vals or vals[0]==0: return ""
    total_change=vals[-1]-vals[0]
    pct_change=total_change/vals[0]*100 if vals[0] else 0
    max_v=max(vals); min_v=min(vals); rng=max_v-min_v
    arrow="▲" if total_change>0 else "▼"
    color="#1dc796" if total_change>0 else "#ff6868"
    bars_html=""
    for v in vals:
        h=int((v-min_v)/rng*28) if rng>0 else 14; h=max(4,h)
        c="#1dc796" if v>=vals[0] else "#ff6868"
        bars_html+=(
            '<div style="width:4px;height:{h}px;background:{c};'
            'border-radius:2px;display:inline-block;margin:0 1px;vertical-align:bottom;"></div>'
        ).format(h=h,c=c)
    val_label=fc(vals[-1]); pct_str="{:+.2f}%".format(pct_change)
    return (
        '<div style="padding:10px 14px;border-radius:14px;'
        'border:1px solid rgba(255,255,255,0.12);'
        'background:rgba(255,255,255,0.05);margin-bottom:6px;">'
        '<div style="font-size:0.72rem;color:#bfd5f2;text-transform:uppercase;'
        'letter-spacing:0.1em;">OI变化 近{n}采样</div>'
        '<div style="margin:6px 0 4px;display:flex;align-items:flex-end;gap:1px;">{bars}</div>'
        '<div style="font-size:1.0rem;font-weight:700;color:{color};">{arrow} {pct}'
        '<span style="font-size:0.8rem;font-weight:400;color:#aac;margin-left:8px;">{val}</span>'
        '</div>'
        '</div>'
    ).format(n=len(recent),bars=bars_html,color=color,arrow=arrow,pct=pct_str,val=val_label)

def render_spot_perp_alerts(alerts, max_show=20):
    if not alerts:
        st.info("暂无现货-合约乖离告警。等待现货WS连接后自动检测。"); return
    for a in sorted(alerts, key=lambda x:x.timestamp_ms, reverse=True)[:max_show]:
        label, color = SPOT_PERP_ALERT_LABELS.get(a.alert_type, ("📢 告警","#aaa"))
        ts=pd.to_datetime(a.timestamp_ms,unit="ms").strftime("%H:%M:%S")
        extra=""
        if a.spread_bps is not None: extra+=f" | 价差 {a.spread_bps:+.1f}bps"
        if a.oi_change_pct is not None: extra+=f" | OI {a.oi_change_pct:+.2f}%"
        sev_border = {"high":"rgba(255,68,68,0.5)","medium":"rgba(255,165,0,0.5)"}.get(a.severity,"rgba(98,194,255,0.3)")
        msg_esc = a.message.replace('"','&quot;')
        st.markdown(
            '<div style="padding:10px 14px;border-radius:12px;margin:4px 0;'
            'border-left:4px solid {bc};border:1px solid {bc};'
            'background:rgba(255,255,255,0.04);">'
            '<span style="color:{col};font-weight:700;font-size:0.88rem;">{lbl}</span>'
            '<span style="color:#aac;font-size:0.76rem;margin-left:8px;">{ts}{extra}</span><br/>'
            '<span style="color:#e0e8ff;font-size:0.84rem;">{msg}</span>'
            '</div>'.format(bc=sev_border,col=color,lbl=label,ts=ts,extra=extra,msg=msg_esc),
            unsafe_allow_html=True)

def build_spot_perp_realtime_figure(spread_hist_by_exchange):
    fig=go.Figure()
    colors={"binance":"#f0b90b","bybit":"#e6a817","okx":"#1267fe"}
    names={"binance":"Binance","bybit":"Bybit","okx":"OKX"}
    has_data=False
    for ek,pts in spread_hist_by_exchange.items():
        if not pts: continue
        has_data=True
        ts=[pd.to_datetime(p.timestamp_ms,unit="ms") for p in pts]
        bps=[p.spread_bps for p in pts]
        fig.add_trace(go.Scatter(x=ts,y=bps,mode="lines",name=names.get(ek,ek),
            line=dict(color=colors.get(ek,"#aaa"),width=1.8)))
    if not has_data:
        fig.add_annotation(text="等待现货WS连接…",showarrow=False,x=0.5,y=0.5,xref="paper",yref="paper",font=dict(color="#aaa"))
    fig.add_hline(y=0,line_color="rgba(255,255,255,0.3)",line_dash="dot",line_width=1)
    fig.update_layout(height=280,margin=dict(l=12,r=12,t=52,b=10),
        paper_bgcolor="rgba(14,22,35,0.56)",plot_bgcolor="rgba(255,255,255,0.045)",
        font=dict(color="#f6f9ff",family="SF Pro Display, Segoe UI, sans-serif"),
        title=dict(text="现货-合约实时价差 Spot-Perp Spread (bps)",x=0.02,y=0.97,xanchor="left",font=dict(size=14,color="#f3f8ff")),
        legend=dict(orientation="h",yanchor="bottom",y=1.02,xanchor="right",x=1))
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True,gridcolor="rgba(255,255,255,0.08)",zeroline=True,zerolinecolor="rgba(255,255,255,0.2)")
    return fig

def build_binance_oi_perp_figure(oi_points, long_short_data, taker_data):
    fig=make_subplots(rows=3,cols=1,shared_xaxes=True,vertical_spacing=0.04,row_heights=[0.45,0.3,0.25],
        subplot_titles=("持仓量 OI (Notional)","多空账户比 L/S Account Ratio","Taker 买卖量比"))
    if oi_points:
        ts=[pd.to_datetime(p.timestamp_ms,unit="ms") for p in oi_points]
        oi_vals=[p.open_interest_notional or p.open_interest or 0 for p in oi_points]
        colors=[]
        for i,v in enumerate(oi_vals):
            if i==0: colors.append("#62c2ff")
            else: colors.append("#1dc796" if v>=oi_vals[i-1] else "#ff6868")
        fig.add_trace(go.Bar(x=ts,y=oi_vals,marker_color=colors,name="OI",opacity=0.85),row=1,col=1)
        fig.add_trace(go.Scatter(x=ts,y=oi_vals,mode="lines",line=dict(color="#62c2ff",width=1.5),name="OI趋势",showlegend=False),row=1,col=1)
    if long_short_data:
        ts2=[pd.to_datetime(d["timestamp_ms"],unit="ms") for d in long_short_data]
        fig.add_trace(go.Scatter(x=ts2,y=[d.get("global_long_pct") for d in long_short_data],mode="lines",name="全市场多头%",line=dict(color="#1dc796",width=2)),row=2,col=1)
        fig.add_trace(go.Scatter(x=ts2,y=[d.get("global_short_pct") for d in long_short_data],mode="lines",name="全市场空头%",line=dict(color="#ff6868",width=2)),row=2,col=1)
        fig.add_trace(go.Scatter(x=ts2,y=[d.get("top_long_pct") for d in long_short_data],mode="lines",name="大户多头%",line=dict(color="#a8ff78",width=1.5,dash="dash")),row=2,col=1)
        fig.add_trace(go.Scatter(x=ts2,y=[d.get("top_short_pct") for d in long_short_data],mode="lines",name="大户空头%",line=dict(color="#ff9a9a",width=1.5,dash="dash")),row=2,col=1)
        fig.add_hline(y=50,line_color="rgba(255,255,255,0.25)",line_dash="dot",row=2,col=1)
    if taker_data:
        ts3=[pd.to_datetime(d["timestamp_ms"],unit="ms") for d in taker_data]
        fig.add_trace(go.Bar(x=ts3,y=[d.get("buy_vol") or 0 for d in taker_data],name="主动买量",marker_color="#1dc796",opacity=0.8),row=3,col=1)
        fig.add_trace(go.Bar(x=ts3,y=[-(d.get("sell_vol") or 0) for d in taker_data],name="主动卖量",marker_color="#ff6868",opacity=0.8),row=3,col=1)
    fig.update_layout(height=580,margin=dict(l=12,r=12,t=60,b=12),
        paper_bgcolor="rgba(14,22,35,0.56)",plot_bgcolor="rgba(255,255,255,0.045)",
        font=dict(color="#f6f9ff",family="SF Pro Display, Segoe UI, sans-serif"),
        barmode="overlay",legend=dict(orientation="h",yanchor="bottom",y=1.02,xanchor="right",x=1))
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True,gridcolor="rgba(255,255,255,0.07)",tickformat=".2s")
    return fig


# ── Sidebar ────────────────────────────────────────────────────────────────────
preset_coin = st.sidebar.selectbox("常用币种", POPULAR_COINS, index=0)
custom_coin = st.sidebar.text_input("自定义币种", value="", placeholder="如 PEPE / TAO")
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
    trade_limit    = st.slider("成交流条数", 100, 2000, 500, 100)
    ratio_limit    = st.slider("多空比历史条数", 20, 200, 80, 10)
    refresh_secs   = st.slider("基础刷新秒数", 1, 10, 2, 1)
    smart_refresh  = st.checkbox("🧠 智能刷新（自动加速）", value=True, key="smart_refresh")
    sample_secs    = st.slider("持仓采样秒数", 5, 60, 15, 5)
    req_timeout    = st.slider("请求超时秒数", 5, 20, 10, 1)
    st.markdown("---")
    st.subheader("📊 K线技术指标")
    show_ma   = st.checkbox("均线 MA5/20/60",      value=True,  key="ind_ma")
    show_bb   = st.checkbox("布林带 BB(20,2)",      value=False, key="ind_bb")
    show_rsi  = st.checkbox("RSI(14)",              value=False, key="ind_rsi")
    show_macd = st.checkbox("MACD(12,26,9)",        value=False, key="ind_macd")
    show_atr  = st.checkbox("ATR(14) 真实波动率",   value=False, key="ind_atr")
    show_levels = st.checkbox("价格关口标注",        value=True,  key="show_levels")
    st.markdown("---")
    st.subheader("📡 多币种轮巡")
    watchlist_input = st.text_input("监控币种（逗号分隔）", value=",".join(WATCHLIST_DEFAULT))
    watchlist_coins = [c.strip().upper() for c in watchlist_input.split(",") if c.strip()]
    st.markdown("---")
    st.subheader("合约映射")
    bybit_sym   = st.text_input("Bybit 合约",       key="symbol_bybit")
    binance_sym = st.text_input("Binance 合约",     key="symbol_binance")
    okx_sym     = st.text_input("OKX 合约",         key="symbol_okx")
    hyper_sym   = st.text_input("Hyperliquid 币种", key="symbol_hyperliquid")
    col1, col2 = st.columns(2)
    restore = col1.button("恢复默认")
    restart = col2.button("重连流")
    if st.button("清空缓存"): st.cache_data.clear()

if restore:
    for k in EXCHANGE_ORDER: st.session_state[f"symbol_{k}"] = base_defaults[k]
    st.rerun()

symbol_map = {
    "bybit":bybit_sym.strip().upper(),"binance":binance_sym.strip().upper(),
    "okx":okx_sym.strip().upper(),"hyperliquid":hyper_sym.strip().upper()}
service = resolve_service(symbol_map, req_timeout, sample_secs, restart)

# ── 资金费率倒计时 ─────────────────────────────────────────────────────────────
_fd_secs, _fd_label, _fd_near, _fd_very_near = get_funding_countdown()
if _fd_very_near:
    _funding_pill = f"⚠️ 结算 {_fd_label}"
elif _fd_near:
    _funding_pill = f"🔔 结算 {_fd_label}"
else:
    _funding_pill = f"💰 费率结算 {_fd_label}"

# Auto-notify near settlement
if _fd_near and _HAS_PUSH_STORAGE:
    _notif_key = f"funding_notified_{_fd_secs // 60}"
    if not st.session_state.get(_notif_key):
        try:
            get_notifier().send_raw(
                "funding_settlement",
                f"⏰ 资金费率即将结算！距结算还有 {_fd_label}",
                severity="medium"
            )
        except Exception:
            pass
        st.session_state[_notif_key] = True

# ── 智能刷新计算（必须在 Hero banner 之前）──────────────────────────────────────
_smart_refresh = st.session_state.get("smart_refresh", True)
_effective_refresh = refresh_secs
if _smart_refresh:
    try:
        _snaps_for_refresh = service.current_snapshots() if service else []
        _effective_refresh = compute_smart_refresh(_snaps_for_refresh, refresh_secs)
    except Exception:
        _effective_refresh = refresh_secs

# ── Hero banner ───────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="hero-shell">
<div class="hero-kicker">Liquid Glass Flow Desk · v5</div>
<div class="hero-title">{base_coin} 多交易所流动性终端</div>
<div class="hero-sub">全市场首页 · OI/爆仓/Funding异动榜 · 爆仓中心(5视角) · 盘口中心 · 告警中心(可筛选/静音) · 合成信号 · 三所Spot WS · 事件回放</div>
<div class="helper-bar">
<div class="helper-pill">主图 {EXCHANGE_TITLES[selected_exchange]}</div>
<div class="helper-pill">周期 {interval}</div>
<div class="helper-pill" id="refresh-pill">刷新 {_effective_refresh}s{"⚡" if _smart_refresh and _effective_refresh < refresh_secs else ""}</div>
<div class="helper-pill" id="funding-pill">{_funding_pill}</div>
<div class="helper-pill">监控 {len(watchlist_coins)} 币</div>
</div></div>""", unsafe_allow_html=True)


# ── Main fragment ──────────────────────────────────────────────────────────────

@st.fragment(run_every=_effective_refresh)
def render_terminal():
    # Read sidebar indicator flags from session state (set outside fragment)
    show_ma   = st.session_state.get("ind_ma",   True)
    show_bb   = st.session_state.get("ind_bb",   False)
    show_rsi  = st.session_state.get("ind_rsi",  False)
    show_macd = st.session_state.get("ind_macd", False)
    show_atr  = st.session_state.get("ind_atr",  False)
    # Lazy-import center modules INSIDE the fragment to avoid early @st.cache_data trigger
    from homepage     import render_homepage
    from liq_center   import render_liq_center
    from ob_center    import render_ob_center
    from alert_center import render_alert_center

    snapshots   = service.current_snapshots()
    ok_snaps    = [s for s in snapshots if s.status=="ok"]

    # P1: 每次渲染周期将 UI 调节的权重同步到 service
    _cw = st.session_state.get("composite_weights")
    if _cw and hasattr(service, "set_composite_weights"):
        try:
            service.set_composite_weights(_cw)
        except Exception:
            pass
    status_text = " · ".join(status_caption(s) for s in snapshots)
    st.markdown(
        '<div class="status-strip">{}</div>'.format(status_text),
        unsafe_allow_html=True)

    alert_events    = service.get_alert_events()
    sp_alerts       = service.get_spot_perp_alerts()
    spread_hist_all = service.get_all_spread_history()

    # Alert badge row
    if alert_events or sp_alerts:
        html = ""
        for e in list(alert_events)[-3:]:
            html += '<span class="alert-badge">🔔 {}</span> '.format(e.name)
        for a in list(sp_alerts)[-3:]:
            lbl = SPOT_PERP_ALERT_LABELS.get(a.alert_type, ("📢","#aaa"))[0]
            sc  = "#ff6868" if a.severity in ("high","strong") else "#ffa94d"
            html += (
                '<span class="alert-badge" style="border-color:rgba(255,100,50,0.5);color:{sc};">'
                '{lbl} {ex}</span> '
            ).format(sc=sc, lbl=lbl, ex=a.exchange)
        st.markdown('<div style="margin-bottom:0.7rem;">{}</div>'.format(html), unsafe_allow_html=True)

    if not ok_snaps:
        st.error("当前没有交易所返回可用数据，请检查合约名或网络。"); return

    snap_by_key = dict(zip(EXCHANGE_ORDER, snapshots))
    sel_snap    = snap_by_key[selected_exchange]
    sel_sym     = symbol_map[selected_exchange]

    # ── Data loading ──────────────────────────────────────────────────────────
    candles      = load_candles(selected_exchange, sel_sym, interval, candle_limit, req_timeout)
    local_book   = service.get_local_book(selected_exchange)
    orderbook    = service.get_local_book_levels(selected_exchange, depth_limit) if local_book.is_ready else load_orderbook(selected_exchange, sel_sym, depth_limit, req_timeout)
    backfill_oi  = load_oi_backfill(selected_exchange, sel_sym, interval, max(60,candle_limit//2), req_timeout)
    merged_oi    = merge_oi_points(backfill_oi, service.get_oi_history(selected_exchange))
    ref_price    = sel_snap.last_price or sel_snap.mark_price or (candles[-1].close if candles else None)
    heat_bars    = aggregate_heat_bars(orderbook, ref_price, heat_window, heat_buckets, heat_bars_side)
    book_sum     = summarize_orderbook(orderbook, ref_price)
    sess_liqs    = service.get_liquidation_history(selected_exchange)
    rest_liqs    = load_liquidations(selected_exchange, sel_sym, liq_limit, req_timeout)
    liq_events   = merge_liquidation_events(rest_liqs, sess_liqs)
    liq_metrics  = build_liquidation_metrics(liq_events, sel_snap.timestamp_ms or int(time.time()*1000), liq_window_min)
    liq_hf       = build_probability_heatmap_frame(candles, orderbook, sel_snap, "liquidation", ref_price, risk_window, risk_buckets)
    tp_hf        = build_probability_heatmap_frame(candles, orderbook, sel_snap, "tp",          ref_price, risk_window, risk_buckets)
    stop_hf      = build_probability_heatmap_frame(candles, orderbook, sel_snap, "stop",        ref_price, risk_window, risk_buckets)
    mbo_frame    = build_mbo_profile_frame(orderbook, ref_price, mbo_rows)
    ws_trades    = service.get_trade_history(selected_exchange)
    rest_trades  = load_trades(selected_exchange, sel_sym, min(trade_limit,500), req_timeout)
    all_trades   = sorted(set((t.timestamp_ms,t.price,t.size) for t in ws_trades+rest_trades), key=lambda x:x[0])
    all_trades_obj = {(t.timestamp_ms,t.price,t.size): t for t in ws_trades+rest_trades}
    unique_trades = [all_trades_obj[k] for k in all_trades]
    ws_cvd = service.get_cvd_history(selected_exchange)
    if len(ws_cvd) >= 20: cvd_points = ws_cvd
    elif any(c.taker_buy_volume is not None for c in candles): cvd_points = build_cvd_from_candles(candles)
    elif unique_trades: cvd_points = build_cvd_from_trades(unique_trades[-trade_limit:], bin_seconds=max(30, interval_to_millis(interval)//2000))
    else: cvd_points = []
    oi_delta_pts  = build_oi_delta_points(merged_oi, candles)
    oi_delta_summ = build_oi_delta_summary(oi_delta_pts, lookback=20)
    top_trader      = load_top_trader("binance", symbol_map["binance"], interval, ratio_limit, req_timeout)
    global_r        = load_global_ratio("binance", symbol_map["binance"], interval, ratio_limit, req_timeout)
    bybit_ratio_raw = load_top_trader("bybit", symbol_map["bybit"], interval, ratio_limit, req_timeout)
    ls_count_data   = load_ls_count(symbol_map["binance"], interval, ratio_limit, req_timeout)
    taker_ratio_data= load_taker_ratio(symbol_map["binance"], interval, ratio_limit, req_timeout)
    spot_prices = {}; spot_volumes = {}
    for ek in ("bybit","binance","okx"):
        sp_ws = service._spot_price.get(ek)
        if sp_ws:
            spot_prices[EXCHANGE_TITLES[ek]] = sp_ws
            sv_ws = service._spot_vol24h.get(ek)
            if sv_ws: spot_volumes[EXCHANGE_TITLES[ek]] = sv_ws
        else:
            sp = load_spot_ticker(ek, base_coin, req_timeout)
            if sp: spot_prices[EXCHANGE_TITLES[ek]]=sp[0]; spot_volumes[EXCHANGE_TITLES[ek]]=sp[1]
    futures_oi_list = load_futures_oi_list("binance", base_coin, req_timeout)
    if not futures_oi_list: futures_oi_list = load_futures_oi_list("bybit", base_coin, req_timeout)
    all_liq_by_exch = {}
    for ek in EXCHANGE_ORDER:
        all_liq_by_exch[ek] = merge_liquidation_events(
            load_liquidations(ek, symbol_map[ek], liq_limit, req_timeout),
            service.get_liquidation_history(ek))
    iceberg_alerts   = service.get_iceberg_alerts()
    liq_gaps         = service.get_liquidity_gaps()
    confirmed_alerts = service.get_confirmed_alerts()
    alert_timeline   = service.get_alert_timeline()

    # ── v6: 推送新确认告警到 Telegram / 浏览器 ──────────────────────────────────
    if _HAS_PUSH_STORAGE:
        try:
            _notifier = get_notifier()
            _seen_alerts = st.session_state.get("_seen_alert_ids", set())
            _new_alerts  = [a for a in confirmed_alerts if a.alert_id not in _seen_alerts]
            for _al in _new_alerts[-5:]:   # max 5 per refresh
                _notifier.send_alert(_al, coin=base_coin)
                # Write to DB
                try:
                    _db = getattr(service, '_archive_db_path', 'market_data.db')
                    init_db(_db)
                    insert_alert_history(_al.alert_type, _al.exchange, _al.severity,
                                         _al.message, _al.score, db_path=_db)
                except Exception:
                    pass
            st.session_state["_seen_alert_ids"] = _seen_alerts | {a.alert_id for a in confirmed_alerts}
        except Exception:
            pass
    liq_clusters_v2  = service.get_liq_clusters_v2()
    composite_sigs_by_ex = {ek: service.get_composite_signals(ek) for ek in EXCHANGE_ORDER}
    ob_quality_sel   = service.get_ob_quality_history(selected_exchange)
    ob_delta_sel     = service.get_ob_delta_history(selected_exchange)
    fake_walls_sel   = service.get_fake_walls(selected_exchange)
    absorb_sel       = service.get_absorption_events(selected_exchange)
    recorded_frames  = service.get_recorded_frames()
    latest_comp_sigs = composite_sigs_by_ex.get(selected_exchange, [])
    latest_comp_sig  = latest_comp_sigs[-1] if latest_comp_sigs else None
    mref   = pd.Series([s.mark_price for s in ok_snaps if s.mark_price]).median()
    total_oi  = sum(s.open_interest_notional or 0 for s in ok_snaps)
    avg_fund  = pd.Series([s.funding_bps for s in ok_snaps if s.funding_bps is not None]).mean() if any(s.funding_bps for s in ok_snaps) else None
    sentiment = build_market_sentiment_summary(ok_snaps, oi_delta_summ, liq_metrics, cvd_points, top_trader)

    # ── Top KPI row ────────────────────────────────────────────────────────────
    c = st.columns(6)
    c[0].metric("在线交易所", str(len(ok_snaps)))
    c[1].metric("市场参考价", fp(mref))
    c[2].metric("持仓总金额", fc(total_oi))
    c[3].metric("主图费率",   frate(sel_snap.funding_rate))
    c[4].metric("OI四象限",  oi_delta_summ.get("dominant_cn","-"))
    c[5].metric("CVD累积",   fc(cvd_points[-1].cvd if cvd_points else None))
    c2 = st.columns(5)
    c2[0].metric("买盘挂单额", fc(book_sum.get("bid_notional")))
    c2[1].metric("卖盘挂单额", fc(book_sum.get("ask_notional")))
    c2[2].metric("盘口失衡",   fpct(book_sum.get("imbalance_pct")))
    c2[3].metric("价差 Spread", fbps(book_sum.get("spread_bps") or (local_book.spread_bps() if local_book.is_ready else None)))
    c2[4].metric("OI速率/min", fc(oi_delta_summ.get("avg_velocity")))

    if sentiment:
        html = "".join(
            '<span class="signal-card">📊 <b>{k}</b>：{v}</span>'.format(k=k,v=v)
            for k,v in sentiment.items())
        st.markdown('<div style="margin:0.3rem 0 0.7rem;">{}</div>'.format(html), unsafe_allow_html=True)

    # Exchange cards — Binance first
    cards = st.columns(len(EXCHANGE_ORDER_UI))
    for col, ek in zip(cards, EXCHANGE_ORDER_UI):
        s = snap_by_key[ek]
        sp_bps = s.spot_perp_spread_bps if hasattr(s,'spot_perp_spread_bps') else None
        delta_str = "OI {}".format(fc(s.open_interest_notional))
        if sp_bps is not None: delta_str += " | 现货差 {:+.1f}bps".format(sp_bps)
        col.metric(s.exchange, fp(s.last_price), delta=delta_str)

    # ── Tabs ──────────────────────────────────────────────────────────────────
    # ══ 两行 Tab 导航 ══════════════════════════════════════════════════════════
    # 第一行：原有功能（Tab 0-16）
    # 第二行：v6 新增功能（Tab 17-20）
    tabs = st.tabs([
        "🏠 全市场首页",
        "📈 深度终端",
        "💧 CVD主动买卖",
        "🔲 本地WS订单簿",
        "👥 OI四象限+速率",
        "🎯 合约情绪真值",
        "🔗 Spot-Perp 价差",
        "📐 Basis+期限结构",
        "💥 爆仓中心",
        "🔍 冰山单+流动性缺口",
        "🧠 合成信号引擎",
        "📋 盘口中心",
        "🔔 告警中心",
        "📼 回放复盘",
        "🌐 全市场对比",
        "📡 多币种轮巡",
        "⚙️ 预警规则",
        "⛓️ HL链上中心",
        "🧬 信号增强中心",
        "🔧 调试",
    ])

    # ══ TAB 0: 全市场首页 ══
    with tabs[0]:
        scan_coins_raw = st.session_state.get("homepage_coins_val", ",".join(_SCAN_COINS[:25]))
        scan_coins = [c.strip().upper() for c in scan_coins_raw.split(",") if c.strip()]
        render_homepage(scan_coins, timeout=req_timeout)
        # Hot coins detection
        try:
            from homepage import build_coin_rows
            from exchanges import build_market_scan_client
            _scan_client = build_market_scan_client(req_timeout)
            _raw = _scan_client.fetch_market_batch(scan_coins[:20], max_workers=6)
            _mrows = build_coin_rows(_raw)
            # P1: 存入 session_state 供「市场热力扫描」Tab 使用
            st.session_state["last_market_scan_rows"] = _mrows
            _hot = detect_hot_coins(_mrows, top_n=5)
            if _hot:
                render_section("🔥 热点异动币种  ·  Hot Coins Auto-Detection", kicker="Alert")
                _hc_cols = st.columns(len(_hot))
                for _hi, _hc in enumerate(_hot):
                    _dir_icon = "🟢" if _hc["direction"]=="bull" else "🔴" if _hc["direction"]=="bear" else "⚪"
                    with _hc_cols[_hi]:
                        st.metric(_hc["coin"], f"{_hc['score']:.0f}分",
                                  delta=_hc["reason"][:30], delta_color="off")
                        st.caption(f"{_dir_icon} {_hc['reason']}")
        except Exception:
            pass

    # ══ TAB 1: 深度终端 ══
    with tabs[1]:
        if latest_comp_sig:
            _sc=latest_comp_sig.composite_score; _col=latest_comp_sig.signal_color
            _spr=sel_snap.spot_perp_spread_bps if hasattr(sel_snap,"spot_perp_spread_bps") else None
            spr_str = "现货差 <b>{}</b>".format(fbps(_spr)) if _spr is not None else ""
            st.markdown(
                '<div style="padding:10px 18px;border-radius:18px;margin-bottom:10px;'
                'border:1px solid {col}40;background:rgba(255,255,255,0.05);'
                'display:flex;align-items:center;gap:20px;flex-wrap:wrap;">'
                '<div style="font-size:1.4rem;font-weight:800;color:{col};">{lbl}</div>'
                '<div style="font-size:0.82rem;color:#aac;">'
                '合成分 <b style="color:{col};">{sc:+.2f}</b> &nbsp;|&nbsp;'
                '置信度 <b>{conf:.0%}</b> &nbsp;|&nbsp;'
                'Funding <b>{fr}</b> &nbsp;|&nbsp; {spr}'
                '</div></div>'.format(
                    col=_col, lbl=latest_comp_sig.signal_label, sc=_sc,
                    conf=latest_comp_sig.confidence, fr=frate(sel_snap.funding_rate), spr=spr_str),
                unsafe_allow_html=True)
        render_section(f"{sel_snap.exchange} {sel_sym}", "K线 · 均线 · 布林带 · RSI · 盘口热力区  |  绿线=现货参考价")
        left, right = st.columns([3.1,1.35], gap="large")
        with left:
            # Build chart with price level overlays
            _chart_fig = build_terminal_chart(candles, heat_bars, sel_snap, interval,
                                              show_ma=show_ma, show_bb=show_bb, show_rsi=show_rsi)
            if st.session_state.get("show_levels", True) and candles and sel_snap.last_price:
                _levels = detect_price_levels(candles, sel_snap.last_price)
                _chart_fig = build_price_levels_annotations(_chart_fig, _levels, sel_snap.last_price)
            st.plotly_chart(_chart_fig, key="pc_main", config={'displayModeBar': True, 'scrollZoom': True})
            # P1: MACD / ATR 独立子图（仅在勾选时显示）
            if show_macd or show_atr:
                _macd_atr_fig = build_macd_atr_figure(candles,
                                                       show_macd=show_macd,
                                                       show_atr=show_atr)
                if _macd_atr_fig:
                    st.plotly_chart(_macd_atr_fig, key="pc_macd_atr",
                                    config={'displayModeBar': True, 'scrollZoom': True})
        with right:
            oi_html = build_oi_change_visual(service.get_oi_history(selected_exchange))
            if oi_html: st.markdown(oi_html, unsafe_allow_html=True)
            oi_fig, oi_lbl = build_oi_figure(merged_oi)
            st.plotly_chart(oi_fig, key="pc_oi", config={'displayModeBar': True, 'scrollZoom': True})
            st.caption(oi_lbl)
            hf = build_heat_frame(heat_bars)
            if hf.empty: st.info("盘口深度不足，暂无热力条。")
            else:
                st.dataframe(hf, width='stretch', hide_index=True,
                    column_config={"挂单量":st.column_config.NumberColumn(format="%.2f"),
                        "热度":st.column_config.ProgressColumn(format="%.2f",min_value=0,max_value=1),
                        "离现价%":st.column_config.NumberColumn(format="%.2f%%")})
        render_section("资金费率对比  ·  Funding Rate", "正=多头拥挤付费；负=空头拥挤付费。")
        st.plotly_chart(build_funding_comparison_figure(ok_snaps), key="pc_funding_main", config={'displayModeBar': True, 'scrollZoom': True})
        render_section("挂单量 + 已发生爆仓  ·  Liquidity + Executed Liquidations")
        fc4 = st.columns(4)
        fc4[0].metric(f"近{liq_window_min}m爆仓额", fc(liq_metrics.get("notional")))
        fc4[1].metric(f"近{liq_window_min}m爆仓单数", str(liq_metrics.get("count",0)))
        fc4[2].metric("主导方向", liq_metrics.get("dominant") or "-")
        fc4[3].metric("四所平均费率", fbps(avg_fund))
        ll, lr = st.columns([2.1,1.35], gap="large")
        with ll: st.plotly_chart(build_liquidation_figure(liq_events), key="pc_liq_main", config={'displayModeBar': True, 'scrollZoom': True})
        with lr:
            lf = build_liquidation_frame(liq_events, limit=24)
            if lf.empty: st.info("暂无爆仓事件。")
            else: st.dataframe(lf, width='stretch', hide_index=True,
                    column_config={"价格":st.column_config.NumberColumn(format="%.2f"),
                        "数量":st.column_config.NumberColumn(format="%.4f"),
                        "名义金额":st.column_config.NumberColumn(format="%.2f")})
        render_section("Risk Maps  ·  风险区推断", "⚠️ 推断区与真实爆仓分开展示，基于模型计算。")
        hcols = st.columns(3, gap="large")
        for _hi,(hcol,fdata,title,cs,etxt) in enumerate([
            (hcols[0],liq_hf,"⚠️ 推断爆仓区",LIQUIDATION_COLORSCALE,"数据不足"),
            (hcols[1],tp_hf,"推断止盈区",TP_COLORSCALE,"数据不足"),
            (hcols[2],stop_hf,"推断止损区",STOP_COLORSCALE,"数据不足")]):
            with hcol:
                st.plotly_chart(build_heatmap_figure(fdata,title,ref_price,cs,etxt), key=f"pc_heat_{_hi}", config={'displayModeBar': True, 'scrollZoom': True})
                st.dataframe(build_heat_zone_frame(fdata), width='stretch', hide_index=True,
                    column_config={"热度":st.column_config.ProgressColumn(format="%.2f",min_value=0,max_value=1)})
        # ── 跨周期K线 ─────────────────────────────────────────────────────────
        render_section("跨周期K线联动  ·  Multi-Timeframe", kicker="MTF")
        _mtf_ivs = [iv for iv in ["5m", "15m", "1h"] if iv != interval][:2]
        _mtf_cols = st.columns(len(_mtf_ivs))
        for _mi, _miv in enumerate(_mtf_ivs):
            _msym = sel_sym
            _mc = load_candles(selected_exchange, _msym, _miv, 60, req_timeout)
            with _mtf_cols[_mi]:
                st.caption(f"**{_miv}** · {selected_exchange.capitalize()}")
                if _mc:
                    _mdf = pd.DataFrame({
                        "ts": pd.to_datetime([c.timestamp_ms for c in _mc], unit="ms"),
                        "o":  [c.open  for c in _mc],
                        "h":  [c.high  for c in _mc],
                        "l":  [c.low   for c in _mc],
                        "c":  [c.close for c in _mc],
                    })
                    _mfig = go.Figure(go.Candlestick(
                        x=_mdf["ts"], open=_mdf["o"], high=_mdf["h"],
                        low=_mdf["l"], close=_mdf["c"],
                        increasing_line_color="#1dc796", decreasing_line_color="#ff6868",
                        name=_miv, line_width=1,
                    ))
                    _mfig.update_layout(
                        height=240,
                        margin=dict(l=4, r=4, t=20, b=4),
                        paper_bgcolor="rgba(14,22,35,0.56)",
                        plot_bgcolor="rgba(255,255,255,0.045)",
                        font=dict(color="#f6f9ff", size=10),
                        xaxis_rangeslider_visible=False,
                        showlegend=False,
                    )
                    # Add MA20 to MTF charts
                    _mclose = _mdf["c"]
                    if len(_mclose) >= 20:
                        _mma = _mclose.rolling(20).mean()
                        _mfig.add_trace(go.Scatter(
                            x=_mdf["ts"], y=_mma, mode="lines",
                            line=dict(color="#62c2ff", width=1), name="MA20",
                        ))
                    st.plotly_chart(_mfig, key=f"pc_mtf_{_miv}",
                                    config={"displayModeBar": False})
                else:
                    st.info(f"无 {_miv} 数据")

        render_section("MBO Profile  ·  盘口队列画像")
        ml, mr = st.columns([2.1,1.35], gap="large")
        with ml: st.plotly_chart(build_mbo_figure(mbo_frame, ref_price), key="pc_mbo", config={'displayModeBar': True, 'scrollZoom': True})
        with mr:
            if mbo_frame.empty: st.info("盘口深度不足。")
            else: st.dataframe(mbo_frame[["方向","价格","挂单量","名义金额","盘口占比","吸收分数"]],
                width='stretch', hide_index=True,
                column_config={"价格":st.column_config.NumberColumn(format="%.2f"),
                    "盘口占比":st.column_config.ProgressColumn(format="%.2f",min_value=0,max_value=1),
                    "吸收分数":st.column_config.ProgressColumn(format="%.2f",min_value=0,max_value=1)})

    # ══ TAB 2: CVD ══
    with tabs[2]:
        render_section("CVD 累积成交量差 + 多空力量面板  ·  Bull/Bear Power", kicker="Flow")
        st.plotly_chart(build_cvd_figure(cvd_points, f"{base_coin} CVD"), key="pc_cvd", config={'displayModeBar': True, 'scrollZoom': True})

        # Bull/Bear Power Panel
        render_section("多空力量实时面板  ·  Bull/Bear Power", kicker="Power")
        _ob_levels_dict = {ek: service.get_local_book_levels(ek, 30) for ek in EXCHANGE_ORDER}
        _cvd_hist_dict  = {ek: service.get_cvd_history(ek) for ek in EXCHANGE_ORDER}
        _bb_fig = build_bull_bear_power_figure(snapshots, _ob_levels_dict, _cvd_hist_dict)
        st.plotly_chart(_bb_fig, key="pc_bb_power", config={'displayModeBar': True, 'scrollZoom': True})
        if unique_trades:
            tc1, tc2 = st.columns([1.6,1])
            with tc1:
                trows=[{"时间":pd.to_datetime(t.timestamp_ms,unit="ms").strftime("%H:%M:%S"),
                    "方向":"主动买▲" if t.side=="buy" else "主动卖▼",
                    "价格":t.price,"数量":t.size,"名义金额":t.notional,"交易所":t.exchange}
                    for t in sorted(unique_trades,key=lambda x:x.timestamp_ms,reverse=True)[:50]]
                st.dataframe(pd.DataFrame(trows), width='stretch', hide_index=True,
                    column_config={"价格":st.column_config.NumberColumn(format="%.2f"),
                        "数量":st.column_config.NumberColumn(format="%.4f"),
                        "名义金额":st.column_config.NumberColumn(format="%.2f")})
            with tc2:
                recent100=unique_trades[-100:]
                buy_v=sum(t.notional for t in recent100 if t.side=="buy")
                sell_v=sum(t.notional for t in recent100 if t.side=="sell")
                tot=buy_v+sell_v
                st.metric("近100笔主动买量", fc(buy_v))
                st.metric("近100笔主动卖量", fc(sell_v))
                st.metric("买卖比", f"{buy_v/max(tot,1)*100:.1f}% 买" if tot>0 else "-")
                st.metric("WS实时成交数", str(len(ws_trades)))
        else: st.info("等待实时成交流…")

    # ══ TAB 3: 本地WS订单簿 ══
    with tabs[3]:
        render_section("本地实时订单簿  ·  Local WebSocket Orderbook", kicker="OrderBook")
        bk_c = st.columns(5)
        bk_c[0].metric("合约WS", "✅ 已建立" if local_book.is_ready else "⏳ 初始化中")
        bk_c[1].metric("最优买价", fp(local_book.best_bid()) if local_book.is_ready else "-")
        bk_c[2].metric("最优卖价", fp(local_book.best_ask()) if local_book.is_ready else "-")
        bk_c[3].metric("实时价差", fbps(local_book.spread_bps()) if local_book.is_ready else "-")
        spot_bybit = service.get_spot_book("bybit"); spot_okx = service.get_spot_book("okx")
        bk_c[4].metric("现货WS",
            "Bybit{} OKX{}".format("✅" if spot_bybit and spot_bybit.is_ready else "⏳",
                                    "✅" if spot_okx   and spot_okx.is_ready   else "⏳"))
        st.plotly_chart(build_local_book_figure(local_book, depth=min(depth_limit,50)), key="pc_localbook", config={'displayModeBar': True, 'scrollZoom': True})

    # ══ TAB 4: OI四象限 ══
    with tabs[4]:
        render_section("OI Delta 四象限  ·  加仓减仓分析 + 速率", kicker="OI")
        oi_cols = st.columns(5)
        oi_cols[0].metric("多头加仓", f"{oi_delta_summ.get('long_add_pct',0):.1f}%")
        oi_cols[1].metric("空头加仓", f"{oi_delta_summ.get('short_add_pct',0):.1f}%")
        oi_cols[2].metric("空头回补", f"{oi_delta_summ.get('short_cover_pct',0):.1f}%")
        oi_cols[3].metric("多头减仓", f"{oi_delta_summ.get('long_unwind_pct',0):.1f}%")
        oi_cols[4].metric("平均OI速率/min", fc(oi_delta_summ.get("avg_velocity")))
        st.plotly_chart(build_oi_delta_figure(oi_delta_pts), key="pc_oi_delta", config={'displayModeBar': True, 'scrollZoom': True})
        st.plotly_chart(build_oi_velocity_figure(oi_delta_pts), key="pc_oi_velocity", config={'displayModeBar': True, 'scrollZoom': True})

    # ══ TAB 5: 合约情绪真值 (v8 方向1) ══
    with tabs[5]:
        render_section(
            "合约情绪真值层  ·  Contract Sentiment Truth",
            "严格区分已确认 vs 未确认 | Binance 4端点并发 | Bybit Taker方向",
            kicker="Sentiment v8"
        )

        # 数据源状态条
        _src_cols = st.columns(4)
        _src_cols[0].metric("Binance", "4端点 ✅ 已确认", help="globalLS / topLS账户 / topLS持仓 / takerlongshortRatio")
        _src_cols[1].metric("Bybit", "buyRatio ✅ 已确认", help="Taker方向主动买比，非持仓比")
        _src_cols[2].metric("OKX", "⚠️ 暂不支持", help="OKX全市场多空比API暂不稳定，已标注，不报错")
        _src_cols[3].metric("Hyperliquid", "ℹ️ 无全市场数据", help="HL为链上去中心化，无全市场多空比端点")

        # 加载当前情绪点
        _sent_pt = load_contract_sentiment(
            symbol_map["binance"], symbol_map["bybit"], interval, req_timeout)

        # session_state 历史累积
        if "sentiment_history" not in st.session_state:
            st.session_state["sentiment_history"] = []
        if _sent_pt is not None:
            hist = st.session_state["sentiment_history"]
            if not hist or hist[-1].timestamp_ms != _sent_pt.timestamp_ms:
                hist.append(_sent_pt)
                st.session_state["sentiment_history"] = hist[-200:]  # 保留最近200点
        _sent_hist = st.session_state.get("sentiment_history", [])

        # 当前情绪仪表盘
        _gauge_col, _chart_col = st.columns([1, 2])
        with _gauge_col:
            st.markdown(build_sentiment_gauge_html(_sent_pt), unsafe_allow_html=True)
            if _sent_pt:
                st.caption(f"已确认来源：{', '.join(_sent_pt.confirmed_sources) or '等待数据…'}")
        with _chart_col:
            st.plotly_chart(
                build_contract_sentiment_figure(_sent_hist),
                key="pc_sentiment_v8", config={"displayModeBar": True})

        # 详细数据表
        if _sent_pt:
            with st.expander("📋 原始数值详情", expanded=False):
                _sent_rows = []
                _data_map = [
                    ("Binance 全市场账户多空比", _sent_pt.binance_global_ratio, "已确认"),
                    ("Binance 全市场多头%",     _sent_pt.binance_global_long_pct, "已确认"),
                    ("Binance 全市场空头%",     _sent_pt.binance_global_short_pct, "已确认"),
                    ("Binance 大户账户多空比",  _sent_pt.binance_top_account_ratio, "已确认"),
                    ("Binance 大户持仓多空比",  _sent_pt.binance_top_position_ratio, "已确认"),
                    ("Binance Taker买比",       _sent_pt.binance_taker_buy_ratio, "已确认"),
                    ("Binance Taker买量",       _sent_pt.binance_taker_buy_vol, "已确认"),
                    ("Binance Taker卖量",       _sent_pt.binance_taker_sell_vol, "已确认"),
                    ("Bybit Taker买比(非持仓)", _sent_pt.bybit_taker_buy_ratio, "已确认"),
                    ("OKX 全市场多空比",        None, "暂不支持"),
                    ("Hyperliquid 全市场数据",  None, "无此数据"),
                ]
                for name, val, status in _data_map:
                    _sent_rows.append({
                        "数据项": name,
                        "数值": f"{val:.4f}" if val is not None else "—",
                        "状态": status,
                    })
                st.dataframe(pd.DataFrame(_sent_rows), hide_index=True, width=600)

    # ══ TAB 6: Spot-Perp 价差 ══
    with tabs[6]:
        render_section("现货-合约实时价差  ·  Spot-Perp Spread & Lead/Lag", kicker="SpotPerp")
        spm_cols=st.columns(4)
        for i,ek in enumerate(("binance","bybit","okx")):
            s=snap_by_key.get(ek)
            if s:
                sp_bps=s.spot_perp_spread_bps if hasattr(s,'spot_perp_spread_bps') else None
                spm_cols[i].metric(f"{EXCHANGE_TITLES[ek]} 现货-合约价差",
                    fbps(sp_bps) if sp_bps is not None else "等待WS…",
                    delta=f"现货 {fp(getattr(s,'spot_price',None))}" if getattr(s,'spot_price',None) else None)
        spm_cols[3].metric("WS状态",
            "Bybit{} OKX{} BN(idx){}".format(
                "✅" if service._spot_price.get("bybit") else "⏳",
                "✅" if service._spot_price.get("okx")   else "⏳",
                "✅" if service._spot_price.get("binance") else "⏳"))
        st.plotly_chart(build_spot_perp_realtime_figure(spread_hist_all), key="pc_spread_rt", config={'displayModeBar': True, 'scrollZoom': True})
        render_section("Spot-Perp 实时告警", kicker="Alerts")
        render_spot_perp_alerts(sp_alerts)

    # ══ TAB 7: Basis ══
    with tabs[7]:
        render_section("Basis 合约溢价率  ·  Spot vs Futures Basis", kicker="Basis")
        st.plotly_chart(build_basis_figure(ok_snaps, spot_prices), key="pc_basis", config={'displayModeBar': True, 'scrollZoom': True})
        render_section("期限结构  ·  Term Structure", kicker="Term")
        st.plotly_chart(build_term_structure_figure(futures_oi_list), key="pc_term", config={'displayModeBar': True, 'scrollZoom': True})
        render_section("现货 vs 合约持仓  ·  Spot Volume vs Perp OI", kicker="SpotPerp")
        st.plotly_chart(build_spot_vs_perp_figure(ok_snaps, spot_volumes), key="pc_spotperp", config={'displayModeBar': True, 'scrollZoom': True})

    # ══ TAB 8: 爆仓中心 ══
    with tabs[8]:
        render_liq_center(all_liq_by_exch, liq_clusters_v2, coin=base_coin)

    # ══ TAB 9: 冰山单+缺口 ══
    with tabs[9]:
        render_section("冰山单检测  ·  Iceberg Order Detection", kicker="Iceberg")
        st.plotly_chart(build_iceberg_figure(iceberg_alerts), key="pc_iceberg", config={'displayModeBar': True, 'scrollZoom': True})
        render_section("流动性缺口  ·  Liquidity Gap", kicker="Gap")
        gap_df = build_liquidity_gap_frame(liq_gaps)
        if gap_df.empty: st.info("暂未检测到流动性缺口（需WebSocket订单簿建立后开始检测）。")
        else:
            st.dataframe(gap_df, width='stretch', hide_index=True,
                column_config={"消失比例":st.column_config.ProgressColumn(format="%.0%",min_value=0,max_value=1),
                    "前挂单":st.column_config.NumberColumn(format="%.0f"),
                    "后挂单":st.column_config.NumberColumn(format="%.0f")})

    # ══ TAB 10: 合成信号 ══
    with tabs[10]:
        render_section("OI + CVD + Funding + 拥挤度  ·  Composite Signal Engine", kicker="Signal")
        sig_cols = st.columns(len(EXCHANGE_ORDER))
        for sc_col, ek in zip(sig_cols, EXCHANGE_ORDER):
            sig_list=composite_sigs_by_ex.get(ek,[])
            sig=sig_list[-1] if sig_list else None
            if sig:
                sc_col.markdown(
                    '<div style="padding:10px;border-radius:14px;border:1px solid rgba(255,255,255,0.13);'
                    'background:rgba(255,255,255,0.06);text-align:center;">'
                    '<div style="font-size:0.7rem;color:#bcd;">{ex}</div>'
                    '<div style="font-size:1.1rem;font-weight:800;color:{col};">{lbl}</div>'
                    '<div style="font-size:0.78rem;color:#aac;">分 {sc:+.2f} | 信 {conf:.0%}</div>'
                    '</div>'.format(ex=EXCHANGE_TITLES.get(ek,ek), col=sig.signal_color,
                                    lbl=sig.signal_label, sc=sig.composite_score, conf=sig.confidence),
                    unsafe_allow_html=True)
            else: sc_col.metric(EXCHANGE_TITLES.get(ek,ek), "等待数据…")
        if latest_comp_sig:
            st.markdown(build_composite_radar_html(latest_comp_sig), unsafe_allow_html=True)
        st.plotly_chart(build_composite_signal_figure(composite_sigs_by_ex), key="pc_composite", config={'displayModeBar': True, 'scrollZoom': True})
        if latest_comp_sig:
            factor_rows=[
                {"因子":"价格动能","得分":f"{latest_comp_sig.price_score:+.3f}","权重":"20%"},
                {"因子":"OI方向","得分":f"{latest_comp_sig.oi_score:+.3f}","权重":"25%"},
                {"因子":"CVD流向","得分":f"{latest_comp_sig.cvd_score:+.3f}","权重":"25%"},
                {"因子":"资金费率","得分":f"{latest_comp_sig.funding_score:+.3f}","权重":"15%"},
                {"因子":"拥挤度","得分":f"{latest_comp_sig.crowd_score:+.3f}","权重":"15%"},
                {"因子":"合成总分","得分":f"{latest_comp_sig.composite_score:+.3f}","权重":"100%"},
            ]
            st.dataframe(pd.DataFrame(factor_rows), width='stretch', hide_index=True)

    # ══ TAB 11: 盘口中心 ══
    with tabs[11]:
        render_ob_center(
            quality_hist = ob_quality_sel,
            fake_walls   = fake_walls_sel,
            absorptions  = absorb_sel,
            wall_hist    = service.get_wall_life_history(selected_exchange),
            active_walls = service.get_active_walls(selected_exchange),
            collapses    = service.get_liq_collapses(selected_exchange),
            large_flows  = service.get_large_order_flow(selected_exchange),
            book         = local_book,
            exchange     = EXCHANGE_TITLES.get(selected_exchange, selected_exchange),
        )

    # ══ TAB 12: 告警中心 ══
    with tabs[12]:
        render_alert_center(confirmed_alerts, alert_timeline, recorded_frames)

    # ══ TAB 13: 回放复盘 ══
    with tabs[13]:
        render_section("回放复盘  ·  Event Replay & Review", kicker="Replay")
        rec_c = st.columns(4)
        is_rec = service.recorder_active
        rec_c[0].metric("录制状态", "🔴 录制中" if is_rec else "⏹ 已停止")
        rec_c[1].metric("已录制帧数", str(len(recorded_frames)))
        rec_c[2].metric("录制时长", f"{len(recorded_frames)}s" if recorded_frames else "0s")
        rec_c[3].metric("容量", "最多3600帧(1h)")
        btn1, btn2, _ = st.columns(3)
        if btn1.button("▶ 开始录制" if not is_rec else "⏸ 暂停录制"):
            if is_rec: service.stop_recording()
            else: service.start_recording()
            st.rerun()
        if btn2.button("🗑 清空录制"): service.clear_recording(); st.rerun()
        if not recorded_frames:
            st.info("点击「开始录制」后，系统每秒录制一帧（价格+CVD+爆仓+费率+价差）。")
        else:
            speed = st.select_slider("回放速度", options=["1x","5x","20x"], value="1x")
            step  = {"1x":1,"5x":5,"20x":20}.get(speed,1)
            frames_to_show = list(recorded_frames)[::step][-300:]
            st.plotly_chart(build_replay_price_figure(frames_to_show, speed), key="pc_replay", config={'displayModeBar': True, 'scrollZoom': True})

    # ══ TAB 14: 全市场对比 ══
    with tabs[14]:
        render_section("全市场对比  ·  Cross-Exchange Snapshot", "Binance排首位")
        ordered_snaps = sorted(snapshots, key=lambda s: 0 if s.exchange=="Binance" else 1)
        st.dataframe(build_snapshot_frame(ordered_snaps), width='stretch', hide_index=True,
            column_config={"最新价":st.column_config.NumberColumn(format="%.2f"),
                "现货价":st.column_config.NumberColumn(format="%.2f"),
                "现货-合约(bps)":st.column_config.NumberColumn(format="%.2f"),
                "持仓金额":st.column_config.NumberColumn(format="%.0f"),
                "费率bps":st.column_config.NumberColumn(format="%.4f")})
        st.plotly_chart(build_funding_comparison_figure(ok_snaps), key="pc_funding_compare", config={'displayModeBar': True, 'scrollZoom': True})

    # ══ TAB 15: 多币种轮巡 ══
    with tabs[15]:
        render_section("多币种轮巡  ·  Multi-Coin Watchlist", f"快速扫描 {len(watchlist_coins)} 个币种", kicker="Watch")
        if not watchlist_coins: st.info("请在左侧侧边栏输入监控币种。")
        else:
            from exchanges import BinanceClient
            client = BinanceClient(timeout=req_timeout)
            wrows=[]
            for coin in watchlist_coins[:15]:
                try:
                    s = client.fetch(f"{coin}USDT")
                    if s.status=="ok":
                        wrows.append({"币种":coin,"最新价":s.last_price,"持仓金额":s.open_interest_notional,
                            "资金费率bps":s.funding_bps,"24h成交额":s.volume_24h_notional,"状态":"✅"})
                    else: wrows.append({"币种":coin,"最新价":None,"持仓金额":None,"资金费率bps":None,"24h成交额":None,"状态":"❌"})
                except: wrows.append({"币种":coin,"最新价":None,"持仓金额":None,"资金费率bps":None,"24h成交额":None,"状态":"⚠️"})
            if wrows:
                st.dataframe(pd.DataFrame(wrows), width='stretch', hide_index=True,
                    column_config={"最新价":st.column_config.NumberColumn(format="%.4f"),
                        "持仓金额":st.column_config.NumberColumn(format="%.0f"),
                        "资金费率bps":st.column_config.NumberColumn(format="%.4f"),
                        "24h成交额":st.column_config.NumberColumn(format="%.0f")})

    # ══ TAB 16: 预警规则 ══
    with tabs[16]:
        render_section("预警规则  ·  Alert Rules", kicker="Alerts")
        if sp_alerts:
            st.subheader(f"🔗 Spot-Perp 告警 ({len(sp_alerts)}条)")
            render_spot_perp_alerts(sp_alerts, max_show=8)
            st.markdown("---")
        if "alert_rules" not in st.session_state: st.session_state["alert_rules"]=[]
        with st.expander("➕ 添加新预警规则", expanded=False):
            a1,a2,a3=st.columns(3)
            n   = a1.text_input("规则名称", placeholder="如：BTC破7万")
            exk = a2.selectbox("交易所", list(EXCHANGE_ORDER), format_func=lambda k: EXCHANGE_TITLES[k], key="al_exch")
            met = a3.selectbox("监控指标", list(ALERT_METRICS.keys()), format_func=lambda k: ALERT_METRICS[k], key="al_met")
            a4,a5,a6=st.columns(3)
            cond  = a4.selectbox("条件", list(ALERT_CONDITIONS.keys()), format_func=lambda k: ALERT_CONDITIONS[k], key="al_cond")
            thresh= a5.number_input("阈值", value=0.0, format="%.6f", key="al_thresh")
            if a6.button("添加"):
                if n.strip():
                    st.session_state["alert_rules"].append(AlertRule(
                        rule_id=str(uuid.uuid4())[:8], name=n.strip(), exchange=exk,
                        symbol=symbol_map[exk], metric=met, condition=cond, threshold=thresh))
                    service.set_alert_rules(st.session_state["alert_rules"])
                    st.success(f"已添加：{n}"); st.rerun()
        if st.session_state["alert_rules"]: service.set_alert_rules(st.session_state["alert_rules"])
        for i,rule in enumerate(st.session_state.get("alert_rules",[])):
            rc1,rc2,rc3,rc4,rc5=st.columns([2,2,2,1,1])
            rc1.write(f"**{rule.name}**")
            rc2.write(f"{EXCHANGE_TITLES.get(rule.exchange,rule.exchange)} · {ALERT_METRICS.get(rule.metric,rule.metric)}")
            rc3.write(f"{ALERT_CONDITIONS.get(rule.condition,rule.condition)} {rule.threshold:.4g}")
            rc4.write("🔔 已触发" if rule.triggered else "⏳ 监听中")
            if rc5.button("删除", key=f"del_{i}"):
                st.session_state["alert_rules"].pop(i)
                service.set_alert_rules(st.session_state["alert_rules"]); st.rerun()
        if not st.session_state.get("alert_rules"): st.info('暂无预警规则。点击"添加新预警规则"来创建。')
        if alert_events:
            st.subheader(f"预警历史 ({len(alert_events)}条)")
            arows=[{"触发时间":pd.to_datetime(e.triggered_at_ms,unit="ms"),"规则":e.name,
                "交易所":EXCHANGE_TITLES.get(e.exchange,e.exchange),"实际值":e.actual_value,"消息":e.message}
                for e in reversed(list(alert_events)[-50:])]
            st.dataframe(pd.DataFrame(arows), width='stretch', hide_index=True,
                column_config={"实际值":st.column_config.NumberColumn(format="%.4g")})

    # ══ TAB 17: HL 链上中心 ══
    with tabs[17]:
        if _HAS_HL_CENTER:
            render_hl_center()
        else:
            st.warning("hl_center.py 未找到，请确认文件已放置在同目录")

    # ══ TAB 18: 信号增强中心 ══
    with tabs[18]:
        if _HAS_SIGNAL_CENTER:
            candles_by_ex = {}
            for ek in EXCHANGE_ORDER:
                syms = default_symbols(base_coin)
                ck = f"candles_{ek}_{syms.get(ek,'')}_{interval}"
                if ck in st.session_state:
                    candles_by_ex[ek] = st.session_state[ck]
            vpin_calcs = service.get_vpin_calculators() if hasattr(service, 'get_vpin_calculators') else {}
            all_liq = []
            for ek in EXCHANGE_ORDER:
                all_liq.extend(list(service.get_liquidation_history(ek)))
            ob_spread_h = {s.exchange: [float(s.spot_perp_spread_bps or 0)] for s in snapshots if s.status=="ok"}
            ob_depth_h = {}
            for ek in EXCHANGE_ORDER:
                book = service.get_local_book(ek)
                if book and book.is_ready:
                    levels = book.to_levels(20)
                    bid_depth = sum(l.price * l.size for l in levels if l.side == "bid")
                    ob_depth_h[ek] = [bid_depth]
            render_signal_center(
                snapshots=snapshots,
                candles_by_exchange=candles_by_ex,
                vpin_calculators=vpin_calcs,
                liq_events=all_liq,
                ob_spread_hist=ob_spread_h,
                ob_depth_hist=ob_depth_h,
                dominance_history=service.get_dominance_history() if hasattr(service, 'get_dominance_history') else [],
            )
        else:
            st.warning("signal_center.py / aggregator.py 未找到")

    # ── Static tabs 19-24 are rendered OUTSIDE the fragment (see below) ──

    # ══ TAB 19: 调试 ══
    with tabs[19]:
        render_section("接口调试 + WS健康状态  ·  API Debug & WS Health")

        # WS Health Panel
        st.markdown("#### 📡 WebSocket 健康状态")
        if hasattr(service, "get_ws_health"):
            ws_health = service.get_ws_health()
            wh_cols = st.columns(4)
            for ci, (ek, info) in enumerate(ws_health.items()):
                status_icon = "🟢" if info["status"] == "正常" else "🟡" if info["status"] == "延迟" else "🔴"
                with wh_cols[ci]:
                    st.markdown(f"**{ek.capitalize()}** {status_icon}")
                    st.caption(f"状态: {info['status']}")
                    st.caption(f"最后消息: {info['secs_since_msg']:.1f}s前")
                    st.caption(f"消息总数: {info['msg_count']:,}")
                    st.caption(f"断线次数: {info['disconnects']}")
                    st.caption(f"运行时间: {info['uptime_min']:.1f}min")
                    if info['reconnect_delay'] > 3:
                        st.caption(f"⚠️ 退避延迟: {info['reconnect_delay']:.0f}s")

        st.markdown("---")
        st.write(f"主图: `{sel_snap.exchange}` | 合约: `{sel_sym}`")
        st.write(f"WS订单簿: `{'已建立' if local_book.is_ready else '初始化中'}` | OI点数: `{len(service.get_oi_history(selected_exchange))}` | WS成交: `{len(ws_trades)}`")
        st.write(f"已确认告警: `{len(confirmed_alerts)}` | 爆仓簇v2: `{len(liq_clusters_v2)}` | 录制帧: `{len(recorded_frames)}`")
        st.write(f"Spot WS: `{ {k: f'{v:.2f}' if v else 'None' for k,v in service._spot_price.items()} }`")
        for s in snapshots:
            with st.expander(f"{s.exchange} | {s.symbol} | {CARD_STATUS.get(s.status,s.status)}"):
                if s.error: st.error(s.error)
                st.json(s.raw)

    # ── Scan coins sidebar input (outside fragment to persist) ─────────────────
    # This is handled via session state key


render_terminal()


# ══════════════════════════════════════════════════════════════════════════════
# 静态功能区 — 推送/历史/v8增强功能（不在自动刷新 fragment 内，避免表单冲突）
# ══════════════════════════════════════════════════════════════════════════════

st.markdown("---")
st.markdown(
    '<div class="glass-section">'
    '<div class="glass-kicker">EXTENDED FEATURES</div>'
    '<div class="glass-title">⚙️ 扩展功能区  ·  推送/历史/v8增强</div>'
    '</div>', unsafe_allow_html=True)

# ── 静态功能区变量安全初始化 ────────────────────────────────────────────────
# 这些变量在 fragment 内计算，静态区用 service 重新获取或给安全默认值
try:
    _static_snapshots = service.current_snapshots()
    _static_ok_snaps  = [s for s in _static_snapshots if s.status == "ok"]
    _static_snap_by_key = dict(zip(("bybit","binance","okx","hyperliquid"), _static_snapshots))
except Exception:
    _static_snapshots = []
    _static_ok_snaps  = []
    _static_snap_by_key = {}

try:
    _sel_snap_static = _static_snap_by_key.get(selected_exchange)
    _ref_price_static = (
        (_sel_snap_static.last_price or _sel_snap_static.mark_price)
        if _sel_snap_static and _sel_snap_static.status == "ok"
        else None
    )
except Exception:
    _sel_snap_static  = None
    _ref_price_static = None

try:
    from analytics import build_oi_delta_points, build_oi_delta_summary
    from exchanges import fetch_exchange_oi_history
    _static_oi_pts = service.get_oi_history(selected_exchange)[-60:]
    from models import OIPoint
    _static_candles = st.session_state.get(f"candles_{selected_exchange}_{symbol_map.get(selected_exchange,'')}_{interval}", [])
    _static_oi_delta = build_oi_delta_points(_static_oi_pts, _static_candles)
except Exception:
    _static_oi_delta = []

try:
    _static_rest_liqs = []
    for _sek in ("bybit","binance","okx","hyperliquid"):
        _static_rest_liqs.extend(list(service.get_liquidation_history(_sek)))
except Exception:
    _static_rest_liqs = []

# Alias these for use in static tab code below
ok_snaps    = _static_ok_snaps
snap_by_key = _static_snap_by_key
oi_delta_pts = _static_oi_delta
ref_price   = _ref_price_static
rest_liqs   = _static_rest_liqs
sel_snap    = _sel_snap_static

_static_tabs = st.tabs([
    "📡 推送&历史数据",
    "💧 现货合约分账",
    "🔥 清算热力图2.0",
    "🐋 鲸鱼热力图",
    "🔐 真实持仓",
    "⚡ 统一风险板",
])

# ══ TAB 19: 推送 & 历史数据 ══
with _static_tabs[0]:
    if _HAS_PUSH_STORAGE:
        render_push_settings(service)
    else:
        st.warning("push_settings.py / notifier.py / storage.py 未找到")

# ══ TAB 20: 现货合约分账 (v8 方向2) ══
with _static_tabs[1]:
    render_section(
        "现货合约分账  ·  Spot / Perp Flow Split",
        "三视角切换 | 多空比仅出现在合约视角 | 现货视角绝不显示全市场多空比",
        kicker="Flow Split v8"
    )
    _flow_view = st.radio(
        "视角选择", ["📦 现货视角", "📊 合约视角", "🔗 联合对照"],
        horizontal=True, key="flow_view_v8")

    # 从 service 获取对应的流数据
    _spot_flows = []
    _perp_flows = []
    _combined_flows = []

    try:
        if hasattr(service, "get_spot_large_flows"):
            _spot_flows = service.get_spot_large_flows(selected_exchange) or []
        if hasattr(service, "get_perp_large_flows"):
            _perp_flows = service.get_perp_large_flows(selected_exchange) or []
    except Exception:
        pass

    # 构建 PerpFlowSnapshot 历史
    _perp_snap_hist = []
    for _oi_pt in service.get_oi_history(selected_exchange)[-100:]:
        try:

            _perp_snap_hist.append(PerpFlowSnapshot(
                timestamp_ms=_oi_pt.timestamp_ms,
                exchange=selected_exchange,
                oi_notional=_oi_pt.open_interest_notional,
            ))
        except Exception:
            pass

    if _flow_view == "📦 现货视角":
        st.info("💡 现货视角仅显示主动买卖方向和盘口数据，不包含多空比（多空比属于合约数据）")

        _spot_snap_hist = []
        for _ek_snap in service.current_snapshots():
            if _ek_snap.exchange.lower() == selected_exchange:
                _book = service.get_local_book(selected_exchange)
                _sum = summarize_orderbook(
                    service.get_local_book_levels(selected_exchange, 50)
                    if _book.is_ready else [], sel_snap.last_price)
                _spot_snap_hist.append(SpotFlowSnapshot(
                    timestamp_ms=int(time.time() * 1000),
                    exchange=selected_exchange,
                    bid_notional=_sum.get("bid_notional"),
                    ask_notional=_sum.get("ask_notional"),
                    ob_imbalance_pct=_sum.get("imbalance_pct"),
                    spread_bps=_sum.get("spread_bps"),
                ))
        if _spot_snap_hist:
            st.plotly_chart(
                build_spot_flow_figure(_spot_snap_hist),
                key="pc_spot_flow_v8")
        else:
            st.info("等待现货流数据建立…")

    elif _flow_view == "📊 合约视角":
        st.info("📊 合约视角 — 包含多空比、OI变化、资金费率等合约专属指标")
        _sent_for_perp = load_contract_sentiment(
            symbol_map["binance"], symbol_map["bybit"], interval, req_timeout)
        _pc1, _pc2, _pc3 = st.columns(3)
        if _sent_for_perp:
            gr = _sent_for_perp.binance_global_ratio
            _pc1.metric("全市场多空比", f"{gr:.3f}" if gr else "—",
                        help="仅合约视角显示，Binance全市场账户")
            pr = _sent_for_perp.binance_top_position_ratio
            _pc2.metric("大户持仓多空比", f"{pr:.3f}" if pr else "—")
            btr = _sent_for_perp.bybit_taker_buy_ratio
            _pc3.metric("Bybit Taker买比", f"{btr:.3f}" if btr else "—",
                        help="Taker方向，非持仓")
        st.plotly_chart(
            build_perp_flow_figure(_perp_snap_hist),
            key="pc_perp_flow_v8")

    else:  # 联合对照
        st.info("🔗 联合视图 — 现货与合约并排对比，发现价格联动信号")
        _comb_c1, _comb_c2 = st.columns(2)
        with _comb_c1:
            st.plotly_chart(build_spot_flow_figure([]), key="pc_combined_spot_v8")
        with _comb_c2:
            st.plotly_chart(build_perp_flow_figure(_perp_snap_hist), key="pc_combined_perp_v8")

# ══ TAB 21: 清算热力图2.0 (v8 方向3) ══
with _static_tabs[2]:
    render_section(
        "清算热力图 2.0  ·  Liquidation Heatmap with Confidence",
        "置信度分级：Bybit WS=1.0真实 / Binance WS=0.5可能漏单 / OKX REST=0.3仅参考 / HL=0.2推断",
        kicker="Liq Heatmap v8"
    )

    # 置信度说明卡片
    _conf_cols = st.columns(4)
    _conf_data = [
        ("Bybit WS", "1.0", "● 实心圆", "#1dc796", "真实爆仓"),
        ("Binance WS", "0.5", "○ 空心圆", "#ffa94d", "可能漏单"),
        ("OKX REST", "0.3", "◆ 菱形",   "#62c2ff", "仅参考"),
        ("HL 推断",  "0.2", "✕ ×",      "#888",    "推断数据"),
    ]
    for col, (ex, conf, sym, color, note) in zip(_conf_cols, _conf_data):
        col.markdown(
            f'<div style="padding:10px;border-radius:12px;border:1px solid rgba(255,255,255,0.12);'
            f'background:rgba(255,255,255,0.05);text-align:center;">'
            f'<div style="font-size:0.75rem;color:#bcd;">{ex}</div>'
            f'<div style="font-size:1.3rem;font-weight:800;color:{color};">{sym}</div>'
            f'<div style="font-size:0.85rem;font-weight:700;color:{color};">置信度 {conf}</div>'
            f'<div style="font-size:0.72rem;color:#888;">{note}</div>'
            f'</div>',
            unsafe_allow_html=True)

    st.markdown("")

    # 构建带置信度标签的清算事件
    _all_liq_raw = []
    for _ek in EXCHANGE_ORDER:
        _all_liq_raw.extend(list(service.get_liquidation_history(_ek)))
    _all_liq_raw.extend(rest_liqs)

    _liq_with_conf = [LiquidationWithConfidence.from_event(e) for e in _all_liq_raw]

    # 统计
    if _liq_with_conf:
        _stats_cols = st.columns(4)
        _bybit_cnt   = sum(1 for x in _liq_with_conf if x.confidence == 1.0)
        _binance_cnt = sum(1 for x in _liq_with_conf if 0.4 < x.confidence < 0.6)
        _okx_cnt     = sum(1 for x in _liq_with_conf if 0.25 <= x.confidence <= 0.35)
        _hl_cnt      = sum(1 for x in _liq_with_conf if x.confidence < 0.25)
        _stats_cols[0].metric("Bybit真实爆仓", str(_bybit_cnt))
        _stats_cols[1].metric("Binance爆仓(可能漏单)", str(_binance_cnt))
        _stats_cols[2].metric("OKX爆仓(仅参考)", str(_okx_cnt))
        _stats_cols[3].metric("HL爆仓(推断)", str(_hl_cnt))

    st.plotly_chart(
        build_liq_confidence_heatmap(
            _liq_with_conf, ref_price=ref_price,
            title=f"清算热力图 2.0 · {base_coin} · 置信度分级渲染"),
        key="pc_liq_heatmap_v8", config={"displayModeBar": True})

# ══ TAB 22: 鲸鱼热力图 (v8 方向4) ══
with _static_tabs[3]:
    render_section(
        "鲸鱼热力图  ·  Whale Order Heatmap",
        "三视角：现货/合约/对照图 | 拆单检测：30s内同价位±0.1%连续≥3笔标记",
        kicker="Whale Heatmap v8"
    )

    _whale_view = st.radio(
        "热力图视角", ["🟢 现货视角", "🔵 合约视角", "🔗 对照图"],
        horizontal=True, key="whale_view_v8")

    _view_key = {"🟢 现货视角": "spot", "🔵 合约视角": "perp", "🔗 对照图": "combined"}.get(_whale_view, "spot")

    # 获取分账大单流（现货和合约独立）
    _spot_lof = []
    _perp_lof = []
    try:
        if hasattr(service, "get_spot_large_flows"):
            _raw_spot = service.get_spot_large_flows(selected_exchange) or []
            for _f in _raw_spot:

                _spot_lof.append(SpotLargeOrderFlow(
                    timestamp_ms=getattr(_f, "timestamp_ms", 0),
                    exchange=getattr(_f, "exchange", selected_exchange),
                    side=getattr(_f, "side", "buy"),
                    price=getattr(_f, "price", 0),
                    notional=getattr(_f, "notional", 0),
                    is_aggressor=getattr(_f, "is_aggressor", False),
                ))
    except Exception:
        pass

    # 现有 large_order_flow 作为合约流（向后兼容）
    _perp_raw = service.get_large_order_flow(selected_exchange)
    for _f in _perp_raw:

        _perp_lof.append(PerpLargeOrderFlow(
            timestamp_ms=getattr(_f, "timestamp_ms", 0),
            exchange=getattr(_f, "exchange", selected_exchange),
            side=getattr(_f, "side", "buy"),
            price=getattr(_f, "price", 0),
            notional=getattr(_f, "notional", 0),
            is_aggressor=getattr(_f, "is_aggressor", False),
        ))

    # 拆单检测
    _min_notional = st.slider("拆单检测最小单笔金额($)", 5_000, 100_000, 20_000, 5_000,
                               key="split_threshold_v8")
    _active_flows = _spot_lof if _view_key == "spot" else _perp_lof
    _split_clusters = []
    if _active_flows:
        try:
            _split_clusters = detect_split_orders(
                _active_flows, window_ms=30_000,
                price_tolerance_pct=0.001, min_count=3,
                min_notional_each=_min_notional)
        except Exception:
            pass

    if _split_clusters:
        st.warning(f"⚠️ 检测到 **{len(_split_clusters)}** 个疑似拆单簇（橙色圆环标记）")
        _sc_rows = [{
            "交易所": c.exchange, "方向": c.side,
            "价格中心": f"{c.price_center:.2f}",
            "笔数": c.order_count,
            "总金额": fc(c.total_notional),
            "持续(s)": f"{(c.last_ms - c.first_ms)/1000:.1f}",
            "平均间隔(ms)": f"{c.avg_interval_ms:.0f}",
        } for c in _split_clusters[:20]]
        st.dataframe(pd.DataFrame(_sc_rows), hide_index=True)

    st.plotly_chart(
        build_whale_heatmap_figure(
            _spot_lof, _perp_lof,
            ref_price=ref_price,
            view=_view_key,
            split_clusters=_split_clusters),
        key="pc_whale_heatmap_v8", config={"displayModeBar": True})

# ══ TAB 23: 真实持仓 (v8 方向5) ══
with _static_tabs[4]:
    render_section(
        "真实持仓  ·  Real Position Viewer",
        "公开模式：HL地址分析 | 私有模式：API Key仅存Session，绝不入数据库",
        kicker="Position v8"
    )

    _pos_mode = st.radio(
        "查询模式",
        ["🌐 公开模式 (HL地址)", "🔐 私有模式 (API Key)"],
        horizontal=True, key="pos_mode_v8")

    if _pos_mode == "🌐 公开模式 (HL地址)":
        st.info("公开模式复用现有 Hyperliquid 链上地址分析。地址为公开链上数据，无需授权。")
        _hl_addr = st.text_input(
            "输入 HL 钱包地址 (0x...)",
            key="hl_addr_pos_v8",
            placeholder="0x1234...")
        if _hl_addr and st.button("查询持仓", key="hl_pos_query"):
            try:
                import hl_client as _hl_cli_mod
                _positions = _hl_cli_mod.fetch_whale_positions(_hl_addr)
                if _positions:
                    _pos_rows = []
                    for _p in _positions:
                        _pos_rows.append({
                            "币种": getattr(_p, "coin", ""),
                            "方向": getattr(_p, "side", ""),
                            "数量": getattr(_p, "size", 0),
                            "名义价值": fc(getattr(_p, "notional", None)),
                            "开仓价": fp(getattr(_p, "entry_price", None)),
                            "标记价": fp(getattr(_p, "mark_price", None)),
                            "未实现盈亏": fp(getattr(_p, "unrealized_pnl", None)),
                            "杠杆": getattr(_p, "leverage", "—"),
                        })
                    st.dataframe(pd.DataFrame(_pos_rows), hide_index=True)
                else:
                    st.info("该地址暂无持仓，或地址格式有误。")
            except Exception as _e:
                st.error(f"查询失败：{_e}")
        elif not _hl_addr:
            st.caption("HL地址分析由 hl_center.py 支持，也可前往「HL链上中心」Tab查看完整分析。")

    else:
        st.error("⚠️ 私有模式安全提示", icon="🔒")
        st.markdown("""
**私有模式安全规则（严格执行）：**
- 🔑 API Key **仅存储在当前浏览器 Session**，刷新页面即清除，**绝不写入数据库**
- 📖 强制**只读模式**，仅调用 GET 方法，不执行任何交易操作
- 🛡️ 本地运行时数据不经过第三方服务器
- ⚠️ 请在安全网络环境下使用，不建议在公共网络输入API Key
        """)

        if not _HAS_PRIVATE_CLIENT:
            st.warning("private_client.py 未找到，私有模式不可用。")
        else:
            _api_key = st.text_input(
                "API Key（仅存Session，不持久化）",
                type="password", key="private_api_key_v8")
            _api_secret = st.text_input(
                "API Secret（仅存Session）",
                type="password", key="private_api_secret_v8")
            _priv_exchange = st.selectbox(
                "交易所", ["Binance", "Bybit", "OKX"],
                key="private_exchange_v8")
            _passphrase = ""
            if _priv_exchange == "OKX":
                _passphrase = st.text_input(
                    "OKX Passphrase（仅存Session）",
                    type="password", key="private_passphrase_v8")

            if _api_key and _api_secret and st.button("只读查询持仓", key="priv_pos_query"):
                st.info("🔒 正在通过只读GET接口查询…API Key仅用于本次Session")
                try:
                    _pos_data = _private_client_mod.fetch_positions_readonly(
                        exchange=_priv_exchange.lower(),
                        api_key=_api_key,
                        api_secret=_api_secret,
                        passphrase=_passphrase,
                    )
                    if _pos_data:
                        st.dataframe(pd.DataFrame(_pos_data), hide_index=True)
                    else:
                        st.info("暂无持仓，或API Key无效。")
                except Exception as _e:
                    st.error(f"查询失败：{_e}")

# ══ TAB 24: 统一风险板 (v8 方向6) ══
with _static_tabs[5]:
    render_section(
        "统一风险板  ·  Unified Risk Dashboard",
        "六维雷达：Funding/基差/OI压力/清算密度/ADL保险基金/HL资产Ctx ⭐独占",
        kicker="Risk Board v8"
    )

    # 计算 HL meta（获取HL独占数据）
    _hl_meta = {}
    try:
        from exchanges import safe_float as _safe_float_ex
        _hl_snap = snap_by_key.get("hyperliquid")
        if _hl_snap and _hl_snap.status == "ok":
            _hl_raw = _hl_snap.raw.get("asset_context", {})
            _oi_cap_raw = _hl_raw.get("openInterestCap")
            _hl_meta = {
                "predicted_funding": _safe_float_ex(_hl_raw.get("funding")),
                "mark_px": _hl_snap.mark_price,
                "oracle_px": _hl_snap.index_price,
                "at_oi_cap": (
                    bool(_oi_cap_raw) and
                    bool(_hl_snap.open_interest) and
                    (_hl_snap.open_interest /
                     max(float(_oi_cap_raw), 1)) > 0.95
                ),
            }
    except Exception:
        pass

    # 构建风险雷达数据
    _risk_pt = None
    try:
        _risk_pt = build_risk_radar_point(
            coin=base_coin,
            snapshots=ok_snaps,
            oi_delta_pts=oi_delta_pts,
            liq_events=liq_events,
            hl_meta=_hl_meta,
        )
    except Exception as _re:
        st.warning(f"风险数据计算异常：{_re}")

    # 缓存历史
    if "risk_history" not in st.session_state:
        st.session_state["risk_history"] = []
    if _risk_pt is not None:
        _rh = st.session_state["risk_history"]
        _rh.append(_risk_pt)
        st.session_state["risk_history"] = _rh[-200:]
    _risk_history = st.session_state.get("risk_history", [])

    # KPI 行
    if _risk_pt:
        _rk_cols = st.columns(6)
        _rk_cols[0].metric("综合风险", _risk_pt.risk_label,
                           delta=f"{_risk_pt.composite_risk:+.3f}")
        _rk_cols[1].metric("Funding风险", f"{_risk_pt.funding_risk:.3f}")
        _rk_cols[2].metric("基差风险", f"{_risk_pt.basis_risk:.3f}")
        _rk_cols[3].metric("OI压力", f"{_risk_pt.oi_pressure:.3f}")
        _rk_cols[4].metric("清算密度", f"{_risk_pt.liq_density:.3f}")
        _rk_cols[5].metric("HL Ctx⭐", f"{_risk_pt.hl_asset_ctx_risk:.3f}",
                           help="Hyperliquid独占维度：预测费率+OI上限+Mark/Oracle偏差")

    # HL 独占维度提示
    if _hl_meta:
        _hl_info_cols = st.columns(3)
        pf = _hl_meta.get("predicted_funding")
        _hl_info_cols[0].metric(
            "HL 预测资金费率",
            f"{pf * 10000:+.4f}bps" if pf else "—",
            help="predictedFundings — HL独占，其他所无此数据")
        at_cap = _hl_meta.get("at_oi_cap", False)
        _hl_info_cols[1].metric(
            "HL OI上限状态",
            "⚠️ 已触碰上限" if at_cap else "✅ 正常",
            help="perpsAtOpenInterestCap — HL独占")
        mk = _hl_meta.get("mark_px"); ok_ = _hl_meta.get("oracle_px")
        dev = abs(mk - ok_) / ok_ * 100 if mk and ok_ and ok_ > 0 else None
        _hl_info_cols[2].metric(
            "Mark/Oracle 偏差",
            f"{dev:.4f}%" if dev else "—",
            help="markPx vs oraclePx — HL独占价格偏差")

    st.markdown("")

    # 双图布局：雷达 + 历史趋势
    _radar_col, _hist_col = st.columns([1, 1])
    with _radar_col:
        st.plotly_chart(
            build_risk_radar_figure(_risk_pt),
            key="pc_risk_radar_v8", config={"displayModeBar": False})
    with _hist_col:
        st.plotly_chart(
            build_risk_history_figure(_risk_history),
            key="pc_risk_history_v8", config={"displayModeBar": True})


# Sidebar scan coins input (needs to be outside fragment)
with st.sidebar:
    st.markdown("---")
    st.subheader("⭐ 自选收藏")
    # Persistent favorites via session_state (survives rerun within session)
    if "favorites" not in st.session_state:
        st.session_state["favorites"] = ["BTC", "ETH", "SOL"]
    fav_input = st.text_input(
        "收藏币种（逗号分隔）",
        value=",".join(st.session_state["favorites"]),
        key="fav_input",
    )
    if fav_input:
        st.session_state["favorites"] = [c.strip().upper() for c in fav_input.split(",") if c.strip()]
    fav_coins = st.session_state["favorites"]
    if fav_coins:
        fav_cols = st.columns(min(5, len(fav_coins)))
        for i, coin in enumerate(fav_coins[:5]):
            if fav_cols[i].button(coin, key=f"fav_btn_{coin}"):
                st.session_state["base_coin_val"] = coin
                st.rerun()

    st.markdown("---")
    st.subheader("🔇 全局静音")
    global_mute = st.checkbox(
        "静音所有声音告警",
        value=st.session_state.get("global_mute", False),
        key="global_mute_cb",
    )
    st.session_state["global_mute"] = global_mute
    # Inject mute state into browser
    st.markdown(
        f"<script>window._globalMuted = {'true' if global_mute else 'false'};</script>",
        unsafe_allow_html=True,
    )

    st.markdown("---")
    st.subheader("🏠 首页扫描币种")
    hp_coins = st.text_input(
        "扫描币种（逗号分隔）",
        value=",".join(_SCAN_COINS[:25]),
        key="homepage_coins_val",
        help="全市场首页扫描的币种列表，每次刷新约30秒缓存")