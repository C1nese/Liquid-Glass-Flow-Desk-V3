from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class ExchangeSnapshot:
    exchange: str
    symbol: str
    last_price: Optional[float] = None
    mark_price: Optional[float] = None
    index_price: Optional[float] = None
    open_interest: Optional[float] = None
    open_interest_notional: Optional[float] = None
    funding_rate: Optional[float] = None
    volume_24h_base: Optional[float] = None
    volume_24h_notional: Optional[float] = None
    timestamp_ms: Optional[int] = None
    status: str = "ok"
    error: Optional[str] = None
    raw: Dict[str, Any] = field(default_factory=dict)
    # Spot price (injected from spot WS)
    spot_price: Optional[float] = None
    spot_volume_24h: Optional[float] = None
    # Long/short ratio (from REST polling)
    long_short_ratio: Optional[float] = None
    long_account_pct: Optional[float] = None
    short_account_pct: Optional[float] = None
    long_position_pct: Optional[float] = None
    short_position_pct: Optional[float] = None

    @property
    def premium_pct(self) -> Optional[float]:
        if self.last_price is None or self.mark_price in (None, 0):
            return None
        return (self.last_price - self.mark_price) / self.mark_price * 100.0

    @property
    def funding_bps(self) -> Optional[float]:
        if self.funding_rate is None:
            return None
        return self.funding_rate * 10000.0

    @property
    def spot_perp_spread_bps(self) -> Optional[float]:
        if self.spot_price and self.last_price and self.spot_price > 0:
            return (self.last_price - self.spot_price) / self.spot_price * 10000.0
        return None

    def to_row(self) -> Dict[str, Any]:
        return {
            "Exchange": self.exchange, "Symbol": self.symbol,
            "Last": self.last_price, "Mark": self.mark_price,
            "Index/Oracle": self.index_price, "Premium %": self.premium_pct,
            "Open Interest": self.open_interest,
            "OI Notional": self.open_interest_notional,
            "Funding Rate": self.funding_rate, "Funding bps": self.funding_bps,
            "24h Base Volume": self.volume_24h_base,
            "24h Notional Volume": self.volume_24h_notional,
            "Timestamp": self.timestamp_ms, "Status": self.status, "Error": self.error,
        }


@dataclass
class Candle:
    timestamp_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    taker_buy_volume: Optional[float] = None
    taker_sell_volume: Optional[float] = None


@dataclass
class OIPoint:
    timestamp_ms: int
    open_interest: Optional[float] = None
    open_interest_notional: Optional[float] = None


@dataclass
class LiquidationEvent:
    exchange: str
    symbol: str
    timestamp_ms: int
    side: str
    price: Optional[float] = None
    size: Optional[float] = None
    notional: Optional[float] = None
    source: str = "unknown"
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class OrderBookLevel:
    price: float
    size: float
    side: str


@dataclass
class LocalOrderBook:
    exchange: str
    symbol: str
    bids: Dict[float, float] = field(default_factory=dict)
    asks: Dict[float, float] = field(default_factory=dict)
    last_update_id: int = 0
    timestamp_ms: int = 0
    is_ready: bool = False

    def to_levels(self, depth: int = 200) -> List[OrderBookLevel]:
        levels: List[OrderBookLevel] = []
        for price in sorted(self.bids.keys(), reverse=True)[:depth]:
            if self.bids[price] > 0:
                levels.append(OrderBookLevel(price=price, size=self.bids[price], side="bid"))
        for price in sorted(self.asks.keys())[:depth]:
            if self.asks[price] > 0:
                levels.append(OrderBookLevel(price=price, size=self.asks[price], side="ask"))
        return levels

    def best_bid(self) -> Optional[float]:
        bids = [p for p, s in self.bids.items() if s > 0]
        return max(bids) if bids else None

    def best_ask(self) -> Optional[float]:
        asks = [p for p, s in self.asks.items() if s > 0]
        return min(asks) if asks else None

    def spread_bps(self) -> Optional[float]:
        bb, ba = self.best_bid(), self.best_ask()
        if bb and ba and bb > 0:
            return (ba - bb) / bb * 10000.0
        return None

    def mid_price(self) -> Optional[float]:
        bb, ba = self.best_bid(), self.best_ask()
        if bb and ba:
            return (bb + ba) / 2.0
        return None


@dataclass
class TradeEvent:
    exchange: str
    symbol: str
    timestamp_ms: int
    price: float
    size: float
    side: str
    notional: float = 0.0
    source: str = "ws"
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CVDPoint:
    timestamp_ms: int
    cvd: float
    delta: float
    buy_volume: float = 0.0
    sell_volume: float = 0.0
    price: Optional[float] = None


@dataclass
class OIDeltaPoint:
    timestamp_ms: int
    oi_notional: Optional[float]
    oi_delta: float
    oi_velocity: float = 0.0
    price: Optional[float] = None
    price_delta_pct: float = 0.0
    quadrant: str = ""
    quadrant_cn: str = ""


@dataclass
class OIVelocityAlert:
    timestamp_ms: int
    exchange: str
    symbol: str
    velocity: float
    direction: str
    quadrant_cn: str = ""


@dataclass
class TopTraderRatio:
    timestamp_ms: int
    long_short_ratio: Optional[float] = None
    long_account_ratio: Optional[float] = None
    global_ratio: Optional[float] = None
    bybit_buy_ratio: Optional[float] = None


@dataclass
class BasisPoint:
    timestamp_ms: int
    perp_price: Optional[float] = None
    spot_price: Optional[float] = None
    basis: Optional[float] = None
    basis_pct: Optional[float] = None
    exchange: str = ""


@dataclass
class FuturesOIPoint:
    expiry: str
    oi_notional: Optional[float] = None
    price: Optional[float] = None
    basis_pct: Optional[float] = None
    exchange: str = ""


@dataclass
class SpotVsPerpPoint:
    timestamp_ms: int
    spot_volume_24h: Optional[float] = None
    perp_oi: Optional[float] = None
    spot_perp_ratio: Optional[float] = None
    exchange: str = ""


@dataclass
class SpotPerpSpreadPoint:
    timestamp_ms: int
    exchange: str
    spot_price: float
    perp_price: float
    spread_bps: float


@dataclass
class SpotPerpAlert:
    timestamp_ms: int
    exchange: str
    alert_type: str
    message: str
    severity: str = "medium"
    spread_bps: Optional[float] = None
    oi_change_pct: Optional[float] = None
    cvd_delta: Optional[float] = None


@dataclass
class IcebergAlert:
    timestamp_ms: int
    exchange: str
    symbol: str
    price: float
    side: str
    refill_count: int
    total_notional: float


@dataclass
class LiquidityGap:
    timestamp_ms: int
    exchange: str
    symbol: str
    price_low: float
    price_high: float
    side: str
    prev_notional: float
    curr_notional: float
    drop_pct: float


@dataclass
class AlertRule:
    rule_id: str
    name: str
    exchange: str
    symbol: str
    metric: str
    condition: str
    threshold: float
    enabled: bool = True
    triggered: bool = False
    triggered_at_ms: Optional[int] = None
    last_value: Optional[float] = None


@dataclass
class AlertEvent:
    rule_id: str
    name: str
    exchange: str
    symbol: str
    metric: str
    condition: str
    threshold: float
    actual_value: float
    triggered_at_ms: int
    message: str = ""


# ── 告警去抖动 & 连续确认 ─────────────────────────────────────────────────────
@dataclass
class ConfirmedAlert:
    """连续 N 次触发后才正式发出的告警（去抖动）"""
    alert_id: str
    alert_type: str        # spot_lead_up / oi_up_cvd_weak / crowd_liq / orderbook_fake / composite_signal …
    exchange: str
    severity: str          # "strong" / "medium" / "weak"
    message: str
    first_seen_ms: int
    confirmed_at_ms: int
    confirm_count: int     # 已连续触发次数
    score: float = 0.0     # 告警强度得分 0-1
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AlertTimeline:
    """告警时间线条目（用于回放 / 复盘）"""
    timestamp_ms: int
    alert_type: str
    exchange: str
    severity: str
    message: str
    score: float = 0.0


# ── 盘口质量 & 撤单追踪 ───────────────────────────────────────────────────────
@dataclass
class OrderBookDeltaPoint:
    """单次盘口 snapshot-to-snapshot 差异"""
    timestamp_ms: int
    exchange: str
    side: str              # "bid" / "ask"
    price: float
    prev_size: float
    curr_size: float
    delta_size: float      # curr - prev  正=新增 负=撤单
    delta_notional: float
    event_type: str        # "add" / "cancel" / "fill"


@dataclass
class FakeWallCandidate:
    """疑似假挂单（大单短暂出现后撤离）"""
    timestamp_ms: int
    exchange: str
    side: str
    price: float
    peak_size: float
    peak_notional: float
    lifespan_ms: int       # 存续时间ms
    refill_after_ms: Optional[int] = None  # 是否快速补单


@dataclass
class WallAbsorptionEvent:
    """墙体被吃掉后快速补单"""
    timestamp_ms: int
    exchange: str
    side: str
    price: float
    absorbed_notional: float
    refill_notional: float
    refill_delay_ms: int


@dataclass
class OrderBookQualitySnapshot:
    """盘口质量综合评分快照"""
    timestamp_ms: int
    exchange: str
    bid_add_notional: float = 0.0      # 新增买单名义额
    bid_cancel_notional: float = 0.0   # 撤销买单名义额
    ask_add_notional: float = 0.0
    ask_cancel_notional: float = 0.0
    bid_net_notional: float = 0.0      # 净变化
    ask_net_notional: float = 0.0
    fake_wall_count: int = 0
    absorption_count: int = 0
    quality_score: float = 0.5         # 0=很差 1=很好


# ── OI+CVD+Funding+Crowd 合成信号 ────────────────────────────────────────────
@dataclass
class CompositeSignal:
    """四因子合成信号"""
    timestamp_ms: int
    exchange: str
    # 各因子得分 -1 到 +1 (正=偏多/强)
    price_score: float = 0.0
    oi_score: float = 0.0
    cvd_score: float = 0.0
    funding_score: float = 0.0
    crowd_score: float = 0.0
    # 合成
    composite_score: float = 0.0       # 加权平均
    signal_label: str = ""             # 偏多推进/偏空推进/拥挤衰竭/吸收中/中性
    signal_color: str = "#aaa"
    confidence: float = 0.0            # 信号可信度 0-1


# ── 爆仓簇 & 跨所联动 ─────────────────────────────────────────────────────────
@dataclass
class LiquidationClusterV2:
    """增强版爆仓簇（支持跨所联动检测）"""
    cluster_id: str
    start_ms: int
    end_ms: int
    duration_ms: int
    exchanges: List[str]               # 涉及的交易所
    cross_exchange: bool               # 是否跨所联动
    long_count: int = 0
    short_count: int = 0
    long_notional: float = 0.0
    short_notional: float = 0.0
    total_notional: float = 0.0
    dominant_side: str = ""            # "long" / "short"
    intensity: float = 0.0            # 爆仓强度 (notional/duration_sec)
    cascade_score: float = 0.0        # 瀑布得分 0-1


# ── 事件录制（回放用）────────────────────────────────────────────────────────
@dataclass
class RecordedFrame:
    """单帧录制数据（用于回放）"""
    timestamp_ms: int
    prices: Dict[str, Optional[float]]           # exchange -> price
    oi_notionals: Dict[str, Optional[float]]
    cvd_values: Dict[str, float]
    liq_events: List[LiquidationEvent]
    spread_bps: Dict[str, Optional[float]]
    composite_signals: Dict[str, "CompositeSignal"]
    funding_rates: Dict[str, Optional[float]]


# ══════════════════════════════════════════════════════════════════════════════
# v5 — 全市场总览、异动榜、深度页、告警中心、爆仓中心、盘口中心
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class CoinMarketRow:
    """全市场总览表的一行 — 按币种聚合跨所数据"""
    coin: str
    price: Optional[float] = None
    price_change_24h_pct: Optional[float] = None
    oi_total: Optional[float] = None          # 跨所合计OI
    oi_change_1h_pct: Optional[float] = None  # OI 1h变化%
    oi_change_24h_pct: Optional[float] = None # OI 24h变化%
    funding_avg: Optional[float] = None       # 各所资金费率均值 (bps)
    liq_24h_total: Optional[float] = None     # 24h总爆仓额
    liq_long_pct: Optional[float] = None      # 多头爆仓占比%
    long_short_ratio: Optional[float] = None  # 全市场多空账户比
    spot_perp_ratio: Optional[float] = None   # 现货成交量/OI
    lead_lag_status: str = "–"                # 现货先行/合约先行/中性
    composite_label: str = "–"               # 合成信号标签
    composite_score: float = 0.0
    top_exchange: str = ""                    # OI最大的所


@dataclass
class AnomalyEntry:
    """异动榜条目"""
    rank: int
    coin: str
    category: str      # oi_surge / liq_spike / funding_extreme / spot_lead / crowd_exhaust
    value: float       # 主要指标值
    value_label: str   # 格式化显示
    direction: str     # "bull" / "bear" / "neutral"
    exchange: str
    detail: str        # 一句话解读


@dataclass
class MarketConclusion:
    """主结论区"""
    timestamp_ms: int
    label: str          # 偏多推进/偏空推进/拥挤回落风险/现货先动/中性观望
    color: str
    confidence: float
    reasons: List[str]  # 支撑该结论的理由列表
    watchlist: List[str]  # 值得重点关注的币种


@dataclass
class WallLifePoint:
    """墙体寿命追踪点"""
    timestamp_ms: int
    exchange: str
    side: str           # bid / ask
    price: float
    size: float
    notional: float
    born_ms: int        # 首次出现时间
    age_ms: int         # 当前存续时长
    is_alive: bool      # 是否还在


@dataclass
class NearLiquidityCollapse:
    """近价流动性塌陷事件"""
    timestamp_ms: int
    exchange: str
    side: str
    price_pct_from_mid: float   # 距中间价的百分比
    notional_lost: float        # 消失的名义金额
    collapse_speed_ms: int      # 多快消失的


@dataclass
class LargeOrderFlow:
    """大单流（成交）"""
    timestamp_ms: int
    exchange: str
    side: str
    price: float
    notional: float
    is_aggressor: bool   # True=主动成交(吃单)


@dataclass
class MultiExchangeLiqSummary:
    """爆仓中心 — 单币种跨时间窗口摘要"""
    coin: str
    window_label: str     # "5m" / "1h" / "4h" / "24h"
    long_notional: float
    short_notional: float
    long_count: int
    short_count: int
    by_exchange: Dict[str, float]   # exchange -> notional
    cluster_count: int
    cross_ex_cluster_count: int
    dominant_side: str
    peak_cluster_notional: float


# ══════════════════════════════════════════════════════════════════════════════
# v5 — 全市场总览、异动榜、深度页、告警中心、爆仓中心、盘口中心
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class CoinMarketRow:
    """全市场总览表的一行"""
    coin: str
    price: Optional[float] = None
    price_change_24h_pct: Optional[float] = None
    oi_total: Optional[float] = None
    oi_change_1h_pct: Optional[float] = None
    oi_change_24h_pct: Optional[float] = None
    funding_avg: Optional[float] = None
    liq_24h_total: Optional[float] = None
    liq_long_pct: Optional[float] = None
    long_short_ratio: Optional[float] = None
    spot_perp_ratio: Optional[float] = None
    lead_lag_status: str = "–"
    composite_label: str = "–"
    composite_score: float = 0.0
    top_exchange: str = ""


@dataclass
class AnomalyEntry:
    """异动榜条目"""
    rank: int
    coin: str
    category: str
    value: float
    value_label: str
    direction: str
    exchange: str
    detail: str


@dataclass
class MarketConclusion:
    """主结论区"""
    timestamp_ms: int
    label: str
    color: str
    confidence: float
    reasons: List[str]
    watchlist: List[str]


@dataclass
class WallLifePoint:
    """墙体寿命追踪"""
    timestamp_ms: int
    exchange: str
    side: str
    price: float
    size: float
    notional: float
    born_ms: int
    age_ms: int
    is_alive: bool


@dataclass
class NearLiquidityCollapse:
    """近价流动性塌陷"""
    timestamp_ms: int
    exchange: str
    side: str
    price_pct_from_mid: float
    notional_lost: float
    collapse_speed_ms: int


@dataclass
class LargeOrderFlow:
    """大单流"""
    timestamp_ms: int
    exchange: str
    side: str
    price: float
    notional: float
    is_aggressor: bool


@dataclass
class MultiExchangeLiqSummary:
    """爆仓中心摘要"""
    coin: str
    window_label: str
    long_notional: float
    short_notional: float
    long_count: int
    short_count: int
    by_exchange: Dict[str, float]
    cluster_count: int
    cross_ex_cluster_count: int
    dominant_side: str
    peak_cluster_notional: float
