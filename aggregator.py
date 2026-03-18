"""
aggregator.py  —  跨所聚合 + 信号增强层（v6）
覆盖：OI聚合 / 套利监控 / 资金费率套利 / VPIN / 微结构异常 / K线形态 / 情绪评分
"""
from __future__ import annotations
import math
import time
import uuid
from collections import deque
from typing import Dict, List, Optional, Tuple

from models import (
    ExchangeSnapshot, Candle, TradeEvent, OIPoint,
    CrossExArbitrageSignal, CrossExFundingArb,
    AggregatedOIPoint, ExchangeDominancePoint,
    MarketSentimentScore, VPINPoint,
    MicrostructureAnomaly, CandlePatternSignal,
    LiquidationEvent,
)

EXCHANGE_ORDER = ("bybit", "binance", "okx", "hyperliquid")


def safe_div(a, b, default=0.0):
    return a / b if b and b != 0 else default


# ══════════════════════════════════════════════════════════════════════════════
# 跨所价格套利监控
# ══════════════════════════════════════════════════════════════════════════════

def detect_arbitrage_signals(snapshots: List[ExchangeSnapshot],
                              min_spread_bps: float = 5.0) -> List[CrossExArbitrageSignal]:
    """检测跨所同币种价格价差套利机会"""
    # Group by coin/symbol
    by_coin: Dict[str, List[ExchangeSnapshot]] = {}
    for snap in snapshots:
        coin = snap.symbol.replace("USDT", "").replace("-USDT-SWAP", "")
        by_coin.setdefault(coin, []).append(snap)

    signals = []
    now_ms = int(time.time() * 1000)
    for coin, snaps in by_coin.items():
        valid = [(s.exchange, s.last_price) for s in snaps
                 if s.last_price and s.last_price > 0 and s.status == "ok"]
        if len(valid) < 2:
            continue
        prices = sorted(valid, key=lambda x: x[1])
        low_ex,  low_price  = prices[0]
        high_ex, high_price = prices[-1]
        spread_bps = (high_price - low_price) / low_price * 10000
        if spread_bps < min_spread_bps:
            continue
        severity = "low"
        if spread_bps > 15: severity = "medium"
        if spread_bps > 30: severity = "high"
        signals.append(CrossExArbitrageSignal(
            coin=coin, timestamp_ms=now_ms,
            high_exchange=high_ex, low_exchange=low_ex,
            high_price=high_price, low_price=low_price,
            spread_bps=spread_bps,
            arbitrage_pct=spread_bps / 100,
            severity=severity,
        ))
    signals.sort(key=lambda x: x.spread_bps, reverse=True)
    return signals


# ══════════════════════════════════════════════════════════════════════════════
# 跨所资金费率套利
# ══════════════════════════════════════════════════════════════════════════════

def detect_funding_arbitrage(snapshots: List[ExchangeSnapshot],
                              min_net_bps: float = 3.0) -> List[CrossExFundingArb]:
    """检测跨所资金费率套利机会"""
    by_coin: Dict[str, Dict[str, float]] = {}
    for snap in snapshots:
        if snap.funding_rate is None or snap.status != "ok":
            continue
        coin = snap.symbol.replace("USDT", "").replace("-USDT-SWAP", "")
        by_coin.setdefault(coin, {})[snap.exchange] = snap.funding_rate

    signals = []
    now_ms = int(time.time() * 1000)
    for coin, rates in by_coin.items():
        if len(rates) < 2:
            continue
        items = sorted(rates.items(), key=lambda x: x[1])
        low_ex,  low_rate  = items[0]
        high_ex, high_rate = items[-1]
        net_bps = (high_rate - low_rate) * 10000
        if net_bps < min_net_bps:
            continue
        # 3 periods per day, 365 days
        annual_yield = net_bps * 3 * 365 / 100
        severity = "low"
        if net_bps > 8:  severity = "medium"
        if net_bps > 20: severity = "high"
        signals.append(CrossExFundingArb(
            coin=coin, timestamp_ms=now_ms,
            long_exchange=low_ex, short_exchange=high_ex,
            long_rate_bps=low_rate * 10000,
            short_rate_bps=high_rate * 10000,
            net_rate_bps=net_bps,
            annual_yield_pct=annual_yield,
            severity=severity,
        ))
    signals.sort(key=lambda x: x.net_rate_bps, reverse=True)
    return signals


# ══════════════════════════════════════════════════════════════════════════════
# OI 聚合指数
# ══════════════════════════════════════════════════════════════════════════════

def build_aggregated_oi(snapshots: List[ExchangeSnapshot], coin: str) -> Optional[AggregatedOIPoint]:
    """构建4所加权OI聚合点"""
    now_ms = int(time.time() * 1000)
    by_ex: Dict[str, float] = {}
    for snap in snapshots:
        snap_coin = snap.symbol.replace("USDT", "").replace("-USDT-SWAP", "")
        if snap_coin != coin or snap.status != "ok":
            continue
        oi_n = snap.open_interest_notional
        if oi_n and oi_n > 0:
            by_ex[snap.exchange] = oi_n
    if not by_ex:
        return None
    total = sum(by_ex.values())
    dom_ex = max(by_ex, key=by_ex.get)
    return AggregatedOIPoint(
        timestamp_ms=now_ms, coin=coin,
        total_notional=total, by_exchange=by_ex,
        dominant_exchange=dom_ex,
        dominant_pct=safe_div(by_ex[dom_ex], total) * 100,
    )


def build_exchange_dominance(snapshots: List[ExchangeSnapshot], coin: str,
                              prev: Optional[ExchangeDominancePoint] = None
                              ) -> Optional[ExchangeDominancePoint]:
    """构建交易所份额动态点"""
    now_ms = int(time.time() * 1000)
    oi_map: Dict[str, float] = {}
    vol_map: Dict[str, float] = {}
    for snap in snapshots:
        snap_coin = snap.symbol.replace("USDT", "").replace("-USDT-SWAP", "")
        if snap_coin != coin or snap.status != "ok":
            continue
        if snap.open_interest_notional:
            oi_map[snap.exchange] = snap.open_interest_notional
        if snap.volume_24h_notional:
            vol_map[snap.exchange] = snap.volume_24h_notional

    total_oi  = sum(oi_map.values())  or 1
    total_vol = sum(vol_map.values()) or 1
    oi_shares  = {ex: v / total_oi  * 100 for ex, v in oi_map.items()}
    vol_shares = {ex: v / total_vol * 100 for ex, v in vol_map.items()}

    oi_shift: Dict[str, float] = {}
    if prev:
        for ex in oi_shares:
            oi_shift[ex] = oi_shares[ex] - prev.oi_shares.get(ex, 0)

    return ExchangeDominancePoint(
        timestamp_ms=now_ms, coin=coin,
        oi_shares=oi_shares, vol_shares=vol_shares, oi_shift=oi_shift,
    )


# ══════════════════════════════════════════════════════════════════════════════
# VPIN — 订单流毒性
# ══════════════════════════════════════════════════════════════════════════════

class VPINCalculator:
    """
    VPIN (Volume-synchronized Probability of Informed Trading)
    基于成交量桶方法计算。
    每当累积成交量达到 bucket_size 时生成一个桶，
    计算买卖失衡度，滚动 n_buckets 个桶的均值即 VPIN。
    """
    def __init__(self, bucket_size: float = 100_000,
                 n_buckets: int = 50, exchange: str = ""):
        self.bucket_size  = bucket_size
        self.n_buckets    = n_buckets
        self.exchange     = exchange
        self._buy_vol_acc  = 0.0
        self._sell_vol_acc = 0.0
        self._vol_acc      = 0.0
        self._buckets: deque = deque(maxlen=n_buckets)   # (buy_vol, sell_vol)
        self._history: List[VPINPoint] = []

    def add_trade(self, trade: TradeEvent):
        notional = trade.notional or trade.price * trade.size
        if trade.side.lower() in ("buy", "b"):
            self._buy_vol_acc  += notional
        else:
            self._sell_vol_acc += notional
        self._vol_acc += notional

        if self._vol_acc >= self.bucket_size:
            # Close current bucket
            self._buckets.append((self._buy_vol_acc, self._sell_vol_acc))
            self._buy_vol_acc  = 0.0
            self._sell_vol_acc = 0.0
            self._vol_acc      = 0.0
            self._emit_point(trade.timestamp_ms)

    def _emit_point(self, ts_ms: int):
        if len(self._buckets) < 2:
            return
        total_buy  = sum(b[0] for b in self._buckets)
        total_sell = sum(b[1] for b in self._buckets)
        total      = total_buy + total_sell
        if total == 0:
            return
        imbalance = abs(total_buy - total_sell) / total
        vpin = imbalance

        last_buy  = self._buckets[-1][0]
        last_sell = self._buckets[-1][1]

        alert = vpin > 0.7  # VPIN > 0.7 is considered high toxicity

        pt = VPINPoint(
            timestamp_ms=ts_ms, exchange=self.exchange,
            vpin=vpin, buy_vol_bucket=last_buy, sell_vol_bucket=last_sell,
            imbalance=imbalance, alert=alert,
        )
        self._history.append(pt)
        if len(self._history) > 500:
            self._history = self._history[-500:]

    def get_history(self) -> List[VPINPoint]:
        return list(self._history)

    def current_vpin(self) -> Optional[float]:
        return self._history[-1].vpin if self._history else None


# ══════════════════════════════════════════════════════════════════════════════
# 市场微结构异常检测
# ══════════════════════════════════════════════════════════════════════════════

def detect_microstructure_anomalies(snapshots: List[ExchangeSnapshot],
                                     ob_spread_history: Dict[str, List[float]],
                                     ob_depth_history: Dict[str, List[float]]
                                     ) -> List[MicrostructureAnomaly]:
    """检测盘口价差突扩、深度塌陷等微结构异常"""
    anomalies = []
    now_ms = int(time.time() * 1000)

    for snap in snapshots:
        if snap.status != "ok":
            continue
        ex = snap.exchange

        # 1. 价差突扩
        spreads = ob_spread_history.get(ex, [])
        if len(spreads) >= 10:
            recent_spread = spreads[-1] if spreads else 0
            avg_spread = sum(spreads[-20:]) / min(len(spreads), 20)
            if avg_spread > 0 and recent_spread > avg_spread * 3:
                anomalies.append(MicrostructureAnomaly(
                    timestamp_ms=now_ms, exchange=ex,
                    anomaly_type="spread_spike",
                    severity="high" if recent_spread > avg_spread * 5 else "medium",
                    detail=f"价差 {recent_spread:.2f}bps，均值 {avg_spread:.2f}bps",
                    value=recent_spread, threshold=avg_spread * 3,
                ))

        # 2. 深度塌陷
        depths = ob_depth_history.get(ex, [])
        if len(depths) >= 10:
            recent_depth = depths[-1] if depths else 0
            avg_depth = sum(depths[-20:]) / min(len(depths), 20)
            if avg_depth > 0 and recent_depth < avg_depth * 0.3:
                anomalies.append(MicrostructureAnomaly(
                    timestamp_ms=now_ms, exchange=ex,
                    anomaly_type="depth_collapse",
                    severity="high",
                    detail=f"深度 {recent_depth/1e6:.2f}M，均值 {avg_depth/1e6:.2f}M（跌至{recent_depth/avg_depth*100:.0f}%）",
                    value=recent_depth, threshold=avg_depth * 0.3,
                ))

    return anomalies


# ══════════════════════════════════════════════════════════════════════════════
# K线形态识别
# ══════════════════════════════════════════════════════════════════════════════

def detect_candle_patterns(candles: List[Candle], exchange: str, symbol: str,
                            min_confidence: float = 0.65) -> List[CandlePatternSignal]:
    """识别 K 线形态：Pin bar / 吞没 / 锤子 / 十字星 / RSI背离"""
    if len(candles) < 5:
        return []
    signals = []

    for i in range(2, len(candles)):
        c  = candles[i]
        p  = candles[i - 1]
        pp = candles[i - 2]
        body = abs(c.close - c.open)
        rng  = c.high - c.low if c.high > c.low else 1e-9
        upper_wick = c.high - max(c.open, c.close)
        lower_wick = min(c.open, c.close) - c.low

        # Pin bar (长影线)
        if lower_wick > body * 2.5 and lower_wick > upper_wick * 2:
            conf = min(0.95, lower_wick / rng)
            if conf >= min_confidence:
                signals.append(CandlePatternSignal(
                    timestamp_ms=c.timestamp_ms, exchange=exchange, symbol=symbol,
                    pattern="pin_bar_bullish", direction="bullish",
                    confidence=conf, price=c.close,
                ))
        elif upper_wick > body * 2.5 and upper_wick > lower_wick * 2:
            conf = min(0.95, upper_wick / rng)
            if conf >= min_confidence:
                signals.append(CandlePatternSignal(
                    timestamp_ms=c.timestamp_ms, exchange=exchange, symbol=symbol,
                    pattern="pin_bar_bearish", direction="bearish",
                    confidence=conf, price=c.close,
                ))

        # Engulfing (吞没)
        p_body = abs(p.close - p.open)
        if (p.close < p.open and c.close > c.open and  # bearish then bullish
                c.close > p.open and c.open < p.close and body > p_body * 1.2):
            signals.append(CandlePatternSignal(
                timestamp_ms=c.timestamp_ms, exchange=exchange, symbol=symbol,
                pattern="bullish_engulfing", direction="bullish",
                confidence=min(0.9, body / p_body * 0.5), price=c.close,
            ))
        elif (p.close > p.open and c.close < c.open and  # bullish then bearish
              c.close < p.open and c.open > p.close and body > p_body * 1.2):
            signals.append(CandlePatternSignal(
                timestamp_ms=c.timestamp_ms, exchange=exchange, symbol=symbol,
                pattern="bearish_engulfing", direction="bearish",
                confidence=min(0.9, body / p_body * 0.5), price=c.close,
            ))

        # Doji (十字星)
        if body / rng < 0.1 and rng > 0:
            signals.append(CandlePatternSignal(
                timestamp_ms=c.timestamp_ms, exchange=exchange, symbol=symbol,
                pattern="doji", direction="neutral",
                confidence=0.7, price=c.close,
            ))

    # RSI divergence (简化版：价格新高/新低但动量背离)
    if len(candles) >= 14:
        closes = [c.close for c in candles[-20:]]
        # Compute simplified RSI
        gains, losses = [], []
        for j in range(1, len(closes)):
            d = closes[j] - closes[j-1]
            gains.append(max(d, 0)); losses.append(max(-d, 0))
        if gains and losses:
            avg_gain = sum(gains[-14:]) / 14
            avg_loss = sum(losses[-14:]) / 14
            rsi = 100 - 100 / (1 + safe_div(avg_gain, avg_loss, 1))
            # Bullish divergence: price making lower lows but RSI making higher lows
            if closes[-1] < closes[-5] and rsi > 35:
                signals.append(CandlePatternSignal(
                    timestamp_ms=candles[-1].timestamp_ms, exchange=exchange, symbol=symbol,
                    pattern="bullish_divergence", direction="bullish",
                    confidence=0.72, price=candles[-1].close,
                ))
            elif closes[-1] > closes[-5] and rsi < 65:
                signals.append(CandlePatternSignal(
                    timestamp_ms=candles[-1].timestamp_ms, exchange=exchange, symbol=symbol,
                    pattern="bearish_divergence", direction="bearish",
                    confidence=0.72, price=candles[-1].close,
                ))

    return signals[-10:]  # return most recent


# ══════════════════════════════════════════════════════════════════════════════
# 多因子情绪评分
# ══════════════════════════════════════════════════════════════════════════════

_SENTIMENT_LABELS = [
    (-80, "极度恐惧", "#ff4444"),
    (-40, "恐惧",    "#ff8855"),
    (-10, "轻微恐惧", "#ffaa44"),
    (10,  "中性",    "#aaaaaa"),
    (40,  "轻微贪婪", "#88dd66"),
    (80,  "贪婪",    "#44cc44"),
    (101, "极度贪婪", "#00ff88"),
]

def _score_label(score: float) -> Tuple[str, str]:
    for threshold, label, color in _SENTIMENT_LABELS:
        if score < threshold:
            return label, color
    return "极度贪婪", "#00ff88"


def compute_sentiment_score(snap: ExchangeSnapshot,
                             oi_pts: List[OIPoint],
                             liq_events: List[LiquidationEvent],
                             vpin: Optional[float],
                             ls_ratio: Optional[float]) -> MarketSentimentScore:
    """
    计算综合市场情绪得分 -100 ~ +100。
    正值 = 贪婪/多头偏向，负值 = 恐惧/空头偏向。
    """
    now_ms = int(time.time() * 1000)
    coin   = snap.symbol.replace("USDT", "").replace("-USDT-SWAP", "")

    # 1. OI Score: OI增加 + 价格涨 = 多头推进 (+), OI增加 + 价格跌 = 空头推进 (-)
    oi_score = 0.0
    if len(oi_pts) >= 2:
        oi_now  = oi_pts[-1].open_interest_notional or 0
        oi_prev = oi_pts[-2].open_interest_notional or 0
        oi_chg  = safe_div(oi_now - oi_prev, oi_prev)
        price_chg = 0.0
        if snap.last_price and len(oi_pts) >= 2:
            price_chg = snap.last_price - (snap.last_price * 0.999)  # placeholder
        if oi_chg > 0.001:
            oi_score = 0.5  # OI rising
        elif oi_chg < -0.001:
            oi_score = -0.3  # OI falling
        oi_score = max(-1.0, min(1.0, oi_score))

    # 2. CVD Score: positive CVD = buyers dominating
    cvd_score = 0.0  # Will be injected externally in UI layer

    # 3. Funding Score: positive funding = overheated longs = bearish contrarian
    funding_score = 0.0
    if snap.funding_rate is not None:
        fr_bps = snap.funding_rate * 10000
        # Contrarian: high positive funding → bearish signal
        if fr_bps > 5:
            funding_score = -min(1.0, fr_bps / 20)
        elif fr_bps < -5:
            funding_score = min(1.0, abs(fr_bps) / 20)
        else:
            funding_score = 0.0

    # 4. L/S Score
    ls_score = 0.0
    if ls_ratio is not None:
        # ratio > 1 = more longs. Contrarian: extreme longs = bearish
        if ls_ratio > 1.5:
            ls_score = -min(1.0, (ls_ratio - 1) / 2)
        elif ls_ratio < 0.7:
            ls_score = min(1.0, (1 - ls_ratio) / 0.5)

    # 5. Liquidation Score: large long liquidations = bearish, short liq = bullish
    liq_score = 0.0
    if liq_events:
        now_ms_cur = int(time.time() * 1000)
        cutoff = now_ms_cur - 3600_000  # last 1h
        long_liq  = sum(e.notional or 0 for e in liq_events if e.side == "long"  and (e.timestamp_ms or 0) > cutoff)
        short_liq = sum(e.notional or 0 for e in liq_events if e.side == "short" and (e.timestamp_ms or 0) > cutoff)
        total_liq = long_liq + short_liq
        if total_liq > 0:
            liq_score = (short_liq - long_liq) / total_liq  # pos = short liqs dominant = bullish

    # 6. VPIN Score: high VPIN = toxic flow = directional move coming
    vpin_score = 0.0
    if vpin is not None:
        vpin_score = (vpin - 0.5) * 2  # map 0-1 to -1 +1

    # Weighted composite
    weights = {"oi": 0.25, "cvd": 0.20, "funding": 0.20, "ls": 0.15, "liq": 0.10, "vpin": 0.10}
    composite = (
        oi_score      * weights["oi"]      +
        cvd_score     * weights["cvd"]     +
        funding_score * weights["funding"] +
        ls_score      * weights["ls"]      +
        liq_score     * weights["liq"]     +
        vpin_score    * weights["vpin"]
    ) * 100  # scale to -100..+100

    label, color = _score_label(composite)
    return MarketSentimentScore(
        timestamp_ms=now_ms, exchange=snap.exchange, coin=coin,
        oi_score=oi_score, cvd_score=cvd_score,
        funding_score=funding_score, ls_score=ls_score,
        liq_score=liq_score, vpin_score=vpin_score,
        composite=composite, label=label, color=color,
    )


# ══════════════════════════════════════════════════════════════════════════════
# 简单信号回测引擎
# ══════════════════════════════════════════════════════════════════════════════

def backtest_candle_signal(candles: List[Candle], signals: List[CandlePatternSignal],
                            hold_bars: int = 3,
                            exchange: str = "", coin: str = "", interval: str = ""
                            ):
    """
    简单持仓回测：信号出现后持有 hold_bars 根K线，计算胜率和平均收益。
    返回 BacktestResult dataclass。
    """
    from models import BacktestResult
    if not candles or not signals:
        return None

    candle_map = {c.timestamp_ms: (i, c) for i, c in enumerate(candles)}
    wins, losses, returns = [], [], []

    for sig in signals:
        if sig.timestamp_ms not in candle_map:
            continue
        idx, entry_candle = candle_map[sig.timestamp_ms]
        if idx + hold_bars >= len(candles):
            continue
        exit_candle = candles[idx + hold_bars]
        entry_price = entry_candle.close
        exit_price  = exit_candle.close
        if entry_price == 0:
            continue

        if sig.direction == "bullish":
            ret = (exit_price - entry_price) / entry_price * 100
        else:
            ret = (entry_price - exit_price) / entry_price * 100

        returns.append(ret)
        if ret > 0: wins.append(ret)
        else:       losses.append(ret)

    n = len(returns)
    if n == 0:
        return None

    win_rate  = len(wins) / n
    avg_ret   = sum(returns) / n
    # Sharpe (simplified, rf=0)
    if len(returns) > 1:
        std = math.sqrt(sum((r - avg_ret)**2 for r in returns) / len(returns))
        sharpe = avg_ret / std if std > 0 else 0.0
    else:
        sharpe = 0.0

    # Max drawdown
    cum, peak, max_dd = 0.0, 0.0, 0.0
    for r in returns:
        cum += r
        peak = max(peak, cum)
        max_dd = max(max_dd, peak - cum)

    from models import BacktestResult
    return BacktestResult(
        signal_type=signals[0].pattern if signals else "",
        exchange=exchange, coin=coin, interval=interval,
        total_signals=n, win_count=len(wins), loss_count=len(losses),
        win_rate=win_rate, avg_return_pct=avg_ret,
        max_drawdown_pct=max_dd, sharpe=sharpe,
        from_ts=candles[0].timestamp_ms, to_ts=candles[-1].timestamp_ms,
    )
