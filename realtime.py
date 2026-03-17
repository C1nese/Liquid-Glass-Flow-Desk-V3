from __future__ import annotations

from collections import deque
from dataclasses import replace, field
import json
import math
import threading
import time
import uuid
from typing import Deque, Dict, List, Optional, Tuple

import websocket

from exchanges import EXCHANGE_ORDER, build_clients, compute_notional, normalize_liquidation_side, safe_float, safe_int
from models import (
    ExchangeSnapshot, LiquidationEvent, OIPoint, TradeEvent,
    CVDPoint, AlertRule, AlertEvent, LocalOrderBook, OrderBookLevel,
    IcebergAlert, LiquidityGap, SpotPerpSpreadPoint, SpotPerpAlert,
    ConfirmedAlert, AlertTimeline,
    OrderBookDeltaPoint, FakeWallCandidate, WallAbsorptionEvent, OrderBookQualitySnapshot,
    CompositeSignal,
    LiquidationClusterV2,
    RecordedFrame,
)

# ── Constants ─────────────────────────────────────────────────────────────────
_ALERT_METRICS = {
    "price": "最新价", "oi": "持仓金额", "funding": "资金费率",
    "liq_notional": "爆仓额(60min)", "cvd": "CVD累积", "spread_bps": "价差bps",
    "oi_velocity": "OI变化速率",
}

ICEBERG_REFILL_WINDOW_MS  = 30_000
ICEBERG_REFILL_MIN_COUNT  = 4
LIQUIDITY_GAP_DROP_PCT    = 0.50
SPREAD_HISTORY_SIZE       = 600

# Spot-perp thresholds
SPOT_LEAD_BPS_THRESHOLD   = 15.0
SPOT_PERP_DIVERGE_BPS     = 20.0
OI_CVD_WEAK_OI_PCT        = 0.8

# Alert debounce: must confirm N consecutive ticks
ALERT_CONFIRM_STRONG = 2   # strong alert: 2 confirms
ALERT_CONFIRM_MEDIUM = 3   # medium: 3 confirms
ALERT_CONFIRM_WEAK   = 4   # weak: 4 confirms

# Orderbook quality
FAKE_WALL_MIN_NOTIONAL = 200_000   # 20万 USD
FAKE_WALL_MAX_LIFE_MS  = 8_000     # 消失快于8秒=疑似假
ABSORPTION_REFILL_MS   = 5_000     # 5秒内补单=吸收

# Liquidation cluster
CLUSTER_WINDOW_MS       = 30_000   # 30s内
CLUSTER_MIN_NOTIONAL    = 100_000  # 10万起算
CLUSTER_CROSS_EX_WINDOW = 15_000   # 15s内跨所=联动

# Composite signal weights
_CS_WEIGHTS = {"price": 0.20, "oi": 0.25, "cvd": 0.25, "funding": 0.15, "crowd": 0.15}

# Recorder
RECORDER_MAX_FRAMES = 3600  # 1小时 @ 1fps


class LiveTerminalService:
    def __init__(self, symbol_map: Dict[str,str], timeout: int = 10,
                 sample_seconds: int = 15, history_size: int = 720,
                 liquidation_history_size: int = 600,
                 trade_history_size: int = 3000,
                 cvd_history_size: int = 800):
        self.symbol_map   = dict(symbol_map)
        self.timeout      = timeout
        self.sample_seconds = max(sample_seconds, 5)
        self.history_size   = max(history_size, 120)
        self.liq_hist_size  = max(liquidation_history_size, 120)
        self.trade_hist_sz  = max(trade_history_size, 200)
        self.cvd_hist_sz    = max(cvd_history_size, 100)

        self.clients   = build_clients(timeout=timeout)
        self.lock      = threading.Lock()
        self.stop_ev   = threading.Event()

        # ── Price / snapshot state ────────────────────────────────────────────
        self.live_by_exchange:    Dict[str, ExchangeSnapshot] = {}
        self.sampled_by_exchange: Dict[str, ExchangeSnapshot] = {}

        # ── History deques ────────────────────────────────────────────────────
        self.oi_history:    Dict[str, Deque[OIPoint]]          = {k: deque(maxlen=history_size) for k in EXCHANGE_ORDER}
        self.liq_history:   Dict[str, Deque[LiquidationEvent]] = {k: deque(maxlen=self.liq_hist_size) for k in EXCHANGE_ORDER}
        self.trade_history: Dict[str, Deque[TradeEvent]]        = {k: deque(maxlen=self.trade_hist_sz) for k in EXCHANGE_ORDER}
        self.cvd_history:   Dict[str, Deque[CVDPoint]]          = {k: deque(maxlen=self.cvd_hist_sz) for k in EXCHANGE_ORDER}
        self._cvd_accum:    Dict[str, float]                    = {k: 0.0 for k in EXCHANGE_ORDER}

        # ── Spot prices (from spot WS) ────────────────────────────────────────
        self._spot_price:  Dict[str, Optional[float]] = {k: None for k in EXCHANGE_ORDER}
        self._spot_vol24h: Dict[str, Optional[float]] = {k: None for k in EXCHANGE_ORDER}

        # ── Spot-perp spread ──────────────────────────────────────────────────
        self.spread_history:    Dict[str, Deque[SpotPerpSpreadPoint]] = {k: deque(maxlen=SPREAD_HISTORY_SIZE) for k in EXCHANGE_ORDER}
        self.spot_perp_alerts:  Deque[SpotPerpAlert]   = deque(maxlen=500)
        self._last_oi_for_alert: Dict[str, Optional[float]] = {k: None for k in EXCHANGE_ORDER}

        # ── Alert debounce state ──────────────────────────────────────────────
        # key -> {"count": int, "first_ms": int, "last_payload": dict}
        self._pending_alerts:   Dict[str, dict] = {}
        self.confirmed_alerts:  Deque[ConfirmedAlert] = deque(maxlen=300)
        self.alert_timeline:    Deque[AlertTimeline]  = deque(maxlen=1000)
        self._alert_cooldown:   Dict[str, float] = {}

        # ── Orderbook (perp + spot) ───────────────────────────────────────────
        self.local_books: Dict[str, LocalOrderBook] = {
            k: LocalOrderBook(exchange=k, symbol=symbol_map.get(k,"")) for k in EXCHANGE_ORDER}
        self.spot_books:  Dict[str, LocalOrderBook] = {
            k: LocalOrderBook(exchange=k, symbol=f"{k}_spot") for k in ("bybit","okx")}

        # Prev snapshots for delta
        self._prev_book_snapshot: Dict[str, Dict[float, float]] = {k:{} for k in EXCHANGE_ORDER}  # price->size
        self._book_first_seen_ms: Dict[str, Dict[float, int]]   = {k:{} for k in EXCHANGE_ORDER}  # price->ts

        # ── Orderbook quality tracking ────────────────────────────────────────
        self.ob_delta_history:   Dict[str, Deque[OrderBookDeltaPoint]]       = {k: deque(maxlen=500)  for k in EXCHANGE_ORDER}
        self.fake_wall_history:  Dict[str, Deque[FakeWallCandidate]]         = {k: deque(maxlen=200)  for k in EXCHANGE_ORDER}
        self.absorption_history: Dict[str, Deque[WallAbsorptionEvent]]       = {k: deque(maxlen=200)  for k in EXCHANGE_ORDER}
        self.ob_quality_history: Dict[str, Deque[OrderBookQualitySnapshot]]  = {k: deque(maxlen=600)  for k in EXCHANGE_ORDER}
        self._ob_quality_window: Dict[str, dict] = {k: {"bid_add":0.,"bid_cancel":0.,"ask_add":0.,"ask_cancel":0.,"fake":0,"absorb":0} for k in EXCHANGE_ORDER}

        # ── Iceberg / gap ─────────────────────────────────────────────────────
        self._iceberg_tracker: Dict[str, Dict[float, list]] = {k:{} for k in EXCHANGE_ORDER}
        self.iceberg_alerts:   Deque[IcebergAlert]  = deque(maxlen=200)
        self.liquidity_gaps:   Deque[LiquidityGap]  = deque(maxlen=200)

        # ── Standard alert rules ──────────────────────────────────────────────
        self.alert_rules:  List[AlertRule]  = []
        self.alert_events: Deque[AlertEvent] = deque(maxlen=300)

        # ── Composite signals ─────────────────────────────────────────────────
        self.composite_signals: Dict[str, Deque[CompositeSignal]] = {k: deque(maxlen=300) for k in EXCHANGE_ORDER}
        self._price_history_short: Dict[str, Deque[float]] = {k: deque(maxlen=30) for k in EXCHANGE_ORDER}

        # ── Liquidation clusters v2 ───────────────────────────────────────────
        self.liq_clusters_v2: Deque[LiquidationClusterV2] = deque(maxlen=200)
        self._cluster_builder: Dict[str, list] = {"_all": []}  # rolling buffer

        # ── Event recorder ────────────────────────────────────────────────────
        self.recorder_active:  bool = False
        self.recorded_frames:  Deque[RecordedFrame] = deque(maxlen=RECORDER_MAX_FRAMES)
        self._recorder_thread: Optional[threading.Thread] = None

        # ── Binance depth buffer ──────────────────────────────────────────────
        self._binance_depth_buffer: Dict[str, list] = {k:[] for k in EXCHANGE_ORDER}
        self._binance_depth_ready:  Dict[str, bool]  = {k:False for k in EXCHANGE_ORDER}

        self.ws_apps: Dict[str, websocket.WebSocketApp] = {}
        self.threads: List[threading.Thread] = []

        self._sample_once()
        self._start_threads()

    # ══════════════════════════════════════════════════════════════════════════
    # Public API
    # ══════════════════════════════════════════════════════════════════════════

    def stop(self):
        self.stop_ev.set()
        for ws in list(self.ws_apps.values()):
            try: ws.close()
            except: pass

    def current_snapshots(self) -> List[ExchangeSnapshot]:
        snapshots = []
        with self.lock:
            for ek in EXCHANGE_ORDER:
                live    = self.live_by_exchange.get(ek)
                sampled = self.sampled_by_exchange.get(ek)
                exname  = self.clients[ek].exchange_name
                sym     = self.symbol_map.get(ek,"")
                if sampled is not None: merged = replace(sampled)
                elif live is not None:  merged = replace(live)
                else: merged = ExchangeSnapshot(exchange=exname, symbol=sym, status="error", error="waiting")
                if live is not None:
                    for fn in ("last_price","mark_price","index_price","open_interest",
                               "open_interest_notional","funding_rate","volume_24h_base",
                               "volume_24h_notional","timestamp_ms"):
                        v = getattr(live, fn)
                        if v is not None: setattr(merged, fn, v)
                    if live.status == "ok":
                        merged.status = "ok"; merged.error = None
                book = self.local_books.get(ek)
                if book and book.is_ready:
                    mid = book.mid_price()
                    if mid and merged.last_price is None:
                        merged.last_price = mid
                sp = self._spot_price.get(ek)
                if sp: merged.spot_price = sp
                sv = self._spot_vol24h.get(ek)
                if sv: merged.spot_volume_24h = sv
                snapshots.append(merged)
        return snapshots

    def get_local_book(self, ek): 
        with self.lock: return self.local_books[ek]
    def get_spot_book(self, ek):
        with self.lock: return self.spot_books.get(ek)
    def get_local_book_levels(self, ek, depth=200):
        with self.lock:
            book = self.local_books[ek]
            return book.to_levels(depth) if book.is_ready else []
    def get_oi_history(self, ek):
        with self.lock: return list(self.oi_history.get(ek,[]))
    def get_liquidation_history(self, ek):
        with self.lock: return list(self.liq_history.get(ek,[]))
    def get_trade_history(self, ek):
        with self.lock: return list(self.trade_history.get(ek,[]))
    def get_cvd_history(self, ek):
        with self.lock: return list(self.cvd_history.get(ek,[]))
    def get_alert_events(self):
        with self.lock: return list(self.alert_events)
    def get_iceberg_alerts(self):
        with self.lock: return list(self.iceberg_alerts)
    def get_liquidity_gaps(self):
        with self.lock: return list(self.liquidity_gaps)
    def set_alert_rules(self, rules):
        with self.lock: self.alert_rules = list(rules)
    def get_alert_rules(self):
        with self.lock: return list(self.alert_rules)
    def get_spread_history(self, ek):
        with self.lock: return list(self.spread_history.get(ek,[]))
    def get_spot_perp_alerts(self):
        with self.lock: return list(self.spot_perp_alerts)
    def get_all_spread_history(self):
        with self.lock: return {k: list(v) for k,v in self.spread_history.items()}
    def get_confirmed_alerts(self):
        with self.lock: return list(self.confirmed_alerts)
    def get_alert_timeline(self):
        with self.lock: return list(self.alert_timeline)
    def get_ob_delta_history(self, ek):
        with self.lock: return list(self.ob_delta_history.get(ek,[]))
    def get_fake_walls(self, ek):
        with self.lock: return list(self.fake_wall_history.get(ek,[]))
    def get_absorption_events(self, ek):
        with self.lock: return list(self.absorption_history.get(ek,[]))
    def get_ob_quality_history(self, ek):
        with self.lock: return list(self.ob_quality_history.get(ek,[]))
    def get_composite_signals(self, ek):
        with self.lock: return list(self.composite_signals.get(ek,[]))
    def get_liq_clusters_v2(self):
        with self.lock: return list(self.liq_clusters_v2)
    def get_recorded_frames(self):
        with self.lock: return list(self.recorded_frames)
    def start_recording(self):
        with self.lock: self.recorder_active = True
    def stop_recording(self):
        with self.lock: self.recorder_active = False
    def clear_recording(self):
        with self.lock: self.recorded_frames.clear()

    # ══════════════════════════════════════════════════════════════════════════
    # Threading
    # ══════════════════════════════════════════════════════════════════════════

    def _start_threads(self):
        t = threading.Thread(target=self._run_sampler, name="sampler", daemon=True)
        t.start(); self.threads.append(t)
        for ek in EXCHANGE_ORDER:
            w = threading.Thread(target=self._run_ws_worker, args=(ek,), name=f"ws-{ek}", daemon=True)
            w.start(); self.threads.append(w)
        for ek in ("bybit","okx"):
            ws = threading.Thread(target=self._run_spot_ws_worker, args=(ek,), name=f"ws-spot-{ek}", daemon=True)
            ws.start(); self.threads.append(ws)
        db = threading.Thread(target=self._run_binance_depth_init, name="binance-depth-init", daemon=True)
        db.start(); self.threads.append(db)
        # Recorder thread
        rec = threading.Thread(target=self._run_recorder, name="recorder", daemon=True)
        rec.start(); self.threads.append(rec)
        # Cluster builder
        cl = threading.Thread(target=self._run_cluster_builder, name="cluster-builder", daemon=True)
        cl.start(); self.threads.append(cl)

    def _run_sampler(self):
        while not self.stop_ev.is_set():
            self._sample_once()
            if self.stop_ev.wait(self.sample_seconds): return

    def _sample_once(self):
        for ek in EXCHANGE_ORDER:
            sym = self.symbol_map.get(ek)
            if not sym: continue
            try: snap = self.clients[ek].fetch(sym)
            except Exception as e:
                snap = ExchangeSnapshot(exchange=self.clients[ek].exchange_name,
                                        symbol=sym, status="error", error=str(e))
            with self.lock:
                self.sampled_by_exchange[ek] = snap
                if snap.status == "ok":
                    self._append_oi_locked(ek, snap)
                    self.live_by_exchange.setdefault(ek, replace(snap))
                    self._check_alerts_locked(ek, snap)
                    # Update price history for composite signal
                    if snap.last_price:
                        self._price_history_short[ek].append(snap.last_price)
                    self._compute_composite_signal_locked(ek, snap)

    # ══════════════════════════════════════════════════════════════════════════
    # Recorder
    # ══════════════════════════════════════════════════════════════════════════

    def _run_recorder(self):
        while not self.stop_ev.is_set():
            if self.stop_ev.wait(1.0): return
            with self.lock:
                if not self.recorder_active: continue
                prices = {ek: (self.live_by_exchange[ek].last_price if ek in self.live_by_exchange else None) for ek in EXCHANGE_ORDER}
                oi_n   = {ek: (self.live_by_exchange[ek].open_interest_notional if ek in self.live_by_exchange else None) for ek in EXCHANGE_ORDER}
                cvd_v  = {ek: self._cvd_accum.get(ek, 0.0) for ek in EXCHANGE_ORDER}
                liqs   = [e for ek in EXCHANGE_ORDER for e in list(self.liq_history[ek])[-3:]]
                spread = {ek: (list(self.spread_history[ek])[-1].spread_bps if self.spread_history[ek] else None) for ek in EXCHANGE_ORDER}
                comp   = {ek: list(self.composite_signals[ek])[-1] if self.composite_signals[ek] else None for ek in EXCHANGE_ORDER}
                fund   = {ek: (self.live_by_exchange[ek].funding_rate if ek in self.live_by_exchange else None) for ek in EXCHANGE_ORDER}
                self.recorded_frames.append(RecordedFrame(
                    timestamp_ms=int(time.time()*1000),
                    prices=prices, oi_notionals=oi_n, cvd_values=cvd_v,
                    liq_events=liqs, spread_bps=spread,
                    composite_signals={k: v for k,v in comp.items() if v},
                    funding_rates=fund))

    # ══════════════════════════════════════════════════════════════════════════
    # Liquidation Cluster Builder v2
    # ══════════════════════════════════════════════════════════════════════════

    def _run_cluster_builder(self):
        while not self.stop_ev.is_set():
            if self.stop_ev.wait(5.0): return
            with self.lock:
                self._build_clusters_locked()

    def _build_clusters_locked(self):
        now_ms = int(time.time()*1000)
        # Collect all recent liquidations across exchanges
        all_evts: List[Tuple[int, str, LiquidationEvent]] = []
        for ek in EXCHANGE_ORDER:
            for e in self.liq_history[ek]:
                if now_ms - e.timestamp_ms <= 300_000:  # last 5 min
                    all_evts.append((e.timestamp_ms, ek, e))
        all_evts.sort(key=lambda x: x[0])
        if not all_evts: return

        clusters: List[LiquidationClusterV2] = []
        used = set()
        for i, (ts, ek, ev) in enumerate(all_evts):
            if i in used: continue
            # Start a new cluster
            cluster_evts = [(ts, ek, ev)]
            cluster_exch = {ek}
            for j, (ts2, ek2, ev2) in enumerate(all_evts[i+1:], start=i+1):
                if ts2 - ts > CLUSTER_WINDOW_MS: break
                if j not in used:
                    cluster_evts.append((ts2, ek2, ev2))
                    cluster_exch.add(ek2)
                    used.add(j)
            used.add(i)
            total_n = sum(e.notional or 0 for _,_,e in cluster_evts)
            if total_n < CLUSTER_MIN_NOTIONAL: continue
            long_n  = sum(e.notional or 0 for _,_,e in cluster_evts if e.side=="long")
            short_n = sum(e.notional or 0 for _,_,e in cluster_evts if e.side=="short")
            duration = cluster_evts[-1][0] - cluster_evts[0][0] if len(cluster_evts)>1 else 1000
            intensity = total_n / max(duration/1000, 0.1)
            cross = len(cluster_exch) >= 2
            cascade_score = min(1.0, (math.log10(max(total_n,1)) - 4) * 0.25 + (0.3 if cross else 0))
            c = LiquidationClusterV2(
                cluster_id=str(uuid.uuid4())[:8],
                start_ms=cluster_evts[0][0], end_ms=cluster_evts[-1][0],
                duration_ms=duration, exchanges=list(cluster_exch),
                cross_exchange=cross,
                long_count=sum(1 for _,_,e in cluster_evts if e.side=="long"),
                short_count=sum(1 for _,_,e in cluster_evts if e.side=="short"),
                long_notional=long_n, short_notional=short_n,
                total_notional=total_n,
                dominant_side="long" if long_n>=short_n else "short",
                intensity=intensity, cascade_score=min(1.0, cascade_score))
            clusters.append(c)

        # Deduplicate with existing clusters by start_ms window
        existing_starts = {c.start_ms for c in self.liq_clusters_v2}
        for c in clusters:
            if not any(abs(c.start_ms - es) < 5000 for es in existing_starts):
                self.liq_clusters_v2.append(c)
                existing_starts.add(c.start_ms)

    # ══════════════════════════════════════════════════════════════════════════
    # Alert Debounce Engine
    # ══════════════════════════════════════════════════════════════════════════

    def _stage_alert(self, key: str, alert_type: str, exchange: str,
                     severity: str, message: str, score: float = 0.5, extra: dict = None):
        """Stage an alert for confirmation. Only fires after N consecutive triggers."""
        now_ms  = int(time.time() * 1000)
        now_sec = time.time()
        confirm_needed = {"strong": ALERT_CONFIRM_STRONG,
                          "medium": ALERT_CONFIRM_MEDIUM,
                          "weak":   ALERT_CONFIRM_WEAK}.get(severity, ALERT_CONFIRM_MEDIUM)
        if key not in self._pending_alerts:
            self._pending_alerts[key] = {"count":0, "first_ms": now_ms, "severity": severity}
        state = self._pending_alerts[key]
        # Reset if too much time passed since last trigger
        if now_ms - state.get("last_ms", now_ms) > 60_000:
            state["count"] = 0
            state["first_ms"] = now_ms
        state["count"] += 1
        state["last_ms"] = now_ms
        state["message"] = message
        state["score"]   = score

        # Check cooldown
        cooldown = {"strong": 90, "medium": 180, "weak": 300}.get(severity, 180)
        if now_sec - self._alert_cooldown.get(key, 0) < cooldown:
            return  # still cooling down

        if state["count"] >= confirm_needed:
            # Fire confirmed alert
            ca = ConfirmedAlert(
                alert_id=str(uuid.uuid4())[:8],
                alert_type=alert_type, exchange=exchange,
                severity=severity, message=message,
                first_seen_ms=state["first_ms"], confirmed_at_ms=now_ms,
                confirm_count=state["count"], score=score, extra=extra or {})
            self.confirmed_alerts.append(ca)
            self.alert_timeline.append(AlertTimeline(
                timestamp_ms=now_ms, alert_type=alert_type,
                exchange=exchange, severity=severity, message=message, score=score))
            # Also push to legacy spot_perp_alerts for backward compat
            self.spot_perp_alerts.append(SpotPerpAlert(
                timestamp_ms=now_ms, exchange=exchange, alert_type=alert_type,
                message=f"[✓×{state['count']}] {message}", severity=severity,
                spread_bps=extra.get("spread_bps") if extra else None,
                oi_change_pct=extra.get("oi_change_pct") if extra else None,
                cvd_delta=extra.get("cvd_delta") if extra else None))
            self._alert_cooldown[key] = now_sec
            state["count"] = 0  # reset after fire

    # ══════════════════════════════════════════════════════════════════════════
    # Spot-Perp Alerts (now with debounce)
    # ══════════════════════════════════════════════════════════════════════════

    def _update_spot_price_locked(self, ek: str, spot_price: float, vol24h: Optional[float] = None):
        self._spot_price[ek] = spot_price
        if vol24h: self._spot_vol24h[ek] = vol24h
        live = self.live_by_exchange.get(ek)
        perp_price = live.last_price if live else None
        if perp_price and spot_price > 0:
            spread_bps = (perp_price - spot_price) / spot_price * 10000.0
            pt = SpotPerpSpreadPoint(
                timestamp_ms=int(time.time()*1000),
                exchange=ek, spot_price=spot_price,
                perp_price=perp_price, spread_bps=spread_bps)
            self.spread_history[ek].append(pt)
            self._check_spot_perp_debounced(ek, pt)

    def _check_spot_perp_debounced(self, ek: str, pt: SpotPerpSpreadPoint):
        exname = self.clients[ek].exchange_name if ek in self.clients else ek
        hist = list(self.spread_history[ek])
        if len(hist) < 5: return
        old_spread  = hist[-5].spread_bps
        spread_chg  = pt.spread_bps - old_spread
        abs_spread  = abs(pt.spread_bps)

        # Spot leading up (spread drops = spot rose more than perp)
        if spread_chg < -SPOT_LEAD_BPS_THRESHOLD:
            sev = "strong" if abs(spread_chg) > SPOT_PERP_DIVERGE_BPS else "medium"
            self._stage_alert(
                f"{ek}_spot_lead_up", "spot_lead_up", exname, sev,
                f"【{exname}】现货先拉↑ 合约未跟！价差变化 {spread_chg:+.1f}bps → 当前 {pt.spread_bps:+.1f}bps",
                score=min(1.0, abs(spread_chg)/30), extra={"spread_bps": pt.spread_bps})
        elif spread_chg > SPOT_LEAD_BPS_THRESHOLD:
            sev = "strong" if abs(spread_chg) > SPOT_PERP_DIVERGE_BPS else "medium"
            self._stage_alert(
                f"{ek}_spot_lead_down", "spot_lead_down", exname, sev,
                f"【{exname}】现货先跌↓ 合约未跟！价差变化 {spread_chg:+.1f}bps → 当前 {pt.spread_bps:+.1f}bps",
                score=min(1.0, abs(spread_chg)/30), extra={"spread_bps": pt.spread_bps})
        else:
            # Gradually decay pending alert count when no signal
            for k in [f"{ek}_spot_lead_up", f"{ek}_spot_lead_down"]:
                if k in self._pending_alerts:
                    self._pending_alerts[k]["count"] = max(0, self._pending_alerts[k].get("count",0)-1)

        # Extreme diverge
        if abs_spread > SPOT_PERP_DIVERGE_BPS * 2.5:
            self._stage_alert(
                f"{ek}_diverge", "diverge_extreme", exname, "strong",
                f"【{exname}】极端乖离！合约 vs 现货差 {pt.spread_bps:+.1f}bps",
                score=min(1.0, abs_spread/100), extra={"spread_bps": pt.spread_bps})

    def _check_oi_cvd_debounced(self, ek: str):
        exname = self.clients[ek].exchange_name if ek in self.clients else ek
        oi_hist  = list(self.oi_history.get(ek,[]))
        cvd_hist = list(self.cvd_history.get(ek,[]))
        if len(oi_hist) < 3 or len(cvd_hist) < 10: return
        oi_new = oi_hist[-1].open_interest_notional or oi_hist[-1].open_interest or 0
        oi_old = oi_hist[-3].open_interest_notional or oi_hist[-3].open_interest or 0
        if oi_old <= 0: return
        oi_chg_pct = (oi_new - oi_old) / oi_old * 100
        cvd_net    = sum(c.delta for c in cvd_hist[-10:])
        score      = min(1.0, abs(oi_chg_pct) / 3)
        if oi_chg_pct > OI_CVD_WEAK_OI_PCT and cvd_net < 0:
            self._stage_alert(f"{ek}_oi_up_cvd_weak", "oi_up_cvd_weak", exname, "medium",
                f"【{exname}】OI升+{oi_chg_pct:.2f}% 但主动卖主导 CVD {cvd_net/1e6:.2f}M ⚠️ 多头加仓遇抛压",
                score=score, extra={"oi_change_pct":oi_chg_pct,"cvd_delta":cvd_net})
        elif oi_chg_pct < -OI_CVD_WEAK_OI_PCT and cvd_net > 0:
            self._stage_alert(f"{ek}_oi_down_cvd_up", "oi_down_cvd_up", exname, "medium",
                f"【{exname}】OI降{oi_chg_pct:.2f}% 但主动买主导 CVD +{cvd_net/1e6:.2f}M 🔵 空头回补/轧空",
                score=score, extra={"oi_change_pct":oi_chg_pct,"cvd_delta":cvd_net})

    def _check_crowd_liq_combo_locked(self, ek: str):
        now = time.time()
        cutoff = int(now*1000) - 60000
        recent  = [e for e in self.liq_history[ek] if e.timestamp_ms >= cutoff]
        if len(recent) < 3: return
        total_n = sum(e.notional or 0 for e in recent)
        if total_n < 50_000: return
        exname = self.clients[ek].exchange_name if ek in self.clients else ek
        long_n  = sum(e.notional or 0 for e in recent if e.side=="long")
        short_n = total_n - long_n
        dominant = "多头" if long_n >= short_n else "空头"
        score = min(1.0, math.log10(max(total_n,1))/6)
        self._stage_alert(f"{ek}_crowd_liq", "crowd_liq_combo", exname, "strong",
            f"【{exname}】账户拥挤+爆仓联动⚡ 60s内{len(recent)}单 ${total_n/1e3:.0f}K {dominant}主导",
            score=score)

    # ══════════════════════════════════════════════════════════════════════════
    # Orderbook Quality & Fake Wall Detection
    # ══════════════════════════════════════════════════════════════════════════

    def _detect_orderbook_patterns_locked(self, ek: str, book: LocalOrderBook):
        now_ms = int(time.time()*1000)
        # Build current notional snapshot
        curr_size: Dict[float, float] = {}
        for p, s in {**book.bids, **book.asks}.items():
            curr_size[p] = s
        prev_size = self._prev_book_snapshot.get(ek, {})
        first_seen = self._book_first_seen_ms.get(ek, {})

        qw = self._ob_quality_window[ek]
        deltas: List[OrderBookDeltaPoint] = []

        all_prices = set(list(curr_size.keys()) + list(prev_size.keys()))
        for price in all_prices:
            cs = curr_size.get(price, 0.0)
            ps = prev_size.get(price, 0.0)
            if abs(cs - ps) < 1e-9: continue
            delta = cs - ps
            notional = abs(delta) * price
            if notional < 500: continue  # ignore tiny changes

            side = "bid" if price in book.bids else "ask"
            if delta > 0:
                event_type = "add"
                if side == "bid": qw["bid_add"] += notional
                else: qw["ask_add"] += notional
                if price not in first_seen:
                    first_seen[price] = now_ms
            else:
                event_type = "cancel"
                if side == "bid": qw["bid_cancel"] += notional
                else: qw["ask_cancel"] += notional
                # Check if this was a fake wall
                appeared_ms = first_seen.pop(price, None)
                if appeared_ms and ps >= FAKE_WALL_MIN_NOTIONAL / price:
                    lifespan = now_ms - appeared_ms
                    if lifespan <= FAKE_WALL_MAX_LIFE_MS:
                        fw = FakeWallCandidate(
                            timestamp_ms=now_ms, exchange=ek, side=side,
                            price=price, peak_size=ps,
                            peak_notional=ps*price, lifespan_ms=lifespan)
                        self.fake_wall_history[ek].append(fw)
                        qw["fake"] += 1
                        self._stage_alert(
                            f"{ek}_fake_wall_{price:.0f}", "fake_wall", 
                            self.clients[ek].exchange_name, "weak",
                            f"【{self.clients[ek].exchange_name}】疑似假挂单 {side} @{price:.2f} 存续{lifespan}ms 撤单${ps*price/1e3:.0f}K",
                            score=min(1.0, ps*price/1e6), extra={"price":price,"side":side,"lifespan_ms":lifespan})

            dp = OrderBookDeltaPoint(
                timestamp_ms=now_ms, exchange=ek, side=side,
                price=price, prev_size=ps, curr_size=cs,
                delta_size=delta, delta_notional=delta*price,
                event_type=event_type)
            deltas.append(dp)

        self.ob_delta_history[ek].extend(deltas)

        # Liquidity gap detection
        for price, prev_notional in {p: prev_size[p]*p for p in prev_size}.items():
            curr_notional = curr_size.get(price, 0.0) * price
            if prev_notional > 0 and curr_notional < prev_notional * (1 - LIQUIDITY_GAP_DROP_PCT):
                drop_pct = (prev_notional - curr_notional) / prev_notional
                s = "bid" if price in book.bids else "ask"
                self.liquidity_gaps.append(LiquidityGap(
                    timestamp_ms=now_ms, exchange=ek, symbol=book.symbol,
                    price_low=price*0.9999, price_high=price*1.0001,
                    side=s, prev_notional=prev_notional,
                    curr_notional=curr_notional, drop_pct=drop_pct))

        # Snapshot quality every ~60 updates
        if len(deltas) > 0 or True:
            bid_net = qw["bid_add"] - qw["bid_cancel"]
            ask_net = qw["ask_add"] - qw["ask_cancel"]
            # Quality score: high add low cancel = good liquidity
            add_total  = qw["bid_add"] + qw["ask_add"] + 1
            canc_total = qw["bid_cancel"] + qw["ask_cancel"] + 1
            q_score = max(0.0, min(1.0, 0.5 + (add_total - canc_total*1.5) / max(add_total + canc_total, 1) * 0.5))
            snap = OrderBookQualitySnapshot(
                timestamp_ms=now_ms, exchange=ek,
                bid_add_notional=qw["bid_add"], bid_cancel_notional=qw["bid_cancel"],
                ask_add_notional=qw["ask_add"], ask_cancel_notional=qw["ask_cancel"],
                bid_net_notional=bid_net, ask_net_notional=ask_net,
                fake_wall_count=qw["fake"], quality_score=q_score)
            self.ob_quality_history[ek].append(snap)
            # Reset window
            for k in qw: qw[k] = 0.0 if isinstance(qw[k], float) else 0

        self._prev_book_snapshot[ek] = dict(curr_size)
        self._book_first_seen_ms[ek] = dict(first_seen)

    # ══════════════════════════════════════════════════════════════════════════
    # Composite Signal Engine
    # ══════════════════════════════════════════════════════════════════════════

    def _compute_composite_signal_locked(self, ek: str, snap: ExchangeSnapshot):
        """OI + CVD + Funding + Crowd + Price → single composite score"""
        now_ms = int(time.time() * 1000)

        # 1. Price momentum score (-1 to +1)
        ph = list(self._price_history_short[ek])
        if len(ph) >= 5:
            p_chg = (ph[-1] - ph[-5]) / max(ph[-5], 1e-9)
            price_score = max(-1.0, min(1.0, p_chg * 200))  # scale
        else:
            price_score = 0.0

        # 2. OI delta score
        oi_hist = list(self.oi_history.get(ek, []))
        if len(oi_hist) >= 3:
            oi_new = oi_hist[-1].open_interest_notional or oi_hist[-1].open_interest or 0
            oi_old = oi_hist[-3].open_interest_notional or oi_hist[-3].open_interest or 0
            oi_chg_pct = (oi_new - oi_old) / max(oi_old, 1) * 100
            # OI rising with price = bullish, OI rising against price = bearish
            if price_score > 0:
                oi_score = max(-1., min(1., oi_chg_pct / 2))   # OI up + price up = +
            else:
                oi_score = max(-1., min(1., -oi_chg_pct / 2))  # OI up + price down = -
        else:
            oi_score = 0.0

        # 3. CVD score
        cvd_hist = list(self.cvd_history.get(ek, []))
        if len(cvd_hist) >= 10:
            recent_cvd = cvd_hist[-10:]
            cvd_net = sum(c.delta for c in recent_cvd)
            vol_ref = sum(c.buy_volume + c.sell_volume for c in recent_cvd) + 1
            cvd_score = max(-1., min(1., cvd_net / vol_ref * 3))
        else:
            cvd_score = 0.0

        # 4. Funding score (negative funding = shorts paying = bullish)
        fr = snap.funding_rate or 0.0
        # funding > 0 = longs paying = crowded long = slightly bearish signal
        funding_score = max(-1., min(1., -fr * 3000))

        # 5. Crowd (L/S ratio from sampled snapshot — approximation via funding)
        # Use funding as proxy: extreme positive = long crowded (bearish pressure)
        crowd_score = max(-1., min(1., -fr * 5000))

        # Weighted composite
        composite = (
            _CS_WEIGHTS["price"]   * price_score +
            _CS_WEIGHTS["oi"]      * oi_score +
            _CS_WEIGHTS["cvd"]     * cvd_score +
            _CS_WEIGHTS["funding"] * funding_score +
            _CS_WEIGHTS["crowd"]   * crowd_score
        )
        composite = max(-1., min(1., composite))

        # Label
        if composite > 0.45:
            label, color = "偏多推进 ▲", "#1dc796"
        elif composite < -0.45:
            label, color = "偏空推进 ▼", "#ff6868"
        elif abs(composite) < 0.15:
            # Low momentum — check for exhaustion
            if abs(fr) > 0.0005:
                label, color = "拥挤衰竭 ⚡", "#ffa94d"
            else:
                label, color = "吸收中 ≈", "#62c2ff"
        elif composite > 0:
            label, color = "弱多 →", "#a8ff78"
        else:
            label, color = "弱空 ←", "#ff9a9a"

        # Confidence: higher when factors agree
        scores = [price_score, oi_score, cvd_score, funding_score, crowd_score]
        signs  = [1 if s > 0.05 else -1 if s < -0.05 else 0 for s in scores]
        non_zero = [s for s in signs if s != 0]
        if non_zero:
            dominant = max(set(non_zero), key=non_zero.count)
            agreement = sum(1 for s in non_zero if s == dominant) / len(non_zero)
        else:
            agreement = 0.0
        confidence = agreement * abs(composite)

        cs = CompositeSignal(
            timestamp_ms=now_ms, exchange=ek,
            price_score=price_score, oi_score=oi_score,
            cvd_score=cvd_score, funding_score=funding_score,
            crowd_score=crowd_score, composite_score=composite,
            signal_label=label, signal_color=color,
            confidence=confidence)
        self.composite_signals[ek].append(cs)

        # Fire alert for extreme composite
        if abs(composite) > 0.6 and confidence > 0.6:
            exname = self.clients[ek].exchange_name
            sev = "strong" if abs(composite) > 0.75 else "medium"
            self._stage_alert(
                f"{ek}_composite_{('bull' if composite>0 else 'bear')}",
                "composite_signal", exname, sev,
                f"【{exname}】合成信号 {label} 综合分 {composite:+.2f} 置信度 {confidence:.0%}",
                score=abs(composite), extra={"composite":composite,"confidence":confidence})

    # ══════════════════════════════════════════════════════════════════════════
    # Trade & CVD
    # ══════════════════════════════════════════════════════════════════════════

    def _append_trade_locked(self, ek: str, trade: TradeEvent):
        self.trade_history[ek].append(trade)
        delta = trade.notional if trade.side == "buy" else -trade.notional
        self._cvd_accum[ek] += delta
        self.cvd_history[ek].append(CVDPoint(
            timestamp_ms=trade.timestamp_ms, cvd=self._cvd_accum[ek],
            delta=delta,
            buy_volume=trade.notional if trade.side=="buy" else 0.0,
            sell_volume=trade.notional if trade.side=="sell" else 0.0,
            price=trade.price))
        self._track_iceberg_locked(ek, trade)

    def _track_iceberg_locked(self, ek: str, trade: TradeEvent):
        now_ms = trade.timestamp_ms
        price  = round(trade.price, 2)
        tracker = self._iceberg_tracker[ek]
        if price not in tracker: tracker[price] = []
        tracker[price].append((now_ms, trade.notional))
        tracker[price] = [(ts, n) for ts, n in tracker[price] if now_ms - ts < ICEBERG_REFILL_WINDOW_MS]
        if len(tracker[price]) >= ICEBERG_REFILL_MIN_COUNT:
            total_notional = sum(n for _, n in tracker[price])
            self.iceberg_alerts.append(IcebergAlert(timestamp_ms=now_ms, exchange=ek,
                symbol=trade.symbol, price=price, side=trade.side,
                refill_count=len(tracker[price]), total_notional=total_notional))
            tracker[price] = []

    def _append_liq_locked(self, ek: str, event: LiquidationEvent):
        history = self.liq_history[ek]
        eid = (event.timestamp_ms, event.side, round(event.price or 0,6), round(event.size or 0,6))
        if history:
            last = history[-1]
            lid  = (last.timestamp_ms, last.side, round(last.price or 0,6), round(last.size or 0,6))
            if lid == eid: return
        history.append(event)
        self._check_crowd_liq_combo_locked(ek)

    def _append_oi_locked(self, ek: str, snap: ExchangeSnapshot):
        if snap.open_interest is None and snap.open_interest_notional is None: return
        history = self.oi_history[ek]
        pt = OIPoint(snap.timestamp_ms or int(time.time()*1000),
                     snap.open_interest, snap.open_interest_notional)
        if history and abs(history[-1].timestamp_ms - pt.timestamp_ms) <= 1000:
            history[-1] = pt
        else:
            history.append(pt)
        self._check_oi_cvd_debounced(ek)

    # ══════════════════════════════════════════════════════════════════════════
    # Alert Engine (standard rules)
    # ══════════════════════════════════════════════════════════════════════════

    def _check_alerts_locked(self, ek: str, snap: ExchangeSnapshot):
        now_ms = int(time.time()*1000)
        for rule in self.alert_rules:
            if not rule.enabled or rule.exchange != ek: continue
            value = self._get_metric_value(rule.metric, snap, ek)
            if value is None: continue
            prev_val = rule.last_value
            rule.last_value = value
            triggered = False
            if rule.condition == "above" and value > rule.threshold: triggered = True
            elif rule.condition == "below" and value < rule.threshold: triggered = True
            elif rule.condition == "cross_up" and prev_val is not None and prev_val <= rule.threshold < value: triggered = True
            elif rule.condition == "cross_down" and prev_val is not None and prev_val >= rule.threshold > value: triggered = True
            if triggered and not rule.triggered:
                rule.triggered = True; rule.triggered_at_ms = now_ms
                cond_cn = {"above":"超过","below":"低于","cross_up":"向上穿越","cross_down":"向下穿越"}.get(rule.condition, rule.condition)
                metric_cn = _ALERT_METRICS.get(rule.metric, rule.metric)
                msg = f"【{rule.name}】{snap.exchange} {snap.symbol} {metric_cn} {cond_cn} {rule.threshold:.4g}，当前值 {value:.4g}"
                self.alert_events.append(AlertEvent(rule_id=rule.rule_id, name=rule.name,
                    exchange=ek, symbol=snap.symbol, metric=rule.metric,
                    condition=rule.condition, threshold=rule.threshold,
                    actual_value=value, triggered_at_ms=now_ms, message=msg))
            elif not triggered: rule.triggered = False

    def _get_metric_value(self, metric: str, snap: ExchangeSnapshot, ek: str) -> Optional[float]:
        if metric == "price": return snap.last_price
        if metric == "oi":    return snap.open_interest_notional
        if metric == "funding": return snap.funding_rate
        if metric == "spread_bps":
            book = self.local_books.get(ek)
            return book.spread_bps() if book and book.is_ready else None
        if metric == "liq_notional":
            cutoff = int(time.time()*1000) - 3_600_000
            return sum(e.notional or 0 for e in self.liq_history[ek] if e.timestamp_ms >= cutoff)
        if metric == "cvd": return self._cvd_accum.get(ek, 0)
        if metric == "oi_velocity":
            h = list(self.oi_history[ek])
            if len(h) < 2: return None
            dt_min = (h[-1].timestamp_ms - h[-2].timestamp_ms) / 60000
            if dt_min <= 0: return None
            oi1 = h[-1].open_interest_notional or h[-1].open_interest or 0
            oi0 = h[-2].open_interest_notional or h[-2].open_interest or 0
            return (oi1 - oi0) / dt_min
        return None

    # ══════════════════════════════════════════════════════════════════════════
    # WebSocket Workers
    # ══════════════════════════════════════════════════════════════════════════

    def _update_live(self, ek: str, symbol: str, **values):
        with self.lock:
            prev = self.live_by_exchange.get(ek)
            snap = replace(prev) if prev else ExchangeSnapshot(
                exchange=self.clients[ek].exchange_name, symbol=symbol)
            snap.exchange = self.clients[ek].exchange_name
            snap.symbol   = symbol; snap.status = "ok"; snap.error = None
            for k, v in values.items():
                if v is not None: setattr(snap, k, v)
            self.live_by_exchange[ek] = snap

    def _run_ws_worker(self, ek: str):
        while not self.stop_ev.is_set():
            sym = self.symbol_map.get(ek)
            if not sym: return
            url = self._ws_url(ek, sym)
            app = websocket.WebSocketApp(url,
                on_open   = lambda ws,k=ek,s=sym: self._on_open(k,s,ws),
                on_message= lambda ws,msg,k=ek,s=sym: self._on_message(k,s,msg),
                on_error  = lambda ws,err,k=ek,s=sym: self._on_error(k,s,err))
            self.ws_apps[ek] = app
            try: app.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e: self._on_error(ek, sym, e)
            if self.stop_ev.wait(3): return

    def _run_spot_ws_worker(self, ek: str):
        while not self.stop_ev.is_set():
            coin = self._get_spot_symbol(ek)
            if not coin:
                if self.stop_ev.wait(5): return
                continue
            url = self._spot_ws_url(ek)
            app = websocket.WebSocketApp(url,
                on_open   = lambda ws,k=ek,c=coin: self._on_spot_open(k,c,ws),
                on_message= lambda ws,msg,k=ek,c=coin: self._on_spot_message(k,c,msg),
                on_error  = lambda ws,err: None)
            self.ws_apps[f"spot_{ek}"] = app
            try: app.run_forever(ping_interval=20, ping_timeout=10)
            except: pass
            if self.stop_ev.wait(3): return

    def _get_spot_symbol(self, ek: str) -> Optional[str]:
        sym = self.symbol_map.get(ek, "")
        if ek == "bybit": return sym if sym else None
        elif ek == "okx":
            if sym.endswith("-SWAP"): return sym.replace("-SWAP","")
            return sym.rsplit("-",1)[0] if "-" in sym else None
        return None

    def _spot_ws_url(self, ek: str) -> str:
        if ek == "bybit": return "wss://stream.bybit.com/v5/public/spot"
        if ek == "okx":   return "wss://ws.okx.com:8443/ws/v5/public"
        return ""

    def _ws_url(self, ek: str, sym: str) -> str:
        if ek == "bybit":   return "wss://stream.bybit.com/v5/public/linear"
        if ek == "binance":
            s = sym.lower()
            return (f"wss://fstream.binance.com/stream?streams="
                    f"{s}@markPrice@1s/{s}@ticker/{s}@forceOrder/{s}@aggTrade/{s}@depth@100ms")
        if ek == "okx":     return "wss://ws.okx.com:8443/ws/v5/public"
        return "wss://api.hyperliquid.xyz/ws"

    def _on_open(self, ek: str, sym: str, ws):
        if ek == "bybit":
            ws.send(json.dumps({"op":"subscribe","args":[
                f"tickers.{sym}", f"allLiquidation.{sym}",
                f"publicTrade.{sym}", f"orderbook.200.{sym}"]}))
        elif ek == "okx":
            ws.send(json.dumps({"op":"subscribe","args":[
                {"channel":"tickers","instId":sym},
                {"channel":"mark-price","instId":sym},
                {"channel":"trades","instId":sym},
                {"channel":"books","instId":sym},
                {"channel":"liquidation-warning","instId":sym}]}))
        elif ek == "hyperliquid":
            ws.send(json.dumps({"method":"subscribe","subscription":{"type":"allMids"}}))
            ws.send(json.dumps({"method":"subscribe","subscription":{"type":"trades","coin":sym}}))
            ws.send(json.dumps({"method":"subscribe","subscription":{"type":"l2Book","coin":sym}}))

    def _on_spot_open(self, ek: str, coin: str, ws):
        if ek == "bybit":
            ws.send(json.dumps({"op":"subscribe","args":[f"tickers.{coin}", f"orderbook.50.{coin}"]}))
        elif ek == "okx":
            ws.send(json.dumps({"op":"subscribe","args":[
                {"channel":"tickers","instId":coin},
                {"channel":"books5","instId":coin}]}))

    def _on_message(self, ek: str, sym: str, message: str):
        try: payload = json.loads(message)
        except: return
        if ek == "bybit":         self._handle_bybit(sym, payload)
        elif ek == "binance":     self._handle_binance(sym, payload)
        elif ek == "okx":         self._handle_okx(sym, payload)
        elif ek == "hyperliquid": self._handle_hl(sym, payload)

    def _on_spot_message(self, ek: str, coin: str, message: str):
        try: payload = json.loads(message)
        except: return
        if ek == "bybit":   self._handle_bybit_spot(coin, payload)
        elif ek == "okx":   self._handle_okx_spot(coin, payload)

    def _on_error(self, ek: str, sym: str, error):
        with self.lock:
            prev = self.live_by_exchange.get(ek)
            snap = replace(prev) if prev else ExchangeSnapshot(
                exchange=self.clients[ek].exchange_name, symbol=sym)
            snap.status = "error"; snap.error = str(error)
            self.live_by_exchange[ek] = snap

    # ── Binance depth ─────────────────────────────────────────────────────────
    def _run_binance_depth_init(self):
        time.sleep(2)
        sym = self.symbol_map.get("binance","")
        if not sym: return
        try:
            import requests
            r = requests.get("https://fapi.binance.com/fapi/v1/depth",
                             params={"symbol":sym,"limit":1000}, timeout=10)
            r.raise_for_status(); data = r.json()
            with self.lock:
                book = self.local_books["binance"]
                book.bids = {float(p):float(s) for p,s in data.get("bids",[]) if float(s)>0}
                book.asks = {float(p):float(s) for p,s in data.get("asks",[]) if float(s)>0}
                book.last_update_id = data.get("lastUpdateId",0)
                book.is_ready = True
                for msg in self._binance_depth_buffer.get("binance",[]):
                    self._apply_binance_depth_delta(book, msg)
                self._binance_depth_buffer["binance"] = []
        except: pass

    def _apply_binance_depth_delta(self, book: LocalOrderBook, data: dict):
        for p,s in data.get("b",[]):
            fp,fs = float(p),float(s)
            if fs==0: book.bids.pop(fp,None)
            else: book.bids[fp]=fs
        for p,s in data.get("a",[]):
            fp,fs = float(p),float(s)
            if fs==0: book.asks.pop(fp,None)
            else: book.asks[fp]=fs
        book.last_update_id = data.get("u", book.last_update_id)
        book.timestamp_ms   = data.get("T", int(time.time()*1000))

    def _update_bybit_book(self, ek: str, data: dict, book_type: str):
        with self.lock:
            book = self.local_books[ek]
            if book_type == "snapshot": book.bids={}; book.asks={}
            for p,s in data.get("b",[]): fp,fs=float(p),float(s); (book.bids.pop(fp,None) if fs==0 else book.bids.update({fp:fs}))
            for p,s in data.get("a",[]): fp,fs=float(p),float(s); (book.asks.pop(fp,None) if fs==0 else book.asks.update({fp:fs}))
            book.is_ready=True; book.timestamp_ms=data.get("ts",int(time.time()*1000))
            self._detect_orderbook_patterns_locked(ek, book)

    def _update_bybit_spot_book(self, data: dict, book_type: str):
        with self.lock:
            book = self.spot_books["bybit"]
            if book_type=="snapshot": book.bids={}; book.asks={}
            for p,s in data.get("b",[]): fp,fs=float(p),float(s); (book.bids.pop(fp,None) if fs==0 else book.bids.update({fp:fs}))
            for p,s in data.get("a",[]): fp,fs=float(p),float(s); (book.asks.pop(fp,None) if fs==0 else book.asks.update({fp:fs}))
            book.is_ready=True

    def _update_okx_book(self, ek: str, data_list: list, action: str):
        with self.lock:
            book = self.local_books[ek]
            if action=="snapshot": book.bids={}; book.asks={}
            for item in data_list:
                for p,s,*_ in item.get("bids",[]): fp,fs=float(p),float(s); (book.bids.pop(fp,None) if fs==0 else book.bids.update({fp:fs}))
                for p,s,*_ in item.get("asks",[]): fp,fs=float(p),float(s); (book.asks.pop(fp,None) if fs==0 else book.asks.update({fp:fs}))
                ts=safe_int(item.get("ts"))
                if ts: book.timestamp_ms=ts
            book.is_ready=True
            self._detect_orderbook_patterns_locked(ek, book)

    def _update_okx_spot_book(self, data_list: list, action: str):
        with self.lock:
            book = self.spot_books["okx"]
            if action=="snapshot": book.bids={}; book.asks={}
            for item in data_list:
                for p,s,*_ in item.get("bids",[]): fp,fs=float(p),float(s); (book.bids.pop(fp,None) if fs==0 else book.bids.update({fp:fs}))
                for p,s,*_ in item.get("asks",[]): fp,fs=float(p),float(s); (book.asks.pop(fp,None) if fs==0 else book.asks.update({fp:fs}))
            book.is_ready=True

    # ── Exchange handlers ─────────────────────────────────────────────────────
    def _handle_bybit(self, sym: str, payload: dict):
        if payload.get("success") is not None: return
        topic = str(payload.get("topic",""))
        data  = payload.get("data") or {}
        if topic.startswith("allLiquidation"):
            items = data if isinstance(data,list) else [data]
            with self.lock:
                for item in items:
                    side  = normalize_liquidation_side(item.get("side") or item.get("S"))
                    price = safe_float(item.get("price") or item.get("p"))
                    size  = safe_float(item.get("size") or item.get("v"))
                    self._append_liq_locked("bybit", LiquidationEvent(
                        exchange=self.clients["bybit"].exchange_name, symbol=sym,
                        timestamp_ms=safe_int(item.get("updatedTime") or item.get("T")) or int(time.time()*1000),
                        side=side,price=price,size=size,notional=compute_notional(price,size),source="ws",raw=item))
        elif topic.startswith("publicTrade"):
            items = data if isinstance(data,list) else [data]
            with self.lock:
                for item in items:
                    price=safe_float(item.get("p")) or 0; size=safe_float(item.get("v")) or 0
                    side="buy" if str(item.get("S","")).lower() in ("buy","b") else "sell"
                    self._append_trade_locked("bybit", TradeEvent(
                        exchange=self.clients["bybit"].exchange_name, symbol=sym,
                        timestamp_ms=safe_int(item.get("T")) or int(time.time()*1000),
                        price=price,size=size,side=side,notional=price*size,source="ws",raw=item))
        elif topic.startswith("orderbook"):
            book_type = "snapshot" if payload.get("type")=="snapshot" else "delta"
            if isinstance(data,dict): self._update_bybit_book("bybit", data, book_type)
        else:
            if isinstance(data,dict):
                self._update_live("bybit", sym,
                    last_price=safe_float(data.get("lastPrice")),
                    mark_price=safe_float(data.get("markPrice")),
                    index_price=safe_float(data.get("indexPrice")),
                    open_interest=safe_float(data.get("openInterest")),
                    open_interest_notional=safe_float(data.get("openInterestValue")),
                    funding_rate=safe_float(data.get("fundingRate")),
                    volume_24h_base=safe_float(data.get("volume24h")),
                    volume_24h_notional=safe_float(data.get("turnover24h")),
                    timestamp_ms=safe_int(payload.get("ts")) or int(time.time()*1000))

    def _handle_bybit_spot(self, coin: str, payload: dict):
        if payload.get("success") is not None: return
        topic = str(payload.get("topic",""))
        data  = payload.get("data") or {}
        if topic.startswith("tickers") and isinstance(data,dict):
            sp = safe_float(data.get("lastPrice"))
            vol= safe_float(data.get("turnover24h"))
            if sp:
                with self.lock: self._update_spot_price_locked("bybit", sp, vol)
        elif topic.startswith("orderbook"):
            book_type = "snapshot" if payload.get("type")=="snapshot" else "delta"
            if isinstance(data,dict): self._update_bybit_spot_book(data, book_type)

    def _handle_binance(self, sym: str, payload: dict):
        stream = str(payload.get("stream",""))
        data   = payload.get("data") or {}
        if "@forceorder" in stream.lower():
            order = data.get("o") or {}
            price = safe_float(order.get("ap")) or safe_float(order.get("p"))
            size  = safe_float(order.get("z")) or safe_float(order.get("q"))
            event = LiquidationEvent(
                exchange=self.clients["binance"].exchange_name,
                symbol=order.get("s") or sym,
                timestamp_ms=safe_int(data.get("E")) or int(time.time()*1000),
                side=normalize_liquidation_side(order.get("S")),
                price=price,size=size,notional=compute_notional(price,size),source="ws",raw=order)
            with self.lock: self._append_liq_locked("binance", event)
        elif "@aggtrade" in stream.lower():
            price=safe_float(data.get("p")) or 0; size=safe_float(data.get("q")) or 0
            side="sell" if data.get("m") else "buy"
            with self.lock:
                self._append_trade_locked("binance", TradeEvent(
                    exchange=self.clients["binance"].exchange_name, symbol=sym,
                    timestamp_ms=safe_int(data.get("T")) or int(time.time()*1000),
                    price=price,size=size,side=side,notional=price*size,source="ws",raw=data))
        elif "@depth" in stream.lower():
            with self.lock:
                book = self.local_books["binance"]
                if book.is_ready:
                    self._apply_binance_depth_delta(book, data)
                    self._detect_orderbook_patterns_locked("binance", book)
                else: self._binance_depth_buffer["binance"].append(data)
        elif "markprice" in stream.lower():
            self._update_live("binance", sym,
                mark_price=safe_float(data.get("p")),
                index_price=safe_float(data.get("i")),
                funding_rate=safe_float(data.get("r")),
                timestamp_ms=safe_int(data.get("E")) or int(time.time()*1000))
        elif "@ticker" in stream.lower():
            lp = safe_float(data.get("c"))
            self._update_live("binance", sym,
                last_price=lp,
                volume_24h_base=safe_float(data.get("v")),
                volume_24h_notional=safe_float(data.get("q")),
                timestamp_ms=safe_int(data.get("E")) or int(time.time()*1000))
            with self.lock:
                live = self.live_by_exchange.get("binance")
                if live and live.index_price and lp:
                    self._update_spot_price_locked("binance", live.index_price)

    def _handle_okx(self, sym: str, payload: dict):
        if payload.get("event"): return
        arg=payload.get("arg") or {}; data_list=payload.get("data") or [{}]
        channel=arg.get("channel"); action=payload.get("action","")
        if channel=="tickers":
            d=data_list[0] if data_list else {}
            self._update_live("okx",sym,last_price=safe_float(d.get("last")),
                volume_24h_base=safe_float(d.get("vol24h")),
                volume_24h_notional=safe_float(d.get("volCcy24h")),
                timestamp_ms=safe_int(d.get("ts")) or int(time.time()*1000))
        elif channel=="mark-price":
            d=data_list[0] if data_list else {}
            self._update_live("okx",sym,mark_price=safe_float(d.get("markPx")),
                timestamp_ms=safe_int(d.get("ts")) or int(time.time()*1000))
        elif channel=="trades":
            with self.lock:
                for item in data_list:
                    price=safe_float(item.get("px")) or 0; size=safe_float(item.get("sz")) or 0
                    side="buy" if str(item.get("side","")).lower()=="buy" else "sell"
                    self._append_trade_locked("okx", TradeEvent(
                        exchange=self.clients["okx"].exchange_name, symbol=sym,
                        timestamp_ms=safe_int(item.get("ts")) or int(time.time()*1000),
                        price=price,size=size,side=side,notional=price*size,source="ws",raw=item))
        elif channel=="books":
            self._update_okx_book("okx",data_list,action or ("snapshot" if not self.local_books["okx"].is_ready else "update"))
        elif channel=="liquidation-warning":
            with self.lock:
                for item in data_list:
                    details=item.get("details") or [item]
                    for d in (details if isinstance(details,list) else [details]):
                        price=safe_float(d.get("bkPx") or d.get("px"))
                        size=safe_float(d.get("sz") or d.get("posSz"))
                        side_raw=str(d.get("posSide") or d.get("side","")).lower()
                        side="long" if "long" in side_raw else "short" if "short" in side_raw else "unknown"
                        self._append_liq_locked("okx", LiquidationEvent(
                            exchange=self.clients["okx"].exchange_name, symbol=sym,
                            timestamp_ms=safe_int(d.get("ts")) or int(time.time()*1000),
                            side=side,price=price,size=size,notional=compute_notional(price,size),source="ws",raw=d))

    def _handle_okx_spot(self, coin: str, payload: dict):
        if payload.get("event"): return
        arg=payload.get("arg") or {}; data_list=payload.get("data") or [{}]
        channel=arg.get("channel"); action=payload.get("action","")
        if channel=="tickers":
            d=data_list[0] if data_list else {}
            sp=safe_float(d.get("last")); vol=safe_float(d.get("volCcy24h"))
            if sp:
                with self.lock: self._update_spot_price_locked("okx", sp, vol)
        elif channel in ("books5","books"):
            self._update_okx_spot_book(data_list, action or "snapshot")

    def _handle_hl(self, sym: str, payload: dict):
        channel=payload.get("channel")
        if channel=="allMids":
            mids=(payload.get("data") or {}).get("mids") or {}
            self._update_live("hyperliquid",sym,last_price=safe_float(mids.get(sym)),timestamp_ms=int(time.time()*1000))
        elif channel=="trades":
            with self.lock:
                for item in payload.get("data") or []:
                    if item.get("coin")!=sym: continue
                    price=safe_float(item.get("px")) or 0; size=safe_float(item.get("sz")) or 0
                    side="buy" if str(item.get("side","")).upper() in ("B","BUY") else "sell"
                    snap=self.live_by_exchange.get("hyperliquid"); mark=snap.mark_price if snap else None
                    notional=price*size if price>10 else size*(mark or price)
                    self._append_trade_locked("hyperliquid", TradeEvent(
                        exchange=self.clients["hyperliquid"].exchange_name, symbol=sym,
                        timestamp_ms=safe_int(item.get("time")) or int(time.time()*1000),
                        price=price,size=size,side=side,notional=notional,source="ws",raw=item))
        elif channel=="l2Book":
            data=payload.get("data") or {}
            if data.get("coin")!=sym: return
            with self.lock:
                book=self.local_books["hyperliquid"]
                levels=data.get("levels",[[],[]])
                book.bids={float(r["px"]):float(r["sz"]) for r in levels[0] if float(r.get("sz",0))>0}
                book.asks={float(r["px"]):float(r["sz"]) for r in levels[1] if float(r.get("sz",0))>0}
                book.is_ready=True; book.timestamp_ms=int(time.time()*1000)
                self._detect_orderbook_patterns_locked("hyperliquid", book)


# ══════════════════════════════════════════════════════════════════════════════
# v5 patch — appended to LiveTerminalService
# Wall Life Tracking + Large Order Flow + Near Liquidity Collapse
# ══════════════════════════════════════════════════════════════════════════════

    # These methods are appended — they reference self and the existing __init__ state
    # They are called from _detect_orderbook_patterns_locked (already exists above)

def _v5_init_extra(self):
    """Call from __init__ to add v5 state — monkey-patch approach"""
    from collections import deque
    self.wall_life:       Dict[str, Dict[float, "WallLifePoint"]] = {k: {} for k in EXCHANGE_ORDER}
    self.wall_life_hist:  Dict[str, Deque]     = {k: deque(maxlen=500) for k in EXCHANGE_ORDER}
    self.liq_collapses:   Dict[str, Deque]     = {k: deque(maxlen=200) for k in EXCHANGE_ORDER}
    self.large_order_flow:Dict[str, Deque]     = {k: deque(maxlen=1000) for k in EXCHANGE_ORDER}
    self._large_order_threshold = 50_000       # $50K = 大单
    self._v5_ready = True

def _v5_track_wall_life_locked(self, ek: str, book: "LocalOrderBook"):
    """Track how long each large order stays in the book"""
    if not hasattr(self, '_v5_ready'): return
    from models import WallLifePoint, NearLiquidityCollapse
    now_ms = int(time.time() * 1000)
    LARGE_WALL = 200_000   # $200K threshold for wall tracking
    mid = book.mid_price()

    current_walls: Dict[float, float] = {}
    for p, s in {**book.bids, **book.asks}.items():
        n = p * s
        if n >= LARGE_WALL:
            current_walls[p] = n

    existing = self.wall_life.get(ek, {})

    # Update existing walls
    for price, notional in current_walls.items():
        side = "bid" if price in book.bids else "ask"
        if price in existing:
            wp = existing[price]
            wp.age_ms = now_ms - wp.born_ms
            wp.size   = book.bids.get(price, book.asks.get(price, 0))
            wp.notional = notional
            wp.is_alive = True
        else:
            existing[price] = WallLifePoint(
                timestamp_ms=now_ms, exchange=ek, side=side,
                price=price, size=notional/max(price,1),
                notional=notional, born_ms=now_ms, age_ms=0, is_alive=True)

    # Detect walls that disappeared (near-price collapse)
    dead_prices = [p for p in list(existing.keys()) if p not in current_walls]
    for price in dead_prices:
        wp = existing.pop(price)
        wp.is_alive  = False
        wp.age_ms    = now_ms - wp.born_ms
        self.wall_life_hist[ek].append(wp)
        # Near-price liquidity collapse?
        if mid and mid > 0:
            dist_pct = abs(price - mid) / mid * 100
            if dist_pct < 1.5 and wp.notional >= 100_000:  # within 1.5% of mid
                collapse = NearLiquidityCollapse(
                    timestamp_ms=now_ms, exchange=ek,
                    side="bid" if wp.side == "bid" else "ask",
                    price_pct_from_mid=dist_pct,
                    notional_lost=wp.notional,
                    collapse_speed_ms=max(wp.age_ms, 100))
                self.liq_collapses[ek].append(collapse)

    self.wall_life[ek] = existing

def _v5_track_large_trade_locked(self, ek: str, trade: "TradeEvent"):
    """Filter and record large trades for big-order flow view"""
    if not hasattr(self, '_v5_ready'): return
    if trade.notional < getattr(self, '_large_order_threshold', 50_000): return
    from models import LargeOrderFlow
    lof = LargeOrderFlow(
        timestamp_ms=trade.timestamp_ms, exchange=ek,
        side=trade.side, price=trade.price,
        notional=trade.notional, is_aggressor=True)
    self.large_order_flow[ek].append(lof)


# Monkey-patch LiveTerminalService with v5 methods
LiveTerminalService._v5_init_extra       = _v5_init_extra
LiveTerminalService._v5_track_wall_life  = _v5_track_wall_life_locked
LiveTerminalService._v5_track_large_trade= _v5_track_large_trade_locked

# Patch __init__ to call _v5_init_extra
_orig_init = LiveTerminalService.__init__
def _patched_init(self, *args, **kwargs):
    _orig_init(self, *args, **kwargs)
    self._v5_init_extra()
LiveTerminalService.__init__ = _patched_init

# Patch _append_trade_locked to also call v5 large trade tracker
_orig_append_trade = LiveTerminalService._append_trade_locked
def _patched_append_trade(self, ek, trade):
    _orig_append_trade(self, ek, trade)
    self._v5_track_large_trade(ek, trade)
LiveTerminalService._append_trade_locked = _patched_append_trade

# Patch _detect_orderbook_patterns_locked to also call v5 wall tracker
_orig_detect = LiveTerminalService._detect_orderbook_patterns_locked
def _patched_detect(self, ek, book):
    _orig_detect(self, ek, book)
    self._v5_track_wall_life(ek, book)
LiveTerminalService._detect_orderbook_patterns_locked = _patched_detect

# Add v5 public getters
def _get_wall_life_history(self, ek):
    with self.lock: return list(self.wall_life_hist.get(ek, []))
def _get_active_walls(self, ek):
    with self.lock: return dict(self.wall_life.get(ek, {}))
def _get_liq_collapses(self, ek):
    with self.lock: return list(self.liq_collapses.get(ek, []))
def _get_large_order_flow(self, ek):
    with self.lock: return list(self.large_order_flow.get(ek, []))

LiveTerminalService.get_wall_life_history  = _get_wall_life_history
LiveTerminalService.get_active_walls       = _get_active_walls
LiveTerminalService.get_liq_collapses      = _get_liq_collapses
LiveTerminalService.get_large_order_flow   = _get_large_order_flow
