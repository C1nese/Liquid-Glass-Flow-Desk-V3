"""
alert_center.py — v5 告警中心
可筛选 · 可静音 · 只看强告警 · 跳转回放
"""
from __future__ import annotations
import time
from typing import Dict, List, Optional, Set
import pandas as pd
import streamlit as st

from models import ConfirmedAlert, AlertTimeline, SpotPerpAlert

_UP  = "#1dc796"; _DN="#ff6868"; _WARN="#ffa94d"; _BLUE="#62c2ff"

SEV_COLOR = {"strong": _DN, "medium": _WARN, "weak": _BLUE}
SEV_ICON  = {"strong": "🔴", "medium": "🟡", "weak": "🔵"}
SEV_CN    = {"strong": "强级", "medium": "中级", "weak": "弱级"}

ATYPE_CN  = {
    "spot_lead_up":    ("🟢 现货先拉↑", _UP),
    "spot_lead_down":  ("🔴 现货先跌↓", _DN),
    "oi_up_cvd_weak":  ("⚠️ OI升/买弱",  _WARN),
    "oi_down_cvd_up":  ("🔵 OI降/轧空",  _BLUE),
    "diverge_extreme": ("🚨 极端乖离",   "#ff4444"),
    "crowd_liq_combo": ("⚡ 拥挤+爆仓", "#ff8c00"),
    "fake_wall":       ("🎭 假挂单",     _WARN),
    "composite_signal":("🧠 合成信号",  "#c084fc"),
}


def render_alert_center(confirmed_alerts: List[ConfirmedAlert],
                        alert_timeline: List[AlertTimeline],
                        recorded_frames: list,
                        jump_to_replay_fn=None):
    """Full alert center UI"""
    st.markdown("""
    <div style="padding:14px 20px 10px;border-radius:22px;margin-bottom:12px;
        border:1px solid rgba(255,165,0,0.3);background:rgba(255,130,0,0.06);
        backdrop-filter:blur(20px);">
      <div style="font-size:0.68rem;color:#fca;text-transform:uppercase;letter-spacing:0.18em;">
        告警中心 · Alert Intelligence Center</div>
      <div style="font-size:1.4rem;font-weight:800;color:#fff;margin-top:3px;">
        可筛选 · 可静音 · 强告警优先 · 跳转回放</div>
    </div>""", unsafe_allow_html=True)

    # ── Session state for mute ─────────────────────────────────────────────────
    if "muted_types" not in st.session_state:
        st.session_state["muted_types"]: Set[str] = set()
    if "alert_min_severity" not in st.session_state:
        st.session_state["alert_min_severity"] = "weak"

    # ── Filter bar ─────────────────────────────────────────────────────────────
    fc1, fc2, fc3, fc4 = st.columns([1.5, 1.5, 2, 1.5])
    min_sev = fc1.selectbox("最低等级", ["strong","medium","weak"],
        index=["strong","medium","weak"].index(st.session_state["alert_min_severity"]),
        format_func=lambda s: SEV_CN[s])
    st.session_state["alert_min_severity"] = min_sev
    ex_filter = fc2.multiselect("交易所过滤", ["Binance","Bybit","OKX","Hyperliquid"],
        default=["Binance","Bybit","OKX","Hyperliquid"])
    type_filter = fc3.multiselect("告警类型", list(ATYPE_CN.keys()),
        default=list(ATYPE_CN.keys()), format_func=lambda k: ATYPE_CN.get(k, (k,""))[0])
    only_confirmed = fc4.checkbox("仅确认告警", value=False)

    SEV_RANK = {"strong": 0, "medium": 1, "weak": 2}
    min_rank = SEV_RANK[min_sev]

    # ── Mute controls ──────────────────────────────────────────────────────────
    with st.expander("🔇 静音管理", expanded=False):
        mc = st.columns(4)
        for i, atype in enumerate(list(ATYPE_CN.keys())):
            label, _ = ATYPE_CN[atype]
            muted = atype in st.session_state["muted_types"]
            btn_label = f"{'🔇' if muted else '🔔'} {label}"
            col = mc[i % 4]
            if col.button(btn_label, key=f"mute_{atype}"):
                if muted: st.session_state["muted_types"].discard(atype)
                else:     st.session_state["muted_types"].add(atype)
                st.rerun()
        if st.session_state["muted_types"]:
            st.caption(f"已静音：{', '.join(st.session_state['muted_types'])}")

    # ── Filter alerts ──────────────────────────────────────────────────────────
    def passes(a: ConfirmedAlert) -> bool:
        if SEV_RANK.get(a.severity, 9) > min_rank: return False
        if a.exchange not in ex_filter: return False
        if a.alert_type not in type_filter: return False
        if a.alert_type in st.session_state["muted_types"]: return False
        return True

    filtered = [a for a in confirmed_alerts if passes(a)]
    filtered.sort(key=lambda a: (SEV_RANK.get(a.severity, 9), -a.confirmed_at_ms))

    # ── Summary row ───────────────────────────────────────────────────────────
    sc = st.columns(4)
    sc[0].metric("显示告警数", str(len(filtered)))
    sc[1].metric("强级", str(sum(1 for a in filtered if a.severity=="strong")))
    sc[2].metric("中级", str(sum(1 for a in filtered if a.severity=="medium")))
    sc[3].metric("已静音类型", str(len(st.session_state["muted_types"])))

    st.markdown("---")

    # ── Alert cards ───────────────────────────────────────────────────────────
    if not filtered:
        st.info("暂无符合条件的告警。" + (f" ({len(confirmed_alerts)-len(filtered)} 条被过滤)" if confirmed_alerts else ""))
    else:
        for a in filtered[:50]:
            sev_col  = SEV_COLOR.get(a.severity, "#aaa")
            sev_icon = SEV_ICON.get(a.severity, "⚪")
            sev_cn   = SEV_CN.get(a.severity, a.severity)
            type_label, type_col = ATYPE_CN.get(a.alert_type, (a.alert_type, "#aaa"))
            ts  = pd.to_datetime(a.confirmed_at_ms, unit="ms").strftime("%H:%M:%S")
            ts_first = pd.to_datetime(a.first_seen_ms, unit="ms").strftime("%H:%M:%S")

            # Find nearest replay frame
            replay_idx = None
            if recorded_frames:
                diffs = [abs(f.timestamp_ms - a.confirmed_at_ms) for f in recorded_frames]
                replay_idx = diffs.index(min(diffs))

            col_card, col_btn = st.columns([5, 1])
            with col_card:
                st.markdown(f"""
                <div style="padding:11px 15px;border-radius:14px;margin:4px 0;
                    border-left:4px solid {sev_col};
                    border:1px solid {sev_col}30;
                    background:rgba(255,255,255,0.04);
                    backdrop-filter:blur(16px);">
                  <div style="display:flex;gap:8px;align-items:center;margin-bottom:4px;">
                    <span style="color:{sev_col};font-size:0.85rem;font-weight:700;">
                      {sev_icon} {sev_cn} ×{a.confirm_count}确认</span>
                    <span style="color:{type_col};font-size:0.8rem;">{type_label}</span>
                    <span style="color:#888;font-size:0.75rem;margin-left:auto;">
                      {a.exchange} · 确认{ts} (首次{ts_first})</span>
                  </div>
                  <div style="color:#e0e8ff;font-size:0.85rem;line-height:1.4;">{a.message}</div>
                  <div style="margin-top:5px;display:flex;gap:6px;align-items:center;">
                    <span style="font-size:0.72rem;color:#888;">强度</span>
                    <div style="width:80px;height:4px;background:rgba(255,255,255,0.1);border-radius:2px;">
                      <div style="width:{int(a.score*100)}%;height:100%;
                           background:{sev_col};border-radius:2px;"></div>
                    </div>
                    <span style="font-size:0.72rem;color:{sev_col};">{a.score:.2f}</span>
                  </div>
                </div>""", unsafe_allow_html=True)
            with col_btn:
                if replay_idx is not None and recorded_frames:
                    if st.button("📼 回放", key=f"replay_{a.alert_id}_{a.confirmed_at_ms}"):
                        st.session_state["replay_frame_idx"] = replay_idx
                        st.session_state["active_tab_hint"] = "replay"
                        st.info(f"已跳转到帧 {replay_idx}，请切换到「回放复盘」Tab")

    # ── Timeline chart ─────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("#### 告警时间线")
    from analytics import build_alert_timeline_figure
    st.plotly_chart(build_alert_timeline_figure(alert_timeline), key="ac_timeline", use_container_width=True)

    # ── Export ─────────────────────────────────────────────────────────────────
    if filtered:
        df_exp = pd.DataFrame([{
            "时间": pd.to_datetime(a.confirmed_at_ms, unit="ms"),
            "等级": SEV_CN.get(a.severity, a.severity),
            "类型": ATYPE_CN.get(a.alert_type, (a.alert_type,))[0],
            "交易所": a.exchange, "确认次数": a.confirm_count,
            "强度": round(a.score, 3), "消息": a.message,
        } for a in filtered])
        csv = df_exp.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ 导出告警 CSV", csv, "alerts_export.csv", "text/csv")
