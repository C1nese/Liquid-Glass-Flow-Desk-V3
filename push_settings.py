"""
push_settings.py  —  推送设置 + 告警历史 + 数据库管理 Tab（v6）
"""
from __future__ import annotations
import time
from typing import List

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from notifier import (
    Notifier, get_notifier, init_notifier,
    BROWSER_NOTIFICATION_JS, get_browser_notification_html,
)
from storage import (
    init_db, get_db_stats, query_alert_history,
    query_oi_history, query_funding_history, cleanup_old_data,
    query_daily_summaries, auto_parquet_archive, get_parquet_files,
)
from models import PushNotificationConfig


# ══════════════════════════════════════════════════════════════════════════════
# 图表
# ══════════════════════════════════════════════════════════════════════════════

def build_oi_history_figure(rows: List[dict], coin: str) -> go.Figure:
    """持久化 OI 历史多折线图（按交易所）"""
    by_ex: dict = {}
    for r in rows:
        by_ex.setdefault(r["exchange"], []).append(r)

    ex_colors = {
        "binance": "#f0b90b", "bybit": "#ff6b00",
        "okx": "#00b4d8", "hyperliquid": "#7c3aed",
    }
    fig = go.Figure()
    for ex, pts in by_ex.items():
        ts  = [p["timestamp_ms"] for p in pts]
        ois = [p["oi_notional"] / 1e9 if p["oi_notional"] else 0 for p in pts]
        fig.add_trace(go.Scatter(
            x=ts, y=ois, mode="lines",
            name=ex.capitalize(),
            line=dict(color=ex_colors.get(ex, "#aaa"), width=2),
        ))
    fig.update_layout(
        height=380, template="plotly_dark",
        title=f"{coin} 持久化 OI 历史（十亿$）",
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        yaxis_title="OI (十亿$)", legend=dict(orientation="h", y=1.1),
        margin=dict(l=40, r=20, t=50, b=40),
    )
    return fig


def build_funding_history_figure(rows: List[dict], coin: str) -> go.Figure:
    """持久化资金费率历史多折线图"""
    by_ex: dict = {}
    for r in rows:
        by_ex.setdefault(r["exchange"], []).append(r)

    ex_colors = {
        "binance": "#f0b90b", "bybit": "#ff6b00",
        "okx": "#00b4d8", "hyperliquid": "#7c3aed",
    }
    fig = go.Figure()
    for ex, pts in by_ex.items():
        ts   = [p["timestamp_ms"] for p in pts]
        rates = [p["funding_rate"] * 10000 if p["funding_rate"] else 0 for p in pts]
        fig.add_trace(go.Scatter(
            x=ts, y=rates, mode="lines",
            name=ex.capitalize(),
            line=dict(color=ex_colors.get(ex, "#aaa"), width=1.5),
        ))
    fig.add_hline(y=0, line_dash="dash", line_color="rgba(255,255,255,0.3)")
    fig.update_layout(
        height=340, template="plotly_dark",
        title=f"{coin} 资金费率历史 (bps)",
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        yaxis_title="费率 (bps)", legend=dict(orientation="h", y=1.1),
        margin=dict(l=40, r=20, t=50, b=40),
    )
    return fig


def build_alert_timeline_from_db(rows: List[dict]) -> go.Figure:
    """数据库告警历史时间线"""
    if not rows:
        return go.Figure()
    severity_colors = {"strong": "#ff4444", "medium": "#ffaa33",
                       "weak": "#55aaff", "high": "#ff4444"}
    fig = go.Figure()
    for sev in ["strong", "medium", "weak"]:
        pts = [r for r in rows if r.get("severity") == sev]
        if not pts:
            continue
        fig.add_trace(go.Scatter(
            x=[p["timestamp_ms"] for p in pts],
            y=[p["score"] for p in pts],
            mode="markers",
            marker=dict(color=severity_colors[sev], size=8),
            name=sev,
            text=[p.get("message", "")[:60] for p in pts],
            hovertemplate="%{text}<extra></extra>",
        ))
    fig.update_layout(
        height=300, template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        yaxis_title="告警强度", legend=dict(orientation="h", y=1.1),
        margin=dict(l=40, r=20, t=30, b=40),
    )
    return fig


# ══════════════════════════════════════════════════════════════════════════════
# 主渲染
# ══════════════════════════════════════════════════════════════════════════════

def render_push_settings(service=None):
    """推送设置 + 数据库管理 Tab"""
    # Inject browser notification JS once
    st.markdown(BROWSER_NOTIFICATION_JS, unsafe_allow_html=True)

    st.markdown("""
    <div class="glass-section">
        <div class="glass-kicker">NOTIFICATIONS & STORAGE</div>
        <div style="font-size:1.1rem;font-weight:600;color:#e8f4ff;">
            📡 推送通知 · 历史数据 · 数据库管理
        </div>
    </div>
    """, unsafe_allow_html=True)

    sub_tabs = st.tabs([
        "📱 Telegram 推送",
        "🔔 浏览器通知",
        "📊 历史数据查询",
        "🗄️ 数据库管理",
        "📋 通知记录",
        "🎯 告警命中率统计",
    ])

    notifier = get_notifier()

    # ── Telegram ──
    with sub_tabs[0]:
        st.markdown("#### Telegram Bot 推送配置")
        st.info("💡 获取 Bot Token：在 Telegram 中找 @BotFather，发 /newbot。\n"
                "获取 Chat ID：找 @userinfobot，转发任意消息给它。")

        with st.form("tg_config_form"):
            tg_enabled = st.checkbox("启用 Telegram 推送",
                                      value=notifier.config.telegram_enabled,
                                      key="tg_enabled")
            tg_token   = st.text_input("Bot Token",
                                        value=notifier.config.telegram_token,
                                        type="password", key="tg_token")
            tg_chat    = st.text_input("Chat ID",
                                        value=notifier.config.telegram_chat_id,
                                        key="tg_chat")
            min_sev    = st.selectbox("最小推送等级",
                                       ["weak", "medium", "strong"],
                                       index=["weak","medium","strong"].index(
                                           notifier.config.min_severity),
                                       key="tg_min_sev")
            cooldown   = st.slider("同类告警冷却时间 (秒)", 10, 300, 60, key="tg_cooldown")

            col1, col2 = st.columns(2)
            saved = col1.form_submit_button("保存配置")
            tested = col2.form_submit_button("测试连接")

        if saved:
            cfg = PushNotificationConfig(
                telegram_enabled=tg_enabled,
                telegram_token=tg_token,
                telegram_chat_id=tg_chat,
                min_severity=min_sev,
                cooldown_seconds=cooldown,
            )
            notifier.update_config(cfg)
            # Persist to session state
            st.session_state["push_config"] = cfg
            st.success("✅ 配置已保存")

        if tested:
            cfg = PushNotificationConfig(
                telegram_enabled=True,
                telegram_token=tg_token or notifier.config.telegram_token,
                telegram_chat_id=tg_chat or notifier.config.telegram_chat_id,
            )
            notifier.update_config(cfg)
            ok, msg = notifier.test_telegram()
            if ok:
                st.success(f"✅ {msg}")
            else:
                st.error(f"❌ {msg}")

        # Show recent sent records
        records = notifier.get_records(20)
        if records:
            st.markdown("---")
            st.markdown("##### 最近发送记录")
            r_rows = [{
                "时间": pd.Timestamp(r.timestamp_ms, unit="ms").strftime("%H:%M:%S"),
                "渠道": r.channel,
                "类型": r.alert_type,
                "等级": r.severity,
                "状态": "✅" if r.success else "❌",
                "消息": r.message[:50] + "…" if len(r.message) > 50 else r.message,
            } for r in records]
            st.dataframe(pd.DataFrame(r_rows), width='stretch', hide_index=True)

    # ── 浏览器通知 ──
    with sub_tabs[1]:
        st.markdown("#### 浏览器桌面通知")
        st.info("浏览器通知需要您在弹窗中点击【允许】。刷新页面后需重新授权。")

        col1, col2 = st.columns(2)
        with col1:
            browser_enabled = st.checkbox("启用浏览器通知",
                                           value=notifier.config.browser_enabled,
                                           key="browser_enabled_cb")
        with col2:
            sound_enabled = st.checkbox("启用声音提示",
                                         value=notifier.config.sound_enabled,
                                         key="sound_enabled_cb")

        if st.button("测试浏览器通知", key="test_browser_notif"):
            st.markdown(
                get_browser_notification_html(
                    "测试通知", "多交易所终端 v6 — 浏览器通知测试成功！", "medium"
                ),
                unsafe_allow_html=True,
            )
            st.success("已发送测试通知")

        if st.button("测试声音告警", key="test_sound"):
            st.markdown("""
            <script>
            try {
                const ctx = new (window.AudioContext || window.webkitAudioContext)();
                const osc = ctx.createOscillator();
                const gain = ctx.createGain();
                osc.connect(gain); gain.connect(ctx.destination);
                osc.frequency.value = 880;
                gain.gain.setValueAtTime(0.3, ctx.currentTime);
                gain.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + 1.0);
                osc.start(ctx.currentTime); osc.stop(ctx.currentTime + 1.0);
            } catch(e) { console.log('Audio error:', e); }
            </script>
            """, unsafe_allow_html=True)
            st.success("已播放测试声音")

        # Update config
        cfg = notifier.config
        cfg.browser_enabled = browser_enabled
        cfg.sound_enabled   = sound_enabled
        notifier.update_config(cfg)

    # ── 历史数据查询 ──
    with sub_tabs[2]:
        st.markdown("#### 持久化历史数据查询")

        db_path = st.text_input("数据库路径", value="market_data.db", key="db_path_query")

        try:
            init_db(db_path)
            stats = get_db_stats(db_path)
        except Exception as e:
            st.error(f"数据库初始化失败: {e}")
            st.stop()

        c1, c2, c3 = st.columns(3)
        c1.metric("OI 记录", f"{stats.get('oi_history', 0):,}")
        c2.metric("资金费率记录", f"{stats.get('funding_history', 0):,}")
        c3.metric("数据库大小", f"{stats.get('db_size_mb', 0)} MB")

        st.markdown("---")
        col_a, col_b, col_c = st.columns(3)
        with col_a:
            hist_coin = st.selectbox("币种", ["BTC", "ETH", "SOL", "XRP", "BNB"],
                                      key="hist_coin")
        with col_b:
            hist_hours = st.slider("查询范围 (小时)", 1, 720, 72, key="hist_hours")
        with col_c:
            hist_type = st.selectbox("数据类型", ["OI历史", "资金费率", "每日摘要"],
                                      key="hist_type")

        if st.button("查询", key="query_hist"):
            if hist_type == "OI历史":
                rows = query_oi_history(hist_coin, hours=hist_hours, db_path=db_path)
                if rows:
                    st.plotly_chart(build_oi_history_figure(rows, hist_coin), config={'displayModeBar': True, 'scrollZoom': True})
                    st.caption(f"共 {len(rows)} 条记录")
                else:
                    st.info("暂无历史数据。请确保已开启自动归档功能（在 Settings 中）。")

            elif hist_type == "资金费率":
                rows = query_funding_history(hist_coin, hours=hist_hours, db_path=db_path)
                if rows:
                    st.plotly_chart(build_funding_history_figure(rows, hist_coin), config={'displayModeBar': True, 'scrollZoom': True})
                else:
                    st.info("暂无资金费率历史数据")

            elif hist_type == "每日摘要":
                rows = query_daily_summaries(hist_coin, days=hist_hours // 24 + 1,
                                              db_path=db_path)
                if rows:
                    df = pd.DataFrame(rows)
                    st.dataframe(df, width='stretch', hide_index=True)
                else:
                    st.info("暂无每日摘要数据")

        # Alert history from DB
        st.markdown("---")
        st.markdown("#### 数据库告警历史")
        alert_hrs = st.slider("查询范围 (小时)", 1, 168, 24, key="alert_hist_hrs")
        alert_rows = query_alert_history(hours=alert_hrs, db_path=db_path)
        if alert_rows:
            st.plotly_chart(build_alert_timeline_from_db(alert_rows), config={'displayModeBar': True, 'scrollZoom': True})
            a_df_rows = [{
                "时间": pd.Timestamp(r["timestamp_ms"], unit="ms").strftime("%m-%d %H:%M"),
                "交易所": r.get("exchange", "-"),
                "类型": r.get("alert_type", "-"),
                "等级": r.get("severity", "-"),
                "消息": (r.get("message", ""))[:60],
                "强度": f"{r.get('score', 0):.2f}",
            } for r in alert_rows[:100]]
            st.dataframe(pd.DataFrame(a_df_rows), width='stretch', hide_index=True)
        else:
            st.info("暂无告警历史记录")

    # ── 数据库管理 ──
    with sub_tabs[3]:
        st.markdown("#### 数据库管理")
        db_path_mgmt = st.text_input("数据库路径", value="market_data.db", key="db_path_mgmt")

        try:
            init_db(db_path_mgmt)
            stats = get_db_stats(db_path_mgmt)
        except Exception as e:
            st.error(f"数据库错误: {e}")
            st.stop()

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("OI 记录",      f"{stats.get('oi_history', 0):,}")
        col2.metric("资金费率记录",  f"{stats.get('funding_history', 0):,}")
        col3.metric("告警记录",      f"{stats.get('alert_history', 0):,}")
        col4.metric("DB 大小",       f"{stats.get('db_size_mb', 0)} MB")

        st.markdown("---")
        st.markdown("##### 数据清理（保留最近 N 天）")
        c1, c2, c3 = st.columns(3)
        keep_oi  = c1.number_input("OI 历史保留 (天)",      1, 365, 30, key="keep_oi")
        keep_fr  = c2.number_input("资金费率历史保留 (天)", 1, 365, 90, key="keep_fr")
        keep_alr = c3.number_input("告警记录保留 (天)",    1, 30,  7,  key="keep_alr")

        if st.button("执行清理", key="do_cleanup", type="secondary"):
            with st.spinner("清理中..."):
                cleanup_old_data(int(keep_oi), int(keep_fr), int(keep_alr), db_path_mgmt)
            stats_after = get_db_stats(db_path_mgmt)
            st.success(f"✅ 清理完成。数据库大小: {stats_after.get('db_size_mb', 0)} MB")

        st.markdown("---")
        st.markdown("##### 自动归档设置")
        st.caption("启用后，后台线程每5分钟自动将 OI / 资金费率快照写入数据库")
        auto_archive = st.checkbox("启用自动 OI / 资金费率归档",
                                    value=st.session_state.get("auto_archive", False),
                                    key="auto_archive_cb")
        st.session_state["auto_archive"] = auto_archive
        if service is not None:
            if auto_archive:
                service.enable_auto_archive(db_path_mgmt)
            else:
                service.disable_auto_archive()

        st.markdown("---")
        st.markdown("##### Parquet 压缩归档")
        st.caption("将数据库导出为 Parquet 文件（snappy压缩），适合长期存储和离线分析")
        col_a, col_b = st.columns(2)
        with col_a:
            parquet_dir = st.text_input("归档目录", value="parquet_archive", key="parquet_dir")
        with col_b:
            parquet_tables = st.multiselect(
                "选择导出的表",
                ["oi_history","funding_history","daily_summary","alert_history"],
                default=["oi_history","funding_history"],
                key="parquet_tables",
            )
        if st.button("立即导出 Parquet", key="do_parquet"):
            with st.spinner("导出中..."):
                results = auto_parquet_archive(parquet_tables, parquet_dir, db_path_mgmt)
            for table, path in results.items():
                if path:
                    st.success(f"✅ {table} → {path}")
                else:
                    st.warning(f"⚠️ {table} 导出失败（可能需要安装 pyarrow：pip install pyarrow）")

        # Show existing parquet files
        parquet_files = get_parquet_files(parquet_dir if 'parquet_dir' in dir() else "parquet_archive")
        if parquet_files:
            st.markdown("**已归档文件：**")
            for pf in parquet_files[:10]:
                import os
                sz = os.path.getsize(pf) / 1024
                st.caption(f"📄 {os.path.basename(pf)}  ({sz:.1f} KB)")

    # ── 通知记录 ──
    with sub_tabs[4]:
        st.markdown("#### 通知发送记录")
        records = notifier.get_records(100)
        if records:
            r_rows = [{
                "时间": pd.Timestamp(r.timestamp_ms, unit="ms").strftime("%m-%d %H:%M:%S"),
                "渠道": r.channel,
                "类型": r.alert_type,
                "等级": r.severity,
                "状态": "✅ 成功" if r.success else "❌ 失败",
                "错误": r.error or "",
                "消息": r.message[:60],
            } for r in records]
            st.dataframe(pd.DataFrame(r_rows), width='stretch', hide_index=True)
            if st.button("清空记录", key="clear_notif_records"):
                notifier.clear_records()
                st.rerun()
        else:
            st.info("暂无通知记录")

    # ── 命中率统计 ──
    with sub_tabs[5]:
        st.markdown("#### 告警命中率统计")
        st.caption("统计各类告警发出后，价格是否在指定时间内按预期方向运动（基于数据库历史）")

        db_path_hr = st.text_input("数据库路径", value="market_data.db", key="db_path_hr")
        col1, col2, col3 = st.columns(3)
        with col1:
            hr_hours = st.slider("统计范围 (小时)", 1, 168, 72, key="hr_hours")
        with col2:
            check_window_min = st.slider("验证窗口 (分钟)", 5, 60, 15, key="hr_check_win")
        with col3:
            min_move_pct = st.slider("最小验证涨跌幅 (%)", 0.1, 2.0, 0.3, step=0.1, key="hr_min_move")

        try:
            init_db(db_path_hr)
            alert_rows = query_alert_history(hours=hr_hours, db_path=db_path_hr)
        except Exception as e:
            st.error(f"数据库错误: {e}")
            alert_rows = []

        if alert_rows:
            # Group by alert_type and compute stats
            from collections import defaultdict
            type_stats = defaultdict(lambda: {"count":0,"bullish":0,"bearish":0,"neutral":0})
            severity_stats = defaultdict(lambda: {"count":0,"high":0,"medium":0,"low":0})

            for row in alert_rows:
                atype = row.get("alert_type","unknown")
                sev   = row.get("severity","medium")
                type_stats[atype]["count"] += 1
                severity_stats[sev]["count"] += 1

            # Summary metrics
            total = len(alert_rows)
            strong_count = sum(1 for r in alert_rows if r.get("severity")=="strong")
            medium_count = sum(1 for r in alert_rows if r.get("severity")=="medium")
            c1,c2,c3,c4 = st.columns(4)
            c1.metric("总告警数", total)
            c2.metric("🔴 强告警", strong_count)
            c3.metric("🟠 中告警", medium_count)
            c4.metric("均值强度", f"{sum(r.get('score',0) for r in alert_rows)/max(total,1):.2f}")

            # By type table
            st.markdown("---")
            st.markdown("##### 按类型统计")
            type_rows = []
            for atype, stats in sorted(type_stats.items(), key=lambda x: x[1]["count"], reverse=True):
                type_rows.append({
                    "告警类型": atype,
                    "触发次数": stats["count"],
                    "占比": f"{stats['count']/total*100:.1f}%",
                })
            st.dataframe(pd.DataFrame(type_rows), width='stretch', hide_index=True)

            # Score distribution chart
            st.markdown("---")
            st.markdown("##### 告警强度分布")
            scores = [r.get("score",0) for r in alert_rows if r.get("score",0) > 0]
            if scores:
                fig_dist = go.Figure()
                fig_dist.add_trace(go.Histogram(
                    x=scores, nbinsx=20,
                    marker_color="#7c3aed", opacity=0.8,
                    name="强度分布",
                ))
                fig_dist.update_layout(
                    height=280, template="plotly_dark",
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    xaxis_title="告警强度分", yaxis_title="次数",
                    margin=dict(l=40,r=20,t=20,b=40),
                )
                st.plotly_chart(fig_dist, key="alert_score_dist",
                                config={'displayModeBar': True, 'scrollZoom': True})

            # Timeline heatmap by hour
            st.markdown("---")
            st.markdown("##### 24小时告警分布热力图")
            hour_counts = defaultdict(int)
            for row in alert_rows:
                try:
                    dt = pd.Timestamp(row["timestamp_ms"], unit="ms")
                    hour_counts[dt.hour] += 1
                except:
                    pass
            if hour_counts:
                hours = list(range(24))
                counts = [hour_counts.get(h, 0) for h in hours]
                fig_hour = go.Figure(go.Bar(
                    x=hours, y=counts,
                    marker_color=["#ff4444" if c == max(counts) else "#7c3aed" for c in counts],
                    text=counts, textposition="outside",
                ))
                fig_hour.update_layout(
                    height=260, template="plotly_dark",
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    xaxis_title="小时 (UTC+8)", yaxis_title="告警次数",
                    margin=dict(l=40,r=20,t=20,b=40),
                )
                st.plotly_chart(fig_hour, key="alert_hour_dist",
                                config={'displayModeBar': True, 'scrollZoom': True})

            st.markdown("---")
            st.caption(f"⚠️ 真实命中率验证需要对每条告警事后匹配价格走势。"
                       f"当前统计为告警频次分析；完整命中率回测请配合 K 线数据使用信号回测引擎（信号增强中心 → K线形态 Tab）。")
        else:
            st.info("数据库中暂无告警历史。请先启用自动归档并等待系统运行一段时间。")
