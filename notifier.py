"""
notifier.py  —  推送通知系统（v6）
覆盖：Telegram Bot API / 浏览器桌面通知 JS片段 / 声音告警 / 冷却管理
"""
from __future__ import annotations
import time
import uuid
import threading
from typing import Dict, List, Optional

import requests

from models import (
    PushNotificationConfig, NotificationRecord, AlertEvent, ConfirmedAlert,
)

TELEGRAM_API = "https://api.telegram.org"

# ── 冷却管理器 ─────────────────────────────────────────────────────────────────

class CooldownManager:
    """相同 alert_type+exchange 在 cooldown 秒内只推送一次"""
    def __init__(self):
        self._last_sent: Dict[str, float] = {}
        self._lock = threading.Lock()

    def can_send(self, key: str, cooldown_seconds: int) -> bool:
        with self._lock:
            now = time.time()
            last = self._last_sent.get(key, 0.0)
            if now - last >= cooldown_seconds:
                self._last_sent[key] = now
                return True
            return False

    def reset(self, key: str):
        with self._lock:
            self._last_sent.pop(key, None)


_cooldown = CooldownManager()


# ── Telegram ───────────────────────────────────────────────────────────────────

def send_telegram(token: str, chat_id: str, text: str,
                  parse_mode: str = "HTML", timeout: int = 8) -> bool:
    """发送 Telegram 消息，返回是否成功"""
    if not token or not chat_id:
        return False
    try:
        url = f"{TELEGRAM_API}/bot{token}/sendMessage"
        resp = requests.post(url, json={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        }, timeout=timeout)
        return resp.status_code == 200
    except Exception:
        return False


def format_alert_message(alert: ConfirmedAlert, coin: str = "") -> str:
    """格式化告警为 Telegram HTML 消息"""
    severity_emoji = {"strong": "🚨", "medium": "⚠️", "weak": "💡"}.get(alert.severity, "📢")
    ts = time.strftime("%H:%M:%S", time.localtime(alert.confirmed_at_ms / 1000))
    lines = [
        f"{severity_emoji} <b>[{alert.alert_type.upper()}]</b>",
        f"交易所: <code>{alert.exchange}</code>",
    ]
    if coin:
        lines.append(f"币种: <code>{coin}</code>")
    lines += [
        f"时间: {ts}",
        f"详情: {alert.message}",
        f"强度: {alert.score:.2f}",
    ]
    return "\n".join(lines)


def format_arbitrage_message(coin: str, high_ex: str, low_ex: str,
                              spread_bps: float) -> str:
    return (
        f"⚡ <b>套利机会</b> {coin}\n"
        f"高价所: <code>{high_ex}</code>  低价所: <code>{low_ex}</code>\n"
        f"价差: <b>{spread_bps:.1f} bps</b>"
    )


def format_funding_arb_message(coin: str, long_ex: str, short_ex: str,
                                net_bps: float, annual_pct: float) -> str:
    return (
        f"💰 <b>资金费率套利</b> {coin}\n"
        f"做多: <code>{long_ex}</code>  做空: <code>{short_ex}</code>\n"
        f"每期净收: <b>{net_bps:.1f} bps</b>  年化: <b>{annual_pct:.1f}%</b>"
    )


# ── 通知分发器 ─────────────────────────────────────────────────────────────────

class Notifier:
    def __init__(self, config: Optional[PushNotificationConfig] = None):
        self.config = config or PushNotificationConfig()
        self._records: List[NotificationRecord] = []
        self._lock = threading.Lock()

    def update_config(self, config: PushNotificationConfig):
        self.config = config

    def _record(self, channel: str, alert_type: str, message: str,
                severity: str, success: bool, error: str = None):
        with self._lock:
            self._records.append(NotificationRecord(
                notification_id=str(uuid.uuid4())[:8],
                timestamp_ms=int(time.time() * 1000),
                channel=channel, alert_type=alert_type,
                message=message, severity=severity,
                success=success, error=error,
            ))
            if len(self._records) > 200:
                self._records = self._records[-200:]

    def send_alert(self, alert: ConfirmedAlert, coin: str = "") -> bool:
        """分发告警到所有启用的渠道，遵守冷却时间"""
        cfg = self.config
        severity_rank = {"weak": 0, "medium": 1, "strong": 2, "critical": 3}
        min_rank = severity_rank.get(cfg.min_severity, 1)
        cur_rank = severity_rank.get(alert.severity, 0)
        if cur_rank < min_rank:
            return False

        cooldown_key = f"{alert.alert_type}_{alert.exchange}"
        if not _cooldown.can_send(cooldown_key, cfg.cooldown_seconds):
            return False

        message = format_alert_message(alert, coin)
        sent_any = False

        if cfg.telegram_enabled and cfg.telegram_token and cfg.telegram_chat_id:
            ok = send_telegram(cfg.telegram_token, cfg.telegram_chat_id, message)
            self._record("telegram", alert.alert_type, message, alert.severity,
                         ok, None if ok else "send failed")
            sent_any = sent_any or ok

        return sent_any

    def send_raw(self, alert_type: str, message: str,
                 severity: str = "medium") -> bool:
        """发送自定义消息"""
        cfg = self.config
        cooldown_key = f"raw_{alert_type}"
        if not _cooldown.can_send(cooldown_key, cfg.cooldown_seconds):
            return False

        sent_any = False
        if cfg.telegram_enabled and cfg.telegram_token and cfg.telegram_chat_id:
            ok = send_telegram(cfg.telegram_token, cfg.telegram_chat_id, message)
            self._record("telegram", alert_type, message, severity,
                         ok, None if ok else "send failed")
            sent_any = sent_any or ok

        return sent_any

    def test_telegram(self) -> Tuple[bool, str]:
        """测试 Telegram 连通性"""
        cfg = self.config
        if not cfg.telegram_token:
            return False, "未填写 Token"
        if not cfg.telegram_chat_id:
            return False, "未填写 Chat ID"
        ok = send_telegram(cfg.telegram_token, cfg.telegram_chat_id,
                           "✅ 多交易所终端 v6 — Telegram 推送测试成功！")
        return ok, "发送成功" if ok else "发送失败，请检查 Token 和 Chat ID"

    def get_records(self, limit: int = 50) -> List[NotificationRecord]:
        with self._lock:
            return list(reversed(self._records[-limit:]))

    def clear_records(self):
        with self._lock:
            self._records.clear()


# ── 浏览器通知 JS 片段（在 Streamlit 中注入）──────────────────────────────────

BROWSER_NOTIFICATION_JS = """
<script>
function requestNotificationPermission() {
    if ('Notification' in window && Notification.permission === 'default') {
        Notification.requestPermission();
    }
}
function sendBrowserNotification(title, body, severity) {
    if (!('Notification' in window)) return;
    if (Notification.permission !== 'granted') return;
    const icons = {strong: '🚨', medium: '⚠️', weak: '💡'};
    new Notification((icons[severity] || '📢') + ' ' + title, {
        body: body,
        icon: '/favicon.ico',
        tag: title,
        requireInteraction: severity === 'strong',
    });
}
function playAlertSound(severity) {
    if (window._globalMuted) return;
    try {
        const ctx = new (window.AudioContext || window.webkitAudioContext)();
        const now = ctx.currentTime;
        if (severity === 'strong') {
            // 急促三连蜂鸣：高频 880Hz 连续3次
            [0, 0.18, 0.36].forEach(offset => {
                const o = ctx.createOscillator();
                const g = ctx.createGain();
                o.connect(g); g.connect(ctx.destination);
                o.type = 'square';
                o.frequency.value = 880;
                g.gain.setValueAtTime(0.28, now + offset);
                g.gain.exponentialRampToValueAtTime(0.001, now + offset + 0.14);
                o.start(now + offset); o.stop(now + offset + 0.15);
            });
        } else if (severity === 'medium') {
            // 单音双击：中频 660Hz
            [0, 0.22].forEach(offset => {
                const o = ctx.createOscillator();
                const g = ctx.createGain();
                o.connect(g); g.connect(ctx.destination);
                o.type = 'sine';
                o.frequency.value = 660;
                g.gain.setValueAtTime(0.22, now + offset);
                g.gain.exponentialRampToValueAtTime(0.001, now + offset + 0.18);
                o.start(now + offset); o.stop(now + offset + 0.2);
            });
        } else {
            // 弱告警：低频单音 440Hz 轻柔
            const o = ctx.createOscillator();
            const g = ctx.createGain();
            o.connect(g); g.connect(ctx.destination);
            o.type = 'sine';
            o.frequency.value = 440;
            g.gain.setValueAtTime(0.12, now);
            g.gain.exponentialRampToValueAtTime(0.001, now + 0.4);
            o.start(now); o.stop(now + 0.42);
        }
    } catch(e) {}
}
function setGlobalMute(muted) {
    window._globalMuted = muted;
    localStorage.setItem('alert_muted', muted ? '1' : '0');
}
window._globalMuted = localStorage.getItem('alert_muted') === '1';
requestNotificationPermission();
</script>
"""


def get_browser_notification_html(title: str, body: str,
                                   severity: str = "medium") -> str:
    """返回注入浏览器通知的 HTML/JS 片段"""
    escaped_title = title.replace('"', '\\"').replace("'", "\\'")
    escaped_body  = body.replace('"', '\\"').replace("'", "\\'")
    return f"""
<script>
(function() {{
    const sev = '{severity}';
    if (typeof sendBrowserNotification !== 'undefined') {{
        sendBrowserNotification('{escaped_title}', '{escaped_body}', sev);
    }}
    if (typeof playAlertSound !== 'undefined') {{
        playAlertSound(sev);
    }}
}})();
</script>
"""


# ── 全局单例 ───────────────────────────────────────────────────────────────────
_global_notifier: Optional[Notifier] = None


def get_notifier() -> Notifier:
    global _global_notifier
    if _global_notifier is None:
        _global_notifier = Notifier()
    return _global_notifier


def init_notifier(config: PushNotificationConfig) -> Notifier:
    global _global_notifier
    _global_notifier = Notifier(config)
    return _global_notifier
