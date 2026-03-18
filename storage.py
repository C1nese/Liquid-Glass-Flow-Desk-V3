"""
storage.py  —  本地 SQLite 持久化层（v6）
覆盖：OI / 资金费率长期历史 / 每日摘要归档 / 告警历史 / 自动清理
"""
from __future__ import annotations
import json
import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from typing import Dict, List, Optional, Tuple

from models import (
    PersistentOIRecord, DailyMarketSummary,
    ExchangeSnapshot, AlertEvent,
)

DEFAULT_DB_PATH = "market_data.db"
_lock = threading.Lock()


@contextmanager
def _conn(db_path: str):
    with _lock:
        con = sqlite3.connect(db_path, check_same_thread=False, timeout=15)
        con.row_factory = sqlite3.Row
        try:
            yield con
            con.commit()
        except Exception:
            con.rollback()
            raise
        finally:
            con.close()


# ══════════════════════════════════════════════════════════════════════════════
# 初始化
# ══════════════════════════════════════════════════════════════════════════════

def init_db(db_path: str = DEFAULT_DB_PATH):
    """创建所有表（如果不存在）"""
    with _conn(db_path) as con:
        con.executescript("""
        CREATE TABLE IF NOT EXISTS oi_history (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp_ms INTEGER NOT NULL,
            coin         TEXT    NOT NULL,
            exchange     TEXT    NOT NULL,
            oi_notional  REAL,
            funding_rate REAL,
            price        REAL
        );
        CREATE INDEX IF NOT EXISTS idx_oi_ts   ON oi_history(timestamp_ms);
        CREATE INDEX IF NOT EXISTS idx_oi_coin ON oi_history(coin, exchange, timestamp_ms);

        CREATE TABLE IF NOT EXISTS daily_summary (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            date_str            TEXT    NOT NULL,
            coin                TEXT    NOT NULL,
            open_price          REAL,
            high_price          REAL,
            low_price           REAL,
            close_price         REAL,
            volume_24h          REAL,
            oi_open             REAL,
            oi_close            REAL,
            oi_change_pct       REAL,
            funding_avg_bps     REAL,
            liq_total           REAL,
            liq_long_pct        REAL,
            max_sentiment_score REAL,
            min_sentiment_score REAL,
            UNIQUE(date_str, coin)
        );

        CREATE TABLE IF NOT EXISTS alert_history (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp_ms INTEGER NOT NULL,
            exchange     TEXT,
            alert_type   TEXT,
            severity     TEXT,
            message      TEXT,
            score        REAL,
            extra_json   TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_alert_ts ON alert_history(timestamp_ms);

        CREATE TABLE IF NOT EXISTS funding_history (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp_ms INTEGER NOT NULL,
            coin         TEXT    NOT NULL,
            exchange     TEXT    NOT NULL,
            funding_rate REAL,
            predicted_rate REAL
        );
        CREATE INDEX IF NOT EXISTS idx_fr_ts ON funding_history(timestamp_ms);
        CREATE INDEX IF NOT EXISTS idx_fr_coin ON funding_history(coin, exchange, timestamp_ms);

        CREATE TABLE IF NOT EXISTS notification_log (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp_ms INTEGER,
            channel      TEXT,
            alert_type   TEXT,
            message      TEXT,
            severity     TEXT,
            success      INTEGER
        );
        """)


# ══════════════════════════════════════════════════════════════════════════════
# OI 历史
# ══════════════════════════════════════════════════════════════════════════════

def insert_oi_record(rec: PersistentOIRecord, db_path: str = DEFAULT_DB_PATH):
    with _conn(db_path) as con:
        con.execute("""
            INSERT INTO oi_history (timestamp_ms, coin, exchange, oi_notional, funding_rate, price)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (rec.timestamp_ms, rec.coin, rec.exchange,
              rec.oi_notional, rec.funding_rate, rec.price))


def insert_oi_from_snapshots(snapshots: List[ExchangeSnapshot],
                              db_path: str = DEFAULT_DB_PATH):
    """批量写入多交易所快照的OI数据"""
    now_ms = int(time.time() * 1000)
    rows = []
    for snap in snapshots:
        if snap.status != "ok" or not snap.open_interest_notional:
            continue
        coin = snap.symbol.replace("USDT", "").replace("-USDT-SWAP", "")
        rows.append((now_ms, coin, snap.exchange,
                     snap.open_interest_notional,
                     snap.funding_rate, snap.last_price))
    if not rows:
        return
    with _conn(db_path) as con:
        con.executemany("""
            INSERT INTO oi_history (timestamp_ms, coin, exchange, oi_notional, funding_rate, price)
            VALUES (?, ?, ?, ?, ?, ?)
        """, rows)


def query_oi_history(coin: str, exchange: str = None,
                     hours: int = 24,
                     db_path: str = DEFAULT_DB_PATH) -> List[Dict]:
    """查询指定币种的OI历史"""
    cutoff = int(time.time() * 1000) - hours * 3_600_000
    with _conn(db_path) as con:
        if exchange:
            rows = con.execute("""
                SELECT timestamp_ms, exchange, oi_notional, funding_rate, price
                FROM oi_history
                WHERE coin=? AND exchange=? AND timestamp_ms>=?
                ORDER BY timestamp_ms
            """, (coin, exchange, cutoff)).fetchall()
        else:
            rows = con.execute("""
                SELECT timestamp_ms, exchange, oi_notional, funding_rate, price
                FROM oi_history
                WHERE coin=? AND timestamp_ms>=?
                ORDER BY timestamp_ms
            """, (coin, cutoff)).fetchall()
    return [dict(r) for r in rows]


def query_oi_multi_exchange(coin: str, hours: int = 72,
                             db_path: str = DEFAULT_DB_PATH
                             ) -> Dict[str, List[Dict]]:
    """按交易所分组返回 OI 历史（用于多折线图）"""
    rows = query_oi_history(coin, hours=hours, db_path=db_path)
    by_ex: Dict[str, List[Dict]] = {}
    for r in rows:
        by_ex.setdefault(r["exchange"], []).append(r)
    return by_ex


# ══════════════════════════════════════════════════════════════════════════════
# 资金费率历史
# ══════════════════════════════════════════════════════════════════════════════

def insert_funding_record(coin: str, exchange: str,
                           funding_rate: float, predicted_rate: float = None,
                           db_path: str = DEFAULT_DB_PATH):
    now_ms = int(time.time() * 1000)
    with _conn(db_path) as con:
        con.execute("""
            INSERT INTO funding_history (timestamp_ms, coin, exchange, funding_rate, predicted_rate)
            VALUES (?, ?, ?, ?, ?)
        """, (now_ms, coin, exchange, funding_rate, predicted_rate))


def insert_funding_from_snapshots(snapshots: List[ExchangeSnapshot],
                                   db_path: str = DEFAULT_DB_PATH):
    now_ms = int(time.time() * 1000)
    rows = []
    for snap in snapshots:
        if snap.funding_rate is None or snap.status != "ok":
            continue
        coin = snap.symbol.replace("USDT", "").replace("-USDT-SWAP", "")
        rows.append((now_ms, coin, snap.exchange, snap.funding_rate, None))
    if not rows:
        return
    with _conn(db_path) as con:
        con.executemany("""
            INSERT INTO funding_history (timestamp_ms, coin, exchange, funding_rate, predicted_rate)
            VALUES (?, ?, ?, ?, ?)
        """, rows)


def query_funding_history(coin: str, exchange: str = None,
                           hours: int = 72,
                           db_path: str = DEFAULT_DB_PATH) -> List[Dict]:
    cutoff = int(time.time() * 1000) - hours * 3_600_000
    with _conn(db_path) as con:
        if exchange:
            rows = con.execute("""
                SELECT timestamp_ms, exchange, funding_rate, predicted_rate
                FROM funding_history
                WHERE coin=? AND exchange=? AND timestamp_ms>=?
                ORDER BY timestamp_ms
            """, (coin, exchange, cutoff)).fetchall()
        else:
            rows = con.execute("""
                SELECT timestamp_ms, exchange, funding_rate, predicted_rate
                FROM funding_history
                WHERE coin=? AND timestamp_ms>=?
                ORDER BY timestamp_ms
            """, (coin, cutoff)).fetchall()
    return [dict(r) for r in rows]


# ══════════════════════════════════════════════════════════════════════════════
# 每日摘要
# ══════════════════════════════════════════════════════════════════════════════

def upsert_daily_summary(summary: DailyMarketSummary,
                          db_path: str = DEFAULT_DB_PATH):
    with _conn(db_path) as con:
        con.execute("""
            INSERT OR REPLACE INTO daily_summary
            (date_str, coin, open_price, high_price, low_price, close_price,
             volume_24h, oi_open, oi_close, oi_change_pct, funding_avg_bps,
             liq_total, liq_long_pct, max_sentiment_score, min_sentiment_score)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (summary.date_str, summary.coin,
              summary.open_price, summary.high_price,
              summary.low_price, summary.close_price,
              summary.volume_24h, summary.oi_open, summary.oi_close,
              summary.oi_change_pct, summary.funding_avg_bps,
              summary.liq_total, summary.liq_long_pct,
              summary.max_sentiment_score, summary.min_sentiment_score))


def query_daily_summaries(coin: str, days: int = 30,
                           db_path: str = DEFAULT_DB_PATH) -> List[Dict]:
    with _conn(db_path) as con:
        rows = con.execute("""
            SELECT * FROM daily_summary
            WHERE coin=?
            ORDER BY date_str DESC
            LIMIT ?
        """, (coin, days)).fetchall()
    return [dict(r) for r in rows]


def auto_archive_daily(snapshots: List[ExchangeSnapshot],
                        db_path: str = DEFAULT_DB_PATH):
    """每天运行一次，归档今日最后快照到 daily_summary"""
    today = time.strftime("%Y-%m-%d")
    by_coin: Dict[str, List[ExchangeSnapshot]] = {}
    for snap in snapshots:
        coin = snap.symbol.replace("USDT", "").replace("-USDT-SWAP", "")
        by_coin.setdefault(coin, []).append(snap)

    for coin, snaps in by_coin.items():
        valid = [s for s in snaps if s.status == "ok" and s.last_price]
        if not valid:
            continue
        avg_price   = sum(s.last_price for s in valid) / len(valid)
        avg_oi      = sum((s.open_interest_notional or 0) for s in valid) / len(valid)
        funding_bps = sum((s.funding_rate or 0) * 10000 for s in valid) / len(valid)
        summary = DailyMarketSummary(
            date_str=today, coin=coin,
            open_price=avg_price, high_price=avg_price,
            low_price=avg_price,  close_price=avg_price,
            volume_24h=sum((s.volume_24h_notional or 0) for s in valid),
            oi_open=avg_oi, oi_close=avg_oi,
            oi_change_pct=0.0, funding_avg_bps=funding_bps,
            liq_total=0.0, liq_long_pct=0.0,
            max_sentiment_score=0.0, min_sentiment_score=0.0,
        )
        upsert_daily_summary(summary, db_path)


# ══════════════════════════════════════════════════════════════════════════════
# 告警历史
# ══════════════════════════════════════════════════════════════════════════════

def insert_alert_history(alert_type: str, exchange: str, severity: str,
                          message: str, score: float = 0.0,
                          extra: dict = None,
                          db_path: str = DEFAULT_DB_PATH):
    now_ms = int(time.time() * 1000)
    extra_json = json.dumps(extra or {})
    with _conn(db_path) as con:
        con.execute("""
            INSERT INTO alert_history (timestamp_ms, exchange, alert_type, severity, message, score, extra_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (now_ms, exchange, alert_type, severity, message, score, extra_json))


def query_alert_history(hours: int = 24, limit: int = 200,
                         db_path: str = DEFAULT_DB_PATH) -> List[Dict]:
    cutoff = int(time.time() * 1000) - hours * 3_600_000
    with _conn(db_path) as con:
        rows = con.execute("""
            SELECT * FROM alert_history
            WHERE timestamp_ms >= ?
            ORDER BY timestamp_ms DESC
            LIMIT ?
        """, (cutoff, limit)).fetchall()
    return [dict(r) for r in rows]


# ══════════════════════════════════════════════════════════════════════════════
# 数据清理（防止 DB 无限增长）
# ══════════════════════════════════════════════════════════════════════════════

def cleanup_old_data(keep_oi_days: int = 30,
                     keep_funding_days: int = 90,
                     keep_alert_days: int = 7,
                     db_path: str = DEFAULT_DB_PATH):
    """清理过期数据"""
    now_ms = int(time.time() * 1000)
    oi_cutoff      = now_ms - keep_oi_days      * 86_400_000
    funding_cutoff = now_ms - keep_funding_days * 86_400_000
    alert_cutoff   = now_ms - keep_alert_days   * 86_400_000
    with _conn(db_path) as con:
        con.execute("DELETE FROM oi_history      WHERE timestamp_ms < ?", (oi_cutoff,))
        con.execute("DELETE FROM funding_history WHERE timestamp_ms < ?", (funding_cutoff,))
        con.execute("DELETE FROM alert_history   WHERE timestamp_ms < ?", (alert_cutoff,))
        con.execute("VACUUM")


def get_db_stats(db_path: str = DEFAULT_DB_PATH) -> Dict[str, int]:
    """获取数据库各表行数"""
    stats = {}
    if not os.path.exists(db_path):
        return stats
    with _conn(db_path) as con:
        for table in ("oi_history", "funding_history", "daily_summary",
                      "alert_history", "notification_log"):
            try:
                row = con.execute(f"SELECT COUNT(*) as n FROM {table}").fetchone()
                stats[table] = row["n"]
            except:
                stats[table] = 0
    size_bytes = os.path.getsize(db_path)
    stats["db_size_mb"] = round(size_bytes / 1_048_576, 2)
    return stats


# ══════════════════════════════════════════════════════════════════════════════
# Parquet 压缩归档（大数据量长期存储）
# ══════════════════════════════════════════════════════════════════════════════

def export_to_parquet(table: str, output_dir: str = "parquet_archive",
                       db_path: str = DEFAULT_DB_PATH) -> Optional[str]:
    """
    将指定表导出为 Parquet 文件（按日期分区）。
    需要安装 pandas + pyarrow / fastparquet。
    返回输出文件路径，失败返回 None。
    """
    import os
    try:
        import pandas as _pd
    except ImportError:
        return None

    os.makedirs(output_dir, exist_ok=True)
    today = __import__("time").strftime("%Y-%m-%d")

    with _conn(db_path) as con:
        df = _pd.read_sql_query(f"SELECT * FROM {table}", con)

    if df.empty:
        return None

    out_path = os.path.join(output_dir, f"{table}_{today}.parquet")
    try:
        df.to_parquet(out_path, index=False, compression="snappy")
        return out_path
    except Exception:
        # fallback to CSV if parquet not available
        csv_path = out_path.replace(".parquet", ".csv")
        df.to_csv(csv_path, index=False)
        return csv_path


def auto_parquet_archive(tables: list = None,
                          output_dir: str = "parquet_archive",
                          db_path: str = DEFAULT_DB_PATH) -> dict:
    """
    批量导出多个表为 Parquet/CSV，返回 {table: output_path}。
    建议每天凌晨运行一次。
    """
    if tables is None:
        tables = ["oi_history", "funding_history", "daily_summary", "alert_history"]
    results = {}
    for table in tables:
        path = export_to_parquet(table, output_dir, db_path)
        results[table] = path
    return results


def get_parquet_files(output_dir: str = "parquet_archive") -> list:
    """列出所有已归档的 Parquet/CSV 文件"""
    import os, glob
    if not os.path.exists(output_dir):
        return []
    files = glob.glob(os.path.join(output_dir, "*.parquet"))
    files += glob.glob(os.path.join(output_dir, "*.csv"))
    return sorted(files, reverse=True)
