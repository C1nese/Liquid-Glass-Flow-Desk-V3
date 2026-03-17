# Liquid-Glass-Flow-Desk-V3
Liquid Glass Flow Desk 普通V3版本
# 多交易所流动性终端 v4
### Multi-Exchange Crypto Liquidity Terminal

> **实盘级**加密货币流动性监控系统。四所 WebSocket 实时数据流 · 三所现货-合约价差 · 告警去抖动引擎 · OI+CVD+Funding 合成信号 · 爆仓簇跨所联动 · 盘口质量与假挂单检测 · 事件回放复盘。

---

## 目录

1. [功能概览](#功能概览)
2. [快速上手](#快速上手)
3. [界面导航（17个Tab）](#界面导航)
4. [系统架构](#系统架构)
5. [数据模型](#数据模型)
6. [告警引擎](#告警引擎)
7. [合成信号](#合成信号)
8. [盘口质量检测](#盘口质量检测)
9. [爆仓簇 V2](#爆仓簇-v2)
10. [回放复盘](#回放复盘)
11. [配置参数](#配置参数)
12. [文件结构](#文件结构)
13. [FAQ](#faq)

---

## 功能概览

| 模块 | 说明 |
|------|------|
| 🏛️ **四所实时行情** | Binance / Bybit / OKX / Hyperliquid 合约 WS |
| 📡 **三所现货 WS** | Bybit Spot + OKX Spot 真实 WS；Binance 用 Index Price |
| 📊 **OI 可视化** | 持仓量 + 彩色变化柱 + 速率曲线 |
| 💧 **CVD 主动买卖** | Taker Buy/Sell 累积量差，Binance K线 taker 字段最准 |
| 🔗 **Spot-Perp 价差** | 三所实时价差折线 + Lead/Lag 告警 |
| 🧠 **合成信号引擎** | 价格×OI×CVD×Funding×Crowd → 偏多/偏空/衰竭/吸收 |
| 💥 **爆仓瀑布 V2** | 多/空分栏 · 单所/跨所联动 · 爆仓簇气泡图 · 甘特时间轴 |
| 📋 **盘口质量** | 逐档新增/撤单追踪 · 假挂单检测 · 吸收事件识别 |
| ⏰ **告警时间线** | 去抖动确认（强2次/中3次/弱4次）· 可视化时间线 |
| 📼 **回放复盘** | 1x/5x/20x 速度 · 价格+CVD+爆仓同帧 · 最多1小时录制 |
| 🔔 **自定义预警** | 价格/OI/费率/CVD/OI速率 阈值告警 |
| 📡 **多币种轮巡** | 最多15个币种快速扫描 |

---

## 快速上手

### 环境要求

- Python **3.10+**
- 依赖（见 `requirements.txt`）：

```bash
pip install streamlit>=1.31 requests>=2.31 pandas>=1.5 plotly>=5.20 websocket-client>=1.8
```

### 启动

```bash
streamlit run app.py
```

默认访问 `http://localhost:8501`

### 第一次使用

1. 左侧侧边栏选择**常用币种**（如 BTC）
2. 点击**恢复默认**自动填入四所合约代码
3. 等待 5–10 秒，WebSocket 连接建立完成后数据自动流入
4. 顶部状态条变绿 = 所有交易所在线

---

## 界面导航

| Tab | 名称 | 核心内容 |
|-----|------|----------|
| 0 | 📈 专业终端 | K线 · 盘口热力 · OI曲线 · 爆仓 · 风险热图 · MBO画像 |
| 1 | 💧 CVD主动买卖 | 累积量差折线 · 实时成交流 · 买卖比 |
| 2 | 🔲 本地WS订单簿 | 合约+现货双订单簿状态 · 深度图 |
| 3 | 👥 OI四象限+速率 | 加仓/减仓/回补/减仓四象限 · 速率曲线 |
| 4 | 📊 多空比矩阵 | Binance OI可视化 · 多空人数比 · Taker买卖 · 拥挤度仪表 |
| 5 | 🔗 Spot-Perp | 三所实时价差折线 · Lead/Lag告警 · 历史记录 |
| 6 | 📐 Basis+期限结构 | 合约溢价 · 期限结构 · 现货vs合约持仓比 |
| 7 | 💥 爆仓瀑布 | 跨所瀑布 · 多/空左右分栏 · 真实vs推断分开 · 簇V2 · 联动甘特 |
| 8 | 🔍 冰山单+缺口 | 冰山单气泡 · 流动性缺口列表 |
| **9** | **🧠 合成信号引擎** | 四所信号卡 · 五因子雷达 · 时序图 · 因子明细 |
| **10** | **📋 盘口质量+假挂单** | 新增/撤单图 · 分价位热力 · 假挂单气泡 · 明细表 |
| **11** | **⏰ 告警时间线** | 已确认告警汇总 · 时间线散点 · 逐条确认次数 |
| **12** | **📼 回放复盘** | 开始/暂停/清空录制 · 速度选择 · 价格+CVD+爆仓回放图 |
| 13 | 🌐 全市场对比 | 四所横向对比 · 现货-合约价差列 |
| 14 | 📡 多币种轮巡 | 快速扫描最多15币种 |
| 15 | 🔔 预警系统 | 自定义阈值规则 · Spot-Perp告警汇总 |
| 16 | 🔧 调试 | 连接状态 · 所有原始 JSON |

---

## 系统架构

```
┌─────────────────────────────────────────────────────────────┐
│                        app.py (Streamlit UI)                │
│  17 Tabs · st.fragment(run_every=N) · 全部可视化渲染          │
└───────────────────┬─────────────────────────────────────────┘
                    │ 调用
┌───────────────────▼─────────────────────────────────────────┐
│              LiveTerminalService (realtime.py)              │
│                                                             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌───────────┐  │
│  │ Binance  │  │  Bybit   │  │   OKX    │  │Hyperliquid│  │
│  │ Perp WS  │  │ Perp WS  │  │ Perp WS  │  │    WS     │  │
│  └──────────┘  └──────────┘  └──────────┘  └───────────┘  │
│                                                             │
│  ┌──────────────────┐  ┌──────────────────┐                │
│  │  Bybit Spot WS   │  │   OKX Spot WS    │                │
│  └──────────────────┘  └──────────────────┘                │
│                                                             │
│  ┌───────────────────────────────────────────────────────┐  │
│  │              后台线程池                                │  │
│  │  sampler(15s) · cluster_builder(5s) · recorder(1s)   │  │
│  │  告警去抖动引擎 · 合成信号引擎 · 盘口质量追踪          │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                    │
┌───────────────────▼─────────────────────────────────────────┐
│            analytics.py (44 可视化函数)                     │
│  Plotly 图表 · HTML 控件 · DataFrame 构造                    │
└─────────────────────────────────────────────────────────────┘
                    │
┌───────────────────▼─────────────────────────────────────────┐
│            exchanges.py (REST 客户端)                       │
│  BybitClient · BinanceClient · OkxClient · HyperliquidClient│
└─────────────────────────────────────────────────────────────┘
```

### 线程模型

| 线程名 | 职责 | 间隔 |
|--------|------|------|
| `sampler` | REST 轮询四所行情 + OI + 触发合成信号 | 15s（可调） |
| `ws-binance` | Binance 合约 WS（mark/ticker/forceOrder/aggTrade/depth） | 长连接 |
| `ws-bybit` | Bybit 合约 WS（tickers/liquidation/trade/orderbook.200） | 长连接 |
| `ws-okx` | OKX 合约 WS（tickers/mark/trades/books/liquidation-warning） | 长连接 |
| `ws-hyperliquid` | HL WS（allMids/trades/l2Book） | 长连接 |
| `ws-spot-bybit` | Bybit 现货 WS（tickers/orderbook.50） | 长连接 |
| `ws-spot-okx` | OKX 现货 WS（tickers/books5） | 长连接 |
| `binance-depth-init` | Binance REST 订单簿快照 + 差量回放 | 一次性 |
| `cluster-builder` | 爆仓簇 V2 重建（跨所联动检测） | 5s |
| `recorder` | 事件录制（每秒一帧） | 1s |

---

## 数据模型

`models.py` 共 29 个数据类：

| 类名 | 用途 |
|------|------|
| `ExchangeSnapshot` | 交易所行情快照（含现货价注入字段） |
| `LiquidationEvent` | 单笔爆仓事件 |
| `OIPoint` | OI 时间序列点 |
| `TradeEvent` | 单笔成交（主动买/卖） |
| `CVDPoint` | 累积量差时间点 |
| `LocalOrderBook` | 本地 WS 维护的订单簿 |
| `OIDeltaPoint` | OI 四象限分析点 |
| `TopTraderRatio` | 多空比（大户/全市场） |
| `FuturesOIPoint` | 期货各到期日 OI（期限结构） |
| `SpotPerpSpreadPoint` | 现货-合约实时价差点 |
| `SpotPerpAlert` | 现货-合约乖离告警（原始） |
| `ConfirmedAlert` | **去抖动后已确认告警** |
| `AlertTimeline` | 告警时间线条目 |
| `OrderBookDeltaPoint` | 盘口单次差量（新增/撤单） |
| `FakeWallCandidate` | 假挂单候选 |
| `WallAbsorptionEvent` | 大单吸收事件 |
| `OrderBookQualitySnapshot` | 盘口质量综合评分 |
| `CompositeSignal` | 四因子合成信号 |
| `LiquidationClusterV2` | 爆仓簇（跨所联动增强版） |
| `RecordedFrame` | 回放录制帧 |
| `AlertRule` / `AlertEvent` | 自定义预警规则和触发记录 |
| `IcebergAlert` | 冰山单检测结果 |
| `LiquidityGap` | 流动性缺口 |

---

## 告警引擎

### 告警等级与确认机制

```
噪声原始信号 → _stage_alert() → 计数累积 → 达到阈值 → ConfirmedAlert
```

| 等级 | 连续确认次数 | 冷却时间 | 颜色 |
|------|------------|---------|------|
| 🔴 强 (strong) | **2 次** | 90 秒 | 红 |
| 🟡 中 (medium) | **3 次** | 180 秒 | 橙 |
| 🔵 弱 (weak)   | **4 次** | 300 秒 | 蓝 |

超过冷却期后重置计数，避免同一信号反复轰炸。

### 告警类型

| 类型 | 触发条件 | 默认等级 |
|------|---------|--------|
| `spot_lead_up` | 现货 5 tick 内上涨幅度比合约超出 **±15 bps** | 强/中 |
| `spot_lead_down` | 现货先于合约下跌 | 强/中 |
| `diverge_extreme` | 现货-合约价差绝对值 > **50 bps** | 强 |
| `oi_up_cvd_weak` | OI 涨幅 > 0.8% 但 CVD 净值为负 | 中 |
| `oi_down_cvd_up` | OI 降幅 > 0.8% 但 CVD 净值为正（轧空信号） | 中 |
| `crowd_liq_combo` | 60s 内爆仓 ≥ 3 笔且总额 > $50K | 强 |
| `fake_wall` | 大单（>$200K）存续 < 8 秒后撤离 | 弱 |
| `composite_signal` | 合成信号绝对值 > 0.6 且置信度 > 60% | 强/中 |

---

## 合成信号

五因子加权模型，每次 REST 采样（默认15s）重算：

```
composite = Σ weight_i × score_i

因子          权重    取值范围    计算方式
─────────────────────────────────────────────
价格动能        20%   [-1, +1]   近5采样点涨跌幅 × 200
OI方向          25%   [-1, +1]   OI变化与价格方向共振判断
CVD流向         25%   [-1, +1]   近10笔净买卖量 / 总量 × 3
资金费率        15%   [-1, +1]   负费率 → 正分（空头付费=偏多）
拥挤度          15%   [-1, +1]   基于资金费率的多空拥挤推断
```

**输出标签**：

| 合成分 | 标签 | 含义 |
|--------|------|------|
| > +0.45 | 偏多推进 ▲ | 多因子共振向上 |
| < −0.45 | 偏空推进 ▼ | 多因子共振向下 |
| \|x\| < 0.15，费率极端 | 拥挤衰竭 ⚡ | 方向不明但仓位极端 |
| \|x\| < 0.15，费率正常 | 吸收中 ≈ | 震荡吸筹/出货 |
| 0.15–0.45 | 弱多 → / 弱空 ← | 方向有倾向但不强 |

**置信度** = 五因子方向一致性 × \|合成分\|。 > 60% 才触发合成告警。

---

## 盘口质量检测

基于本地 WebSocket 订单簿每次差量（`_detect_orderbook_patterns_locked`）：

### 假挂单识别

```
条件：
  峰值名义金额 > $200,000
  AND 存续时间 < 8,000ms
  AND 完全撤单（delta = -peak_size）
→ 记录 FakeWallCandidate，触发弱级告警
```

### 质量得分算法

```python
q_score = 0.5 + (add_total - cancel_total * 1.5) / (add_total + cancel_total) * 0.5
```

- **> 0.6**：健康流动性，挂单持续流入
- **0.35–0.6**：正常波动
- **< 0.35**：大量撤单，可能为流动性退出信号

### 大单吸收检测（WallAbsorptionEvent）

大单被完全成交后 5 秒内同价位有补单，则记录为吸收事件 — 表明该价位有主动承接方。

---

## 爆仓簇 V2

后台每 5 秒运行一次 `_build_clusters_locked()`：

```
参数：
  CLUSTER_WINDOW_MS    = 30,000ms   # 30秒内的爆仓归为一簇
  CLUSTER_MIN_NOTIONAL = $100,000   # 簇总额门槛
  CLUSTER_CROSS_EX_WINDOW = 15,000ms  # 15秒内跨所=联动
```

**跨所联动** (`cross_exchange=True`)：同一簇涉及 ≥ 2 个交易所。

**瀑布得分** (`cascade_score`)：

```python
cascade_score = log10(total_notional - 4) × 0.25 + (0.3 if cross_exchange else 0)
```

越大代表爆仓越集中、传导越广。

---

## 回放复盘

录制器每秒保存一个 `RecordedFrame`（最多 3600 帧 = 1 小时）：

```python
RecordedFrame:
  timestamp_ms          # 帧时间戳
  prices                # {exchange: price}
  oi_notionals          # {exchange: oi}
  cvd_values            # {exchange: cvd_accum}
  liq_events            # 该秒内爆仓列表
  spread_bps            # {exchange: spot_perp_spread}
  composite_signals     # {exchange: CompositeSignal}
  funding_rates         # {exchange: funding_rate}
```

**回放模式**：

| 速度 | 说明 |
|------|------|
| 1x   | 显示所有帧 |
| 5x   | 每5帧取1帧 |
| 20x  | 每20帧取1帧 |

---

## 配置参数

侧边栏所有参数均实时生效，无需重启：

| 参数 | 默认 | 说明 |
|------|------|------|
| K线周期 | 5m | 1m/3m/5m/15m/30m/1h/4h/1d |
| K线数量 | 240 | 120–480 |
| 盘口深度 | 160 | 50–400档 |
| 爆仓统计分钟 | 60 | 统计窗口 |
| 成交流条数 | 500 | CVD计算用 |
| 多空比历史 | 80 | Binance REST拉取条数 |
| 界面刷新秒数 | 2 | 1–10s |
| 持仓采样秒数 | 15 | sampler线程间隔 |
| 请求超时 | 10 | REST超时 |

**合约代码格式**：

| 交易所 | 示例 |
|--------|------|
| Binance | `BTCUSDT` |
| Bybit | `BTCUSDT` |
| OKX | `BTC-USDT-SWAP` |
| Hyperliquid | `BTC` |

---

## 文件结构

```
.
├── app.py              # Streamlit 主界面（1353行，17 Tab）
├── realtime.py         # 实时引擎（1192行，10 线程）
├── analytics.py        # 可视化函数库（1220行，44 函数）
├── exchanges.py        # 交易所 REST 客户端（579行）
├── models.py           # 数据模型（432行，29 数据类）
├── requirements.txt    # 依赖清单
└── README.md           # 本文档
```

---

## FAQ

**Q: 启动后状态条显示"异常"？**

检查合约代码格式是否正确（见上表），点击「重连流」重新建立 WS。

**Q: 现货价显示"等待WS…"？**

Bybit/OKX 现货 WS 需要合约代码正确（如 `BTCUSDT` / `BTC-USDT-SWAP`），系统会自动推导现货符号。Binance 使用 Index Price 作为近似现货价。

**Q: 告警一直不触发？**

去抖动机制要求**连续 N 次**触发才确认。如果信号断断续续则不会确认。可在「调试」Tab 查看 `_pending_alerts` 中的累积计数。

**Q: 合成信号为"中性"？**

因子相互抵消时合成分接近 0。这本身是有效信号——代表方向不明确，适合观望。

**Q: 回放图没有数据？**

需要先在「回放复盘」Tab 点击「▶ 开始录制」，等待至少几秒钟后才有帧数据可回放。

**Q: 如何只看某一时段的爆仓？**

在「爆仓瀑布」Tab 使用 Plotly 图表的框选功能缩放时间轴，或调整侧边栏「爆仓统计分钟」参数。

**Q: 能加只读 API Key 吗？**

当前版本仅使用公开端点（无需 API Key）。如需私有账户数据（持仓、盈亏），可在 `exchanges.py` 中扩展各交易所客户端的签名逻辑。

---

## 免责声明

> 本工具为**信息展示**系统，所有"推断爆仓区"、"推断止盈区"等均为**模型推断**，非交易所真实清算数据。合成信号评分不构成投资建议。请独立判断，自担风险。
