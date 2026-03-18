# 多交易所流动性终端 

> **Liquid Glass Flow Desk** — 面向加密货币交易者的专业级实时流动性监控系统  
> 基于 Streamlit 构建，WebSocket 驱动，支持 Binance / Bybit / OKX / Hyperliquid 四所并发数据。

---

## 目录

- [功能概览](#功能概览)
- [快速启动](#快速启动)
- [依赖安装](#依赖安装)
- [界面与 Tab 导航](#界面与-tab-导航)
- [信号增强中心](#信号增强中心)
- [告警与推送系统](#告警与推送系统)
- [持久化存储](#持久化存储)
- [项目结构](#项目结构)
- [数据模型](#数据模型)
- [配置参数](#配置参数)
- [版本变更记录](#版本变更记录)

---

## 功能概览

| 类别 | 功能 |
|------|------|
| **实时行情** | 四所 WebSocket 并发，价格 / OI / 资金费率 / 成交流毫秒级更新 |
| **订单簿** | 本地维护 Level-2 订单簿，假墙检测 / 吸收事件 / 流动性缺口 |
| **CVD / 主动买卖** | Taker 成交量累积差分，WS 实时 + K线回溯两路来源自动切换 |
| **OI 分析** | 四象限（加仓/减仓/回补/减仓）+ 速率图 + 跨周期回填 |
| **爆仓中心** | 五视角：瀑布图 / 密度热图 / 跨所联动 / 级联评分 / 时间轴 |
| **合成信号** | Price × OI × CVD × Funding × Crowd 五因子加权，置信度评估，**权重可调** |
| **技术指标** | MA(5/20/60) / 布林带(20,2) / RSI(14) / **MACD(12,26,9)** / **ATR(14)** |
| **市场扫描** | 28 币种批量扫描，热点异动自动排名，**Treemap 热力图**（5 种指标） |
| **跨所套利** | 价格价差套利信号 + 资金费率套利年化估算 |
| **HL 链上中心** | Hyperliquid 专属：鲸鱼持仓 / 排行榜 / 预测资金费率 / 清算密度 |
| **推送告警** | Telegram Bot + 浏览器通知，冷却时间 + 去抖确认机制 |
| **历史归档** | SQLite WAL 模式，OI / 资金费率 / 告警历史本地持久化 |
| **事件回放** | 最长 1 小时录制，逐帧回放价格 / OI / CVD / 告警 |

---

## 快速启动

```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 启动
streamlit run app.py

# 3. 浏览器访问
# http://localhost:8501
```

启动后在左侧边栏选择币种（默认 BTC），系统会自动建立四所 WebSocket 连接并开始实时更新。

---

## 依赖安装

**Python ≥ 3.9** 推荐 3.11。

```txt
streamlit>=1.35.0
pandas>=2.0.0
plotly>=5.20.0
websocket-client>=1.8.0
requests>=2.31.0
```

完整安装：

```bash
pip install streamlit pandas plotly websocket-client requests
```

Telegram 推送（可选）：无需额外依赖，在「推送&历史数据」Tab 填入 Bot Token 和 Chat ID 即可。

---

## 界面与 Tab 导航

系统分为 **21 个主 Tab**，覆盖从行情到信号的完整分析链路：

### 第一行：核心功能

| Tab | 功能 | 核心内容 |
|-----|------|---------|
| 🏠 全市场首页 | 市场大盘扫描 | 28 币种 OI / Funding / 爆仓异动排名，热点自动识别 |
| 📈 深度终端 | 主力分析视图 | K 线 + 技术指标 + 热力盘口 + 爆仓叠加 + 跨周期联动 |
| 💧 CVD主动买卖 | 成交流分析 | CVD 时序图 + 多空力量面板 + 成交明细流 |
| 🔲 本地WS订单簿 | 实时盘口 | WebSocket 维护的 Level-2 深度图，买卖双向可视化 |
| 👥 OI四象限+速率 | 持仓方向 | 加仓/减仓/回补/减仓四象限散点图 + 速率趋势 |
| 📊 多空比矩阵 | 多空拥挤度 | Binance 大户 / 全市场账户比 + Bybit Taker 比 + 仪表盘 |
| 🔗 Spot-Perp 价差 | 现货领先信号 | 三所实时价差走势 + 乖离告警（现货先行 / 极端乖离） |
| 📐 Basis+期限结构 | 基差分析 | 合约溢价率 + 期限结构图 + 现货 vs 合约持仓对比 |

### 第二行：专项分析

| Tab | 功能 | 核心内容 |
|-----|------|---------|
| 💥 爆仓中心 | 五视角爆仓 | 瀑布图 / 热力密度 / 跨所联动时间轴 / 级联评分 |
| 🔍 冰山单+流动性缺口 | 隐蔽订单 | 同价位反复成交检测冰山单 + 挂单骤减缺口 |
| 🧠 合成信号引擎 | 综合信号 | 五因子加权合成分 + 雷达图 + 因子明细表 |
| 📋 盘口中心 | 盘口质量 | 假挂单检测 / 墙体吸收 / 近价流动性崩溃 / 大单流 |
| 🔔 告警中心 | 告警管理 | 已确认告警流 + 时间轴 + 声音 / 静音控制 |
| 📼 回放复盘 | 事件录制 | 最长 1h 录制，逐帧回放价格、OI、CVD、告警 |
| 🌐 全市场对比 | 多所对比 | 四所关键指标并排对比 |
| 📡 多币种轮巡 | 批量监控 | 自选列表多币种快速巡视 |
| ⚙️ 预警规则 | 自定义告警 | 价格 / OI / 资金费率 / 爆仓额 / CVD / 价差 / OI速率 七类条件 |

### 第三行：增强功能

| Tab | 功能 | 核心内容 |
|-----|------|---------|
| ⛓️ HL链上中心 | Hyperliquid 专属 | 鲸鱼持仓排行 / 预测资金费率 / 金库列表 / 清算密度 |
| 🧬 信号增强中心 | 深度信号分析 | 套利监控 / 情绪评分 / VPIN / K线形态 / **权重调节** / **热力扫描** |
| 📡 推送&历史数据 | 推送配置 | Telegram / 浏览器通知配置 + OI / 资金费率历史查询 |
| 🔧 调试 | 系统状态 | WS 健康状态 / 快照原始数据 / 连接诊断 |

---

## 信号增强中心

「🧬 信号增强中心」包含 **12 个子 Tab**：

| 子 Tab | 说明 |
|--------|------|
| ⚡ 套利监控 | 跨所价差套利（最小价差可调）+ 资金费率套利年化估算 |
| 🌡️ 情绪评分 | 六因子情绪仪表盘（OI / CVD / Funding / L-S / 爆仓 / VPIN） |
| 🧪 VPIN毒性 | 成交量桶订单流毒性，高于 0.7 触发方向性预警 |
| 📊 K线形态 | Pin bar / 吞没 / 锤子 / 十字星自动识别 + 快速回测 |
| 🏛️ 交易所主导权 | 四所 OI 份额实时动态，主导权转移可视化 |
| 🔬 微结构异常 | 价差突扩 / 深度塌陷检测，流动性危机早期信号 |
| 💥 聚合爆仓流 | 四所爆仓事件合并流，按交易所分组柱状图 + 过滤器 |
| 🐋 大单流量聚合 | 鲸鱼踪迹：大额成交方向性分析，时间窗口可选 |
| 🎭 墙体消失告警 | 假挂单（快速撤离）+ 吸收事件（被吃后补单） |
| 📈 多币种横向对比 | 多币同框对比 + 分组柱状图 + **热力矩阵** |
| ⚖️ 信号权重调节 | **P1 新增**：五因子权重滑块 + 三套快速预设，实时同步到合成信号引擎 |
| 🗺️ 市场热力扫描 | **P1 新增**：全市场 Treemap，支持 OI变化% / 资金费率 / 爆仓额 / 成交量 / 价格变化五种指标 |

### 合成信号权重预设

| 预设方案 | Price | OI | CVD | Funding | Crowd | 适用场景 |
|---------|-------|-----|-----|---------|-------|---------|
| 📊 均衡 | 20% | 20% | 20% | 20% | 20% | 多因子平衡 |
| 🌊 流动性优先 | 10% | 30% | 40% | 10% | 10% | 成交流主导行情 |
| 💰 费率优先 | 15% | 20% | 15% | 35% | 15% | 资金费率套利场景 |

---

## 告警与推送系统

### 内置告警类型

系统内置 **8 类**自动告警，由 WebSocket 实时触发，经去抖确认后推送：

| 告警类型 | 触发条件 |
|---------|---------|
| 🟢 现货先拉↑ | 现货价格领先合约上涨超阈值 |
| 🔴 现货先跌↓ | 现货价格领先合约下跌超阈值 |
| ⚠️ OI升/买弱 | OI 持续上升但 CVD 偏弱，隐患累积 |
| 🔵 OI降/轧空 | OI 下降同时 CVD 转正，潜在逼空 |
| 🚨 极端乖离 | 现货-合约价差超过极端阈值 |
| ⚡ 拥挤+爆仓 | 多头拥挤 + 大规模爆仓同时出现 |
| 🎭 假挂单 | 大额挂单出现后 8 秒内消失 |
| 🧠 合成信号 | 五因子综合分绝对值 > 0.6 且置信度 > 60% |

### 自定义预警规则

在「⚙️ 预警规则」Tab 中可创建任意数量的自定义规则：

- **监控指标**：最新价 / 持仓金额 / 资金费率 / 爆仓额(60min) / CVD累积 / 价差bps / OI速率/min
- **触发条件**：超过 / 低于 / 向上穿越 / 向下穿越
- **去抖机制**：Strong=2次确认 / Medium=3次 / Weak=4次，防止噪音触发

### 推送渠道

| 渠道 | 配置方式 | 说明 |
|------|---------|------|
| **Telegram** | Bot Token + Chat ID | 支持格式化消息，含告警类型 / 交易所 / 严重度 |
| **浏览器通知** | 页面授权 | 基于 Web Notification API，标签页后台运行时仍可推送 |

推送频率受冷却时间保护：Strong=90s / Medium=180s / Weak=300s。

---

## 持久化存储

本地 SQLite 数据库（`market_data.db`），**WAL 模式**，支持读写并发：

| 表名 | 内容 | 用途 |
|------|------|------|
| `oi_history` | 时间戳 / 币种 / 交易所 / OI名义值 / 资金费率 / 价格 | OI 历史回溯，支持多所多币查询 |
| `funding_history` | 时间戳 / 币种 / 交易所 / 费率 / 预测费率 | 资金费率历史，可导出 CSV |
| `daily_summary` | 日期 / 开高低收 / 成交量 / OI变化 / 平均费率 / 爆仓总额 | 每日市场摘要，UNIQUE(date, coin) |
| `alert_history` | 时间戳 / 交易所 / 告警类型 / 严重度 / 消息 / 得分 | 告警历史回溯 |
| `notification_log` | 时间戳 / 渠道 / 消息 / 是否成功 | 推送日志 |

数据自动每 5 分钟归档（由后台 sampler 线程触发），可在「推送&历史数据」Tab 查询历史并导出。

---

## 项目结构

```
.
├── app.py                # 主入口：Streamlit 页面 + Fragment 渲染 + 侧边栏
├── analytics.py          # 图表构建：K线/CVD/OI/爆仓/热图/信号/MACD/ATR
├── aggregator.py         # 跨所聚合：套利信号/情绪评分/VPIN/热力图/权重计算
├── realtime.py           # LiveTerminalService：WS管理/快照/告警引擎/录制
├── exchanges.py          # 交易所 REST 客户端：Bybit/Binance/OKX/Hyperliquid
├── models.py             # 数据模型：61 个 dataclass（行情/订单簿/信号/告警等）
├── signal_center.py      # 信号增强中心 Tab（12个子Tab）
├── homepage.py           # 全市场首页：批量扫描/异动排行/热点识别
├── liq_center.py         # 爆仓中心 Tab：五视角爆仓分析
├── ob_center.py          # 盘口中心 Tab：质量评分/假墙/吸收事件
├── alert_center.py       # 告警中心 Tab：告警流/时间轴/声音控制
├── hl_center.py          # Hyperliquid 链上中心 Tab
├── hl_client.py          # Hyperliquid 完整 API 封装
├── notifier.py           # 推送通知：Telegram/浏览器，冷却管理
├── push_settings.py      # 推送配置 Tab + 历史数据查询
├── storage.py            # SQLite 持久化层，WAL 模式
└── market_data.db        # 本地数据库（运行后自动生成）
```

---

## 数据模型

`models.py` 定义了 **61 个 dataclass**，覆盖系统所有数据对象：

**行情与订单簿**
`ExchangeSnapshot` · `Candle` · `OIPoint` · `OrderBookLevel` · `LocalOrderBook` · `TradeEvent` · `CVDPoint`

**信号与分析**
`CompositeSignal` · `OIDeltaPoint` · `SpotPerpSpreadPoint` · `SpotPerpAlert` · `IcebergAlert` · `LiquidityGap` · `FakeWallCandidate` · `WallAbsorptionEvent` · `OrderBookQualitySnapshot`

**爆仓**
`LiquidationEvent` · `LiquidationClusterV2` · `NearLiquidityCollapse` · `MultiExchangeLiqSummary`

**告警**
`AlertRule` · `AlertEvent` · `ConfirmedAlert` · `AlertTimeline`

**聚合与情绪**
`CrossExArbitrageSignal` · `CrossExFundingArb` · `AggregatedOIPoint` · `ExchangeDominancePoint` · `MarketSentimentScore` · `VPINPoint` · `MicrostructureAnomaly` · `CandlePatternSignal` · `BacktestResult`

**Hyperliquid 专属**
`HLWhalePosition` · `HLLeaderEntry` · `HLPredictedFunding` · `HLVaultInfo` · `HLLiquidationDensity`

**持久化**
`PersistentOIRecord` · `DailyMarketSummary` · `PushNotificationConfig` · `NotificationRecord`

---

## 配置参数

所有参数均可在左侧边栏实时调节，**无需重启**：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| 主图交易所 | Binance | 深度终端的主图数据来源 |
| K线周期 | 5m | 1m / 3m / 5m / 15m / 30m / 1h / 4h / 1d |
| K线数量 | 240 | 120 ~ 480 根 |
| 盘口深度 | 160 | REST 拉取档位数（WS 实时维护独立） |
| 基础刷新秒数 | 2s | 1 ~ 10s，智能刷新模式下自动调速 |
| 智能刷新 | 开启 | 波动大时加速至 1s，平静时减速至 5s |
| 持仓采样秒数 | 15s | OI / Funding REST 轮询间隔 |
| 请求超时 | 10s | REST 请求超时时间 |
| 爆仓统计分钟 | 60min | 爆仓指标的时间窗口 |

**K线技术指标**（侧边栏勾选启用）：

| 指标 | 参数 | 说明 |
|------|------|------|
| MA 均线 | 5 / 20 / 60 | 黄/蓝/橙三色均线 |
| 布林带 | 20, 2σ | 紫色虚线 + 填充带 |
| RSI | 14 | 紫色，超买/超卖区域高亮 |
| MACD | 12, 26, 9 | 绿/红柱 + DIF/DEA 线，独立子图 |
| ATR | 14 | Wilder EMA 真实波动率，橙色面积图 |

---

## 版本变更记录

### v7（当前版本）

**P0 — Bug 修复**
- 修复 `build_bull_bear_power_figure` 中 `add_hline` 对 Plotly Indicator 子图检查 `xaxis` 导致的 `PlotlyKeyError` 崩溃，改用 `add_shape`

**P1 — 功能深化**
- `analytics.py`：新增 `calc_macd` / `calc_atr` / `build_macd_atr_figure`，MACD 和 ATR 作为独立子图嵌入 K 线图下方
- `aggregator.py`：新增 `compute_composite_score`（权重可配置）、`build_market_heatmap`、`build_market_heatmap_figure`（Treemap）
- `signal_center.py`：新增「⚖️ 信号权重调节」Tab（滑块 + 三套预设）和「🗺️ 市场热力扫描」Tab
- `realtime.py`：新增 `set_composite_weights` 公共方法，合成信号权重从 UI 实时注入

**P2 — 架构优化**
- `app.py`：所有 `load_*` 缓存函数从裸 `except: pass` 改为结构化 `logging.warning`，错误不再静默
- `storage.py`：启用 SQLite WAL 日志模式 + `synchronous=NORMAL` + 8MB 页缓存，高频写入延迟显著降低
- `realtime.py`：Spot WS Worker 重连从固定 3s 改为指数退避（3→6→12…→60s），与主合约 WS 策略对齐

### v6
- 信号增强中心（VPIN / 微结构异常 / K线形态 / 回测）
- Hyperliquid 链上中心（鲸鱼持仓 / 排行榜 / 预测资金费率 / 金库）
- 推送通知系统（Telegram / 浏览器）+ SQLite 历史归档
- 事件录制与回放（最长 1 小时）
- 爆仓级联评分 v2 + 跨所联动检测

### v5
- 现货 WS 接入（Bybit / OKX），Spot-Perp 价差实时计算
- 盘口中心（假挂单 / 墙体吸收 / 近价流动性崩溃 / 大单流）
- 告警中心（确认告警流 / 时间轴 / 声音控制）
- 复合信号引擎（雷达图 + 五因子明细）

---

## 注意事项

1. **网络要求**：需要能访问 Binance / Bybit / OKX / Hyperliquid 的 API 域名，部分地区需代理
2. **首次启动**：WebSocket 建立约需 5~10 秒，期间部分指标显示「等待数据」属正常
3. **多用户场景**：`st.cache_data` 为进程级缓存，多人同时使用同一实例时数据共享，如需隔离请分别部署
4. **数据库清理**：`market_data.db` 长期运行会持续增长，可在「推送&历史数据」Tab 手动触发清理，或设置操作系统定时任务删除旧数据

---

*本项目为个人量化工具，数据仅供参考，不构成任何投资建议。*

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
