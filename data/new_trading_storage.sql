--  Awesome—here’s a clean, production-ready Fluss SQL pack that matches the flow we drew (orders → fills → trade match → lots → rollup → P&L → portfolio → risk).


-- =========================================
--  FLUSS STORAGE: Trading Engine (Flink SQL)
-- =========================================

SET 'execution.runtime-mode' = 'streaming';
USE CATALOG fluss_catalog;

-- -------------------------------------------------
-- Conventions
-- - History tables: append-only lakehouse
-- - Latest tables: upsert w/ PK (enable true deletes)
-- - All timestamps carry a watermark for streaming
-- -------------------------------------------------

-- =========================
-- 1) ORDERS & EXEC REPORTS
-- =========================

-- Orders accepted by the system (append-only)
CREATE TABLE IF NOT EXISTS fluss.orders_history (
  accountId STRING,
  strategyId STRING,
  orderId STRING,
  symbol STRING,
  side STRING,              -- BUY/SELL
  orderType STRING,         -- LIMIT/MARKET/...
  qty DOUBLE,
  price DOUBLE,             -- null if market
  status STRING,            -- NEW, CANCELED, REPLACED, etc.
  ts TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '30s'
);

-- Exchange/order gateway events (append-only)
CREATE TABLE IF NOT EXISTS fluss.order_events_history (
  accountId STRING,
  strategyId STRING,
  orderId STRING,
  eventType STRING,         -- ACK, REJECT, PARTIAL_FILL, FILL, CANCEL, ...
  eventData STRING,         -- JSON blob if needed
  ts TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '30s'
);

-- Raw execution reports / fills (append-only)
CREATE TABLE IF NOT EXISTS fluss.exec_report_history (
  accountId STRING,
  strategyId STRING,
  orderId STRING,
  fillId STRING,
  symbol STRING,
  side STRING,
  fillQty DOUBLE,
  fillPrice DOUBLE,
  liquidity STRING,         -- MAKER/TAKER (optional)
  fee DOUBLE,               -- optional
  ts TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '30s'
);

-- Accepted trade signals (append-only)
CREATE TABLE IF NOT EXISTS fluss.trade_signal_history (
  accountId STRING,
  strategyId STRING,
  symbol STRING,
  intendedQty DOUBLE,
  intendedPrice DOUBLE,
  ts TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '30s'
);

-- =======================================
-- 2) MATCHING & P&L (append + upsert mix)
-- =======================================

-- FIFO/LIFO matches (append-only, audit)
CREATE TABLE IF NOT EXISTS fluss.trade_match_history (
  matchId STRING,
  accountId STRING,
  strategyId STRING,
  symbol STRING,
  buyOrderId STRING,
  sellOrderId STRING,
  matchedQty DOUBLE,
  buyPrice DOUBLE,
  sellPrice DOUBLE,
  realizedPnl DOUBLE,
  buyTs TIMESTAMP(3),
  sellTs TIMESTAMP(3),
  matchTs TIMESTAMP(3),
  WATERMARK FOR matchTs AS matchTs - INTERVAL '5' SECOND
) WITH (
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '30s'
);

-- Optional: position-close “summary” row (append-only)
CREATE TABLE IF NOT EXISTS fluss.position_close_history (
  accountId STRING,
  strategyId STRING,
  symbol STRING,
  totalQty DOUBLE,
  avgPrice DOUBLE,
  openTs TIMESTAMP(3),
  closeTs TIMESTAMP(3),
  realizedPnl DOUBLE,
  WATERMARK FOR closeTs AS closeTs - INTERVAL '5' SECOND
) WITH (
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '30s'
);

-- Realized P&L latest (cumulative per acct/symbol/strategy)
CREATE TABLE IF NOT EXISTS fluss.realized_pnl_latest (
  accountId STRING NOT NULL,
  strategyId STRING NOT NULL,
  symbol STRING NOT NULL,
  realizedPnl DOUBLE,
  ts TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND,
  PRIMARY KEY (accountId, strategyId, symbol) NOT ENFORCED
) WITH (
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '30s'
);

-- Unrealized P&L latest (per acct/symbol/strategy)
CREATE TABLE IF NOT EXISTS fluss.unrealized_pnl_latest (
  accountId STRING NOT NULL,
  strategyId STRING NOT NULL,
  symbol STRING NOT NULL,
  unrealizedPnl DOUBLE,
  currentPrice DOUBLE,
  avgPrice DOUBLE,
  netQty DOUBLE,
  ts TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND,
  PRIMARY KEY (accountId, strategyId, symbol) NOT ENFORCED
) WITH (
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '30s'
);

-- ==============================
-- 3) POSITIONS: LOTS & ROLLUP
-- ==============================

-- Source of truth: one row per open lot (upsert/delete)
-- Emit -U/-D on reduce/close so downstream sees true deletes.
CREATE TABLE IF NOT EXISTS fluss.open_positions_lots (
  accountId  STRING NOT NULL,
  strategyId STRING NOT NULL,
  symbol     STRING NOT NULL,
  lotId      STRING NOT NULL,     -- deterministic: e.g., first fill’s (orderId, fillId)
  side       STRING,              -- LONG/SHORT
  qtyOpen    DOUBLE,              -- original
  qtyRem     DOUBLE,              -- remaining (0 => lot is closed => emit delete)
  avgPrice   DOUBLE,
  tsOpen     TIMESTAMP(3),
  tsUpdated  TIMESTAMP(3),
  WATERMARK FOR tsUpdated AS tsUpdated - INTERVAL '5' SECOND,
  PRIMARY KEY (accountId, strategyId, symbol, lotId) NOT ENFORCED
) WITH (
  'table.datalake.enabled' = 'true',   -- keeps full change history in lakehouse
  'table.datalake.freshness' = '30s'
);

-- Fast lookup: aggregated exposure per acct/strategy/symbol (upsert/delete)
CREATE TABLE IF NOT EXISTS fluss.open_positions_rollup (
  accountId  STRING NOT NULL,
  strategyId STRING NOT NULL,
  symbol     STRING NOT NULL,
  side       STRING,               -- effective side based on netQty
  netQty     DOUBLE,
  avgPrice   DOUBLE,               -- weighted avg of remaining lots
  lastUpdated TIMESTAMP(3),
  WATERMARK FOR lastUpdated AS lastUpdated - INTERVAL '5' SECOND,
  PRIMARY KEY (accountId, strategyId, symbol) NOT ENFORCED
) WITH (
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '30s'
);

-- ===================
-- 4) PORTFOLIO / RISK
-- ===================

-- Portfolio snapshot per account (latest)
CREATE TABLE IF NOT EXISTS fluss.portfolio_latest (
  accountId STRING NOT NULL,
  equity DOUBLE,          -- cash + unrealized + realized
  cashBalance DOUBLE,
  exposure DOUBLE,        -- sum(|qty * price|)
  marginUsed DOUBLE,
  ts TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND,
  PRIMARY KEY (accountId) NOT ENFORCED
) WITH (
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '30s'
);

-- Account/strategy risk limits (latest)
CREATE TABLE IF NOT EXISTS fluss.risk_limits_latest (
  accountId STRING NOT NULL,
  strategyId STRING NOT NULL,
  maxOpenPositions INT,
  maxSymbolExposure DOUBLE,   -- e.g. notional cap per symbol
  maxDailyLoss DOUBLE,        -- kill-switch threshold
  enabled BOOLEAN,
  ts TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND,
  PRIMARY KEY (accountId, strategyId) NOT ENFORCED
) WITH (
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '30s'
);

-- Risk state (derived, latest)
CREATE TABLE IF NOT EXISTS fluss.risk_state_latest (
  accountId STRING NOT NULL,
  strategyId STRING NOT NULL,
  isKillSwitchActive BOOLEAN,
  reason STRING,
  ts TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND,
  PRIMARY KEY (accountId, strategyId) NOT ENFORCED
) WITH (
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '30s'
);

-- Risk alerts (append-only audit)
CREATE TABLE IF NOT EXISTS fluss.risk_alerts_history (
  accountId STRING,
  strategyId STRING,
  level STRING,        -- INFO/WARN/ERROR
  message STRING,
  ts TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '30s'
);


Production hardening (quick wins)

Idempotency: keep a small ValueState<Set<String>> (or BloomFilter) of processed (orderId, fillId) with TTL to dedupe late/replayed fills.

State TTL: add TTL for lotsState and dedupe state on flat keys to shrink snapshots.

Serialization: register POJOs (lots/rollup/change types) to avoid Kryo fallback; consider TypeSerializerSnapshot if you’ll evolve schemas.

Exactly-once: enable checkpoints + Fluss transactional sink (or Table/RowKind bridge) so deletes are not lost on failover.

Watermarks: your engine uses 5s; align upstream executor watermarking and decide max tolerated lateness (e.g., crypto websockets hiccups).

Parallelism & keys: keep keyBy(accountId,strategyId,symbol) stable; consider rescale tests (savepoint → restore) to verify deterministic state.

Metrics: counters for fillsProcessed, lotsOpened/Reduced/Closed, gauges for openLotsPerKey, and timers for matchLatency.

Guardrails: reject NaN/inf qty/price; treat ~0 with an epsilon; assert side ∈ {BUY,SELL}.

If you want, I can add dedupe by (orderId,fillId) with TTL next, or register TypeInformation explicitly to avoid Kryo fallback.
