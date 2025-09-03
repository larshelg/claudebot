-- ===================================================
-- Trading Engine Storage (Fluss) - Latest + History
-- ===================================================

SET 'execution.runtime-mode' = 'streaming';
USE CATALOG fluss_catalog;

-- =====================
-- APPEND-ONLY HISTORY
-- =====================

CREATE TABLE fluss.trendsummary10 (
  symbol STRING,
  ts TIMESTAMP(3),
  alpha DOUBLE,
  confidence DOUBLE,
  phase STRING,
  direction STRING,
  breakoutDirection STRING,
  lastBreakoutTs TIMESTAMP(3),
  breakoutRecency DOUBLE,
  breakoutStrength DOUBLE,
  linesUsed INT
) WITH (
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '30s'
);



-- Raw execution reports (fills)
CREATE TABLE IF NOT EXISTS fluss.exec_report_history (
  accountId STRING,
  orderId STRING,
  symbol STRING,
  fillQty DOUBLE,
  fillPrice DOUBLE,
  status STRING,
  ts TIMESTAMP(3)
) WITH (
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '30s'
);

-- Accepted trade signals (append-only)
CREATE TABLE IF NOT EXISTS fluss.trade_signal_history (
  accountId STRING,
  symbol STRING,
  qty DOUBLE,
  price DOUBLE,
  ts TIMESTAMP(3)
) WITH (
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '30s'
);

-- FIFO matches with realized P&L
CREATE TABLE IF NOT EXISTS fluss.trade_match_history (
  matchId STRING,
  accountId STRING,
  symbol STRING,
  buyOrderId STRING,
  sellOrderId STRING,
  matchedQty DOUBLE,
  buyPrice DOUBLE,
  sellPrice DOUBLE,
  realizedPnl DOUBLE,
  matchTimestamp TIMESTAMP(3),
  buyTimestamp TIMESTAMP(3),
  sellTimestamp TIMESTAMP(3)
) WITH (
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '30s'
);

-- Position close events (emitted once on flatten)
CREATE TABLE IF NOT EXISTS fluss.position_close_history (
  accountId STRING,
  symbol STRING,
  totalQty DOUBLE,
  avgPrice DOUBLE,
  openTs TIMESTAMP(3),    -- optional, when available
  closeTs TIMESTAMP(3),
  realizedPnl DOUBLE      -- optional aggregate across the position lifetime
) WITH (
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '30s'
);

-- Risk alerts history
CREATE TABLE IF NOT EXISTS fluss.risk_alerts (
  accountId STRING,
  message STRING
) WITH (
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '30s'
);

-- =====================
-- LATEST/UPSERT VIEWS
-- =====================

-- Open positions latest (non-zero only)
CREATE TABLE IF NOT EXISTS fluss.open_positions_latest (
  accountId STRING NOT NULL,
  symbol STRING NOT NULL,
  netQty DOUBLE,
  avgPrice DOUBLE,
  lastUpdated TIMESTAMP(3),
  PRIMARY KEY (accountId, symbol) NOT ENFORCED
) WITH (
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '30s'
);

-- Realized P&L latest (cumulative)
CREATE TABLE IF NOT EXISTS fluss.realized_pnl_latest (
  accountId STRING NOT NULL,
  symbol STRING NOT NULL,
  realizedPnl DOUBLE,
  ts TIMESTAMP(3),
  PRIMARY KEY (accountId, symbol) NOT ENFORCED
) WITH (
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '30s'
);

-- Unrealized P&L latest
CREATE TABLE IF NOT EXISTS fluss.unrealized_pnl_latest (
  accountId STRING NOT NULL,
  symbol STRING NOT NULL,
  unrealizedPnl DOUBLE,
  currentPrice DOUBLE,
  avgPrice DOUBLE,
  netQty DOUBLE,
  ts TIMESTAMP(3),
  PRIMARY KEY (accountId, symbol) NOT ENFORCED
) WITH (
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '30s'
);

-- Portfolio latest
CREATE TABLE IF NOT EXISTS fluss.portfolio_latest (
  accountId STRING NOT NULL,
  equity DOUBLE,
  cashBalance DOUBLE,
  exposure DOUBLE,
  marginUsed DOUBLE,
  PRIMARY KEY (accountId) NOT ENFORCED
) WITH (
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '30s'
);







