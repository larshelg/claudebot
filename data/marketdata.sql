-- ===================================================
-- MARKET DATA INGEST + STREAM VIEW (FLINK/FLUSS)
-- ===================================================

-- 1) Batch import candles from JSON file into Fluss
USE CATALOG default_catalog;

-- Temporary file import table for batch loading
CREATE TABLE IF NOT EXISTS temp_array_source (
  arrays ARRAY<ARRAY<DOUBLE>>
) WITH (
  'connector' = 'filesystem',
  'path' = 'file:///opt/flink/data/SUI_USDT-1m-flink.json',
  'format' = 'json'
);

-- Temporary processing view for batch import
CREATE VIEW IF NOT EXISTS temp_candles_batch AS
SELECT 
  CAST(FROM_UNIXTIME(CAST(arr[1] AS BIGINT) / 1000) AS TIMESTAMP(3)) AS `time`,
  'SUI_USDT' AS symbol,
  CAST(arr[2] AS DECIMAL(10,4)) AS `open`,
  CAST(arr[3] AS DECIMAL(10,4)) AS high,
  CAST(arr[4] AS DECIMAL(10,4)) AS low,
  CAST(arr[5] AS DECIMAL(10,4)) AS `close`,
  CAST(arr[6] AS BIGINT) AS volume
FROM temp_array_source
CROSS JOIN UNNEST(arrays) AS t(arr);

-- Fluss persistent storage for market data history
USE CATALOG fluss_catalog;
CREATE TABLE IF NOT EXISTS market_data_history (
  `time` TIMESTAMP(3),
  symbol STRING,
  `open` DECIMAL(10,4),
  high DECIMAL(10,4), 
  low DECIMAL(10,4),
  `close` DECIMAL(10,4),
  volume BIGINT,
  WATERMARK FOR `time` AS `time` - INTERVAL '5' SECOND
  ) WITH (
     'table.datalake.enabled' = 'true',
     'table.datalake.freshness' = '30s'
);

-- Create indicators table as a primary key table for latest indicator values
CREATE TABLE IF NOT EXISTS indicators (
    symbol STRING NOT NULL,
    `time` TIMESTAMP(3) NOT NULL,
    sma5 DOUBLE,
    sma14 DOUBLE,
    sma21 DOUBLE,
    ema5 DOUBLE,
    ema14 DOUBLE,
    ema21 DOUBLE,
    rsi14 DOUBLE,
    WATERMARK FOR `time` AS `time` - INTERVAL '5' SECOND,
    PRIMARY KEY (symbol, `time`) NOT ENFORCED
) WITH (
     'table.datalake.enabled' = 'true',
     'table.datalake.freshness' = '30s'
);

CREATE TABLE IF NOT EXISTS fluss.strategy_signals (
  run_id STRING,
  symbol STRING,
  `time` TIMESTAMP(3),
  `close` DECIMAL(10,4),
  sma5 DECIMAL(18,8),
  sma21 DECIMAL(18,8),
  signal STRING,
  signal_strength DECIMAL(5,2)
) WITH (
     'table.datalake.enabled' = 'true',
     'table.datalake.freshness' = '30s'
);

-- Batch import historical data from file
INSERT INTO market_data_history
SELECT * FROM default_catalog.default_database.temp_candles_batch;

-- 2) Streaming view over Fluss candles for event-time processing
USE CATALOG default_catalog;
SET 'execution.runtime-mode' = 'streaming';

