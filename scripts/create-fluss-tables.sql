-- Create Fluss tables for the crossover strategy demo

USE CATALOG fluss_catalog;

-- Create market data history table
CREATE TABLE IF NOT EXISTS fluss.market_data_history (
  `time` TIMESTAMP(3),
  symbol STRING,
  `open` DECIMAL(10,4),
  high DECIMAL(10,4),
  low DECIMAL(10,4),
  `close` DECIMAL(10,4),
  volume BIGINT
) WITH (
  'bucket' = '10',
  'fluss.remote.log.storage.enable' = 'true'
);

-- Create indicators table  
CREATE TABLE IF NOT EXISTS fluss.indicators (
  symbol STRING,
  `time` TIMESTAMP(3),
  sma5 DOUBLE,
  sma14 DOUBLE,
  sma21 DOUBLE,
  ema5 DOUBLE,
  ema14 DOUBLE,
  ema21 DOUBLE,
  rsi14 DOUBLE
) WITH (
  'bucket' = '10',
  'fluss.remote.log.storage.enable' = 'true'
);

-- Create strategy signals table
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
  'bucket' = '10',
  'fluss.remote.log.storage.enable' = 'true'
);

-- Insert some sample market data to test
INSERT INTO fluss.market_data_history VALUES
  (TIMESTAMP '2024-01-01 10:00:00', 'BTC/USDT', 42000.0000, 42100.0000, 41900.0000, 42050.0000, 1000),
  (TIMESTAMP '2024-01-01 10:01:00', 'BTC/USDT', 42050.0000, 42200.0000, 42000.0000, 42150.0000, 1100),
  (TIMESTAMP '2024-01-01 10:02:00', 'BTC/USDT', 42150.0000, 42300.0000, 42100.0000, 42250.0000, 1200),
  (TIMESTAMP '2024-01-01 10:03:00', 'BTC/USDT', 42250.0000, 42400.0000, 42200.0000, 42350.0000, 1300),
  (TIMESTAMP '2024-01-01 10:04:00', 'BTC/USDT', 42350.0000, 42500.0000, 42300.0000, 42450.0000, 1400),
  (TIMESTAMP '2024-01-01 10:05:00', 'BTC/USDT', 42450.0000, 42600.0000, 42400.0000, 42550.0000, 1500);

-- Show tables to verify creation
SHOW TABLES;
