-- Create a simple strategy signals table that matches the working indicators pattern

USE CATALOG fluss_catalog;

-- Drop the existing table if it exists (to recreate with correct schema)
DROP TABLE IF EXISTS fluss.strategy_signals;

-- Create strategy signals table with DOUBLE types (like indicators table)
CREATE TABLE fluss.strategy_signals (
  run_id STRING,
  symbol STRING,
  `time` TIMESTAMP(3),
  `close` DOUBLE,
  sma5 DOUBLE,
  sma21 DOUBLE,
  signal STRING,
  signal_strength DOUBLE
) WITH (
  'bucket' = '10'
);

-- Verify table was created
SHOW TABLES;
DESCRIBE fluss.strategy_signals;
