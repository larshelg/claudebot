-- ===================================================
-- SIMPLE NORMALIZED INDICATOR SYSTEM - SMA(20) ONLY
-- ===================================================

-- ====== 0) SET MODE ======
SET 'execution.runtime-mode' = 'streaming';
SET 'table.exec.source.idle-timeout' = '5 s';

USE CATALOG fluss_catalog;

-- ===================================================
-- 1) LATEST STATE TABLE (for fast lookups)
-- ===================================================
CREATE TABLE IF NOT EXISTS fluss.indicators_latest (
  symbol STRING,
  indicator_name STRING,
  indicator_value DECIMAL(18,8),
  `time` TIMESTAMP(3),
  PRIMARY KEY (symbol, indicator_name) NOT ENFORCED
);

-- ===================================================
-- 2) NORMALIZED TIMESERIES TABLE (for historical data)
-- ===================================================
CREATE TABLE IF NOT EXISTS fluss.indicators_timeseries (
  run_id STRING,
  symbol STRING,
  `time` TIMESTAMP(3),
  indicator_name STRING,
  indicator_value DECIMAL(18,8),
  PRIMARY KEY (run_id, symbol, `time`, indicator_name) NOT ENFORCED,
  WATERMARK FOR `time` AS `time` - INTERVAL '5' SECOND
);

-- ===================================================
-- 2) BASE SOURCE VIEW
-- ===================================================
USE CATALOG default_catalog;

CREATE VIEW IF NOT EXISTS candles_base AS
SELECT 
  `time`,
  symbol,
  `close`
FROM `fluss_catalog`.`fluss`.`market_data_history`
WHERE `close` > 0;

-- ===================================================
-- 3) SMA(20) CALCULATION
-- ===================================================
CREATE VIEW IF NOT EXISTS sma_calculation AS
SELECT
  symbol,
  `time`,
  'SMA_20' AS indicator_name,
  CAST(AVG(`close`) OVER (
    PARTITION BY symbol
    ORDER BY `time`
    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
  ) AS DECIMAL(18,8)) AS indicator_value
FROM candles_base;

-- ===================================================
-- 4) UPSERT INTO LATEST STATE TABLE
-- ===================================================
INSERT INTO `fluss_catalog`.`fluss`.`indicators_latest`
SELECT
  symbol,
  indicator_name,
  indicator_value,
  `time`
FROM sma_calculation;

-- ===================================================
-- 5) APPEND TO TIMESERIES TABLE
-- ===================================================
INSERT INTO `fluss_catalog`.`fluss`.`indicators_timeseries`
SELECT
  'live' AS run_id,
  symbol,
  `time`,
  indicator_name,
  indicator_value
FROM sma_calculation;

-- ===================================================
-- 6) QUERY HELPERS
-- ===================================================

-- Latest SMA values per symbol (fast lookup)
CREATE VIEW IF NOT EXISTS latest_sma AS
SELECT
  symbol,
  indicator_value AS sma_20,
  `time`
FROM `fluss_catalog`.`fluss`.`indicators_latest`
WHERE indicator_name = 'SMA_20';

-- Historical SMA trend analysis
CREATE VIEW IF NOT EXISTS sma_trends AS
SELECT
  symbol,
  `time`,
  indicator_value AS current_sma,
  LAG(indicator_value) OVER (PARTITION BY symbol ORDER BY `time`) AS previous_sma,
  CASE 
    WHEN indicator_value > LAG(indicator_value) OVER (PARTITION BY symbol ORDER BY `time`) 
    THEN 'RISING'
    WHEN indicator_value < LAG(indicator_value) OVER (PARTITION BY symbol ORDER BY `time`) 
    THEN 'FALLING'
    ELSE 'FLAT'
  END AS trend_direction
FROM `fluss_catalog`.`fluss`.`indicators_timeseries`
WHERE run_id = 'live' 
  AND indicator_name = 'SMA_20';

-- All latest indicators for a symbol (ready for multiple indicators)
CREATE VIEW IF NOT EXISTS symbol_latest_indicators AS
SELECT
  symbol,
  indicator_name,
  indicator_value,
  `time`
FROM `fluss_catalog`.`fluss`.`indicators_latest`
ORDER BY symbol, indicator_name;

-- ===================================================
-- 7) SMA CROSSOVER STRATEGY
-- ===================================================

-- Simple SMA crossover strategy view
CREATE VIEW IF NOT EXISTS sma_crossover_signals AS
SELECT
  s20.symbol,
  s20.`time`,
  s20.indicator_value AS sma_20,
  s40.indicator_value AS sma_40,
  CASE 
    WHEN s20.indicator_value > s40.indicator_value THEN 'BUY_ZONE'
    WHEN s20.indicator_value < s40.indicator_value THEN 'SELL_ZONE'
    ELSE 'NEUTRAL'
  END AS current_signal,
  CASE 
    WHEN s20.indicator_value > s40.indicator_value 
         AND LAG(s20.indicator_value) OVER (PARTITION BY s20.symbol ORDER BY s20.`time`) <= LAG(s40.indicator_value) OVER (PARTITION BY s20.symbol ORDER BY s20.`time`)
    THEN 'BUY'
    WHEN s20.indicator_value < s40.indicator_value 
         AND LAG(s20.indicator_value) OVER (PARTITION BY s20.symbol ORDER BY s20.`time`) >= LAG(s40.indicator_value) OVER (PARTITION BY s20.symbol ORDER BY s20.`time`)
    THEN 'SELL' 
    ELSE 'HOLD'
  END AS crossover_signal
FROM `fluss_catalog`.`fluss`.`indicators_timeseries` s20
JOIN `fluss_catalog`.`fluss`.`indicators_timeseries` s40
  ON s20.symbol = s40.symbol 
  AND s20.`time` = s40.`time`
  AND s20.run_id = s40.run_id
WHERE s20.run_id = 'live'
  AND s20.indicator_name = 'SMA_20'
  AND s40.indicator_name = 'SMA_40';



CREATE VIEW indicators_pivot AS
SELECT 
  run_id,
  symbol,
  `time`,
  MAX(CASE WHEN indicator_name = 'SMA_20' THEN indicator_value END) AS sma_20,
  MAX(CASE WHEN indicator_name = 'SMA_40' THEN indicator_value END) AS sma_40,
  MAX(CASE WHEN indicator_name = 'SMA_5' THEN indicator_value END) AS sma_5
  -- Add more indicators as needed
FROM `fluss_catalog`.`fluss`.`indicators_timeseries`
GROUP BY run_id, symbol, `time`;


CREATE VIEW buy_signals AS
SELECT 
  run_id,
  symbol,
  time,
  'BUY_SIGNAL' AS signal_type,
  1 AS signal_value
FROM indicators_pivot
WHERE sma_20 > sma_40 
  AND sma_20 IS NOT NULL 
  AND sma_40 IS NOT NULL;



SELECT run_id, symbol, `time`, indicator_name, indicator_value
FROM `fluss_catalog`.`fluss`.`indicators_timeseries`
ORDER BY `time` DESC, indicator_name
LIMIT 20;