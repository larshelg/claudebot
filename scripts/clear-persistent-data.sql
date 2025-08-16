-- ===================================================
-- CLEAR ALL PERSISTENT DATA FROM FLUSS STORAGE
-- ===================================================

-- This script clears all data from Fluss persistent tables
-- while keeping the table structures intact

-- Switch to Fluss catalog
USE CATALOG fluss_catalog;

-- Clear historical market data
DELETE FROM market_data_history;

-- Clear technical indicators
DELETE FROM technical_indicators;

-- Clear trading signals history
DELETE FROM strategy_signals;

-- Clear portfolio positions
DELETE FROM portfolio_positions;

-- Clear account capital tracking
DELETE FROM account_capital;

-- Verify all tables are empty
SELECT 'market_data_history' as table_name, COUNT(*) as row_count FROM market_data_history
UNION ALL
SELECT 'technical_indicators' as table_name, COUNT(*) as row_count FROM technical_indicators
UNION ALL
SELECT 'strategy_signals' as table_name, COUNT(*) as row_count FROM strategy_signals
UNION ALL
SELECT 'portfolio_positions' as table_name, COUNT(*) as row_count FROM portfolio_positions
UNION ALL
SELECT 'account_capital' as table_name, COUNT(*) as row_count FROM account_capital;

SELECT 'All persistent data cleared successfully!' as status;