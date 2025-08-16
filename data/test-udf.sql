-- Test our custom UDF registration
CREATE FUNCTION reflection_indicators_map AS 'com.flinkudf.indicators.ReflectionIndicatorsMapUdf';

-- Show all functions to verify it's registered  
SHOW FUNCTIONS;

-- Test a simple call to make sure the UDF works
-- Note: This creates a dummy row to test the function
SELECT reflection_indicators_map(ROW('test', 1.23)) as result;
