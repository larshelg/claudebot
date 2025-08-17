package com.example.flink;

import com.example.flink.serializer.CandleFlexible;
import com.example.flink.serializer.IndicatorRowFlexible;
import com.example.flink.strategy.TradingStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class ValueStateCrossoverProcessFunctionTest {

    // Mock trading strategy for testing
    public static class MockCrossoverStrategy implements TradingStrategy {
        @Override
        public StrategySignal process(IndicatorRowFlexible indicator, CandleFlexible candle) {
            double sma5 = indicator.indicators.get("sma5");
            double sma21 = indicator.indicators.get("sma21");
            double close = candle.getClose();
            
            // Simple crossover logic: generate BUY signal when sma5 > sma21
            if (sma5 > sma21) {
                return new StrategySignal(
                    "test-run",
                    indicator.symbol,
                    indicator.getTimestamp(),
                    close,
                    sma5,
                    sma21,
                    "BUY",
                    1.0
                );
            }
            return null; // No signal
        }

        @Override
        public boolean isCEP() {
            return false;
        }

        @Override
        public String getName() {
            return "MockCrossoverStrategy";
        }
    }

    @Test
    public void testValueStateCrossoverProcessFunction() throws Exception {
        // 1. Create Flink environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. Create test data - sequence that shows crossover
        List<IndicatorWithPrice> testData = new ArrayList<>();
        testData.add(new IndicatorWithPrice("BTCUSD", 1000L, 50000.0, 19.5, 21.0, 30.0)); // sma5 < sma21
        testData.add(new IndicatorWithPrice("BTCUSD", 1001L, 50200.0, 20.0, 20.8, 32.0)); // still below
        testData.add(new IndicatorWithPrice("BTCUSD", 1002L, 50500.0, 21.5, 20.5, 35.0)); // crossover! sma5 > sma21

        // 3. Create source stream
        DataStream<IndicatorWithPrice> source = env.fromCollection(testData);

        // 4. Apply ValueState process function with mock strategy
        MockCrossoverStrategy strategy = new MockCrossoverStrategy();
        ValueStateCrossoverProcessFunction processFunction = new ValueStateCrossoverProcessFunction(strategy);
        
        DataStream<StrategySignal> signals = source
                .keyBy(indicator -> indicator.symbol)
                .process(processFunction);

        // 5. Collect results
        List<StrategySignal> result = signals.executeAndCollect(10);

        // 6. Assertions
        System.out.println("ValueState test results:");
        System.out.println("Number of signals generated: " + result.size());
        
        for (StrategySignal signal : result) {
            System.out.println("Signal: " + signal);
        }

        // We expect at least one signal (from the crossover event)
        assertFalse(result.isEmpty(), "Expected at least one signal from crossover");
        
        // Verify the signal properties
        StrategySignal signal = result.get(result.size() - 1); // Get the last signal
        assertEquals("BTCUSD", signal.symbol);
        assertEquals("BUY", signal.signal);
        assertTrue(signal.sma5 > signal.sma21, "Signal should be generated when sma5 > sma21");
        
        System.out.println("ValueState crossover test passed!");
    }

    @Test
    public void testValueStateWithNoSignals() throws Exception {
        // Test case where no crossover occurs (sma5 always below sma21)
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<IndicatorWithPrice> testData = new ArrayList<>();
        testData.add(new IndicatorWithPrice("ETHUSD", 2000L, 3000.0, 18.0, 21.0, 25.0)); // sma5 < sma21
        testData.add(new IndicatorWithPrice("ETHUSD", 2001L, 3100.0, 19.0, 21.2, 27.0)); // still below
        testData.add(new IndicatorWithPrice("ETHUSD", 2002L, 3050.0, 19.5, 21.1, 26.0)); // still below

        DataStream<IndicatorWithPrice> source = env.fromCollection(testData);
        
        MockCrossoverStrategy strategy = new MockCrossoverStrategy();
        ValueStateCrossoverProcessFunction processFunction = new ValueStateCrossoverProcessFunction(strategy);
        
        DataStream<StrategySignal> signals = source
                .keyBy(indicator -> indicator.symbol)
                .process(processFunction);

        List<StrategySignal> result = signals.executeAndCollect(5);

        System.out.println("No-signal test results:");
        System.out.println("Number of signals generated: " + result.size());

        // Should generate no signals since sma5 never crosses above sma21
        assertTrue(result.isEmpty(), "Expected no signals when no crossover occurs");
        
        System.out.println("No-signal test passed!");
    }

    @Test
    public void testValueStateWithMultipleSymbols() throws Exception {
        // Test that ValueState correctly handles multiple symbols independently
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<IndicatorWithPrice> testData = new ArrayList<>();
        // BTC data - will trigger signal
        testData.add(new IndicatorWithPrice("BTCUSD", 3000L, 50000.0, 19.0, 21.0, 30.0));
        testData.add(new IndicatorWithPrice("BTCUSD", 3001L, 51000.0, 22.0, 20.5, 35.0)); // crossover
        
        // ETH data - no crossover
        testData.add(new IndicatorWithPrice("ETHUSD", 3000L, 3000.0, 18.0, 20.0, 25.0));
        testData.add(new IndicatorWithPrice("ETHUSD", 3001L, 3100.0, 19.0, 20.5, 27.0)); // no crossover

        DataStream<IndicatorWithPrice> source = env.fromCollection(testData);
        
        MockCrossoverStrategy strategy = new MockCrossoverStrategy();
        ValueStateCrossoverProcessFunction processFunction = new ValueStateCrossoverProcessFunction(strategy);
        
        DataStream<StrategySignal> signals = source
                .keyBy(indicator -> indicator.symbol)
                .process(processFunction);

        List<StrategySignal> result = signals.executeAndCollect(10);

        System.out.println("Multi-symbol test results:");
        System.out.println("Number of signals generated: " + result.size());
        
        for (StrategySignal signal : result) {
            System.out.println("Signal: " + signal);
        }

        // Should have signals only from BTC (crossover), not from ETH
        assertFalse(result.isEmpty(), "Expected at least one signal from BTC crossover");
        
        // Verify all signals are from BTC
        for (StrategySignal signal : result) {
            assertEquals("BTCUSD", signal.symbol, "All signals should be from BTCUSD");
            assertEquals("BUY", signal.signal);
        }
        
        System.out.println("Multi-symbol test passed!");
    }
}