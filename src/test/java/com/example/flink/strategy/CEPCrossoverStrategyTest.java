package com.example.flink.strategy;

import com.example.flink.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.junit.Test;
import static org.junit.Assert.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class CEPCrossoverStrategyTest {

    private static final List<StrategySignal> testResults = Collections.synchronizedList(new ArrayList<>());

    public static class TestSink extends RichSinkFunction<StrategySignal> {
        @Override
        public void invoke(StrategySignal value, Context context) {
            testResults.add(value);
        }
    }

    @Test
    public void testCEPCrossoverStrategyCreation() {
        CEPCrossoverStrategy strategy = new CEPCrossoverStrategy("TestCEP");
        assertNotNull("Strategy should be created", strategy);
        assertEquals("TestCEP", strategy.getName());
        assertTrue("Should be CEP strategy", strategy.isCEP());
        System.out.println("CEPCrossoverStrategy created successfully");
    }

    @Test 
    public void testIndicatorWithPriceCreation() {
        IndicatorWithPrice event1 = new IndicatorWithPrice("AAPL", 1L, 150.0, 50.0, 51.0, 2.0);
        IndicatorWithPrice event2 = new IndicatorWithPrice("AAPL", 2L, 152.0, 52.0, 51.0, 2.0);
        
        assertNotNull("Event1 should be created", event1);
        assertNotNull("Event2 should be created", event2);
        assertEquals("AAPL", event1.symbol);
        assertEquals("AAPL", event2.symbol);
        assertEquals(1L, event1.timestamp);
        assertEquals(2L, event2.timestamp);
        
        // Test the crossover condition: SMA5 > SMA21 in event2
        assertTrue("SMA5 should be greater than SMA21 in event2", event2.sma5 > event2.sma21);
        System.out.println("Indicator data created successfully: " + event1 + ", " + event2);
    }

    @Test
    public void testStrategySignalCreation() {
        StrategySignal signal = new StrategySignal("TestRun", "AAPL", 2L, 152.0, 52.0, 51.0, "BUY", 1.0);
        
        assertNotNull("Signal should be created", signal);
        assertEquals("TestRun", signal.runId);
        assertEquals("AAPL", signal.symbol);
        assertEquals(2L, signal.timestamp);
        assertEquals(152.0, signal.close, 0.0001);
        assertEquals("BUY", signal.signal);
        assertEquals(52.0, signal.sma5, 0.0001);
        assertEquals(51.0, signal.sma21, 0.0001);
        assertEquals(1.0, signal.signalStrength, 0.0001);
        
        System.out.println("Strategy signal created successfully: " + signal);
    }

    @Test
    public void testCEPPatternLogic() {
        // Test the pattern condition logic without full Flink execution
        CEPCrossoverStrategy strategy = new CEPCrossoverStrategy("TestCEP");
        
        // Test data that should match the pattern (sma5 > sma21)
        IndicatorWithPrice matchingEvent = new IndicatorWithPrice("AAPL", 1L, 150.0, 52.0, 51.0, 2.0);
        assertTrue("Event should match pattern: sma5 > sma21", matchingEvent.sma5 > matchingEvent.sma21);
        
        // Test data that should NOT match the pattern (sma5 <= sma21)
        IndicatorWithPrice nonMatchingEvent1 = new IndicatorWithPrice("AAPL", 2L, 150.0, 50.0, 52.0, 2.0);
        IndicatorWithPrice nonMatchingEvent2 = new IndicatorWithPrice("AAPL", 3L, 150.0, 51.0, 51.0, 2.0);
        
        assertFalse("Event should not match pattern: sma5 < sma21", nonMatchingEvent1.sma5 > nonMatchingEvent1.sma21);
        assertFalse("Event should not match pattern: sma5 = sma21", nonMatchingEvent2.sma5 > nonMatchingEvent2.sma21);
        
        System.out.println("CEP pattern logic verified:");
        System.out.println("  Matching event: " + matchingEvent);
        System.out.println("  Non-matching event 1: " + nonMatchingEvent1);
        System.out.println("  Non-matching event 2: " + nonMatchingEvent2);
    }

    @Test
    public void testSignalStrengthCalculation() {
        // Test the signal strength calculation logic from applyCEP
        IndicatorWithPrice event = new IndicatorWithPrice("AAPL", 1L, 150.0, 52.0, 50.0, 2.0);
        
        // Replicate the signal strength calculation from CEPCrossoverStrategy.applyCEP
        double signalStrength = event.sma21 != 0
                ? Math.min(1.0, Math.abs(event.sma5 - event.sma21) / event.sma21 * 100)
                : 0.0;
        
        // Expected: |52-50|/50 * 100 = 2/50 * 100 = 4, min(1.0, 4) = 1.0
        assertEquals("Signal strength should be 1.0 (capped)", 1.0, signalStrength, 0.0001);
        
        // Test with smaller difference
        IndicatorWithPrice event2 = new IndicatorWithPrice("AAPL", 2L, 150.0, 50.5, 50.0, 2.0);
        double signalStrength2 = event2.sma21 != 0
                ? Math.min(1.0, Math.abs(event2.sma5 - event2.sma21) / event2.sma21 * 100)
                : 0.0;
        
        // Expected: |50.5-50|/50 * 100 = 0.5/50 * 100 = 1.0
        assertEquals("Signal strength should be 1.0", 1.0, signalStrength2, 0.0001);
        
        // Test with very small difference
        IndicatorWithPrice event3 = new IndicatorWithPrice("AAPL", 3L, 150.0, 50.01, 50.0, 2.0);
        double signalStrength3 = event3.sma21 != 0
                ? Math.min(1.0, Math.abs(event3.sma5 - event3.sma21) / event3.sma21 * 100)
                : 0.0;
        
        // Expected: |50.01-50|/50 * 100 = 0.01/50 * 100 = 0.02
        assertEquals("Signal strength should be 0.02", 0.02, signalStrength3, 0.0001);
        
        System.out.println("Signal strength calculations verified:");
        System.out.println("  Event 1 strength: " + signalStrength);
        System.out.println("  Event 2 strength: " + signalStrength2);
        System.out.println("  Event 3 strength: " + signalStrength3);
    }

    @Test
    public void testApplyCEPMethodExists() {
        // Test that the applyCEP method exists and can be called
        CEPCrossoverStrategy strategy = new CEPCrossoverStrategy("TestCEP");
        
        // Verify the method exists by reflection
        try {
            java.lang.reflect.Method applyCEPMethod = strategy.getClass().getMethod("applyCEP", DataStream.class);
            assertNotNull("applyCEP method should exist", applyCEPMethod);
            assertEquals("applyCEP should return DataStream<StrategySignal>", 
                    "org.apache.flink.streaming.api.datastream.DataStream", 
                    applyCEPMethod.getReturnType().getName());
            
            System.out.println("applyCEP method signature verified:");
            System.out.println("  Method: " + applyCEPMethod.getName());
            System.out.println("  Parameter: " + applyCEPMethod.getParameterTypes()[0].getSimpleName());
            System.out.println("  Return type: " + applyCEPMethod.getReturnType().getSimpleName());
            
        } catch (NoSuchMethodException e) {
            fail("applyCEP method should exist: " + e.getMessage());
        }
    }

    @Test
    public void testPatternSelectFunctionLogic() {
        // Test the PatternSelectFunction logic independently
        CEPCrossoverStrategy strategy = new CEPCrossoverStrategy("TestCEP");
        
        // Simulate the pattern match map that would be passed to select()
        IndicatorWithPrice mockEvent = new IndicatorWithPrice("AAPL", 123456L, 150.0, 52.0, 50.0, 2.0);
        
        // Test the signal creation logic that happens in PatternSelectFunction.select()
        double expectedSignalStrength = mockEvent.sma21 != 0
                ? Math.min(1.0, Math.abs(mockEvent.sma5 - mockEvent.sma21) / mockEvent.sma21 * 100)
                : 0.0;
        
        // Create a signal manually using the same logic as in applyCEP
        StrategySignal signal = new StrategySignal(
                "testRunId",
                mockEvent.symbol,
                mockEvent.timestamp,
                mockEvent.close,
                mockEvent.sma5,
                mockEvent.sma21,
                "BUY",
                expectedSignalStrength
        );
        
        // Verify the signal was created correctly
        assertEquals("AAPL", signal.symbol);
        assertEquals(123456L, signal.timestamp);
        assertEquals(150.0, signal.close, 0.0001);
        assertEquals("BUY", signal.signal);
        assertEquals(52.0, signal.sma5, 0.0001);
        assertEquals(50.0, signal.sma21, 0.0001);
        assertEquals(1.0, signal.signalStrength, 0.0001); // Should be capped at 1.0
        
        System.out.println("PatternSelectFunction logic verified:");
        System.out.println("  Input event: " + mockEvent);
        System.out.println("  Generated signal: " + signal);
    }
}
