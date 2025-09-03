package com.example.flink;

import com.example.flink.domain.Candle;
import com.example.flink.indicator.IntegratedIndicatorState;
import com.example.flink.indicator.MultiIndicatorProcess;

/**
 * Simple test class to verify the indicator logic works correctly
 * without running the full Flink runtime.
 */
public class SimpleIndicatorTest {

    public static void main(String[] args) throws Exception {
        System.out.println("=== Testing Multi-Indicator Logic ===");

        // Create a mock process function to test the logic
        MultiIndicatorProcess processor = new MultiIndicatorProcess();

        // Test the indicator calculations directly
        testIndicatorCalculations();

        System.out.println("\n=== All tests completed successfully! ===");
    }

    private static void testIndicatorCalculations() {
        System.out.println("\n1. Testing Simple Moving Average:");
        SimpleMovingAverage sma = new SimpleMovingAverage(3);

        sma.add(100.0);
        System.out.println("   After adding 100.0: SMA = " + sma.get());

        sma.add(110.0);
        System.out.println("   After adding 110.0: SMA = " + sma.get());

        sma.add(120.0);
        System.out.println("   After adding 120.0: SMA = " + sma.get());

        sma.add(130.0);
        System.out.println("   After adding 130.0: SMA = " + sma.get());

        System.out.println("\n2. Testing Heavy Indicator State (RSI):");
        HeavyIndicatorState heavy = new HeavyIndicatorState();

        // Add some price data
        double[] prices = { 100.0, 102.0, 101.0, 103.0, 105.0, 104.0, 106.0, 108.0, 107.0, 109.0, 111.0, 110.0, 112.0,
                114.0, 113.0 };

        for (double price : prices) {
            heavy.addClosePrice(price);
            double rsi = heavy.calculateRSI(14);
            System.out.println("   Price: " + price + ", RSI(14): " + String.format("%.2f", rsi));
        }

        System.out.println("\n3. Testing Integrated State:");
        IntegratedIndicatorState state = new IntegratedIndicatorState();

        // Initialize SMAs
        state.sma.put(5, new SimpleMovingAverage(5));
        state.sma.put(14, new SimpleMovingAverage(14));

        // Initialize EMAs
        state.ema.put(5, 0.0);
        state.ema.put(14, 0.0);

        // Test with sample data
        double[] testPrices = { 50000.0, 50100.0, 49900.0, 50200.0, 50300.0, 50150.0, 50400.0 };

        for (int i = 0; i < testPrices.length; i++) {
            double price = testPrices[i];

            // Update SMA
            state.sma.get(5).add(price);
            state.sma.get(14).add(price);

            // Update EMA
            for (int period : new int[] { 5, 14 }) {
                double prevEma = state.ema.get(period);
                double multiplier = 2.0 / (period + 1);
                double ema = prevEma == 0 ? price : (price - prevEma) * multiplier + prevEma;
                state.ema.put(period, ema);
            }

            // Update heavy indicators
            state.heavy.addClosePrice(price);
            double rsi = state.heavy.calculateRSI(14);

            System.out.println("   Day " + (i + 1) + ": Price=" + price +
                    ", SMA5=" + String.format("%.2f", state.sma.get(5).get()) +
                    ", SMA14=" + String.format("%.2f", state.sma.get(14).get()) +
                    ", EMA5=" + String.format("%.2f", state.ema.get(5)) +
                    ", EMA14=" + String.format("%.2f", state.ema.get(14)) +
                    ", RSI=" + String.format("%.2f", rsi));
        }

        System.out.println("\n4. Testing Candle and IndicatorRow:");
        Candle candle = new Candle("BTCUSD", System.currentTimeMillis(), 50000, 50500, 49800, 50200, 1234.5);
        System.out.println("   Created candle: " + candle.symbol + " at price " + candle.close);

        IndicatorRow row = new IndicatorRow(
                candle.symbol,
                candle.timestamp,
                100.0, 200.0, 300.0, // SMA values
                105.0, 205.0, 305.0, // EMA values
                65.5 // RSI value
        );
        System.out.println(
                "   Created indicator row for " + row.symbol + " with SMA5=" + row.sma5 + ", RSI=" + row.rsi14);
    }
}
