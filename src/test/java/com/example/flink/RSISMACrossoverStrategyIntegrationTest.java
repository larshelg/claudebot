package com.example.flink;

import com.example.flink.serializer.*;
import com.example.flink.strategy.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.data.RowData;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.*;

public class RSISMACrossoverStrategyIntegrationTest {

    @ClassRule
    static final MiniClusterWithClientResource flinkCluster = new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberSlotsPerTaskManager(2)
                    .setNumberTaskManagers(1)
                    .build());

    // Test sink to collect signals
    public static class TestRSISMASink implements Sink<RowData> {
        private static final Queue<StrategySignal> collectedSignals = new ConcurrentLinkedQueue<>();

        public static void clear() {
            collectedSignals.clear();
        }

        public static List<StrategySignal> getResults() {
            return new ArrayList<>(collectedSignals);
        }

        @Override
        public SinkWriter<RowData> createWriter(InitContext context) {
            return new SinkWriter<RowData>() {
                @Override
                public void write(RowData element, Context context) {
                    collectedSignals.add(RowDataConverter.fromRowData(element));
                }

                @Override
                public void flush(boolean endOfInput) {
                }

                @Override
                public void close() {
                }
            };
        }
    }

    // Helper methods for cleaner test data construction
    private static IndicatorRowFlexible createIndicator(String symbol, long timestamp, double sma5, double sma21,
            double rsi) {
        var indicators = new HashMap<String, Double>();
        indicators.put("sma5", sma5);
        indicators.put("sma21", sma21);
        indicators.put("rsi", rsi);
        return new IndicatorRowFlexible(symbol, timestamp, indicators);
    }

    private static CandleFlexible createCandle(String symbol, long timestamp, double close, long volume) {
        var priceData = new HashMap<String, Double>();
        priceData.put("close", close);
        return new CandleFlexible(symbol, timestamp, priceData, volume);
    }

    private static DataStream<IndicatorRowFlexible> createIndicatorStream(StreamExecutionEnvironment env,
            List<IndicatorRowFlexible> data) {
        return env.fromCollection(data)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<IndicatorRowFlexible>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner((e, ts) -> e.timestamp));
    }

    private static DataStream<CandleFlexible> createMarketStream(StreamExecutionEnvironment env,
            List<CandleFlexible> data) {
        return env.fromCollection(data)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<CandleFlexible>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner((e, ts) -> e.timestamp));
    }

    @Test
    public void testRSISMACrossoverStrategyBuyPattern() throws Exception {
        TestRSISMASink.clear();

        // 1️⃣ Create environment
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 3️⃣ Create test data that triggers BUY pattern
        // Pattern: RSI < 25 → SMA5 ≤ SMA21 → SMA5 > SMA21
        var baseTime = 1_600_000_000_000L;

        var indicatorData = Arrays.asList(
                // Step 1: RSI oversold (RSI < 25)
                createIndicator("BTCUSD", baseTime, 20.0, 22.0, 20.0), // Oversold condition

                // Step 2: SMA5 still below SMA21 (prerequisite for crossover)
                createIndicator("BTCUSD", baseTime + 60000, 21.5, 22.0, 35.0), // RSI recovering

                // Step 3: SMA5 crosses above SMA21 (BUY signal trigger)
                createIndicator("BTCUSD", baseTime + 120000, 22.5, 22.0, 45.0), // RSI normalized

                // Additional symbol for testing isolation
                createIndicator("ETHUSD", baseTime, 18.0, 20.0, 50.0) // No signal expected
        );

        var marketData = Arrays.asList(
                createCandle("BTCUSD", baseTime, 50000.0, 1000L),
                createCandle("BTCUSD", baseTime + 60000, 50500.0, 1100L),
                createCandle("BTCUSD", baseTime + 120000, 51000.0, 1200L),
                createCandle("ETHUSD", baseTime, 3000.0, 800L));

        // 4️⃣ Sources with watermarks
        var indicatorStream = createIndicatorStream(env, indicatorData);
        var marketStream = createMarketStream(env, marketData);

        // 5️⃣ Run pipeline with RSI-SMA strategy
        var rsiSmaStrategy = new RSISMACrossoverStrategy("TestRSISMACrossover");
        var job = new GenericStrategyJob(rsiSmaStrategy, indicatorStream, marketStream,
                new TestRSISMASink());
        job.run();

        // 6️⃣ Execute job
        env.execute("RSI-SMA Crossover Strategy Integration Test");

        // 7️⃣ Validate results
        var results = TestRSISMASink.getResults();

        assertFalse(results.isEmpty(), "Expected at least one signal from RSI-SMA strategy");

        // Verify we got a BUY signal for BTCUSD
        boolean hasBuyForBTC = results.stream()
                .anyMatch(s -> "BUY".equals(s.signal) && "BTCUSD".equals(s.symbol));
        assertTrue(hasBuyForBTC, "Expected BUY signal for BTCUSD from RSI-SMA pattern");

        // Verify signal properties
        for (StrategySignal signal : results) {
            assertNotNull(signal.symbol);
            assertNotNull(signal.signal);
            assertTrue(signal.signalStrength >= 0.0 && signal.signalStrength <= 1.0,
                    "Signal strength should be between 0 and 1, got: " + signal.signalStrength);
            assertTrue("BUY".equals(signal.signal) || "SELL".equals(signal.signal),
                    "Signal should be BUY or SELL, got: " + signal.signal);
        }

        System.out.println("RSI-SMA BUY Pattern Test Results: " + results);
    }

    @Test
    public void testRSISMACrossoverStrategySellPattern() throws Exception {
        TestRSISMASink.clear();

        // 1️⃣ Create environment
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 3️⃣ Create test data that triggers SELL pattern
        // Pattern: RSI > 72 → Price ≥ SMA5 → Price < SMA5
        var baseTime = 1_600_000_000_000L;

        var indicatorData = Arrays.asList(
                // Step 1: RSI overbought (RSI > 72)
                createIndicator("ETHUSD", baseTime, 3000.0, 2950.0, 80.0), // Overbought condition

                // Step 2: Price above SMA5 (prerequisite for breakdown)
                createIndicator("ETHUSD", baseTime + 60000, 3000.0, 2950.0, 65.0), // RSI cooling down

                // Step 3: Price crosses below SMA5 (SELL signal trigger)
                createIndicator("ETHUSD", baseTime + 120000, 3000.0, 2950.0, 45.0) // RSI normalized
        );

        var marketData = Arrays.asList(
                createCandle("ETHUSD", baseTime, 3100.0, 1000L), // Above SMA5
                createCandle("ETHUSD", baseTime + 60000, 3050.0, 1100L), // Still above SMA5
                createCandle("ETHUSD", baseTime + 120000, 2950.0, 1200L) // Below SMA5
        );

        // 4️⃣ Sources with watermarks
        var indicatorStream = createIndicatorStream(env, indicatorData);
        var marketStream = createMarketStream(env, marketData);

        // 5️⃣ Run pipeline with RSI-SMA strategy
        var rsiSmaStrategy = new RSISMACrossoverStrategy("TestRSISMACrossover");
        var job = new GenericStrategyJob(rsiSmaStrategy, indicatorStream, marketStream,
                new TestRSISMASink());
        job.run();

        // 6️⃣ Execute job
        env.execute("RSI-SMA Crossover Strategy SELL Test");

        // 7️⃣ Validate results
        var results = TestRSISMASink.getResults();

        assertFalse(results.isEmpty(), "Expected at least one signal from RSI-SMA strategy");

        // Verify we got a SELL signal for ETHUSD
        boolean hasSellForETH = results.stream()
                .anyMatch(s -> "SELL".equals(s.signal) && "ETHUSD".equals(s.symbol));
        assertTrue(hasSellForETH, "Expected SELL signal for ETHUSD from RSI-SMA pattern");

        // Verify signal properties
        for (StrategySignal signal : results) {
            assertNotNull(signal.symbol);
            assertNotNull(signal.signal);
            assertTrue(signal.signalStrength >= 0.0 && signal.signalStrength <= 1.0,
                    "Signal strength should be between 0 and 1, got: " + signal.signalStrength);
            assertEquals("SELL", signal.signal, "Expected SELL signal in this test");
            assertEquals("ETHUSD", signal.symbol, "Expected ETHUSD symbol");
        }

        System.out.println("RSI-SMA SELL Pattern Test Results: " + results);
    }

    @Test
    public void testRSISMACrossoverStrategyNoSignals() throws Exception {
        TestRSISMASink.clear();

        // 1️⃣ Create environment
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 3️⃣ Create test data that should NOT trigger any patterns
        var baseTime = 1_600_000_000_000L;

        var indicatorData = Arrays.asList(
                // RSI in normal range, no extremes
                createIndicator("BTCUSD", baseTime, 20.0, 21.0, 50.0), // Normal RSI
                createIndicator("BTCUSD", baseTime + 60000, 20.5, 21.0, 55.0) // Still normal
        );

        var marketData = Arrays.asList(
                createCandle("BTCUSD", baseTime, 50000.0, 1000L),
                createCandle("BTCUSD", baseTime + 60000, 50500.0, 1100L));

        // 4️⃣ Sources with watermarks
        var indicatorStream = createIndicatorStream(env, indicatorData);
        var marketStream = createMarketStream(env, marketData);

        // 5️⃣ Run pipeline with RSI-SMA strategy
        var rsiSmaStrategy = new RSISMACrossoverStrategy("TestRSISMACrossover");
        var job = new GenericStrategyJob(rsiSmaStrategy, indicatorStream, marketStream,
                new TestRSISMASink());
        job.run();

        // 6️⃣ Execute job
        env.execute("RSI-SMA Crossover Strategy No Signals Test");

        // 7️⃣ Validate results - should be empty
        var results = TestRSISMASink.getResults();

        assertTrue(results.isEmpty(), "Expected no signals with normal RSI values, but got: " + results);

        System.out.println("RSI-SMA No Signals Test Results: " + results.size() + " signals (expected 0)");
    }
}
