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

    @Test
    public void testRSISMACrossoverStrategyBuyPattern() throws Exception {
        TestRSISMASink.clear();

        // 1️⃣ Create environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2️⃣ Register Kryo types
        env.getConfig().registerKryoType(IndicatorRowFlexible.class);
        env.getConfig().registerKryoType(CandleFlexible.class);
        env.getConfig().registerKryoType(IndicatorWithPrice.class);
        env.getConfig().registerKryoType(StrategySignal.class);
        env.getConfig().registerKryoType(HashMap.class);

        // 3️⃣ Create test data that triggers BUY pattern
        // Pattern: RSI < 25 → SMA5 ≤ SMA21 → SMA5 > SMA21
        long baseTime = 1_600_000_000_000L;

        List<IndicatorRowFlexible> indicatorData = Arrays.asList(
                // Step 1: RSI oversold (RSI < 25)
                new IndicatorRowFlexible("BTCUSD", baseTime, new HashMap<>() {
                    {
                        put("sma5", 20.0);
                        put("sma21", 22.0);
                        put("rsi", 20.0); // Oversold condition
                    }
                }),

                // Step 2: SMA5 still below SMA21 (prerequisite for crossover)
                new IndicatorRowFlexible("BTCUSD", baseTime + 60000, new HashMap<>() {
                    {
                        put("sma5", 21.5);
                        put("sma21", 22.0);
                        put("rsi", 35.0); // RSI recovering
                    }
                }),

                // Step 3: SMA5 crosses above SMA21 (BUY signal trigger)
                new IndicatorRowFlexible("BTCUSD", baseTime + 120000, new HashMap<>() {
                    {
                        put("sma5", 22.5);
                        put("sma21", 22.0);
                        put("rsi", 45.0); // RSI normalized
                    }
                }),

                // Additional symbol for testing isolation
                new IndicatorRowFlexible("ETHUSD", baseTime, new HashMap<>() {
                    {
                        put("sma5", 18.0);
                        put("sma21", 20.0);
                        put("rsi", 50.0); // No signal expected
                    }
                }));

        List<CandleFlexible> marketData = Arrays.asList(
                new CandleFlexible("BTCUSD", baseTime, new HashMap<>() {
                    {
                        put("close", 50000.0);
                    }
                }, 1000L),
                new CandleFlexible("BTCUSD", baseTime + 60000, new HashMap<>() {
                    {
                        put("close", 50500.0);
                    }
                }, 1100L),
                new CandleFlexible("BTCUSD", baseTime + 120000, new HashMap<>() {
                    {
                        put("close", 51000.0);
                    }
                }, 1200L),
                new CandleFlexible("ETHUSD", baseTime, new HashMap<>() {
                    {
                        put("close", 3000.0);
                    }
                }, 800L));

        // 4️⃣ Sources with watermarks
        DataStream<IndicatorRowFlexible> indicatorStream = env.fromCollection(indicatorData)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<IndicatorRowFlexible>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner((e, ts) -> e.timestamp));

        DataStream<CandleFlexible> marketStream = env.fromCollection(marketData)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<CandleFlexible>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner((e, ts) -> e.timestamp));

        // 5️⃣ Run pipeline with RSI-SMA strategy
        RSISMACrossoverStrategy rsiSmaStrategy = new RSISMACrossoverStrategy("TestRSISMACrossover");
        GenericStrategyJob job = new GenericStrategyJob(rsiSmaStrategy, indicatorStream, marketStream,
                new TestRSISMASink());
        job.run();

        // 6️⃣ Execute job
        env.execute("RSI-SMA Crossover Strategy Integration Test");

        // 7️⃣ Validate results
        List<StrategySignal> results = TestRSISMASink.getResults();

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
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2️⃣ Register Kryo types
        env.getConfig().registerKryoType(IndicatorRowFlexible.class);
        env.getConfig().registerKryoType(CandleFlexible.class);
        env.getConfig().registerKryoType(IndicatorWithPrice.class);
        env.getConfig().registerKryoType(StrategySignal.class);
        env.getConfig().registerKryoType(HashMap.class);

        // 3️⃣ Create test data that triggers SELL pattern
        // Pattern: RSI > 72 → Price ≥ SMA5 → Price < SMA5
        long baseTime = 1_600_000_000_000L;

        List<IndicatorRowFlexible> indicatorData = Arrays.asList(
                // Step 1: RSI overbought (RSI > 72)
                new IndicatorRowFlexible("ETHUSD", baseTime, new HashMap<>() {
                    {
                        put("sma5", 3000.0);
                        put("sma21", 2950.0);
                        put("rsi", 80.0); // Overbought condition
                    }
                }),

                // Step 2: Price above SMA5 (prerequisite for breakdown)
                new IndicatorRowFlexible("ETHUSD", baseTime + 60000, new HashMap<>() {
                    {
                        put("sma5", 3000.0);
                        put("sma21", 2950.0);
                        put("rsi", 65.0); // RSI cooling down
                    }
                }),

                // Step 3: Price crosses below SMA5 (SELL signal trigger)
                new IndicatorRowFlexible("ETHUSD", baseTime + 120000, new HashMap<>() {
                    {
                        put("sma5", 3000.0);
                        put("sma21", 2950.0);
                        put("rsi", 45.0); // RSI normalized
                    }
                }));

        List<CandleFlexible> marketData = Arrays.asList(
                new CandleFlexible("ETHUSD", baseTime, new HashMap<>() {
                    {
                        put("close", 3100.0);
                    }
                }, 1000L), // Above SMA5
                new CandleFlexible("ETHUSD", baseTime + 60000, new HashMap<>() {
                    {
                        put("close", 3050.0);
                    }
                }, 1100L), // Still above SMA5
                new CandleFlexible("ETHUSD", baseTime + 120000, new HashMap<>() {
                    {
                        put("close", 2950.0);
                    }
                }, 1200L) // Below SMA5
        );

        // 4️⃣ Sources with watermarks
        DataStream<IndicatorRowFlexible> indicatorStream = env.fromCollection(indicatorData)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<IndicatorRowFlexible>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner((e, ts) -> e.timestamp));

        DataStream<CandleFlexible> marketStream = env.fromCollection(marketData)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<CandleFlexible>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner((e, ts) -> e.timestamp));

        // 5️⃣ Run pipeline with RSI-SMA strategy
        RSISMACrossoverStrategy rsiSmaStrategy = new RSISMACrossoverStrategy("TestRSISMACrossover");
        GenericStrategyJob job = new GenericStrategyJob(rsiSmaStrategy, indicatorStream, marketStream,
                new TestRSISMASink());
        job.run();

        // 6️⃣ Execute job
        env.execute("RSI-SMA Crossover Strategy SELL Test");

        // 7️⃣ Validate results
        List<StrategySignal> results = TestRSISMASink.getResults();

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
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2️⃣ Register Kryo types
        env.getConfig().registerKryoType(IndicatorRowFlexible.class);
        env.getConfig().registerKryoType(CandleFlexible.class);
        env.getConfig().registerKryoType(IndicatorWithPrice.class);
        env.getConfig().registerKryoType(StrategySignal.class);
        env.getConfig().registerKryoType(HashMap.class);

        // 3️⃣ Create test data that should NOT trigger any patterns
        long baseTime = 1_600_000_000_000L;

        List<IndicatorRowFlexible> indicatorData = Arrays.asList(
                // RSI in normal range, no extremes
                new IndicatorRowFlexible("BTCUSD", baseTime, new HashMap<>() {
                    {
                        put("sma5", 20.0);
                        put("sma21", 21.0);
                        put("rsi", 50.0); // Normal RSI
                    }
                }),

                new IndicatorRowFlexible("BTCUSD", baseTime + 60000, new HashMap<>() {
                    {
                        put("sma5", 20.5);
                        put("sma21", 21.0);
                        put("rsi", 55.0); // Still normal
                    }
                }));

        List<CandleFlexible> marketData = Arrays.asList(
                new CandleFlexible("BTCUSD", baseTime, new HashMap<>() {
                    {
                        put("close", 50000.0);
                    }
                }, 1000L),
                new CandleFlexible("BTCUSD", baseTime + 60000, new HashMap<>() {
                    {
                        put("close", 50500.0);
                    }
                }, 1100L));

        // 4️⃣ Sources with watermarks
        DataStream<IndicatorRowFlexible> indicatorStream = env.fromCollection(indicatorData)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<IndicatorRowFlexible>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner((e, ts) -> e.timestamp));

        DataStream<CandleFlexible> marketStream = env.fromCollection(marketData)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<CandleFlexible>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner((e, ts) -> e.timestamp));

        // 5️⃣ Run pipeline with RSI-SMA strategy
        RSISMACrossoverStrategy rsiSmaStrategy = new RSISMACrossoverStrategy("TestRSISMACrossover");
        GenericStrategyJob job = new GenericStrategyJob(rsiSmaStrategy, indicatorStream, marketStream,
                new TestRSISMASink());
        job.run();

        // 6️⃣ Execute job
        env.execute("RSI-SMA Crossover Strategy No Signals Test");

        // 7️⃣ Validate results - should be empty
        List<StrategySignal> results = TestRSISMASink.getResults();

        assertTrue(results.isEmpty(), "Expected no signals with normal RSI values, but got: " + results);

        System.out.println("RSI-SMA No Signals Test Results: " + results.size() + " signals (expected 0)");
    }
}
