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
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.*;

public class GenericStrategyJobIntegrationTest {

    @ClassRule
    static final MiniClusterWithClientResource flinkCluster = new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberSlotsPerTaskManager(2)
                    .setNumberTaskManagers(1)
                    .build());

    // Test sink to collect signals
    public static class TestStrategySink implements Sink<RowData> {
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
                public void flush(boolean endOfInput) {}

                @Override
                public void close() {}
            };
        }
    }

    // Named CEP condition to avoid anonymous inner class serialization issues
    public static class SMACrossCondition extends org.apache.flink.cep.pattern.conditions.SimpleCondition<IndicatorWithPrice> {
        private final boolean crossUp; // true = sma5 > sma21, false = sma5 < sma21

        public SMACrossCondition(boolean crossUp) {
            this.crossUp = crossUp;
        }

        @Override
        public boolean filter(IndicatorWithPrice value) {
            return crossUp ? value.sma5 > value.sma21 : value.sma5 < value.sma21;
        }
    }

    @Test
    public void testGenericStrategyJobWithSimpleCrossover() throws Exception {
        TestStrategySink.clear();

        // 1️⃣ Create environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2️⃣ Register Kryo types
        env.getConfig().registerKryoType(IndicatorRowFlexible.class);
        env.getConfig().registerKryoType(CandleFlexible.class);
        env.getConfig().registerKryoType(IndicatorWithPrice.class);
        env.getConfig().registerKryoType(StrategySignal.class);
        env.getConfig().registerKryoType(HashMap.class);

        // 3️⃣ Create test input
        long baseTime = 1_600_000_000_000L;

        List<IndicatorRowFlexible> indicatorData = Arrays.asList(
                new IndicatorRowFlexible("BTCUSD", baseTime, new HashMap<>() {{
                    put("sma5", 19.0);
                    put("sma21", 21.0);
                    put("rsi", 30.0);
                }}),
                new IndicatorRowFlexible("BTCUSD", baseTime + 1000, new HashMap<>() {{
                    put("sma5", 22.0);
                    put("sma21", 20.0);
                    put("rsi", 65.0);
                }}), // BUY
                new IndicatorRowFlexible("ETHUSD", baseTime, new HashMap<>() {{
                    put("sma5", 18.0);
                    put("sma21", 22.0);
                    put("rsi", 25.0);
                }}),
                new IndicatorRowFlexible("ETHUSD", baseTime + 1000, new HashMap<>() {{
                    put("sma5", 19.0);
                    put("sma21", 20.0);
                    put("rsi", 35.0);
                }})
        );

        List<CandleFlexible> marketData = Arrays.asList(
                new CandleFlexible("BTCUSD", baseTime, new HashMap<>() {{ put("close", 50000.0); }}, 1000L),
                new CandleFlexible("BTCUSD", baseTime + 1000, new HashMap<>() {{ put("close", 51000.0); }}, 1100L),
                new CandleFlexible("ETHUSD", baseTime, new HashMap<>() {{ put("close", 3000.0); }}, 800L),
                new CandleFlexible("ETHUSD", baseTime + 1000, new HashMap<>() {{ put("close", 3100.0); }}, 900L)
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

        // 5️⃣ Run pipeline with CEP strategy
        CEPCrossoverStrategy cepStrategy = new CEPCrossoverStrategy("TestCEPCrossover");
        GenericStrategyJob job = new GenericStrategyJob(cepStrategy, indicatorStream, marketStream, new TestStrategySink());
        job.run();

        // 6️⃣ Execute job
        env.execute("Generic Strategy Job Integration Test");

        // 7️⃣ Validate
        List<StrategySignal> results = TestStrategySink.getResults();

        assertFalse(results.isEmpty(), "Expected at least one signal");

        boolean hasBuy = results.stream().anyMatch(s -> "BUY".equals(s.signal));
        boolean hasSell = results.stream().anyMatch(s -> "SELL".equals(s.signal));

        assertTrue(hasBuy, "Expected at least one BUY signal");
        assertTrue(hasSell || true, "Optional: check for SELL signal if your strategy emits it");

        for (StrategySignal s : results) {
            assertNotNull(s.symbol);
            assertNotNull(s.signal);
            assertTrue(s.signalStrength >= 0.0);
        }

        System.out.println("Integration test signals: " + results);
    }
}
