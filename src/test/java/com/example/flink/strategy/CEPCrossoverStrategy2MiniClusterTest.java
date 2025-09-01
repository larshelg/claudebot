package com.example.flink.strategy;

import com.example.flink.IndicatorWithPrice;
import com.example.flink.domain.StrategySignal;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.ClassRule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MiniCluster integration test for CEPCrossoverStrategy2.
 * Verifies BUY on golden cross and SELL on death cross.
 */
public class CEPCrossoverStrategy2MiniClusterTest {

    @ClassRule
    public static final MiniClusterWithClientResource FLINK =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(2)
                            .build());

    @BeforeEach
    void setup() {
        StrategySignalCollectSink.clear();
    }

    @Test
    void emitsBuyThenSell() throws Exception {
        // --- env
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(0L);
        env.getConfig().enableObjectReuse();

        // --- input sequence for one symbol ("BTC"):
        // t1: non-golden (sma5 <= sma21)
        // t2: golden     (sma5 >  sma21)  -> BUY
        // t3: non-death  (sma5 >= sma21)
        // t4: death      (sma5 <  sma21)  -> SELL
        var t1 = ind("BTC", 1_000L, /*close*/10, /*sma5*/ 9.9, /*sma21*/10.0);
        var t2 = ind("BTC", 2_000L, /*close*/11, /*sma5*/10.1, /*sma21*/10.0);
        var t3 = ind("BTC", 3_000L, /*close*/12, /*sma5*/10.1, /*sma21*/10.0);
        var t4 = ind("BTC", 4_000L, /*close*/ 9, /*sma5*/ 9.5, /*sma21*/10.0);

        var indicators = env.fromElements(t1, t2, t3, t4)
                .returns(TypeInformation.of(new TypeHint<IndicatorWithPrice>(){}));

        // --- strategy
        var strat = new CEPCrossoverStrategy2(
                "sma-xover:v1",
                org.apache.flink.streaming.api.windowing.time.Time.minutes(30),
                Duration.ofSeconds(0), // ordered events in this test
                0L                      // no cooldown
        );

        var signals = strat.apply(indicators);

        // --- collect
        signals.sinkTo(new StrategySignalCollectSink()).name("collect");

        // --- execute
        env.execute("CEPCrossoverStrategy2 MiniCluster Test");

        // --- assert
        List<StrategySignal> out = StrategySignalCollectSink.drain();
        assertEquals(2, out.size(), "expected BUY then SELL");

        var s1 = out.get(0);
        var s2 = out.get(1);

        assertEquals("BTC", s1.symbol);
        assertEquals("BUY", s1.signal);
        assertEquals(2_000L, s1.timestamp);

        assertEquals("BTC", s2.symbol);
        assertEquals("SELL", s2.signal);
        assertEquals(4_000L, s2.timestamp);

        // monotonic by timestamp in this test
        assertTrue(s1.timestamp < s2.timestamp);
    }

    @Test
    void respectsCooldownDropsCloseCrosses() throws Exception {
        // --- env
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(0L);
        env.getConfig().enableObjectReuse();

        // We’ll create two golden crosses very close together (should drop the 2nd due to cooldown),
        // then a third golden cross after the cooldown window (should emit).
        // Timeline (ms):
        // 1000: non-golden
        // 1100: golden         -> BUY #1 (emitted)
        // 1150: non-golden
        // 1200: golden         -> BUY (within 500ms of #1) => DROPPED by cooldown
        // 2000: non-golden
        // 2600: golden         -> BUY #2 (emitted; 1500ms after #1)
        var e1 = ind("BTC", 1000L, 10,  9.9, 10.0);
        var e2 = ind("BTC", 1100L, 11, 10.1, 10.0); // BUY #1
        var e3 = ind("BTC", 1150L, 10,  9.95,10.0);
        var e4 = ind("BTC", 1200L, 10, 10.05,10.0); // BUY (should be DROPPED)
        var e5 = ind("BTC", 2000L, 10,  9.9, 10.0);
        var e6 = ind("BTC", 2600L, 12, 10.3, 10.0); // BUY #2

        var indicators = env.fromElements(e1, e2, e3, e4, e5, e6)
                .returns(TypeInformation.of(new TypeHint<IndicatorWithPrice>(){}));

        // Strategy with 500ms cooldown and no lateness (events ordered in this test)
        var strat = new CEPCrossoverStrategy2(
                "sma-xover:v1",
                org.apache.flink.streaming.api.windowing.time.Time.minutes(30),
                Duration.ofSeconds(0),
                /*cooldownMs*/ 500L
        );

        var signals = strat.apply(indicators);

        // Collect emitted signals
        StrategySignalCollectSink.clear();
        signals.sinkTo(new StrategySignalCollectSink()).name("collect");

        // Execute
        env.execute("CEPCrossoverStrategy2 Cooldown Test");

        // Assert: exactly two BUYs (at 1100 and 2600); the 1200 BUY was dropped by cooldown
        var out = StrategySignalCollectSink.drain();
        assertEquals(2, out.size(), "expected 2 BUYs due to cooldown dropping the middle one");

        var s1 = out.get(0);
        var s2 = out.get(1);

        assertEquals("BTC", s1.symbol);
        assertEquals("BUY", s1.signal);
        assertEquals(1100L, s1.timestamp);

        assertEquals("BTC", s2.symbol);
        assertEquals("BUY", s2.signal);
        assertEquals(2600L, s2.timestamp);

        assertTrue(s1.timestamp < s2.timestamp);
    }


    // helper to create IndicatorWithPrice rows
    private static IndicatorWithPrice ind(String symbol, long ts, double close, double sma5, double sma21) {
        IndicatorWithPrice x = new IndicatorWithPrice();
        x.symbol = symbol;
        x.timestamp = ts;
        x.close = close;
        x.sma5 = sma5;
        x.sma21 = sma21;
        return x;
    }
}
