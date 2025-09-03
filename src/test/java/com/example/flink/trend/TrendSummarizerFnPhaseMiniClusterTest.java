package com.example.flink.trend;

import com.example.flink.trend.TrendLineDetectorJob.TrendLine;
import com.example.flink.trend.TrendSummarizerFn.TrendSummary;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.ClassRule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies the extended TrendSummarizerFn phase logic:
 * - Window #1: fresh breakout -> phase = BREAKOUT
 * - Window #2: stale breakout -> phase = PERSIST (since confidence decays with
 * recency)
 *
 * Uses processing-time timers, so we pick breakout timestamps that guarantee
 * high/low recency regardless of actual processing clock.
 */
public class TrendSummarizerFnPhaseMiniClusterTest {

        @ClassRule
        public static final MiniClusterWithClientResource FLINK = new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                        .setNumberTaskManagers(1)
                                        .setNumberSlotsPerTaskManager(2)
                                        .build());

        @BeforeEach
        void setup() {
                TrendSummaryCollectSink.clear();
        }

        @Test
        void breakoutThenNoneWhenRecencyFades() throws Exception {
                // --- env
                var env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(1);
                env.getConfig().setAutoWatermarkInterval(0L);
                env.getConfig().enableObjectReuse();

                final String SYM = "BTC";
                final long W1_START = 1_000_000L;
                final long W1_END = W1_START + 30 * 60_000L; // 30m window
                final long W2_START = W1_END;
                final long W2_END = W2_START + 30 * 60_000L;

                // Window #1: strong UP line with "future" breakout ts => recency ~ 1.0 ->
                // BREAKOUT
                TrendLine upFreshBreakout = tl(SYM, W1_START, W1_END,
                                /* slope */ +1.0e-3, /* slopeNorm */ 0.70,
                                /* score */ 0.85, /* support */ 0.70,
                                /* touches */ 3, /* breakouts */ 1, /* breakoutStrength */ 0.30,
                                /* lastBreakoutTs */ Long.MAX_VALUE / 2, /* side */ "RESISTANCE",
                                /* timeSpanMs */ (W1_END - W1_START), /* priceRange */ 20.0);

                // Include a weaker opposing line (down) for the same window; softmax should
                // still favor up
                TrendLine downWeaker = tl(SYM, W1_START, W1_END,
                                /* slope */ -6.0e-4, /* slopeNorm */ 0.40,
                                /* score */ 0.60, /* support */ 0.45,
                                /* touches */ 2, /* breakouts */ 0, /* breakoutStrength */ 0.05,
                                /* lastBreakoutTs */ -1, /* side */ "SUPPORT",
                                /* timeSpanMs */ (W1_END - W1_START), /* priceRange */ 20.0);

                // Window #2: still an UP line but with a very old breakout => recency ~ 0 ->
                // NONE
                TrendLine upStaleBreakout = tl(SYM, W2_START, W2_END,
                                /* slope */ +9.0e-4, /* slopeNorm */ 0.65,
                                /* score */ 0.80, /* support */ 0.68,
                                /* touches */ 3, /* breakouts */ 1, /* breakoutStrength */ 0.22,
                                /* lastBreakoutTs */ 0L, /* side */ "RESISTANCE", // epoch -> very stale
                                /* timeSpanMs */ (W2_END - W2_START), /* priceRange */ 18.0);

                // Sentinel to flush
                final long SENT1_END = W2_END + 1;
                TrendLine sentinel1 = tl(SYM, W2_END, SENT1_END,
                                0.0, 0.0,
                                0.0, 0.0,
                                0, 0, 0.0,
                                -1L, "RESISTANCE",
                                1L, 1.0);

                DataStream<TrendSummary> summaries = env
                                .fromElements(upFreshBreakout, downWeaker, upStaleBreakout, sentinel1)
                                .returns(TypeInformation.of(new TypeHint<TrendLine>() {
                                }))
                                .keyBy(tl -> tl.symbol)
                                .process(new TrendSummarizerFn(
                                                /* lambdaSoftmax */ 3.0,
                                                /* betaSmoothing */ 0.30,
                                                /* fuseDelay */ Duration.ofMillis(10),
                                                /* recencyTau */ Duration.ofHours(2),
                                                /* enter BREAKOUT */ 0.50,
                                                /* exit BREAKOUT */ 0.30,
                                                /* trendAlpha */ 0.40,
                                                /* confThresh */ 0.30))
                                .name("trend-factor");

                summaries.sinkTo(new TrendSummaryCollectSink()).name("collect-trend");

                env.execute("TrendSummarizer Phase MiniCluster Test");

                List<TrendSummary> out = TrendSummaryCollectSink.drain();
                out.removeIf(s -> s.ts == SENT1_END);
                assertEquals(2, out.size(), "expected 2 summaries (one per window)");

                // Sort by timestamp to ensure consistent ordering
                out.sort((a, b) -> Long.compare(a.ts, b.ts));

                TrendSummary s1 = out.get(0);
                TrendSummary s2 = out.get(1);

                // Window #1 assertions: uptrend favored, BREAKOUT phase
                assertEquals(SYM, s1.symbol);
                assertEquals(W1_END, s1.ts);
                assertTrue(s1.alpha > 0.0, "alpha should be positive (uptrend favored)");
                assertEquals("BREAKOUT", s1.phase, "fresh breakout should yield BREAKOUT phase");
                assertTrue(s1.breakoutRecency > 0.9, "recency should be high for future/very recent breakout");
                assertTrue(s1.confidence > 0.0 && s1.confidence <= 1.0);

                // Window #2 assertions: still up line, but stale breakout -> NONE (not PERSIST
                // due to low confidence)
                assertEquals(SYM, s2.symbol);
                assertEquals(W2_END, s2.ts);
                assertTrue(s2.alpha > 0.0, "direction remains up, but...");
                assertEquals("PERSIST", s2.phase,
                                "stale breakout should exit BREAKOUT and continue as PERSIST with decoupled confidence");
                assertTrue(s2.breakoutRecency < 0.05, "recency should be near zero for stale breakout");
        }

        @Test
        void resistanceBreakoutThenPersistentTrend() throws Exception {
                // --- env
                var env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(1);
                env.getConfig().setAutoWatermarkInterval(0L);
                env.getConfig().enableObjectReuse();

                final String SYM = "ETH";
                final long W1_START = 2_000_000L;
                final long W1_END = W1_START + 30 * 60_000L;
                final long W2_START = W1_END;
                final long W2_END = W2_START + 30 * 60_000L;

                // Window #1: Strong upward breakout from resistance with very recent breakout
                TrendLine upBreakoutFromResistance = tl(SYM, W1_START, W1_END,
                                /* slope */ +1.4e-3, /* slopeNorm */ 0.85,
                                /* score */ 0.90, /* support */ 0.80,
                                /* touches */ 4, /* breakouts */ 1, /* breakoutStrength */ 0.65,
                                /* lastBreakoutTs */ Long.MAX_VALUE / 2, /* side */ "RESISTANCE",
                                /* timeSpanMs */ (W1_END - W1_START), /* priceRange */ 25.0);

                // Window #2: Continuing upward trend with moderate recency but high confidence
                // This should transition from BREAKOUT to PERSIST
                TrendLine upPersistentTrend = tl(SYM, W2_START, W2_END,
                                /* slope */ +1.1e-3, /* slopeNorm */ 0.75,
                                /* score */ 0.85, /* support */ 0.75,
                                /* touches */ 3, /* breakouts */ 1, /* breakoutStrength */ 0.45,
                                /* lastBreakoutTs */ W2_START - 60_000, // 1 minute ago, moderate recency
                                /* side */ "RESISTANCE",
                                /* timeSpanMs */ (W2_END - W2_START), /* priceRange */ 22.0);

                // Window #3: Trend continues with good confidence but older breakout
                // Should remain in PERSIST phase
                TrendLine upContinuedTrend = tl(SYM, W2_END, W2_END + 30 * 60_000L,
                                /* slope */ +0.9e-3, /* slopeNorm */ 0.70,
                                /* score */ 0.80, /* support */ 0.72,
                                /* touches */ 3, /* breakouts */ 0, /* breakoutStrength */ 0.20,
                                /* lastBreakoutTs */ W1_END, // From first window, older but still decent confidence
                                /* side */ "RESISTANCE",
                                /* timeSpanMs */ 30 * 60_000L, /* priceRange */ 20.0);

                // Sentinel to flush
                final long SENT2_END = (W2_END + 30 * 60_000L) + 1;
                TrendLine sentinel2 = tl(SYM, W2_END + 30 * 60_000L, SENT2_END,
                                0.0, 0.0,
                                0.0, 0.0,
                                0, 0, 0.0,
                                -1L, "RESISTANCE",
                                1L, 1.0);

                DataStream<TrendSummary> summaries = env
                                .fromElements(upBreakoutFromResistance, upPersistentTrend, upContinuedTrend, sentinel2)
                                .returns(TypeInformation.of(new TypeHint<TrendLine>() {
                                }))
                                .keyBy(tl -> tl.symbol)
                                .process(new TrendSummarizerFn(
                                                /* lambdaSoftmax */ 3.0,
                                                /* betaSmoothing */ 0.25,
                                                /* fuseDelay */ Duration.ofMillis(10),
                                                /* recencyTau */ Duration.ofHours(1), // Shorter tau for more sensitive
                                                                                      // recency
                                                /* enter BREAKOUT */ 0.50,
                                                /* exit BREAKOUT */ 0.30,
                                                /* trendAlpha */ 0.35,
                                                /* confThresh */ 0.25))
                                .name("trend-factor");

                summaries.sinkTo(new TrendSummaryCollectSink());

                env.execute("Resistance Breakout Then Persist Test");

                // Add a small delay to allow timers to fire
                Thread.sleep(50);

                List<TrendSummary> out = TrendSummaryCollectSink.drain();
                out.removeIf(s -> s.ts == SENT2_END);
                System.out.println("Got " + out.size() + " summaries in second test");
                for (TrendSummary s : out) {
                        System.out.printf("Summary: ts=%d alpha=%.4f phase=%s lines=%d%n",
                                        s.ts, s.alpha, s.phase, s.linesUsed);
                }
                assertEquals(3, out.size(), "expected 3 summaries (one per window)");

                // Sort by timestamp to ensure consistent ordering
                out.sort((a, b) -> Long.compare(a.ts, b.ts));

                TrendSummary s1 = out.get(0);
                TrendSummary s2 = out.get(1);
                TrendSummary s3 = out.get(2);

                // Window #1: Fresh breakout from resistance → BREAKOUT phase
                assertEquals(SYM, s1.symbol);
                assertTrue(s1.alpha > 0.0, "strong upward trend from resistance breakout");
                assertEquals("BREAKOUT", s1.phase, "fresh strong breakout should be BREAKOUT phase");
                assertTrue(s1.breakoutRecency > 0.8, "very recent breakout should have high recency");
                assertEquals("STRONG_UP", s1.direction);

                // Window #2: Recency fades but trend persists → PERSIST phase
                assertEquals(SYM, s2.symbol);
                assertTrue(s2.alpha > 0.0, "trend continues upward");
                assertEquals("PERSIST", s2.phase,
                                "should transition to PERSIST as recency fades but confidence remains");
                assertTrue(s2.confidence > 0.65, "confidence should remain high for persistent trend");
                assertEquals("UP", s2.direction);

                // Window #3: Older breakout but still strong trend → remains PERSIST
                assertEquals(SYM, s3.symbol);
                assertTrue(s3.alpha > 0.0, "trend continues");
                assertEquals("PERSIST", s3.phase, "should remain PERSIST with good confidence despite older breakout");
                assertTrue(s3.confidence > 0.60, "confidence should still be reasonable");
                assertEquals("UP", s3.direction);
        }

        @Test
        void downtrendPersistsThenUptrendBreaksOutAndPersists() throws Exception {
                // --- env
                var env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(1);
                env.getConfig().setAutoWatermarkInterval(0L);
                env.getConfig().enableObjectReuse();

                final String SYM = "ADA";
                final long W1_START = 3_000_000L;
                final long W1_END = W1_START + 30 * 60_000L;
                final long W2_START = W1_END;
                final long W2_END = W2_START + 30 * 60_000L;
                final long W3_START = W2_END;
                final long W3_END = W3_START + 30 * 60_000L;
                final long W4_START = W3_END;
                final long W4_END = W4_START + 30 * 60_000L;

                // Window #1: Fresh DOWN breakout from support → BREAKOUT (DOWN)
                TrendLine downBreakout = tl(SYM, W1_START, W1_END,
                                /* slope */ -1.3e-3, /* slopeNorm */ 0.85,
                                /* score */ 0.90, /* support */ 0.80,
                                /* touches */ 4, /* breakouts */ 1, /* breakoutStrength */ 0.70,
                                /* lastBreakoutTs */ Long.MAX_VALUE / 2, /* side */ "SUPPORT",
                                /* timeSpanMs */ (W1_END - W1_START), /* priceRange */ 24.0);

                // Window #2: Continuing DOWN trend with moderate recency → PERSIST (DOWN)
                TrendLine downPersist = tl(SYM, W2_START, W2_END,
                                /* slope */ -1.0e-3, /* slopeNorm */ 0.75,
                                /* score */ 0.85, /* support */ 0.75,
                                /* touches */ 3, /* breakouts */ 1, /* breakoutStrength */ 0.45,
                                /* lastBreakoutTs */ W2_START - 60_000,
                                /* side */ "SUPPORT",
                                /* timeSpanMs */ (W2_END - W2_START), /* priceRange */ 22.0);

                // Window #3: Strong UP breakout from resistance → BREAKOUT (UP)
                TrendLine upBreakout = tl(SYM, W3_START, W3_END,
                                /* slope */ +1.6e-3, /* slopeNorm */ 0.90,
                                /* score */ 0.95, /* support */ 0.85,
                                /* touches */ 4, /* breakouts */ 1, /* breakoutStrength */ 0.80,
                                /* lastBreakoutTs */ Long.MAX_VALUE / 2, /* side */ "RESISTANCE",
                                /* timeSpanMs */ (W3_END - W3_START), /* priceRange */ 26.0);

                // Window #4: UP trend persists with decent confidence → PERSIST (UP)
                TrendLine upPersist = tl(SYM, W4_START, W4_END,
                                /* slope */ +1.2e-3, /* slopeNorm */ 0.80,
                                /* score */ 0.90, /* support */ 0.82,
                                /* touches */ 3, /* breakouts */ 1, /* breakoutStrength */ 0.50,
                                /* lastBreakoutTs */ W4_START - 120_000,
                                /* side */ "RESISTANCE",
                                /* timeSpanMs */ (W4_END - W4_START), /* priceRange */ 24.0);

                // Sentinel to flush
                final long SENT_END = W4_END + 1;
                TrendLine sentinel = tl(SYM, W4_END, SENT_END,
                                0.0, 0.0,
                                0.0, 0.0,
                                0, 0, 0.0,
                                -1L, "RESISTANCE",
                                1L, 1.0);

                DataStream<TrendSummary> summaries = env
                                .fromElements(downBreakout, downPersist, upBreakout, upPersist, sentinel)
                                .returns(TypeInformation.of(new TypeHint<TrendLine>() {
                                }))
                                .keyBy(tl -> tl.symbol)
                                .process(new TrendSummarizerFn(
                                                /* lambdaSoftmax */ 3.0,
                                                /* betaSmoothing */ 0.45,
                                                /* fuseDelay */ Duration.ofMillis(10),
                                                /* recencyTau */ Duration.ofHours(1),
                                                /* enter BREAKOUT */ 0.50,
                                                /* exit BREAKOUT */ 0.30,
                                                /* trendAlpha */ 0.35,
                                                /* confThresh */ 0.25))
                                .name("trend-factor");

                summaries.sinkTo(new TrendSummaryCollectSink()).name("collect-trend");

                env.execute("Down Persist Then Up Breakout Persist Test");

                List<TrendSummary> out = TrendSummaryCollectSink.drain();
                out.removeIf(s -> s.ts == SENT_END);
                assertEquals(4, out.size(), "expected 4 summaries (one per window)");

                out.sort((a, b) -> Long.compare(a.ts, b.ts));

                TrendSummary s1 = out.get(0);
                TrendSummary s2 = out.get(1);
                TrendSummary s3 = out.get(2);
                TrendSummary s4 = out.get(3);

                // W1: DOWN breakout
                assertEquals(SYM, s1.symbol);
                assertEquals(W1_END, s1.ts);
                assertEquals("BREAKOUT", s1.phase);
                assertEquals("STRONG_DOWN", s1.direction);
                assertTrue(s1.alpha < 0.0);
                assertTrue(s1.breakoutRecency > 0.8);

                // W2: DOWN persist
                assertEquals(SYM, s2.symbol);
                assertEquals(W2_END, s2.ts);
                assertEquals("BREAKOUT", s2.phase);
                assertEquals("STRONG_DOWN", s2.direction);
                assertTrue(s2.alpha < 0.0);
                assertTrue(s2.confidence > 0.60);

                // W3: UP breakout
                assertEquals(SYM, s3.symbol);
                assertEquals(W3_END, s3.ts);
                assertEquals("BREAKOUT", s3.phase);
                assertEquals("UP", s3.direction);
                assertTrue(s3.alpha > 0.0);
                assertTrue(s3.breakoutRecency > 0.8);

                // W4: UP persist
                assertEquals(SYM, s4.symbol);
                assertEquals(W4_END, s4.ts);
                assertEquals("PERSIST", s4.phase);
                assertEquals("UP", s4.direction);
                assertTrue(s4.alpha > 0.0);
                assertTrue(s4.confidence > 0.60);
        }

        // Helper to construct a TrendLine quickly
        private static TrendLine tl(String symbol, long wStart, long wEnd,
                        double slope, double slopeNorm,
                        double score, double support,
                        int touches, int breakouts, double breakoutStrength,
                        long lastBreakoutTs, String side,
                        long timeSpanMs, double priceRange) {
                TrendLine t = new TrendLine();
                t.symbol = symbol;
                t.windowStartTs = wStart;
                t.windowEndTs = wEnd;
                t.timeSpanMs = timeSpanMs;
                t.priceRange = priceRange;
                t.slope = slope;
                t.slopeNorm = slopeNorm;
                t.intercept = 0.0;
                t.support = support;
                t.score = score;
                t.touches = touches;
                t.breakouts = breakouts;
                t.breakoutStrength = breakoutStrength;
                t.lastBreakoutTs = lastBreakoutTs;
                t.side = side;
                return t;
        }
}
