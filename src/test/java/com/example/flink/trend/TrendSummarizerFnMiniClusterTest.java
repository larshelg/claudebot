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
 * Verifies TrendSummarizerFn:
 * - Within a window: softmax-blend favors the higher-confidence line (sign of
 * alpha).
 * - Across windows: smoothing dampens flips (alpha changes sign but magnitude
 * is moderated).
 */
public class TrendSummarizerFnMiniClusterTest {

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
        void softmaxBlendAndSmoothingWork() throws Exception {
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

                // --- Window #1: two lines (uptrend stronger than downtrend) ---
                TrendLine upStrong = tl(SYM, W1_START, W1_END,
                                /* slope */ +1.0e-3, /* slopeNorm */ 0.60,
                                /* score */ 0.80, /* support */ 0.70,
                                /* touches */ 3, /* breakouts */ 1, /* breakoutStrength */ 0.30,
                                /* lastBreakoutTs */ W1_END - 60_000, /* side */ "RESISTANCE",
                                /* timeSpanMs */ (W1_END - W1_START), /* priceRange */ 20.0);

                TrendLine downWeaker = tl(SYM, W1_START, W1_END,
                                /* slope */ -8.0e-4, /* slopeNorm */ 0.40,
                                /* score */ 0.60, /* support */ 0.50,
                                /* touches */ 2, /* breakouts */ 0, /* breakoutStrength */ 0.10,
                                /* lastBreakoutTs */ -1, /* side */ "SUPPORT",
                                /* timeSpanMs */ (W1_END - W1_START), /* priceRange */ 20.0);

                // --- Window #2: a strong downtrend line to flip direction (smoothing should
                // dampen) ---
                TrendLine downStrong = tl(SYM, W2_START, W2_END,
                                /* slope */ -1.2e-3, /* slopeNorm */ 0.70,
                                /* score */ 0.70, /* support */ 0.65,
                                /* touches */ 3, /* breakouts */ 1, /* breakoutStrength */ 0.25,
                                /* lastBreakoutTs */ W2_END - 30_000, /* side */ "SUPPORT",
                                /* timeSpanMs */ (W2_END - W2_START), /* priceRange */ 18.0);

                // Sentinel window to flush previous windows deterministically
                final long SENT_END = W2_END + 1; // strictly greater than last real window
                TrendLine sentinel = tl(SYM, W2_END, SENT_END,
                                /* slope */ 0.0, /* slopeNorm */ 0.0,
                                /* score */ 0.0, /* support */ 0.0,
                                /* touches */ 0, /* breakouts */ 0, /* breakoutStrength */ 0.0,
                                /* lastBreakoutTs */ -1L, /* side */ "RESISTANCE",
                                /* timeSpanMs */ 1L, /* priceRange */ 1.0);

                // feed them in order; summarizer will fuse per windowEndTs
                DataStream<TrendSummary> summaries = env
                                .fromElements(upStrong, downWeaker, downStrong, sentinel)
                                .returns(TypeInformation.of(new TypeHint<TrendLine>() {
                                }))
                                // keyBy symbol so summarizer keeps per-symbol state
                                .keyBy(tl -> tl.symbol)
                                .process(new TrendSummarizerFn(
                                                /* lambdaSoftmax */ 3.0,
                                                /* betaSmoothing */ 0.30,
                                                /* fuseDelay */ Duration.ofMillis(10),
                                                /* recencyTau */ Duration.ofHours(2)))
                                .name("trend-factor");

                summaries.sinkTo(new TrendSummaryCollectSink());

                env.execute("TrendSummarizer MiniCluster Test");

                List<TrendSummary> out = TrendSummaryCollectSink.drain();
                // Drop sentinel window
                out.removeIf(s -> s.ts == SENT_END);

                // Sort summaries by timestamp for consistent test ordering

                // Expect two summaries (one per window)
                assertEquals(2, out.size(), "expected 2 trend summaries (one per window)");

                // Sort by timestamp to ensure consistent ordering
                out.sort((a, b) -> Long.compare(a.ts, b.ts));

                TrendSummary s1 = out.get(0);
                TrendSummary s2 = out.get(1);

                // --- Assertions for Window #1 (blend should lean UP) ---
                assertEquals(SYM, s1.symbol);
                assertEquals(W1_END, s1.ts);
                assertTrue(s1.alpha > 0.0, "alpha should be positive (uptrend favored)");
                assertTrue(s1.confidence > 0.0 && s1.confidence <= 1.0, "confidence must be (0,1]");
                assertTrue(s1.linesUsed >= 2, "first window should fuse two lines");

                // --- Assertions for Window #2 (flip to DOWN but smoothed) ---
                assertEquals(SYM, s2.symbol);
                assertEquals(W2_END, s2.ts);
                // Allow small positive alpha when smoothing dominates, otherwise expect
                // negative
                double a2 = s2.alpha;
                assertTrue(a2 < 0.0 || a2 <= 0.10,
                                "alpha should flip negative or remain near-zero positive under smoothing");
                // smoothing should damp the flip; we don't require a specific magnitude, just
                // sanity
                assertTrue(Math.abs(a2) < 0.8, "smoothed magnitude should be reasonable (<0.8)");
                assertTrue(s2.confidence > 0.0 && s2.confidence <= 1.0);
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
                t.intercept = 0.0; // not used in summarizer
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
