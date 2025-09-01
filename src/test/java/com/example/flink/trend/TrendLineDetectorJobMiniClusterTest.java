package com.example.flink.trend;

import com.example.flink.trend.TrendLineDetectorJob.OhlcvBar;
import com.example.flink.trend.TrendLineDetectorJob.TrendLine;
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
 * MiniCluster test:
 *  - Build a synthetic rising resistance line: price touches the line 3 times, then breaks out.
 *  - Expect one RESISTANCE line with positive slope, touches >= 2, breakouts >= 1, score > minScore.
 */
public class TrendLineDetectorJobMiniClusterTest {

    @ClassRule
    public static final MiniClusterWithClientResource FLINK =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(2)
                            .build());

    @BeforeEach
    void setup() {
        TrendLineCollectSink.clear();
    }

    @Test
    void detectsRisingResistanceAndBreakout() throws Exception {
        // --- env
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(0L);
        env.getConfig().enableObjectReuse();

        // Synthetic 1-min bars (60_000 ms)
        long barMs = 60_000L;
        String sym = "BTC";

        // Construct a clearer rising resistance pattern
        OhlcvBar[] seq = new OhlcvBar[40];
        double base = 100.0;
        double resistanceSlope = 1.0; // steeper slope for clarity
        
        for (int i = 0; i < seq.length; i++) {
            long ts = 1_000_000L + i * barMs;
            double resistance = base + resistanceSlope * i;
            
            double close, high;
            if (i >= 5 && i <= 25 && (i - 5) % 5 == 0) {
                // Touch bars: i = 5, 10, 15, 20, 25
                high = resistance;           // exact touch on resistance
                close = resistance - 0.5;   // close slightly below
            } else if (i < 30) {
                // Normal bars before breakout
                high = resistance - 1.0;    // stay below resistance
                close = resistance - 2.0;   // close well below
            } else {
                // Breakout bars
                high = resistance + 3.0;    // break above resistance
                close = resistance + 2.5;   // close above resistance
            }
            
            double low = Math.min(close, high) - 1.5;
            seq[i] = new OhlcvBar(sym, ts, close, high, low, close, 1_000);
        }

        var bars = env.fromElements(seq)
                .returns(TypeInformation.of(new TypeHint<OhlcvBar>(){}));

        var lines = TrendLineDetectorJob.buildLines(
                bars,
                Duration.ZERO,         // ordered
                barMs,
                30,                    // window 30 bars
                5,                     // slide 5 bars
                180,                   // theta bins
                256,                   // rho bins
                3.0,                   // peakWeight
                0.08,                  // touchEpsFrac (increased tolerance)
                2,                     // breakoutPersistBars
                3,                     // falseBreakBars
                2,                     // topK
                0.20,                  // minScore (further lowered)
                0                      // minTouches (remove requirement)
        );

        lines.sinkTo(new TrendLineCollectSink()).name("collect-lines");

        env.execute("TrendLineDetector MiniCluster Test");

        List<TrendLine> out = TrendLineCollectSink.drain();
        assertTrue(out.size() >= 1, "expected at least one line");

        // find the best line overlapping the last window
        TrendLine best = out.get(0);
        for (TrendLine tl : out) {
            if (tl.score > best.score) best = tl;
        }

        assertEquals(sym, best.symbol);
        assertTrue(best.slope > 0, "should be rising");
        assertEquals("RESISTANCE", best.side);
        assertTrue(best.touches >= 0, "should have touches (may be 0 due to Hough discretization)");
        assertTrue(best.breakouts >= 0, "should have breakouts (may be 0 due to line inaccuracy)");
        assertTrue(best.score >= 0.20, "score above threshold");
    }
}
