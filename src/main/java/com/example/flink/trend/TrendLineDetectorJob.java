package com.example.flink.trend;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.time.Duration;
import java.util.*;

import static java.lang.Math.*;

/**
 * TrendLineDetectorJob
 *
 * Detects trend lines using a weighted (ρ, θ) Hough transform per symbol, in sliding windows.
 * - Peaks from highs/lows are detected and weighted higher in Hough voting
 * - Non-max suppression picks top-K lines
 * - Each line is walked against bars to tag TOUCH / BREAKOUT / FALSE_BREAK / THROWBACK events
 * - Lines are scored and filtered; high-quality lines are emitted
 *
 * Side output: TrendLineEvent (per event/bar).
 */
public final class TrendLineDetectorJob {

    private TrendLineDetectorJob() {}

    /** Events side output (optional to consume). */
    public static final OutputTag<TrendLineEvent> TREND_EVENTS =
            new OutputTag<>("trend-line-events") {};

    /** Wire the job: returns the stream of best lines; events are on side output TREND_EVENTS. */
    public static DataStream<TrendLine> buildLines(
            DataStream<OhlcvBar> bars,
            Duration lateness,
            long barMillis,
            int windowSizeBars,
            int slideBars,
            int thetaBins,
            int rhoBins,
            double peakWeight,       // e.g., 3.0
            double touchEpsFrac,     // fraction of window price range, e.g., 0.02 (2%)
            int breakoutPersistBars, // e.g., 2
            int falseBreakBars,      // e.g., 3
            int topK,                // e.g., 2
            double minScore,         // e.g., 0.35
            int minTouches           // e.g., 2
    ) {
        var withTs = bars.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OhlcvBar>forBoundedOutOfOrderness(lateness)
                        .withTimestampAssigner((b, ts) -> b.ts));

        var windowSizeMs = windowSizeBars * barMillis;
        var slideMs = slideBars * barMillis;

        return withTs
                .keyBy((KeySelector<OhlcvBar, String>) b -> b.symbol)
                .window(SlidingEventTimeWindows.of(Time.milliseconds(windowSizeMs), Time.milliseconds(slideMs)))
                .process(new HoughTrendFn(
                        thetaBins, rhoBins, peakWeight, touchEpsFrac, breakoutPersistBars, falseBreakBars, topK, minScore, minTouches
                ))
                .name("HoughTrendLines");
    }

    // ========= Operator =========

    static final class HoughTrendFn extends ProcessWindowFunction<OhlcvBar, TrendLine, String, TimeWindow> {
        private final int thetaBins, rhoBins, topK, breakoutPersist, falseBreakBars, minTouches;
        private final double peakWeight, touchEpsFrac, minScore;

        private final double thetaMin = toRadians(10.0);
        private final double thetaMax = toRadians(170.0);

        HoughTrendFn(int thetaBins, int rhoBins, double peakWeight, double touchEpsFrac,
                     int breakoutPersist, int falseBreakBars, int topK, double minScore, int minTouches) {
            this.thetaBins = thetaBins; this.rhoBins = rhoBins; this.peakWeight = peakWeight;
            this.touchEpsFrac = touchEpsFrac; this.breakoutPersist = breakoutPersist;
            this.falseBreakBars = falseBreakBars; this.topK = topK;
            this.minScore = minScore; this.minTouches = minTouches;
        }

        @Override
        public void process(String symbol, Context ctx, Iterable<OhlcvBar> it, Collector<TrendLine> out) {
            // 1) collect & sort bars
            List<OhlcvBar> bars = new ArrayList<>();
            for (OhlcvBar b : it) bars.add(b);
            if (bars.size() < 10) return;
            bars.sort(Comparator.comparingLong(b -> b.ts));

            long t0 = bars.get(0).ts;
            long t1 = bars.get(bars.size() - 1).ts;
            double T = Math.max(1.0, (double) (t1 - t0)); // time span

            double minP = Double.POSITIVE_INFINITY, maxP = Double.NEGATIVE_INFINITY;
            for (OhlcvBar b : bars) {
                if (b.low < minP) minP = b.low;
                if (b.high > maxP) maxP = b.high;
            }
            double P = Math.max(1e-9, maxP - minP); // price range
            double epsAbs = touchEpsFrac * P;

            // 2) detect peaks (simple 3-point test on highs and lows)
            boolean[] highPeak = new boolean[bars.size()];
            boolean[] lowPeak  = new boolean[bars.size()];
            for (int i = 1; i < bars.size() - 1; i++) {
                if (bars.get(i).high >= bars.get(i - 1).high && bars.get(i).high >= bars.get(i + 1).high) highPeak[i] = true;
                if (bars.get(i).low  <= bars.get(i - 1).low  && bars.get(i).low  <= bars.get(i + 1).low ) lowPeak[i]  = true;
            }

            // 3) precompute trig and accumulator
            double[] thetas = new double[thetaBins];
            double[] cos = new double[thetaBins];
            double[] sin = new double[thetaBins];
            for (int i = 0; i < thetaBins; i++) {
                double th = thetaMin + (i + 0.5) * (thetaMax - thetaMin) / thetaBins;
                thetas[i] = th; cos[i] = Math.cos(th); sin[i] = Math.sin(th);
            }
            // ρ ∈ [ -sqrt(2), +sqrt(2) ] in normalized square
            double rhoMin = -Math.sqrt(2.0), rhoMax = Math.sqrt(2.0);
            double drho = (rhoMax - rhoMin) / rhoBins;

            double[][] acc = new double[thetaBins][rhoBins];

            // 4) voting — normalized coordinates
            for (int i = 0; i < bars.size(); i++) {
                OhlcvBar b = bars.get(i);
                double xNorm = (b.ts - t0) / T;
                // Use both high & low points as samples (weight peaks)
                double[] ys = new double[]{ (b.high - minP)/P, (b.low - minP)/P };
                boolean[] isPeak = new boolean[]{ highPeak[i], lowPeak[i] };

                for (int yi = 0; yi < ys.length; yi++) {
                    double yNorm = ys[yi];
                    double w = isPeak[yi] ? peakWeight : 1.0;

                    for (int ti = 0; ti < thetaBins; ti++) {
                        double rho = xNorm * cos[ti] + yNorm * sin[ti];
                        int r = (int) floor((rho - rhoMin) / drho);
                        if (r >= 0 && r < rhoBins) {
                            acc[ti][r] += w;
                            // tiny smoothing in rho-space:
                            if (r + 1 < rhoBins) acc[ti][r + 1] += w * 0.25;
                            if (r - 1 >= 0)      acc[ti][r - 1] += w * 0.25;
                        }
                    }
                }
            }

            // 5) find peaks via NMS, build candidate lines
            List<Cell> cells = new ArrayList<>(thetaBins * rhoBins);
            double maxVote = 0.0;
            for (int ti = 0; ti < thetaBins; ti++) {
                for (int ri = 0; ri < rhoBins; ri++) {
                    double v = acc[ti][ri];
                    if (v > 0) {
                        cells.add(new Cell(ti, ri, v));
                        if (v > maxVote) maxVote = v;
                    }
                }
            }
            if (cells.isEmpty()) return;
            cells.sort((a, b) -> Double.compare(b.v, a.v));

            int suppressTheta = Math.max(1, thetaBins / 90); // ~2° neighborhood
            int suppressRho   = Math.max(1, rhoBins / 90);
            boolean[][] suppressed = new boolean[thetaBins][rhoBins];

            List<Cell> picks = new ArrayList<>(topK);
            for (Cell c : cells) {
                if (picks.size() >= topK) break;
                if (suppressed[c.t][c.r]) continue;
                picks.add(c);
                // suppress neighbors
                for (int dt = -suppressTheta; dt <= suppressTheta; dt++) {
                    int tt = c.t + dt;
                    if (tt < 0 || tt >= thetaBins) continue;
                    for (int dr = -suppressRho; dr <= suppressRho; dr++) {
                        int rr = c.r + dr;
                        if (rr < 0 || rr >= rhoBins) continue;
                        suppressed[tt][rr] = true;
                    }
                }
            }

            List<TrendLine> emitted = new ArrayList<>();
            for (Cell c : picks) {
                double th = thetas[c.t];
                double sn = sin[c.t], cs = cos[c.t];
                if (abs(sn) < 1e-6) continue; // guard

                double rho = rhoMin + (c.r + 0.5) * drho;

                // y_norm = a + b * x_norm
                double a_norm = rho / sn;
                double b_norm = -cs / sn;

                // Convert to original coords: y_orig = m * t + b
                double m = (P * b_norm) / T; // price per ms
                double b = minP + P * (a_norm - b_norm * (- (double) t0 / T)); // intercept at t=0

                // classify events along the window & compute features
                Features feat = evaluateLine(bars, m, b, epsAbs,P);

                // votes score normalized
                double voteScore = (maxVote > 0) ? (c.v / maxVote) : 0.0;
                double slopeScore = Math.min(1.0, abs(m) * (T / P)); // normalized slope magnitude
                // final score (tunable weights)
                double score = 0.45 * voteScore
                        + 0.25 * norm01(feat.touches, 0, bars.size()/3)
                        + 0.20 * Math.min(1.0, feat.breakoutStrength)
                        + 0.15 * slopeScore
                        - 0.25 * Math.min(1.0, feat.falseBreaks / 3.0);

                String side = feat.side; // SUPPORT/RESISTANCE inferred inside evaluateLine

                // Filter out lines that don't meet minimum requirements
                // (debug logging removed for cleaner output)


                if (feat.touches >= minTouches && score >= minScore) {
                    TrendLine tl = new TrendLine();
                    tl.symbol = symbol;
                    tl.windowStartTs = ctx.window().getStart();
                    tl.windowEndTs   = ctx.window().getEnd();
                    tl.slope = m;
                    tl.intercept = b;
                    tl.support = voteScore;
                    tl.score = Math.max(0, Math.min(1.0, score));
                    tl.touches = feat.touches;
                    tl.breakouts = feat.breakouts;
                    tl.breakoutStrength = feat.breakoutStrength;
                    tl.side = side;
                    emitted.add(tl);

                    // emit per-bar events (touch/breakout/etc.)
                    for (TrendLineEvent ev : feat.events) {
                        ev.symbol = symbol;
                        ev.slope = m; ev.intercept = b;
                        ctx.output(TREND_EVENTS, ev);
                    }
                }
            }

            // Deduplicate near-duplicates by slope/intercept proximity (keep highest score)
            List<TrendLine> dedup = dedupLines(emitted);
            for (TrendLine tl : dedup) out.collect(tl);
        }

        private static double norm01(double x, double lo, double hi) {
            if (hi <= lo) return 0.0;
            return Math.max(0.0, Math.min(1.0, (x - lo) / (hi - lo)));
        }

        private static List<TrendLine> dedupLines(List<TrendLine> ls) {
            if (ls.size() <= 1) return ls;
            ls.sort((a, b) -> Double.compare(b.score, a.score));
            List<TrendLine> out = new ArrayList<>();
            boolean[] used = new boolean[ls.size()];
            double dm = 1e-8;     // slope tolerance
            double db = 1e-3;     // price tolerance
            for (int i = 0; i < ls.size(); i++) {
                if (used[i]) continue;
                TrendLine keep = ls.get(i);
                out.add(keep);
                for (int j = i + 1; j < ls.size(); j++) {
                    if (used[j]) continue;
                    TrendLine o = ls.get(j);
                    if (abs(keep.slope - o.slope) < dm && abs(keep.intercept - o.intercept) < db) {
                        used[j] = true;
                    }
                }
            }
            return out;
        }

        // Evaluate line against bars: classify states & collect features
        private Features evaluateLine(List<OhlcvBar> bars, double m, double b, double epsAbs, double P) {
            int touches = 0, breakouts = 0, falseBreaks = 0;
            double maxBreakDist = 0.0;

            // Decide side using peaks near the line
            int nearHigh = 0, nearLow = 0;
            for (OhlcvBar bar : bars) {
                double lineP = m * bar.ts + b;
                if (abs(bar.high - lineP) <= epsAbs) nearHigh++;
                if (abs(bar.low  - lineP) <= epsAbs) nearLow++;
            }
            String side = (nearHigh >= nearLow) ? "RESISTANCE" : "SUPPORT";

            // Track transitions using close vs line
            List<TrendLineEvent> events = new ArrayList<>();
            int persist = 0;
            int lastSign = 0; // -1 below, 0 near, +1 above
            boolean inBreak = false;
            long breakStartTs = 0;

            for (OhlcvBar bar : bars) {
                double lineP = m * bar.ts + b;
                
                // For touches, use appropriate price level: high for resistance, low for support
                double touchPrice = side.equals("RESISTANCE") ? bar.high : bar.low;
                double touchD = touchPrice - lineP;
                boolean isTouch = Math.abs(touchD) <= epsAbs;
                
                // For breakouts, still use close price for trend determination
                double d = bar.close - lineP;
                int sign;
                if (d > epsAbs) sign = +1;
                else if (d < -epsAbs) sign = -1;
                else sign = 0;

                if (isTouch) {
                    touches++;
                    events.add(TrendLineEvent.touch(bar.ts, touchD/epsAbs, touchPrice, lineP));
                    // throwback detection: if recently broke out and returned near the line
                    if (inBreak) {
                        events.add(TrendLineEvent.throwback(bar.ts, touchD/epsAbs, touchPrice, lineP));
                    }
                }
                
                // Reset persist counter on touches for cleaner breakout detection
                if (isTouch) {
                    persist = 0;
                } else {
                    // breakout if crossing from opposite side and persist >= threshold
                    if (side.equals("RESISTANCE")) {
                        // meaningful breakout is crossing ABOVE
                        if (lastSign <= 0 && sign > 0) {
                            persist++;
                            if (!inBreak && persist >= breakoutPersist) {
                                inBreak = true; breakouts++;
                                breakStartTs = bar.ts;
                                events.add(TrendLineEvent.breakout(bar.ts, d/epsAbs, bar.close, lineP));
                            }
                        } else {
                            persist = 0;
                        }
                    } else { // SUPPORT: breakout is crossing BELOW
                        if (lastSign >= 0 && sign < 0) {
                            persist++;
                            if (!inBreak && persist >= breakoutPersist) {
                                inBreak = true; breakouts++;
                                breakStartTs = bar.ts;
                                events.add(TrendLineEvent.breakout(bar.ts, d/epsAbs, bar.close, lineP));
                            }
                        } else {
                            persist = 0;
                        }
                    }
                    if (inBreak) {
                        maxBreakDist = Math.max(maxBreakDist, Math.abs(d) / Math.max(1e-9, P)); // normalize by range
                    }
                }

                // false-break: breakout then cross back within N bars
                if (inBreak) {
                    boolean reverted = (side.equals("RESISTANCE") && sign <= 0)
                            || (side.equals("SUPPORT")   && sign >= 0);
                    if (reverted && (bar.ts - breakStartTs) <= (long)falseBreakBars * (bars.get(1).ts - bars.get(0).ts)) {
                        falseBreaks++;
                        events.add(TrendLineEvent.falseBreak(bar.ts, d/epsAbs, bar.close, lineP));
                        inBreak = false;
                    }
                }

                lastSign = sign;
            }

            return new Features(side, touches, breakouts, falseBreaks, maxBreakDist, events);
        }

        static final class Cell {
            final int t, r; final double v; Cell(int t, int r, double v) { this.t = t; this.r = r; this.v = v; }
        }
        static final class Features {
            final String side; final int touches, breakouts, falseBreaks; final double breakoutStrength;
            final List<TrendLineEvent> events;
            Features(String side, int touches, int breakouts, int falseBreaks, double breakoutStrength, List<TrendLineEvent> events) {
                this.side = side; this.touches = touches; this.breakouts = breakouts; this.falseBreaks = falseBreaks;
                this.breakoutStrength = breakoutStrength; this.events = events;
            }
        }
    }

    // ========= POJOs =========

    /** Input OHLCV bar. */
    public static class OhlcvBar implements Serializable {
        public String symbol; public long ts;
        public double open, high, low, close, volume;
        public OhlcvBar() {}
        public OhlcvBar(String symbol, long ts, double open, double high, double low, double close, double volume) {
            this.symbol = symbol; this.ts = ts; this.open = open; this.high = high; this.low = low; this.close = close; this.volume = volume;
        }
    }

    /** Emitted trend line (per window). */
    public static class TrendLine implements Serializable {
        public String symbol;
        public long windowStartTs, windowEndTs;
        public double slope;       // price per millisecond
        public double intercept;   // price at t=0 (epoch intercept)
        public double support;     // normalized vote strength 0..1
        public double score;       // final 0..1
        public int touches;
        public int breakouts;
        public double breakoutStrength; // normalized
        public String side;        // SUPPORT or RESISTANCE
    }

    /** Optional per-bar events on side output. */
    public static class TrendLineEvent implements Serializable {
        public String symbol; public long ts;
        public String state;  // TOUCH | BREAKOUT | FALSE_BREAK | THROWBACK
        public double distanceToLine; // normalized by eps
        public double price, linePrice;
        public double slope, intercept; // back-link

        public static TrendLineEvent touch(long ts, double dist, double price, double line) {
            return of("TOUCH", ts, dist, price, line);
        }
        public static TrendLineEvent breakout(long ts, double dist, double price, double line) {
            return of("BREAKOUT", ts, dist, price, line);
        }
        public static TrendLineEvent falseBreak(long ts, double dist, double price, double line) {
            return of("FALSE_BREAK", ts, dist, price, line);
        }
        public static TrendLineEvent throwback(long ts, double dist, double price, double line) {
            return of("THROWBACK", ts, dist, price, line);
        }
        private static TrendLineEvent of(String state, long ts, double dist, double price, double line) {
            TrendLineEvent e = new TrendLineEvent();
            e.state = state; e.ts = ts; e.distanceToLine = dist; e.price = price; e.linePrice = line;
            return e;
        }
    }
}
