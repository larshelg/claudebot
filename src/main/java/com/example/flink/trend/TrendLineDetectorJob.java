package com.example.flink.trend;

import com.example.flink.domain.Candle;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
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
 * Detects trend lines using a weighted (ρ, θ) Hough transform per symbol, in
 * sliding windows.
 * - Peaks from highs/lows are detected and weighted higher in Hough voting
 * - Non-max suppression picks top-K lines
 * - Each line is walked against bars to tag TOUCH / BREAKOUT / FALSE_BREAK /
 * THROWBACK events
 * - Lines are scored and filtered; high-quality lines are emitted
 *
 * Side output: TrendLineEvent (per event/bar).
 */
// ... package + imports unchanged ...

public final class TrendLineDetectorJob {

    private TrendLineDetectorJob() {
    }

    public static final OutputTag<TrendLineEvent> TREND_EVENTS = new OutputTag<>("trend-line-events") {
    };

    public static SingleOutputStreamOperator<TrendLine> buildLines(
            DataStream<Candle> bars,
            Duration lateness,
            long barMillis,
            int windowSizeBars,
            int slideBars,
            int thetaBins,
            int rhoBins,
            double peakWeight,
            double touchEpsFrac,
            int breakoutPersistBars,
            int falseBreakBars,
            int topK,
            double minScore,
            int minTouches) {
        var withTs = bars.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Candle>forBoundedOutOfOrderness(lateness)
                        .withTimestampAssigner((b, ts) -> b.timestamp));

        var windowSizeMs = windowSizeBars * barMillis;
        var slideMs = slideBars * barMillis;

        return withTs
                .keyBy((KeySelector<Candle, String>) b -> b.symbol)
                .window(SlidingEventTimeWindows.of(Time.milliseconds(windowSizeMs), Time.milliseconds(slideMs)))
                .process(new HoughTrendFn(
                        thetaBins, rhoBins, peakWeight, touchEpsFrac, breakoutPersistBars, falseBreakBars, topK,
                        minScore, minTouches))
                .name("HoughTrendLines");
    }

    /**
     * Variant that fuses bars with externally detected swing points via coGroup.
     */
    public static SingleOutputStreamOperator<TrendLine> buildLinesWithSwings(
            DataStream<Candle> bars,
            DataStream<com.example.flink.indicators.SwingPointDetectorJob.SwingPoint> swings,
            Duration lateness,
            long barMillis,
            int windowSizeBars,
            int slideBars,
            int thetaBins,
            int rhoBins,
            double peakWeight,
            double touchEpsFrac,
            int breakoutPersistBars,
            int falseBreakBars,
            int topK,
            double minScore,
            int minTouches) {
        var barsWithTs = bars.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Candle>forBoundedOutOfOrderness(lateness)
                        .withTimestampAssigner((b, ts) -> b.timestamp));

        var swingsWithTs = swings.assignTimestampsAndWatermarks(
                WatermarkStrategy.<com.example.flink.indicators.SwingPointDetectorJob.SwingPoint>forBoundedOutOfOrderness(
                        lateness)
                        .withTimestampAssigner((s, ts) -> s.ts));

        var windowSizeMs = windowSizeBars * barMillis;
        var slideMs = slideBars * barMillis;

        DataStream<TrendBundle> bundles = barsWithTs
                .coGroup(swingsWithTs)
                .where((KeySelector<Candle, String>) b -> b.symbol)
                .equalTo(
                        (KeySelector<com.example.flink.indicators.SwingPointDetectorJob.SwingPoint, String>) s -> s.symbol)
                .window(SlidingEventTimeWindows.of(Time.milliseconds(windowSizeMs), Time.milliseconds(slideMs)))
                .apply(new HoughTrendCoGroupFn(
                        thetaBins, rhoBins, peakWeight, touchEpsFrac, breakoutPersistBars, falseBreakBars, topK,
                        minScore, minTouches));

        return bundles
                .process(new ProcessFunction<TrendBundle, TrendLine>() {
                    @Override
                    public void processElement(TrendBundle bundle, Context ctx, Collector<TrendLine> out) {
                        for (TrendLine tl : bundle.lines) {
                            out.collect(tl);
                        }
                        for (TrendLineEvent ev : bundle.events) {
                            ctx.output(TREND_EVENTS, ev);
                        }
                    }
                })
                .name("TrendBundleUnpack");
    }

    static final class HoughTrendFn extends ProcessWindowFunction<Candle, TrendLine, String, TimeWindow> {
        private final int thetaBins, rhoBins, topK, breakoutPersist, falseBreakBars, minTouches;
        private final double peakWeight, touchEpsFrac, minScore;
        private final double thetaMin = toRadians(10.0);
        private final double thetaMax = toRadians(170.0);

        HoughTrendFn(int thetaBins, int rhoBins, double peakWeight, double touchEpsFrac,
                int breakoutPersist, int falseBreakBars, int topK, double minScore, int minTouches) {
            this.thetaBins = thetaBins;
            this.rhoBins = rhoBins;
            this.peakWeight = peakWeight;
            this.touchEpsFrac = touchEpsFrac;
            this.breakoutPersist = breakoutPersist;
            this.falseBreakBars = falseBreakBars;
            this.topK = topK;
            this.minScore = minScore;
            this.minTouches = minTouches;
        }

        @Override
        public void process(String symbol, Context ctx, Iterable<Candle> it, Collector<TrendLine> out) {
            List<Candle> bars = new ArrayList<>();
            for (Candle b : it)
                bars.add(b);
            if (bars.size() < 10)
                return;
            bars.sort(Comparator.comparingLong(b -> b.timestamp));

            long t0 = bars.get(0).timestamp;
            long t1 = bars.get(bars.size() - 1).timestamp;
            double T = Math.max(1.0, (double) (t1 - t0)); // ΔT

            double minP = Double.POSITIVE_INFINITY, maxP = Double.NEGATIVE_INFINITY;
            for (Candle b : bars) {
                if (b.low < minP)
                    minP = b.low;
                if (b.high > maxP)
                    maxP = b.high;
            }
            double P = Math.max(1e-9, maxP - minP); // ΔP
            double epsAbs = touchEpsFrac * P;

            // === (unchanged) peaks, Hough accumulator, NMS → picks ===
            // ... [same as your code up to computing m & b] ...

            // 3) precompute trig and accumulator
            double[] thetas = new double[thetaBins];
            double[] cos = new double[thetaBins];
            double[] sin = new double[thetaBins];
            for (int i = 0; i < thetaBins; i++) {
                double th = thetaMin + (i + 0.5) * (thetaMax - thetaMin) / thetaBins;
                thetas[i] = th;
                cos[i] = Math.cos(th);
                sin[i] = Math.sin(th);
            }
            double rhoMin = -Math.sqrt(2.0), rhoMax = Math.sqrt(2.0);
            double drho = (rhoMax - rhoMin) / rhoBins;
            double[][] acc = new double[thetaBins][rhoBins];

            // voting
            boolean[] highPeak = new boolean[bars.size()];
            boolean[] lowPeak = new boolean[bars.size()];
            for (int i = 1; i < bars.size() - 1; i++) {
                if (bars.get(i).high >= bars.get(i - 1).high && bars.get(i).high >= bars.get(i + 1).high)
                    highPeak[i] = true;
                if (bars.get(i).low <= bars.get(i - 1).low && bars.get(i).low <= bars.get(i + 1).low)
                    lowPeak[i] = true;
            }
            for (int i = 0; i < bars.size(); i++) {
                Candle b = bars.get(i);
                double xNorm = (b.timestamp - t0) / T;
                double[] ys = new double[] { (b.high - minP) / P, (b.low - minP) / P };
                boolean[] isPeak = new boolean[] { highPeak[i], lowPeak[i] };
                for (int yi = 0; yi < ys.length; yi++) {
                    double yNorm = ys[yi];
                    double w = isPeak[yi] ? peakWeight : 1.0;
                    for (int ti = 0; ti < thetaBins; ti++) {
                        double rho = xNorm * cos[ti] + yNorm * sin[ti];
                        int r = (int) floor((rho - rhoMin) / drho);
                        if (r >= 0 && r < rhoBins) {
                            acc[ti][r] += w;
                            if (r + 1 < rhoBins)
                                acc[ti][r + 1] += w * 0.25;
                            if (r - 1 >= 0)
                                acc[ti][r - 1] += w * 0.25;
                        }
                    }
                }
            }

            List<Cell> cells = new ArrayList<>(thetaBins * rhoBins);
            double maxVote = 0.0;
            for (int ti = 0; ti < thetaBins; ti++) {
                for (int ri = 0; ri < rhoBins; ri++) {
                    double v = acc[ti][ri];
                    if (v > 0) {
                        cells.add(new Cell(ti, ri, v));
                        if (v > maxVote)
                            maxVote = v;
                    }
                }
            }
            if (cells.isEmpty())
                return;
            cells.sort((a, b) -> Double.compare(b.v, a.v));

            int suppressTheta = Math.max(1, thetaBins / 90);
            int suppressRho = Math.max(1, rhoBins / 90);
            boolean[][] suppressed = new boolean[thetaBins][rhoBins];
            List<Cell> picks = new ArrayList<>(topK);
            for (Cell c : cells) {
                if (picks.size() >= topK)
                    break;
                if (suppressed[c.t][c.r])
                    continue;
                picks.add(c);
                for (int dt = -suppressTheta; dt <= suppressTheta; dt++) {
                    int tt = c.t + dt;
                    if (tt < 0 || tt >= thetaBins)
                        continue;
                    for (int dr = -suppressRho; dr <= suppressRho; dr++) {
                        int rr = c.r + dr;
                        if (rr < 0 || rr >= rhoBins)
                            continue;
                        suppressed[tt][rr] = true;
                    }
                }
            }

            List<TrendLine> emitted = new ArrayList<>();

            List<TrendLineEvent> eventsCollected = new ArrayList<>();
            for (Cell c : picks) {
                double th = thetas[c.t];
                double sn = sin[c.t], cs = cos[c.t];
                if (abs(sn) < 1e-6)
                    continue;

                double rho = rhoMin + (c.r + 0.5) * drho;

                // y_norm = a + b * x_norm
                double a_norm = rho / sn;
                double b_norm = -cs / sn;

                // y_orig = m * t + b
                double m = (P * b_norm) / T; // price per ms
                double b = minP + P * (a_norm - b_norm * (-(double) t0 / T));

                // classify & features (now returns lastBreakoutTs)
                Features feat = evaluateLineShared(bars, m, b, epsAbs, P, breakoutPersist, falseBreakBars);

                double voteScore = (maxVote > 0) ? (c.v / maxVote) : 0.0;
                double slopeScore = Math.min(1.0, abs(m) * (T / P));
                double score = 0.45 * voteScore
                        + 0.25 * norm01(feat.touches, 0, bars.size() / 3)
                        + 0.20 * Math.min(1.0, feat.breakoutStrength)
                        + 0.15 * slopeScore
                        - 0.25 * Math.min(1.0, feat.falseBreaks / 3.0);

                if (feat.touches >= minTouches && score >= minScore) {
                    TrendLine tl = new TrendLine();
                    tl.symbol = symbol;
                    tl.windowStartTs = ctx.window().getStart();
                    tl.windowEndTs = ctx.window().getEnd();
                    tl.timeSpanMs = (long) T; // NEW: ΔT
                    tl.priceRange = P; // NEW: ΔP
                    tl.slope = m;
                    tl.intercept = b;
                    tl.support = voteScore;
                    tl.score = Math.max(0, Math.min(1.0, score));
                    tl.touches = feat.touches;
                    tl.breakouts = feat.breakouts;
                    tl.breakoutStrength = feat.breakoutStrength;
                    tl.lastBreakoutTs = feat.lastBreakoutTs; // NEW
                    tl.side = feat.side;
                    // NEW: slopeNorm ready-to-use
                    tl.slopeNorm = Math.min(1.0, Math.abs(tl.slope) * (tl.timeSpanMs / Math.max(1e-9, tl.priceRange)));

                    emitted.add(tl);

                    for (TrendLineEvent ev : feat.events) {
                        ev.symbol = symbol;
                        ev.slope = m;
                        ev.intercept = b;
                        ctx.output(TREND_EVENTS, ev);
                    }
                }
            }

            for (TrendLine tl : dedupLines(emitted))
                out.collect(tl);
        }

    }

    // === CoGroup variant using external swings ===
    static final class HoughTrendCoGroupFn
            implements
            CoGroupFunction<Candle, com.example.flink.indicators.SwingPointDetectorJob.SwingPoint, TrendBundle> {

        private final int thetaBins, rhoBins, topK, breakoutPersist, falseBreakBars, minTouches;
        private final double peakWeight, touchEpsFrac, minScore;
        private final double thetaMin = toRadians(10.0);
        private final double thetaMax = toRadians(170.0);

        HoughTrendCoGroupFn(int thetaBins, int rhoBins, double peakWeight, double touchEpsFrac,
                int breakoutPersist, int falseBreakBars, int topK, double minScore, int minTouches) {
            this.thetaBins = thetaBins;
            this.rhoBins = rhoBins;
            this.peakWeight = peakWeight;
            this.touchEpsFrac = touchEpsFrac;
            this.breakoutPersist = breakoutPersist;
            this.falseBreakBars = falseBreakBars;
            this.topK = topK;
            this.minScore = minScore;
            this.minTouches = minTouches;
        }

        @Override
        public void coGroup(Iterable<Candle> barsIt,
                Iterable<com.example.flink.indicators.SwingPointDetectorJob.SwingPoint> swingsIt,
                Collector<TrendBundle> out) {

            // collect & sort bars
            List<Candle> bars = new ArrayList<>();
            for (Candle b : barsIt)
                bars.add(b);
            if (bars.size() < 10)
                return;
            bars.sort(Comparator.comparingLong(b -> b.timestamp));

            // collect swings
            Set<Long> swingHighTs = new HashSet<>();
            Set<Long> swingLowTs = new HashSet<>();
            for (var s : swingsIt) {
                if ("HIGH".equalsIgnoreCase(s.type))
                    swingHighTs.add(s.ts);
                else if ("LOW".equalsIgnoreCase(s.type))
                    swingLowTs.add(s.ts);
            }

            long t0 = bars.get(0).timestamp;
            long t1 = bars.get(bars.size() - 1).timestamp;
            double T = Math.max(1.0, (double) (t1 - t0));

            double minP = Double.POSITIVE_INFINITY, maxP = Double.NEGATIVE_INFINITY;
            for (Candle b : bars) {
                if (b.low < minP)
                    minP = b.low;
                if (b.high > maxP)
                    maxP = b.high;
            }
            double P = Math.max(1e-9, maxP - minP);
            double epsAbs = touchEpsFrac * P;

            // precompute
            double[] thetas = new double[thetaBins];
            double[] cos = new double[thetaBins];
            double[] sin = new double[thetaBins];
            for (int i = 0; i < thetaBins; i++) {
                double th = thetaMin + (i + 0.5) * (thetaMax - thetaMin) / thetaBins;
                thetas[i] = th;
                cos[i] = Math.cos(th);
                sin[i] = Math.sin(th);
            }
            double rhoMin = -Math.sqrt(2.0), rhoMax = Math.sqrt(2.0);
            double drho = (rhoMax - rhoMin) / rhoBins;
            double[][] acc = new double[thetaBins][rhoBins];

            // voting with swing weighting
            for (Candle b : bars) {
                double xNorm = (b.timestamp - t0) / T;
                double[] ys = new double[] { (b.high - minP) / P, (b.low - minP) / P };
                double wHigh = swingHighTs.contains(b.timestamp) ? peakWeight : 1.0;
                double wLow = swingLowTs.contains(b.timestamp) ? peakWeight : 1.0;

                for (int ti = 0; ti < thetaBins; ti++) {
                    double rho = xNorm * cos[ti] + ys[0] * sin[ti];
                    int r = (int) floor((rho - rhoMin) / drho);
                    if (r >= 0 && r < rhoBins) {
                        acc[ti][r] += wHigh;
                        if (r + 1 < rhoBins)
                            acc[ti][r + 1] += wHigh * 0.25;
                        if (r - 1 >= 0)
                            acc[ti][r - 1] += wHigh * 0.25;
                    }
                }
                for (int ti = 0; ti < thetaBins; ti++) {
                    double rho = xNorm * cos[ti] + ys[1] * sin[ti];
                    int r = (int) floor((rho - rhoMin) / drho);
                    if (r >= 0 && r < rhoBins) {
                        acc[ti][r] += wLow;
                        if (r + 1 < rhoBins)
                            acc[ti][r + 1] += wLow * 0.25;
                        if (r - 1 >= 0)
                            acc[ti][r - 1] += wLow * 0.25;
                    }
                }
            }

            List<Cell> cells = new ArrayList<>(thetaBins * rhoBins);
            double maxVote = 0.0;
            for (int ti = 0; ti < thetaBins; ti++) {
                for (int ri = 0; ri < rhoBins; ri++) {
                    double v = acc[ti][ri];
                    if (v > 0) {
                        cells.add(new Cell(ti, ri, v));
                        if (v > maxVote)
                            maxVote = v;
                    }
                }
            }
            if (cells.isEmpty())
                return;
            cells.sort((a, b) -> Double.compare(b.v, a.v));

            int suppressTheta = Math.max(1, thetaBins / 90);
            int suppressRho = Math.max(1, rhoBins / 90);
            boolean[][] suppressed = new boolean[thetaBins][rhoBins];
            List<Cell> picks = new ArrayList<>(topK);
            for (Cell c : cells) {
                if (picks.size() >= topK)
                    break;
                if (suppressed[c.t][c.r])
                    continue;
                picks.add(c);
                for (int dt = -suppressTheta; dt <= suppressTheta; dt++) {
                    int tt = c.t + dt;
                    if (tt < 0 || tt >= thetaBins)
                        continue;
                    for (int dr = -suppressRho; dr <= suppressRho; dr++) {
                        int rr = c.r + dr;
                        if (rr < 0 || rr >= rhoBins)
                            continue;
                        suppressed[tt][rr] = true;
                    }
                }
            }

            List<TrendLine> emitted = new ArrayList<>();
            List<TrendLineEvent> eventsCollected = new ArrayList<>();
            for (Cell c : picks) {
                double th = thetas[c.t];
                double sn = sin[c.t], cs = cos[c.t];
                if (abs(sn) < 1e-6)
                    continue;

                double rho = rhoMin + (c.r + 0.5) * drho;
                double a_norm = rho / sn;
                double b_norm = -cs / sn;
                double m = (P * b_norm) / T;
                double b = minP + P * (a_norm - b_norm * (-(double) t0 / T));

                Features feat = evaluateLineShared(bars, m, b, epsAbs, P, breakoutPersist, falseBreakBars);

                double voteScore = (maxVote > 0) ? (c.v / maxVote) : 0.0;
                double slopeScore = Math.min(1.0, abs(m) * (T / P));
                double score = 0.45 * voteScore
                        + 0.25 * norm01(feat.touches, 0, bars.size() / 3)
                        + 0.20 * Math.min(1.0, feat.breakoutStrength)
                        + 0.15 * slopeScore
                        - 0.25 * Math.min(1.0, feat.falseBreaks / 3.0);

                if (feat.touches >= minTouches && score >= minScore) {
                    TrendLine tl = new TrendLine();
                    tl.symbol = bars.get(0).symbol;
                    tl.windowStartTs = t0;
                    tl.windowEndTs = t1;
                    tl.timeSpanMs = (long) T;
                    tl.priceRange = P;
                    tl.slope = m;
                    tl.intercept = b;
                    tl.support = voteScore;
                    tl.score = Math.max(0, Math.min(1.0, score));
                    tl.touches = feat.touches;
                    tl.breakouts = feat.breakouts;
                    tl.breakoutStrength = feat.breakoutStrength;
                    tl.lastBreakoutTs = feat.lastBreakoutTs;
                    tl.side = feat.side;
                    tl.slopeNorm = Math.min(1.0, Math.abs(tl.slope) * (tl.timeSpanMs / Math.max(1e-9, tl.priceRange)));
                    emitted.add(tl);
                    eventsCollected.addAll(feat.events);
                }
            }

            List<TrendLine> dedup = dedupLines(emitted);
            out.collect(new TrendBundle(dedup, eventsCollected));
        }
    }

    // Bundle type passed between the two stages
    static final class TrendBundle {
        final List<TrendLine> lines;
        final List<TrendLineEvent> events;

        TrendBundle(List<TrendLine> lines, List<TrendLineEvent> events) {
            this.lines = lines;
            this.events = events;
        }
    }

    // ---- Shared helpers (visible to both variants) ----
    static final class Cell {
        final int t, r;
        final double v;

        Cell(int t, int r, double v) {
            this.t = t;
            this.r = r;
            this.v = v;
        }
    }

    static final class Features {
        final String side;
        final int touches, breakouts, falseBreaks;
        final double breakoutStrength;
        final long lastBreakoutTs;
        final List<TrendLineEvent> events;

        Features(String side, int touches, int breakouts, int falseBreaks, double breakoutStrength, long lastBreakoutTs,
                List<TrendLineEvent> events) {
            this.side = side;
            this.touches = touches;
            this.breakouts = breakouts;
            this.falseBreaks = falseBreaks;
            this.breakoutStrength = breakoutStrength;
            this.lastBreakoutTs = lastBreakoutTs;
            this.events = events;
        }
    }

    private static double norm01(double x, double lo, double hi) {
        if (hi <= lo)
            return 0.0;
        return Math.max(0.0, Math.min(1.0, (x - lo) / (hi - lo)));
    }

    private static List<TrendLine> dedupLines(List<TrendLine> ls) {
        if (ls.size() <= 1)
            return ls;
        ls.sort((a, b) -> Double.compare(b.score, a.score));
        List<TrendLine> out = new ArrayList<>();
        boolean[] used = new boolean[ls.size()];
        double dm = 1e-8, db = 1e-3;
        for (int i = 0; i < ls.size(); i++) {
            if (used[i])
                continue;
            TrendLine keep = ls.get(i);
            out.add(keep);
            for (int j = i + 1; j < ls.size(); j++) {
                if (used[j])
                    continue;
                TrendLine o = ls.get(j);
                if (abs(keep.slope - o.slope) < dm && abs(keep.intercept - o.intercept) < db) {
                    used[j] = true;
                }
            }
        }
        return out;
    }

    private static Features evaluateLineShared(List<Candle> bars, double m, double b, double epsAbs, double P,
            int breakoutPersist, int falseBreakBars) {
        int touches = 0, breakouts = 0, falseBreaks = 0;
        double maxBreakDist = 0.0;
        long lastBreakoutTs = -1L;

        int nearHigh = 0, nearLow = 0;
        double sumCloseMinusLine = 0.0;
        for (Candle bar : bars) {
            double lineP = m * bar.timestamp + b;
            if (abs(bar.high - lineP) <= epsAbs)
                nearHigh++;
            if (abs(bar.low - lineP) <= epsAbs)
                nearLow++;
            sumCloseMinusLine += (bar.close - lineP);
        }
        String side;
        if (nearHigh > nearLow) {
            side = "RESISTANCE";
        } else if (nearLow > nearHigh) {
            side = "SUPPORT";
        } else {
            // Tie-breaker: use average sign of (close - line) if it shows a clear bias,
            // otherwise fall back to slope sign (m > 0 => SUPPORT, else RESISTANCE)
            double avgDelta = sumCloseMinusLine / Math.max(1, bars.size());
            double biasThresh = epsAbs * 0.25; // require a modest average separation to bias
            if (avgDelta > biasThresh) {
                side = "SUPPORT";
            } else if (avgDelta < -biasThresh) {
                side = "RESISTANCE";
            } else {
                side = (m > 0.0) ? "SUPPORT" : "RESISTANCE";
            }
        }

        List<TrendLineEvent> events = new ArrayList<>();
        int persist = 0;
        int lastSign = 0;
        boolean inBreak = false;
        long breakStartTs = 0;

        for (Candle bar : bars) {
            double lineP = m * bar.timestamp + b;

            double touchPrice = side.equals("RESISTANCE") ? bar.high : bar.low;
            double touchD = touchPrice - lineP;
            boolean isTouch = Math.abs(touchD) <= epsAbs;

            double d = bar.close - lineP;
            int sign;
            if (d > epsAbs)
                sign = +1;
            else if (d < -epsAbs)
                sign = -1;
            else
                sign = 0;

            if (isTouch) {
                touches++;
                events.add(TrendLineEvent.touch(bar.timestamp, touchD / epsAbs, touchPrice, lineP));
                if (inBreak) {
                    events.add(TrendLineEvent.throwback(bar.timestamp, touchD / epsAbs, touchPrice, lineP));
                }
                persist = 0;
            } else {
                if (side.equals("RESISTANCE")) {
                    if (lastSign <= 0 && sign > 0) {
                        persist++;
                        if (!inBreak && persist >= breakoutPersist) {
                            inBreak = true;
                            breakouts++;
                            breakStartTs = bar.timestamp;
                            lastBreakoutTs = bar.timestamp;
                            events.add(TrendLineEvent.breakout(bar.timestamp, d / epsAbs, bar.close, lineP));
                        }
                    } else
                        persist = 0;
                } else { // SUPPORT
                    if (lastSign >= 0 && sign < 0) {
                        persist++;
                        if (!inBreak && persist >= breakoutPersist) {
                            inBreak = true;
                            breakouts++;
                            breakStartTs = bar.timestamp;
                            lastBreakoutTs = bar.timestamp;
                            events.add(TrendLineEvent.breakout(bar.timestamp, d / epsAbs, bar.close, lineP));
                        }
                    } else
                        persist = 0;
                }
                if (inBreak) {
                    maxBreakDist = Math.max(maxBreakDist, Math.abs(d) / Math.max(1e-9, P));
                }
            }

            if (inBreak) {
                boolean reverted = (side.equals("RESISTANCE") && sign <= 0)
                        || (side.equals("SUPPORT") && sign >= 0);
                if (reverted && (bar.timestamp - breakStartTs) <= (long) falseBreakBars
                        * (bars.get(1).timestamp - bars.get(0).timestamp)) {
                    falseBreaks++;
                    events.add(TrendLineEvent.falseBreak(bar.timestamp, d / epsAbs, bar.close, lineP));
                    inBreak = false;
                }
            }
            lastSign = sign;
        }

        // simple breakout strength proxy
        double breakoutStrength = maxBreakDist;
        return new Features(side, touches, breakouts, falseBreaks, breakoutStrength, lastBreakoutTs, events);
    }

    public static class TrendLine implements Serializable {
        public String symbol;
        public long windowStartTs, windowEndTs;
        public long timeSpanMs; // NEW: ΔT of window
        public double priceRange; // NEW: ΔP of window
        public double slope; // price per millisecond
        public double slopeNorm; // NEW: |slope| * (ΔT/ΔP), clamped to [0,1]
        public double intercept;
        public double support;
        public double score;
        public int touches;
        public int breakouts;
        public double breakoutStrength;
        public long lastBreakoutTs; // NEW: -1 if none
        public String side; // SUPPORT or RESISTANCE

        @Override
        public String toString() {
            return "TrendLine{" +
                    "symbol='" + symbol + '\'' +
                    ", side=" + side +
                    ", slope=" + String.format("%.6f", slope) +
                    ", intercept=" + String.format("%.2f", intercept) +
                    ", touches=" + touches +
                    ", breakouts=" + breakouts +
                    ", breakoutStrength=" + String.format("%.3f", breakoutStrength) +
                    ", score=" + String.format("%.3f", score) +
                    ", support=" + String.format("%.3f", support) +
                    ", window=[" + windowStartTs + "," + windowEndTs + "]" +
                    '}';
        }
    }

    public static class TrendLineEvent implements Serializable {
        public String symbol;
        public long ts;
        public String state; // TOUCH | BREAKOUT | FALSE_BREAK | THROWBACK
        public double distanceToLine;
        public double price, linePrice;
        public double slope, intercept;

        @Override
        public String toString() {
            return "TrendEvent{" +
                    "symbol='" + symbol + '\'' +
                    ", state=" + state +
                    ", ts=" + ts +
                    ", price=" + String.format("%.2f", price) +
                    ", linePrice=" + String.format("%.2f", linePrice) +
                    ", dist=" + String.format("%.3f", distanceToLine) +
                    '}';
        }

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
            e.state = state;
            e.ts = ts;
            e.distanceToLine = dist;
            e.price = price;
            e.linePrice = line;
            return e;
        }
    }
}
