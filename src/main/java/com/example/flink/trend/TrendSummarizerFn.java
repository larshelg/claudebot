package com.example.flink.trend;

import com.example.flink.trend.TrendLineDetectorJob.TrendLine;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.*;

/**
 * Fuses TrendLine rows per (symbol, windowEndTs) into one TrendSummary:
 * - alpha in [-1,1], confidence in [0,1] (with EMA smoothing)
 * - phase: BREAKOUT / PERSIST / NONE (with hysteresis)
 * - breakout metadata: lastBreakoutTs, breakoutRecency, breakoutStrength
 */
public class TrendSummarizerFn extends KeyedProcessFunction<String, TrendLine, TrendSummarizerFn.TrendSummary> {

    // -------- Public output POJO --------
    public static class TrendSummary implements Serializable {
        public String symbol;
        public long ts; // windowEndTs
        public double alpha; // [-1, 1]
        public double confidence; // [0, 1]
        public String phase; // BREAKOUT | PERSIST | NONE
        public String direction; // "STRONG_UP", "WEAK_UP", "SIDEWAYS", "WEAK_DOWN", "STRONG_DOWN"
        public String breakoutDirection; // "UP", "DOWN", or null if no recent breakout
        public long lastBreakoutTs; // -1 if none
        public double breakoutRecency; // [0,1] = exp(-age/τ)
        public double breakoutStrength; // [0,1]
        public int linesUsed; // diagnostics
    }

    // -------- Config --------
    private final double lambdaSoftmax; // softmax temperature on conf (e.g., 3.0)
    private final double betaSmoothing; // EMA smoothing (e.g., 0.3)
    private final long fuseDelayMs; // to gather all lines for the same window (e.g., 10ms)
    private final long recencyTauMs; // exp-decay horizon for breakout recency (e.g., 2h)

    // Phase thresholds (hysteresis)
    private final double phaseEnterBreakout; // e.g., 0.50
    private final double phaseExitBreakout; // e.g., 0.30

    // Optional trend gating hints (used only for choosing PERSIST vs NONE based on
    // alpha/conf)
    private final double trendAlphaThresh; // e.g., 0.40
    private final double confThresh; // e.g., 0.30

    // -------- State --------
    private transient MapState<Long, List<TrendLine>> byWindow; // windowEndTs -> lines
    private transient ValueState<Double> prevAlpha;
    private transient ValueState<Double> prevConf;
    private transient ValueState<String> prevPhase; // for hysteresis
    private transient ValueState<Long> lastProcessedWindow; // for immediate processing mode

    public TrendSummarizerFn(
            double lambdaSoftmax,
            double betaSmoothing,
            Duration fuseDelay,
            Duration recencyTau,
            // phase config (you can expose setters or overload a ctor if you like)
            double phaseEnterBreakout,
            double phaseExitBreakout,
            double trendAlphaThresh,
            double confThresh) {
        this.lambdaSoftmax = lambdaSoftmax;
        this.betaSmoothing = betaSmoothing;
        this.fuseDelayMs = fuseDelay.toMillis();
        this.recencyTauMs = recencyTau.toMillis();
        this.phaseEnterBreakout = phaseEnterBreakout;
        this.phaseExitBreakout = phaseExitBreakout;
        this.trendAlphaThresh = trendAlphaThresh;
        this.confThresh = confThresh;
    }

    // Convenience ctor with sensible defaults
    public TrendSummarizerFn(double lambdaSoftmax, double betaSmoothing, Duration fuseDelay, Duration recencyTau) {
        this(lambdaSoftmax, betaSmoothing, fuseDelay, recencyTau,
                /* phaseEnterBreakout */ 0.50,
                /* phaseExitBreakout */ 0.30,
                /* trendAlphaThresh */ 0.40,
                /* confThresh */ 0.30);
    }

    @Override
    public void open(Configuration parameters) {
        byWindow = getRuntimeContext().getMapState(new MapStateDescriptor<>(
                "linesByWindow", Long.class, (Class<List<TrendLine>>) (Class<?>) List.class));
        prevAlpha = getRuntimeContext().getState(new ValueStateDescriptor<>("prevAlpha", Double.class));
        prevConf = getRuntimeContext().getState(new ValueStateDescriptor<>("prevConf", Double.class));
        prevPhase = getRuntimeContext().getState(new ValueStateDescriptor<>("prevPhase", String.class));
        lastProcessedWindow = getRuntimeContext()
                .getState(new ValueStateDescriptor<>("lastProcessedWindow", Long.class));
    }

    @Override
    public void processElement(TrendLine tl, Context ctx, Collector<TrendSummary> out) throws Exception {
        List<TrendLine> list = byWindow.get(tl.windowEndTs);
        if (list == null)
            list = new ArrayList<>();
        list.add(tl);
        byWindow.put(tl.windowEndTs, list);

        // Always use timer-based processing. For test-friendly runs (tiny fuseDelay),
        // use a minimal delay based on event time
        // to guarantee onTimer scheduling occurs before the bounded job finishes.
        long delay = (fuseDelayMs <= 10) ? 1L : fuseDelayMs;
        long fireAt = tl.windowEndTs + delay;
        ctx.timerService().registerEventTimeTimer(fireAt);

        // Additionally, process all but the most recent window immediately so earlier
        // windows emit
        if (fuseDelayMs <= 10) {
            processWindowsImmediately(ctx, out);
        }
    }

    private void processWindowsImmediately(Context ctx, Collector<TrendSummary> out) throws Exception {
        // In immediate mode, we need to be smart about when to process windows
        // Strategy: process older windows when we see a newer window arrive

        // Find all windows and sort them
        List<Long> allWindows = new ArrayList<>();
        for (Long winEnd : byWindow.keys()) {
            allWindows.add(winEnd);
        }
        allWindows.sort(Long::compareTo);

        // If we have multiple windows, process all but the most recent one
        // (the most recent might still be accumulating lines)
        if (allWindows.size() > 1) {
            for (int i = 0; i < allWindows.size() - 1; i++) {
                Long winEnd = allWindows.get(i);
                List<TrendLine> lines = byWindow.get(winEnd);
                if (lines != null && !lines.isEmpty()) {
                    // Use the window end time as event time for immediate processing
                    processWindow(winEnd, lines, winEnd, ctx, out);
                    byWindow.remove(winEnd);
                }
            }
        }
        // If there's only one window and we're finishing (no more elements expected),
        // we'll rely on the test to finish or the onTimer to fire
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<TrendSummary> out) throws Exception {
        List<Long> toRemove = new ArrayList<>();
        for (Long winEnd : byWindow.keys()) {
            List<TrendLine> lines = byWindow.get(winEnd);
            if (lines == null || lines.isEmpty())
                continue;

            processWindow(winEnd, lines, timestamp, ctx, out);
            toRemove.add(winEnd);
        }
        for (Long k : toRemove)
            byWindow.remove(k);
    }

    private void processWindow(Long winEnd, List<TrendLine> lines, long timestamp, Context ctx,
            Collector<TrendSummary> out) throws Exception {
        // Fuse all lines for this window (confidence decoupled from recency in
        // fuseLines)
        long now = timestamp; // event-time for recency decay
        Fusion fused = fuseLines(lines, now);

        // Smooth alpha/conf across windows
        Double pa = prevAlpha.value();
        Double pc = prevConf.value();
        if (pa == null) {
            pa = fused.alpha;
        }
        if (pc == null) {
            pc = fused.confidence;
        }
        double alphaSm = (1 - betaSmoothing) * pa + betaSmoothing * fused.alpha;
        double confSm = Math.max(pc * (1 - betaSmoothing), fused.confidence);

        prevAlpha.update(alphaSm);
        prevConf.update(confSm);

        // Phase with hysteresis
        String phasePrev = prevPhase.value();
        String phaseNow = decidePhase(phasePrev, fused.breakoutRecency, alphaSm, confSm);
        prevPhase.update(phaseNow);

        // Emit summary
        TrendSummary s = new TrendSummary();
        s.symbol = ctx.getCurrentKey();
        s.ts = winEnd;
        s.alpha = clamp(alphaSm, -1, 1);
        s.confidence = clamp(confSm, 0, 1);
        s.phase = phaseNow;
        // More nuanced direction based on alpha strength
        if (alphaSm > 0.3) {
            s.direction = "STRONG_UP";
        } else if (alphaSm > 0.1) {
            s.direction = "WEAK_UP";
        } else if (alphaSm >= -0.1) {
            s.direction = "SIDEWAYS";
        } else if (alphaSm >= -0.3) {
            s.direction = "WEAK_DOWN";
        } else {
            s.direction = "STRONG_DOWN";
        }

        // Determine breakout direction from trend line slopes at breakout
        if (fused.breakoutRecency > 0.1 && fused.lastBreakoutTs > 0) {
            // Use alpha sign at breakout to determine direction
            s.breakoutDirection = (alphaSm >= 0.0) ? "UP" : "DOWN";
        } else {
            s.breakoutDirection = null; // No recent breakout
        }

        s.lastBreakoutTs = fused.lastBreakoutTs;
        s.breakoutRecency = clamp(fused.breakoutRecency, 0, 1);
        s.breakoutStrength = clamp(fused.breakoutStrength, 0, 1);
        s.linesUsed = lines.size();
        out.collect(s);
    }

    // ----- internals -----

    private static double clamp(double x, double lo, double hi) {
        return max(lo, min(hi, x));
    }

    /**
     * Decide phase with hysteresis on BREAKOUT; PERSIST requires decent trend &
     * conf; otherwise NONE.
     */
    private String decidePhase(String prev, double breakoutRecency, double alpha, double conf) {
        boolean strongTrend = conf >= confThresh && abs(alpha) >= trendAlphaThresh;
        boolean directionStable = (prev != null)
                && (alpha == 0.0 ? false : ("BREAKOUT".equals(prev) || "PERSIST".equals(prev)));

        if ("BREAKOUT".equals(prev)) {
            if (breakoutRecency >= phaseExitBreakout)
                return "BREAKOUT"; // stay until recency fades below exit
            // else fall through to PERSIST/NONE decision
        } else {
            // Enter BREAKOUT only if recency is high AND the fused trend is confident and
            // directional
            if (breakoutRecency >= phaseEnterBreakout && conf >= confThresh && Math.abs(alpha) >= trendAlphaThresh)
                return "BREAKOUT"; // fresh breakout with conviction
        }

        // Allow persistence on high confidence with directional continuity even if
        // alpha magnitude is modest
        if (strongTrend || (conf >= confThresh && directionStable))
            return "PERSIST";
        return "NONE";
    }

    /** Container for fused metrics. */
    private static final class Fusion {
        final double alpha, confidence;
        final long lastBreakoutTs;
        final double breakoutRecency;
        final double breakoutStrength;

        Fusion(double alpha, double confidence, long lastBreakoutTs, double breakoutRecency, double breakoutStrength) {
            this.alpha = alpha;
            this.confidence = confidence;
            this.lastBreakoutTs = lastBreakoutTs;
            this.breakoutRecency = breakoutRecency;
            this.breakoutStrength = breakoutStrength;
        }
    }

    /**
     * Softmax-blend multiple lines into alpha/conf + fused breakout meta.
     * Option A: CONFIDENCE is decoupled from recency (wRec only affects ALPHA &
     * phase).
     */
    private Fusion fuseLines(List<TrendLine> lines, long nowMs) {
        int n = lines.size();
        double[] alpha = new double[n];
        double[] conf = new double[n];
        double[] w = new double[n];

        long lastTsMax = -1L;
        double[] rec = new double[n]; // breakout recency per line (0..1)
        double[] brkStr = new double[n]; // breakout strength per line (0..1)

        for (int i = 0; i < n; i++) {
            TrendLine tl = lines.get(i);

            int dir = tl.slope >= 0 ? +1 : -1;

            double slopeNorm = clamp(tl.slopeNorm, 0, 1);
            double wQual = clamp(tl.score, 0, 1);
            double wBreak = tl.breakouts > 0 ? 1.0 : 0.6;
            double wRec = (tl.lastBreakoutTs > 0)
                    ? Math.exp(-(double) Math.max(0L, nowMs - tl.lastBreakoutTs) / Math.max(1.0, recencyTauMs))
                    : 0.0; // no breakout -> 0 recency (used for phase, not conf)

            double bStrength = clamp(tl.breakoutStrength, 0, 1);

            // Directional conviction with recency weighting
            // Give lines without breakouts some base weight, but lines with recent
            // breakouts get boosted
            double recencyBoost = (tl.lastBreakoutTs > 0) ? wRec : 1.0;
            double lineAlpha = dir
                    * slopeNorm
                    * (wQual * wBreak)
                    * (0.5 + 0.5 * bStrength)
                    * recencyBoost;

            // CONFIDENCE is now decoupled from recency (Option A)
            double lineConf = wQual
                    * (0.5 + 0.5 * slopeNorm);

            alpha[i] = lineAlpha;
            conf[i] = clamp(lineConf, 0, 1);

            // breakout meta aggregation
            if (tl.lastBreakoutTs > 0 && tl.lastBreakoutTs > lastTsMax)
                lastTsMax = tl.lastBreakoutTs;
            rec[i] = wRec;
            brkStr[i] = bStrength;
        }

        // softmax weights over confidence
        double sumExp = 0.0;
        for (double c : conf)
            sumExp += Math.exp(lambdaSoftmax * c);
        for (int i = 0; i < n; i++)
            w[i] = Math.exp(lambdaSoftmax * conf[i]) / Math.max(1e-12, sumExp);

        // blend
        double alphaBlend = 0.0, confBlend = 0.0, recBlend = 0.0, brkStrBlend = 0.0;
        for (int i = 0; i < n; i++) {
            alphaBlend += w[i] * alpha[i];
            confBlend += w[i] * conf[i];
            recBlend = Math.max(recBlend, rec[i]); // MAX: “is a breakout fresh?”
            brkStrBlend += w[i] * brkStr[i]; // weighted avg strength
        }

        return new Fusion(
                alphaBlend,
                clamp(confBlend, 0, 1),
                lastTsMax,
                clamp(recBlend, 0, 1),
                clamp(brkStrBlend, 0, 1));
    }

}
