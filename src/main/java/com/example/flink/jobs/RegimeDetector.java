package com.example.flink.jobs;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * RegimeDetector
 *
 * Input:  OHLCV bars (uniform bucket).  One row per (symbol, ts).
 * Output: Regime events per symbol (emitted only when regime changes).
 * Optional: StrategyRegimePref (static config) as a separate stream.
 *
 * Heuristics (simple, fast, explainable):
 *  - Realized Volatility (RV): stddev of log returns over window.
 *  - Trendiness (ADX-lite): average directional movement over window normalized by TR.
 *
 * Regime mapping:
 *   if RV > rvHigh     -> HIGHVOL
 *   else if RV < rvLow -> LOWVOL
 *   else if Trend >= trendHigh -> TREND
 *   else if Trend <= trendLow  -> MEANREV
 *   else -> CHAOS
 */
public final class RegimeDetector {

    private RegimeDetector() {}

    // ======== Public API ========

    /** Wire the job and return the regime stream (per-symbol). */
    public static DataStream<Regime> build(
            StreamExecutionEnvironment env,
            DataStream<OhlcvBar> ohlcv,
            Duration lateness,
            Time featureWindow,          // e.g., Time.minutes(60)
            double rvLow, double rvHigh, // realized vol cutoffs (e.g., 0.005, 0.03 for 0.5% / 3%)
            double trendLow, double trendHigh // trendiness cutoffs (e.g., 0.15, 0.35)
    ) {
        // Event-time and watermarking
        DataStream<OhlcvBar> withTs = ohlcv.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OhlcvBar>forBoundedOutOfOrderness(lateness)
                        .withTimestampAssigner((row, ts) -> row.ts)
        );

        // Compute features per symbol over tumbling window
        DataStream<Features> feats = withTs
                .keyBy((KeySelector<OhlcvBar, String>) b -> b.symbol)
                .window(TumblingEventTimeWindows.of(featureWindow))
                .process(new FeatureFn())
                .name("regime-features");

        // Map features to regime, emit only on change
        return feats
                .keyBy((KeySelector<Features, String>) f -> f.symbol)
                .process(new ClassifierFn(rvLow, rvHigh, trendLow, trendHigh))
                .name("regime-classifier");
    }

    /** Optional: static preferences per (strategyId, regime). */
    public static DataStream<StrategyRegimePref> staticPrefs(StreamExecutionEnvironment env) {
        // Seed the table here or read from a config/Fluss table in production.
        Map<String, Double> trendPrefs = Map.of(
                // in TREND regime
                "sma-xover:v1|TREND", 1.0,
                "meanrev:v2|TREND",   0.2
        );
        Map<String, Double> mrPrefs = Map.of(
                // in MEANREV regime
                "sma-xover:v1|MEANREV", 0.3,
                "meanrev:v2|MEANREV",   1.0
        );
        Map<String, Double> base = new HashMap<>();
        base.putAll(trendPrefs);
        base.putAll(mrPrefs);
        base.put("sma-xover:v1|HIGHVOL", 0.8);
        base.put("meanrev:v2|HIGHVOL",   0.4);
        base.put("sma-xover:v1|LOWVOL",  0.6);
        base.put("meanrev:v2|LOWVOL",    0.8);
        base.put("sma-xover:v1|CHAOS",   0.0);
        base.put("meanrev:v2|CHAOS",     0.0);

        StrategyRegimePref[] arr = base.entrySet().stream()
                .map(e -> {
                    String[] parts = e.getKey().split("\\|");
                    return new StrategyRegimePref(parts[0], parts[1], e.getValue(), System.currentTimeMillis());
                })
                .toArray(StrategyRegimePref[]::new);

        return env.fromElements(arr);
    }

    // ======== Feature computation ========

    /** Computes Features over a tumbling event-time window per symbol. */
    static final class FeatureFn extends org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction<OhlcvBar, Features, String, TimeWindow> {
        @Override
        public void process(String symbol, Context ctx, Iterable<OhlcvBar> bars, Collector<Features> out) {
            double prevClose = Double.NaN;
            double sum = 0.0, sumSq = 0.0;
            int nRet = 0;

            // Wilder's ADX-lite components
            double trSum = 0.0;      // true range sum
            double dmPlusSum = 0.0;  // positive directional movement
            double dmMinusSum = 0.0;
            double prevHigh = Double.NaN, prevLow = Double.NaN;

            long lastTs = ctx.window().getEnd();

            for (OhlcvBar b : bars) {
                // returns for realized vol
                if (!Double.isNaN(prevClose) && prevClose > 0.0 && b.close > 0.0) {
                    double r = Math.log(b.close / prevClose);
                    sum += r;
                    sumSq += r * r;
                    nRet++;
                }
                prevClose = b.close;

                // TR/DM for trendiness proxy
                if (!Double.isNaN(prevHigh)) {
                    double upMove = b.high - prevHigh;
                    double dnMove = prevLow - b.low;
                    if (upMove < 0) upMove = 0;
                    if (dnMove < 0) dnMove = 0;
                    if (upMove > dnMove) {
                        dmPlusSum += upMove;
                    } else if (dnMove > upMove) {
                        dmMinusSum += dnMove;
                    }
                    double tr = Math.max(b.high - b.low, Math.max(Math.abs(b.high - prevClose), Math.abs(b.low - prevClose)));
                    trSum += Math.max(tr, 0.0);
                }
                prevHigh = b.high;
                prevLow = b.low;
                lastTs = Math.max(lastTs, b.ts);
            }

            double rv = 0.0;
            if (nRet > 1) {
                double mean = sum / nRet;
                double var = Math.max(0, (sumSq / nRet) - mean * mean);
                rv = Math.sqrt(var); // realized vol (per bar)
            }

            double trendiness = 0.0;
            if (trSum > 1e-12) {
                double diPlus = dmPlusSum / trSum;
                double diMinus = dmMinusSum / trSum;
                trendiness = Math.abs(diPlus - diMinus); // 0..~1 proxy
            }

            out.collect(new Features(symbol, lastTs, rv, trendiness));
        }
    }

    // ======== Classifier + change emitter ========

    static final class ClassifierFn extends KeyedProcessFunction<String, Features, Regime> {
        private final double rvLow, rvHigh, trendLow, trendHigh;
        private transient ValueState<String> lastRegime;

        ClassifierFn(double rvLow, double rvHigh, double trendLow, double trendHigh) {
            this.rvLow = rvLow; this.rvHigh = rvHigh; this.trendLow = trendLow; this.trendHigh = trendHigh;
        }

        @Override
        public void open(Configuration parameters) {
            lastRegime = getRuntimeContext().getState(new ValueStateDescriptor<>("lastRegime", String.class));
        }

        @Override
        public void processElement(Features f, Context ctx, Collector<Regime> out) throws Exception {
            String curr;
            if (f.rv > rvHigh)       curr = "HIGHVOL";
            else if (f.rv < rvLow)   curr = "LOWVOL";
            else if (f.trend >= trendHigh) curr = "TREND";
            else if (f.trend <= trendLow)  curr = "MEANREV";
            else                      curr = "CHAOS";

            String prev = lastRegime.value();
            if (prev == null || !prev.equals(curr)) {
                lastRegime.update(curr);
               // out.collect(new Regime("SYMBOL", f.symbol, curr, f.ts));
            }
        }
    }

    // ======== POJOs ========

    /** Input OHLCV bar. Use your existing class if you have one. */
    public static class OhlcvBar implements Serializable {
        public String symbol; public long ts;
        public double open, high, low, close, volume;
        public OhlcvBar() {}
        public OhlcvBar(String symbol, long ts, double open, double high, double low, double close, double volume) {
            this.symbol = symbol; this.ts = ts; this.open=open; this.high=high; this.low=low; this.close=close; this.volume=volume;
        }
    }

    /** Computed features per window. */
    public static class Features implements Serializable {
        public String symbol; public long tsEnd; public double rv; public double trend;
        public Features() {}
        public Features(String symbol, long tsEnd, double rv, double trend) { this.symbol=symbol; this.tsEnd=tsEnd; this.rv=rv; this.trend=trend; }
    }

    /** Emitted regime event. */
    public static class Regime implements Serializable {
        public String scope;   // "SYMBOL" or "GLOBAL"
        public String symbol;  // set when scope == SYMBOL
        public String regime;  // "TREND","MEANREV","HIGHVOL","LOWVOL","CHAOS"
        public long ts;
        public Regime() {}
        public Regime(String scope, String symbol, String regime, long ts) { this.scope=scope; this.symbol=symbol; this.regime=regime; this.ts=ts; }
    }

    /** Preference weight per (strategyId, regime). */
    public static class StrategyRegimePref implements Serializable {
        public String strategyId; public String regime; public double weight; public long ts;
        public StrategyRegimePref() {}
        public StrategyRegimePref(String strategyId, String regime, double weight, long ts) {
            this.strategyId=strategyId; this.regime=regime; this.weight=weight; this.ts=ts;
        }
    }
}
