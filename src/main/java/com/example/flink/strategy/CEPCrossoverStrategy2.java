package com.example.flink.strategy;

import com.example.flink.IndicatorWithPrice;
import com.example.flink.domain.StrategySignal;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;

/**
 * CEP-based SMA crossover strategy.
 * Emits BUY on Golden Cross (sma5 crosses above sma21) and SELL on Death Cross).
 * NOTE: CEP user state is not supported; cooldown is enforced in a downstream keyed process.
 */
public class CEPCrossoverStrategy2 implements Serializable {

    private final String runId;               // e.g., "sma-xover:v1"
    private final Time window;                // max time between prev and curr
    private final Duration maxLateness;       // watermark lateness
    private final long cooldownMs;            // per-symbol debounce (applied after CEP)

    private final OutputTag<String> DEBUG = new OutputTag<String>("cep-debug") {};

    public CEPCrossoverStrategy2(String runId) {
        this(runId, Time.hours(1), Duration.ofSeconds(5), 0L);
    }
    public CEPCrossoverStrategy2(String runId, Time window, Duration maxLateness, long cooldownMs) {
        this.runId = runId;
        this.window = window;
        this.maxLateness = maxLateness;
        this.cooldownMs = cooldownMs;
    }

    public static boolean isGolden(IndicatorWithPrice v) { return v.sma5 > v.sma21; }
    public static boolean isNonGolden(IndicatorWithPrice v) { return v.sma5 <= v.sma21; }
    public static boolean isDeath(IndicatorWithPrice v) { return v.sma5 < v.sma21; }
    public static boolean isNonDeath(IndicatorWithPrice v) { return v.sma5 >= v.sma21; }

    /** Build stream of BUY/SELL StrategySignals from IndicatorWithPrice. */
    public DataStream<StrategySignal> apply(DataStream<IndicatorWithPrice> input) {

        // Robust watermarks: bounded out-of-orderness
        DataStream<IndicatorWithPrice> withTs =
                input.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<IndicatorWithPrice>forBoundedOutOfOrderness(maxLateness)
                                .withTimestampAssigner((ev, ts) -> ev.timestamp)
                );

        // Golden cross: not-golden -> golden (BUY)
        Pattern<IndicatorWithPrice, ?> golden =
                Pattern.<IndicatorWithPrice>begin("prev", AfterMatchSkipStrategy.skipPastLastEvent())
                        .where(new SimpleCondition<>() { @Override public boolean filter(IndicatorWithPrice v) { return isNonGolden(v); }})
                        .followedBy("curr")
                        .where(new SimpleCondition<>() { @Override public boolean filter(IndicatorWithPrice v) { return isGolden(v); }})
                        .within(window);

        // Death cross: not-death -> death (SELL)
        Pattern<IndicatorWithPrice, ?> death =
                Pattern.<IndicatorWithPrice>begin("prev", AfterMatchSkipStrategy.skipPastLastEvent())
                        .where(new SimpleCondition<>() { @Override public boolean filter(IndicatorWithPrice v) { return isNonDeath(v); }})
                        .followedBy("curr")
                        .where(new SimpleCondition<>() { @Override public boolean filter(IndicatorWithPrice v) { return isDeath(v); }})
                        .within(window);

        var buy = CEP.pattern(withTs.keyBy(ev -> ev.symbol), golden)
                .process(new EmitSignal(runId, "BUY", DEBUG))
                .name("golden-cross");

        var sell = CEP.pattern(withTs.keyBy(ev -> ev.symbol), death)
                .process(new EmitSignal(runId, "SELL", DEBUG))
                .name("death-cross");

        var rawSignals = buy.union(sell);

        // Apply per-symbol cooldown AFTER CEP (keyed state supported here)
        return (cooldownMs > 0)
                ? rawSignals.keyBy(s -> s.symbol).process(new CooldownFn(cooldownMs)).name("signals-cooled")
                : rawSignals;
    }

    /** Expose debug side-output to the caller if they want it in tests/logs. */
    public DataStream<String> debugOf(SingleOutputStreamOperator<StrategySignal> main) {
        return main.getSideOutput(DEBUG);
    }

    // --- CEP process fn (stateless) ---
    static final class EmitSignal extends PatternProcessFunction<IndicatorWithPrice, StrategySignal> {
        private final String runId;
        private final String direction; // BUY or SELL
        private final OutputTag<String> debugTag;

        EmitSignal(String runId, String direction, OutputTag<String> debugTag) {
            this.runId = runId;
            this.direction = direction;
            this.debugTag = debugTag;
        }

        @Override
        public void processMatch(Map<String, java.util.List<IndicatorWithPrice>> match, Context ctx, Collector<StrategySignal> out) {
            var prev = match.get("prev").get(0);
            var curr = match.get("curr").get(0);

            double strength = (curr.sma21 != 0.0)
                    ? Math.min(1.0, Math.abs(curr.sma5 - curr.sma21) / Math.abs(curr.sma21)) // 0..1
                    : 0.0;

            ctx.output(debugTag, String.format(
                    "XOVER %s %s: prev(%.4f/%.4f @%d) -> curr(%.4f/%.4f @%d) strength=%.4f",
                    curr.symbol, direction, prev.sma5, prev.sma21, prev.timestamp, curr.sma5, curr.sma21, curr.timestamp, strength
            ));

            out.collect(new StrategySignal(
                    runId,
                    curr.symbol,
                    curr.timestamp,
                    curr.close,
                    curr.sma5,
                    curr.sma21,
                    direction,
                    strength
            ));
        }
    }

    // --- cooldown enforcer (keyed by symbol) ---
    static final class CooldownFn extends KeyedProcessFunction<String, StrategySignal, StrategySignal> {
        private final long cooldownMs;
        private transient ValueState<Long> lastTs;

        CooldownFn(long cooldownMs) { this.cooldownMs = cooldownMs; }

        @Override
        public void open(Configuration parameters) {
            lastTs = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("lastEmittedTs", Long.class));
        }

        @Override
        public void processElement(StrategySignal s, Context ctx, Collector<StrategySignal> out) throws Exception {
            Long last = lastTs.value();
            if (last == null || (s.timestamp - last) >= cooldownMs) {
                out.collect(s);
                lastTs.update(s.timestamp);
            }
            // else: drop due to cooldown
        }
    }
}
