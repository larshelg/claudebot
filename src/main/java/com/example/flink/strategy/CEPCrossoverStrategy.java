package com.example.flink.strategy;

import com.example.flink.*;
import com.example.flink.serializer.CandleFlexible;
import com.example.flink.serializer.IndicatorRowFlexible;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class CEPCrossoverStrategy implements TradingStrategy, Serializable {

    private final String name;

    public CEPCrossoverStrategy(String name) {
        this.name = name;
    }

    @Override
    public StrategySignal process(IndicatorRowFlexible indicator, CandleFlexible candle) {
        // CEP strategies do not use this; return null
        return null;
    }

    @Override
    public boolean isCEP() {
        return true;
    }

    @Override
    public String getName() {
        return name;
    }

    public DataStream<StrategySignal> applyCEP(DataStream<IndicatorWithPrice> input) {

        // If your timestamps are event-time, assign them; otherwise skip this.
        DataStream<IndicatorWithPrice> withTs =
                input.assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<IndicatorWithPrice>forMonotonousTimestamps()
                                .withTimestampAssigner((ev, ts) -> ev.timestamp)
                );

        // Allow irrelevant events between prev and curr (relaxed contiguity).
        Pattern<IndicatorWithPrice, ?> crossoverPattern =
                Pattern.<IndicatorWithPrice>begin("prev",
                                AfterMatchSkipStrategy.skipPastLastEvent())
                        .where(new SimpleCondition<IndicatorWithPrice>() {
                            @Override
                            public boolean filter(IndicatorWithPrice value) {
                                return value.sma5 <= value.sma21;  // under or equal
                            }
                        })
                        .followedBy("curr")                          // NOT next()
                        .where(new SimpleCondition<IndicatorWithPrice>() {
                            @Override
                            public boolean filter(IndicatorWithPrice value) {
                                return value.sma5 > value.sma21;   // now over
                            }
                        })
                        .within(Time.hours(1));                     // bound state (tune as needed)

        var patternStream = CEP.pattern(
                withTs.keyBy(ev -> ev.symbol),
                crossoverPattern
        );

        // Side output for debug
        final OutputTag<String> DEBUG = new OutputTag<String>("cep-debug") {};

        SingleOutputStreamOperator<StrategySignal> out =
                patternStream.process(new PatternProcessFunction<IndicatorWithPrice, StrategySignal>() {
                    @Override
                    public void processMatch(
                            java.util.Map<String, java.util.List<IndicatorWithPrice>> match,
                            Context ctx,
                            Collector<StrategySignal> out) {

                        IndicatorWithPrice prev = match.get("prev").get(0);
                        IndicatorWithPrice curr = match.get("curr").get(0);

                        // Debug visibility
                        ctx.output(DEBUG, String.format(
                                "MATCH %s: prev(sma5=%.4f sma21=%.4f ts=%d) -> curr(sma5=%.4f sma21=%.4f ts=%d)",
                                curr.symbol, prev.sma5, prev.sma21, prev.timestamp, curr.sma5, curr.sma21, curr.timestamp
                        ));

                        double signalStrength = curr.sma21 != 0
                                ? Math.min(1.0, Math.abs(curr.sma5 - curr.sma21) / curr.sma21 * 100)
                                : 0.0;

                        out.collect(new StrategySignal(
                                String.valueOf(System.currentTimeMillis() / 1000),
                                curr.symbol,
                                curr.timestamp,
                                curr.close,
                                curr.sma5,
                                curr.sma21,
                                "BUY",
                                signalStrength
                        ));
                    }
                });

        // You can .print() this in tests or collect it separately
        DataStream<String> debug = out.getSideOutput(DEBUG);
        debug.print(); // or return it from the method if you prefer

        return out;
    }


}

