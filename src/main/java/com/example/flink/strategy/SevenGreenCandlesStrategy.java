package com.example.flink.strategy;

import com.example.flink.*;
import com.example.flink.serializer.CandleFlexible;
import com.example.flink.serializer.IndicatorRowFlexible;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class SevenGreenCandlesStrategy implements TradingStrategy {

    private final String name;

    public SevenGreenCandlesStrategy(String name) {
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

        // Assign timestamps and watermarks for event-time processing
        DataStream<IndicatorWithPrice> withTs = input.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<IndicatorWithPrice>forMonotonousTimestamps()
                        .withTimestampAssigner((ev, ts) -> ev.timestamp));

        // Pattern for 7 consecutive green candles
        // Note: This assumes we have access to open price somehow.
        // For now, we'll use a proxy: consecutive rising closes as "green candles"
        Pattern<IndicatorWithPrice, ?> sevenGreenCandles = Pattern
                .<IndicatorWithPrice>begin("green1", AfterMatchSkipStrategy.skipPastLastEvent())
                .where(new SimpleCondition<IndicatorWithPrice>() {
                    @Override
                    public boolean filter(IndicatorWithPrice value) {
                        return true; // First candle - we'll check progression in next steps
                    }
                })
                .next("green2")
                .where(new SimpleCondition<IndicatorWithPrice>() {
                    @Override
                    public boolean filter(IndicatorWithPrice value) {
                        return value.close > 0; // Basic validity check
                    }
                })
                .next("green3")
                .where(new SimpleCondition<IndicatorWithPrice>() {
                    @Override
                    public boolean filter(IndicatorWithPrice value) {
                        return value.close > 0;
                    }
                })
                .next("green4")
                .where(new SimpleCondition<IndicatorWithPrice>() {
                    @Override
                    public boolean filter(IndicatorWithPrice value) {
                        return value.close > 0;
                    }
                })
                .next("green5")
                .where(new SimpleCondition<IndicatorWithPrice>() {
                    @Override
                    public boolean filter(IndicatorWithPrice value) {
                        return value.close > 0;
                    }
                })
                .next("green6")
                .where(new SimpleCondition<IndicatorWithPrice>() {
                    @Override
                    public boolean filter(IndicatorWithPrice value) {
                        return value.close > 0;
                    }
                })
                .next("green7")
                .where(new SimpleCondition<IndicatorWithPrice>() {
                    @Override
                    public boolean filter(IndicatorWithPrice value) {
                        return value.close > 0;
                    }
                })
                .within(Duration.ofHours(12)); // Allow up to 12 hours for the sequence

        // Create pattern stream
        var patternStream = CEP.pattern(
                withTs.keyBy(ev -> ev.symbol),
                sevenGreenCandles);

        // Side output for debug
        final OutputTag<String> DEBUG_TAG = new OutputTag<String>("seven-green-debug") {
        };

        // Process the pattern matches
        SingleOutputStreamOperator<StrategySignal> signals = patternStream
                .process(new PatternProcessFunction<IndicatorWithPrice, StrategySignal>() {
                    @Override
                    public void processMatch(
                            Map<String, List<IndicatorWithPrice>> match,
                            Context ctx,
                            Collector<StrategySignal> out) {

                        // Extract all 7 candles
                        IndicatorWithPrice candle1 = match.get("green1").get(0);
                        IndicatorWithPrice candle2 = match.get("green2").get(0);
                        IndicatorWithPrice candle3 = match.get("green3").get(0);
                        IndicatorWithPrice candle4 = match.get("green4").get(0);
                        IndicatorWithPrice candle5 = match.get("green5").get(0);
                        IndicatorWithPrice candle6 = match.get("green6").get(0);
                        IndicatorWithPrice candle7 = match.get("green7").get(0);

                        // Validate that we have 7 consecutive rising closes (proxy for green candles)
                        boolean isConsecutiveRising = candle2.close > candle1.close &&
                                candle3.close > candle2.close &&
                                candle4.close > candle3.close &&
                                candle5.close > candle4.close &&
                                candle6.close > candle5.close &&
                                candle7.close > candle6.close;

                        if (isConsecutiveRising) {
                            // Debug visibility
                            ctx.output(DEBUG_TAG, String.format(
                                    "7 GREEN CANDLES MATCH %s: %.4f -> %.4f -> %.4f -> %.4f -> %.4f -> %.4f -> %.4f (ts: %d to %d)",
                                    candle7.symbol,
                                    candle1.close, candle2.close, candle3.close, candle4.close,
                                    candle5.close, candle6.close, candle7.close,
                                    candle1.timestamp, candle7.timestamp));

                            // Calculate signal strength based on:
                            // 1. Total price appreciation
                            // 2. Consistency of gains
                            double totalGain = (candle7.close - candle1.close) / candle1.close;
                            double avgGainPerCandle = totalGain / 6.0; // 6 transitions

                            // Normalize signal strength (0 to 1)
                            double signalStrength = Math.min(1.0, totalGain * 10); // 10% gain = max strength

                            // Emit BUY signal after 7 consecutive green candles
                            out.collect(new StrategySignal(
                                    String.valueOf(System.currentTimeMillis() / 1000),
                                    candle7.symbol,
                                    candle7.timestamp,
                                    candle7.close,
                                    candle7.sma5 != null ? candle7.sma5 : 0.0,
                                    candle7.sma21 != null ? candle7.sma21 : 0.0,
                                    "BUY",
                                    signalStrength));
                        }
                    }
                });

        // Debug output
        DataStream<String> debugStream = signals.getSideOutput(DEBUG_TAG);
        debugStream.print("SEVEN-GREEN-CANDLES-DEBUG");

        return signals;
    }
}
