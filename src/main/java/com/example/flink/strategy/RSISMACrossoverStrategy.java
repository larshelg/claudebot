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

public class RSISMACrossoverStrategy implements TradingStrategy {

    private final String name;

    public RSISMACrossoverStrategy(String name) {
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

        // BUY PATTERN: RSI under 25 followed by SMA5 crossing over SMA21
        Pattern<IndicatorWithPrice, ?> buyPattern = Pattern.<IndicatorWithPrice>begin("rsi_oversold",
                AfterMatchSkipStrategy.skipPastLastEvent())
                .where(new SimpleCondition<IndicatorWithPrice>() {
                    @Override
                    public boolean filter(IndicatorWithPrice value) {
                        return value.rsi != null && value.rsi < 25.0;
                    }
                })
                .followedBy("sma_before_cross")
                .where(new SimpleCondition<IndicatorWithPrice>() {
                    @Override
                    public boolean filter(IndicatorWithPrice value) {
                        return value.sma5 != null && value.sma21 != null &&
                                value.sma5 <= value.sma21; // SMA5 under or equal to SMA21
                    }
                })
                .followedBy("sma_cross_up")
                .where(new SimpleCondition<IndicatorWithPrice>() {
                    @Override
                    public boolean filter(IndicatorWithPrice value) {
                        return value.sma5 != null && value.sma21 != null &&
                                value.sma5 > value.sma21; // SMA5 crosses above SMA21
                    }
                })
                .within(Duration.ofHours(4)); // Allow up to 4 hours for the sequence

        // SELL PATTERN: RSI over 72 followed by price crossing under SMA5
        Pattern<IndicatorWithPrice, ?> sellPattern = Pattern.<IndicatorWithPrice>begin("rsi_overbought",
                AfterMatchSkipStrategy.skipPastLastEvent())
                .where(new SimpleCondition<IndicatorWithPrice>() {
                    @Override
                    public boolean filter(IndicatorWithPrice value) {
                        return value.rsi != null && value.rsi > 72.0;
                    }
                })
                .followedBy("price_above_sma5")
                .where(new SimpleCondition<IndicatorWithPrice>() {
                    @Override
                    public boolean filter(IndicatorWithPrice value) {
                        return value.sma5 != null && value.close >= value.sma5; // Price above or equal to SMA5
                    }
                })
                .followedBy("price_cross_under")
                .where(new SimpleCondition<IndicatorWithPrice>() {
                    @Override
                    public boolean filter(IndicatorWithPrice value) {
                        return value.sma5 != null && value.close < value.sma5; // Price crosses under SMA5
                    }
                })
                .within(Duration.ofHours(4)); // Allow up to 4 hours for the sequence

        // Create pattern streams
        var buyPatternStream = CEP.pattern(
                withTs.keyBy(ev -> ev.symbol),
                buyPattern);

        var sellPatternStream = CEP.pattern(
                withTs.keyBy(ev -> ev.symbol),
                sellPattern);

        // Side outputs for debug
        final OutputTag<String> BUY_DEBUG = new OutputTag<String>("rsi-sma-buy-debug") {
        };
        final OutputTag<String> SELL_DEBUG = new OutputTag<String>("rsi-sma-sell-debug") {
        };

        // Process BUY signals
        SingleOutputStreamOperator<StrategySignal> buySignals = buyPatternStream
                .process(new PatternProcessFunction<IndicatorWithPrice, StrategySignal>() {
                    @Override
                    public void processMatch(
                            Map<String, List<IndicatorWithPrice>> match,
                            Context ctx,
                            Collector<StrategySignal> out) {

                        IndicatorWithPrice rsiOversold = match.get("rsi_oversold").get(0);
                        IndicatorWithPrice smaBefore = match.get("sma_before_cross").get(0);
                        IndicatorWithPrice smaCross = match.get("sma_cross_up").get(0);

                        // Debug visibility
                        ctx.output(BUY_DEBUG, String.format(
                                "BUY MATCH %s: rsi_oversold(rsi=%.2f ts=%d) -> sma_before(sma5=%.4f sma21=%.4f ts=%d) -> sma_cross(sma5=%.4f sma21=%.4f ts=%d)",
                                smaCross.symbol, rsiOversold.rsi, rsiOversold.timestamp,
                                smaBefore.sma5, smaBefore.sma21, smaBefore.timestamp,
                                smaCross.sma5, smaCross.sma21, smaCross.timestamp));

                        // Calculate signal strength based on RSI distance from oversold and SMA spread
                        double rsiStrength = Math.min(1.0, (30.0 - rsiOversold.rsi) / 30.0); // More oversold = stronger
                        double smaStrength = smaCross.sma21 != 0
                                ? Math.min(1.0, Math.abs(smaCross.sma5 - smaCross.sma21) / smaCross.sma21 * 100)
                                : 0.0;
                        double signalStrength = (rsiStrength + smaStrength) / 2.0;

                        out.collect(new StrategySignal(
                                String.valueOf(System.currentTimeMillis() / 1000),
                                smaCross.symbol,
                                smaCross.timestamp,
                                smaCross.close,
                                smaCross.sma5,
                                smaCross.sma21,
                                "BUY",
                                signalStrength));
                    }
                });

        // Process SELL signals
        SingleOutputStreamOperator<StrategySignal> sellSignals = sellPatternStream
                .process(new PatternProcessFunction<IndicatorWithPrice, StrategySignal>() {
                    @Override
                    public void processMatch(
                            Map<String, List<IndicatorWithPrice>> match,
                            Context ctx,
                            Collector<StrategySignal> out) {

                        IndicatorWithPrice rsiOverbought = match.get("rsi_overbought").get(0);
                        IndicatorWithPrice priceAbove = match.get("price_above_sma5").get(0);
                        IndicatorWithPrice priceCross = match.get("price_cross_under").get(0);

                        // Debug visibility
                        ctx.output(SELL_DEBUG, String.format(
                                "SELL MATCH %s: rsi_overbought(rsi=%.2f ts=%d) -> price_above(close=%.4f sma5=%.4f ts=%d) -> price_cross(close=%.4f sma5=%.4f ts=%d)",
                                priceCross.symbol, rsiOverbought.rsi, rsiOverbought.timestamp,
                                priceAbove.close, priceAbove.sma5, priceAbove.timestamp,
                                priceCross.close, priceCross.sma5, priceCross.timestamp));

                        // Calculate signal strength based on RSI distance from overbought and price vs
                        // SMA5
                        double rsiStrength = Math.min(1.0, (rsiOverbought.rsi - 70.0) / 30.0); // More overbought =
                                                                                               // stronger
                        double priceStrength = priceCross.sma5 != 0
                                ? Math.min(1.0, Math.abs(priceCross.close - priceCross.sma5) / priceCross.sma5 * 100)
                                : 0.0;
                        double signalStrength = (rsiStrength + priceStrength) / 2.0;

                        out.collect(new StrategySignal(
                                String.valueOf(System.currentTimeMillis() / 1000),
                                priceCross.symbol,
                                priceCross.timestamp,
                                priceCross.close,
                                priceCross.sma5,
                                priceCross.sma21,
                                "SELL",
                                signalStrength));
                    }
                });

        // Union buy and sell signals
        DataStream<StrategySignal> allSignals = buySignals.union(sellSignals);

        // Debug outputs - you can collect these separately or print them
        DataStream<String> buyDebug = buySignals.getSideOutput(BUY_DEBUG);
        DataStream<String> sellDebug = sellSignals.getSideOutput(SELL_DEBUG);

        buyDebug.print("RSI-SMA-BUY-DEBUG");
        sellDebug.print("RSI-SMA-SELL-DEBUG");

        return allSignals;
    }
}
