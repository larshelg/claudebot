package com.example.flink.strategy;

import com.example.flink.*;
import com.example.flink.serializer.CandleFlexible;
import com.example.flink.serializer.IndicatorRowFlexible;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;

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
        // Define the pattern: SMA5 crosses above SMA21 (only BUY)
        Pattern<IndicatorWithPrice, IndicatorWithPrice> crossoverPattern =
                Pattern.<IndicatorWithPrice>begin("prev")
                        .next("curr")
                        .where(new SimpleCondition<IndicatorWithPrice>() {
                            @Override
                            public boolean filter(IndicatorWithPrice value) {
                                return value.sma5 > value.sma21;
                            }
                        });

        // Apply pattern on keyed stream
        var patternStream = CEP.pattern(input.keyBy(ev -> ev.symbol), crossoverPattern);

        // Map matched pattern to StrategySignal
        return patternStream.select(new PatternSelectFunction<IndicatorWithPrice, StrategySignal>() {
            @Override
            public StrategySignal select(Map<String, List<IndicatorWithPrice>> pattern) {
                IndicatorWithPrice curr = pattern.get("curr").get(0);
                double signalStrength = curr.sma21 != 0
                        ? Math.min(1.0, Math.abs(curr.sma5 - curr.sma21) / curr.sma21 * 100)
                        : 0.0;
                return new StrategySignal(
                        String.valueOf(System.currentTimeMillis() / 1000),
                        curr.symbol,
                        curr.timestamp,
                        curr.close,
                        curr.sma5,
                        curr.sma21,
                        "BUY",
                        signalStrength
                );
            }
        });
    }
}
