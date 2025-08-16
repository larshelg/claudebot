package com.example.flink.strategy;

import com.example.flink.StrategySignal;
import com.example.flink.serializer.CandleFlexible;
import com.example.flink.serializer.IndicatorRowFlexible;

public class SimpleCrossoverStrategy implements TradingStrategy {

    private final String name;

    public SimpleCrossoverStrategy(String name) {
        this.name = name;
    }

    @Override
    public StrategySignal process(IndicatorRowFlexible indicator, CandleFlexible candle) {
        Double sma5 = indicator.indicators.get("sma5");
        Double sma21 = indicator.indicators.get("sma21");

        if (sma5 == null || sma21 == null) {
            return null;
        }

        if (sma5 > sma21) {
            return new StrategySignal(
                    String.valueOf(System.currentTimeMillis() / 1000),
                    indicator.symbol,
                    indicator.timestamp,
                    candle.getClose(),
                    sma5,
                    sma21,
                    "BUY",
                    Math.abs((sma5 - sma21) / sma21)
            );
        } else if (sma5 < sma21) {
            return new StrategySignal(
                    String.valueOf(System.currentTimeMillis() / 1000),
                    indicator.symbol,
                    indicator.timestamp,
                    candle.getClose(),
                    sma5,
                    sma21,
                    "SELL",
                    Math.abs((sma21 - sma5) / sma21)
            );
        }

        return null;
    }

    @Override
    public String getName() {
        return name;
    }
}
