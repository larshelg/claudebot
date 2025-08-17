package com.example.flink.strategy;

import com.example.flink.*;

import com.example.flink.serializer.CandleFlexible;
import com.example.flink.serializer.IndicatorRowFlexible;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.functions.MapFunction;

import java.io.Serializable;

public class RSIThresholdStrategy implements TradingStrategy, Serializable {

    private final String name;
    private final double oversoldThreshold;
    private final double overboughtThreshold;

    public RSIThresholdStrategy(String name, double oversoldThreshold, double overboughtThreshold) {
        this.name = name;
        this.oversoldThreshold = oversoldThreshold;
        this.overboughtThreshold = overboughtThreshold;
    }

    @Override
    public StrategySignal process(IndicatorRowFlexible indicator, CandleFlexible candle) {
        Double rsi = indicator.indicators.get("rsi");
        if (rsi == null) return null;

        String signal = null;
        if (rsi < oversoldThreshold) {
            signal = "BUY";
        } else if (rsi > overboughtThreshold) {
            signal = "SELL";
        }

        if (signal == null) return null;

        return new StrategySignal(
                String.valueOf(System.currentTimeMillis() / 1000),
                indicator.symbol,
                indicator.timestamp,
                candle.getClose(),
                indicator.indicators.getOrDefault("sma5", 0.0),
                indicator.indicators.getOrDefault("sma21", 0.0),
                signal,
                Math.abs(rsi - 50) / 50 // example of signal strength
        );
    }

    @Override
    public boolean isCEP() {
        return false; // no CEP needed
    }

    @Override
    public String getName() {
        return name;
    }
}
