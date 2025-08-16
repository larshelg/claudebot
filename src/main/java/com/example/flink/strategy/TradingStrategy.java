package com.example.flink.strategy;

import com.example.flink.IndicatorWithPrice;
import com.example.flink.StrategySignal;
import com.example.flink.serializer.CandleFlexible;
import com.example.flink.serializer.IndicatorRowFlexible;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.List;

public interface TradingStrategy {

    /**
     * Process a single joined row of indicator + candle.
     * Used for ValueState-style strategies.
     */
    StrategySignal process(IndicatorRowFlexible indicator, CandleFlexible candle);

    /**
     * Optional: Apply CEP if supported. Default is null.
     */
    default DataStream<StrategySignal> applyCEP(DataStream<IndicatorWithPrice> joinedStream) {
        return null; // Not all strategies need CEP
    }

    /**
     * Indicates whether this strategy uses CEP
     */
    default boolean isCEP() {
        return false;
    }
}
