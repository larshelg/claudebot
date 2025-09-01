package com.example.flink.strategy;

import com.example.flink.IndicatorWithPrice;
import com.example.flink.domain.StrategySignal;
import com.example.flink.serializer.CandleFlexible;
import com.example.flink.serializer.IndicatorRowFlexible;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;

public interface TradingStrategy extends Serializable {
    // For old-style process (ValueState)
    StrategySignal process(IndicatorRowFlexible indicator, CandleFlexible candle);

    // For CEP-based patterns
    default boolean isCEP() { return false; }

    default DataStream<StrategySignal> applyCEP(DataStream<IndicatorWithPrice> stream) {
        throw new UnsupportedOperationException("CEP not implemented");
    }

    default boolean requiresState() {return false;}

    String getName();
}
