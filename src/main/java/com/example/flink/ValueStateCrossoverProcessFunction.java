package com.example.flink;

import com.example.flink.serializer.CandleFlexible;
import com.example.flink.serializer.IndicatorRowFlexible;
import com.example.flink.strategy.TradingStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Generic wrapper around a TradingStrategy.
 */
public class ValueStateCrossoverProcessFunction
        extends KeyedProcessFunction<String, IndicatorWithPrice, StrategySignal> {

    private final TradingStrategy strategy;
    private transient ValueState<IndicatorWithPrice> lastEventState;

    public ValueStateCrossoverProcessFunction(TradingStrategy strategy) {
        this.strategy = strategy;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        lastEventState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastEvent", IndicatorWithPrice.class));
    }

    @Override
    public void processElement(IndicatorWithPrice value, Context ctx, Collector<StrategySignal> out) throws Exception {
        IndicatorWithPrice last = lastEventState.value();

        if (last != null) {
            // Wrap close into a map for the new CandleFlexible constructor
            java.util.Map<String, Double> fields = java.util.Map.of("close", value.close);

            CandleFlexible candleFlexible = new CandleFlexible(
                    value.symbol,
                    value.timestamp,
                    fields,
                    0L // or null if you prefer
            );

            // Delegate decision-making to the strategy
            StrategySignal signal = strategy.process(
                    new IndicatorRowFlexible(value.symbol, value.timestamp,
                            java.util.Map.of("sma5", value.sma5, "sma21", value.sma21)),
                    candleFlexible
            );

            if (signal != null) {
                out.collect(signal);
            }
        }

        lastEventState.update(value);
    }

}
