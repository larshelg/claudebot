package com.example.flink;

import com.example.flink.domain.StrategySignal;
import com.example.flink.serializer.CandleFlexible;
import com.example.flink.serializer.IndicatorRowFlexible;
import com.example.flink.strategy.TradingStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * KeyedProcessFunction that wraps a TradingStrategy and keeps track of previous SMA values.
 */
public class ValueStateCrossoverProcessFunction
        extends KeyedProcessFunction<String, IndicatorWithPrice, StrategySignal> {

    private final TradingStrategy strategy;

    private transient ValueState<Double> prevSma5State;
    private transient ValueState<Double> prevSma21State;

    public ValueStateCrossoverProcessFunction(TradingStrategy strategy) {
        this.strategy = strategy;
    }

    @Override
    public void open(Configuration parameters) {
        prevSma5State = getRuntimeContext().getState(
                new ValueStateDescriptor<>("prevSma5", Double.class)
        );
        prevSma21State = getRuntimeContext().getState(
                new ValueStateDescriptor<>("prevSma21", Double.class)
        );
    }

    @Override
    public void processElement(IndicatorWithPrice value, Context ctx, Collector<StrategySignal> out) throws Exception {
        Double prevSma5 = prevSma5State.value();
        Double prevSma21 = prevSma21State.value();

        if (prevSma5 != null && prevSma21 != null) {
            // Wrap the IndicatorWithPrice into Flink's flexible POJOs for the strategy
            IndicatorRowFlexible indicatorRow = new IndicatorRowFlexible(
                    value.symbol,
                    value.timestamp,
                    java.util.Map.of("sma5", value.sma5, "sma21", value.sma21)
            );

            CandleFlexible candle = new CandleFlexible(
                    value.symbol,
                    value.timestamp,
                    java.util.Map.of("close", value.close),
                    0L
            );

            StrategySignal signal = strategy.process(indicatorRow, candle);

            if (signal != null) {
                out.collect(signal);
            }
        }

        // Update the state for next element
        prevSma5State.update(value.sma5);
        prevSma21State.update(value.sma21);
    }
}
