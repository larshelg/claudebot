package com.example.flink;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Keyed process function that detects SMA crossovers for trading signals
 */
public class CrossoverDetectionProcess extends KeyedProcessFunction<String, IndicatorWithPrice, StrategySignal> {

    private ValueState<Double> prevSma5State;
    private ValueState<Double> prevSma21State;

    @Override
    public void open(Configuration parameters) throws Exception {
        prevSma5State = getRuntimeContext().getState(
                new ValueStateDescriptor<>("prevSma5", Double.class));
        prevSma21State = getRuntimeContext().getState(
                new ValueStateDescriptor<>("prevSma21", Double.class));
    }

    @Override
    public void processElement(IndicatorWithPrice value, Context ctx, Collector<StrategySignal> out) throws Exception {
        Double prevSma5 = prevSma5State.value();
        Double prevSma21 = prevSma21State.value();

        System.out.println("DEBUG: Processing crossover for " + value.symbol +
                " - Current: SMA5=" + value.sma5 + " SMA21=" + value.sma21 +
                " Previous: SMA5=" + prevSma5 + " SMA21=" + prevSma21);

        String signal = "HOLD";

        // Check for crossovers only if we have previous values
        if (prevSma5 != null && prevSma21 != null) {
            // Golden Cross: SMA5 crosses above SMA21 (BUY signal)
            if (value.sma5 > value.sma21 && prevSma5 <= prevSma21) {
                signal = "BUY";
                System.out.println("DEBUG: *** GOLDEN CROSS DETECTED *** BUY signal for " + value.symbol);
            }
            // Death Cross: SMA5 crosses below SMA21 (SELL signal)
            else if (value.sma5 < value.sma21 && prevSma5 >= prevSma21) {
                signal = "SELL";
                System.out.println("DEBUG: *** DEATH CROSS DETECTED *** SELL signal for " + value.symbol);
            }
        } else {
            System.out.println("DEBUG: No previous values yet for " + value.symbol + ", skipping crossover check");
        }

        // Calculate signal strength as percentage difference between SMAs
        double signalStrength = 0.0;
        if (value.sma21 != 0) {
            signalStrength = Math.min(1.0, Math.abs(value.sma5 - value.sma21) / value.sma21 * 100);
        }

        // Only emit signals for actual crossovers (not HOLD)
        if (!"HOLD".equals(signal)) {
            String runId = String.valueOf(System.currentTimeMillis() / 1000);
            StrategySignal strategySignal = new StrategySignal(
                    runId,
                    value.symbol,
                    value.timestamp,
                    value.close,
                    value.sma5,
                    value.sma21,
                    signal,
                    signalStrength);
            System.out.println("DEBUG: *** EMITTING SIGNAL *** " + strategySignal);
            out.collect(strategySignal);
        } else {
            System.out.println("DEBUG: HOLD signal, not emitting for " + value.symbol);
        }

        // Update state with current values for next iteration
        prevSma5State.update(value.sma5);
        prevSma21State.update(value.sma21);
    }
}
