package com.example.flink;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

public class MultiIndicatorProcess
        extends KeyedProcessFunction<String, Candle, IndicatorRow> {

    private transient ValueState<IntegratedIndicatorState> state;
    private final List<Integer> smaPeriods = Arrays.asList(5, 14, 21);
    private final List<Integer> emaPeriods = Arrays.asList(5, 14, 21);
    private final int rsiPeriod = 14;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<IntegratedIndicatorState> descriptor = new ValueStateDescriptor<>(
                "multi-indicator-state",
                TypeInformation.of(new TypeHint<>() {
                }));
        state = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(Candle candle,
            Context ctx,
            Collector<IndicatorRow> out) throws Exception {
        IntegratedIndicatorState s = state.value();
        if (s == null) {
            s = new IntegratedIndicatorState();
            // Init light indicators
            for (int p : smaPeriods)
                s.sma.put(p, new SimpleMovingAverage(p));
            for (int p : emaPeriods)
                s.ema.put(p, 0.0);
        }

        // --- Light: SMA ---
        for (int p : smaPeriods) {
            s.sma.get(p).add(candle.close);
        }

        // --- Light: EMA ---
        for (int p : emaPeriods) {
            double prevEma = s.ema.get(p);
            double multiplier = 2.0 / (p + 1);
            double ema = prevEma == 0 ? candle.close : (candle.close - prevEma) * multiplier + prevEma;
            s.ema.put(p, ema);
        }

        // --- Heavy: RSI ---
        s.heavy.addClosePrice(candle.close); // keep deque of last N closes internally
        double rsi = s.heavy.calculateRSI(rsiPeriod);

        // (Add other heavy indicators here: ATR, Bollinger, MACD…)

        // Update state
        state.update(s);

        // Emit row with all indicators
        IndicatorRow row = new IndicatorRow(
                candle.symbol,
                candle.timestamp,
                s.sma.get(5).get(),
                s.sma.get(14).get(),
                s.sma.get(21).get(),
                s.ema.get(5),
                s.ema.get(14),
                s.ema.get(21),
                rsi
        // add heavy indicators here
        );
        out.collect(row);
    }
}
