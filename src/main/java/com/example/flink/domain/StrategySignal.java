package com.example.flink.domain;

import java.io.Serializable;

/**
 * Represents a trading strategy signal with buy/sell/hold decisions
 */
public class StrategySignal implements Serializable {
    public  String runId;
    public  String symbol;
    public  long timestamp;
    public  double close;
    public  double sma5;
    public  double sma21;
    public  String signal;
    public  double signalStrength;

    public StrategySignal() {}

    public StrategySignal(String runId, String symbol, long timestamp, double close,
            double sma5, double sma21, String signal, double signalStrength) {
        this.runId = runId;
        this.symbol = symbol;
        this.timestamp = timestamp;
        this.close = close;
        this.sma5 = sma5;
        this.sma21 = sma21;
        this.signal = signal;
        this.signalStrength = signalStrength;
    }

    @Override
    public String toString() {
        return String.format("StrategySignal{symbol='%s', time=%d, signal='%s', sma5=%.4f, sma21=%.4f}",
                symbol, timestamp, signal, sma5, sma21);
    }
}
