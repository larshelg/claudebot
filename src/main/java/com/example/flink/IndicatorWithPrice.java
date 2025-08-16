package com.example.flink;

import java.io.Serializable;

public class IndicatorWithPrice implements Serializable {
    public final String symbol;
    public final long timestamp;
    public final double close;
    public final Double sma5;
    public final Double sma21;
    public final Double rsi;

    public IndicatorWithPrice(String symbol, long timestamp, double close, Double sma5, Double sma21, Double rsi) {
        this.symbol = symbol;
        this.timestamp = timestamp;
        this.close = close;
        this.sma5 = sma5;
        this.sma21 = sma21;
        this.rsi = rsi;
    }

    @Override
    public String toString() {
        return "IndicatorWithPrice{" +
                "symbol='" + symbol + '\'' +
                ", timestamp=" + timestamp +
                ", close=" + close +
                ", sma5=" + sma5 +
                ", sma21=" + sma21 +
                ", rsi=" + rsi +
                '}';
    }
}
