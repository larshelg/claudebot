package com.example.flink;

import java.io.Serializable;

public class IndicatorRow implements Serializable {
    public String symbol;
    public long timestamp; // Using long instead of Instant for Kryo compatibility
    public double sma5, sma14, sma21;
    public double ema5, ema14, ema21;
    public double rsi14;

    public IndicatorRow(String symbol, long timestamp,
            double sma5, double sma14, double sma21,
            double ema5, double ema14, double ema21,
            double rsi14) {
        this.symbol = symbol;
        this.timestamp = timestamp;
        this.sma5 = sma5;
        this.sma14 = sma14;
        this.sma21 = sma21;
        this.ema5 = ema5;
        this.ema14 = ema14;
        this.ema21 = ema21;
        this.rsi14 = rsi14;
    }

    @Override
    public String toString() {
        return String.format(
                "IndicatorRow{symbol='%s', timestamp=%d, SMA(5)=%.2f, SMA(14)=%.2f, SMA(21)=%.2f, EMA(5)=%.2f, EMA(14)=%.2f, EMA(21)=%.2f, RSI(14)=%.2f}",
                symbol, timestamp, sma5, sma14, sma21, ema5, ema14, ema21, rsi14);
    }
}
