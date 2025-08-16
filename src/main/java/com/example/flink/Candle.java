package com.example.flink;

public class Candle {
    public String symbol;
    public long timestamp;
    public double open;
    public double high;
    public double low;
    public double close;
    public double volume;

    public Candle() {
    }

    public Candle(String symbol, long timestamp, double open, double high, double low, double close, double volume) {
        this.symbol = symbol;
        this.timestamp = timestamp;
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.volume = volume;
    }
}
