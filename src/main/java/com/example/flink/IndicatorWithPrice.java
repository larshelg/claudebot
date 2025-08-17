package com.example.flink;

import java.io.Serializable;

public class IndicatorWithPrice implements Serializable {
    public  String symbol;
    public  long timestamp;
    public  double close;
    public  Double sma5;
    public  Double sma21;
    public  Double rsi;


    public IndicatorWithPrice() {}

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
