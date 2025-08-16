package com.example.flink.serializer;

import java.util.Map;

public class IndicatorRowFlexible {
    public final String symbol;
    public final long timestamp;
    public final Map<String, Double> indicators; // dynamic map of selected indicators

    public IndicatorRowFlexible(String symbol, long timestamp, Map<String, Double> indicators) {
        this.symbol = symbol;
        this.timestamp = timestamp;
        this.indicators = indicators;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
