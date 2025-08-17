package com.example.flink.serializer;

import java.io.Serializable;
import java.util.Map;

public class IndicatorRowFlexible implements Serializable {
    public  String symbol;
    public  long timestamp;
    public  Map<String, Double> indicators; // dynamic map of selected indicators


    public IndicatorRowFlexible() {} // empty constructor needed by Flink/Kryo
    public IndicatorRowFlexible(String symbol, long timestamp, Map<String, Double> indicators) {
        this.symbol = symbol;
        this.timestamp = timestamp;
        this.indicators = indicators;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
