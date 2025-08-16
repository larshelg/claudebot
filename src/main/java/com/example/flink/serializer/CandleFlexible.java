package com.example.flink.serializer;

import java.io.Serializable;
import java.util.Map;

public class CandleFlexible implements Serializable {

    public final String symbol;
    public final long timestamp;
    public final Map<String, Double> fields; // e.g. open, high, low, close
    public final Long volume;                // optional (nullable)

    public CandleFlexible(String symbol,
                          long timestamp,
                          Map<String, Double> fields,
                          Long volume) {
        this.symbol = symbol;
        this.timestamp = timestamp;
        this.fields = fields;
        this.volume = volume;
    }

    // Convenience getters
    public Double get(String field) {
        return fields.get(field);
    }

    public Double getClose() {
        return fields.get("close");
    }

    public Double getOpen() {
        return fields.get("open");
    }

    public Double getHigh() {
        return fields.get("high");
    }

    public Double getLow() {
        return fields.get("low");
    }

    @Override
    public String toString() {
        return "CandleFlexible{" +
                "symbol='" + symbol + '\'' +
                ", timestamp=" + timestamp +
                ", fields=" + fields +
                ", volume=" + volume +
                '}';
    }
}
