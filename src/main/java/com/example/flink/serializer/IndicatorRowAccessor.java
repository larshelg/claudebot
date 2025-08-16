package com.example.flink.serializer;

import java.util.Collections;
import java.util.Map;

public class IndicatorRowAccessor {
    private final String symbol;
    private final long timestamp;
    private final Map<String, Double> indicators;

    public IndicatorRowAccessor(IndicatorRowFlexible row) {
        this.symbol = row.symbol;
        this.timestamp = row.timestamp;
        this.indicators = row.indicators != null ? row.indicators : Collections.emptyMap();
    }

    public String getSymbol() {
        return symbol;
    }

    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Generic getter for any indicator
     */
    public Double get(String indicatorName) {
        return indicators.get(indicatorName);
    }

    // Convenience typed getters for common indicators
    public Double getSma(String period) {
        return indicators.get("sma" + period);
    }

    public Double getEma(String period) {
        return indicators.get("ema" + period);
    }

    public Double getRsi(String period) {
        return indicators.get("rsi" + period);
    }

    public Map<String, Double> getAllIndicators() {
        return indicators;
    }
}
