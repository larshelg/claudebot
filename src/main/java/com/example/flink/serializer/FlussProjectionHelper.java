package com.example.flink.serializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class FlussProjectionHelper {

    /**
     * Build projected columns list for Fluss source.
     * Always includes "symbol" and "time".
     *
     * @param indicatorsNeeded Set of indicator names your strategy requires (e.g. "sma5", "ema14")
     * @return Ordered list suitable for FlussSource.setColumns()
     */
    public static List<String> buildProjectedColumns(Set<String> indicatorsNeeded) {
        List<String> projected = new ArrayList<>();
        projected.add("symbol");   // mandatory
        projected.add("time");     // mandatory
        projected.addAll(indicatorsNeeded); // requested indicators
        return projected;
    }
}