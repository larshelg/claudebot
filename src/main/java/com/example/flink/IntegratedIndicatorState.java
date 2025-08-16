package com.example.flink;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class IntegratedIndicatorState implements Serializable {
    // --- Heavy indicators ---
    public HeavyIndicatorState heavy = new HeavyIndicatorState();

    // --- Light indicators ---
    public Map<Integer, SimpleMovingAverage> sma = new HashMap<>();
    public Map<Integer, Double> ema = new HashMap<>();
}
