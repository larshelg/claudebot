package com.example.flink;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.Deque;

public class HeavyIndicatorState implements Serializable {
    private final Deque<Double> closes = new ArrayDeque<>();
    private double gainSum = 0;
    private double lossSum = 0;

    public void addClosePrice(double close) {
        if (!closes.isEmpty()) {
            double change = close - closes.getLast();
            if (change > 0) {
                gainSum += change;
            } else {
                lossSum -= change;
            }
        }
        closes.addLast(close);
        if (closes.size() > 14) {
            double removed = closes.removeFirst();
            if (!closes.isEmpty()) {
                double prev = closes.peekFirst();
                double change = prev - removed;
                if (change > 0) {
                    gainSum -= change;
                } else {
                    lossSum += change;
                }
            }
        }
    }

    public double calculateRSI(int period) {
        if (closes.size() < period)
            return 0;
        double avgGain = gainSum / period;
        double avgLoss = lossSum / period;
        if (avgLoss == 0)
            return 100;
        double rs = avgGain / avgLoss;
        return 100 - (100 / (1 + rs));
    }
}
