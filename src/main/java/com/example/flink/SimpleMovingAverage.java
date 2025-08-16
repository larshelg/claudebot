package com.example.flink;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.Deque;

public class SimpleMovingAverage implements Serializable {
    private final int period;
    private final Deque<Double> values = new ArrayDeque<>();
    private double sum;

    public SimpleMovingAverage(int period) {
        this.period = period;
    }

    public void add(double value) {
        values.addLast(value);
        sum += value;
        if (values.size() > period) {
            sum -= values.removeFirst();
        }
    }

    public double get() {
        return values.isEmpty() ? 0 : sum / values.size();
    }
}
