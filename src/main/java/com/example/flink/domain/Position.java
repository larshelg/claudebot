package com.example.flink.domain;

public class Position {
    public String accountId;
    public String symbol;
    public double netQty;
    public double avgPrice;
    public long lastUpdated; // Timestamp for Fluss logging

    public Position() {
    }

    public Position(String accountId, String symbol) {
        this.accountId = accountId;
        this.symbol = symbol;
        this.netQty = 0.0;
        this.avgPrice = 0.0;
        this.lastUpdated = System.currentTimeMillis();
    }
}
