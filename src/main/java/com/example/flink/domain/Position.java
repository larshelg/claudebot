package com.example.flink.domain;

public class Position {
    public String accountId;
    public String symbol;
    public double netQty;
    public double avgPrice;
    public double realizedPnl;
    public double unrealizedPnl;

    public Position() {
    }

    public Position(String accountId, String symbol) {
        this.accountId = accountId;
        this.symbol = symbol;
        this.netQty = 0.0;
        this.avgPrice = 0.0;
        this.realizedPnl = 0.0;
        this.unrealizedPnl = 0.0;
    }
}
