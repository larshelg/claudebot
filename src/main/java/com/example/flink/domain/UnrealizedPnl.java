package com.example.flink.domain;

public class UnrealizedPnl {
    public String accountId;
    public String symbol;
    public double unrealizedPnl;
    public double currentPrice;
    public double avgPrice;
    public double netQty;
    public long ts;

    public UnrealizedPnl() {
    }

    public UnrealizedPnl(String accountId, String symbol, double unrealizedPnl,
            double currentPrice, double avgPrice, double netQty, long ts) {
        this.accountId = accountId;
        this.symbol = symbol;
        this.unrealizedPnl = unrealizedPnl;
        this.currentPrice = currentPrice;
        this.avgPrice = avgPrice;
        this.netQty = netQty;
        this.ts = ts;
    }
}
