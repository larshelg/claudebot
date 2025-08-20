package com.example.flink.domain;

public class RealizedPnl {
    public String accountId;
    public String symbol;
    public double realizedPnl;
    public long ts;

    public RealizedPnl() {
    }

    public RealizedPnl(String accountId, String symbol, double realizedPnl, long ts) {
        this.accountId = accountId;
        this.symbol = symbol;
        this.realizedPnl = realizedPnl;
        this.ts = ts;
    }
}
