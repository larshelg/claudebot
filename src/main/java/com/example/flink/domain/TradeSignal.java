package com.example.flink.domain;

public class TradeSignal {
    public String accountId;
    public String symbol;
    public double qty;
    public double price;
    public long ts;

    public TradeSignal() {
    }

    public TradeSignal(String accountId, String symbol, double qty, double price, long ts) {
        this.accountId = accountId;
        this.symbol = symbol;
        this.qty = qty;
        this.price = price;
        this.ts = ts;
    }
}
