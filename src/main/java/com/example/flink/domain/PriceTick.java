package com.example.flink.domain;

public class PriceTick {
    public String accountId;
    public String symbol;
    public double price;
    public long ts;

    public PriceTick() {
    }

    public PriceTick(String accountId, String symbol, double price, long ts) {
        this.accountId = accountId;
        this.symbol = symbol;
        this.price = price;
        this.ts = ts;
    }
}
