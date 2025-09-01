package com.example.flink.domain;

import java.io.Serializable;

public class TradeSignal {
    public String signalId;
    public String accountId, strategyId, symbol, action;
    public double qty;
    public double price;
    public long ts;

    public TradeSignal() {
    }

    public TradeSignal(String accountId, String strategyId, String symbol, String action, double qty, double price, long ts) {
        this.accountId = accountId;
        this.strategyId = strategyId;
        this.symbol = symbol;
        this.action = action;
        this.qty = qty;
        this.price = price;
        this.ts = ts;
    }

    public TradeSignal(String account1, String symbol, double v, double close, long timestamp) {
    }
}

