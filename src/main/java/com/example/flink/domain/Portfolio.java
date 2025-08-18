package com.example.flink.domain;

public class Portfolio {
    public String accountId;
    public double equity;
    public double cashBalance;
    public double exposure;
    public double marginUsed;

    public Portfolio() {
    }

    public Portfolio(String accountId) {
        this.accountId = accountId;
        this.equity = 0.0;
        this.cashBalance = 0.0;
        this.exposure = 0.0;
        this.marginUsed = 0.0;
    }
}
