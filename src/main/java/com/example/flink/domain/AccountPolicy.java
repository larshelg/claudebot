package com.example.flink.domain;

public class AccountPolicy {
    public String accountId;
    public int maxOpenSymbols;
    public String status; // ACTIVE / BLOCKED
    public double initialCapital;
    public long ts;

    public AccountPolicy() {
    }

    public AccountPolicy(String accountId, int maxOpenSymbols, String status, long ts) {
        this.accountId = accountId;
        this.maxOpenSymbols = maxOpenSymbols;
        this.status = status;
        this.initialCapital = 100_000.0; // Default capital
        this.ts = ts;
    }

    public AccountPolicy(String accountId, int maxOpenSymbols, String status, double initialCapital, long ts) {
        this.accountId = accountId;
        this.maxOpenSymbols = maxOpenSymbols;
        this.status = status;
        this.initialCapital = initialCapital;
        this.ts = ts;
    }
}
