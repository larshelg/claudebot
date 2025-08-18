package com.example.flink.domain;

public class AccountPolicy {
    public String accountId;
    public int maxOpenSymbols;
    public String status; // ACTIVE / BLOCKED
    public long ts;

    public AccountPolicy() {
    }

    public AccountPolicy(String accountId, int maxOpenSymbols, String status, long ts) {
        this.accountId = accountId;
        this.maxOpenSymbols = maxOpenSymbols;
        this.status = status;
        this.ts = ts;
    }
}
