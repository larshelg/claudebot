package com.example.flink.domain;


import java.io.Serializable;

// --- State row for positions map ---
public class RollupRow implements Serializable {
    public RollupRow() {}
    public String accountId, strategyId, symbol;
    public double netQty, avgPrice;
    public long ts;
    public RollupRow(String a, String s, String sym, double q, double avg, long ts) {
        this.accountId=a; this.strategyId=s; this.symbol=sym; this.netQty=q; this.avgPrice=avg; this.ts=ts;
    }
}
