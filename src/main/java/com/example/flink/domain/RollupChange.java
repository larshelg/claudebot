package com.example.flink.domain;

import java.io.Serializable;

// --- Changelog op ---



// --- Rollup changelog input (from fluss.open_positions_rollup) ---
public class RollupChange implements Serializable {
    public RollupChange() {}
    public Op op;
    public String accountId, strategyId, symbol;
    public double netQty, avgPrice;
    public long ts;
    public static RollupChange upsert(String a, String s, String sym, double netQty, double avgPrice, long ts) {
        RollupChange r = new RollupChange();
        r.op = Op.UPSERT; r.accountId=a; r.strategyId=s; r.symbol=sym; r.netQty=netQty; r.avgPrice=avgPrice; r.ts=ts; return r;
    }
    public static RollupChange delete(String a, String s, String sym, long ts) {
        RollupChange r = new RollupChange();
        r.op = Op.DELETE; r.accountId=a; r.strategyId=s; r.symbol=sym; r.ts=ts; return r;
    }
}
