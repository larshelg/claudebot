package com.example.flink.domain;

import java.io.Serializable;

// --- Unrealized PnL output (to fluss.unrealized_pnl_latest) ---
public class UnrealizedPnlChange implements Serializable {
    public UnrealizedPnlChange() {}
    public Op op;
    public String accountId, strategyId, symbol;
    public double unrealizedPnl, currentPrice, avgPrice, netQty;
    public long ts;
    public static UnrealizedPnlChange upsert(String a, String s, String sym,
                                             double pnl, double currentPrice, double avg, double netQty, long ts) {
        UnrealizedPnlChange u = new UnrealizedPnlChange();
        u.op = Op.UPSERT; u.accountId=a; u.strategyId=s; u.symbol=sym;
        u.unrealizedPnl=pnl; u.currentPrice=currentPrice; u.avgPrice=avg; u.netQty=netQty; u.ts=ts; return u;
    }
    public static UnrealizedPnlChange delete(String a, String s, String sym, long ts) {
        UnrealizedPnlChange u = new UnrealizedPnlChange();
        u.op = Op.DELETE; u.accountId=a; u.strategyId=s; u.symbol=sym; u.ts=ts; return u;
    }
}
