package com.example.flink.domain;

public class PositionClose {
    public String accountId;
    public String symbol;
    public double totalQty; // quantity closed
    public double avgPrice; // average price at close
    public long openTs; // optional; best-effort if known
    public long closeTs; // time of close event
    public double realizedPnl; // optional; aggregate over lifetime if available

    public PositionClose() {
    }

    public PositionClose(String accountId, String symbol, double totalQty, double avgPrice,
            long openTs, long closeTs, double realizedPnl) {
        this.accountId = accountId;
        this.symbol = symbol;
        this.totalQty = totalQty;
        this.avgPrice = avgPrice;
        this.openTs = openTs;
        this.closeTs = closeTs;
        this.realizedPnl = realizedPnl;
    }
}
