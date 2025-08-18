package com.example.flink.domain;

public class ExecReport {
    public String accountId;
    public String orderId;
    public String symbol;
    public double fillQty;
    public double fillPrice;
    public String status; // FILLED / PARTIAL / REJECTED
    public long ts;

    public ExecReport() {
    }

    public ExecReport(String accountId, String orderId, String symbol, double fillQty, double fillPrice, String status,
            long ts) {
        this.accountId = accountId;
        this.orderId = orderId;
        this.symbol = symbol;
        this.fillQty = fillQty;
        this.fillPrice = fillPrice;
        this.status = status;
        this.ts = ts;
    }
}
