package com.example.flink.domain;

import java.io.Serializable;
import java.util.UUID;

public class PositionLot implements Serializable {
    public PositionLot() {}

    public String accountId;
    public String strategyId;
    public String symbol;
    public String lotId;
    public String side;        // LONG / SHORT
    public double qtyOpen;     // original open qty
    public double qtyRem;      // remaining qty
    public double avgPrice;    // entry price
    public long tsOpen;        // open timestamp (ms)
    public long tsUpdated;     // last update timestamp (ms)

    // provenance for trade-match
    public String sourceOrderId;
    public String sourceFillId;

    public static PositionLot openNew(String accountId, String strategyId, String symbol,
                                      String orderId, String fillId,
                                      double qty, double price, long nowMs, String side) {
        PositionLot l = new PositionLot();
        l.accountId = accountId;
        l.strategyId = strategyId;
        l.symbol = symbol;
        l.lotId = (orderId != null && fillId != null) ? (orderId + "#" + fillId) : UUID.randomUUID().toString();
        l.side = side;
        l.qtyOpen = qty;
        l.qtyRem = qty;
        l.avgPrice = price;
        l.tsOpen = nowMs;
        l.tsUpdated = nowMs;
        l.sourceOrderId = orderId;
        l.sourceFillId = fillId;
        return l;
    }
}
