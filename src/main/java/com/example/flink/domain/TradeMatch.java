package com.example.flink.domain;

import java.util.UUID;

public class TradeMatch {
    public String matchId;        // UUID for unique matching record
    public String accountId;
    public String symbol;
    public String buyOrderId;     // Reference to buy ExecReport
    public String sellOrderId;    // Reference to sell ExecReport
    public double matchedQty;     // Quantity matched between buy/sell
    public double buyPrice;       // Price from buy execution
    public double sellPrice;      // Price from sell execution
    public double realizedPnl;    // (sell_price - buy_price) * matched_qty
    public long matchTimestamp;   // When the match was created
    public long buyTimestamp;     // Original buy execution time
    public long sellTimestamp;    // Original sell execution time

    public TradeMatch() {
    }

    public TradeMatch(String accountId, String symbol, 
                     String buyOrderId, String sellOrderId,
                     double matchedQty, double buyPrice, double sellPrice,
                     long buyTimestamp, long sellTimestamp) {
        this.matchId = UUID.randomUUID().toString();
        this.accountId = accountId;
        this.symbol = symbol;
        this.buyOrderId = buyOrderId;
        this.sellOrderId = sellOrderId;
        this.matchedQty = matchedQty;
        this.buyPrice = buyPrice;
        this.sellPrice = sellPrice;
        this.realizedPnl = (sellPrice - buyPrice) * matchedQty;
        this.matchTimestamp = System.currentTimeMillis();
        this.buyTimestamp = buyTimestamp;
        this.sellTimestamp = sellTimestamp;
    }
}