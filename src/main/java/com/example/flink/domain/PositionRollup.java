package com.example.flink.domain;


import java.io.Serializable;

/** Flink POJO used in keyed state (summary per account/strategy/symbol). */
public class PositionRollup implements Serializable {
    public PositionRollup() {}

    public String accountId;
    public String strategyId;
    public String symbol;
    public String side;       // LONG / SHORT / null (flat)
    public double netQty;
    public double avgPrice;   // positive, weighted by remaining lots
    public long lastUpdated;  // ms
}

