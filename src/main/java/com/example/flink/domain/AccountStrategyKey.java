package com.example.flink.domain;

import java.io.Serializable;
import java.util.Objects;

// --- MapState key per account+strategy ---
public class AccountStrategyKey implements Serializable {
    public AccountStrategyKey() {}
    public String accountId;
    public String strategyId;
    public AccountStrategyKey(String a, String s) { this.accountId=a; this.strategyId=s; }
    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AccountStrategyKey)) return false;
        AccountStrategyKey that = (AccountStrategyKey) o;
        return Objects.equals(accountId, that.accountId) && Objects.equals(strategyId, that.strategyId);
    }
    @Override public int hashCode() { return Objects.hash(accountId, strategyId); }
}
