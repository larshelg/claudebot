package com.example.flink.domain;

public class RiskAlert {
    public String accountId;
    public String message;

    public RiskAlert() {
    }

    public RiskAlert(String accountId, String message) {
        this.accountId = accountId;
        this.message = message;
    }
}
