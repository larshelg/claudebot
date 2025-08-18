package com.example.flink.tradeengine;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import com.example.flink.domain.Portfolio;
import com.example.flink.domain.RiskAlert;

public class RiskEngine extends ProcessFunction<Portfolio, RiskAlert> {
    @Override
    public void processElement(Portfolio pf, Context ctx, Collector<RiskAlert> out) {
        if (pf.exposure > 1_000_000) {
            RiskAlert alert = new RiskAlert(pf.accountId, "Exposure limit breached!");
            out.collect(alert);
        }
    }
}