package com.example.flink.tradeengine;

import org.apache.flink.api.common.functions.MapFunction;

import com.example.flink.domain.ExecReport;
import com.example.flink.domain.TradeSignal;

import java.util.UUID;

public class FakeFill implements MapFunction<TradeSignal, ExecReport> {
    @Override
    public ExecReport map(TradeSignal signal) {
        String orderId = UUID.randomUUID().toString();
        return new ExecReport(
                signal.accountId,
                orderId,
                signal.symbol,
                signal.qty,
                signal.price,
                "FILLED",
                signal.ts);
    }
}