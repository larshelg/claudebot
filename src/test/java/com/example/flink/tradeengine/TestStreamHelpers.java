package com.example.flink.tradeengine;

import com.example.flink.domain.AccountPolicy;
import com.example.flink.domain.ExecReport;
import com.example.flink.domain.TradeSignal;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;

public class TestStreamHelpers {

    public static TradeSignal createTradeSignal(String accountId, String symbol, double qty,
            double price,
            long timestamp) {
        return new TradeSignal(accountId, symbol, qty, price, timestamp);
    }

    public static ExecReport createExecReport(String accountId, String orderId, String symbol,
            double fillQty, double fillPrice, String status, long timestamp) {
        return new ExecReport(accountId, orderId, symbol, fillQty, fillPrice, status, timestamp);
    }

    public static DataStream<TradeSignal> createTradeSignalStream(StreamExecutionEnvironment env,
            List<TradeSignal> data) {
        DataStream<TradeSignal> base;
        if (data == null || data.isEmpty()) {
            base = env.fromElements(new TradeSignal("__empty__", "__empty__", 0.0, 0.0, 0L))
                    .filter(e -> false);
        } else {
            base = env.fromCollection(data);
        }
        return base.assignTimestampsAndWatermarks(
                WatermarkStrategy.<TradeSignal>forBoundedOutOfOrderness(
                        Duration.ofSeconds(1))
                        .withTimestampAssigner((e, ts) -> e.ts));
    }

    public static DataStream<ExecReport> createExecReportStream(StreamExecutionEnvironment env,
            List<ExecReport> data) {
        DataStream<ExecReport> base;
        if (data == null || data.isEmpty()) {
            base = env.fromElements(
                    createExecReport("__empty__", "__empty__", "__empty__", 0.0, 0.0, "FILLED", 0L))
                    .filter(e -> false);
        } else {
            base = env.fromCollection(data);
        }
        return base.assignTimestampsAndWatermarks(
                WatermarkStrategy.<ExecReport>forBoundedOutOfOrderness(
                        Duration.ofSeconds(1))
                        .withTimestampAssigner((e, ts) -> e.ts));
    }

    public static DataStream<AccountPolicy> createAccountPolicyStreamWithWatermarks(
            StreamExecutionEnvironment env, List<AccountPolicy> data) {
        DataStream<AccountPolicy> base;
        if (data == null || data.isEmpty()) {
            base = env.fromElements(new AccountPolicy("__empty__", 0, "ACTIVE", 0.0, 0L))
                    .filter(e -> false);
        } else {
            base = env.fromCollection(data);
        }
        return base.assignTimestampsAndWatermarks(
                WatermarkStrategy.<AccountPolicy>forBoundedOutOfOrderness(
                        Duration.ofSeconds(1))
                        .withTimestampAssigner((policy, ts) -> policy.ts));
    }

    public static DataStream<TradeSignal> createTradeSignalStreamWithWatermarks(
            StreamExecutionEnvironment env, List<TradeSignal> data) {
        DataStream<TradeSignal> base;
        if (data == null || data.isEmpty()) {
            base = env.fromElements(new TradeSignal("__empty__", "__empty__", 0.0, 0.0, 0L))
                    .filter(e -> false);
        } else {
            base = env.fromCollection(data);
        }
        return base.assignTimestampsAndWatermarks(
                WatermarkStrategy.<TradeSignal>forBoundedOutOfOrderness(
                        Duration.ofSeconds(1))
                        .withTimestampAssigner((signal, ts) -> signal.ts));
    }
}
