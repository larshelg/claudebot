package com.example.flink.tradeengine;

import com.example.flink.Candle;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.datastream.DataStream;
import com.example.flink.domain.AccountPolicy;
import com.example.flink.domain.ExecReport;
import com.example.flink.domain.TradeSignal;

import java.io.Serializable;

/**
 * Local test order execution job that processes trade signals through risk
 * checks
 * and produces execution reports for local testing/simulation.
 */
public class LocalTestOrderExecutionJob implements Serializable {
    private final DataStream<TradeSignal> tradeSignals;
    private final DataStream<AccountPolicy> accountPolicies;
    private final DataStream<Candle> candleStream;
    private final Sink<ExecReport> execReportSink;

    public LocalTestOrderExecutionJob(DataStream<TradeSignal> tradeSignals,
            DataStream<AccountPolicy> accountPolicies,
            Sink<ExecReport> execReportSink, DataStream<Candle> candleStream) {
        this.tradeSignals = tradeSignals;
        this.accountPolicies = accountPolicies;
        this.execReportSink = execReportSink;
        this.candleStream = candleStream;
    }

    public LocalTestOrderExecutionJob(DataStream<TradeSignal> tradeSignals,
            Sink<ExecReport> execReportSink, DataStream<Candle> candleStream) {
        this.tradeSignals = tradeSignals;
        this.accountPolicies = tradeSignals
                .map(ts -> new AccountPolicy(ts.accountId, 3, "ACTIVE", 100_000.0, ts.ts))
                .returns(AccountPolicy.class);
        this.execReportSink = execReportSink;
        this.candleStream = candleStream;
    }

    public void run() throws Exception {
        // Apply pre-trade risk checks
        DataStream<TradeSignal> acceptedOrders = tradeSignals
                .keyBy(ts -> ts.accountId)
                .connect(accountPolicies.keyBy(p -> p.accountId))
                .process(new PreTradeRiskCheckWithPolicy());

        // Convert accepted orders to filled execution reports (simulate instant fill)
        DataStream<ExecReport> execReports = acceptedOrders
                .map(new FakeFill());

        // Sink execution reports
        if (execReportSink != null) {
            execReports.sinkTo(execReportSink);
        } else {
            execReports.print("LOCAL_EXEC");
        }

        // Debug output
        acceptedOrders.print("ACCEPTED");
        execReports.print("FILLED");
    }
}
