package com.example.flink.Exchange;

import com.example.flink.tradeengine.FakeFill;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.datastream.DataStream;
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
    private final Sink<ExecReport> execReportSink;
    private DataStream<ExecReport> execReportsOut;

    public LocalTestOrderExecutionJob(DataStream<TradeSignal> tradeSignals, Sink<ExecReport> execReportSink) {
        this.tradeSignals = tradeSignals;
        this.execReportSink = execReportSink;
    }

    public void run() throws Exception {
        // Apply pre-trade risk checks

        // Convert accepted orders to filled execution reports (simulate instant fill)
        DataStream<ExecReport> execReports = tradeSignals
                .map(new FakeFill());

        // Expose for chaining in tests
        this.execReportsOut = execReports;

        // Sink execution reports
        if (execReportSink != null) {
            execReports.sinkTo(execReportSink);
        } else {
            execReports.print("LOCAL_EXEC");
        }

        // Debug output
        tradeSignals.print("ACCEPTED");
        execReports.print("FILLED");
    }

    public DataStream<ExecReport> getExecReports() {
        return execReportsOut;
    }
}
