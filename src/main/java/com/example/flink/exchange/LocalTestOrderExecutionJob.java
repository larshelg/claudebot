package com.example.flink.exchange;

import com.example.flink.tradeengine.FakeFill;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.datastream.DataStream;
import com.example.flink.domain.ExecReport;
import com.example.flink.domain.TradeSignal;

/**
 * Static utility for local test order execution that processes trade signals
 * and produces execution reports for local testing/simulation.
 */
public class LocalTestOrderExecutionJob {
    
    /**
     * Process trade signals and return execution reports stream
     */
    public static DataStream<ExecReport> processTradeSignals(
            DataStream<TradeSignal> tradeSignals, 
            Sink<ExecReport> execReportSink) {
        
        // Convert accepted orders to filled execution reports (simulate instant fill)
        DataStream<ExecReport> execReports = tradeSignals
                .map(new FakeFill());

        // Sink execution reports
        if (execReportSink != null) {
            execReports.sinkTo(execReportSink);
        } else {
            execReports.print("LOCAL_EXEC");
        }

        // Debug output
        tradeSignals.print("ACCEPTED");
        execReports.print("FILLED");
        
        return execReports;
    }
}
