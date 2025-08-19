package com.example.flink.tradeengine;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.connector.sink2.Sink;

import com.example.flink.domain.ExecReport;
import com.example.flink.domain.TradeMatch;

import java.time.Duration;

/**
 * Standalone Flink job for trade matching and P&L calculation.
 * 
 * This job reads ExecReports from a source (e.g., Fluss, Kafka, or test data)
 * and processes them through the FIFO trade matching engine to generate
 * TradeMatch records with realized P&L calculations.
 * 
 * Architecture:
 * ExecReports → KeyBy(account|symbol) → FIFOTradeMatchingEngine → TradeMatches → Sink
 */
public class TradeMatchingJob {
    
    private final DataStream<ExecReport> execReportSource;
    private final Sink<TradeMatch> tradeMatchSink;
    
    /**
     * Constructor for TradeMatchingJob
     * @param execReportSource Stream of execution reports to process
     * @param tradeMatchSink Sink for outputting trade matches
     */
    public TradeMatchingJob(DataStream<ExecReport> execReportSource, Sink<TradeMatch> tradeMatchSink) {
        this.execReportSource = execReportSource;
        this.tradeMatchSink = tradeMatchSink;
    }
    
    /**
     * Execute the trade matching job
     */
    public void run() throws Exception {
        // Process trade matching with event time and watermarks
        DataStream<ExecReport> timedExecReports = execReportSource
                .assignTimestampsAndWatermarks(
                    WatermarkStrategy.<ExecReport>forBoundedOutOfOrderness(
                        Duration.ofSeconds(5)) // Allow 5 second out-of-order events
                        .withTimestampAssigner((exec, ts) -> exec.ts));
        
        // Apply FIFO trade matching per account-symbol
        DataStream<TradeMatch> tradeMatches = timedExecReports
                .keyBy(exec -> exec.accountId + "|" + exec.symbol)
                .process(new FIFOTradeMatchingEngine());
        
        // Sink trade matches to output (Fluss, test sink, etc.)
        tradeMatches.sinkTo(tradeMatchSink);
        
        // Optional: Add debugging output
        tradeMatches.print("TRADE_MATCH");
    }
    
    /**
     * Main method for running as standalone job
     * This would be used in production with proper source/sink configuration
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // In production, this would read from Fluss:
        // DataStream<ExecReport> execReports = env
        //     .addSource(new FlussSource<>("exec_reports", ExecReport.class))
        //     .name("ExecReport Source");
        
        // For now, create empty stream as placeholder
        DataStream<ExecReport> execReports = env.fromCollection(java.util.Collections.<ExecReport>emptyList());
        
        // In production, this would write to Fluss:
        // Sink<TradeMatch> tradeMatchSink = new FlussSink<>("trade_matches");
        
        // For now, just print
        Sink<TradeMatch> tradeMatchSink = null; // Would be configured in production
        
        // Create and run job
        TradeMatchingJob job = new TradeMatchingJob(execReports, tradeMatchSink);
        job.run();
        
        env.execute("Trade Matching Job");
    }
}