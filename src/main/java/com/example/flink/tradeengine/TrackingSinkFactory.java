package com.example.flink.tradeengine;

import org.apache.flink.api.connector.sink2.Sink;
import com.example.flink.domain.ExecReport;
import com.example.flink.domain.TradeMatch;

/**
 * Factory interface for creating tracking sinks.
 * Provides abstraction between test sinks and production sinks (e.g., Fluss).
 */
public interface TrackingSinkFactory {
    
    /**
     * Creates a sink for ExecReport tracking.
     * @return Sink for storing execution reports
     */
    Sink<ExecReport> createExecReportSink();
    
    /**
     * Creates a sink for TradeMatch tracking.
     * @return Sink for storing trade matches with P&L
     */
    Sink<TradeMatch> createTradeMatchSink();
}